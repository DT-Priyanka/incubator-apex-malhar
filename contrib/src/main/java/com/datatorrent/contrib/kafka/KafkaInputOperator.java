package com.datatorrent.contrib.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.commons.lang3.tuple.MutablePair;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.kafka.KafkaConsumer.KafkaMessage;

public class KafkaInputOperator extends AbstractKafkaInputOperator<KafkaConsumer>
{
  private transient KafkaConsumer.KafkaMessage pendingMessage;
  private transient int emitCount = 0;
  private transient long emitTotalMsgSize = 0;
  public final transient DefaultOutputPort<MutablePair<Message, MutablePair<Long, Integer>>> outputPort = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    emitCount = 0;
    emitTotalMsgSize = 0;
  }

  @Override
  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      Map<KafkaPartition, MutablePair<Long, Integer>> recoveredData = (Map<KafkaPartition, MutablePair<Long, Integer>>) windowDataManager.load(operatorId, windowId);
      if (recoveredData != null) {
        Map<String, List<PartitionMetadata>> pms = KafkaMetadataUtil.getPartitionsForTopic(getConsumer().brokers, getConsumer().topic);
        if (pms != null) {
          SimpleKafkaConsumer cons = (SimpleKafkaConsumer) getConsumer();
          // add all partition request in one Fretch request together
          FetchRequestBuilder frb = new FetchRequestBuilder().clientId(cons.getClientId());
          for (Map.Entry<KafkaPartition, MutablePair<Long, Integer>> rc : recoveredData.entrySet()) {
            KafkaPartition kp = rc.getKey();
            List<PartitionMetadata> pmsVal = pms.get(kp.getClusterId());

            Iterator<PartitionMetadata> pmIterator = pmsVal.iterator();
            PartitionMetadata pm = pmIterator.next();
            while (pm.partitionId() != kp.getPartitionId()) {
              if (!pmIterator.hasNext())
                break;
              pm = pmIterator.next();
            }
            if (pm.partitionId() != kp.getPartitionId())
              continue;

            Broker bk = pm.leader();

            frb.addFetch(consumer.topic, rc.getKey().getPartitionId(), rc.getValue().left, cons.getBufferSize());
            FetchRequest req = frb.build();

            SimpleConsumer ksc = new SimpleConsumer(bk.host(), bk.port(), cons.getTimeout(), cons.getBufferSize(), cons.getClientId());
            FetchResponse fetchResponse = ksc.fetch(req);
            Integer count = 0;
            for (MessageAndOffset msg : fetchResponse.messageSet(consumer.topic, kp.getPartitionId())) {
              KafkaMessage kafkaMessage = new KafkaMessage(kp, msg.message(), msg.offset());
              emitTuple(kafkaMessage);
              offsetStats.put(kp, msg.offset());
              count = count + 1;
              if (count.equals(rc.getValue().right))
                break;
            }
          }
        }
      }
      if (windowId == windowDataManager.getLargestRecoveryWindow()) {
        // Start the consumer at the largest recovery window
        SimpleKafkaConsumer cons = (SimpleKafkaConsumer) getConsumer();
        // Set the offset positions to the consumer
        Map<KafkaPartition, Long> currentOffsets = new HashMap<KafkaPartition, Long>(cons.getCurrentOffsets());
        // Increment the offsets
        for (Map.Entry<KafkaPartition, Long> e : offsetStats.entrySet()) {
          currentOffsets.put(e.getKey(), e.getValue() + 1);
        }
        cons.resetOffset(currentOffsets);
        cons.start();
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= windowDataManager.getLargestRecoveryWindow()) {
      return;
    }
    int count = consumer.messageSize() + ((pendingMessage != null) ? 1 : 0);
    if (getMaxTuplesPerWindow() > 0) {
      count = Math.min(count, getMaxTuplesPerWindow() - emitCount);
    }
    KafkaConsumer.KafkaMessage message = null;
    for (int i = 0; i < count; i++) {
      if (pendingMessage != null) {
        message = pendingMessage;
        pendingMessage = null;
      } else {
        message = consumer.pollMessage();
      }
      // If the total size transmitted in the window will be exceeded don't transmit anymore messages in this window
      // Make an exception for the case when no message has been transmitted in the window and transmit at least one
      // message even if the condition is violated so that the processing doesn't get stuck
      if ((emitCount > 0) && ((getMaxTotalMsgSizePerWindow() - emitTotalMsgSize) < message.msg.size())) {
        pendingMessage = message;
        break;
      }
      emitTuple(message);
      emitCount++;
      emitTotalMsgSize += message.msg.size();
      offsetStats.put(message.kafkaPart, message.offSet);
      MutablePair<Long, Integer> offsetAndCount = currentWindowRecoveryState.get(message.kafkaPart);
      if (offsetAndCount == null) {
        currentWindowRecoveryState.put(message.kafkaPart, new MutablePair<Long, Integer>(message.offSet, 1));
      } else {
        offsetAndCount.setRight(offsetAndCount.right + 1);
      }
    }
  }

  protected void emitTuple(KafkaMessage message)
  {
    outputPort.emit(new MutablePair<>(message.getMsg(), new MutablePair<>(message.offSet, message.getKafkaPart().getPartitionId())));
  }

  @Override
  protected void emitTuple(Message message)
  {
    // placeholder implementation won't use it
  }

}
