/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.tabular;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AppDataTabularServerMapTest
{
  public static final String SIMPLE_RESULT = "{\"id\":\"1\",\"type\":\"dataQuery\",\"data\":[{\"count\":\"2\",\"word\":\"a\"},{\"count\":\"3\",\"word\":\"b\"}],\"countdown\":10}";
  public static final String SIMPLE_QUERY = "{\"id\": \"1\",\n"
                                            + "\"type\": \"dataQuery\",\n"
                                            + "\"data\": {\n"
                                            + "\"fields\": [ \"word\", \"count\" ]\n"
                                            + "},\n"
                                            + "\"countdown\":10\n"
                                            + "}";
  public static final String SIMPLE_SCHEMA = "{\n"
                                             + "  \"values\": [{\"name\": \"word\", \"type\": \"string\"},\n"
                                             + "             {\"name\": \"count\", \"type\": \"integer\"}]\n"
                                             + "}";

  @Test
  public void simpleTest() throws Exception
  {
    AppDataTabularServerMap tabularServer = new AppDataTabularServerMap();

    tabularServer.setTabularSchemaJSON(SIMPLE_SCHEMA);

    //// Input Data

    List<Map<String, Object>> dataList = Lists.newArrayList();
    Map<String, Object> data = Maps.newHashMap();
    data.put("word", "a");
    data.put("count", 2);

    Map<String, Object> data1 = Maps.newHashMap();
    data1.put("word", "b");
    data1.put("count", 3);

    dataList.add(data);
    dataList.add(data1);

    ////

    CollectorTestSink<String> resultSink = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tempResultSink = (CollectorTestSink) resultSink;
    tabularServer.queryResult.setSink(tempResultSink);

    tabularServer.setup(null);

    tabularServer.beginWindow(0L);
    tabularServer.input.put(dataList);
    tabularServer.endWindow();

    tabularServer.beginWindow(1L);
    tabularServer.query.put(SIMPLE_QUERY);
    tabularServer.endWindow();

    LOG.debug("result {}", tempResultSink.collectedTuples.get(0));

    Assert.assertEquals("Should get only 1 result back", 1, tempResultSink.collectedTuples.size());
    Assert.assertEquals("The result was incorrect.", SIMPLE_RESULT, tempResultSink.collectedTuples.get(0));

    //Test serialization
    TestUtils.clone(new Kryo(), tabularServer);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AppDataTabularServerMapTest.class);
}
