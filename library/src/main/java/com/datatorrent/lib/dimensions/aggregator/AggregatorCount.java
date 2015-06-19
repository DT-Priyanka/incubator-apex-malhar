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
package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.Aggregate;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 * This {@link IncrementalAggregator} performs a count of the number of times an input is encountered.
 */
public class AggregatorCount<EVENT> extends AbstractIncrementalAggregator<EVENT>
{
  private static final long serialVersionUID = 20154301645L;

  /**
   * This is a map whose keys represent input types and whose values
   * represent the corresponding output types.
   */
  public static transient final Map<Type, Type> TYPE_CONVERSION_MAP;

  static {
    Map<Type, Type> typeConversionMap = Maps.newHashMap();

    for(Type type: Type.values()) {
      typeConversionMap.put(type, Type.LONG);
    }

    TYPE_CONVERSION_MAP = Collections.unmodifiableMap(typeConversionMap);
  }

  /**
   * This constructor is not exposed for singleton pattern.
   */
  private AggregatorCount()
  {
    //Do nothing
  }

  @Override
  public void aggregate(Aggregate dest, EVENT src)
  {
    long[] fieldsLong = dest.getAggregates().getFieldsLong();

    for(int index = 0;
        index < fieldsLong.length;
        index++) {
      //increment count
      fieldsLong[index]++;
    }
  }

  @Override
  public void aggregate(Aggregate destAgg, Aggregate srcAgg)
  {
    long[] destLongs = destAgg.getAggregates().getFieldsLong();
    long[] srcLongs = srcAgg.getAggregates().getFieldsLong();

    for(int index = 0;
        index < destLongs.length;
        index++) {
      //aggregate count
      destLongs[index] += srcLongs[index];
    }
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return TYPE_CONVERSION_MAP.get(inputType);
  }
}
