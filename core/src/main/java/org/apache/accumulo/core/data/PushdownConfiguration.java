/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.PushdownReaderRequest;

public class PushdownConfiguration {

  private final String rfileClass;

  private Map<String,String> rfileReaderOptions = new HashMap<>();

  private List<IterInfo> pushdownList = new ArrayList<>();

  private Map<String,Map<String,String>> pushdownOptions = new HashMap<>();

  private PushdownConfiguration(String rfileClass, Map<String,String> rfileReaderOptions,
      List<IterInfo> pushdownList, Map<String,Map<String,String>> pushdownOptions) {
    this.rfileClass = rfileClass;
    this.rfileReaderOptions = rfileReaderOptions;
    this.pushdownList = pushdownList;
    this.pushdownOptions = pushdownOptions;
  }

  public PushdownReaderRequest toThrift() {
    PushdownReaderRequest rqst = new PushdownReaderRequest();
    rqst.readerClassName = this.rfileClass;
    rqst.readerClassOptions = rfileReaderOptions;
    rqst.pushDownList = this.pushdownList;
    rqst.pushDownListOptions = this.pushdownOptions;
    return rqst;
  }

  public static class PushhDownConfigurationBuilder {
    // canonical name of the class.
    private final String rfileClass;

    private Map<String,String> rfileReaderOptions = new HashMap<>();

    private List<IterInfo> pushdownList = new ArrayList<>();

    private Map<String,Map<String,String>> pushdownOptions = new HashMap<>();

    private PushhDownConfigurationBuilder(String rfileClass) {
      this.rfileClass = rfileClass;
    }

    public static PushdownConfiguration empty() {
      return newBuilder("").build();
    }

    public static PushhDownConfigurationBuilder newBuilder(String rfileClass) {
      return new PushhDownConfigurationBuilder(rfileClass);
    }

    public PushhDownConfigurationBuilder withRfileReaderOptions(Map<String,String> options) {
      if (null != options) {
        this.rfileReaderOptions = options;
      }
      return this;
    }

    public PushhDownConfigurationBuilder withPushdown(String name, String clazz,
        Map<String,String> options) {
      Objects.requireNonNull(name);
      Objects.requireNonNull(clazz);
      pushdownOptions.put(name, new HashMap<>());
      if (null != options) {
        pushdownOptions.get(name).putAll(options);
      }
      pushdownList.add(new IterInfo(pushdownList.size(), clazz, name));
      return this;
    }

    public PushdownConfiguration build() {
      return new PushdownConfiguration(rfileClass, rfileReaderOptions, pushdownList,
          pushdownOptions);
    }

  }
}
