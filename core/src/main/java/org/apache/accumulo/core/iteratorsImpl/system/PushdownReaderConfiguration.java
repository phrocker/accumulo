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
package org.apache.accumulo.core.iteratorsImpl.system;

import org.apache.accumulo.core.dataImpl.thrift.PushdownReaderRequest;
import org.apache.accumulo.core.file.rfile.VisibilityPushdownFiltering;
import org.apache.commons.collections4.map.LRUMap;

public class PushdownReaderConfiguration {

  private static LRUMap<String,Boolean> canSkipVisibilityFilter = new LRUMap<>();
  private final boolean skipcfSkippingIterator;
  private final boolean skipCqSkippingIterator;
  private final boolean skipVisibilityFiltering;

  private PushdownReaderRequest readerRequest;

  private boolean canSkipVisibilityFiltering = false;

  private PushdownReaderConfiguration(PushdownReaderRequest readerRequest) {
    this.readerRequest = readerRequest;
    if (null != readerRequest) {
      if (!readerRequest.getReaderClassName().isEmpty()) {
        canSkipVisibilityFiltering = checkCacheForFiltering(readerRequest.readerClassName);
      }

      skipcfSkippingIterator = readerRequest.isDisableCfsIter();
      skipCqSkippingIterator = readerRequest.isDisableCqsIter();
      if (canSkipVisibilityFiltering) {
        skipVisibilityFiltering = readerRequest.isDisableVisIter();
      } else {
        skipVisibilityFiltering = false;
      }
    } else {
      skipVisibilityFiltering = false;
      skipcfSkippingIterator = false;
      skipCqSkippingIterator = false;
    }
  }

  private static synchronized boolean checkCacheForFiltering(String clazzName) {
    boolean canSkipFiltering = false;
    if (!clazzName.isEmpty()) {
      if (canSkipVisibilityFilter.containsKey(clazzName)) {
        canSkipFiltering = canSkipVisibilityFilter.get(clazzName);
      } else {
        try {
          if (VisibilityPushdownFiltering.class.isAssignableFrom(Class.forName(clazzName))) {
            canSkipFiltering = true;
          } else {
            canSkipFiltering = false;
          }
          canSkipVisibilityFilter.put(clazzName, canSkipFiltering);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return canSkipFiltering;
  }

  public static PushdownReaderConfiguration
      createReaderConfiguration(PushdownReaderRequest readerRequest) {
    return new PushdownReaderConfiguration(readerRequest);
  }

  public static PushdownReaderConfiguration defaultConfiguration() {
    return new PushdownReaderConfiguration(null);
  }

  public boolean isSkipVisibilityFiltering() {
    return skipVisibilityFiltering;
  }

  public boolean isSkipcfSkippingIterator() {
    return skipcfSkippingIterator;
  }

  public boolean isSkipCqSkippingIterator() {
    return skipCqSkippingIterator;
  }
}
