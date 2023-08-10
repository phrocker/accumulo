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
package org.apache.accumulo.core.file.rfile.rfiletests;

import java.io.File;
import java.util.Comparator;

import org.apache.hadoop.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class TestRuntime implements Comparator<TestRuntime>, Comparable<TestRuntime> {
  public final long averageRuntime;
  public final long keysFound;
  public final long fileSize;

  public final String filename;

  public long totalRuntime;
  public String description;

  public TestRuntime(String description, String filename, long keysFound, long averageRuntime,
      long totalRuntime) {
    this.description = description;
    this.filename = filename;
    this.keysFound = keysFound;
    this.averageRuntime = averageRuntime;
    this.totalRuntime = totalRuntime;
    this.fileSize = new File(filename).length();
  }

  @Override
  public int compare(TestRuntime testRuntime, TestRuntime t1) {
    int compare = Long.compare(testRuntime.totalRuntime, t1.totalRuntime);
    if (compare == 0) {
      compare = Long.compare(testRuntime.fileSize, t1.fileSize);
      if (compare == 0) {
        compare = testRuntime.description.compareTo(t1.description);
      }
    }
    return compare;
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int compareTo(TestRuntime testRuntime) {
    return compare(this, testRuntime);
  }
}
