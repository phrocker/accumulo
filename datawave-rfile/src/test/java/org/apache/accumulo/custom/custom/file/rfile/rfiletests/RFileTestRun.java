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
package org.apache.accumulo.custom.custom.file.rfile.rfiletests;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public abstract class RFileTestRun {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  final String filename;
  final File file;

  long startTime = System.currentTimeMillis();
  long endTime = System.currentTimeMillis();

  TestRFile rfileTest;

  SortedKeyValueIterator<Key,Value> topIter = null;

  Set<String> auths;

  long keysCounted = 0;

  public RFileTestRun(TestRFile rfileTest, String filename) {
    this.rfileTest = rfileTest;
    this.filename = filename;
    this.file = new File(this.filename);
  }

  public void setStartTime() {
    this.startTime = System.currentTimeMillis();
  }

  public void setEndTime() {
    this.endTime = System.currentTimeMillis();
  }

  public long getRuntime(int numberRuns) {
    return (endTime - startTime) / numberRuns;
  }

  public abstract void configureIterators() throws IOException;

  public abstract void configurebaseLayer() throws IOException;

  public void setAuths(Set<String> auths) {
    this.auths = auths;
  }

  public void resetKeysCounted() {
    this.keysCounted = 0;
  }

  public long consumeAllKeys(Range range, Writer outstream) throws IOException {

    // rfileTest.reader.seek(range,EMPTY_COL_FAMS, false);
    topIter.seek(range, EMPTY_COL_FAMS, false);

    while (topIter.hasTop()) {
      var kv = topIter.getTopKey();
      if (null != outstream) {
        var write = kv.toString();
        write += "\n";
        outstream.write(write);
        // System.out.println("keysie " + kv.toString());
      }
      topIter.next();
      ++keysCounted;
    }

    return keysCounted;
  }

  public void close() throws IOException {
    rfileTest.closeReader();
  }

  public TestRuntime getDescription(int numberRuns) {
    return new TestRuntime(this.getClass().getName() + " " + filename, filename, keysCounted,
        getRuntime(numberRuns), getRuntime(1));
  }

}
