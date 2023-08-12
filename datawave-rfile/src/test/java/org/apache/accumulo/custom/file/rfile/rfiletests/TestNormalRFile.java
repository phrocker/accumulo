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
package org.apache.accumulo.custom.file.rfile.rfiletests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.custom.file.rfile.uids.proto.GlobalIndexMatchingFilter;
import org.apache.accumulo.custom.file.rfile.uids.proto.GlobalIndexTermMatchingIterator;

public class TestNormalRFile extends RFileTestRun {

  protected TestNormalRFile(TestRFile rfileTest, String filename) {
    super(rfileTest, filename);
  }

  @Override
  public void configureIterators() throws IOException {
    topIter = VisibilityFilter.wrap(rfileTest.reader, new Authorizations(auths.iterator().next()),
        "".getBytes());
    var gi = new GlobalIndexTermMatchingIterator();
    Map<String,String> options = new HashMap<String,String>();
    options.put(GlobalIndexMatchingFilter.PATTERN + "1", "c.*");
    options.put(GlobalIndexTermMatchingIterator.UNIQUE_TERMS_IN_FIELD, "true");
    gi.init(topIter, options, null);
    topIter = gi;
  }

  @Override
  public void configurebaseLayer() throws IOException {
    rfileTest.openNormalReader(file, file.length(), false);
  }

  public static class Builder {

    private Set<String> auths;
    private TestRFile rfileTest;
    private String filename;

    private Builder() {

    }

    public static Builder newBuilder(TestRFile rfileTest, String filename) {
      Builder b = new Builder();
      b.rfileTest = rfileTest;
      b.filename = filename;
      return b;
    }

    public Builder withAuths(Set<String> auths) {
      this.auths = auths;
      return this;
    }

    public TestNormalRFile build() {
      var tr = new TestNormalRFile(rfileTest, filename);
      tr.setAuths(auths);
      return tr;
    }
  }

}
