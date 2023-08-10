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

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.file.rfile.predicate.KeyPredicate;
import org.apache.accumulo.core.file.rfile.predicate.RowPredicate;

public class TestSequentialRFile extends RFileTestRun {

  private RowPredicate rowPredicate;

  private KeyPredicate keyPredicate;

  protected TestSequentialRFile(TestRFile rfileTest, String filename) {
    super(rfileTest, filename);
  }

  @Override
  public void configureIterators() throws IOException {

    topIter = rfileTest.reader;
  }

  @Override
  public void configurebaseLayer() throws IOException {
    rfileTest.openSequentialReader(file, file.length(), false,
        auths != null ? auths.iterator().next() : null, keyPredicate);
  }

  public static class Builder {

    private Set<String> auths;
    private TestRFile rfileTest;
    private String filename;
    private KeyPredicate keyPredicate;

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

    public TestSequentialRFile build() {
      var tr = new TestSequentialRFile(rfileTest, filename);
      tr.setAuths(auths);
      tr.setKeyPredicate(keyPredicate);
      return tr;
    }

    public Builder withKeyPredicate(KeyPredicate keyPredicate) {
      this.keyPredicate = keyPredicate;
      return this;
    }

  }

  private void setRowPredicate(RowPredicate rowPredicate) {
    this.rowPredicate = rowPredicate;
  }

  private void setKeyPredicate(KeyPredicate keyPredicate) {
    this.keyPredicate = keyPredicate;
  }

}
