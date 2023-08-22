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
package org.apache.accumulo.custom.file.rfile;

import java.io.IOException;

import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.BaseLocalityGroupReader;
import org.apache.accumulo.core.file.rfile.LocalityGroupMetadata;
import org.apache.accumulo.core.file.rfile.RFileReader;
import org.apache.accumulo.core.file.rfile.VisibilityPushdownFiltering;
import org.apache.accumulo.file.rfile.predicate.KeyPredicate;

public class SequentialRFileReader extends RFileReader implements VisibilityPushdownFiltering {

  String auths = null;

  KeyPredicate keyPredicate = null;

  public SequentialRFileReader(CachableBlockFile.Reader rdr) throws IOException {
    super(rdr);
  }

  protected SequentialRFileReader(SequentialRFileReader r,
      BaseLocalityGroupReader<PushdownRelativeKey>[] sampleReaders) {
    super(r, sampleReaders);
  }

  protected SequentialRFileReader(SequentialRFileReader r, boolean useSample) {
    super(r, useSample);
  }

  public SequentialRFileReader(CachableBlockFile.CachableBuilder b) throws IOException {
    this(new CachableBlockFile.Reader(b));
  }

  public SequentialRFileReader(CachableBlockFile.CachableBuilder cb, String auths,
      KeyPredicate keyPredicate) throws IOException {
    super(new CachableBlockFile.Reader(cb), false);
    this.auths = auths;
    this.keyPredicate = keyPredicate;
    init();
  }

  void setAuths(String auths) {
    this.auths = auths;
  }

  @Override
  protected BaseLocalityGroupReader<?> getReaderInstance(CachableBlockFile.Reader reader,
      LocalityGroupMetadata metadata, int ver) {
    return new FilteredLocalityGroupReader(reader, metadata, ver, auths)
        .withKeyPredicate(keyPredicate);
  }

  @Override
  protected BaseLocalityGroupReader<?>[] createArray(int size) {
    return new FilteredLocalityGroupReader[size];
  }

  @SuppressWarnings("unchecked")
  @Override
  protected RFileReader createCopy() {
    SequentialRFileReader copy = new SequentialRFileReader(this,
        (BaseLocalityGroupReader<PushdownRelativeKey>[]) sampleReaders);
    copy.setInterruptFlagInternal(interruptFlag);
    return copy;
  }

  @Override
  public boolean pushdownFilter() {
    return true;
  }
}
