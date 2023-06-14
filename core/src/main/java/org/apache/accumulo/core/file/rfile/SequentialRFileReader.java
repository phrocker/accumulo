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
package org.apache.accumulo.core.file.rfile;

import java.io.IOException;

import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;

public class SequentialRFileReader extends RFileReader {

  public SequentialRFileReader(CachableBlockFile.Reader rdr) throws IOException {
    super(rdr);
  }

  protected SequentialRFileReader(SequentialRFileReader r, LocalityGroupReader[] sampleReaders) {
    super(r, sampleReaders);
  }

  protected SequentialRFileReader(SequentialRFileReader r, boolean useSample) {
    super(r, useSample);
  }

  public SequentialRFileReader(CachableBlockFile.CachableBuilder b) throws IOException {
    this(new CachableBlockFile.Reader(b));
  }

  @Override
  protected LocalityGroupReader getReaderInstance(CachableBlockFile.Reader reader,
      LocalityGroupMetadata metadata, int ver) {
    return new ReadAheadLocalityGroupReader(reader, metadata, ver);
  }

}
