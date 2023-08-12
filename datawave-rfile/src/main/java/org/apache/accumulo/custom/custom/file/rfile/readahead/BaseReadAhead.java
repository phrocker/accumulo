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
package org.apache.accumulo.custom.custom.file.rfile.readahead;

import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.file.rfile.MultiLevelIndex;

public abstract class BaseReadAhead implements ReadAheadRequestor {

  protected MultiLevelIndex.Reader.IndexIterator iiter;

  public BaseReadAhead(final MultiLevelIndex.Reader.IndexIterator iiter) {
    this.iiter = iiter;
  }

  @Override
  public BlockedRheadAhead getNextBlock() throws ExecutionException, InterruptedException {
    return null;
  }

  public void setIterator(final MultiLevelIndex.Reader.IndexIterator iiter) {
    this.iiter = iiter;
  }

  @Override
  public abstract boolean hasNextRead();

  @Override
  public BlockedRheadAhead peek() throws ExecutionException, InterruptedException {
    return null;
  }

  @Override
  public void checkpoint(long entriesRemaining) {

  }

  @Override
  public void reset() {

  }
}
