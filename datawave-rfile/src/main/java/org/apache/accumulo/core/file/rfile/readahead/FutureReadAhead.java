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
package org.apache.accumulo.core.file.rfile.readahead;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.core.data.Key;

public class FutureReadAhead implements Future<BlockedRheadAhead> {

  private final Future<BlockedRheadAhead> delegate;
  private Key nextKey = null;

  public FutureReadAhead(Future<BlockedRheadAhead> delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean cancel(boolean b) {
    return delegate.cancel(b);
  }

  @Override
  public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override
  public boolean isDone() {
    return delegate.isDone();
  }

  public void setNextKey(Key nextKey) {
    this.nextKey = nextKey;
  }

  public Key getNextKey() {
    return this.nextKey;
  }

  @Override
  public BlockedRheadAhead get() throws InterruptedException, ExecutionException {
    var rt = delegate.get();
    rt.nextBlockKey = nextKey;
    return rt;
  }

  @Override
  public BlockedRheadAhead get(long l, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.get(l, timeUnit);
  }
}
