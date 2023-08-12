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
package org.apache.accumulo.custom.file.rfile.readahead;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.accumulo.core.file.rfile.MultiLevelIndex;

public class SingleReadAhead extends BaseReadAhead {

  private final org.apache.accumulo.custom.file.rfile.readahead.BlockSupplier dataRetrieval;
  org.apache.accumulo.custom.file.rfile.readahead.BlockReadAhead readAhead;
  Future<BlockedRheadAhead> nextRead = null;

  public SingleReadAhead(BlockReadAhead readAhead,
      org.apache.accumulo.custom.file.rfile.readahead.BlockSupplier dataRetrieval) {
    super(null);
    this.dataRetrieval = dataRetrieval;
    this.readAhead = readAhead;
    this.nextRead = null;
  }

  @Override
  public BlockedRheadAhead getNextBlock() throws ExecutionException, InterruptedException {
    var block = nextRead == null ? null : nextRead.get();
    if (iiter.hasNext()) {
      var nxt = iiter.next();
      nextRead = initiateReadAhead(nxt, dataRetrieval);
      if (null == block) {
        return getNextBlock();
      }
    } else {
      nextRead = null;
    }

    return block;
  }

  @Override
  public boolean hasNextRead() {
    return null != nextRead || (null != iiter && iiter.hasNext());
  }

  @Override
  public void checkpoint(long entriesRemaining) {
    // nextRead = readAhead.submitReadAheadRequest(onRun);
  }

  private Future<BlockedRheadAhead> initiateReadAhead(MultiLevelIndex.IndexEntry indexEntry,
      BlockSupplier dataRetrieval) {
    return readAhead.submitReadAheadRequest(() -> {
      BlockedRheadAhead readAhead = new BlockedRheadAhead();
      readAhead.entriesLeft = indexEntry.getNumEntries();
      readAhead.currBlock = dataRetrieval.get(indexEntry);
      readAhead.numEntries = indexEntry.getNumEntries();
      if (readAhead.numEntries < 50000) {
        readAhead.threshHold = 0;
      } else {
        readAhead.threshHold = (int) (Math.ceil(readAhead.numEntries * .80));
      }
      readAhead.topKey = indexEntry.getKey();
      return readAhead;
    });
  }
}
