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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadAheadLocalityGroupReader extends LocalityGroupReader {

  static BlockReadAhead readAheadExecutor = new BlockReadAhead(25);
  Future<BlockReadAhead.BlockedRheadAhead> readAhead;
  private static final Logger log = LoggerFactory.getLogger(ReadAheadLocalityGroupReader.class);
  private double threshhold;

  public ReadAheadLocalityGroupReader(CachableBlockFile.Reader reader, LocalityGroupMetadata lgm,
      int version) {
    super(reader, lgm, version);
  }

  Future<BlockReadAhead.BlockedRheadAhead>
      initiateReadAhead(MultiLevelIndex.IndexEntry indexEntry) {
    return readAheadExecutor.submitReadAheadRequest(() -> {
      try {
        BlockReadAhead.BlockedRheadAhead readAhead = new BlockReadAhead.BlockedRheadAhead();
        readAhead.entriesLeft = indexEntry.getNumEntries();
        readAhead.currBlock = super.getDataBlock(indexEntry);
        readAhead.numEntries = indexEntry.getNumEntries();
        readAhead.threshhold = (int) (Math.ceil(numEntries * .10));
        readAhead.topKey = indexEntry.getKey();
        return readAhead;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  protected void _next() throws IOException {

    if (!hasTop) {
      throw new IllegalStateException();
    }

    if (entriesLeft == 0) {
      currBlock.close();
      if (metricsGatherer != null) {
        metricsGatherer.startBlock();
      }
      if (readAhead != null) {
        BlockReadAhead.BlockedRheadAhead readAheadResult = null;
        try {
          readAheadResult = readAhead.get();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
        entriesLeft = readAheadResult.entriesLeft;
        currBlock = readAheadResult.currBlock;
        numEntries = readAheadResult.numEntries;
        threshhold = readAheadResult.threshhold;
        checkRange = range.afterEndKey(readAheadResult.topKey);
        readAhead = null;
      } else if (iiter.hasNext()) {
        MultiLevelIndex.IndexEntry indexEntry = iiter.next();
        entriesLeft = indexEntry.getNumEntries();
        currBlock = getDataBlock(indexEntry);
        numEntries = indexEntry.getNumEntries();
        // if we have arbitrarily small number of entries don't
        // do the work
        if (numEntries < 1000) {
          threshhold = 0;
        } else {
          threshhold = numEntries * .10;
        }

        checkRange = range.afterEndKey(indexEntry.getKey());
        if (!checkRange) {
          hasTop = true;
        }

      } else {
        rk = null;
        val = null;
        hasTop = false;
        return;
      }
    }

    prevKey = rk.getKey();
    rk.readFields(currBlock);
    val.readFields(currBlock);

    if (metricsGatherer != null) {
      metricsGatherer.addMetric(rk.getKey(), val);
    }

    entriesLeft--;
    if (checkRange) {
      hasTop = !range.afterEndKey(rk.getKey());
    }
    if (hasTop && threshhold > 0 && entriesLeft <= threshhold) {
      if (iiter.hasNext()) {
        MultiLevelIndex.IndexEntry indexEntry = iiter.next();
        readAhead = initiateReadAhead(indexEntry);
      }
    }
  }
}
