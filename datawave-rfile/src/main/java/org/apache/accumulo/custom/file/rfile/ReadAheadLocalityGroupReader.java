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
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.LocalityGroupMetadata;
import org.apache.accumulo.core.file.rfile.LocalityGroupReader;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex;
import org.apache.accumulo.custom.custom.file.rfile.readahead.BaseReadAhead;
import org.apache.accumulo.custom.custom.file.rfile.readahead.BlockReadAhead;
import org.apache.accumulo.custom.custom.file.rfile.readahead.BlockedRheadAhead;
import org.apache.accumulo.custom.custom.file.rfile.readahead.MultiReadAhead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadAheadLocalityGroupReader extends LocalityGroupReader {

  BaseReadAhead blockreadAhead;
  BlockedRheadAhead readAheadResult;
  private static final Logger log = LoggerFactory.getLogger(ReadAheadLocalityGroupReader.class);
  private double threshhold;

  public ReadAheadLocalityGroupReader(CachableBlockFile.Reader reader, LocalityGroupMetadata lgm,
      int version) {
    super(reader, lgm, version);
    blockreadAhead =
        new MultiReadAhead(new BlockReadAhead(10), (MultiLevelIndex.IndexEntry entry) -> {
          try {
            return super.getDataBlock(entry);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, 3);
  }

  @Override
  protected void _seek(Range range) throws IOException {
    super._seek(range);
    blockreadAhead.setIterator(iiter);
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
      if (blockreadAhead.hasNextRead()) {
        try {
          readAheadResult = blockreadAhead.getNextBlock();
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        entriesLeft = readAheadResult.entriesLeft;
        currBlock = readAheadResult.currBlock;
        numEntries = readAheadResult.numEntries;
        threshhold = readAheadResult.threshHold;
        checkRange = range.afterEndKey(readAheadResult.topKey);
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
  }

  /**
   * Filtered next
   *
   * @throws IOException i/o exception during read
   */
  protected void _f_next() throws IOException {

    if (!hasTop) {
      throw new IllegalStateException();
    }

    if (entriesLeft == 0) {
      currBlock.close();
      if (metricsGatherer != null) {
        metricsGatherer.startBlock();
      }
      if (blockreadAhead.hasNextRead()) {
        try {
          readAheadResult = blockreadAhead.getNextBlock();
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        entriesLeft = readAheadResult.entriesLeft;
        currBlock = readAheadResult.currBlock;
        numEntries = readAheadResult.numEntries;
        threshhold = readAheadResult.threshHold;
        checkRange = range.afterEndKey(readAheadResult.topKey);
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
  }
}
