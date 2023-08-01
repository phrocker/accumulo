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

import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.predicate.ColumnFamilyPredicate;
import org.apache.accumulo.core.file.rfile.predicate.KeyPredicate;
import org.apache.accumulo.core.file.rfile.predicate.RowPredicate;
import org.apache.accumulo.core.file.rfile.readahead.BaseReadAhead;
import org.apache.accumulo.core.file.rfile.readahead.BlockReadAhead;
import org.apache.accumulo.core.file.rfile.readahead.BlockedRheadAhead;
import org.apache.accumulo.core.file.rfile.readahead.MultiReadAhead;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilteredLocalityGroupReader extends BaseLocalityGroupReader<PushdownRelativeKey> {

  BaseReadAhead blockreadAhead;
  BlockedRheadAhead readAheadResult;
  private static final Logger log = LoggerFactory.getLogger(FilteredLocalityGroupReader.class);

  String auths;
  private RowPredicate rowPredicate =null;

  private KeyPredicate keyPredicate=null;

  public FilteredLocalityGroupReader(CachableBlockFile.Reader reader, LocalityGroupMetadata lgm,
      int version, String auths) {
    super(reader, lgm, version);
    blockreadAhead =
        new MultiReadAhead(new BlockReadAhead(10), (MultiLevelIndex.IndexEntry entry) -> {
          try {
            return super.getDataBlock(entry);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, 3);
    this.auths = auths;
  }

  @Override
  public boolean hasTop() {
    return hasTop;
  }

  @Override
  protected void _seek(Range range) throws IOException {
    super._seek(range);
  }

  @Override
  protected void resetInternals() {
    blockreadAhead.setIterator(iiter);
  }



  @Override
  protected void configureRelativeKey(RelativeKey key) {
    ((PushdownRelativeKey) key).setAuths(auths);
    ((PushdownRelativeKey) key).setkeyPredicate(keyPredicate);

  }

  @Override
  protected void _next() throws IOException {
    _f_next();
  }

  @Override
  protected SkippedRelativeKey<PushdownRelativeKey> fastSkip(DataInput in, Key seekKey,
      MutableByteSequence value, Key prevKey, Key currKey, int entriesLeft) throws IOException {
    return PushdownRelativeKey.pushdownFastSkip(auths,in, seekKey, value, prevKey, currKey, entriesLeft, keyPredicate);
  }

  /**
   * Filtered next
   *
   * @throws IOException
   */
  protected void _f_next() throws IOException {

    if (!hasTop) {
      throw new IllegalStateException();
    }
    do {

      if (entriesLeft == 0) {
        currBlock.close();
        if (metricsGatherer != null) {
          metricsGatherer.startBlock();
        }
        if (blockreadAhead.hasNextRead()) {
          try {
            readAheadResult = blockreadAhead.getNextBlock();
          } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          entriesLeft = readAheadResult.entriesLeft;
          currBlock = readAheadResult.currBlock;
          currBlock.seek(0);
          numEntries = readAheadResult.numEntries;
          checkRange = range.afterEndKey(readAheadResult.topKey);
          rk.resetFilters();
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
      if (rk.isLastKeyFiltered()) {
        // was it filtered b/c of the predicate?
        if (null != readAheadResult && readAheadResult.nextBlockKey != null){
          //System.out.println("checking...");
          if (!keyPredicate.accept(readAheadResult.nextBlockKey,0)) {
            System.out.println("yep");
            // we can actually check the next block.
            entriesLeft = 0;
          }
        }
        continue;
      }
      if (checkRange) {
        hasTop = !range.afterEndKey(rk.getKey());

      }
    } while ((rk.isLastKeyFiltered())  && hasTop);
  }

  public FilteredLocalityGroupReader withRowPredicate(RowPredicate predicate) {
    this.rowPredicate =predicate;
    return this;
  }

  public FilteredLocalityGroupReader withKeyPredicate(KeyPredicate keyPredicate) {
    this.keyPredicate =keyPredicate;
    return this;
  }
}
