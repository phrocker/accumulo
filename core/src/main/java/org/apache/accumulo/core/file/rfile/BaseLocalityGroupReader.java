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

import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_3;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_4;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.LocalityGroupIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseLocalityGroupReader<R extends RelativeKey>
    extends LocalityGroupIterator.LocalityGroup implements FileSKVIterator {

  private static final Logger log = LoggerFactory.getLogger(BaseLocalityGroupReader.class);
  protected CachableBlockFile.Reader reader;
  protected MultiLevelIndex.Reader index;
  protected int blockCount;
  protected Key firstKey;
  protected int startBlock;
  private boolean closed = false;
  protected int version;
  boolean checkRange = true;

  protected int numEntries;

  protected MultiLevelIndex.Reader.IndexIterator iiter;
  protected int entriesLeft;
  protected CachableBlockFile.CachedBlockRead currBlock;
  protected R rk;
  protected Value val;
  protected Key prevKey = null;
  protected Range range = null;
  protected boolean hasTop = false;
  protected AtomicBoolean interruptFlag;

  public BaseLocalityGroupReader(CachableBlockFile.Reader reader, LocalityGroupMetadata lgm,
      int version) {
    super(lgm.columnFamilies, lgm.isDefaultLG);
    this.firstKey = lgm.firstKey;
    this.index = lgm.indexReader;
    this.startBlock = lgm.startBlock;
    blockCount = index.size();
    this.version = version;

    this.reader = reader;

  }

  public BaseLocalityGroupReader(BaseLocalityGroupReader<?> lgr) {
    super(lgr.columnFamilies, lgr.isDefaultLocalityGroup);
    this.firstKey = lgr.firstKey;
    this.index = lgr.index;
    this.startBlock = lgr.startBlock;
    this.blockCount = lgr.blockCount;
    this.reader = lgr.reader;
    this.version = lgr.version;
  }

  Iterator<MultiLevelIndex.IndexEntry> getIndex() throws IOException {
    return index.lookup(new Key());
  }

  @Override
  public void close() throws IOException {
    closed = true;
    hasTop = false;
    if (currBlock != null) {
      currBlock.close();
    }

  }

  @Override
  public Key getTopKey() {
    return rk.getKey();
  }

  @Override
  public Value getTopValue() {
    return val;
  }

  @Override
  public boolean hasTop() {
    return hasTop;
  }

  @Override
  public void next() throws IOException {
    try {
      _next();
    } catch (IOException ioe) {
      reset();
      throw ioe;
    }
  }

  protected void _next() throws IOException {

    if (!hasTop) {
      throw new IllegalStateException();
    }

    if (entriesLeft == 0) {
      currBlock.close();
      if (metricsGatherer != null) {
        metricsGatherer.startBlock();
      }

      if (iiter.hasNext()) {
        MultiLevelIndex.IndexEntry indexEntry = iiter.next();
        entriesLeft = indexEntry.getNumEntries();
        numEntries = indexEntry.getNumEntries();
        currBlock = getDataBlock(indexEntry);

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
  }

  protected CachableBlockFile.CachedBlockRead getDataBlock(MultiLevelIndex.IndexEntry indexEntry)
      throws IOException {
    if (interruptFlag != null && interruptFlag.get()) {
      throw new IterationInterruptedException();
    }

    if (version == RINDEX_VER_3 || version == RINDEX_VER_4) {
      return reader.getDataBlock(startBlock + iiter.previousIndex());
    } else {
      return reader.getDataBlock(indexEntry.getOffset(), indexEntry.getCompressedSize(),
          indexEntry.getRawSize());
    }

  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    if (closed) {
      throw new IllegalStateException("Locality group reader closed");
    }

    if (!columnFamilies.isEmpty() || inclusive) {
      throw new IllegalArgumentException("I do not know how to filter column families");
    }

    if (interruptFlag != null && interruptFlag.get()) {
      throw new IterationInterruptedException();
    }

    try {
      _seek(range);
    } catch (IOException ioe) {
      reset();
      throw ioe;
    }
  }

  protected void reset() {
    rk = null;
    hasTop = false;
    if (currBlock != null) {
      try {
        try {
          currBlock.close();
        } catch (IOException e) {
          log.warn("Failed to close block reader", e);
        }
      } finally {
        currBlock = null;
      }
    }
  }

  protected void configureRelativeKey(RelativeKey key) {

  }

  protected abstract SkippedRelativeKey<R> fastSkip(DataInput in, Key seekKey,
      MutableByteSequence value, Key prevKey, Key currKey, int entriesLeft) throws IOException;

  protected void _seek(Range range) throws IOException {

    this.range = range;
    this.checkRange = true;

    if (blockCount == 0) {
      // it's an empty file
      rk = null;
      return;
    }

    Key startKey = range.getStartKey();
    if (startKey == null) {
      startKey = new Key();
    }

    boolean reseek = true;

    if (range.afterEndKey(firstKey)) {
      // range is before first key in rfile, so there is nothing to do
      reset();
      reseek = false;
    }

    if (rk != null) {
      if (null == prevKey) {
        System.out.println("ohhh");
      }
      if (range.beforeStartKey(prevKey) && range.afterEndKey(getTopKey())) {
        // range is between the two keys in the file where the last range seeked to stopped, so
        // there is
        // nothing to do
        reseek = false;
      }

      if (startKey.compareTo(getTopKey()) <= 0 && startKey.compareTo(prevKey) > 0) {
        // current location in file can satisfy this request, no need to seek
        reseek = false;
      }

      if (entriesLeft > 0 && startKey.compareTo(getTopKey()) >= 0
          && startKey.compareTo(iiter.peekPrevious().getKey()) <= 0) {
        // start key is within the unconsumed portion of the current block

        // this code intentionally does not use the index associated with a cached block
        // because if only forward seeks are being done, then there is no benefit to building
        // and index for the block... could consider using the index if it exist but not
        // causing the build of an index... doing this could slow down some use cases and
        // and speed up others.

        MutableByteSequence valbs = new MutableByteSequence(new byte[64], 0, 0);
        SkippedRelativeKey<R> skippr =
            fastSkip(currBlock, startKey, valbs, prevKey, getTopKey(), entriesLeft);
        if (skippr.skipped > 0) {
          entriesLeft -= skippr.skipped;
          val = new Value(valbs.toArray());
          prevKey = skippr.prevKey;
          rk = skippr.rk;
          configureRelativeKey(rk);
        }

        reseek = false;
      }

      if (entriesLeft == 0 && startKey.compareTo(getTopKey()) > 0
          && startKey.compareTo(iiter.peekPrevious().getKey()) <= 0) {
        // In the empty space at the end of a block. This can occur when keys are shortened in the
        // index creating index entries that do not exist in the
        // block. These shortened index entries fall between the last key in a block and first key
        // in the next block, but may not exist in the data.
        // Just proceed to the next block.
        reseek = false;
      }

      if (iiter.previousIndex() == 0 && getTopKey().equals(firstKey)
          && startKey.compareTo(firstKey) <= 0) {
        // seeking before the beginning of the file, and already positioned at the first key in
        // the file
        // so there is nothing to do
        reseek = false;
      }
    }

    if (reseek) {

      iiter = index.lookup(startKey);
      reset();

      if (iiter.hasNext()) {

        // if the index contains the same key multiple times, then go to the
        // earliest index entry containing the key
        while (iiter.hasPrevious() && iiter.peekPrevious().getKey().equals(iiter.peek().getKey())) {
          iiter.previous();
        }

        if (iiter.hasPrevious()) {
          prevKey = new Key(iiter.peekPrevious().getKey()); // initially prevKey is the last key
        } else {
          // of the prev block
          prevKey = new Key(); // first block in the file, so set prev key to minimal key
        }

        MultiLevelIndex.IndexEntry indexEntry = iiter.next();
        entriesLeft = indexEntry.getNumEntries();
        currBlock = getDataBlock(indexEntry);

        checkRange = range.afterEndKey(indexEntry.getKey());
        if (!checkRange) {
          hasTop = true;
        }

        MutableByteSequence valbs = new MutableByteSequence(new byte[64], 0, 0);

        Key currKey = null;

        if (currBlock.isIndexable()) {
          BlockIndex blockIndex = BlockIndex.getIndex(currBlock, indexEntry);
          if (blockIndex != null) {
            BlockIndex.BlockIndexEntry bie = blockIndex.seekBlock(startKey, currBlock);
            if (bie != null) {
              // we are seeked to the current position of the key in the index
              // need to prime the read process and read this key from the block
              RelativeKey tmpRk = new RelativeKey();
              tmpRk.setPrevKey(bie.getPrevKey());
              tmpRk.readFields(currBlock);
              val = new Value();

              val.readFields(currBlock);
              valbs = new MutableByteSequence(val.get(), 0, val.getSize());

              // just consumed one key from the input stream, so subtract one from entries left
              entriesLeft = bie.getEntriesLeft() - 1;
              prevKey = new Key(bie.getPrevKey());
              currKey = tmpRk.getKey();
            }
          }
        }

        SkippedRelativeKey<R> skippr =
            fastSkip(currBlock, startKey, valbs, prevKey, currKey, entriesLeft);

        prevKey = skippr.prevKey;
        entriesLeft -= skippr.skipped;
        val = new Value(valbs.toArray());
        // set rk when everything above is successful, if exception
        // occurs rk will not be set
        rk = skippr.rk;
        configureRelativeKey(rk);
      } else {
        // past the last key
      }
    }

    hasTop = rk != null && !range.afterEndKey(rk.getKey());

    resetInternals();
    while (hasTop() && range.beforeStartKey(getTopKey())) {
      next();
    }

    if (metricsGatherer != null) {
      metricsGatherer.startLocalityGroup(rk.getKey().getColumnFamily());
      metricsGatherer.addMetric(rk.getKey(), val);
    }
  }

  protected void resetInternals() {}

  @Override
  public Key getFirstKey() {
    return firstKey;
  }

  @Override
  public Key getLastKey() {
    if (index.size() == 0) {
      return null;
    }
    return index.getLastKey();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeDeepCopies() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataInputStream getMetaStore(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
  }

  @Override
  public InterruptibleIterator getIterator() {
    return this;
  }

  MetricsGatherer<?> metricsGatherer;

  public void registerMetrics(MetricsGatherer<?> vmg) {
    metricsGatherer = vmg;
  }

  @Override
  public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCacheProvider(CacheProvider cacheProvider) {
    throw new UnsupportedOperationException();
  }
}
