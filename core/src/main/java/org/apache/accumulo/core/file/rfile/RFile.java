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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Writer.BlockAppender;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class RFile {

  public static final String EXTENSION = "rf";

  private static final Logger log = LoggerFactory.getLogger(RFile.class);

  private RFile() {}

  static final int RINDEX_MAGIC = 0x20637474;

  static final int RINDEX_VER_8 = 8; // Added sample storage. There is a sample locality group for
                                     // each locality group. Sample are built using a Sampler and
                                     // sampler configuration. The Sampler and its configuration are
                                     // stored in RFile. Persisting the method of producing the
                                     // sample allows a user of RFile to determine if the sample is
                                     // useful.
                                     //
                                     // Selected smaller keys for index by doing two things. First
                                     // internal stats were used to look for keys that were below
                                     // average in size for the index. Also keys that were
                                     // statistically large were excluded from the index. Second
                                     // shorter keys
                                     // (that may not exist in data) were generated for the index.
  static final int RINDEX_VER_7 = 7; // Added support for prefix encoding and encryption. Before
                                     // this change only exact matches within a key field were
                                     // deduped
                                     // for consecutive keys. After this change, if consecutive key
                                     // fields have the same prefix then the prefix is only stored
                                     // once.
  static final int RINDEX_VER_6 = 6; // Added support for multilevel indexes. Before this the index
                                     // was one list with an entry for each data block. For large
                                     // files, a large index needed to be read into memory before
                                     // any seek could be done. After this change the index is a fat
                                     // tree, and opening a large rfile is much faster. Like the
                                     // previous version of Rfile, each index node in the tree is
                                     // kept
                                     // in memory serialized and used in its serialized form.
  // static final int RINDEX_VER_5 = 5; // unreleased
  static final int RINDEX_VER_4 = 4; // Added support for seeking using serialized indexes. After
                                     // this change index is no longer deserialized when rfile
                                     // opened.
                                     // Entire serialized index is read into memory as single byte
                                     // array. For seeks, serialized index is used to find blocks
                                     // (the binary search deserializes the specific entries its
                                     // needs). This resulted in less memory usage (no object
                                     // overhead)
                                     // and faster open times for RFiles.
  static final int RINDEX_VER_3 = 3; // Initial released version of RFile. R is for relative
                                     // encoding. A keys is encoded relative to the previous key.
                                     // The
                                     // initial version deduped key fields that were the same for
                                     // consecutive keys. For sorted data this is a common
                                     // occurrence.
                                     // This version supports locality groups. Each locality group
                                     // has an index pointing to set of data blocks. Each data block
                                     // contains relatively encoded keys and values.

  // Buffer sample data so that many sample data blocks are stored contiguously.
  private static int sampleBufferSize = 10000000;

  @VisibleForTesting
  public static void setSampleBufferSize(int bufferSize) {
    sampleBufferSize = bufferSize;
  }

  private static class SampleEntry {
    Key key;
    Value val;

    SampleEntry(Key key, Value val) {
      this.key = new Key(key);
      this.val = new Value(val);
    }
  }

  private static class SampleLocalityGroupWriter {

    private Sampler sampler;

    private List<SampleEntry> entries = new ArrayList<>();
    private long dataSize = 0;

    private LocalityGroupWriter lgr;

    public SampleLocalityGroupWriter(LocalityGroupWriter lgr, Sampler sampler) {
      this.lgr = lgr;
      this.sampler = sampler;
    }

    public void append(Key key, Value value) {
      if (sampler.accept(key)) {
        entries.add(new SampleEntry(key, value));
        dataSize += key.getSize() + value.getSize();
      }
    }

    public void close() throws IOException {
      for (SampleEntry se : entries) {
        lgr.append(se.key, se.val);
      }

      lgr.close();
    }

    public void flushIfNeeded() throws IOException {
      if (dataSize > sampleBufferSize) {
        // the reason to write out all but one key is so that closeBlock() can always eventually be
        // called with true
        List<SampleEntry> subList = entries.subList(0, entries.size() - 1);

        if (!subList.isEmpty()) {
          for (SampleEntry se : subList) {
            lgr.append(se.key, se.val);
          }

          lgr.closeBlock(subList.get(subList.size() - 1).key, false);

          subList.clear();
          dataSize = 0;
        }
      }
    }
  }

  private static class LocalityGroupWriter {

    private BCFile.Writer fileWriter;
    private BlockAppender blockWriter;

    private final long blockSize;
    private final long maxBlockSize;
    private int entries = 0;

    private LocalityGroupMetadata currentLocalityGroup = null;

    private Key lastKeyInBlock = null;

    private Key prevKey = new Key();

    private SampleLocalityGroupWriter sample;

    // Use windowed stats to fix ACCUMULO-4669
    private RollingStats keyLenStats = new RollingStats(2017);
    private double averageKeySize = 0;

    LocalityGroupWriter(BCFile.Writer fileWriter, long blockSize, long maxBlockSize,
        LocalityGroupMetadata currentLocalityGroup, SampleLocalityGroupWriter sample) {
      this.fileWriter = fileWriter;
      this.blockSize = blockSize;
      this.maxBlockSize = maxBlockSize;
      this.currentLocalityGroup = currentLocalityGroup;
      this.sample = sample;
    }

    private boolean isGiantKey(Key k) {
      double mean = keyLenStats.getMean();
      double stddev = keyLenStats.getStandardDeviation();
      return k.getSize() > mean + Math.max(9 * mean, 4 * stddev);
    }

    public void append(Key key, Value value) throws IOException {

      if (key.compareTo(prevKey) < 0) {
        throw new IllegalArgumentException(
            "Keys appended out-of-order.  New key " + key + ", previous key " + prevKey);
      }

      currentLocalityGroup.updateColumnCount(key);

      if (currentLocalityGroup.getFirstKey() == null) {
        currentLocalityGroup.setFirstKey(key);
      }

      if (sample != null) {
        sample.append(key, value);
      }

      if (blockWriter == null) {
        blockWriter = fileWriter.prepareDataBlock();
      } else if (blockWriter.getRawSize() > blockSize) {

        // Look for a key that's short to put in the index, defining short as average or below.
        if (averageKeySize == 0) {
          // use the same average for the search for a below average key for a block
          averageKeySize = keyLenStats.getMean();
        }

        // Possibly produce a shorter key that does not exist in data. Even if a key can be
        // shortened, it may not be below average.
        Key closeKey = KeyShortener.shorten(prevKey, key);

        if ((closeKey.getSize() <= averageKeySize || blockWriter.getRawSize() > maxBlockSize)
            && !isGiantKey(closeKey)) {
          closeBlock(closeKey, false);
          blockWriter = fileWriter.prepareDataBlock();
          // set average to zero so its recomputed for the next block
          averageKeySize = 0;
          // To constrain the growth of data blocks, we limit our worst case scenarios to closing
          // blocks if they reach the maximum configurable block size of Integer.MAX_VALUE.
          // 128 bytes added for metadata overhead
        } else if (((long) key.getSize() + (long) value.getSize() + blockWriter.getRawSize() + 128L)
            >= Integer.MAX_VALUE) {
          closeBlock(closeKey, false);
          blockWriter = fileWriter.prepareDataBlock();
          averageKeySize = 0;

        }
      }

      RelativeKey rk = new RelativeKey(lastKeyInBlock, key);

      rk.write(blockWriter);
      value.write(blockWriter);
      entries++;

      keyLenStats.addValue(key.getSize());

      prevKey = new Key(key);
      lastKeyInBlock = prevKey;

    }

    private void closeBlock(Key key, boolean lastBlock) throws IOException {
      blockWriter.close();

      if (lastBlock) {
        currentLocalityGroup.indexWriter.addLast(key, entries, blockWriter.getStartPos(),
            blockWriter.getCompressedSize(), blockWriter.getRawSize());
      } else {
        currentLocalityGroup.indexWriter.add(key, entries, blockWriter.getStartPos(),
            blockWriter.getCompressedSize(), blockWriter.getRawSize());
      }

      if (sample != null) {
        sample.flushIfNeeded();
      }

      blockWriter = null;
      lastKeyInBlock = null;
      entries = 0;
    }

    public void close() throws IOException {
      if (blockWriter != null) {
        closeBlock(lastKeyInBlock, true);
      }

      if (sample != null) {
        sample.close();
      }
    }
  }

  public static class Writer implements FileSKVWriter {

    public static final int MAX_CF_IN_DLG = 1000;
    private static final double MAX_BLOCK_MULTIPLIER = 1.1;

    private BCFile.Writer fileWriter;

    private final long blockSize;
    private final long maxBlockSize;
    private final int indexBlockSize;

    private ArrayList<LocalityGroupMetadata> localityGroups = new ArrayList<>();
    private ArrayList<LocalityGroupMetadata> sampleGroups = new ArrayList<>();
    private LocalityGroupMetadata currentLocalityGroup = null;
    private LocalityGroupMetadata sampleLocalityGroup = null;

    private boolean dataClosed = false;
    private boolean closed = false;
    private boolean startedDefaultLocalityGroup = false;

    private HashSet<ByteSequence> previousColumnFamilies;
    private long length = -1;

    private LocalityGroupWriter lgWriter;

    private SamplerConfigurationImpl samplerConfig;
    private Sampler sampler;

    public Writer(BCFile.Writer bfw, int blockSize) throws IOException {
      this(bfw, blockSize, (int) DefaultConfiguration.getInstance()
          .getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX), null, null);
    }

    public Writer(BCFile.Writer bfw, int blockSize, int indexBlockSize,
        SamplerConfigurationImpl samplerConfig, Sampler sampler) {
      this.blockSize = blockSize;
      this.maxBlockSize = (long) (blockSize * MAX_BLOCK_MULTIPLIER);
      this.indexBlockSize = indexBlockSize;
      this.fileWriter = bfw;
      previousColumnFamilies = new HashSet<>();
      this.samplerConfig = samplerConfig;
      this.sampler = sampler;
    }

    @Override
    public synchronized void close() throws IOException {

      if (closed) {
        return;
      }

      closeData();

      BlockAppender mba = fileWriter.prepareMetaBlock("RFile.index");

      mba.writeInt(RINDEX_MAGIC);
      mba.writeInt(RINDEX_VER_8);

      if (currentLocalityGroup != null) {
        localityGroups.add(currentLocalityGroup);
        sampleGroups.add(sampleLocalityGroup);
      }

      mba.writeInt(localityGroups.size());

      for (LocalityGroupMetadata lc : localityGroups) {
        lc.write(mba);
      }

      if (samplerConfig == null) {
        mba.writeBoolean(false);
      } else {
        mba.writeBoolean(true);

        for (LocalityGroupMetadata lc : sampleGroups) {
          lc.write(mba);
        }

        samplerConfig.write(mba);
      }

      mba.close();
      fileWriter.close();
      length = fileWriter.getLength();

      closed = true;
    }

    private void closeData() throws IOException {

      if (dataClosed) {
        return;
      }

      dataClosed = true;

      if (lgWriter != null) {
        lgWriter.close();
      }
    }

    @Override
    public void append(Key key, Value value) throws IOException {

      if (dataClosed) {
        throw new IllegalStateException("Cannot append, data closed");
      }

      lgWriter.append(key, value);
    }

    @Override
    public DataOutputStream createMetaStore(String name) throws IOException {
      closeData();

      return fileWriter.prepareMetaBlock(name);
    }

    private void _startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies)
        throws IOException {
      if (dataClosed) {
        throw new IllegalStateException("data closed");
      }

      if (startedDefaultLocalityGroup) {
        throw new IllegalStateException(
            "Can not start anymore new locality groups after default locality group started");
      }

      if (lgWriter != null) {
        lgWriter.close();
      }

      if (currentLocalityGroup != null) {
        localityGroups.add(currentLocalityGroup);
        sampleGroups.add(sampleLocalityGroup);
      }

      if (columnFamilies == null) {
        startedDefaultLocalityGroup = true;
        currentLocalityGroup =
            new LocalityGroupMetadata(previousColumnFamilies, indexBlockSize, fileWriter);
        sampleLocalityGroup =
            new LocalityGroupMetadata(previousColumnFamilies, indexBlockSize, fileWriter);
      } else {
        if (!Collections.disjoint(columnFamilies, previousColumnFamilies)) {
          HashSet<ByteSequence> overlap = new HashSet<>(columnFamilies);
          overlap.retainAll(previousColumnFamilies);
          throw new IllegalArgumentException(
              "Column families over lap with previous locality group : " + overlap);
        }
        currentLocalityGroup =
            new LocalityGroupMetadata(name, columnFamilies, indexBlockSize, fileWriter);
        sampleLocalityGroup =
            new LocalityGroupMetadata(name, columnFamilies, indexBlockSize, fileWriter);
        previousColumnFamilies.addAll(columnFamilies);
      }

      SampleLocalityGroupWriter sampleWriter = null;
      if (sampler != null) {
        sampleWriter = new SampleLocalityGroupWriter(
            new LocalityGroupWriter(fileWriter, blockSize, maxBlockSize, sampleLocalityGroup, null),
            sampler);
      }
      lgWriter = new LocalityGroupWriter(fileWriter, blockSize, maxBlockSize, currentLocalityGroup,
          sampleWriter);
    }

    @Override
    public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies)
        throws IOException {
      if (columnFamilies == null) {
        throw new NullPointerException();
      }

      _startNewLocalityGroup(name, columnFamilies);
    }

    @Override
    public void startDefaultLocalityGroup() throws IOException {
      _startNewLocalityGroup(null, null);
    }

    @Override
    public boolean supportsLocalityGroups() {
      return true;
    }

    @Override
    public long getLength() {
      if (!closed) {
        return fileWriter.getLength();
      }
      return length;
    }
  }
}
