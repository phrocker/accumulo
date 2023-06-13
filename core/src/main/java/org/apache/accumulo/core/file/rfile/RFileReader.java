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

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_MAGIC;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_3;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_4;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_6;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_7;
import static org.apache.accumulo.core.file.rfile.RFile.RINDEX_VER_8;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.HeapIterator;
import org.apache.accumulo.core.iteratorsImpl.system.LocalityGroupIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class RFileReader extends HeapIterator implements FileSKVIterator {

  protected static final Logger log = LoggerFactory.getLogger(RFileReader.class);
  protected final CachableBlockFile.Reader reader;

  protected final ArrayList<LocalityGroupMetadata> localityGroups = new ArrayList<>();
  protected final ArrayList<LocalityGroupMetadata> sampleGroups = new ArrayList<>();

  protected final LocalityGroupReader[] currentReaders;
  protected final LocalityGroupReader[] readers;
  protected final LocalityGroupReader[] sampleReaders;
  protected final LocalityGroupIterator.LocalityGroupContext lgContext;
  protected LocalityGroupIterator.LocalityGroupSeekCache lgCache;

  protected List<RFileReader> deepCopies;
  protected boolean deepCopy = false;

  protected AtomicBoolean interruptFlag;

  protected SamplerConfigurationImpl samplerConfig = null;

  protected int rfileVersion;

  public RFileReader(CachableBlockFile.Reader rdr) throws IOException {
    this.reader = rdr;

    try (CachableBlockFile.CachedBlockRead mb = reader.getMetaBlock("RFile.index")) {
      int magic = mb.readInt();
      int ver = mb.readInt();
      rfileVersion = ver;

      if (magic != RINDEX_MAGIC) {
        throw new IOException("Did not see expected magic number, saw " + magic);
      }
      if (ver != RINDEX_VER_8 && ver != RINDEX_VER_7 && ver != RINDEX_VER_6 && ver != RINDEX_VER_4
          && ver != RINDEX_VER_3) {
        throw new IOException("Did not see expected version, saw " + ver);
      }

      int size = mb.readInt();
      currentReaders = new LocalityGroupReader[size];

      deepCopies = new LinkedList<>();

      for (int i = 0; i < size; i++) {
        LocalityGroupMetadata lgm = new LocalityGroupMetadata(ver, rdr);
        lgm.readFields(mb);
        localityGroups.add(lgm);

        currentReaders[i] = getReaderInstance(reader, lgm, ver);
      }

      readers = currentReaders;

      if (ver == RINDEX_VER_8 && mb.readBoolean()) {
        sampleReaders = new LocalityGroupReader[size];

        for (int i = 0; i < size; i++) {
          LocalityGroupMetadata lgm = new LocalityGroupMetadata(ver, rdr);
          lgm.readFields(mb);
          sampleGroups.add(lgm);

          sampleReaders[i] = getReaderInstance(reader, lgm, ver);
        }

        samplerConfig = new SamplerConfigurationImpl(mb);
      } else {
        sampleReaders = null;
        samplerConfig = null;
      }

    }

    lgContext = new LocalityGroupIterator.LocalityGroupContext(currentReaders);

    createHeap(currentReaders.length);
  }

  protected RFileReader(RFileReader r, LocalityGroupReader[] sampleReaders) {
    super(sampleReaders.length);
    this.reader = r.reader;
    this.currentReaders = new LocalityGroupReader[sampleReaders.length];
    this.deepCopies = r.deepCopies;
    this.deepCopy = false;
    this.readers = r.readers;
    this.sampleReaders = r.sampleReaders;
    this.samplerConfig = r.samplerConfig;
    this.rfileVersion = r.rfileVersion;
    for (int i = 0; i < sampleReaders.length; i++) {
      this.currentReaders[i] = sampleReaders[i];
      this.currentReaders[i].setInterruptFlag(r.interruptFlag);
    }
    this.lgContext = new LocalityGroupIterator.LocalityGroupContext(currentReaders);
  }

  protected RFileReader(RFileReader r, boolean useSample) {
    super(r.currentReaders.length);
    this.reader = r.reader;
    this.currentReaders = new LocalityGroupReader[r.currentReaders.length];
    this.deepCopies = r.deepCopies;
    this.deepCopy = true;
    this.samplerConfig = r.samplerConfig;
    this.rfileVersion = r.rfileVersion;
    this.readers = r.readers;
    this.sampleReaders = r.sampleReaders;

    for (int i = 0; i < r.readers.length; i++) {
      if (useSample) {
        this.currentReaders[i] = new LocalityGroupReader(r.sampleReaders[i]);
        this.currentReaders[i].setInterruptFlag(r.interruptFlag);
      } else {
        this.currentReaders[i] = new LocalityGroupReader(r.readers[i]);
        this.currentReaders[i].setInterruptFlag(r.interruptFlag);
      }

    }
    this.lgContext = new LocalityGroupIterator.LocalityGroupContext(currentReaders);
  }

  public RFileReader(CachableBlockFile.CachableBuilder b) throws IOException {
    this(new CachableBlockFile.Reader(b));
  }

  protected void closeLocalityGroupReaders() {
    for (LocalityGroupReader lgr : currentReaders) {
      try {
        lgr.close();
      } catch (IOException e) {
        log.warn("Errored out attempting to close LocalityGroupReader.", e);
      }
    }
  }

  @Override
  public void closeDeepCopies() {
    if (deepCopy) {
      throw new RuntimeException("Calling closeDeepCopies on a deep copy is not supported");
    }

    for (RFileReader deepCopy : deepCopies) {
      deepCopy.closeLocalityGroupReaders();
    }

    deepCopies.clear();
  }

  @Override
  public void close() throws IOException {
    if (deepCopy) {
      throw new RuntimeException("Calling close on a deep copy is not supported");
    }

    closeDeepCopies();
    closeLocalityGroupReaders();

    if (sampleReaders != null) {
      for (LocalityGroupReader lgr : sampleReaders) {
        try {
          lgr.close();
        } catch (IOException e) {
          log.warn("Errored out attempting to close LocalityGroupReader.", e);
        }
      }
    }

    try {
      reader.close();
    } finally {
      /**
       * input Stream is passed to CachableBlockFile and closed there
       */
    }
  }

  protected LocalityGroupReader getReaderInstance(CachableBlockFile.Reader reader,
      LocalityGroupMetadata metadata, int ver) {
    return new LocalityGroupReader(reader, metadata, ver);
  }

  @Override
  public Key getFirstKey() throws IOException {
    if (currentReaders.length == 0) {
      return null;
    }

    Key minKey = null;

    for (LocalityGroupReader currentReader : currentReaders) {
      if (minKey == null) {
        minKey = currentReader.getFirstKey();
      } else {
        Key firstKey = currentReader.getFirstKey();
        if (firstKey != null && firstKey.compareTo(minKey) < 0) {
          minKey = firstKey;
        }
      }
    }

    return minKey;
  }

  @Override
  public Key getLastKey() throws IOException {
    if (currentReaders.length == 0) {
      return null;
    }

    Key maxKey = null;

    for (LocalityGroupReader currentReader : currentReaders) {
      if (maxKey == null) {
        maxKey = currentReader.getLastKey();
      } else {
        Key lastKey = currentReader.getLastKey();
        if (lastKey != null && lastKey.compareTo(maxKey) > 0) {
          maxKey = lastKey;
        }
      }
    }

    return maxKey;
  }

  @Override
  public DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException {
    try {
      return this.reader.getMetaBlock(name);
    } catch (MetaBlockDoesNotExist e) {
      throw new NoSuchMetaStoreException("name = " + name, e);
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    if (env != null && env.isSamplingEnabled()) {
      SamplerConfiguration sc = env.getSamplerConfiguration();
      if (sc == null) {
        throw new SampleNotPresentException();
      }

      if (this.samplerConfig != null
          && this.samplerConfig.equals(new SamplerConfigurationImpl(sc))) {
        RFileReader copy = new RFileReader(this, true);
        copy.setInterruptFlagInternal(interruptFlag);
        deepCopies.add(copy);
        return copy;
      } else {
        throw new SampleNotPresentException();
      }
    } else {
      RFileReader copy = new RFileReader(this, false);
      copy.setInterruptFlagInternal(interruptFlag);
      deepCopies.add(copy);
      return copy;
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  /**
   * @return map of locality group names to column families. The default locality group will have
   *         {@code null} for a name. RFile will only track up to
   *         {@value RFile.Writer#MAX_CF_IN_DLG} families for the default locality group. After this
   *         it will stop tracking. For the case where the default group has more thn
   *         {@value RFile.Writer#MAX_CF_IN_DLG} families an empty list of families is returned.
   * @see LocalityGroupUtil#seek(FileSKVIterator, Range, String, Map)
   */
  public Map<String,ArrayList<ByteSequence>> getLocalityGroupCF() {
    Map<String,ArrayList<ByteSequence>> cf = new HashMap<>();

    for (LocalityGroupMetadata lcg : localityGroups) {
      ArrayList<ByteSequence> setCF;

      if (lcg.columnFamilies == null) {
        Preconditions.checkState(lcg.isDefaultLG, "Group %s has null families. "
            + "Only expect default locality group to have null families.", lcg.name);
        setCF = new ArrayList<>();
      } else {
        setCF = new ArrayList<>(lcg.columnFamilies.keySet());
      }

      cf.put(lcg.name, setCF);
    }

    return cf;
  }

  /**
   * Method that registers the given MetricsGatherer. You can only register one as it will clobber
   * any previously set. The MetricsGatherer should be registered before iterating through the
   * LocalityGroups.
   *
   * @param vmg MetricsGatherer to be registered with the LocalityGroupReaders
   */
  public void registerMetrics(MetricsGatherer<?> vmg) {
    vmg.init(getLocalityGroupCF());
    for (LocalityGroupReader lgr : currentReaders) {
      lgr.registerMetrics(vmg);
    }

    if (sampleReaders != null) {
      for (LocalityGroupReader lgr : sampleReaders) {
        lgr.registerMetrics(vmg);
      }
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    lgCache =
        LocalityGroupIterator.seek(this, lgContext, range, columnFamilies, inclusive, lgCache);
  }

  int getNumLocalityGroupsSeeked() {
    return (lgCache == null ? 0 : lgCache.getNumLGSeeked());
  }

  public FileSKVIterator getIndex() throws IOException {

    ArrayList<Iterator<MultiLevelIndex.IndexEntry>> indexes = new ArrayList<>();

    for (LocalityGroupReader lgr : currentReaders) {
      indexes.add(lgr.getIndex());
    }

    return new MultiIndexIterator(this, indexes);
  }

  @Override
  public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
    requireNonNull(sampleConfig);

    if (this.samplerConfig != null && this.samplerConfig.equals(sampleConfig)) {
      RFileReader copy = new RFileReader(this, sampleReaders);
      copy.setInterruptFlagInternal(interruptFlag);
      return copy;
    }

    return null;
  }

  // only visible for printinfo
  FileSKVIterator getSample() {
    if (samplerConfig == null) {
      return null;
    }
    return getSample(this.samplerConfig);
  }

  public void printInfo(boolean includeIndexDetails) throws IOException {

    System.out.printf("%-24s : %d\n", "RFile Version", rfileVersion);
    System.out.println();

    for (LocalityGroupMetadata lgm : localityGroups) {
      lgm.printInfo(false, includeIndexDetails);
    }

    if (!sampleGroups.isEmpty()) {

      System.out.println();
      System.out.printf("%-24s :\n", "Sample Configuration");
      System.out.printf("\t%-22s : %s\n", "Sampler class ", samplerConfig.getClassName());
      System.out.printf("\t%-22s : %s\n", "Sampler options ", samplerConfig.getOptions());
      System.out.println();

      for (LocalityGroupMetadata lgm : sampleGroups) {
        lgm.printInfo(true, includeIndexDetails);
      }
    }
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    if (deepCopy) {
      throw new RuntimeException("Calling setInterruptFlag on a deep copy is not supported");
    }

    if (!deepCopies.isEmpty()) {
      throw new RuntimeException("Setting interrupt flag after calling deep copy not supported");
    }

    setInterruptFlagInternal(flag);
  }

  protected void setInterruptFlagInternal(AtomicBoolean flag) {
    this.interruptFlag = flag;
    for (LocalityGroupReader lgr : currentReaders) {
      lgr.setInterruptFlag(interruptFlag);
    }
  }

  @Override
  public void setCacheProvider(CacheProvider cacheProvider) {
    reader.setCacheProvider(cacheProvider);
  }
}
