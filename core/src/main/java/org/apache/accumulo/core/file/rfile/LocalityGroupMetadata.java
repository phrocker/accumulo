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

import static org.apache.accumulo.core.file.rfile.RFile.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.io.Writable;

class LocalityGroupMetadata implements Writable {

  int startBlock = -1;
  Key firstKey;
  Map<ByteSequence,MutableLong> columnFamilies;

  boolean isDefaultLG = false;
  String name;
  Set<ByteSequence> previousColumnFamilies;

  MultiLevelIndex.BufferedWriter indexWriter;
  MultiLevelIndex.Reader indexReader;
  int version;

  public LocalityGroupMetadata(int version, CachableBlockFile.Reader br) {
    columnFamilies = new HashMap<>();
    indexReader = new MultiLevelIndex.Reader(br, version);
    this.version = version;
  }

  public LocalityGroupMetadata(Set<ByteSequence> pcf, int indexBlockSize, BCFile.Writer bfw) {
    isDefaultLG = true;
    columnFamilies = new HashMap<>();
    previousColumnFamilies = pcf;

    indexWriter =
        new MultiLevelIndex.BufferedWriter(new MultiLevelIndex.Writer(bfw, indexBlockSize));
  }

  public LocalityGroupMetadata(String name, Set<ByteSequence> cfset, int indexBlockSize,
      BCFile.Writer bfw) {
    this.name = name;
    isDefaultLG = false;
    columnFamilies = new HashMap<>();
    for (ByteSequence cf : cfset) {
      columnFamilies.put(cf, new MutableLong(0));
    }

    indexWriter =
        new MultiLevelIndex.BufferedWriter(new MultiLevelIndex.Writer(bfw, indexBlockSize));
  }

  Key getFirstKey() {
    return firstKey;
  }

  void setFirstKey(Key key) {
    if (firstKey != null) {
      throw new IllegalStateException();
    }
    this.firstKey = new Key(key);
  }

  public void updateColumnCount(Key key) {

    if (isDefaultLG && columnFamilies == null) {
      if (!previousColumnFamilies.isEmpty()) {
        // only do this check when there are previous column families
        ByteSequence cf = key.getColumnFamilyData();
        if (previousColumnFamilies.contains(cf)) {
          throw new IllegalArgumentException("Added column family \"" + cf
              + "\" to default locality group that was in previous locality group");
        }
      }

      // no longer keeping track of column families, so return
      return;
    }

    ByteSequence cf = key.getColumnFamilyData();
    MutableLong count = columnFamilies.get(cf);

    if (count == null) {
      if (!isDefaultLG) {
        throw new IllegalArgumentException("invalid column family : " + cf);
      }

      if (previousColumnFamilies.contains(cf)) {
        throw new IllegalArgumentException("Added column family \"" + cf
            + "\" to default locality group that was in previous locality group");
      }

      if (columnFamilies.size() > RFile.Writer.MAX_CF_IN_DLG) {
        // stop keeping track, there are too many
        columnFamilies = null;
        return;
      }
      count = new MutableLong(0);
      columnFamilies.put(new ArrayByteSequence(cf.getBackingArray(), cf.offset(), cf.length()),
          count);

    }

    count.increment();

  }

  @Override
  public void readFields(DataInput in) throws IOException {

    isDefaultLG = in.readBoolean();
    if (!isDefaultLG) {
      name = in.readUTF();
    }

    if (version == RINDEX_VER_3 || version == RINDEX_VER_4 || version == RINDEX_VER_6
        || version == RINDEX_VER_7) {
      startBlock = in.readInt();
    }

    int size = in.readInt();

    if (size == -1) {
      if (!isDefaultLG) {
        throw new IllegalStateException(
            "Non default LG " + name + " does not have column families");
      }

      columnFamilies = null;
    } else {
      if (columnFamilies == null) {
        columnFamilies = new HashMap<>();
      } else {
        columnFamilies.clear();
      }

      for (int i = 0; i < size; i++) {
        int len = in.readInt();
        byte[] cf = new byte[len];
        in.readFully(cf);
        long count = in.readLong();

        columnFamilies.put(new ArrayByteSequence(cf), new MutableLong(count));
      }
    }

    if (in.readBoolean()) {
      firstKey = new Key();
      firstKey.readFields(in);
    } else {
      firstKey = null;
    }

    indexReader.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeBoolean(isDefaultLG);
    if (!isDefaultLG) {
      out.writeUTF(name);
    }

    if (isDefaultLG && columnFamilies == null) {
      // only expect null when default LG, otherwise let a NPE occur
      out.writeInt(-1);
    } else {
      out.writeInt(columnFamilies.size());

      for (Map.Entry<ByteSequence,MutableLong> entry : columnFamilies.entrySet()) {
        out.writeInt(entry.getKey().length());
        out.write(entry.getKey().getBackingArray(), entry.getKey().offset(),
            entry.getKey().length());
        out.writeLong(entry.getValue().longValue());
      }
    }

    out.writeBoolean(firstKey != null);
    if (firstKey != null) {
      firstKey.write(out);
    }

    indexWriter.close(out);
  }

  public void printInfo(boolean isSample, boolean includeIndexDetails) throws IOException {
    PrintStream out = System.out;
    out.printf("%-24s : %s\n", (isSample ? "Sample " : "") + "Locality group ",
        (isDefaultLG ? "<DEFAULT>" : name));
    if (version == RINDEX_VER_3 || version == RINDEX_VER_4 || version == RINDEX_VER_6
        || version == RINDEX_VER_7) {
      out.printf("\t%-22s : %d\n", "Start block", startBlock);
    }
    out.printf("\t%-22s : %,d\n", "Num   blocks", indexReader.size());
    TreeMap<Integer,Long> sizesByLevel = new TreeMap<>();
    TreeMap<Integer,Long> countsByLevel = new TreeMap<>();
    indexReader.getIndexInfo(sizesByLevel, countsByLevel);
    for (Map.Entry<Integer,Long> entry : sizesByLevel.descendingMap().entrySet()) {
      out.printf("\t%-22s : %,d bytes  %,d blocks\n", "Index level " + entry.getKey(),
          entry.getValue(), countsByLevel.get(entry.getKey()));
    }
    out.printf("\t%-22s : %s\n", "First key", firstKey);

    Key lastKey = null;
    if (indexReader.size() > 0) {
      lastKey = indexReader.getLastKey();
    }

    out.printf("\t%-22s : %s\n", "Last key", lastKey);

    long numKeys = 0;
    MultiLevelIndex.Reader.IndexIterator countIter = indexReader.lookup(new Key());
    while (countIter.hasNext()) {
      MultiLevelIndex.IndexEntry indexEntry = countIter.next();
      numKeys += indexEntry.getNumEntries();
    }

    out.printf("\t%-22s : %,d\n", "Num entries", numKeys);
    out.printf("\t%-22s : %s\n", "Column families",
        (isDefaultLG && columnFamilies == null ? "<UNKNOWN>" : columnFamilies.keySet()));

    if (includeIndexDetails) {
      out.printf("\t%-22s :\nIndex Entries", lastKey);
      String prefix = "\t   ";
      indexReader.printIndex(prefix, out);
    }
  }

}
