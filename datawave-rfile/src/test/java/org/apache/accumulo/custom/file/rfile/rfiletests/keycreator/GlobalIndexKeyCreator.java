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
package org.apache.accumulo.custom.file.rfile.rfiletests.keycreator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.custom.file.rfile.uids.UID;
import org.apache.hadoop.io.Text;

public abstract class GlobalIndexKeyCreator {

  protected static final Value EMPTY_VALUE = new Value();
  public static final String NULL = "\u0000";

  protected final RFile.Writer writer;

  static ZoneId defaultZoneId = ZoneId.systemDefault();

  static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ENGLISH);
  protected final CreatorConfiguration config;

  public GlobalIndexKeyCreator(CreatorConfiguration config, RFile.Writer writer) {
    this.config = config;
    this.writer = writer;
  }

  public void writeGlobalIndex() throws IOException {
    Value emptyValue = new Value();
    long totalKeys = 0;
    long keysMade = 0;

    int totalFieldValues = config.fieldValues.size();
    int currentFieldValue = 0;

    for (Text fv : config.fieldValues) {
      SortedSet<KeyValue> keys = new TreeSet<>(new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue keyValueEntry, KeyValue t1) {
          return keyValueEntry.getKey().compareTo(t1.getKey());
        }
      });

      SortedSet<String> myFieldNames = config.indexedFieldNames;
      var injectedMapping = config.fvToFieldName.get(fv);
      if (null != injectedMapping) {
        myFieldNames = new TreeSet<>();
        myFieldNames.add(injectedMapping.fieldName);
      }

      for (String fieldName : myFieldNames) {
        Text cf = new Text(fieldName);
        for (String datatype : config.dataTypes) {
          for (String myShard : config.shardMapping.keySet()) {
            Text cq = new Text(datatype + NULL + myShard);

            var uidListForShard = config.shardMapping.get(myShard);

            // Multimap<Text,UID> mymapping = ArrayListMultimap.create();
            TreeSet<Text> cvs = new TreeSet<>();
            if (null != injectedMapping) {
              cvs.add(injectedMapping.auth);
            } else {
              cvs = config.shardToAuthList.get(myShard);
            }

            // TreeSet<Text> cvs = new TreeSet<>( mymapping.keys());
            for (Text cv : cvs) {
              // var uidBuilder = Uid.List.newBuilder();
              // var mapping = mymapping.get(cv);

              var docsInfv = config.fieldValueToDoc.get(fv);

              // var intersection =
              // mapping.stream().filter(docsInfv::contains).collect(Collectors.toList());

              /*
               * docsInfv.forEach(x -> uidBuilder.addUID(x.toString()));
               * uidBuilder.setCOUNT(docsInfv.size()); uidBuilder.setIGNORE(false);
               *
               */
              var kv = formKeyPart(datatype, fv, fieldName, docsInfv, cv, myShard);
              keysMade += kv.size();

              keys.addAll(kv);

            }

          }
        }

      }
      for (var key : keys) {
        // super.get
        ++totalKeys;
        writer.append(key.getKey(), key.getValue());
      }
      var pct = ((double) ++currentFieldValue / (double) totalFieldValues) * 100;
      if ((pct % 5) == 0) {
        System.out.println(pct + "% complete....(" + currentFieldValue + " of " + totalFieldValues
            + ") total keys " + totalKeys + " total keys made " + keysMade);
      }
    }
    System.out.println(totalKeys + " keys written");

  }

  public static byte[] shardToBitstream_v0(String shards, int shardsPerDay) {
    String date = shards.substring(0, shards.indexOf("_"));
    String shard = shards.substring(shards.indexOf("_") + 1);
    LocalDate dateTime = LocalDate.parse(date, formatter);
    return shardToBitstream_v0(Date.from(dateTime.atStartOfDay(defaultZoneId).toInstant()), shard,
        shardsPerDay);
  }

  public static byte[] shardToBitstream_v0(Date date, String shardIdentifier, int shardsPerDay) {
    // 20160101_01 is 11*16 = 176 bits or 44 bytes.
    // we can condense this to 4 bytes
    // start date for this function is 20160101 or 0
    Date baseDate =
        Date.from(LocalDate.parse("2016-01-01").atStartOfDay(defaultZoneId).toInstant());
    long daysSince = ChronoUnit.DAYS.between(baseDate.toInstant(), date.toInstant());
    // System.out.println(daysSince);
    var shardNumber = Integer.valueOf(shardIdentifier);
    var shardsSince = shardsPerDay * daysSince;
    int myDate = (int) ((int) daysSince + shardsSince + (shardNumber));
    // return new UIntegerLexicoder().encode(myDate);
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(myDate);
    return bb.array();
  }

  protected abstract Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName,
      Collection<UID> uids, Text cv, String myShard);
}
