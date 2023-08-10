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

package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;

import org.apache.accumulo.core.conf.AccumuloConfiguration;

public class ShardIndexGenerator extends ShardGenerator {

  private final int shardCount;
  protected boolean generateShard = false;
  /*
   * Map<String,List<UID>> shardMapping;
   *
   * List<String> shards = new ArrayList<>();
   *
   * Multimap<String,UID> authToDocId = ArrayListMultimap.create();
   *
   * Multimap<Text,ShardGenerator> generators = ArrayListMultimap.create();
   *
   * protected Multimap<Text, UID> fieldValueToDoc = ArrayListMultimap.create();
   *
   */

  public ShardIndexGenerator(CreatorConfiguration config,
      AccumuloConfiguration accumuloConfiguration, FileOutputStream out, HashSet<String> dataTypes,
      HashSet<String> indexedFieldNames, int shardCount) {
    super(config, accumuloConfiguration, out, "", dataTypes, indexedFieldNames, null);
    this.shardCount = shardCount;
  }

  @Override
  public boolean generateKeys(IndexingStrategy option) throws IOException {
    // field index
    // shard fi\x00FIELD_NAME field_value\x00datatype\x00uid
    if (generateShard) {
      // writeFiKeys();
    }

    try {
      var bldr = KeyCreatorBuilder.newBuilder(config, writer)
          .withStrategy(GlobalIndexStrategy.class.cast(option));
      if (null != bldr) {
        bldr.build().writeGlobalIndex();
      } else {
        System.out.println("Skipping...");
        return false;
      }

      // doc
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
      // shard datatype\x00uid FIELD_NAME\x00FIELD_VALUE
    } finally {
      writer.close();
    }
    return true;
  }

  static ZoneId defaultZoneId = ZoneId.systemDefault();

  static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ENGLISH);

  public static byte[] shardToBitstream_v0(String shards, int shardsPerDay) {
    // System.out.println(shards);
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

}
