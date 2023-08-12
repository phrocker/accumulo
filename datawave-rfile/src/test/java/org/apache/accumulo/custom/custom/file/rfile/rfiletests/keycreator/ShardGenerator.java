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
package org.apache.accumulo.custom.custom.file.rfile.rfiletests.keycreator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.custom.custom.file.rfile.uids.UID;
import org.apache.hadoop.io.Text;

public class ShardGenerator extends TestRFileGenerator {

  protected static final String NULL = "\u0000";
  /*
   * public final SortedSet<UID> uuidHashSet; protected final SortedSet<String> unindexedFieldNames;
   * protected final SortedSet<String> indexedFieldNames;
   *
   * protected final SortedSet<String> allFieldNames;
   *
   * protected final SortedSet<String> dataTypes;
   *
   *
   * protected SortedSet<Text> fieldValues; protected long numberDocs=1000;
   *
   * protected long numberOfFieldValues=2; protected List<AuthEstimate> auths;
   *
   * protected Multimap<UID,Text> docToAuthMap = ArrayListMultimap.create();
   *
   * protected Multimap<String, FieldInjector> injectedMappings = ArrayListMultimap.create();
   */

  protected final CreatorConfiguration config;

  protected final Text shard;

  public ShardGenerator(CreatorConfiguration config, AccumuloConfiguration accumuloConfiguration,
      FileOutputStream out, String shard, HashSet<String> dataTypes,
      HashSet<String> indexedFieldNames, HashSet<String> unindexedFieldNames) {
    super(accumuloConfiguration, out);
    this.config = config;
    Objects.requireNonNull(dataTypes);
    this.shard = new Text(shard);
  }

  @Override
  public boolean generateKeys(IndexingStrategy option) throws IOException {
    // field index
    // shard fi\x00FIELD_NAME field_value\x00datatype\x00uid

    writeFiKeys();

    // doc

    // shard datatype\x00uid FIELD_NAME\x00FIELD_VALUE
    writer.close();
    return true;
  }

  public byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(x);
    return buffer.array();
  }

  /**
   * To avoid writing out of order
   */
  protected void writeFiKeys() throws IOException {
    Value emptyValue = new Value();
    for (String fieldName : config.indexedFieldNames) {
      SortedSet<Key> keys = new TreeSet<>();
      Text cf = new Text("fi\u0000" + fieldName);
      var fieldValueMappings = config.injectedMappings.get(fieldName);
      if (null != fieldValueMappings && !fieldValueMappings.isEmpty()) {
        for (var collection : fieldValueMappings) {
          Text fv = collection.fieldValue;
          while (collection.numberOfDocs > 0) {
            for (String datatype : config.dataTypes) {
              for (UID uid : config.uuidHashSet) {
                Text cq = new Text(fv + NULL + datatype + NULL + uid.toString());

                var key = new Key(shard, cf, cq, collection.auth);
                keys.add(key);
                if (--collection.numberOfDocs <= 0) {
                  break;
                }
              }
            }
          }
        }
      }

      for (Text fv : config.fieldValues) {
        for (String datatype : config.dataTypes) {
          for (UID uid : config.uuidHashSet) {
            Text cq = new Text(fv + NULL + datatype + NULL + uid.toString());
            var cvList = config.docToAuthMap.get(uid);
            if (null == cvList || cvList.isEmpty()) {
              var key = new Key(shard, cf, cq, config.defaultAuth);
              keys.add(key);
            } else {
              for (var cv : cvList) {
                var key = new Key(shard, cf, cq, cv);
                keys.add(key);
              }
            }

          }
        }
      }
      for (var key : keys) {
        // super.get
        writer.append(key, emptyValue);
      }
    }

  }

  public static void main(String args[]) {

  }
}
