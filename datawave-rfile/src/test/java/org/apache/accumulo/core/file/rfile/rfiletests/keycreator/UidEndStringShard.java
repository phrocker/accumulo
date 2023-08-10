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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.uids.UID;
import org.apache.hadoop.io.Text;

public class UidEndStringShard extends GlobalIndexKeyCreator {
  public UidEndStringShard(CreatorConfiguration config, RFile.Writer writer) {
    super(config, writer);
  }

  @Override
  public void writeGlobalIndex() throws IOException {
    Value emptyValue = new Value();
    long totalKeys = 0;
    long keysMade = 0;

    int currentFieldValue = 0;
    // config.injectum();
    int totalFieldValues = config.fieldValuesWithStringShards.size();
    for (Text fv : config.fieldValuesWithStringShards) {
      SortedSet<KeyValue> keys = new TreeSet<>(new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue keyValueEntry, KeyValue t1) {
          return keyValueEntry.getKey().compareTo(t1.getKey());
        }
      });

      SortedSet<String> myFieldNames = config.indexedFieldNames;

      var injectedMapping = config.fvToFieldNameWithShard.get(fv);
      if (null != injectedMapping) {
        myFieldNames = new TreeSet<>();
        myFieldNames.add(injectedMapping.fieldName);
      }

      for (String fieldName : myFieldNames) {
        Text cf = new Text(fieldName);
        for (String datatype : config.dataTypes) {
          var fvStr = fv.toString();

          if (fvStr.length() <= 12) {
            continue;
          }
          var txtFv = new Text(fvStr.substring(0, fvStr.length() - 13));
          var myShard = fvStr.substring(fvStr.length() - 12);

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

            var docsInfv = config.fieldValueToDoc.get(txtFv);

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

  @Override
  protected Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName,
      Collection<UID> docsInfv, Text cv, String myShard) {
    Text cf = new Text(fieldName);
    List<KeyValue> kvs = new ArrayList<>();
    for (var docId : docsInfv) {
      Text cq = new Text(datatype + NULL + docId.toString());
      var key = new Key(fv, cf, cq, cv);
      kvs.add(new KeyValue(key, EMPTY_VALUE));
    }
    return kvs;
  }
}
