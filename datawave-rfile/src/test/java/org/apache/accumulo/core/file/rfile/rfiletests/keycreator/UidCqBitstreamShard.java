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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.uids.UID;
import org.apache.hadoop.io.Text;

public class UidCqBitstreamShard extends GlobalIndexKeyCreator {
  public UidCqBitstreamShard(CreatorConfiguration config, RFile.Writer writer) {
    super(config, writer);
  }

  @Override
  protected Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName,
      Collection<UID> uids, Text cv, String myShard) {
    var bt = shardToBitstream_v0(myShard, 5);

    Text row = new Text(fv);
    row.append(bt, 0, bt.length);

    var docsInfv = config.fieldValueToDoc.get(fv);

    // var intersection = mapping.stream().filter(docsInfv::contains).collect(Collectors.toList());

    List<KeyValue> kvs = new ArrayList<>();
    for (var docId : docsInfv) {
      Text cq = new Text(datatype + NULL + docId.toString());
      var key = new Key(row, new Text(fieldName), cq, cv);
      kvs.add(new KeyValue(key, EMPTY_VALUE));
    }
    return kvs;

  }
}
