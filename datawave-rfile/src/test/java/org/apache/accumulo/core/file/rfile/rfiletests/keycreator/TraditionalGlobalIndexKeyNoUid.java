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

import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.uids.UID;
import org.apache.accumulo.core.file.rfile.uids.proto.Uid;
import org.apache.hadoop.io.Text;

public class TraditionalGlobalIndexKeyNoUid extends GlobalIndexKeyCreator {
  public TraditionalGlobalIndexKeyNoUid(CreatorConfiguration config, RFile.Writer writer) {
    super(config, writer);
  }

  @Override
  protected Collection<KeyValue> formKeyPart(String datatype, Text fv, String fieldName,
      Collection<UID> docsInfv, Text cv, String myShard) {
    var uidBuilder = Uid.List.newBuilder();
    // var mapping = mymapping.get(cv);

    Text cq = new Text(datatype + NULL + myShard);

    // var intersection = mapping.stream().filter(docsInfv::contains).collect(Collectors.toList());

    docsInfv.forEach(x -> uidBuilder.addUID(x.toString()));
    uidBuilder.setCOUNT(docsInfv.size());
    uidBuilder.setIGNORE(false);
    var key = new Key(fv, new Text(fieldName), cq, cv);
    return Collections.singleton(new KeyValue(key, EMPTY_VALUE));
  }
}
