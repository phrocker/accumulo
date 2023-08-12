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
package org.apache.accumulo.custom.file.rfile;

import java.io.DataInput;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.rfile.RelativeKey;
import org.apache.hadoop.io.WritableUtils;

public class CustomRelativeKey extends RelativeKey {

  /**
   * This constructor is used when one needs to read from an input stream
   */
  public CustomRelativeKey() {

  }

  /**
   * This constructor is used when constructing a key for writing to an output stream
   */
  public CustomRelativeKey(Key prevKey, Key key) {
    super(prevKey, key);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fieldsSame = in.readByte();
    if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
      fieldsPrefixed = in.readByte();
    } else {
      fieldsPrefixed = 0;
    }

    final byte[] row, cf, cq, cv;
    final long ts;

    row = getData(in, ROW_SAME, ROW_COMMON_PREFIX, () -> prevKey.getRowData());
    cf = getData(in, CF_SAME, CF_COMMON_PREFIX, () -> prevKey.getColumnFamilyData());
    cq = getData(in, CQ_SAME, CQ_COMMON_PREFIX, () -> prevKey.getColumnQualifierData());
    cv = getData(in, CV_SAME, CV_COMMON_PREFIX, () -> prevKey.getColumnVisibilityData());

    if ((fieldsSame & TS_SAME) == TS_SAME) {
      ts = prevKey.getTimestamp();
    } else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      ts = WritableUtils.readVLong(in) + prevKey.getTimestamp();
    } else {
      ts = WritableUtils.readVLong(in);
    }

    this.key = new Key(row, cf, cq, cv, ts, (fieldsSame & DELETED) == DELETED, false);
    this.prevKey = this.key;
  }

}
