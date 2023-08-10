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
package org.apache.accumulo.core.file.rfile.predicate;

import java.util.Arrays;

import org.apache.accumulo.core.data.ByteSequence;

public class UniqueRowPredicate extends RowPredicate {

  byte[] prev = null;

  @Override
  public boolean acceptRow(ByteSequence row, boolean isSame) {
    // in this edition let's assume the row matches our terms
    if (isSame) {
      prev = row.getBackingArray();
      return false;
    } else {
      if (null != prev) {
        if (Arrays.equals(row.getBackingArray(), prev)) {
          return false;
        }
      }
      prev = row.getBackingArray();
      return true;
    }
  }

  public boolean acceptRow(byte[] row, boolean isSame) {
    // in this edition let's assume the row matches our terms
    if (isSame) {
      prev = row;
      return false;
    } else {
      if (null != prev) {
        if (Arrays.equals(row, prev)) {
          return false;
        }
      }
      prev = row;
      return true;
    }
  }

}
