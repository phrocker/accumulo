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
package org.apache.accumulo.file.rfile.predicate;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

public abstract class KeyPredicate {

  public Key topKey;

  public Key nextBlockKey;

  public boolean accept(Key key, int fieldSame) {
    return true;
  }

  public boolean acceptColumn(ByteSequence row, boolean rowIsSame, ByteSequence cf,
      boolean cfIsSame) {
    return acceptColumn(row.getBackingArray(), rowIsSame, cf.getBackingArray(), cfIsSame, true);
  }

  public boolean acceptColumn(byte[] row, boolean rowIsSame, byte[] cf, boolean cfIsSame,
      boolean set) {
    return true;
  }

  public boolean acceptRow(ByteSequence row, boolean isSame) {
    return acceptRow(row.getBackingArray(), isSame);
  }

  public boolean acceptRow(byte[] row, boolean isSame) {
    return true;
  }

  public abstract boolean getLastKeyRowFiltered();

  public abstract boolean getLastRowCfFiltered();

  public boolean endKeyComparison() {
    return true;
  }
}