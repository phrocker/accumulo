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

import org.apache.accumulo.core.data.Key;

public class SkippedRelativeKey<R extends RelativeKey> {

  public R rk;
  public int skipped;
  public Key prevKey;
  public boolean filtered;

  public org.apache.accumulo.file.rfile.predicate.KeyPredicate keyPredicate = null;

  public SkippedRelativeKey(R rk, int skipped, Key prevKey,
      org.apache.accumulo.file.rfile.predicate.KeyPredicate keyPredicate) {
    this(rk, skipped, prevKey, false, keyPredicate);
  }

  public SkippedRelativeKey(R rk, int skipped, Key prevKey, boolean filtered,
      org.apache.accumulo.file.rfile.predicate.KeyPredicate keyPredicate) {
    this.rk = rk;
    this.skipped = skipped;
    this.prevKey = prevKey;
    this.filtered = filtered;
    this.keyPredicate = keyPredicate;
  }

}