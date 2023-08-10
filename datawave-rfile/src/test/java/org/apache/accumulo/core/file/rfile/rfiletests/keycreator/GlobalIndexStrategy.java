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

public class GlobalIndexStrategy implements IndexingStrategy {

  public enum STRATEGY {
    ORIGINAL;
    /*
     * BITSTREAMSHARD, UIDATEND_REGULARSHARD, UIDATEND_BITSTREAMSHARDINROW, ORIGINAL_NOUIDS;
     *
     */
    // UIDATEND_STRINGSHARDINROW
    // UIDATEND_BITSTREAMSHARDINCQ,
    // UIDATEND_BITSTREAMSHARDINCQ_DATATYPEINCF

  };

  final STRATEGY strat;

  public GlobalIndexStrategy(STRATEGY strat) {
    this.strat = strat;
  }

  @Override
  public void select(TestRFileGenerator generator) throws IOException {
    var gen = ShardIndexGenerator.class.cast(generator);
    /*
     * switch (strat) { case ORIGINAL: gen.writeGlobalIndexOld(); break; case BITSTREAMSHARD:
     * gen.writeGlobalIndexOld2(); break; case UIDATEND_REGULARSHARD: gen.writeGlobalIndexNew();
     * break; case UIDATEND_BITSTREAMSHARDINROW: gen.writeGlobalIndexNewNew(); break; // case
     * UIDATEND_STRINGSHARDINROW: // gen.writeGlobalIndexNewNewNew2(); // break; /* case
     * UIDATEND_BITSTREAMSHARDINCQ: gen.writeGlobalIndexNewNew2(); break; case
     * UIDATEND_BITSTREAMSHARDINCQ_DATATYPEINCF: gen.writeGlobalIndexNewNew3(); break;
     */
  }
}
