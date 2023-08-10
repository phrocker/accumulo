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

import org.apache.accumulo.core.file.rfile.RFile;

public class KeyCreatorBuilder {
  private final RFile.Writer writer;
  private final CreatorConfiguration config;
  private GlobalIndexStrategy strategy;

  private KeyCreatorBuilder(CreatorConfiguration config, RFile.Writer writer) {
    this.config = config;
    this.writer = writer;
  }

  public static KeyCreatorBuilder newBuilder(CreatorConfiguration config, RFile.Writer writer) {
    return new KeyCreatorBuilder(config, writer);
  }

  public KeyCreatorBuilder withStrategy(GlobalIndexStrategy strat) {
    this.strategy = strat;
    return this;
  }

  public GlobalIndexKeyCreator build() {
    switch (strategy.strat) {
      case ORIGINAL:
        return new TraditionalGlobalIndexKey(config, writer);
      /*
       * case ORIGINAL_NOUIDS: return new TraditionalGlobalIndexKeyNoUid(config, writer); case
       * BITSTREAMSHARD: return new UidValueBitstreamShard(config, writer); case
       * UIDATEND_REGULARSHARD: return new UidEndStringShard(config, writer); case
       * UIDATEND_BITSTREAMSHARDINROW: return new UidCqBitstreamShard(config, writer); default:
       * throw new IllegalStateException("Unexpected value: " + strategy);
       *
       */
      default:
        return null;
    }
  }

}
