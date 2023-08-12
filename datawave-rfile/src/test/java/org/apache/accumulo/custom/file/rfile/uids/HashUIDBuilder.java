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
package org.apache.accumulo.custom.file.rfile.uids;

import java.util.Date;

/**
 * Builds traditional hash-based UIDs
 */
public class HashUIDBuilder extends AbstractUIDBuilder<HashUID> {

  private static final byte[] EMPTY_BYTES = {};

  private final Date time;

  /**
   * Default constructor
   */
  protected HashUIDBuilder() {
    this(null);
  }

  /**
   * Constructor for generating UIDs based on a fixed time, unless a time is deliberately passed to
   * one of the <code>newId(..)</code> methods
   *
   * @param time a date/time instance
   */
  protected HashUIDBuilder(final Date time) {
    this.time = time;
  }

  @Override
  public HashUID newId(final String... extras) {
    return newId(EMPTY_BYTES, this.time, extras);
  }

  @Override
  public HashUID newId(final byte[] data, final String... extras) {
    return newId(data, this.time, extras);
  }

  @Override
  public HashUID newId(final Date time, final String... extras) {
    return newId(EMPTY_BYTES, time, extras);
  }

  @Override
  public HashUID newId(final byte[] data, final Date time, final String... extras) {
    return (null != data) ? new HashUID(data, time, extras)
        : new HashUID(EMPTY_BYTES, time, extras);
  }

  /**
   * Create a new HashUID from a different HashUID
   *
   * @param template the prototype HashUID
   * @param extras any extra values to append
   * @return a new HashUID
   */
  protected static HashUID newId(final HashUID template, final String... extras) {
    final HashUID newId;
    if (null != template) {
      // Get the existing and new extras, if any
      final String extra1 = template.getExtra();
      final String extra2 = HashUID.mergeExtras(extras);

      // Create a new UID based on existing and new extras
      if ((null != extra1) && (null != extra2)) {
        newId = new HashUID(template, extra1, extra2);
      } else if ((null != extra1) && (null == extra2)) { // Create a new UID as a copy of the
                                                         // template (with existing extras)
        newId = new HashUID(template, extra1);
      } else if ((null == extra1) && (null != extra2)) { // Create a new UID based only on new
                                                         // extra(s)
        newId = new HashUID(template, extra2);
      } else { // Create a new UID as a copy of the template (without existing extras)
        newId = new HashUID(template);
      }
    } else {
      newId = null;
    }

    return newId;
  }
}
