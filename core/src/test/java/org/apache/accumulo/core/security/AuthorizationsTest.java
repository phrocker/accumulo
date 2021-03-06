/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.util.ByteArraySet;
import org.junit.Test;

public class AuthorizationsTest {

  @Test
  public void testSetOfByteArrays() {
    assertTrue(ByteArraySet.fromStrings("a", "b", "c").contains("a".getBytes()));
  }

  @Test
  public void testEncodeDecode() {
    Authorizations a = new Authorizations("a", "abcdefg", "hijklmno", ",");
    byte[] array = a.getAuthorizationsArray();
    Authorizations b = new Authorizations(array);
    assertEquals(a, b);

    // test encoding empty auths
    a = new Authorizations();
    array = a.getAuthorizationsArray();
    b = new Authorizations(array);
    assertEquals(a, b);

    // test encoding multi-byte auths
    a = new Authorizations("五", "b", "c", "九");
    array = a.getAuthorizationsArray();
    b = new Authorizations(array);
    assertEquals(a, b);
  }

}
