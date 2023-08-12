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
package org.apache.accumulo.custom.crypto;

import static org.apache.accumulo.core.conf.Property.INSTANCE_CRYPTO_FACTORY;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.spi.crypto.AESCryptoService;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;

public class CryptoTest {

  private static final SecureRandom random = new SecureRandom();
  private static final int MARKER_INT = 0xCADEFEDD;
  private static final String MARKER_STRING = "1 2 3 4 5 6 7 8 a b c d e f g h ";
  private static final Configuration hadoopConf = new Configuration();

  public enum ConfigMode {
    CRYPTO_OFF, CRYPTO_TABLE_ON, CRYPTO_WAL_ON, CRYPTO_TABLE_ON_DISABLED, CRYPTO_WAL_ON_DISABLED
  }

  @BeforeAll
  public static void setupKeyFiles() throws IOException {
    setupKeyFiles(CryptoTest.class);
  }

  public static void setupKeyFiles(Class<?> testClass) throws IOException {
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    Path aesPath = new Path(keyPath(testClass));
    try (FSDataOutputStream out = fs.create(aesPath)) {
      out.writeUTF("sixteenbytekey"); // 14 + 2 from writeUTF
    }
    try (FSDataOutputStream out = fs.create(new Path(emptyKeyPath(testClass)))) {
      // auto close after creating
      assertNotNull(out);
    }
  }

  public static ConfigurationCopy getAccumuloConfig(ConfigMode configMode, Class<?> testClass) {
    ConfigurationCopy cfg = new ConfigurationCopy(DefaultConfiguration.getInstance());
    switch (configMode) {
      case CRYPTO_TABLE_ON_DISABLED:
        cfg.set(INSTANCE_CRYPTO_FACTORY, PerTableCryptoServiceFactory.class.getName());
        cfg.set(PerTableCryptoServiceFactory.TABLE_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "false");
        break;
      case CRYPTO_TABLE_ON:
        cfg.set(INSTANCE_CRYPTO_FACTORY, PerTableCryptoServiceFactory.class.getName());
        cfg.set(PerTableCryptoServiceFactory.TABLE_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "true");
        break;
      case CRYPTO_WAL_ON_DISABLED:
        cfg.set(INSTANCE_CRYPTO_FACTORY, GenericCryptoServiceFactory.class.getName());
        cfg.set(GenericCryptoServiceFactory.GENERAL_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "false");
        break;
      case CRYPTO_WAL_ON:
        cfg.set(INSTANCE_CRYPTO_FACTORY, GenericCryptoServiceFactory.class.getName());
        cfg.set(GenericCryptoServiceFactory.GENERAL_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "true");
        break;
      case CRYPTO_OFF:
        break;
    }
    return cfg;
  }

  private ConfigurationCopy getAccumuloConfig(ConfigMode configMode) {
    return getAccumuloConfig(configMode, getClass());
  }

  private Map<String,String> getAllCryptoProperties(ConfigMode configMode) {
    var cc = getAccumuloConfig(configMode);
    return cc.getAllCryptoProperties();
  }

  public static String keyPath(Class<?> testClass) {
    return System.getProperty("user.dir") + "/target/" + testClass.getSimpleName() + "-testkeyfile";
  }

  public static String emptyKeyPath(Class<?> testClass) {
    return System.getProperty("user.dir") + "/target/" + testClass.getSimpleName()
        + "-emptykeyfile";
  }

}
