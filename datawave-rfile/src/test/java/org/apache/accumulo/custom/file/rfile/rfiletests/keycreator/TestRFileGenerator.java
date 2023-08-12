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
package org.apache.accumulo.custom.file.rfile.rfiletests.keycreator;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

public abstract class TestRFileGenerator {

  protected Configuration conf = new Configuration();

  private final FileOutputStream out;

  protected AccumuloConfiguration accumuloConfiguration;
  private FSDataOutputStream dos;
  protected RFile.Writer writer;

  public TestRFileGenerator(AccumuloConfiguration accumuloConfiguration, FileOutputStream out) {
    this.out = out;
    this.accumuloConfiguration = accumuloConfiguration;
    if (this.accumuloConfiguration == null) {
      this.accumuloConfiguration = DefaultConfiguration.getInstance();
    }
  }

  public abstract boolean generateKeys(IndexingStrategy option) throws IOException;

  public void init(int blockSize, boolean startDLG) throws IOException {
    dos = new FSDataOutputStream(out, new FileSystem.Statistics("a"));
    CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
        accumuloConfiguration.getAllCryptoProperties());

    BCFile.Writer _cbw = new BCFile.Writer(dos, null, "gz", conf, cs);

    SamplerConfigurationImpl samplerConfig =
        SamplerConfigurationImpl.newSamplerConfig(accumuloConfiguration);
    Sampler sampler = null;

    if (samplerConfig != null) {
      // sampler = SamplerFactory.newSampler(samplerConfig, accumuloConfiguration);
    }

    writer = new RFile.Writer(_cbw, blockSize, 256 * 1024, samplerConfig, sampler);

    if (startDLG) {
      writer.startDefaultLocalityGroup();
    }
  }

}
