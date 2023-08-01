package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.rfiletests.keycreator.IndexingStrategy;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileOutputStream;
import java.io.IOException;

public abstract class TestRFileGenerator {


    protected Configuration conf = new Configuration();

    private final FileOutputStream out;

    protected AccumuloConfiguration accumuloConfiguration;
    private FSDataOutputStream dos;
    protected RFile.Writer writer;


    public TestRFileGenerator(AccumuloConfiguration accumuloConfiguration, FileOutputStream out){
        this.out=out;
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
