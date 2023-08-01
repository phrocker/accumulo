package org.apache.accumulo.core.file.rfile.rfiletests;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager;
import org.apache.accumulo.core.file.blockfile.impl.BasicCacheProvider;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileReader;
import org.apache.accumulo.core.file.rfile.SequentialRFileReader;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.predicate.KeyPredicate;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;

public class TestRFile {

    private static final SecureRandom random = new SecureRandom();

    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    private static final Configuration hadoopConf = new Configuration();

    protected Configuration conf = new Configuration();
    public RFile.Writer writer;
    protected FSDataOutputStream dos;
    protected SeekableByteArrayInputStream bais;
    protected FSDataInputStream in;
    protected AccumuloConfiguration accumuloConfiguration;
    public RFileReader reader;
    public SortedKeyValueIterator<Key, Value> iter;
    private BlockCacheManager manager;

    public TestRFile(AccumuloConfiguration accumuloConfiguration) {
        this.accumuloConfiguration = accumuloConfiguration;
        if (this.accumuloConfiguration == null) {
            this.accumuloConfiguration = DefaultConfiguration.getInstance();
        }
    }

    public AccumuloConfiguration getAccumuloConfiguration(){
        return this.accumuloConfiguration;
    }

    public void openWriter(FileOutputStream fos, boolean startDLG) throws IOException {
        openWriter(fos, startDLG, 1000);
    }

    public void openWriter(FileOutputStream fos, boolean startDLG, int blockSize)
            throws IOException {
        dos = new FSDataOutputStream(fos, new FileSystem.Statistics("a"));
        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
                accumuloConfiguration.getAllCryptoProperties());

        BCFile.Writer _cbw = new BCFile.Writer(dos, null, "gz", conf, cs);

        SamplerConfigurationImpl samplerConfig =
                SamplerConfigurationImpl.newSamplerConfig(accumuloConfiguration);
        Sampler sampler = null;

        if (samplerConfig != null) {
            // sampler = SamplerFactory.newSampler(samplerConfig, accumuloConfiguration);
        }

        writer = new RFile.Writer(_cbw, blockSize, 128 * 1024, samplerConfig, sampler);

        if (startDLG) {
            writer.startDefaultLocalityGroup();
        }
    }

    public void openWriter(FileOutputStream fos) throws IOException {
        openWriter(fos, 100000);
    }

    public void openWriter(FileOutputStream fos, int blockSize) throws IOException {
        openWriter(fos, true, blockSize);
    }

    public void closeWriter() throws IOException {
        dos.flush();
        writer.close();
        dos.close();
    }

    public void  openSequentialReader(File file, long fileLength, boolean cfsi, String auths, KeyPredicate keyPredicate)
            throws IOException {
        Path pt = new Path(file.getAbsolutePath());
        FileSystem fs = FileSystem.get(new Configuration());
        in = fs.open(pt);

        DefaultConfiguration dc = DefaultConfiguration.getInstance();
        ConfigurationCopy cc = new ConfigurationCopy(dc);
        cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
        try {
            manager = BlockCacheManagerFactory.getInstance(cc);
        } catch (Exception e) {
            throw new RuntimeException("Error creating BlockCacheManager", e);
        }
        cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(1 * 1024 * 1024));
        cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(100000000));
        cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(100000000));
        manager.start(BlockCacheConfiguration.forTabletServer(cc));
        LruBlockCache indexCache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);
        LruBlockCache dataCache = (LruBlockCache) manager.getBlockCache(CacheType.DATA);

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
                accumuloConfiguration.getAllCryptoProperties());

        CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder().input(in, "source-1").length(fileLength).conf(conf)
                .cacheProvider(new BasicCacheProvider(indexCache, dataCache)).cryptoService(cs);
        reader = new SequentialRFileReader(cb, auths, keyPredicate);
        if (cfsi) {
            iter = new ColumnFamilySkippingIterator(reader);
        }

        //checkIndex(reader);
    }

    public void openNormalReader(File file, long fileLength, boolean cfsi) throws IOException {

        Path pt = new Path(file.getAbsolutePath());
        FileSystem fs = FileSystem.get(new Configuration());
        in = fs.open(pt);

        DefaultConfiguration dc = DefaultConfiguration.getInstance();
        ConfigurationCopy cc = new ConfigurationCopy(dc);
        cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
        try {
            manager = BlockCacheManagerFactory.getInstance(cc);
        } catch (Exception e) {
            throw new RuntimeException("Error creating BlockCacheManager", e);
        }
        cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(1 * 1024 * 1024));
        cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(100000000));
        cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(100000000));
        manager.start(BlockCacheConfiguration.forTabletServer(cc));
        LruBlockCache indexCache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);
        LruBlockCache dataCache = (LruBlockCache) manager.getBlockCache(CacheType.DATA);

        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
                accumuloConfiguration.getAllCryptoProperties());

        CachableBlockFile.CachableBuilder cb = new CachableBlockFile.CachableBuilder().input(in, "source-1").length(fileLength).conf(conf)
                .cacheProvider(new BasicCacheProvider(indexCache, dataCache)).cryptoService(cs);
        reader = new RFileReader(cb);
        if (cfsi) {
            iter = new ColumnFamilySkippingIterator(reader);
        }

        //checkIndex(reader);
    }

    public void closeReader() throws IOException {
        reader.close();
        in.close();
        if (null != manager) {
            manager.stop();
        }
    }

    public void seek(Key nk) throws IOException {
        iter.seek(new Range(nk, null), EMPTY_COL_FAMS, false);
    }

    public void append(Key key, Value value) throws IOException {
        writer.append(key, value);
    }

    private static void checkIndex(RFileReader reader) throws IOException {
        FileSKVIterator indexIter = reader.getIndex();

        if (indexIter.hasTop()) {
            Key lastKey = new Key(indexIter.getTopKey());

            if (reader.getFirstKey().compareTo(lastKey) > 0) {
                throw new RuntimeException(
                        "First key out of order " + reader.getFirstKey() + " " + lastKey);
            }

            indexIter.next();

            while (indexIter.hasTop()) {
                if (lastKey.compareTo(indexIter.getTopKey()) > 0) {
                    throw new RuntimeException(
                            "Indext out of order " + lastKey + " " + indexIter.getTopKey());
                }

                lastKey = new Key(indexIter.getTopKey());
                indexIter.next();

            }

            if (!reader.getLastKey().equals(lastKey)) {
                throw new RuntimeException("Last key out of order " + reader.getLastKey() + " " + lastKey);
            }
        }
    }
    /**
     *
     *
     * CLASSES
     *
     *
     */

    static class SeekableByteArrayInputStream extends ByteArrayInputStream
            implements Seekable, PositionedReadable {

        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (mark != 0) {
                throw new IllegalStateException();
            }

            reset();
            long skipped = skip(pos);

            if (skipped != pos) {
                throw new IOException();
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) {

            if (position >= buf.length) {
                throw new IllegalArgumentException();
            }
            if (position + length > buf.length) {
                throw new IllegalArgumentException();
            }
            if (length > buffer.length) {
                throw new IllegalArgumentException();
            }

            System.arraycopy(buf, (int) position, buffer, offset, length);
            return length;
        }

        @Override
        public void readFully(long position, byte[] buffer) {
            read(position, buffer, 0, buffer.length);

        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) {
            read(position, buffer, offset, length);
        }

    }
}