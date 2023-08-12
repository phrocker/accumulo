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
package org.apache.accumulo.custom.custom.file.rfile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileReader;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.TestNormalRFile;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.TestRFile;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.TestRuntime;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.TestSequentialRFile;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.keycreator.CreatorConfiguration;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.keycreator.GlobalIndexStrategy;
import org.apache.accumulo.custom.custom.file.rfile.rfiletests.keycreator.ShardIndexGenerator;
import org.apache.accumulo.custom.file.rfile.predicate.UniqueFieldPredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;

@SuppressWarnings({"unchecked", "deprecation", "static-method", "static"})
public class RFileTester {

  private static final SecureRandom random = new SecureRandom();

  public static class SampleIE implements IteratorEnvironment {

    private SamplerConfiguration samplerConfig;

    SampleIE(SamplerConfiguration config) {
      this.samplerConfig = config;
    }

    @Override
    public boolean isSamplingEnabled() {
      return samplerConfig != null;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
      return samplerConfig;
    }
  }

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
  private static final Configuration hadoopConf = new Configuration();

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupCryptoKeyFile() throws Exception {
    // CryptoTest.setupKeyFiles(RFileTester.class);
  }

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

  static Key newKey(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }

  static Value newValue(String val) {
    return new Value(val);
  }

  static String formatString(String prefix, int i) {
    return String.format(prefix + "%06d", i);
  }

  public AccumuloConfiguration conf = null;

  public byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(x);
    return buffer.array();
  }

  @Test
  public void testWriteRfiles() throws IOException {

    // test an empty file

    TestRFile trf = new TestRFile(conf);
    Key key = null;
    try (FileOutputStream fileStream =
        new FileOutputStream(new File("/mnt/ExtraDrive/data/rfiles/file7.rf"))) {
      trf.openWriter(fileStream, 1024 * 1024);

      Value def = new Value();

      for (long i = 0; i < 300000; i++) {

        // 1000 keys for every 10k will have auths
        boolean haveAuths = false;
        if ((i % 10000) == 0) {
          haveAuths = true;
        } else {
          haveAuths = false;
        }
        for (long j = 0; j < 1000; j++) {

          if (!haveAuths) {
            key = new Key(new Text("20120808_abc"), new Text(longToBytes(i)),
                new Text(longToBytes(j)), new Text("EFG"));
          } else {
            key = new Key(new Text("20120808_abc"), new Text(longToBytes(i)),
                new Text(longToBytes(j)), new Text("ABC"));
          }
          trf.append(key, def);
        }

      }

      trf.closeWriter();

      /*
       * trf.openReader(); trf.iter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
       * Assertions.assertFalse(trf.iter.hasTop());
       *
       * Assertions.assertNull(trf.reader.getLastKey());
       *
       * trf.closeReader();
       *
       */
    }
  }

  @Test
  public void test2() throws IOException {

    // test an empty file

    TestRFile trf = new TestRFile(conf);

    File file = new File("/mnt/ExtraDrive/data/rfiles/file8.rf");

    trf.openNormalReader(file, file.length(), false);

    Collection<ByteSequence> empty = Collections.emptyList();
    Key startKey = new Key(new Text("20120808_abc"), new Text(longToBytes(50)));
    Key endKey = new Key(new Text("20120808_abc"), new Text(longToBytes(700)));
    // trf.reader.seek(new Range(startKey,true,endKey,false), empty, false);
    trf.reader.seek(new Range(), empty, false);

    long count = 0;
    long ts = System.currentTimeMillis();
    while (trf.reader.hasTop()) {
      var kv = trf.reader.getTopKey();
      trf.reader.next();

      if ((++count % 10000000) == 0) {
        System.out.println(
            "went through " + count + " keys in " + (System.currentTimeMillis() - ts) + " ms");
      }
    }
    System.out
        .println("went through " + count + " keys in " + (System.currentTimeMillis() - ts) + " ms");

    trf.closeReader();

    /*
     * trf.openReader(); trf.iter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
     * Assertions.assertFalse(trf.iter.hasTop());
     *
     * Assertions.assertNull(trf.reader.getLastKey());
     *
     * trf.closeReader();
     *
     */
  }

  static ZoneId defaultZoneId = ZoneId.systemDefault();

  static DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ENGLISH).withZone(ZoneId.systemDefault());

  public static byte[] shardToBitstream_v0(String shards, int shardsPerDay) {
    String date = shards.substring(0, shards.indexOf("_"));
    String shard = shards.substring(shards.indexOf("_") + 1);
    LocalDate dateTime = LocalDate.parse(date, formatter);
    return shardToBitstream_v0(Date.from(dateTime.atStartOfDay(defaultZoneId).toInstant()), shard,
        shardsPerDay);
  }

  public static byte[] shardToBitstream_v0(Date date, String shardIdentifier, int shardsPerDay) {
    // 20160101_01 is 11*16 = 176 bits or 44 bytes.
    // we can condense this to 4 bytes
    // start date for this function is 20160101 or 0
    Date baseDate =
        Date.from(LocalDate.parse("2016-01-01").atStartOfDay(defaultZoneId).toInstant());
    long daysSince = ChronoUnit.DAYS.between(baseDate.toInstant(), date.toInstant());
    // System.out.println(daysSince);
    var shardNumber = Integer.valueOf(shardIdentifier);
    var shardsSince = shardsPerDay * daysSince;
    int myDate = (int) ((int) daysSince + shardsSince + (shardNumber));

    System.out.println("got " + myDate + " " + new UIntegerLexicoder().encode(myDate).length);
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(myDate);
    return bb.array();
  }

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-w", "--write"}, description = "write the rfiles")
    boolean writeFiles = false;

    @Parameter(names = {"--numvalues"}, description = "Specify the number of values to produce")
    int numValues = 5000;

    @Parameter(names = {"--numdocs"},
        description = "Specify the number of unique doc ids to produce")
    int numDocs = 50000;

    @Parameter(names = {"--auths"}, description = "AUTHS")
    String auths = "ABC";

    @Parameter(names = {"--beginRow"}, description = "begin row")
    String beginRow = "catinthehat";

    @Parameter(names = {"--endRow"}, description = "end row")
    String endRow = "catinthehat\uffff";

    @Parameter(names = {"--shards"}, description = "number of shards per day")
    int numberShardsPerDay = 5;

    @Parameter(names = {"--days"}, description = "number of days")
    int numberofDays = 5;

    @Parameter(names = {"--start"}, description = "startDate in format YYYYMMdd")
    String startDate = "20190606";

    @Parameter(names = {"--runs"}, description = "number of query runs")
    int numberRuns = 1;

    @Parameter(names = {"--exclude"}, description = "Exclude top N")
    int exclusions = 0;

  }

  RFileTester() {

  }

  PrintWriter getWriter(String filename, String name) throws IOException {
    File outFile = new File(filename + "." + name + ".outfile");
    outFile.createNewFile();
    var fos = new FileOutputStream(outFile);

    // runners w/o visibility
    return new PrintWriter(fos);
  }

  void runWith(Set<String> filenames, Range range, String auth, int numberRuns, int exclusions)
      throws IOException {

    SortedSet<TestRuntime> runs = new TreeSet<>();

    filenames.forEach(System.out::println);
    for (String filename : filenames) {
      try {
        System.out.println("Running test against " + filename);
        var stringwriter = getWriter(filename, "normal");
        stringwriter.write("Running test against " + filename + "\n");
        TestRFile tester = new TestRFile(null);
        Set<String> auths = new HashSet<>();
        auths.add(auth);
        var runner = TestNormalRFile.Builder.newBuilder(tester, filename).withAuths(auths).build();
        // configure the reader
        runner.configurebaseLayer();
        // configure any iterators to test
        runner.configureIterators();
        long lastCount = 0;
        runner.setStartTime();
        for (int i = 0; i < numberRuns; i++) {
          runner.resetKeysCounted();
          var myLastCount = runner.consumeAllKeys(range, stringwriter);
          if (lastCount > 0) {
            if (myLastCount != lastCount) {
              throw new Exception("Invalid counts");
            }
          }
          lastCount = myLastCount;
        }
        runner.setEndTime();
        runner.close();
        var desc = runner.getDescription(numberRuns);
        runs.add(desc);

        System.out.println("Completed" + desc);
        stringwriter.write("Completed" + desc + "\n");
        stringwriter.close();
      } catch (Exception e) {
        e.printStackTrace();
        ;
      }

    }

    System.out.println("********************************");
    boolean checkCount = false;
    for (String filename : filenames) {
      try {
        System.out.println("Running test against " + filename);
        var stringwriter = getWriter(filename, "sequential");
        stringwriter.write("Running test against " + filename + "\n");
        Set<String> auths = new HashSet<>();
        auths.add("ABC");
        TestRFile tester = new TestRFile(null);
        // var runner = TestSequentialRFile.Builder.newBuilder(tester, filename).build();
        var runnerbld = TestSequentialRFile.Builder.newBuilder(tester, filename).withAuths(auths)
            .withKeyPredicate(new UniqueFieldPredicate());
        // var runnerbld = TestSequentialRFile.Builder.newBuilder(tester,
        // filename).withAuths(auths);
        var runner = runnerbld.build();
        // configure the reader
        runner.configurebaseLayer();
        // configure any iterators to test
        runner.configureIterators();

        long lastCount = 0;
        runner.setStartTime();
        for (int i = 0; i < numberRuns; i++) {
          stringwriter.println("***** Run  " + i + " ***** ");
          runner.resetKeysCounted();

          var myLastCount = runner.consumeAllKeys(range, stringwriter);
          if (lastCount > 0) {
            if (checkCount && (myLastCount != lastCount)) {
              throw new Exception("Invalid counts " + myLastCount + " " + lastCount);
            }
          }
          lastCount = myLastCount;
        }
        runner.setEndTime();

        runner.close();

        var desc = runner.getDescription(numberRuns);
        runs.add(desc);

        System.out.println("Completed" + desc);
        stringwriter.write("Completed" + desc + "\n");
        stringwriter.close();
      } catch (Exception e) {

        e.printStackTrace();
      }

    }

    System.out.println("******************************");
    System.out.println("With range " + range);
    System.out.println("******************************");
    for (var run : runs) {
      System.out.println(run.toString());
    }

    System.out.println("******************************");
    System.out.println("/ With range " + range);

    System.out.println("******************************");

    SortedSet<File> files = new TreeSet<>(new Comparator<File>() {
      @Override
      public int compare(File file, File t1) {
        return Long.compare(file.length(), t1.length());
      }
    });

    filenames.stream().map(x -> new File(x)).forEach(files::add);

    files.forEach(file -> System.out.println(file.getName() + " (" + file.length() + ")"));

  }

  void execute(String[] args) throws IOException, InterruptedException {
    Opts opts = new Opts();
    opts.parseArgs("RFileTester", args);

    // int multipliers [] = {128, 256, 512, 1024, 128*1024 };
    int multipliers[] = {128};
    int fieldValues = opts.numValues;
    final Set<String> filenames = new ConcurrentSkipListSet<>();

    HashSet<String> dataTypes = Sets.newHashSet("csv", "json");
    HashSet<String> fieldNames = Sets.newHashSet("source", "destination", "animal");
    HashSet<String> unindexedFields = Sets.newHashSet("other");

    CreatorConfiguration config =
        new CreatorConfiguration(dataTypes, fieldNames, opts.numberShardsPerDay);
    if (opts.writeFiles) {
      LocalDate dateTime = LocalDate.parse(opts.startDate, formatter);
      for (int i = 0; i < opts.numberofDays; i++) {

        var shard = formatter.format(dateTime.atStartOfDay(defaultZoneId).toInstant());
        config.addShard(shard);
        dateTime = dateTime.plusDays(1);
      }
      List<CreatorConfiguration.AuthEstimate> estimates = new ArrayList<>();
      estimates.add(new CreatorConfiguration.AuthEstimate(0.15, "ABC"));
      config.setNumDocuments(opts.numValues);
      config.setNumFieldValues(fieldValues);
      config.addAuths(estimates, "BCD");

      config.addInjectedField("SOURCE", "catinthehat", "BCD", 20000);
      // add a field
      config.addInjectedField("SOURCE", "catinthehat", "ABC", 50000);

      config.generate();
      System.out.println("Generation is complete");
    }

    ExecutorService executor = Executors.newFixedThreadPool(32);
    for (GlobalIndexStrategy.STRATEGY strat : GlobalIndexStrategy.STRATEGY.values()) {
      String name = strat.name().toLowerCase();
      for (int multiplier : multipliers) {

        String fileName = "/mnt/ExtraDrive/data/rfiles/work/shard_index_" + name + "_" + multiplier
            + "_" + fieldValues + "-fv.rf";

        if (opts.writeFiles) {

          executor.submit(() -> {
            try {
              TestRFile trf = new TestRFile(null);
              Key key = null;
              try (FileOutputStream fileStream = new FileOutputStream(fileName)) {

                ShardIndexGenerator generator =
                    new ShardIndexGenerator(config, trf.getAccumuloConfiguration(), fileStream,
                        dataTypes, fieldNames, opts.numberShardsPerDay);

                generator.init(multiplier * 1024, true);
                boolean shouldAdd = true;
                shouldAdd = generator.generateKeys(new GlobalIndexStrategy(strat));
                System.out.println("******************************");
                if (shouldAdd) {
                  System.out.println("Wrote out " + fileName);
                  filenames.add(fileName);
                } else {
                  System.out.println("Did not write " + fileName);
                }
                System.out.println("******************************");
              }
            } catch (Throwable te) {
              te.printStackTrace();
              throw te;
            }
            return null;
          });
        } else {
          System.out.println("Adding filename " + fileName);
          filenames.add(fileName);
        }
      }
    }

    executor.shutdown();
    while (executor.awaitTermination(60, TimeUnit.MINUTES)) {
      if (executor.isShutdown() || executor.isTerminated()) {
        break;
      }
      System.out.println("Still waiting...");
    }

    HashSet<String> resultingFileNames = new HashSet<>(filenames.stream().filter(fn -> {
      return new File(fn).exists();
    }).collect(Collectors.toSet()));

    // runWith(filenames, new Range());
    Key startKey = new Key(opts.beginRow);
    Key endKey = new Key(opts.endRow);
    runWith(resultingFileNames, new Range(startKey, true, endKey, true), opts.auths,
        opts.numberRuns, opts.exclusions);
    // runWith(filenames, new Range());
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    RFileTester tester = new RFileTester();

    tester.execute(args);

    System.out.println("complete....");
    System.exit(1);
  }

  public static final class RFileTestRun {

    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis();

    long getRuntime() {
      return endTime - startTime;
    }

  }

}
