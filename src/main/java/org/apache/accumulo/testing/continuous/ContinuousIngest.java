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
package org.apache.accumulo.testing.continuous;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.testing.TestProps;
import org.apache.accumulo.testing.util.FastFormat;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class ContinuousIngest {

  private static final Logger log = LoggerFactory.getLogger(ContinuousIngest.class);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static List<ColumnVisibility> visibilities;
  private static long lastPauseNs;
  private static long pauseWaitSec;
  private static boolean pauseEnabled;
  private static int pauseMin;
  private static int pauseMax;

  private static boolean zipfianEnabled;
  private static int minSize;
  private static int maxSize;
  private static double exponent;

  private static RandomDataGenerator rnd;

  public interface RandomGeneratorFactory extends Supplier<LongSupplier> {
    static RandomGeneratorFactory create(ContinuousEnv env, AccumuloClient client,
        Supplier<SortedSet<Text>> splitSupplier, Random random) {
      final long rowMin = env.getRowMin();
      final long rowMax = env.getRowMax();
      Properties testProps = env.getTestProperties();
      final int maxTablets =
          Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_TABLETS));

      if (maxTablets == Integer.MAX_VALUE) {
        return new MinMaxRandomGeneratorFactory(rowMin, rowMax, random);
      } else {
        return new MaxTabletsRandomGeneratorFactory(rowMin, rowMax, maxTablets, splitSupplier,
            random);
      }
    }
  }

  public static class MinMaxRandomGeneratorFactory implements RandomGeneratorFactory {
    private final LongSupplier generator;

    public MinMaxRandomGeneratorFactory(long rowMin, long rowMax, Random random) {
      Preconditions.checkState(0 <= rowMin && rowMin <= rowMax,
          "Bad rowMin/rowMax, must conform to: 0 <= rowMin <= rowMax");
      generator = () -> ContinuousIngest.genLong(rowMin, rowMax, random);
    }

    @Override
    public LongSupplier get() {
      return generator;
    }
  }

  /**
   * Chooses X random tablets and only generates random rows that fall within those tablets.
   */
  public static class MaxTabletsRandomGeneratorFactory implements RandomGeneratorFactory {
    private final int maxTablets;
    private final Supplier<SortedSet<Text>> splitSupplier;
    private final Random random;
    private final long minRow;
    private final long maxRow;

    public MaxTabletsRandomGeneratorFactory(long minRow, long maxRow, int maxTablets,
        Supplier<SortedSet<Text>> splitSupplier, Random random) {
      // writing to a single tablet does not make much sense because this test it predicated on
      // having rows in tablets point to rows in other tablet to detect errors
      Preconditions.checkState(maxTablets > 1, "max tablets config must be > 1");
      this.maxTablets = maxTablets;
      this.splitSupplier = splitSupplier;
      this.random = random;
      this.minRow = minRow;
      this.maxRow = maxRow;
    }

    @Override
    public LongSupplier get() {
      var splits = splitSupplier.get();
      if (splits.size() < maxTablets) {
        // There are less tablets so generate within the entire range
        return new MinMaxRandomGeneratorFactory(minRow, maxRow, random).get();
      } else {
        long prev = minRow;
        List<LongSupplier> allGenerators = new ArrayList<>(splits.size() + 1);
        for (var split : splits) {
          // splits are derived from inspecting rfile indexes and rfile indexes can shorten rows
          // introducing non-hex chars so need to handle non-hex chars in the splits
          // TODO this handling may not be correct, but it will not introduce errors but may cause
          // writing a small amount of data to an extra tablet.
          byte[] bytes = split.copyBytes();
          int len = bytes.length;
          int last = bytes.length - 1;
          if (bytes[last] < '0') {
            len = last;
          } else if (bytes[last] > '9' && bytes[last] < 'a') {
            bytes[last] = '9';
          } else if (bytes[last] > 'f') {
            bytes[last] = 'f';
          }

          var splitStr = new String(bytes, 0, len, UTF_8);
          var splitNum = Long.parseLong(splitStr, 16) << (64 - splitStr.length() * 4);
          allGenerators.add(new MinMaxRandomGeneratorFactory(prev, splitNum, random).get());
          prev = splitNum;
        }
        allGenerators.add(new MinMaxRandomGeneratorFactory(prev, maxRow, random).get());

        Collections.shuffle(allGenerators, random);
        var generators = List.copyOf(allGenerators.subList(0, maxTablets));

        return () -> {
          // pick a generator for random tablet
          var generator = generators.get(random.nextInt(generators.size()));
          // pick a random long that falls within that tablet
          return generator.getAsLong();
        };
      }
    }
  }

  public interface BatchWriterFactory {
    BatchWriter create(String tableName) throws TableNotFoundException;

    static BatchWriterFactory create(AccumuloClient client, ContinuousEnv env,
        Supplier<SortedSet<Text>> splitSupplier) {
      Properties testProps = env.getTestProperties();
      final String bulkWorkDir = testProps.getProperty(TestProps.CI_INGEST_BULK_WORK_DIR);
      if (bulkWorkDir == null || bulkWorkDir.isBlank()) {
        return client::createBatchWriter;
      } else {
        try {
          var conf = new Configuration();
          var workDir = new Path(bulkWorkDir);
          var filesystem = workDir.getFileSystem(conf);
          var memLimit = Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_BULK_MEM_LIMIT));
          return tableName -> new FlakyBulkBatchWriter(client, tableName, filesystem, workDir,
              memLimit, splitSupplier);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  private static ColumnVisibility getVisibility(Random rand) {
    return visibilities.get(rand.nextInt(visibilities.size()));
  }

  private static boolean pauseEnabled(Properties props) {
    String value = props.getProperty(TestProps.CI_INGEST_PAUSE_ENABLED);
    return Boolean.parseBoolean(value);
  }

  private static int getPause(Random rand) {
    if (pauseMax == pauseMin) {
      return pauseMin;
    }
    return (rand.nextInt(pauseMax - pauseMin) + pauseMin);
  }

  private static float getDeleteProbability(Properties props) {
    String stringValue = props.getProperty(TestProps.CI_INGEST_DELETE_PROBABILITY);
    float prob = Float.parseFloat(stringValue);
    Preconditions.checkArgument(prob >= 0.0 && prob <= 1.0,
        "Delete probability should be between 0.0 and 1.0");
    return prob;
  }

  private static int getFlushEntries(Properties props) {
    return Integer.parseInt(props.getProperty(TestProps.CI_INGEST_FLUSH_ENTRIES, "1000000"));
  }

  private static void pauseCheck(Random rand) throws InterruptedException {
    if (pauseEnabled) {
      long elapsedNano = System.nanoTime() - lastPauseNs;
      if (elapsedNano > (TimeUnit.SECONDS.toNanos(pauseWaitSec))) {
        long pauseDurationSec = getPause(rand);
        log.info("PAUSING for {}s", pauseDurationSec);
        Thread.sleep(TimeUnit.SECONDS.toMillis(pauseDurationSec));
        lastPauseNs = System.nanoTime();
        pauseWaitSec = getPause(rand);
        log.info("INGESTING for {}s", pauseWaitSec);
      }
    }
  }

  static Supplier<SortedSet<Text>> createSplitSupplier(AccumuloClient client, String tableName) {

    Supplier<SortedSet<Text>> splitSupplier = Suppliers.memoizeWithExpiration(() -> {
      try {
        var splits = client.tableOperations().listSplits(tableName);
        return new TreeSet<>(splits);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }

    }, 10, TimeUnit.MINUTES);
    return splitSupplier;
  }

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {

      AccumuloClient client = env.getAccumuloClient();

      String tableName = env.getAccumuloTableName();
      Properties testProps = env.getTestProperties();
      final int maxColF = env.getMaxColF();
      final int maxColQ = env.getMaxColQ();
      Random random = env.getRandom();
      final long numEntries =
          Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_CLIENT_ENTRIES));
      final boolean checksum =
          Boolean.parseBoolean(testProps.getProperty(TestProps.CI_INGEST_CHECKSUM));

      var splitSupplier = createSplitSupplier(client, tableName);
      var randomFactory = RandomGeneratorFactory.create(env, client, splitSupplier, random);
      var batchWriterFactory = BatchWriterFactory.create(client, env, splitSupplier);
      doIngest(client, randomFactory, batchWriterFactory, tableName, testProps, maxColF, maxColQ,
          numEntries, checksum, random);
    }
  }

  protected static void doIngest(AccumuloClient client, RandomGeneratorFactory randomFactory,
      BatchWriterFactory batchWriterFactory, String tableName, Properties testProps, int maxColF,
      int maxColQ, long numEntries, boolean checksum, Random random)
      throws TableNotFoundException, MutationsRejectedException, InterruptedException {

    if (!client.tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName,
          "Consult the README and create the table before starting ingest.");
    }

    byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes(UTF_8);
    log.info("Ingest instance ID: {} current time: {}ms", new String(ingestInstanceId, UTF_8),
        System.currentTimeMillis());

    long entriesWritten = 0L;
    long entriesDeleted = 0L;
    final int flushInterval = getFlushEntries(testProps);
    log.info("A flush will occur after every {} entries written", flushInterval);
    final int maxDepth = 25;

    // always want to point back to flushed data. This way the previous item should
    // always exist in accumulo when verifying data. To do this make insert N point
    // back to the row from insert (N - flushInterval). The array below is used to keep
    // track of all inserts.
    MutationInfo[][] nodeMap = new MutationInfo[maxDepth][flushInterval];

    long lastFlushTime = System.currentTimeMillis();

    log.info("Total entries to be written: {}", numEntries);

    visibilities = parseVisibilities(testProps.getProperty(TestProps.CI_INGEST_VISIBILITIES));

    pauseEnabled = pauseEnabled(testProps);

    pauseMin = Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_PAUSE_WAIT_MIN));
    pauseMax = Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_PAUSE_WAIT_MAX));
    Preconditions.checkState(0 < pauseMin && pauseMin <= pauseMax,
        "Bad pause wait min/max, must conform to: 0 < min <= max");

    if (pauseEnabled) {
      lastPauseNs = System.nanoTime();
      pauseWaitSec = getPause(random);
      log.info("PAUSING enabled");
      log.info("INGESTING for {}s", pauseWaitSec);
    }

    final float deleteProbability = getDeleteProbability(testProps);
    log.info("DELETES will occur with a probability of {}",
        String.format("%.02f", deleteProbability));

    try (BatchWriter bw = batchWriterFactory.create(tableName)) {
      zipfianEnabled =
          Boolean.parseBoolean(testProps.getProperty("test.ci.ingest.zipfian.enabled"));

      if (zipfianEnabled) {
        minSize = Integer.parseInt(testProps.getProperty("test.ci.ingest.zipfian.min.size"));
        maxSize = Integer.parseInt(testProps.getProperty("test.ci.ingest.zipfian.max.size"));
        exponent = Double.parseDouble(testProps.getProperty("test.ci.ingest.zipfian.exponent"));
        rnd = new RandomDataGenerator();

        log.info("Zipfian distribution enabled with min size: {}, max size: {}, exponent: {}",
            minSize, maxSize, exponent);
      }

      out: while (true) {
        ColumnVisibility cv = getVisibility(random);

        // generate sets nodes that link to previous set of nodes
        for (int depth = 0; depth < maxDepth; depth++) {
          // use the same random generator for each flush interval
          LongSupplier randomRowGenerator = randomFactory.get();
          for (int index = 0; index < flushInterval; index++) {
            long rowLong = randomRowGenerator.getAsLong();

            byte[] prevRow = depth == 0 ? null : genRow(nodeMap[depth - 1][index].row);

            int cfInt = random.nextInt(maxColF);
            int cqInt = random.nextInt(maxColQ);

            nodeMap[depth][index] = new MutationInfo(rowLong, cfInt, cqInt);
            Mutation m = genMutation(rowLong, cfInt, cqInt, cv, ingestInstanceId, entriesWritten,
                prevRow, checksum);
            entriesWritten++;
            bw.addMutation(m);
          }

          lastFlushTime = flush(bw, entriesWritten, entriesDeleted, lastFlushTime);
          if (entriesWritten >= numEntries)
            break out;
          pauseCheck(random);
        }

        // random chance that the entries will be deleted
        final boolean delete = random.nextFloat() < deleteProbability;

        // if the previously written entries are scheduled to be deleted
        if (delete) {
          log.info("Deleting last portion of written entries");
          // add delete mutations in the reverse order in which they were written
          for (int depth = nodeMap.length - 1; depth >= 0; depth--) {
            for (int index = nodeMap[depth].length - 1; index >= 0; index--) {
              MutationInfo currentNode = nodeMap[depth][index];
              Mutation m = new Mutation(genRow(currentNode.row));
              m.putDelete(genCol(currentNode.cf), genCol(currentNode.cq));
              entriesDeleted++;
              bw.addMutation(m);
            }
            lastFlushTime = flush(bw, entriesWritten, entriesDeleted, lastFlushTime);
            pauseCheck(random);
          }
        } else {
          // create one big linked list, this makes all the first inserts point to something
          for (int index = 0; index < flushInterval - 1; index++) {
            MutationInfo firstEntry = nodeMap[0][index];
            MutationInfo lastEntry = nodeMap[maxDepth - 1][index + 1];
            Mutation m = genMutation(firstEntry.row, firstEntry.cf, firstEntry.cq, cv,
                ingestInstanceId, entriesWritten, genRow(lastEntry.row), checksum);
            entriesWritten++;
            bw.addMutation(m);
          }
          lastFlushTime = flush(bw, entriesWritten, entriesDeleted, lastFlushTime);
        }

        if (entriesWritten >= numEntries)
          break out;
        pauseCheck(random);
      }
    }
  }

  private static class MutationInfo {

    long row;
    int cf;
    int cq;

    public MutationInfo(long row, int cf, int cq) {
      this.row = row;
      this.cf = cf;
      this.cq = cq;
    }
  }

  public static List<ColumnVisibility> parseVisibilities(String visString) {
    List<ColumnVisibility> vis;
    if (visString == null) {
      vis = Collections.singletonList(new ColumnVisibility());
    } else {
      vis = new ArrayList<>();
      for (String v : visString.split(",")) {
        vis.add(new ColumnVisibility(v.trim()));
      }
    }
    return vis;
  }

  private static long flush(BatchWriter bw, long entriesWritten, long entriesDeleted,
      long lastFlushTime) throws MutationsRejectedException {
    long t1 = System.currentTimeMillis();
    bw.flush();
    long t2 = System.currentTimeMillis();
    log.info("FLUSH - duration: {}ms, since last flush: {}ms, total written: {}, total deleted: {}",
        (t2 - t1), (t2 - lastFlushTime), entriesWritten, entriesDeleted);
    return t2;
  }

  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv,
      byte[] ingestInstanceId, long entriesWritten, byte[] prevRow, boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not
    // good for small data.... so used CRC32 instead
    CRC32 cksum = null;

    byte[] rowString = genRow(rowLong);

    byte[] cfString = genCol(cfInt);
    byte[] cqString = genCol(cqInt);

    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
      cksum.update(cv.getExpression());
    }

    Mutation m = new Mutation(rowString);

    m.put(cfString, cqString, cv, createValue(ingestInstanceId, entriesWritten, prevRow, cksum));
    return m;
  }

  public static byte[] genCol(int cfInt) {
    return FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
  }

  /**
   * Generates a random long within the range [min,max)
   */
  public static long genLong(long min, long max, Random r) {
    return ((r.nextLong() & 0x7fffffffffffffffL) % (max - min)) + min;
  }

  static byte[] genRow(long min, long max, Random r) {
    return genRow(genLong(min, max, r));
  }

  public static byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }

  public static byte[] createValue(byte[] ingestInstanceId, long entriesWritten, byte[] prevRow,
      Checksum cksum) {
    final int numOfSeparators = 4;
    int dataLen =
        ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + numOfSeparators;
    if (cksum != null)
      dataLen += 8;

    int zipfLength = 0;
    if (zipfianEnabled) {
      // add the length of the zipfian data to the value
      int range = maxSize - minSize;
      zipfLength = rnd.nextZipf(range, exponent) + minSize;
      dataLen += zipfLength;
    }

    byte[] val = new byte[dataLen];

    // add the ingest instance id to the value
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;

    val[index++] = ':';

    // add the count of entries written to the value
    int added = FastFormat.toZeroPaddedString(val, index, entriesWritten, 16, 16, EMPTY_BYTES);
    if (added != 16)
      throw new RuntimeException(" " + added);
    index += 16;

    val[index++] = ':';

    // add the previous row to the value
    if (prevRow != null) {
      System.arraycopy(prevRow, 0, val, index, prevRow.length);
      index += prevRow.length;
    }

    val[index++] = ':';

    if (zipfianEnabled) {
      // add random data to the value of length zipfLength
      for (int i = 0; i < zipfLength; i++) {
        val[index++] = (byte) rnd.nextInt(0, 256);
      }

      val[index++] = ':';
    }

    // add the checksum to the value
    if (cksum != null) {
      cksum.update(val, 0, index);
      cksum.getValue();
      FastFormat.toZeroPaddedString(val, index, cksum.getValue(), 8, 16, EMPTY_BYTES);
    }
    return val;
  }
}
