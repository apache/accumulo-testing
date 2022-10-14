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
package org.apache.accumulo.testing.performance.tests;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;

public class ConditionalMutationsPT implements PerformanceTest {

  private static final String conditionsPerSec = "conditions/sec";

  @Override
  public SystemConfiguration getSystemConfig() {
    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    String tableName = "foo";

    Report.Builder reportBuilder = Report.builder();
    reportBuilder.id("Conditional Mutations");
    reportBuilder.description("Runs conditional mutations tests with and without randomization,"
        + "setting of block size, and with and without randomization of batch writing/reading.");

    runConditionalMutationsTest(env, tableName, reportBuilder);
    runRandomizeConditionalMutationsTest(env, tableName, reportBuilder);
    runRandomizeBatchScanAndWriteTest(env, tableName, reportBuilder);
    runSetBlockSizeTest(env, tableName, reportBuilder);

    return reportBuilder.build();
  }

  private static void runConditionalMutationsTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    // warm-up run
    conditionalMutationsTime(cw, 0);

    double rateSum = 0.0;
    for (long i = 1; i <= 20; i++) {
      rateSum += conditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 1-20",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "ConditionalMutationsTest: average rate to run sequence 1-20");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0.0;
    for (long i = 21; i <= 40; i++) {
      rateSum += conditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 21-40",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "ConditionalMutationsTest: average rate to run sequence 21-40");
  }

  public static double conditionalMutationsTime(ConditionalWriter cw, long seq) throws Exception {

    final int numOfConditions = 10_000;

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    for (int i = 0; i < numOfConditions; i++) {
      Condition cond = new Condition("meta", "seq");
      if (seq != 0) {
        cond.setValue("" + seq);
      }

      ConditionalMutation cm = new ConditionalMutation(String.format("r%07d", i), cond);
      cm.put("meta", "seq", seq == 0 ? "1" : (seq + 1) + "");
      cmuts.add(cm);
    }

    long startTime = System.nanoTime();

    int count = 0;
    Iterator<ConditionalWriter.Result> results = cw.write(cmuts.iterator());
    while (results.hasNext()) {
      ConditionalWriter.Result result = results.next();

      if (ConditionalWriter.Status.ACCEPTED != result.getStatus()) {
        throw new RuntimeException();
      }
      count++;
    }

    if (cmuts.size() != count) {
      throw new RuntimeException();
    }

    long stopTime = System.nanoTime();

    // return number of conditions per second
    return numOfConditions / nanosToSecs(stopTime - startTime);
  }

  private static void runRandomizeConditionalMutationsTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    // warm-up run
    randomizeConditionalMutationsTime(cw, 0);

    double rateSum = 0;
    for (long i = 1; i <= 20; i++) {
      rateSum += randomizeConditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 1-20",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "RandomizeConditionalMutationsTest: average rate to run sequence 1-20");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (long i = 21; i <= 40; i++) {
      rateSum += randomizeConditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 21-40",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "RandomizeConditionalMutationsTest: average rate to run sequence 21-40");
  }

  private static double randomizeConditionalMutationsTime(ConditionalWriter cw, long seq)
      throws Exception {

    final int numOfConditions = 10_000;

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();
    ConditionalMutation cm = new ConditionalMutation("r01");
    ArrayList<Integer> ints = new ArrayList<>(numOfConditions);

    for (int i = 0; i < numOfConditions; i++) {
      ints.add(i);
    }
    Collections.shuffle(ints);

    for (int i = 0; i < numOfConditions; i++) {
      String qual = String.format("q%07d", ints.get(i));

      Condition cond = new Condition("seq", qual);
      if (seq != 0) {
        cond.setValue("" + seq);
      }

      cm.addCondition(cond);

      cm.put("seq", qual, seq == 0 ? "1" : (seq + 1) + "");
    }
    cmuts.add(cm);

    long startTime = System.nanoTime();

    int count = 0;
    Iterator<ConditionalWriter.Result> results = cw.write(cmuts.iterator());
    while (results.hasNext()) {
      ConditionalWriter.Result result = results.next();

      if (ConditionalWriter.Status.ACCEPTED != result.getStatus()) {
        throw new RuntimeException();
      }
      count++;
    }

    if (cmuts.size() != count) {
      throw new RuntimeException();
    }

    long stopTime = System.nanoTime();

    // return number of conditions per second
    return numOfConditions / nanosToSecs(stopTime - startTime);
  }

  private static void runRandomizeBatchScanAndWriteTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());
    BatchScanner bs = env.getClient().createBatchScanner(tableName, Authorizations.EMPTY, 1);

    // warm-up run
    randomizeBatchWriteAndScanTime(bw, bs, 0);

    double rateSum = 0;

    for (long i = 1; i <= 20; i++) {
      rateSum += randomizeBatchWriteAndScanTime(bw, bs, i);
    }

    reportBuilder.result("avgRate: 1-20",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "RandomizeBatchScanAndWriteTest: average rate to write and scan sequence 1-20");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (long i = 21; i <= 40; i++) {
      rateSum += randomizeBatchWriteAndScanTime(bw, bs, i);
    }

    reportBuilder.result("avgRate: 21-40",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "RandomizeBatchScanAndWriteTest: average rate to write and scan sequence 21-40 post flush");
  }

  private static double randomizeBatchWriteAndScanTime(BatchWriter bw, BatchScanner bs, long seq)
      throws Exception {

    final int numOfConditions = 10_000;

    ArrayList<Range> ranges = new ArrayList<>();
    Mutation cm = new Mutation("r01");
    ArrayList<Integer> ints = new ArrayList<>(numOfConditions);

    for (int i = 0; i < numOfConditions; i++) {
      ints.add(i);
    }
    Collections.shuffle(ints);

    for (int i = 0; i < numOfConditions; i++) {
      String qual = String.format("q%07d", ints.get(i));
      cm.put("seq", qual, seq == 0 ? "1" : (seq + 1) + "");
      // look between existing values
      ranges.add(Range.exact("r01", "seq", qual + ".a"));
    }

    bw.addMutation(cm);
    bw.flush();

    long startTime = System.nanoTime();

    bs.setRanges(ranges);

    int count = Iterables.size(bs);
    if (0 != count) {
      throw new RuntimeException("count = " + count);
    }

    long stopTime = System.nanoTime();

    // return number of conditions per second
    return numOfConditions / nanosToSecs(stopTime - startTime);
  }

  private static void runSetBlockSizeTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName, new NewTableConfiguration().setProperties(
        Collections.singletonMap(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "8K")));

    env.getClient().tableOperations().setLocalityGroups(tableName,
        ImmutableMap.of("lg1", ImmutableSet.of(new Text("ntfy"))));

    int numRows = 100000;
    int numCols = 100;
    int numTest = 10;

    writeData(env, tableName, numRows, numCols);
    writeLgData(env, tableName, numRows);

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    double rateSum = 0;
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols);
    }

    reportBuilder.result("avgRate1",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / numTest)), conditionsPerSec,
        "SetBlockSizeTest: average rate");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols);
    }

    reportBuilder.result("avgRate2",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / numTest)), conditionsPerSec,
        "SetBlockSizeTest: average rate post flush");

    env.getClient().tableOperations().compact(tableName, null, null, true, true);

    rateSum = 0;
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols);
    }

    reportBuilder.result("avgRate3",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)), conditionsPerSec,
        "SetBlockSizeTest: average rate post compaction");
    reportBuilder.parameter("numRows", numRows, "SetBlockSizeTest: The number of rows");
    reportBuilder.parameter("numCols", numCols, "SetBlockSizeTest: The number of columns");
    reportBuilder.parameter("numTest", numTest,
        "SetBlockSizeTest: The number of tests ran per trial");
  }

  private static void writeData(Environment env, String tableName, int numRows, int numCols)
      throws TableNotFoundException, MutationsRejectedException {

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());
    for (int row = 0; row < numRows; row++) {
      bw.addMutation(genRow(row, numCols));
    }
    bw.close();
  }

  private static Mutation genRow(int row, int numCols) {
    String r = String.format("%08x", Math.abs(Hashing.murmur3_32_fixed().hashInt(row).asInt()));
    Mutation m = new Mutation(r);

    for (int col = 0; col < numCols; col++) {
      String c = String.format("%04x",
          Math.abs(Hashing.murmur3_32_fixed().hashInt(col).asInt() & 0xffff));
      m.put("data", c, "1");
    }
    return m;
  }

  private static void writeLgData(Environment env, String tableName, int numRows)
      throws TableNotFoundException, MutationsRejectedException {

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());
    numRows = numRows * 10;
    for (int row = 0; row < numRows; row++) {
      String r = String.format("%08x", Math.abs(Hashing.murmur3_32_fixed().hashInt(row).asInt()));
      Mutation m = new Mutation(r);
      m.put("ntfy", "absfasf3", "");
      bw.addMutation(m);
    }
    bw.close();
  }

  private static String randRow(Random rand, int numRows) {
    int row = rand.nextInt(numRows);
    return String.format("%08x", Math.abs(Hashing.murmur3_32_fixed().hashInt(row).asInt()));
  }

  private static Collection<String> randCols(Random rand, int num, int numCols) {
    HashSet<String> cols = new HashSet<>();
    while (cols.size() < num) {
      int col = rand.nextInt(numCols);
      String c = String.format("%04x",
          Math.abs(Hashing.murmur3_32_fixed().hashInt(col).asInt() & 0xffff));
      cols.add(c);
    }
    return cols;
  }

  private static double setBlockSizeTime(ConditionalWriter cw, int numRows, int numCols)
      throws Exception {

    final int rows = 3000;
    final int cols = 10;
    final int numOfConditions = rows * cols;

    Random rand = new Random();
    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    for (int row = 0; row < rows; row++) {
      ConditionalMutation cm = new ConditionalMutation(randRow(rand, numRows));

      for (String col : randCols(rand, cols, numCols)) {
        cm.addCondition(new Condition("data", col).setValue("1"));
        cm.put("data", col, "1");
      }

      cmuts.add(cm);
    }

    long startTime = System.nanoTime();

    int count = 0;

    Iterator<ConditionalWriter.Result> results = cw.write(cmuts.iterator());
    while (results.hasNext()) {
      ConditionalWriter.Result result = results.next();

      if (ConditionalWriter.Status.ACCEPTED != result.getStatus()) {
        throw new RuntimeException();
      }
      count++;
    }

    if (cmuts.size() != count) {
      throw new RuntimeException();
    }

    long stopTime = System.nanoTime();

    // return number of conditions per second
    return numOfConditions / nanosToSecs(stopTime - startTime);
  }

  // Convert nanoseconds to seconds
  private static double nanosToSecs(long nanos) {
    double nanosPerSec = (double) TimeUnit.SECONDS.toNanos(1);
    return nanos / nanosPerSec;
  }
}
