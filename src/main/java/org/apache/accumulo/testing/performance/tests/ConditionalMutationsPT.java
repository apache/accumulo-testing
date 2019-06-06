package org.apache.accumulo.testing.performance.tests;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;

public class ConditionalMutationsPT implements PerformanceTest {

  @Override
  public SystemConfiguration getSystemConfig() {
    return new SystemConfiguration();
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    String tableName = "foo";

    Report.Builder reportBuilder = Report.builder();
    reportBuilder.id("condmut");
    reportBuilder.description("Runs conditional mutations tests with and without randomization," +
            "setting of block size, and with and without randomization of batch writing/reading.");

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
    env.getClient().tableOperations().setProperty(tableName,
        Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    conditionalMutationsTime(cw, null, "0", reportBuilder);

    double rateSum = 0.0;
    for (int i = 1; i < 20; i++) {
      rateSum += conditionalMutationsTime(cw, (long) i, ((Integer) i).toString(), reportBuilder);
    }

    reportBuilder.result("avgRate", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "ConditionalMutationsTest:  average rate (in seconds) to run sequence 1-19");

    // System.out.printf("rate avg : %6.2f conditionalMutations/sec \n", rateSum / 20);
    // System.out.println("Flushing");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0.0;
    for (int i = 20; i < 40; i++) {
      rateSum += conditionalMutationsTime(cw, (long) i, ((Integer) i).toString(), reportBuilder);
    }

    reportBuilder.result("avgRate", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "ConditionalMutationsTest:  average rate (in seconds) to run sequence 20-39");

    // System.out.printf("rate avg: %6.2f conditionalMutations/sec \n", rateSum / 20);

  }

  public static double conditionalMutationsTime(ConditionalWriter cw, Long seq, String testRun,
      Report.Builder reportBuilder) throws Exception {
    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      Condition cond = new Condition("meta", "seq");
      if (seq != null) {
        cond.setValue("" + seq);
      }

      ConditionalMutation cm = new ConditionalMutation(String.format("r%07d", i), cond);
      cm.put("meta", "seq", seq == null ? "1" : (seq + 1) + "");
      cmuts.add(cm);
    }

    long t1 = System.currentTimeMillis();

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
    long t2 = System.currentTimeMillis();

    double rate = 10000 / ((t2 - t1) / 1000.0);

    reportBuilder.info("time" + testRun, (t2 - t1), "ConditionalMutationsTest: time in seconds");
    reportBuilder.info("rate" + testRun, new Double(new DecimalFormat("#0.00").format(rate)),
        "ConditionalMutationsTest:  conditionalMutations/sec");

    // System.out.printf("time: %d ms rate : %6.2f conditionalMutations/sec \n", (t2 - t1), rate);

    return rate;
  }

  private static void runRandomizeConditionalMutationsTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);
    env.getClient().tableOperations().setProperty(tableName,
        Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    boolean randomize = true;

    randomizeConditionalMutationsTime(cw, null, "0", reportBuilder, randomize);

    double rateSum = 0;
    for (int i = 1; i < 20; i++) {
      rateSum += randomizeConditionalMutationsTime(cw, (long) i, ((Integer) i).toString(),
          reportBuilder, randomize);
    }

    reportBuilder.result("avgRate", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeConditionalMutationsTest:  average rate (in seconds) to run sequence 1-19");

    // System.out.printf("rate avg : %6.2f conditionalMutations/sec \n", rateSum / 20);
    // System.out.println("Flushing");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (int i = 20; i < 40; i++) {
      rateSum += randomizeConditionalMutationsTime(cw, (long) i, ((Integer) i).toString(),
          reportBuilder, randomize);
    }

    reportBuilder.result("avgRate", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeConditionalMutationsTest:  average rate (in seconds) to run sequence 20-39");

    // System.out.printf("rate avg: %6.2f conditionalMutations/sec \n", rateSum / 20);
  }

  private static double randomizeConditionalMutationsTime(ConditionalWriter cw, Long seq,
      String testRun, Report.Builder reportBuilder, boolean randomize) throws Exception {

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    ConditionalMutation cm = new ConditionalMutation("r01");

    ArrayList<Integer> ints = new ArrayList<>(10000);

    for (int i = 0; i < 10000; i++) {
      ints.add(i);
    }

    if (randomize) {
      Collections.shuffle(ints);
    }

    for (int i = 0; i < 10000; i++) {
      String qual = String.format("q%07d", ints.get(i));

      Condition cond = new Condition("seq", qual);
      if (seq != null) {
        cond.setValue("" + seq);
      }

      cm.addCondition(cond);

      cm.put("seq", qual, seq == null ? "1" : (seq + 1) + "");
    }

    cmuts.add(cm);

    long t1 = System.currentTimeMillis();

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
    long t2 = System.currentTimeMillis();

    double rate = 10000 / ((t2 - t1) / 1000.0);

    reportBuilder.info("time" + testRun, (t2 - t1),
        "RandomizeConditionalMutationsTest:  time in seconds");
    reportBuilder.info("rate" + testRun, new Double(new DecimalFormat("#0.00").format(rate)),
        "RandomizeConditionalMutationsTest:  conditionalMutations/sec");

    // System.out.printf("time: %d ms rate : %6.2f conditionalMutations/sec \n", (t2 - t1), rate);

    return rate;
  }

  private static void runRandomizeBatchScanAndWriteTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);
    env.getClient().tableOperations().setProperty(tableName,
        Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());
    BatchScanner bs = env.getClient().createBatchScanner(tableName, Authorizations.EMPTY, 1);

    boolean randomize = true;

    randomizeBatchWriteAndScanTime(bw, bs, null, "0", reportBuilder, randomize);

    double rateSum = 0;

    for (int i = 1; i < 20; i++) {
      rateSum += randomizeBatchWriteAndScanTime(bw, bs, (long) i, ((Integer) i).toString(),
          reportBuilder, randomize);
    }

    reportBuilder.result("avgRate", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeBatchScanAndWriteTest:  average rate (in seconds) to write and scan sequence 1-19");

    // System.out.printf("rate avg : %6.2f \n", rateSum / 20);
    // System.out.println("Flushing");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (int i = 20; i < 40; i++) {
      rateSum += randomizeBatchWriteAndScanTime(bw, bs, (long) i, ((Integer) i).toString(),
          reportBuilder, randomize);
    }

    reportBuilder.result("avgRate", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeBatchScanAndWriteTest:  average rate (in seconds) to write and scan sequence 20-39 post flush");

    // System.out.printf("rate avg : %6.2f \n", rateSum / 20);
  }

  private static double randomizeBatchWriteAndScanTime(BatchWriter bw, BatchScanner bs, Long seq,
      String testRun, Report.Builder reportBuilder, boolean randomize) throws Exception {

    ArrayList<Range> ranges = new ArrayList<>();

    Mutation cm = new Mutation("r01");

    ArrayList<Integer> ints = new ArrayList<>(10000);

    for (int i = 0; i < 10000; i++) {
      ints.add(i);
    }

    if (randomize) {
      Collections.shuffle(ints);
    }

    for (int i = 0; i < 10000; i++) {
      String qual = String.format("q%07d", ints.get(i));
      cm.put("seq", qual, seq == null ? "1" : (seq + 1) + "");
      // look between existing values
      ranges.add(Range.exact("r01", "seq", qual + ".a"));
    }

    long t1 = System.currentTimeMillis();
    bw.addMutation(cm);
    bw.flush();

    long t2 = System.currentTimeMillis();

    bs.setRanges(ranges);

    int count = 0;
    for (Map.Entry<Key,Value> entry : bs) {
      count++;
    }
    if (0 != count) {
      throw new RuntimeException("count = " + count);
    }

    long t3 = System.currentTimeMillis();

    double rate1 = 10000 / ((t2 - t1) / 1000.0);
    double rate2 = 10000 / ((t3 - t2) / 1000.0);

    reportBuilder.info("time" + testRun, (t2 - t1),
        "RandomizeBatchScanAndWriteTest: time in milliseconds to write and flush mutations");
    reportBuilder.info("writeRate" + testRun, new Double(new DecimalFormat("#0.00").format(rate1)),
        "RandomizeBatchScanAndWriteTest:  writes/sec");
    reportBuilder.info("readRate" + testRun, new Double(new DecimalFormat("#0.00").format(rate2)),
        "RandomizeBatchScanAndWriteTest:  reads/sec");

    // System.out.printf("time: %d ms rate : %6.2f writes/sec %6.2f reads/sec \n", (t2 - t1), rate1,
    // rate2);

    return rate2;
  }

  private static void runSetBlockSizeTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);
    env.getClient().tableOperations().setProperty(tableName,
        Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
    env.getClient().tableOperations().setProperty(tableName,
        Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "8K");
    env.getClient().tableOperations().setLocalityGroups(tableName,
        ImmutableMap.of("lg1", ImmutableSet.of(new Text("ntfy"))));

    int numRows = 100000;
    int numCols = 100;
    int numTest = 10;

    writeData(env, tableName, numRows, numCols, reportBuilder);
    writeLgData(env, tableName, numRows, reportBuilder);

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    double rateSum = 0;
    // String testRun = "_SetBlockSizeTest1";
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols, ((Integer) i).toString(), reportBuilder);
    }

    reportBuilder.result("avgRate1",
        new Double(new DecimalFormat("#0.00").format(rateSum / numTest)),
        "SetBlockSizeTest:  average rate in conditions/sec");

    // System.out.printf("rate avg : %6.2f conditions/sec \n", rateSum / numTest);
    // System.out.println("Flushing");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    // testRun = "_SetBlockSizeTest2";
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols, ((Integer) i).toString(), reportBuilder);
    }

    reportBuilder.result("avgRate2",
        new Double(new DecimalFormat("#0.00").format(rateSum / numTest)),
        "SetBlockSizeTest:  average rate in conditions/sec post flush");

    // System.out.printf("rate avg : %6.2f conditions/sec \n", rateSum / numTest);
    // System.out.println("Compacting");

    env.getClient().tableOperations().compact(tableName, null, null, true, true);

    rateSum = 0;
    // testRun = "_SetBlockSizeTest3";
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols, ((Integer) i).toString(), reportBuilder);
    }

    reportBuilder.result("avgRate3", new Double(new DecimalFormat("#0.00").format(rateSum / 20)),
        "SetBlockSizeTest:  average rate in conditions/sec post compaction");

    // System.out.printf("rate avg : %6.2f conditions/sec \n", rateSum / numTest);
  }

  private static void writeData(Environment env, String tableName, int numRows, int numCols,
      Report.Builder reportBuilder) throws TableNotFoundException, MutationsRejectedException {

    long t1 = System.currentTimeMillis();

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());

    for (int row = 0; row < numRows; row++) {
      bw.addMutation(genRow(row, numCols));
    }

    bw.close();
    long t2 = System.currentTimeMillis();

    double rate = numRows * numCols / ((t2 - t1) / 1000.0);

    reportBuilder.info("time", (t2 - t1),
        "WriteData_SetBlockSizeTest:  time (in seconds) to write the entries");
    reportBuilder.info("rate", new Double(new DecimalFormat("#0.00").format(rate)),
        "WriteData_SetBlockSizeTest:  entries/sec");

    // System.out.printf("time: %d ms rate : %6.2f entries/sec written\n", (t2 - t1), rate);
  }

  private static Mutation genRow(int row, int numCols) {
    String r = String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));

    Mutation m = new Mutation(r);

    for (int col = 0; col < numCols; col++) {
      String c = String.format("%04x",
          Math.abs(Hashing.murmur3_32().hashInt(col).asInt() & 0xffff));
      m.put("data", c, "1");
    }

    return m;
  }

  private static void writeLgData(Environment env, String tableName, int numRows,
      Report.Builder reportBuilder) throws TableNotFoundException, MutationsRejectedException {

    String testName = "WriteLgDataForSetBlockSizeTest";

    long t1 = System.currentTimeMillis();

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());

    numRows = numRows * 10;

    for (int row = 0; row < numRows; row++) {
      String r = String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));

      Mutation m = new Mutation(r);
      m.put("ntfy", "absfasf3", "");
      bw.addMutation(m);
    }

    bw.close();
    long t2 = System.currentTimeMillis();

    double rate = numRows / ((t2 - t1) / 1000.0);

    reportBuilder.info("time", (t2 - t1),
        "WriteLgData_SetBlockSizeTest:  time (in seconds) to write the entries");
    reportBuilder.info("rate", new Double(new DecimalFormat("#0.00").format(rate)),
        "WriteLgData_SetBlockSizeTest:  entries/sec");

    // System.out.printf("time: %d ms rate : %6.2f entries/sec written\n", (t2 - t1), rate);
  }

  private static String randRow(Random rand, int numRows) {
    int row = rand.nextInt(numRows);
    return String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));
  }

  private static Collection<String> randCols(Random rand, int num, int numCols) {
    HashSet<String> cols = new HashSet<String>();
    while (cols.size() < num) {
      int col = rand.nextInt(numCols);
      String c = String.format("%04x",
          Math.abs(Hashing.murmur3_32().hashInt(col).asInt() & 0xffff));
      cols.add(c);
    }

    return cols;
  }

  private static double setBlockSizeTime(ConditionalWriter cw, int numRows, int numCols,
      String testRun, Report.Builder reportBuilder) throws Exception {

    Random rand = new Random();

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    for (int row = 0; row < 3000; row++) {
      ConditionalMutation cm = new ConditionalMutation(randRow(rand, numRows));

      for (String col : randCols(rand, 10, numCols)) {
        cm.addCondition(new Condition("data", col).setValue("1"));
        cm.put("data", col, "1");
      }

      cmuts.add(cm);
    }

    long t1 = System.currentTimeMillis();

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
    long t2 = System.currentTimeMillis();

    double rate = 30000 / ((t2 - t1) / 1000.0);

    reportBuilder.info("time" + testRun, (t2 - t1),
        "SetBlockSizeTest:  time (in seconds) to write conditions");
    reportBuilder.info("rate" + testRun, new Double(new DecimalFormat("#0.00").format(rate)),
        "SetBlockSizeTest:  conditions/sec");

    // System.out.printf("time: %d ms rate : %6.2f conditions/sec \n", (t2 - t1), rate);

    return rate;
  }
}
