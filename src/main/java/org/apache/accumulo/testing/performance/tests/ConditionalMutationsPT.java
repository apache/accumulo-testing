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

    conditionalMutationsTime(cw, 0);

    double rateSum = 0.0;
    for (long i = 1; i < 20; i++) {
      rateSum += conditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 1-19",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "ConditionalMutationsTest: average rate (conditions/sec) to run sequence 1-19");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0.0;
    for (long i = 20; i < 40; i++) {
      rateSum += conditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 20-39",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "ConditionalMutationsTest: average rate (conditions/sec)  to run sequence 20-39");
  }

  public static double conditionalMutationsTime(ConditionalWriter cw, long seq) throws Exception {

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      Condition cond = new Condition("meta", "seq");
      if (seq != 0) {
        cond.setValue("" + seq);
      }

      ConditionalMutation cm = new ConditionalMutation(String.format("r%07d", i), cond);
      cm.put("meta", "seq", seq == 0 ? "1" : (seq + 1) + "");
      cmuts.add(cm);
    }

    long t1 = System.nanoTime();

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

    long t2 = System.nanoTime();

    return 10000.0 / TimeUnit.NANOSECONDS.toSeconds(t2 - t1);
  }

  private static void runRandomizeConditionalMutationsTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);

    ConditionalWriter cw = env.getClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig());

    randomizeConditionalMutationsTime(cw, 0);

    double rateSum = 0;
    for (long i = 1; i < 20; i++) {
      rateSum += randomizeConditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 1-19",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeConditionalMutationsTest: average rate (conditions/sec)  to run sequence 1-19");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (long i = 20; i < 40; i++) {
      rateSum += randomizeConditionalMutationsTime(cw, i);
    }

    reportBuilder.result("avgRate: 20-39",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeConditionalMutationsTest: average rate (conditions/sec)  to run sequence 20-39");
  }

  private static double randomizeConditionalMutationsTime(ConditionalWriter cw, long seq)
      throws Exception {

    ArrayList<ConditionalMutation> cmuts = new ArrayList<>();
    ConditionalMutation cm = new ConditionalMutation("r01");
    ArrayList<Integer> ints = new ArrayList<>(10000);

    for (int i = 0; i < 10000; i++) {
      ints.add(i);
    }
    Collections.shuffle(ints);

    for (int i = 0; i < 10000; i++) {
      String qual = String.format("q%07d", ints.get(i));

      Condition cond = new Condition("seq", qual);
      if (seq != 0) {
        cond.setValue("" + seq);
      }

      cm.addCondition(cond);

      cm.put("seq", qual, seq == 0 ? "1" : (seq + 1) + "");
    }
    cmuts.add(cm);

    long t1 = System.nanoTime();

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

    long t2 = System.nanoTime();

    return 10000.0 / TimeUnit.NANOSECONDS.toSeconds(t2 - t1);
  }

  private static void runRandomizeBatchScanAndWriteTest(Environment env, String tableName,
      Report.Builder reportBuilder) throws Exception {

    try {
      env.getClient().tableOperations().delete(tableName);
    } catch (TableNotFoundException e) {}

    env.getClient().tableOperations().create(tableName);

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());
    BatchScanner bs = env.getClient().createBatchScanner(tableName, Authorizations.EMPTY, 1);

    randomizeBatchWriteAndScanTime(bw, bs, 0);

    double rateSum = 0;

    for (long i = 1; i < 20; i++) {
      rateSum += randomizeBatchWriteAndScanTime(bw, bs, i);
    }

    reportBuilder.result("avgRate: 1-19",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeBatchScanAndWriteTest: average rate (conditions/sec)  to write and scan sequence 1-19");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (long i = 20; i < 40; i++) {
      rateSum += randomizeBatchWriteAndScanTime(bw, bs, i);
    }

    reportBuilder.result("avgRate: 20-39",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "RandomizeBatchScanAndWriteTest: average rate (conditions/sec)  to write and scan sequence 20-39 post flush");
  }

  private static double randomizeBatchWriteAndScanTime(BatchWriter bw, BatchScanner bs, long seq)
      throws Exception {

    ArrayList<Range> ranges = new ArrayList<>();
    Mutation cm = new Mutation("r01");
    ArrayList<Integer> ints = new ArrayList<>(10000);

    for (int i = 0; i < 10000; i++) {
      ints.add(i);
    }
    Collections.shuffle(ints);

    for (int i = 0; i < 10000; i++) {
      String qual = String.format("q%07d", ints.get(i));
      cm.put("seq", qual, seq == 0 ? "1" : (seq + 1) + "");
      // look between existing values
      ranges.add(Range.exact("r01", "seq", qual + ".a"));
    }

    bw.addMutation(cm);
    bw.flush();

    long t1 = System.nanoTime();

    bs.setRanges(ranges);

    int count = Iterables.size(bs);
    if (0 != count) {
      throw new RuntimeException("count = " + count);
    }

    long t2 = System.nanoTime();

    return 10000.0 / TimeUnit.NANOSECONDS.toSeconds(t2 - t1);
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
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / numTest)),
        "SetBlockSizeTest: average rate in conditions/sec");

    env.getClient().tableOperations().flush(tableName, null, null, true);

    rateSum = 0;
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols);
    }

    reportBuilder.result("avgRate2",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / numTest)),
        "SetBlockSizeTest: average rate in conditions/sec post flush");

    env.getClient().tableOperations().compact(tableName, null, null, true, true);

    rateSum = 0;
    for (int i = 0; i < numTest; i++) {
      rateSum += setBlockSizeTime(cw, numRows, numCols);
    }

    reportBuilder.result("avgRate3",
        Double.parseDouble(new DecimalFormat("#0.00").format(rateSum / 20)),
        "SetBlockSizeTest: average rate in conditions/sec post compaction");
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
    String r = String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));
    Mutation m = new Mutation(r);

    for (int col = 0; col < numCols; col++) {
      String c = String.format("%04x",
          Math.abs(Hashing.murmur3_32().hashInt(col).asInt() & 0xffff));
      m.put("data", c, "1");
    }
    return m;
  }

  private static void writeLgData(Environment env, String tableName, int numRows)
      throws TableNotFoundException, MutationsRejectedException {

    BatchWriter bw = env.getClient().createBatchWriter(tableName, new BatchWriterConfig());
    numRows = numRows * 10;
    for (int row = 0; row < numRows; row++) {
      String r = String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));
      Mutation m = new Mutation(r);
      m.put("ntfy", "absfasf3", "");
      bw.addMutation(m);
    }
    bw.close();
  }

  private static String randRow(Random rand, int numRows) {
    int row = rand.nextInt(numRows);
    return String.format("%08x", Math.abs(Hashing.murmur3_32().hashInt(row).asInt()));
  }

  private static Collection<String> randCols(Random rand, int num, int numCols) {
    HashSet<String> cols = new HashSet<>();
    while (cols.size() < num) {
      int col = rand.nextInt(numCols);
      String c = String.format("%04x",
          Math.abs(Hashing.murmur3_32().hashInt(col).asInt() & 0xffff));
      cols.add(c);
    }
    return cols;
  }

  private static double setBlockSizeTime(ConditionalWriter cw, int numRows, int numCols)
      throws Exception {

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

    long t1 = System.nanoTime();

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

    long t2 = System.nanoTime();

    return 30000.0 / TimeUnit.NANOSECONDS.toSeconds(t2 - t1);
  }
}
