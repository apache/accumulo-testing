package org.apache.accumulo.testing.performance.tests;

import java.util.Arrays;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;

public class DurabilityWriteSpeedPT implements PerformanceTest {
  static final long N = 100000;

  @Override
  public SystemConfiguration getSystemConfig() {
    return new SystemConfiguration();
  }

  private String[] init(AccumuloClient c) throws Exception {
    String[] tableNames = getUniqueNames(4);
    TableOperations tableOps = c.tableOperations();
    createTable(c, tableNames[0]);
    createTable(c, tableNames[1]);
    createTable(c, tableNames[2]);
    createTable(c, tableNames[3]);
    // default is sync
    tableOps.setProperty(tableNames[1], Property.TABLE_DURABILITY.getKey(), "flush");
    tableOps.setProperty(tableNames[2], Property.TABLE_DURABILITY.getKey(), "log");
    tableOps.setProperty(tableNames[3], Property.TABLE_DURABILITY.getKey(), "none");
    return tableNames;
  }

  private void createTable(AccumuloClient c, String tableName) throws Exception {
    c.tableOperations().create(tableName);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder();
    reportBuilder.id("durability");
    reportBuilder.description("Compares writes speeds at different durability levels");
    try (AccumuloClient client = env.getClient()) {
      TableOperations tableOps = client.tableOperations();
      String[] tableNames = init(client);
      // write some gunk, delete the table to keep that table from messing with the performance
      // numbers of successive calls
      // sync
      long t0 = writeSome(reportBuilder, client, tableNames[0], N, "Sync");
      tableOps.delete(tableNames[0]);
      // flush
      long t1 = writeSome(reportBuilder, client, tableNames[1], N, "Flush");
      tableOps.delete(tableNames[1]);
      // log
      long t2 = writeSome(reportBuilder, client, tableNames[2], N, "Log");
      tableOps.delete(tableNames[2]);
      // none
      long t3 = writeSome(reportBuilder, client, tableNames[3], N, "No durability");
      tableOps.delete(tableNames[3]);

      reportBuilder.result("Sync Median", t0, "Median time result for sync");
      reportBuilder.result("Flush Median", t1, "Median time result for flush");
      reportBuilder.result("Log Median", t2, "Median time result for log");
      reportBuilder.result("No Durability Median", t3,
          "Median time result for no durability level");
      reportBuilder.parameter("rows", N, "Number of random rows written.");
    }

    return reportBuilder.build();
  }

  private long writeSome(Report.Builder reportBuilder, AccumuloClient c, String table, long count,
      String durabilityLevel) throws Exception {
    int iterations = 5;
    long[] attempts = new long[iterations];
    for (int attempt = 0; attempt < iterations; attempt++) {
      long now = System.currentTimeMillis();
      try (BatchWriter bw = c.createBatchWriter(table)) {
        for (int i = 1; i < count + 1; i++) {
          Mutation m = new Mutation("" + i);
          m.put("", "", "");
          bw.addMutation(m);
          if (i % (Math.max(1, count / 100)) == 0) {
            bw.flush();
          }
        }
      }
      attempts[attempt] = System.currentTimeMillis() - now;
      reportBuilder.info(durabilityLevel + " attempt " + attempt, System.currentTimeMillis() - now,
          "Times for each attempt in ms");
    }
    Arrays.sort(attempts);

    // Return the median duration
    return attempts[2];
  }

  private String[] getUniqueNames(int num) {
    String[] names = new String[num];

    for (int i = 0; i < num; ++i) {
      names[i] = this.getClass().getSimpleName() + "_" + i;
    }

    return names;
  }
}
