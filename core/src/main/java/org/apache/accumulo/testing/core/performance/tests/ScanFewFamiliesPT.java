package org.apache.accumulo.testing.core.performance.tests;

import java.util.HashSet;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.performance.Environment;
import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.accumulo.testing.core.performance.Report;
import org.apache.accumulo.testing.core.performance.SystemConfiguration;
import org.apache.accumulo.testing.core.performance.util.TestData;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

public class ScanFewFamiliesPT implements PerformanceTest {

  private static final String DESC = "This test times fetching a few column famlies when rows have many column families.";

  private static final int NUM_ROWS = 500;
  private static final int NUM_FAMS = 10000;
  private static final int NUM_QUALS = 1;

  @Override
  public SystemConfiguration getConfiguration() {
    return new SystemConfiguration();
  }

  @Override
  public Report runTest(Environment env) throws Exception {

    String tableName = "bigFamily";

    env.getConnector().tableOperations().create(tableName);

    long t1 = System.currentTimeMillis();
    TestData.generate(env.getConnector(), tableName, NUM_ROWS, NUM_FAMS, NUM_QUALS);
    long t2 = System.currentTimeMillis();
    env.getConnector().tableOperations().compact(tableName, null, null, true, true);
    long t3 = System.currentTimeMillis();
    // warm up run
    runScans(env, tableName, 1);

    Report.Builder builder = Report.builder();

    for (int numFams : new int[] {1, 2, 4, 8, 16}) {
      LongSummaryStatistics stats = runScans(env, tableName, numFams);
      String fams = Strings.padStart(numFams + "", 2, '0');
      builder.info("f" + fams + "_stats", stats, "Times in ms to fetch " + numFams + " families from all rows");
      builder.result("f" + fams, stats.getAverage(), "Average time in ms to fetch " + numFams + " families from all rows");
    }

    builder.id("sfewfam");
    builder.description(DESC);
    builder.info("write", NUM_ROWS * NUM_FAMS * NUM_QUALS, t2 - t1, "Data write rate entries/sec ");
    builder.info("compact", NUM_ROWS * NUM_FAMS * NUM_QUALS, t3 - t2, "Compact rate entries/sec ");
    builder.parameter("rows", NUM_ROWS, "Rows in test table");
    builder.parameter("familes", NUM_FAMS, "Families per row in test table");
    builder.parameter("qualifiers", NUM_QUALS, "Qualifiers per family in test table");

    return builder.build();
  }

  private LongSummaryStatistics runScans(Environment env, String tableName, int numFamilies) throws TableNotFoundException {
    Random rand = new Random();
    LongSummaryStatistics stats = new LongSummaryStatistics();
    for (int i = 0; i < 50; i++) {
      stats.accept(scan(tableName, env.getConnector(), rand, numFamilies));
    }
    return stats;
  }

  private static long scan(String tableName, Connector c, Random rand, int numFamilies) throws TableNotFoundException {

    Set<Text> families = new HashSet<>(numFamilies);
    while(families.size() < numFamilies) {
      families.add(new Text(TestData.fam(rand.nextInt(NUM_FAMS))));
    }

    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      families.forEach(scanner::fetchColumnFamily);
      if (Iterables.size(scanner) != NUM_ROWS * NUM_QUALS * numFamilies) {
        throw new RuntimeException("bad count " + count);
      }
    }

    return System.currentTimeMillis() - t1;
  }
}
