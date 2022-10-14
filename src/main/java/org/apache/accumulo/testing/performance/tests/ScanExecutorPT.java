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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.accumulo.testing.performance.util.TestData;
import org.apache.accumulo.testing.performance.util.TestExecutor;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

public class ScanExecutorPT implements PerformanceTest {

  private static final int NUM_SHORT_SCANS_THREADS = 5;
  private static final int NUM_LONG_SCANS = 50;

  private static final int NUM_ROWS = 10000;
  private static final int NUM_FAMS = 10;
  private static final int NUM_QUALS = 10;

  private static final String SCAN_EXECUTOR_THREADS = "2";
  private static final String SCAN_PRIORITIZER = "org.apache.accumulo.core.spi.scan.HintScanPrioritizer";

  private static final String TEST_DESC = "Scan Executor Test. Test running lots of short scans "
      + "while long scans are running in the background. Each short scan reads a random row and "
      + "family. Using execution hints, short scans are randomly either given a high priority or "
      + "a dedicated executor. If the scan prioritizer or dispatcher is not working properly, "
      + "then the short scans will be orders of magnitude slower.";

  @Override
  public SystemConfiguration getSystemConfig() {
    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "200");
    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "200");

    final String tservSEprefix = Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey();

    siteCfg.put(tservSEprefix + "se1.threads", SCAN_EXECUTOR_THREADS);
    siteCfg.put(tservSEprefix + "se1.prioritizer", SCAN_PRIORITIZER);
    siteCfg.put(tservSEprefix + "se1.prioritizer.opts.priority.short", "1");
    siteCfg.put(tservSEprefix + "se1.prioritizer.opts.priority.long", "2");

    siteCfg.put(tservSEprefix + "se2.threads", SCAN_EXECUTOR_THREADS);
    siteCfg.put(tservSEprefix + "se2.prioritizer", SCAN_PRIORITIZER);

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {

    AccumuloClient client = env.getClient();

    final String tableName = "scept";

    Map<String,String> props = new HashMap<>();
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey() + "executor", "se1");
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey() + "executor.dedicated", "se2");
    props.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

    long t1 = System.currentTimeMillis();
    TestData.generate(client, tableName, NUM_ROWS, NUM_FAMS, NUM_QUALS);
    long t2 = System.currentTimeMillis();
    client.tableOperations().compact(tableName, null, null, true, true);
    long t3 = System.currentTimeMillis();

    AtomicBoolean stop = new AtomicBoolean(false);

    TestExecutor<Long> longScans = startLongScans(client, tableName, stop);

    LongSummaryStatistics shortStats1 = runShortScans(client, tableName, 50000);
    LongSummaryStatistics shortStats2 = runShortScans(client, tableName, 100000);

    stop.set(true);
    long t4 = System.currentTimeMillis();

    LongSummaryStatistics longStats = longScans.stream().mapToLong(l -> l).summaryStatistics();

    longScans.close();

    Report.Builder builder = Report.builder();

    final String ms = TimeUnit.MILLISECONDS.toString();

    builder.id("sexec").description(TEST_DESC);
    builder.info("write_rate", NUM_ROWS * NUM_FAMS * NUM_QUALS, t2 - t1, "entries/sec",
        "Data write rate");
    builder.info("compact_rate", NUM_ROWS * NUM_FAMS * NUM_QUALS, t3 - t2, "entries/sec",
        "Compact rate");
    builder.info("short_times1", shortStats1, ms, "Duration of each short scan from first run.");
    builder.info("short_times2", shortStats2, ms, "Duration of each short scan from second run.");
    builder.result("short_average", shortStats2.getAverage(), ms,
        "Average duration of short scans from second run.");
    builder.info("long_counts", longStats, "entries read",
        "Entries read by each long scan threads");
    builder.info("long_rate", longStats.getSum(), (t4 - t3), "entries/sec",
        "Combined rate of all long scans");
    builder.parameter("short_threads", NUM_SHORT_SCANS_THREADS, "Threads used to run short scans.");
    builder.parameter("long_threads", NUM_LONG_SCANS,
        "Threads running long scans. Each thread repeatedly scans entire table for duration of test.");
    builder.parameter("rows", NUM_ROWS, "Rows in test table");
    builder.parameter("families", NUM_FAMS, "Families per row in test table");
    builder.parameter("qualifiers", NUM_QUALS, "Qualifiers per family in test table");
    builder.parameter("server_scan_threads", SCAN_EXECUTOR_THREADS,
        "Server side scan handler threads");
    builder.parameter("prioritizer", SCAN_PRIORITIZER, "Server side scan prioritizer");

    return builder.build();
  }

  private static long scan(String tableName, AccumuloClient c, byte[] row, byte[] fam,
      Map<String,String> hints) throws TableNotFoundException {
    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setExecutionHints(hints);
      scanner.setRange(Range.exact(new Text(row), new Text(fam)));
      if (Iterables.size(scanner) != NUM_QUALS) {
        throw new RuntimeException("bad count " + count);
      }
    }

    return System.currentTimeMillis() - t1;
  }

  private long scan(String tableName, AccumuloClient c, AtomicBoolean stop,
      Map<String,String> hints) throws TableNotFoundException {
    long count = 0;
    while (!stop.get()) {
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setExecutionHints(hints);
        for (Iterator<Entry<Key,Value>> iter = scanner.iterator(); iter.hasNext(); iter.next()) {
          count++;
          if (stop.get()) {
            return count;
          }
        }
      }
    }
    return count;
  }

  private LongSummaryStatistics runShortScans(AccumuloClient client, String tableName,
      int numScans) {

    Map<String,String> execHints = Map.of("scan_type", "dedicated");
    Map<String,String> prioHints = Map.of("scan_type", "short");

    try (TestExecutor<Long> executor = new TestExecutor<>(NUM_SHORT_SCANS_THREADS)) {
      Random rand = new Random();

      for (int i = 0; i < numScans; i++) {
        byte[] row = TestData.row(rand.nextInt(NUM_ROWS));
        byte[] fam = TestData.fam(rand.nextInt(NUM_FAMS));
        // scans have a 20% chance of getting dedicated thread pool and 80% chance of getting high
        // priority
        Map<String,String> hints = rand.nextInt(10) <= 1 ? execHints : prioHints;
        executor.submit(() -> scan(tableName, client, row, fam, hints));
      }

      return executor.stream().mapToLong(l -> l).summaryStatistics();
    }
  }

  private TestExecutor<Long> startLongScans(AccumuloClient client, String tableName,
      AtomicBoolean stop) {
    Map<String,String> hints = Map.of("scan_type", "long");

    TestExecutor<Long> longScans = new TestExecutor<>(NUM_LONG_SCANS);

    for (int i = 0; i < NUM_LONG_SCANS; i++) {
      longScans.submit(() -> scan(tableName, client, stop, hints));
    }
    return longScans;
  }
}
