/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.testing.performance.tests;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
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
import com.google.common.collect.Iterators;

public class YieldingScanExecutorPT implements PerformanceTest {

  private static final int NUM_SHORT_SCANS_THREADS = 5;
  private static final int NUM_LONG_SCANS = 50;

  private static final int NUM_ROWS = 100000;
  private static final int NUM_FAMS = 10;
  private static final int NUM_QUALS = 10;

  private static final String SCAN_EXECUTOR_THREADS = "2";

  private static final String TEST_DESC = "Scan Executor Test.  Test running lots of short scans "
      + "while many filters that return little data are running in the background.  If nothing is "
      + "done these filters will prevent the short scans from running.  This test configures "
      + "Accumulo so that the short scans should get a chance to run.  It does three things to "
      + "facilitate the short scans : the filters yield, there a two scan executors, and a scan "
      + "dispatcher sends long running scans to the second executor. If yielding and dispatching "
      + "are working correctly then the short scans should have very short response times.  This "
      + "happens because the filters should end up in a separate thread pool than the short scan.";

  private static final String FILTER_PROBABILITIES = "0.01,0.001,0.0001,0.00001,0.000001";
  private static final String FILTER_YIELD_TIME = "1000";

  private static final String QUICK_SCAN_TIME = "500";

  @Override
  public SystemConfiguration getSystemConfig() {
    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "200");
    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "200");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se1.threads",
        SCAN_EXECUTOR_THREADS);
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "se2.threads",
        SCAN_EXECUTOR_THREADS);
    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {

    String tableName = "scept";

    Map<String,String> props = new HashMap<>();
    // set up a scan dispatcher that send long runnning scans (> 500ms) to the second executor
    props.put(Property.TABLE_SCAN_DISPATCHER.getKey(), TimedScanDispatcher.class.getName());
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey() + "quick.executor", "se1");
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey() + "quick.time.ms", QUICK_SCAN_TIME);
    props.put(Property.TABLE_SCAN_DISPATCHER_OPTS.getKey() + "long.executor", "se2");

    env.getClient().tableOperations().create(tableName,
        new NewTableConfiguration().setProperties(props));

    long t1 = System.currentTimeMillis();
    TestData.generate(env.getClient(), tableName, NUM_ROWS, NUM_FAMS, NUM_QUALS);
    long t2 = System.currentTimeMillis();
    env.getClient().tableOperations().compact(tableName, null, null, true, true);
    long t3 = System.currentTimeMillis();

    AtomicBoolean stop = new AtomicBoolean(false);

    TestExecutor<Long> longScans = startLongScans(env, tableName, stop);

    LongSummaryStatistics shortStats1 = runShortScans(env, tableName, 50000);
    LongSummaryStatistics shortStats2 = runShortScans(env, tableName, 100000);

    stop.set(true);
    long t4 = System.currentTimeMillis();

    LongSummaryStatistics longStats = longScans.stream().mapToLong(l -> l).summaryStatistics();

    longScans.close();

    Report.Builder builder = Report.builder();

    builder.id("yfexec").description(TEST_DESC);
    builder.info("write", NUM_ROWS * NUM_FAMS * NUM_QUALS, t2 - t1, "Data write rate entries/sec ");
    builder.info("compact", NUM_ROWS * NUM_FAMS * NUM_QUALS, t3 - t2, "Compact rate entries/sec ");
    builder.info("short_times1", shortStats1, "Times in ms for each short scan.  First run.");
    builder.info("short_times2", shortStats2, "Times in ms for each short scan. Second run.");
    builder.result("short", shortStats2.getAverage(),
        "Average times in ms for short scans from 2nd run.");
    builder.info("long_counts", longStats, "Entries read by each of the filter threads");
    builder.info("long", longStats.getSum(), (t4 - t3),
        "Combined rate in entries/second of all long scans.  This should be low but non-zero.");
    builder.parameter("short_threads", NUM_SHORT_SCANS_THREADS, "Threads used to run short scans.");
    builder.parameter("long_threads", NUM_LONG_SCANS,
        "Threads running long fileter scans.  Each thread repeatedly scans entire table for "
            + "duration of test randomly returning a few of the keys.");
    builder.parameter("rows", NUM_ROWS, "Rows in test table");
    builder.parameter("familes", NUM_FAMS, "Families per row in test table");
    builder.parameter("qualifiers", NUM_QUALS, "Qualifiers per family in test table");
    builder.parameter("server_scan_threads", SCAN_EXECUTOR_THREADS,
        "Server side scan handler threads that each executor has.  There are 2 executors.");

    builder.parameter("filter_probabilities", FILTER_PROBABILITIES,
        "The chances that one of the long "
            + "filter scans will return any key it sees. The probabilites are cycled through when "
            + "starting long scans.");
    builder.parameter("filter_yield_time", FILTER_YIELD_TIME,
        "The time in ms after which one of " + "the long filter scans will yield.");
    builder.parameter("quick_scan_time", QUICK_SCAN_TIME, "The threshold time in ms for deciding "
        + "what is a quick vs long scan.  Times less than this are sent to one executor and longer "
        + "times are sent to another.");

    return builder.build();
  }

  private static long scan(String tableName, AccumuloClient c, byte[] row, byte[] fam)
      throws TableNotFoundException {
    long t1 = System.currentTimeMillis();
    int count = 0;
    try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(new Text(row), new Text(fam)));
      if (Iterables.size(scanner) != NUM_QUALS) {
        throw new RuntimeException("bad count " + count);
      }
    }

    return System.currentTimeMillis() - t1;
  }

  private long scan(String tableName, AccumuloClient c, AtomicBoolean stop,
      String filterProbability) throws TableNotFoundException {
    long count = 0;
    while (!stop.get()) {
      try (Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY)) {

        IteratorSetting is = new IteratorSetting(30, ProbabilityFilter.class);
        is.addOption("probability", filterProbability);
        is.addOption("yieldTimeMS", FILTER_YIELD_TIME);

        scanner.addScanIterator(is);

        // scanner.setExecutionHints(hints);
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

  private LongSummaryStatistics runShortScans(Environment env, String tableName, int numScans)
      throws InterruptedException, ExecutionException {

    try (TestExecutor<Long> executor = new TestExecutor<>(NUM_SHORT_SCANS_THREADS)) {
      Random rand = new Random();

      for (int i = 0; i < numScans; i++) {
        byte[] row = TestData.row(rand.nextInt(NUM_ROWS));
        byte[] fam = TestData.fam(rand.nextInt(NUM_FAMS));
        executor.submit(() -> scan(tableName, env.getClient(), row, fam));
      }

      return executor.stream().mapToLong(l -> l).summaryStatistics();
    }
  }

  private TestExecutor<Long> startLongScans(Environment env, String tableName, AtomicBoolean stop) {

    Iterator<String> fpi = Iterators.cycle(FILTER_PROBABILITIES.split(","));
    TestExecutor<Long> longScans = new TestExecutor<>(NUM_LONG_SCANS);

    for (int i = 0; i < NUM_LONG_SCANS; i++) {
      longScans.submit(() -> scan(tableName, env.getClient(), stop, fpi.next()));
    }
    return longScans;
  }
}
