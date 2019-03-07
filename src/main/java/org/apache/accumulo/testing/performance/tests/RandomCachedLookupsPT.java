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

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

public class RandomCachedLookupsPT implements PerformanceTest {

  private static final int NUM_LOOKUPS_PER_THREAD = 25000;
  private static final int NUM_ROWS = 100000;

  @Override
  public SystemConfiguration getSystemConfig() {
    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "1000");
    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "256");
    siteCfg.put(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS.getKey(), "32");
    siteCfg.put(Property.TABLE_DURABILITY.getKey(), "flush");
    siteCfg.put(Property.TSERV_DATACACHE_SIZE.getKey(), "2G");
    siteCfg.put(Property.TSERV_INDEXCACHE_SIZE.getKey(), "1G");

    // TODO it would be good if this could request a minimum amount of tserver memory

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder();

    writeData(reportBuilder, env.getClient(), NUM_ROWS);

    long warmup = doLookups(env.getClient(), 32, NUM_LOOKUPS_PER_THREAD);

    long d1 = doLookups(env.getClient(), 1, NUM_LOOKUPS_PER_THREAD);
    long d4 = doLookups(env.getClient(), 4, NUM_LOOKUPS_PER_THREAD);
    long d8 = doLookups(env.getClient(), 8, NUM_LOOKUPS_PER_THREAD);
    long d16 = doLookups(env.getClient(), 16, NUM_LOOKUPS_PER_THREAD);
    long d32 = doLookups(env.getClient(), 32, NUM_LOOKUPS_PER_THREAD);
    long d64 = doLookups(env.getClient(), 64, NUM_LOOKUPS_PER_THREAD);
    long d128 = doLookups(env.getClient(), 128, NUM_LOOKUPS_PER_THREAD);

    reportBuilder.id("smalls");
    reportBuilder.description(
        "Runs multiple threads each doing lots of small random scans.  For this test data and index cache are enabled.");
    reportBuilder.info("warmup", 32 * NUM_LOOKUPS_PER_THREAD, warmup,
        "Random lookup per sec for 32 threads");
    reportBuilder.info("lookups_1", NUM_LOOKUPS_PER_THREAD, d1,
        "Random lookup per sec rate for 1 thread");
    reportBuilder.info("lookups_4", 4 * NUM_LOOKUPS_PER_THREAD, d4,
        "Random lookup per sec rate for 4 threads");
    reportBuilder.info("lookups_8", 8 * NUM_LOOKUPS_PER_THREAD, d8,
        "Random lookup per sec rate for 8 threads");
    reportBuilder.info("lookups_16", 16 * NUM_LOOKUPS_PER_THREAD, d16,
        "Random lookup per sec rate for 16 threads");
    reportBuilder.info("lookups_32", 32 * NUM_LOOKUPS_PER_THREAD, d32,
        "Random lookup per sec rate for 32 threads");
    reportBuilder.info("lookups_64", 64 * NUM_LOOKUPS_PER_THREAD, d64,
        "Random lookup per sec rate for 64 threads");
    reportBuilder.info("lookups_128", 128 * NUM_LOOKUPS_PER_THREAD, d128,
        "Random lookup per sec rate for 128 threads");

    reportBuilder.result("avg_1", d1 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 1 thread");
    reportBuilder.result("avg_4", d4 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 4 threads");
    reportBuilder.result("avg_8", d8 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 8 threads");
    reportBuilder.result("avg_16", d16 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 16 threads");
    reportBuilder.result("avg_32", d32 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 32 threads");
    reportBuilder.result("avg_64", d64 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 64 threads");
    reportBuilder.result("avg_128", d128 / (double) NUM_LOOKUPS_PER_THREAD,
        "Average milliseconds per lookup for 128 threads");

    return reportBuilder.build();
  }

  public static void writeData(Report.Builder reportBuilder, AccumuloClient client, int numRows)
      throws Exception {

    reportBuilder.parameter("rows", numRows,
        "Number of random rows written.  Each row has 4 columns.");

    NewTableConfiguration ntc = new NewTableConfiguration();
    Map<String,String> props = new HashMap<>();
    props.put("table.file.compress.blocksize.index", "256K");
    props.put("table.file.compress.blocksize", "8K");
    props.put("table.cache.index.enable", "true");
    props.put("table.cache.block.enable", "true");
    ntc.setProperties(props);

    long t1 = System.currentTimeMillis();
    try {
      client.tableOperations().create("scanpt", ntc);
    } catch (TableExistsException tee) {
      client.tableOperations().delete("scanpt");
      client.tableOperations().create("scanpt", ntc);
    }

    long t2 = System.currentTimeMillis();

    SortedSet<Text> partitionKeys = new TreeSet<>(
        Stream.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f")
            .map(Text::new).collect(toList()));
    client.tableOperations().addSplits("scanpt", partitionKeys);

    long t3 = System.currentTimeMillis();

    BatchWriter bw = client.createBatchWriter("scanpt", new BatchWriterConfig());

    Random rand = new Random();

    for (int i = 0; i < numRows; i++) {
      Mutation m = new Mutation(toHex(rand.nextLong()));
      int c1 = rand.nextInt(1 << 10);
      int c2 = rand.nextInt(1 << 10);
      while (c1 == c2)
        c2 = rand.nextInt(1 << 10);
      int c3 = rand.nextInt(1 << 10);
      while (c1 == c3 || c2 == c3)
        c3 = rand.nextInt(1 << 10);
      int c4 = rand.nextInt(1 << 10);
      while (c1 == c4 || c2 == c4 || c3 == c4)
        c4 = rand.nextInt(1 << 10);
      m.put("fam1", toHex(c1, 3), toHex(rand.nextLong()));
      m.put("fam1", toHex(c2, 3), toHex(rand.nextLong()));
      m.put("fam1", toHex(c3, 3), toHex(rand.nextLong()));
      m.put("fam1", toHex(c4, 3), toHex(rand.nextLong()));
      bw.addMutation(m);
    }

    bw.close();

    long t4 = System.currentTimeMillis();

    client.tableOperations().compact("scanpt", new CompactionConfig().setFlush(true).setWait(true));

    long t5 = System.currentTimeMillis();

    try (Scanner scanner = client.createScanner("scanpt", Authorizations.EMPTY)) {
      // scan entire table to bring it into cache
      Iterables.size(scanner);
    }

    long t6 = System.currentTimeMillis();

    reportBuilder.info("create", t2 - t1, "Time to create table in ms");
    reportBuilder.info("split", t3 - t2, "Time to split table in ms");
    reportBuilder.info("write", 4 * numRows, t4 - t3, "Rate to write data in entries/sec");
    reportBuilder.info("compact", 4 * numRows, t5 - t4, "Rate to compact table in entries/sec");
    reportBuilder.info("fullScan", 4 * numRows, t6 - t5,
        "Rate to do full table scan in entries/sec");
  }

  private static long doLookups(AccumuloClient client, int numThreads, int numScansPerThread)
      throws Exception {

    ExecutorService es = Executors.newFixedThreadPool(numThreads);

    List<Future<?>> futures = new ArrayList<>(numThreads);

    long t1 = System.currentTimeMillis();

    for (int i = 0; i < numThreads; i++) {
      futures.add(es.submit(() -> doLookups(client, numScansPerThread)));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    long t2 = System.currentTimeMillis();

    es.shutdown();

    return t2 - t1;
  }

  private static void doLookups(AccumuloClient client, int numScans) {
    try {
      Random rand = new Random();

      for (int i = 0; i < numScans; i++) {
        Scanner scanner = client.createScanner("scanpt", Authorizations.EMPTY);

        scanner.setRange(new Range(toHex(rand.nextLong())));

        Iterables.size(scanner);

        scanner.close();
      }
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static String toHex(long l) {
    String s = Long.toHexString(l);
    return Strings.padStart(s, 16, '0');
  }

  public static String toHex(int i) {
    String s = Integer.toHexString(i);
    return Strings.padStart(s, 8, '0');
  }

  public static String toHex(int i, int len) {
    String s = Integer.toHexString(i);
    return Strings.padStart(s, len, '0');
  }
}
