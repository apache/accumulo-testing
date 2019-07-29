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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HerdingPT implements PerformanceTest {

  private static final Logger LOG = LoggerFactory.getLogger(HerdingPT.class);

  private static final byte[] COL_FAM = "pinky".getBytes();

  private static final int NUM_ROWS = 1_000_000;
  private static final int NUM_COLS = 10;
  private static final int NUM_THREADS = 32;
  private static final int NUM_LOAD_ATTEMPTS = 1_000;

  private static final String TABLE_NAME = "herd";
  private static final String DESCRIPTION = "Test herding performance with " + NUM_THREADS
      + " threads attempting to " + "simultaneously load the same block " + NUM_LOAD_ATTEMPTS
      + " times.";

  private final Random random = new Random();

  @Override
  public SystemConfiguration getSystemConfig() {
    Map<String,String> config = new HashMap<>();

    config.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    return new SystemConfiguration().setAccumuloConfig(config);
  }

  @Override
  public Report runTest(final Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder().id("herdingPT").description(DESCRIPTION)
        .parameter("table_name", TABLE_NAME, "The name of the table.")
        .parameter("num_threads", NUM_THREADS, "The number of threads.")
        .parameter("num_load_attempts", NUM_LOAD_ATTEMPTS,
            "The number of times the threads will attempt to load the same block.")
        .parameter("num_rows", NUM_ROWS, "The number of rows that will be loaded into the table.");

    final AccumuloClient client = env.getClient();
    initTable(client);
    long herdTime = getHerdingDuration(client);
    reportBuilder.result("herd_time", herdTime, "The time (in ms) it took herding to complete.");

    return reportBuilder.build();
  }

  private void initTable(final AccumuloClient client) throws TableExistsException,
      AccumuloSecurityException, AccumuloException, TableNotFoundException {
    client.tableOperations().create(TABLE_NAME);
    writeEntries(client);
    client.tableOperations().flush(TABLE_NAME, null, null, true);
  }

  private void writeEntries(final AccumuloClient client)
      throws TableNotFoundException, MutationsRejectedException {
    try (BatchWriter bw = client.createBatchWriter(TABLE_NAME)) {
      for (int row = 0; row < NUM_ROWS; row++) {
        bw.addMutation(createMutation(row));
      }
    }
  }

  private Mutation createMutation(final int rowNum) {
    byte[] row = toZeroPaddedString(rowNum, 8);
    Mutation mutation = new Mutation(row);
    for (int col = 0; col < NUM_COLS; col++) {
      byte[] qualifer = toZeroPaddedString(col, 4);
      byte[] value = new byte[32];
      random.nextBytes(value);
      mutation.put(COL_FAM, qualifer, value);
    }
    return mutation;
  }

  private long getHerdingDuration(final AccumuloClient client)
      throws ExecutionException, InterruptedException {
    final ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
    long start = System.currentTimeMillis();
    final List<Future<?>> futures = addThreadsToPool(pool, client);
    for (Future<?> future : futures) {
      future.get();
    }
    long duration = System.currentTimeMillis() - start;
    pool.shutdown();
    return duration;
  }

  private List<Future<?>> addThreadsToPool(final ExecutorService pool,
      final AccumuloClient client) {
    final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(pool.submit(createThread(client, barrier)));
    }
    return futures;
  }

  private Runnable createThread(final AccumuloClient client, final CyclicBarrier barrier) {
    return () -> {
      try {
        Scanner scanner = client.createScanner(TABLE_NAME, Authorizations.EMPTY);
        for (int numAttempt = 0; numAttempt < NUM_LOAD_ATTEMPTS; numAttempt++) {
          barrier.await();
          byte[] row = toZeroPaddedString(numAttempt * NUM_LOAD_ATTEMPTS, 8);
          scanner.setRange(Range.exact(new String(row)));
          // Scan each entry in the range, but don't do anything with them.
          scanner.forEach(e -> {});
        }
      } catch (Exception e) {
        LOG.error("Error occurred during scan", e);
      }
    };
  }

  private byte[] toZeroPaddedString(final long num, final int width) {
    return new byte[Math.max(Long.toString(num, 16).length(), width)];
  }
}
