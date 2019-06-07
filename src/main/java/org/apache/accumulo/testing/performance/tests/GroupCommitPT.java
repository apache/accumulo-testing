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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;

public class GroupCommitPT implements PerformanceTest {

  static Mutation createRandomMutation(Random rand) {
    byte row[] = new byte[16];

    rand.nextBytes(row);

    Mutation m = new Mutation(row);

    byte cq[] = new byte[8];
    byte val[] = new byte[16];

    for (int i = 0; i < 3; i++) {
      rand.nextBytes(cq);
      rand.nextBytes(val);
      m.put("cf".getBytes(), cq, val);
    }

    return m;
  }

  static class WriteTask implements Runnable {

    private int numToWrite;
    private int numToBatch;
    private BatchWriter bw;
    private volatile long time = -1;

    WriteTask(BatchWriter bw, int numToWrite, int numToBatch) throws Exception {
      this.bw = bw;
      this.numToWrite = numToWrite;
      this.numToBatch = numToBatch;
    }

    @Override
    public void run() {
      Random rand = new Random();

      try {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < numToWrite; i++) {
          Mutation mut = createRandomMutation(rand);

          for (int j = 0; j < numToBatch; j++) {
            bw.addMutation(mut);
          }

          bw.flush();
        }

        // bw.flush();

        long t2 = System.currentTimeMillis();
        this.time = t2 - t1;
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          e.printStackTrace();
        }

      }
    }

    long getTime() {
      return time;
    }
  }

  @Override
  public SystemConfiguration getSystemConfig() {

    Map<String,String> siteCfg = new HashMap<>();

    siteCfg.put(Property.TSERV_MINTHREADS.getKey(), "128");

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    boolean walog = true;

    int tests[] = new int[] {1, 2, 8, 16, 32, 128};

    Report.Builder report = Report.builder();

    report.id("mutslam");
    report.description("Runs multiple threads to test performance of a group commit. "
        + " This tests threads with client side group commit, using a single batch writer");

    int testRun = 0;
    for (int i = 0; i < 6; i++) {
      // This test threads w/ group commit on the client side, using a single batch writer.
      // Each thread flushes after each mutation
      for (int numThreads : tests) {
        runBatch(env, report, numThreads, 1, true, walog, testRun);
        testRun++;
      }

      // This test threads w/ group commit on the server side, using a batch writer per thread.
      // Each thread flushes after each mutation.
      for (int numThreads : tests) {
        runBatch(env, report, numThreads, 1, false, walog, testRun);
        testRun++;
      }

      // This test a single thread write a different batch sizes of mutations, flushing after each
      // batch.
      // Group commit should approach these times for the same number mutations.
      for (int numToBatch : tests) {
        runBatch(env, report, 1, numToBatch, false, walog, testRun);
        testRun++;
      }

      walog = !walog;
    }

    return report.build();
  }

  private void runBatch(Environment env, Report.Builder report, int numThreads, int numToBatch,
      boolean useSharedBW, boolean walog, int testRun) throws Exception {

    String tableName = "mutslam";

    try {
      if (!walog) {
        env.getClient().tableOperations().create(tableName, new NewTableConfiguration()
            .setProperties(Collections.singletonMap(Property.TABLE_DURABILITY.getKey(), "none")));
      } else {
        env.getClient().tableOperations().create(tableName, new NewTableConfiguration()
            .setProperties(Collections.singletonMap(Property.TABLE_DURABILITY.getKey(), "log")));
      }
    } catch (TableExistsException tee) {}

    // scan just to wait for tablet be online
    Scanner scanner = env.getClient().createScanner(tableName, Authorizations.EMPTY);
    for (Map.Entry<Key,Value> entry : scanner) {
      entry.getValue();
    }

    // number of batches each thread should write
    int numToWrite = 100;

    ArrayList<WriteTask> wasks = new ArrayList<WriteTask>();
    ArrayList<Thread> threads = new ArrayList<Thread>();

    BatchWriter bw = null;
    SharedBatchWriter sbw = null;

    if (useSharedBW) {
      bw = env.getClient().createBatchWriter(tableName,
          new BatchWriterConfig().setMaxWriteThreads(1));
      sbw = new SharedBatchWriter(bw);
    }

    for (int i = 0; i < numThreads; i++) {
      WriteTask wask;

      if (useSharedBW) {
        wask = new WriteTask(sbw, numToWrite, numToBatch);
      } else {
        wask = new WriteTask(env.getClient().createBatchWriter(tableName,
            new BatchWriterConfig().setMaxWriteThreads(1)), numToWrite, numToBatch);
      }

      wasks.add(wask);
      Thread thread = new Thread(wask);
      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    if (useSharedBW)
      bw.close();

    long sum = 0;
    for (WriteTask writeTask : wasks) {
      sum += writeTask.getTime();
    }

    int totalNumMutations = numToWrite * numThreads * numToBatch;

    double rate = totalNumMutations / (sum / (double) wasks.size());

    report.info("time" + testRun, sum / (double) wasks.size(),
        "Time it took the task to run in milliseconds");
    report.info("threads" + testRun, numThreads, "Number of threads");
    report.info("batch" + testRun, numToBatch, "Number of batches");
    report.info("mutations" + testRun, totalNumMutations, "Total number of mutations");
    report.result("rate" + testRun, rate, "Mutations per millisecond");

    System.out.printf(
        "\ttime: %8.2f #threads: %3d #batch: %2d #mutations: %4d rate: %6.2f mutations/ms\n",
        sum / (double) wasks.size(), numThreads, numToBatch, totalNumMutations, rate);

    env.getClient().tableOperations().delete(tableName);
  }

}
