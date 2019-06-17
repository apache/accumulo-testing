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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
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
    siteCfg.put(Property.TABLE_DURABILITY.getKey(), "sync");

    return new SystemConfiguration().setAccumuloConfig(siteCfg);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    int numThreads = 128;

    Report.Builder report = Report.builder();
    report.id("mutslam");
    report.description("Runs multiple threads to test performance of a group commit. "
        + " This tests threads with client side group commit, using a single batch writer");

    double batchValues;

    for (int i = 0; i < 6; i++) {
      // This test threads w/ group commit on the client side, using a single batch writer.
      // Each thread flushes after each mutation.
      batchValues = runBatch(env, numThreads, 1);

      report.info("threadsTime" + i, new Double(new DecimalFormat("#0.00").format(batchValues)),
          "Time it took the task to run in milliseconds");
      report.info("threads" + i, i, "Number of threads");
      report.info("batch" + i, 1, "Number of batches");
    }

    for (int i = 0; i < 6; i++) {
      // This tests a single thread writing a different batch sizes of mutations,
      // flushing after each batch. Group commit should approach these times for the same number
      // mutations.

      batchValues = runBatch(env, 1, numThreads);

      report.info("batchTime" + i, new Double(new DecimalFormat("#0.00").format(batchValues)),
          "Time it took the task to run in milliseconds");
      report.info("threads" + i, i, "Number of threads");
      report.info("batch" + i, 1, "Number of batches");
    }

    return report.build();
  }

  private double runBatch(Environment env, int numThreads, int numToBatch) throws Exception {

    String tableName = "mutslam";
    env.getClient().tableOperations().create(tableName);

    // scan just to wait for tablet be online
    Scanner scanner = env.getClient().createScanner(tableName, Authorizations.EMPTY);
    for (Map.Entry<Key,Value> entry : scanner) {
      entry.getValue();
    }

    // number of batches each thread should write
    int numToWrite = 100;

    ArrayList<WriteTask> wasks = new ArrayList<WriteTask>();
    ArrayList<Thread> threads = new ArrayList<Thread>();

    for (int i = 0; i < numThreads; i++) {
      WriteTask wask = new WriteTask(env.getClient().createBatchWriter(tableName,
          new BatchWriterConfig().setMaxWriteThreads(1)), numToWrite, numToBatch);

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

    long sum = 0;
    for (WriteTask writeTask : wasks) {
      sum += writeTask.getTime();
    }

    // System.out.printf(
    // "\ttime: %8.2f #threads: %3d #batch: %2d #mutations: %4d rate: %6.2f mutations/ms\n",
    // sum / (double) wasks.size(), numThreads, numToBatch, totalNumMutations,
    // totalNumMutations / (sum / (double) wasks.size()));

    env.getClient().tableOperations().delete(tableName);

    return sum / (double) wasks.size(); // time
  }

}
