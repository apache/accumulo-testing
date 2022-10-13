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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class GroupCommitPT implements PerformanceTest {

  private static final int NUM_MUTATIONS = 2048 * 1024;
  private static final int NUM_FLUSHES = 1024;

  static Mutation createRandomMutation(Random rand) {
    byte[] row = new byte[16];

    rand.nextBytes(row);

    Mutation m = new Mutation(row);

    byte[] cq = new byte[8];
    byte[] val = new byte[16];

    for (int i = 0; i < 3; i++) {
      rand.nextBytes(cq);
      rand.nextBytes(val);
      m.put("cf".getBytes(), cq, val);
    }

    return m;
  }

  static class WriteTask implements Runnable {

    private final int batchSize;
    private final BatchWriter bw;
    private volatile int written = 0;

    WriteTask(BatchWriter bw, int numMutations) throws Exception {
      Preconditions.checkArgument(numMutations >= NUM_FLUSHES);
      Preconditions.checkArgument(numMutations % NUM_FLUSHES == 0);
      this.bw = bw;
      this.batchSize = numMutations / NUM_FLUSHES;
    }

    @Override
    public void run() {
      Random rand = new Random();

      int written = 0;

      try {
        for (int i = 0; i < NUM_FLUSHES; i++) {
          Mutation mut = createRandomMutation(rand);

          for (int j = 0; j < batchSize; j++) {
            bw.addMutation(mut);
            written++;
          }

          bw.flush();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          e.printStackTrace();
        }

      }

      this.written = written;
    }

    public int getWritten() {
      return written;
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

    Report.Builder report = Report.builder();
    report.id("mutslam");
    report.description("Data written to Accumulo is appended to a write ahead log before its made "
        + "available for scan.  There is a single write ahead log per tablet server.  Data from "
        + "multiple concurrent clients is batched together and appended to the write ahead log, "
        + "this is called group commit.  If group commit is not working properly, then performance"
        + " of concurrent writes could suffer.  This performance test measures group commit.  In "
        + "an Accumulo client, when the batch writer is flushed this forces an append to the write"
        + " ahead log.  The batch writer flush call does not return until the append is complete. "
        + "This test writes the same amount of data using different numbers of threads to check if"
        + " group commit is working properly.  When the test is using one thread it will write "
        + "2048K total mutations calling flush on the batchwriter 1024 times.  When"
        + " the test is running two threads, each thread will write 1024K mutations calling "
        + "flush 1024 times.  The pattern is that as the number of threads increases"
        + ", the amount of data written per thread decreases proportionally.  However the number of"
        + " flushes done by threads is constant.  If group commit is working well, then the overall"
        + " write rate should not be significantly less as the number of threads increases.");

    report.parameter("num_mutations", NUM_MUTATIONS,
        "The total number of mutations each test will write.  Each thread in a test will write num_mutations/num_threads_in_test mutations.");
    report.parameter("num_flushes", NUM_FLUSHES,
        "The number of times each thread will flush its batch writer.  The flushes are spread evenly between mutations.");

    // number of threads to run for each test
    int[] tests = new int[] {1, 2, 4, 8, 16, 32, 64};

    // run warm up test
    for (int numThreads : tests) {
      runTest(report, env, numThreads, true);
    }

    // run real test
    for (int numThreads : tests) {
      runTest(report, env, numThreads, false);
    }

    return report.build();
  }

  private void runTest(Report.Builder report, Environment env, int numThreads, boolean warmup)
      throws Exception {

    Preconditions.checkArgument(NUM_MUTATIONS % numThreads == 0);

    // presplit tablet to allow more concurrency to tablet in memory map updates, so this does not
    // impede write ahead log appends.
    NewTableConfiguration ntc = new NewTableConfiguration();
    SortedSet<Text> splits = new TreeSet<>();
    for (int s = 16; s < 256; s += 16) {
      splits.add(new Text(new byte[] {(byte) s}));
    }
    ntc.withSplits(splits);

    String tableName = "mutslam";
    env.getClient().tableOperations().create(tableName, ntc);

    // scan just to wait for tablet be online
    Scanner scanner = env.getClient().createScanner(tableName, Authorizations.EMPTY);
    for (Map.Entry<Key,Value> entry : scanner) {
      entry.getValue();
    }

    int mutationsPerThread = NUM_MUTATIONS / numThreads;

    ArrayList<WriteTask> wasks = new ArrayList<WriteTask>();
    ArrayList<Thread> threads = new ArrayList<Thread>();

    for (int i = 0; i < numThreads; i++) {
      WriteTask wask = new WriteTask(env.getClient().createBatchWriter(tableName,
          new BatchWriterConfig().setMaxWriteThreads(1)), mutationsPerThread);

      wasks.add(wask);
      Thread thread = new Thread(wask);
      threads.add(thread);
    }

    long t1 = System.currentTimeMillis();

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    long t2 = System.currentTimeMillis();

    // ensure all thread wrote the expected number of mutations
    Preconditions.checkState(wasks.stream().mapToInt(WriteTask::getWritten).sum() == NUM_MUTATIONS);

    env.getClient().tableOperations().delete(tableName);
    if (warmup) {
      report.info("warmup_rate_" + numThreads, NUM_MUTATIONS, t2 - t1, "mutations/sec",
          "The warmup rate at which " + numThreads + " threads wrote data.");
    } else {
      report.result("rate_" + numThreads, NUM_MUTATIONS, t2 - t1, "mutations/sec",
          "The rate at which " + numThreads + " threads wrote data.");
    }
  }

}
