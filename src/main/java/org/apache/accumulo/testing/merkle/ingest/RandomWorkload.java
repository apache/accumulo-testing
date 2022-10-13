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
package org.apache.accumulo.testing.merkle.ingest;

import java.util.Random;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.cli.ClientOpts;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Generates some random data with a given percent of updates to be deletes.
 */
public class RandomWorkload {

  public static class RandomWorkloadOpts extends ClientOpts {

    @Parameter(names = {"-t", "--table"}, description = "table to use")
    String tableName = "randomWorkload";

    @Parameter(names = {"-n", "--num"}, required = true, description = "Num records to write")
    public long numRecords;

    @Parameter(names = {"-r", "--rows"}, required = true,
        description = "Range of rows that can be generated")
    public int rowMax;

    @Parameter(names = {"-cf", "--colfams"}, required = true,
        description = "Range of column families that can be generated")
    public int cfMax;

    @Parameter(names = {"-cq", "--colquals"}, required = true,
        description = "Range of column qualifiers that can be generated")
    public int cqMax;

    @Parameter(names = {"-d", "--deletes"}, required = false,
        description = "Percentage of updates that should be deletes")
    public int deletePercent = 5;
  }

  public void run(RandomWorkloadOpts opts) throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      run(client, opts.tableName, opts.numRecords, opts.rowMax, opts.cfMax, opts.cqMax,
          opts.deletePercent);
    }
  }

  public void run(final AccumuloClient client, final String tableName, final long numRecords,
      int rowMax, int cfMax, int cqMax, int deletePercent) throws Exception {

    final Random rowRand = new Random(12345);
    final Random cfRand = new Random(12346);
    final Random cqRand = new Random(12347);
    final Random deleteRand = new Random(12348);
    long valueCounter = 0L;

    if (!client.tableOperations().exists(tableName)) {
      client.tableOperations().create(tableName);
    }

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      final Text row = new Text(), cf = new Text(), cq = new Text();
      final Value value = new Value();
      for (long i = 0; i < numRecords; i++) {
        row.set(Integer.toString(rowRand.nextInt(rowMax)));
        cf.set(Integer.toString(cfRand.nextInt(cfMax)));
        cq.set(Integer.toString(cqRand.nextInt(cqMax)));

        Mutation m = new Mutation(row);

        // Choose a random value between [0,100)
        int deleteValue = deleteRand.nextInt(100);

        // putDelete if the value we chose is less than our delete
        // percentage
        if (deleteValue < deletePercent) {
          m.putDelete(cf, cq);
        } else {
          value.set(Long.toString(valueCounter).getBytes());
          m.put(cf, cq, valueCounter, value);
        }

        bw.addMutation(m);

        valueCounter++;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    RandomWorkloadOpts opts = new RandomWorkloadOpts();
    opts.parseArgs(RandomWorkload.class.getSimpleName(), args);

    RandomWorkload rw = new RandomWorkload();

    rw.run(opts);
  }
}
