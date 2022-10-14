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

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
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

  private void createTable(AccumuloClient c, String table, String durability) throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Collections.singletonMap("table.durability", durability));
    c.tableOperations().create(table, ntc);
  }

  @Override
  public Report runTest(Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder();
    reportBuilder.id("durability");
    reportBuilder.description("Compares writes speeds at different durability levels");
    try (AccumuloClient client = env.getClient()) {
      TableOperations tableOps = client.tableOperations();
      for (String durability : new String[] {"sync", "flush", "log", "none"}) {
        String tableName = durability + "T";
        createTable(client, tableName, durability);
        long median = writeSome(reportBuilder, client, tableName, N, durability);
        tableOps.delete(tableName);
        reportBuilder.result(durability + " Median", median, TimeUnit.MILLISECONDS.toString(),
            "Median time result for " + durability);
      }
    }
    reportBuilder.parameter("rows", N, "Number of random rows written.");
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
          TimeUnit.MILLISECONDS.toString(), "Times for each attempt");
    }
    Arrays.sort(attempts);
    // Return the median duration
    return attempts[2];
  }
}
