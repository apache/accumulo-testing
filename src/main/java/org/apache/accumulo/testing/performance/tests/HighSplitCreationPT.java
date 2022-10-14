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

import java.nio.charset.StandardCharsets;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.testing.TestProps;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;
import org.apache.accumulo.testing.performance.SystemConfiguration;
import org.apache.hadoop.io.Text;

public class HighSplitCreationPT implements PerformanceTest {

  private static final int NUM_SPLITS = 10_000;
  private static final int MIN_REQUIRED_SPLITS_PER_SECOND = 100;
  private static final int ONE_SECOND = 1000;
  private static final String TABLE_NAME = "highSplitCreation";
  private static final String METADATA_TABLE_SPLITS = "123456789abcde";

  @Override
  public SystemConfiguration getSystemConfig() {
    return new SystemConfiguration();
  }

  @Override
  public Report runTest(final Environment env) throws Exception {
    Report.Builder reportBuilder = Report.builder().id("high_split_creation")
        .description("Evaluate the speed of creating many splits.")
        .parameter("table_name", TABLE_NAME, "The name of the test table")
        .parameter("num_splits", NUM_SPLITS, "The high number of splits to add.")
        .parameter("min_required_splits_per_second", MIN_REQUIRED_SPLITS_PER_SECOND,
            "The minimum average number of splits that must be created per second before performance is considered too slow.");

    AccumuloClient client = env.getClient();
    client.tableOperations().create(TABLE_NAME);
    client.tableOperations().addSplits(TestProps.METADATA_TABLE_NAME, getMetadataTableSplits());

    SortedSet<Text> splits = getTestTableSplits();

    long start = System.currentTimeMillis();
    client.tableOperations().addSplits(TABLE_NAME, splits);
    long totalTime = System.currentTimeMillis() - start;
    double splitsPerSecond = NUM_SPLITS / (totalTime / ONE_SECOND);

    reportBuilder.result("splits_per_second", splitsPerSecond, "splits/sec",
        "The average rate of split creation.");

    return reportBuilder.build();
  }

  private SortedSet<Text> getMetadataTableSplits() {
    SortedSet<Text> splits = new TreeSet<>();
    for (byte b : METADATA_TABLE_SPLITS.getBytes(StandardCharsets.UTF_8)) {
      splits.add(new Text(new byte[] {'1', ';', b}));
    }
    return splits;
  }

  private SortedSet<Text> getTestTableSplits() {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < NUM_SPLITS; i++) {
      splits.add(new Text(Integer.toHexString(i)));
    }
    return splits;
  }
}
