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
package org.apache.accumulo.testing.continuous;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ManySplits {
  private static final Logger log = LoggerFactory.getLogger(ManySplits.class);

  private static final String NAMESPACE = "manysplits";

  static int tableCount = 3;
  static int initialSplits = 0;
  static int initialData = 10_000_000;
  static String splitThreshold = "1G";
  static int reductionFactor = 10;
  static int testRoundsToPerform = 3;

  public static void main(String[] args) throws Exception {
    try (ContinuousEnv env = new ContinuousEnv(args)) {

      AccumuloClient client = env.getAccumuloClient();
      final long rowMin = 0;
      final long rowMax = Long.MAX_VALUE;
      Properties testProps = env.getTestProperties();
      final int maxColF = 32767;
      final int maxColQ = 32767;
      final Random random = env.getRandom();

      Preconditions.checkArgument(tableCount > 0, "Test cannot run without any tables");

      final List<String> tableNames = IntStream.range(1, tableCount + 1)
          .mapToObj(i -> NAMESPACE + ".table" + i).collect(Collectors.toList());

      try {
        client.namespaceOperations().create(NAMESPACE);
      } catch (NamespaceExistsException e) {
        log.info("The namespace '{}' already exists. Continuing with existing namespace.",
            NAMESPACE);
      }

      final NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), splitThreshold));

      log.info("Properties being used to create tables for this test: {}",
          ntc.getProperties().toString());

      String firstTable = tableNames.get(0);

      log.info("Creating initial table: {}", firstTable);
      try {
        client.tableOperations().create(firstTable, ntc);
      } catch (TableExistsException e) {
        log.info("Test probably wont work if the table already exists with data present", e);
      }

      log.info("Ingesting {} entries into first table, {}.", initialData, firstTable);
      ContinuousIngest.doIngest(client, rowMin, rowMax, firstTable, testProps, maxColF, maxColQ,
          initialData, false, random);

      client.tableOperations().flush(firstTable);

      // clone tables instead of ingesting into each. it's a lot quicker
      log.info("Creating {} tables by cloning the first", tableCount);
      tableNames.stream().parallel().skip(1).forEach(tableName -> {
        try {
          client.tableOperations().clone(firstTable, tableName,
              CloneConfiguration.builder().build());
        } catch (TableExistsException e) {
          log.info(
              "table {} already exists. Continuing with existing table. This might mess with the expected values",
              tableName);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Map<String,Integer> thresholdToExpectedTabletCount = new LinkedHashMap<>(3);
      thresholdToExpectedTabletCount.put("100M", 4);
      thresholdToExpectedTabletCount.put("10M", 32);
      thresholdToExpectedTabletCount.put("1M", 512);

      // main loop
      // reduce the split threshold then wait for the expected number of tablets to appear
      for (var entry : thresholdToExpectedTabletCount.entrySet()) {
        String oldSplitThreshold = splitThreshold;
        splitThreshold = entry.getKey();
        final int expectedTabletCount = entry.getValue();

        log.info("Changing split threshold on all tables from {} to {}", oldSplitThreshold,
            splitThreshold);

        long beforeThresholdUpdate = System.nanoTime();

        // update the split threshold on all tables
        tableNames.stream().parallel().forEach(tableName -> {
          try {
            client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(),
                splitThreshold);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        log.info("Waiting for tablet count on all tables to be greater than {}.",
            expectedTabletCount - 1);

        // wait for all tables to reach the expected number of tablets
        tableNames.stream().parallel().forEach(tableName -> {
          int tabletCount;
          int ellapsedMillis = 0;
          int sleepMillis = 1000;
          try {
            while ((tabletCount = client.tableOperations().listSplits(tableName).size() + 1)
                < expectedTabletCount) {
              ellapsedMillis += sleepMillis;
              // log every 3 seconds
              if (ellapsedMillis % SECONDS.toMillis(3) == 0) {
                log.info("{} has {} tablets. waiting for > {}", tableName, tabletCount,
                    expectedTabletCount - 1);
              }
              MILLISECONDS.sleep(sleepMillis);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        long timeTaken = System.nanoTime() - beforeThresholdUpdate;

        log.info(
            "Time taken for all tables to reach expected tablet count ({} tablets): {} seconds ({}ms {}ns)",
            expectedTabletCount, NANOSECONDS.toSeconds(timeTaken), NANOSECONDS.toMillis(timeTaken),
            timeTaken);
      }

    }
  }

}
