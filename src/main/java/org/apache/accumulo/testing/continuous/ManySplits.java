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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ManySplits {
  private static final Logger log = LoggerFactory.getLogger(ManySplits.class);

  private static final String NAMESPACE = "manysplits";

  public static void main(String[] args) throws Exception {
    try (ContinuousEnv env = new ContinuousEnv(args)) {

      AccumuloClient client = env.getAccumuloClient();
      Properties testProps = env.getTestProperties();
      final int tableCount =
          Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_TABLE_COUNT));
      final long rowMin = Long.parseLong(testProps.getProperty(TestProps.CI_SPLIT_INGEST_ROW_MIN));
      final long rowMax = Long.parseLong(testProps.getProperty(TestProps.CI_SPLIT_INGEST_ROW_MAX));
      final int maxColF = Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_INGEST_MAX_CF));
      final int maxColQ = Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_INGEST_MAX_CQ));
      final int initialTabletCount =
          Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_INITIAL_TABLETS));
      final int initialData =
          Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_WRITE_SIZE));
      String initialSplitThresholdStr = testProps.getProperty(TestProps.CI_SPLIT_THRESHOLD);
      final long initialSplitThreshold =
          ConfigurationTypeHelper.getFixedMemoryAsBytes(initialSplitThresholdStr);
      final int splitThresholdReductionFactor =
          Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_THRESHOLD_REDUCTION_FACTOR));
      final int testRounds =
          Integer.parseInt(testProps.getProperty(TestProps.CI_SPLIT_TEST_ROUNDS));

      // disable deletes for ingest
      testProps.setProperty(TestProps.CI_INGEST_DELETE_PROBABILITY, "0.0");

      final Random random = env.getRandom();

      Preconditions.checkArgument(tableCount > 0, "Test cannot run without any tables");

      final List<String> tableNames = IntStream.range(1, tableCount + 1)
          .mapToObj(i -> NAMESPACE + ".table" + i).collect(Collectors.toList());

      try {
        client.namespaceOperations().create(NAMESPACE);
      } catch (NamespaceExistsException e) {
        log.warn("The namespace '{}' already exists. Continuing with existing namespace.",
            NAMESPACE);
      }

      final String firstTable = tableNames.get(0);

      Map<String,String> tableProps =
          Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(), initialSplitThresholdStr);

      log.info("Properties being used to create tables for this test: {}", tableProps);

      log.info("Creating initial table: {}", firstTable);
      CreateTable.createTable(client, firstTable, initialTabletCount, rowMin, rowMax, tableProps,
          Map.of());

      log.info("Ingesting {} entries into first table, {}.", initialData, firstTable);
      ContinuousIngest.doIngest(client, rowMin, rowMax, firstTable, testProps, maxColF, maxColQ,
          initialData, false, random);

      client.tableOperations().flush(firstTable);

      // clone tables instead of ingesting into each. it's a lot quicker
      log.info("Creating {} more tables by cloning the first", tableCount - 1);
      tableNames.stream().parallel().skip(1).forEach(tableName -> {
        try {
          client.tableOperations().clone(firstTable, tableName, true, null, null);
        } catch (TableExistsException e) {
          log.warn(
              "table {} already exists. Continuing with existing table. Previous data will affect splits",
              tableName);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      StringBuilder testResults = new StringBuilder();
      testResults.append("Test results:\n");
      testResults.append("Total test rounds: ").append(testRounds).append("\n");
      testResults.append("Table count: ").append(tableCount).append("\n");

      SECONDS.sleep(5);

      // main loop
      // reduce the split threshold then wait for the expected file size per tablet to be reached
      long previousSplitThreshold = initialSplitThreshold;
      for (int i = 0; i < testRounds; i++) {

        // apply the reduction factor to the previous threshold
        final long splitThreshold = previousSplitThreshold / splitThresholdReductionFactor;
        final String splitThresholdStr = bytesToMemoryString(splitThreshold);
        final int totalSplitCountBefore = getTotalSplitCount(client, tableNames);

        log.info("Changing split threshold on all tables from {} to {}",
            bytesToMemoryString(previousSplitThreshold), splitThresholdStr);

        long beforeThresholdUpdate = System.nanoTime();

        // update the split threshold on all tables
        tableNames.stream().parallel().forEach(tableName -> {
          try {
            client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(),
                splitThresholdStr);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        log.info("Waiting for each tablet to have a sum file size <= {}", splitThresholdStr);

        // wait for all tablets to reach the expected sum file size
        tableNames.stream().parallel().forEach(tableName -> {
          int elapsedMillis = 0;
          long sleepMillis = SECONDS.toMillis(1);
          try {
            // wait for each tablet to reach the expected sum file size
            while (true) {
              Collection<Long> tabletFileSizes = getTabletFileSizes(client, tableName).values();
              // filter out the tablets that are already the expected size
              Set<Long> offendingTabletSizes =
                  tabletFileSizes.stream().filter(tabletFileSize -> tabletFileSize > splitThreshold)
                      .collect(Collectors.toSet());
              // if all tablets are good, move on
              if (offendingTabletSizes.isEmpty()) {
                break;
              }

              elapsedMillis += sleepMillis;
              // log every 3 seconds
              if (elapsedMillis % SECONDS.toMillis(3) == 0) {
                double averageFileSize =
                    offendingTabletSizes.stream().mapToLong(l -> l).average().orElse(0);
                long diff = (long) (averageFileSize - splitThreshold);
                log.info(
                    "{} tablets have file sizes not yet <= {} on table {}. Diff of avg offending file(s): {}",
                    offendingTabletSizes.size(), splitThresholdStr, tableName,
                    bytesToMemoryString(diff));
              }
              MILLISECONDS.sleep(sleepMillis);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        long timeTakenNanos = System.nanoTime() - beforeThresholdUpdate;
        long seconds = NANOSECONDS.toSeconds(timeTakenNanos);
        long millis = NANOSECONDS.toMillis(timeTakenNanos);

        final int splitCountAfter = getTotalSplitCount(client, tableNames);
        final int splitCountThisRound = splitCountAfter - totalSplitCountBefore;

        log.info(
            "Time taken for all tables to reach expected total file size ({}): {} seconds ({}ms)",
            splitThresholdStr, seconds, millis);

        testResults.append("Test round ").append(i).append(":\n");
        testResults.append("TABLE_SPLIT_THRESHOLD ")
            .append(bytesToMemoryString(previousSplitThreshold)).append(" -> ")
            .append(splitThresholdStr).append("\n");
        testResults.append("Splits count:         ").append(totalSplitCountBefore).append(" -> ")
            .append(splitCountAfter).append("\n");
        String splitsPerSecond = String.format("%.2f", (double) splitCountThisRound / seconds);
        testResults.append("Splits per second:    ").append(splitsPerSecond).append("\n");

        previousSplitThreshold = splitThreshold;
      }

      log.info("Test completed successfully.");
      log.info(testResults.toString());
      log.info("Deleting tables");
      tableNames.stream().parallel().forEach(tableName -> {
        try {
          client.tableOperations().delete(tableName);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      log.info("Deleting namespace");
      client.namespaceOperations().delete(NAMESPACE);

    }
  }

  /**
   * @return a map of tablets to the sum of their file size
   */
  private static Map<Text,Long> getTabletFileSizes(AccumuloClient client, String tableName)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
    try (Scanner scanner = client.createScanner("accumulo.metadata")) {
      scanner.fetchColumnFamily("file");
      scanner.setRange(getMetaRangeForTable(tableId.canonical()));

      Map<Text,Long> result = new HashMap<>();
      for (var entry : scanner) {
        String encodedDFV = new String(entry.getValue().get(), UTF_8);
        String[] ba = encodedDFV.split(",", 2);
        long tabletFileSize = Long.parseLong(ba[0]);
        result.merge(entry.getKey().getRow(), tabletFileSize, Long::sum);
      }

      return result;
    }
  }

  public static String bytesToMemoryString(long bytes) {
    if (bytes < 1024) {
      return bytes + "B"; // Bytes
    } else if (bytes < 1024 * 1024) {
      return (bytes / 1024) + "K"; // Kilobytes
    } else if (bytes < 1024 * 1024 * 1024) {
      return (bytes / (1024 * 1024)) + "M"; // Megabytes
    } else {
      return (bytes / (1024 * 1024 * 1024)) + "G"; // Gigabytes
    }
  }

  private static Range getMetaRangeForTable(String tableId) {
    return new Range(tableId + ";", false, tableId + "<", true);
  }

  /**
   * @return the total number of splits across all given tables
   */
  private static int getTotalSplitCount(AccumuloClient client, List<String> tableNames) {
    return tableNames.stream().parallel().mapToInt(tableName -> {
      try {
        return client.tableOperations().listSplits(tableName).size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).sum();
  }

}
