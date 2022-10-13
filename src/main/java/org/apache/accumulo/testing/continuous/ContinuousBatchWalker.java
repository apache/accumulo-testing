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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class ContinuousBatchWalker {
  private static final Logger log = LoggerFactory.getLogger(ContinuousBatchWalker.class);

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {
      Authorizations auths = env.getRandomAuthorizations();
      AccumuloClient client = env.getAccumuloClient();

      try (Scanner scanner = ContinuousUtil.createScanner(client, env.getAccumuloTableName(),
          auths)) {
        int scanBatchSize = Integer.parseInt(env.getTestProperty(TestProps.CI_BW_BATCH_SIZE));
        scanner.setBatchSize(scanBatchSize);
        ConsistencyLevel cl = TestProps
            .getScanConsistencyLevel(env.getTestProperty(TestProps.CI_BW_CONSISTENCY_LEVEL));
        scanner.setConsistencyLevel(cl);
        Duration bwSleep = Duration
            .ofMillis(Integer.parseInt(env.getTestProperty(TestProps.CI_BW_SLEEP_MS)));
        while (true) {
          try (BatchScanner bs = client.createBatchScanner(env.getAccumuloTableName(), auths)) {
            Set<Text> batch = getBatch(scanner, env.getRowMin(), env.getRowMax(), scanBatchSize,
                env.getRandom());
            List<Range> ranges = batch.stream().map(Range::new).collect(Collectors.toList());
            runBatchScan(scanBatchSize, bs, batch, ranges);
          }
          sleepUninterruptibly(bwSleep);
        }
      }
    }
  }

  private static void runBatchScan(int batchSize, BatchScanner bs, Set<Text> batch,
      List<Range> ranges) {
    bs.setRanges(ranges);

    Set<Text> rowsSeen = new HashSet<>();

    int count = 0;

    long t1 = System.currentTimeMillis();

    for (Entry<Key,Value> entry : bs) {
      ContinuousWalk.validate(entry.getKey(), entry.getValue());

      rowsSeen.add(entry.getKey().getRow());

      addRow(batchSize, entry.getValue());

      count++;
    }
    bs.close();

    long t2 = System.currentTimeMillis();

    if (!rowsSeen.equals(batch)) {
      Set<Text> extraSeen = Sets.difference(rowsSeen, batch);
      Set<Text> notSeen = Sets.difference(batch, rowsSeen);

      log.info("DIF {} {} {}", t1, extraSeen.size(), notSeen.size());
      log.info("DIF {} {} {}", t1, extraSeen.size(), notSeen.size());
      log.info("Extra seen : {}", extraSeen);
      log.info("Not seen   : {}", notSeen);
    } else {
      log.info("BRQ {} {} {} {} {}", t1, (t2 - t1), rowsSeen.size(), count,
          (rowsSeen.size() / ((t2 - t1) / 1000.0)));
    }
  }

  private static void addRow(int batchSize, Value v) {
    byte[] val = v.get();

    int offset = ContinuousWalk.getPrevRowOffset(val);
    if (offset > 1) {
      Text prevRow = new Text();
      prevRow.set(val, offset, 16);
      if (rowsToQuery.size() < 3 * batchSize) {
        rowsToQuery.add(prevRow);
      }
    }
  }

  private static final HashSet<Text> rowsToQuery = new HashSet<>();

  private static Set<Text> getBatch(Scanner scanner, long min, long max, int batchSize, Random r) {

    while (rowsToQuery.size() < batchSize) {
      byte[] scanStart = ContinuousIngest.genRow(min, max, r);
      scanner.setRange(new Range(new Text(scanStart), null));

      int count = 0;

      long t1 = System.currentTimeMillis();

      Iterator<Entry<Key,Value>> iter = scanner.iterator();
      while (iter.hasNext() && rowsToQuery.size() < 3 * batchSize) {
        Entry<Key,Value> entry = iter.next();
        ContinuousWalk.validate(entry.getKey(), entry.getValue());
        addRow(batchSize, entry.getValue());
        count++;
      }

      long t2 = System.currentTimeMillis();

      log.info("FSB {} {} {}", t1, (t2 - t1), count);

      sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    HashSet<Text> ret = new HashSet<>();

    Iterator<Text> iter = rowsToQuery.iterator();

    for (int i = 0; i < batchSize; i++) {
      ret.add(iter.next());
      iter.remove();
    }

    return ret;
  }
}
