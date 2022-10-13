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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousScanner {
  private static final Logger log = LoggerFactory.getLogger(ContinuousScanner.class);

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {

      long distance = 1_000_000_000_000L;

      AccumuloClient client = env.getAccumuloClient();

      int numToScan = Integer.parseInt(env.getTestProperty(TestProps.CI_SCANNER_ENTRIES));
      int scannerSleepMs = Integer.parseInt(env.getTestProperty(TestProps.CI_SCANNER_SLEEP_MS));
      ConsistencyLevel cl = TestProps
          .getScanConsistencyLevel(env.getTestProperty(TestProps.CI_SCANNER_CONSISTENCY_LEVEL));

      double delta = Math.min(.05, .05 / (numToScan / 1000.0));
      try (Scanner scanner = ContinuousUtil.createScanner(client, env.getAccumuloTableName(),
          env.getRandomAuthorizations())) {
        while (true) {
          long startRow = ContinuousIngest.genLong(env.getRowMin(), env.getRowMax() - distance,
              env.getRandom());
          byte[] scanStart = ContinuousIngest.genRow(startRow);
          byte[] scanStop = ContinuousIngest.genRow(startRow + distance);

          scanner.setRange(new Range(new Text(scanStart), new Text(scanStop)));
          scanner.setConsistencyLevel(cl);

          long t1 = System.currentTimeMillis();

          long count = scanner.stream()
              .peek(entry -> ContinuousWalk.validate(entry.getKey(), entry.getValue())).count();

          long t2 = System.currentTimeMillis();

          if (count < (1 - delta) * numToScan || count > (1 + delta) * numToScan) {
            if (count == 0) {
              distance = distance * 10;
              if (distance < 0)
                distance = 1_000_000_000_000L;
            } else {
              double ratio = (double) numToScan / count;
              // move ratio closer to 1 to make change slower
              ratio = ratio - (ratio - 1.0) * (2.0 / 3.0);
              distance = (long) (ratio * distance);
            }
          }

          log.debug("SCAN - start: {}ms, start row: {}, duration: {}ms, total scanned: {}", t1,
              new String(scanStart, UTF_8), (t2 - t1), count);

          if (scannerSleepMs > 0) {
            sleepUninterruptibly(scannerSleepMs, TimeUnit.MILLISECONDS);
          }
        }
      }
    }
  }
}
