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
package org.apache.accumulo.testing.healthprobe;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class Monitor {

  private static final Logger log = LoggerFactory.getLogger(Monitor.class);
  static long distance = 1l;
  static List<TabletId> tablets;
  static boolean cacheTablets = true;

  public static void main(String[] args) throws Exception {
    MonitorOpts opts = new MonitorOpts();
    opts.parseArgs(Monitor.class.getName(), args);
    distance = opts.distance;
    Authorizations auth = new Authorizations();
    if (opts.auth != "") {
      auth = new Authorizations(opts.auth);
    }

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build();
        Scanner scanner = client.createScanner(opts.tableName, auth)) {
      if (opts.isolate) {
        scanner.enableIsolation();
      }
      int scannerSleepMs = opts.sleep_ms;
      LoopControl scanning_condition = opts.continuous ? new ContinuousLoopControl()
          : new IterativeLoopControl(opts.scan_iterations);

      while (scanning_condition.keepScanning()) {

        Random tablet_index_generator = new Random();
        TabletId pickedTablet = pickTablet(client.tableOperations(), opts.tableName,
            tablet_index_generator);
        Range range = pickedTablet.toRange();
        scanner.setRange(range);

        if (opts.batch_size > 0) {
          scanner.setBatchSize(opts.batch_size);
        }
        try {
          long startTime = System.nanoTime();
          long count = consume(scanner, opts.distance);
          long stopTime = System.nanoTime();
          MDC.put("StartTime", String.valueOf(startTime));
          MDC.put("TabletId", String.valueOf(pickedTablet));
          MDC.put("TableName", String.valueOf(opts.tableName));
          MDC.put("TotalTime", String.valueOf((stopTime - startTime)));
          MDC.put("StartRow", String.valueOf(range.getStartKey()));
          MDC.put("EndRow", String.valueOf(range.getEndKey()));
          MDC.put("TotalRecords", String.valueOf(count));

          log.info("SCN starttime={} tabletindex={} tablename={} totaltime={} totalrecords={}",
              startTime, tablet_index_generator, opts.tableName, (stopTime - startTime), count);
          if (scannerSleepMs > 0) {
            sleepUninterruptibly(scannerSleepMs, TimeUnit.MILLISECONDS);
          }
        } catch (Exception e) {
          log.error(String.format(
              "Exception while scanning range %s. Check the state of Accumulo for errors.", range),
              e);
        }
      }
    }
  }

  public static int consume(Iterable<Entry<Key,Value>> scanner, long numberOfRows) {
    RowIterator rowIter = new RowIterator(scanner);
    int count = 0;

    while (rowIter.hasNext()) {
      count++;
      if (count >= numberOfRows) {
        break;
      }
    }
    return count;
  }

  public static TabletId pickTablet(TableOperations tops, String table, Random r)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {

    if (cacheTablets) {
      Locations locations = tops.locate(table, Collections.singleton(new Range()));
      tablets = new ArrayList<>(locations.groupByTablet().keySet());
      cacheTablets = false;
    }
    int index = r.nextInt(tablets.size());
    return tablets.get(index);
  }

  /*
   * These interfaces + implementations are used to determine how many times the scanner should look
   * up a random tablet and scan it.
   */
  interface LoopControl {
    boolean keepScanning();
  }

  // Does a finite number of iterations
  static class IterativeLoopControl implements LoopControl {
    private final int max;
    private int current;

    public IterativeLoopControl(int max) {
      this.max = max;
      this.current = 0;
    }

    @Override
    public boolean keepScanning() {
      if (current < max) {
        ++current;
        return true;
      } else {
        return false;
      }
    }
  }

  // Does an infinite number of iterations
  static class ContinuousLoopControl implements LoopControl {
    @Override
    public boolean keepScanning() {
      return true;
    }
  }
}
