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
package org.apache.accumulo.testing.healthprobe;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.collect.Lists;

public class Monitor {

  private static final Logger log = LoggerFactory.getLogger(Monitor.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  Random r = new Random();
  static long distance = 100l;

  public static void main(String[] args) throws Exception {
    MonitorOpts opts = new MonitorOpts();
    opts.parseArgs(Monitor.class.getName(), args);
    distance = opts.distance;
    Authorizations auth = new Authorizations(opts.auth);

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build();
        Scanner scanner = client.createScanner(opts.tableName, auth )) {
      if (opts.isolate) {
        scanner.enableIsolation();
      }
      int scannerSleepMs = opts.sleep_ms;
      LoopControl scanning_condition = opts.continuous ? new ContinuousLoopControl()
          : new IterativeLoopControl(opts.scan_iterations);

      while (scanning_condition.keepScanning()) {
        Random tablet_index_generator = new Random();
        Range range = pickRange(client.tableOperations(), opts.tableName, tablet_index_generator);
        scanner.setRange(range);
        if (opts.batch_size > 0) {
          scanner.setBatchSize(opts.batch_size);
        }
        try {
          long startTime = System.nanoTime();
          int count = consume(scanner);
          long stopTime = System.nanoTime();
          MDC.put("StartTime", String.valueOf(startTime));
          MDC.put("TabletIndex", String.valueOf(tablet_index_generator));
          MDC.put("TableName", String.valueOf(opts.tableName));
          MDC.put("TotalTime", String.valueOf((stopTime - startTime)));
          MDC.put("StartRow", String.valueOf(range.getStartKey()));
          MDC.put("EndRow", String.valueOf(range.getEndKey()));
          MDC.put("TotalRecords", String.valueOf(count));

          log.info("SCN starttime={} tabletindex={} tablename={} totaltime={} totalrecords={}", startTime,
              tablet_index_generator, opts.tableName, (stopTime - startTime), count);
          if (scannerSleepMs > 0) {
            sleepUninterruptibly(scannerSleepMs, TimeUnit.MILLISECONDS);
          }
        } catch (Exception e) {
          log.error(String.format(
              "Exception while scanning range %s. Check the state of Accumulo for errors.", range), e);
        }
      }
    }
  }

  public static int consume(Iterable<Entry<Key,Value>> iterable) {
    Iterator<Entry<Key,Value>> itr = iterable.iterator();
    int count = 0;
    while (itr.hasNext()) {
      Entry<Key,Value> e = itr.next();
      Key key = e.getKey();
      String readRow= key.getRow() + " " + key.getColumnFamily() + " " + key.getColumnQualifier() + " " + e.getValue();
      count++;
    }
    return count;
  }

  public static long genLong(long min, long max, Random r) {
    return ((r.nextLong() & 0x7fffffffffffffffL) % (max - min)) + min;
  }

  static byte[] genRow(long min, long max, Random r) {
    return genRow(genLong(min, max, r));
  }

  public static byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }

  public static Text genMaxRow(Text splitEnd) {
    String padded = splitEnd.toString() + "0000000000000000".substring(splitEnd.toString().length());
    Text maxRow = new Text(genRow( Long.parseLong(padded.trim(), 16) - 1l));
    return maxRow;
  }

  public static Text genMinRow(Text splitStart) {
    String padded = splitStart.toString()
        + "0000000000000000".substring(splitStart.toString().length());
    Text minRow = new Text(genRow(Long.parseLong(padded.trim(), 16)));
    return minRow;
  }

  public static Range pickRange(TableOperations tops, String table, Random r)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {

    ArrayList<Text> splits = Lists.newArrayList(tops.listSplits(table));
    if (splits.isEmpty()) {
      return new Range();
    } else {
      int index = r.nextInt(splits.size());
      Text maxRow = splits.get(index);
      maxRow = (maxRow.getLength() < 16) ? genMaxRow(maxRow) : maxRow;
      Text minRow = index == 0 ? new Text(genRow(0)) : splits.get(index - 1);
      minRow = (minRow.getLength() < 16) ? genMinRow(minRow) : minRow;
      long maxRowlong = Long.parseLong(maxRow.toString().trim(), 16);
      long minRowlong = Long.parseLong(minRow.toString().trim(), 16);

      if((maxRowlong - minRowlong) <= distance)
      {
        byte[] scanStart = genRow(minRowlong);
        byte[] scanStop = genRow(maxRowlong);
        return new Range(new Text(scanStart), false, new Text(scanStop), true);
      }

      long startRow = genLong(minRowlong, maxRowlong - distance, r);
      byte[] scanStart = genRow(startRow);
      byte[] scanStop = genRow(startRow + distance);

      return new Range(new Text(scanStart), false, new Text(scanStop), true);
    }
  }

  /*
   * These interfaces + implementations are used to determine how many times the scanner should look
   * up a random tablet and scan it.
   */
  static interface LoopControl {
    public boolean keepScanning();
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
