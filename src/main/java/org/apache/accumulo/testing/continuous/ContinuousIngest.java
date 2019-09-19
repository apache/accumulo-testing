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
package org.apache.accumulo.testing.continuous;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.testing.TestProps.CI_INGEST_PAUSE_DURATION_MAX;
import static org.apache.accumulo.testing.TestProps.CI_INGEST_PAUSE_DURATION_MIN;
import static org.apache.accumulo.testing.TestProps.CI_INGEST_PAUSE_WAIT_MAX;
import static org.apache.accumulo.testing.TestProps.CI_INGEST_PAUSE_WAIT_MIN;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.testing.TestProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ContinuousIngest {

  private static final Logger log = LoggerFactory.getLogger(ContinuousIngest.class);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static List<ColumnVisibility> visibilities;
  private static long lastPauseNs;
  private static long pauseWaitSec;

  private static ColumnVisibility getVisibility(Random rand) {
    return visibilities.get(rand.nextInt(visibilities.size()));
  }

  private static boolean pauseEnabled(Properties props) {
    String value = props.getProperty(TestProps.CI_INGEST_PAUSE_ENABLED);
    return Boolean.parseBoolean(value);
  }

  private static int getPause(Properties props, Random rand, String minProp, String maxProp) {
    int min = Integer.parseInt(props.getProperty(minProp));
    int max = Integer.parseInt(props.getProperty(maxProp));
    Preconditions.checkState(max >= min && min > 0);
    if (max == min) {
      return min;
    }
    return (rand.nextInt(max - min) + min);
  }

  private static int getFlushEntries(Properties props) {
    return Integer.parseInt(props.getProperty(TestProps.CI_INGEST_FLUSH_ENTRIES, "1000000"));
  }

  private static void pauseCheck(Properties props, Random rand) throws InterruptedException {
    if (pauseEnabled(props)) {
      long elapsedNano = System.nanoTime() - lastPauseNs;
      if (elapsedNano > (TimeUnit.SECONDS.toNanos(pauseWaitSec))) {
        long pauseDurationSec = getPause(props, rand, CI_INGEST_PAUSE_DURATION_MIN,
            CI_INGEST_PAUSE_DURATION_MAX);
        log.info("PAUSING for " + pauseDurationSec + "s");
        Thread.sleep(TimeUnit.SECONDS.toMillis(pauseDurationSec));
        lastPauseNs = System.nanoTime();
        pauseWaitSec = getPause(props, rand, CI_INGEST_PAUSE_WAIT_MIN, CI_INGEST_PAUSE_WAIT_MAX);
        log.info("INGESTING for " + pauseWaitSec + "s");
      }
    }
  }

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {

      visibilities = parseVisibilities(env.getTestProperty(TestProps.CI_INGEST_VISIBILITIES));

      long rowMin = env.getRowMin();
      long rowMax = env.getRowMax();
      if (rowMin < 0 || rowMax < 0 || rowMax <= rowMin) {
        throw new IllegalArgumentException("bad min and max");
      }

      AccumuloClient client = env.getAccumuloClient();
      String tableName = env.getAccumuloTableName();
      if (!client.tableOperations().exists(tableName)) {
        throw new TableNotFoundException(null, tableName,
            "Consult the README and create the table before starting ingest.");
      }

      BatchWriter bw = client.createBatchWriter(tableName);

      Random r = new Random();

      byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes(UTF_8);

      log.info(String.format("UUID %d %s", System.currentTimeMillis(),
          new String(ingestInstanceId, UTF_8)));

      long count = 0;
      final int flushInterval = getFlushEntries(env.getTestProperties());
      final int maxDepth = 25;

      // always want to point back to flushed data. This way the previous item should
      // always exist in accumulo when verifying data. To do this make insert N point
      // back to the row from insert (N - flushInterval). The array below is used to keep
      // track of this.
      long[] prevRows = new long[flushInterval];
      long[] firstRows = new long[flushInterval];
      int[] firstColFams = new int[flushInterval];
      int[] firstColQuals = new int[flushInterval];

      long lastFlushTime = System.currentTimeMillis();

      int maxColF = env.getMaxColF();
      int maxColQ = env.getMaxColQ();
      boolean checksum = Boolean.parseBoolean(env.getTestProperty(TestProps.CI_INGEST_CHECKSUM));
      long numEntries = Long.parseLong(env.getTestProperty(TestProps.CI_INGEST_CLIENT_ENTRIES));

      Properties testProps = env.getTestProperties();
      if (pauseEnabled(testProps)) {
        lastPauseNs = System.nanoTime();
        pauseWaitSec = getPause(testProps, r, CI_INGEST_PAUSE_WAIT_MIN, CI_INGEST_PAUSE_WAIT_MAX);
        log.info("PAUSING enabled");
        log.info("INGESTING for " + pauseWaitSec + "s");
      }

      out: while (true) {
        // generate first set of nodes
        ColumnVisibility cv = getVisibility(r);

        for (int index = 0; index < flushInterval; index++) {
          long rowLong = genLong(rowMin, rowMax, r);
          prevRows[index] = rowLong;
          firstRows[index] = rowLong;

          int cf = r.nextInt(maxColF);
          int cq = r.nextInt(maxColQ);

          firstColFams[index] = cf;
          firstColQuals[index] = cq;

          Mutation m = genMutation(rowLong, cf, cq, cv, ingestInstanceId, count, null, checksum);
          count++;
          bw.addMutation(m);
        }

        lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
        if (count >= numEntries)
          break out;

        // generate subsequent sets of nodes that link to previous set of nodes
        for (int depth = 1; depth < maxDepth; depth++) {
          for (int index = 0; index < flushInterval; index++) {
            long rowLong = genLong(rowMin, rowMax, r);
            byte[] prevRow = genRow(prevRows[index]);
            prevRows[index] = rowLong;
            Mutation m = genMutation(rowLong, r.nextInt(maxColF), r.nextInt(maxColQ), cv,
                ingestInstanceId, count, prevRow, checksum);
            count++;
            bw.addMutation(m);
          }

          lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
          if (count >= numEntries)
            break out;
          pauseCheck(testProps, r);
        }

        // create one big linked list, this makes all of the first inserts point to something
        for (int index = 0; index < flushInterval - 1; index++) {
          Mutation m = genMutation(firstRows[index], firstColFams[index], firstColQuals[index], cv,
              ingestInstanceId, count, genRow(prevRows[index + 1]), checksum);
          count++;
          bw.addMutation(m);
        }
        lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
        if (count >= numEntries)
          break out;
        pauseCheck(testProps, r);
      }
      bw.close();
    }
  }

  public static List<ColumnVisibility> parseVisibilities(String visString) {
    List<ColumnVisibility> vis;
    if (visString == null) {
      vis = Collections.singletonList(new ColumnVisibility());
    } else {
      vis = new ArrayList<>();
      for (String v : visString.split(",")) {
        vis.add(new ColumnVisibility(v.trim()));
      }
    }
    return vis;
  }

  private static long flush(BatchWriter bw, long count, final int flushInterval, long lastFlushTime)
      throws MutationsRejectedException {
    long t1 = System.currentTimeMillis();
    bw.flush();
    long t2 = System.currentTimeMillis();
    log.info(String.format("FLUSH %d %d %d %d %d", t2, (t2 - lastFlushTime), (t2 - t1), count,
        flushInterval));
    lastFlushTime = t2;
    return lastFlushTime;
  }

  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv,
      byte[] ingestInstanceId, long count, byte[] prevRow, boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not
    // good for small data.... so used CRC32 instead
    CRC32 cksum = null;

    byte[] rowString = genRow(rowLong);

    byte[] cfString = genCol(cfInt);
    byte[] cqString = genCol(cqInt);

    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
      cksum.update(cv.getExpression());
    }

    Mutation m = new Mutation(rowString);

    m.put(cfString, cqString, cv, createValue(ingestInstanceId, count, prevRow, cksum));
    return m;
  }

  public static byte[] genCol(int cfInt) {
    return FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
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

  public static byte[] createValue(byte[] ingestInstanceId, long count, byte[] prevRow,
      Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte[] val = new byte[dataLen];
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;
    val[index++] = ':';
    int added = FastFormat.toZeroPaddedString(val, index, count, 16, 16, EMPTY_BYTES);
    if (added != 16)
      throw new RuntimeException(" " + added);
    index += 16;
    val[index++] = ':';
    if (prevRow != null) {
      System.arraycopy(prevRow, 0, val, index, prevRow.length);
      index += prevRow.length;
    }

    val[index++] = ':';

    if (cksum != null) {
      cksum.update(val, 0, index);
      cksum.getValue();
      FastFormat.toZeroPaddedString(val, index, cksum.getValue(), 8, 16, EMPTY_BYTES);
    }
    return val;
  }
}
