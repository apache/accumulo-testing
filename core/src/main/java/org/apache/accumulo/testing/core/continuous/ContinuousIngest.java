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
package org.apache.accumulo.testing.core.continuous;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.CountSampler;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static int getPauseWaitSec(Properties props, Random rand) {
    int waitMin = Integer.parseInt(props.getProperty(TestProps.CI_INGEST_PAUSE_WAIT_MIN));
    int waitMax = Integer.parseInt(props.getProperty(TestProps.CI_INGEST_PAUSE_WAIT_MAX));
    Preconditions.checkState(waitMax >= waitMin && waitMin > 0);
    if (waitMax == waitMin) {
      return waitMin;
    }
    return (rand.nextInt(waitMax - waitMin) + waitMin);
  }

  private static int getPauseDurationSec(Properties props, Random rand) {
    int durationMin = Integer.parseInt(props.getProperty(TestProps.CI_INGEST_PAUSE_DURATION_MIN));
    int durationMax = Integer.parseInt(props.getProperty(TestProps.CI_INGEST_PAUSE_DURATION_MAX));
    Preconditions.checkState(durationMax >= durationMin && durationMin > 0);
    if (durationMax == durationMin) {
      return durationMin;
    }
    return (rand.nextInt(durationMax - durationMin) + durationMin);
  }

  private static void pauseCheck(Properties props, Random rand) throws InterruptedException {
    if (pauseEnabled(props)) {
      long elapsedNano = System.nanoTime() - lastPauseNs;
      if (elapsedNano > (TimeUnit.SECONDS.toNanos(pauseWaitSec))) {
        long pauseDurationSec = getPauseDurationSec(props, rand);
        log.info("PAUSING for " + pauseDurationSec + "s");
        Thread.sleep(TimeUnit.SECONDS.toMillis(pauseDurationSec));
        lastPauseNs = System.nanoTime();
        pauseWaitSec = getPauseWaitSec(props, rand);
        log.info("INGESTING for " + pauseWaitSec + "s");
      }
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.err.println("Usage: ContinuousIngest <testPropsPath> <clientPropsPath>");
      System.exit(-1);
    }

    ContinuousEnv env = new ContinuousEnv(args[0], args[1]);

    String vis = env.getTestProperty(TestProps.CI_INGEST_VISIBILITIES);
    if (vis == null) {
      visibilities = Collections.singletonList(new ColumnVisibility());
    } else {
      visibilities = new ArrayList<>();
      for (String v : vis.split(",")) {
        visibilities.add(new ColumnVisibility(v.trim()));
      }
    }

    long rowMin = env.getRowMin();
    long rowMax = env.getRowMax();
    if (rowMin < 0 || rowMax < 0 || rowMax <= rowMin) {
      throw new IllegalArgumentException("bad min and max");
    }

    Connector conn = env.getAccumuloConnector();
    String tableName = env.getAccumuloTableName();
    if (!conn.tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, "Consult the README and create the table before starting ingest.");
    }

    BatchWriter bw = conn.createBatchWriter(tableName);
    bw = Trace.wrapAll(bw, new CountSampler(1024));

    Random r = new Random();

    byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes(UTF_8);

    log.info(String.format("UUID %d %s", System.currentTimeMillis(), new String(ingestInstanceId, UTF_8)));

    long count = 0;
    final int flushInterval = 1000000;
    final int maxDepth = 25;

    // always want to point back to flushed data. This way the previous item
    // should
    // always exist in accumulo when verifying data. To do this make insert
    // N point
    // back to the row from insert (N - flushInterval). The array below is
    // used to keep
    // track of this.
    long prevRows[] = new long[flushInterval];
    long firstRows[] = new long[flushInterval];
    int firstColFams[] = new int[flushInterval];
    int firstColQuals[] = new int[flushInterval];

    long lastFlushTime = System.currentTimeMillis();

    int maxColF = env.getMaxColF();
    int maxColQ = env.getMaxColQ();
    boolean checksum = Boolean.parseBoolean(env.getTestProperty(TestProps.CI_INGEST_CHECKSUM));
    long numEntries = Long.parseLong(env.getTestProperty(TestProps.CI_INGEST_CLIENT_ENTRIES));

    Properties testProps = env.getTestProperties();
    if (pauseEnabled(testProps)) {
      lastPauseNs = System.nanoTime();
      pauseWaitSec = getPauseWaitSec(testProps, r);
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

      // generate subsequent sets of nodes that link to previous set of
      // nodes
      for (int depth = 1; depth < maxDepth; depth++) {
        for (int index = 0; index < flushInterval; index++) {
          long rowLong = genLong(rowMin, rowMax, r);
          byte[] prevRow = genRow(prevRows[index]);
          prevRows[index] = rowLong;
          Mutation m = genMutation(rowLong, r.nextInt(maxColF), r.nextInt(maxColQ), cv, ingestInstanceId, count, prevRow, checksum);
          count++;
          bw.addMutation(m);
        }

        lastFlushTime = flush(bw, count, flushInterval, lastFlushTime);
        if (count >= numEntries)
          break out;
        pauseCheck(testProps, r);
      }

      // create one big linked list, this makes all of the first inserts
      // point to something
      for (int index = 0; index < flushInterval - 1; index++) {
        Mutation m = genMutation(firstRows[index], firstColFams[index], firstColQuals[index], cv, ingestInstanceId, count, genRow(prevRows[index + 1]),
            checksum);
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

  private static long flush(BatchWriter bw, long count, final int flushInterval, long lastFlushTime) throws MutationsRejectedException {
    long t1 = System.currentTimeMillis();
    bw.flush();
    long t2 = System.currentTimeMillis();
    log.info(String.format("FLUSH %d %d %d %d %d", t2, (t2 - lastFlushTime), (t2 - t1), count, flushInterval));
    lastFlushTime = t2;
    return lastFlushTime;
  }

  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv, byte[] ingestInstanceId, long count, byte[] prevRow,
      boolean checksum) {
    // Adler32 is supposed to be faster, but according to wikipedia is not
    // good for small data.... so used CRC32 instead
    CRC32 cksum = null;

    byte[] rowString = genRow(rowLong);

    byte[] cfString = FastFormat.toZeroPaddedString(cfInt, 4, 16, EMPTY_BYTES);
    byte[] cqString = FastFormat.toZeroPaddedString(cqInt, 4, 16, EMPTY_BYTES);

    if (checksum) {
      cksum = new CRC32();
      cksum.update(rowString);
      cksum.update(cfString);
      cksum.update(cqString);
      cksum.update(cv.getExpression());
    }

    Mutation m = new Mutation(new Text(rowString));

    m.put(new Text(cfString), new Text(cqString), cv, createValue(ingestInstanceId, count, prevRow, cksum));
    return m;
  }

  public static long genLong(long min, long max, Random r) {
    return ((r.nextLong() & 0x7fffffffffffffffL) % (max - min)) + min;
  }

  static byte[] genRow(long min, long max, Random r) {
    return genRow(genLong(min, max, r));
  }

  static byte[] genRow(long rowLong) {
    return FastFormat.toZeroPaddedString(rowLong, 16, 16, EMPTY_BYTES);
  }

  private static Value createValue(byte[] ingestInstanceId, long count, byte[] prevRow, Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte val[] = new byte[dataLen];
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

    // System.out.println("val "+new String(val));

    return new Value(val);
  }
}
