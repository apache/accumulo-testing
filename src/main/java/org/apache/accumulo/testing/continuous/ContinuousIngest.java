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
import org.apache.accumulo.testing.TestProps;
import org.apache.accumulo.testing.util.FastFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ContinuousIngest {

  private static final Logger log = LoggerFactory.getLogger(ContinuousIngest.class);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static List<ColumnVisibility> visibilities;
  private static long lastPauseNs;
  private static long pauseWaitSec;
  private static boolean pauseEnabled;
  private static int pauseMin;
  private static int pauseMax;

  private static ColumnVisibility getVisibility(Random rand) {
    return visibilities.get(rand.nextInt(visibilities.size()));
  }

  private static boolean pauseEnabled(Properties props) {
    String value = props.getProperty(TestProps.CI_INGEST_PAUSE_ENABLED);
    return Boolean.parseBoolean(value);
  }

  private static int getPause(Random rand) {
    if (pauseMax == pauseMin) {
      return pauseMin;
    }
    return (rand.nextInt(pauseMax - pauseMin) + pauseMin);
  }

  private static float getDeleteProbability(Properties props) {
    String stringValue = props.getProperty(TestProps.CI_INGEST_DELETE_PROBABILITY);
    float prob = Float.parseFloat(stringValue);
    Preconditions.checkArgument(prob >= 0.0 && prob <= 1.0,
        "Delete probability should be between 0.0 and 1.0");
    return prob;
  }

  private static int getFlushEntries(Properties props) {
    return Integer.parseInt(props.getProperty(TestProps.CI_INGEST_FLUSH_ENTRIES, "1000000"));
  }

  private static void pauseCheck(Random rand) throws InterruptedException {
    if (pauseEnabled) {
      long elapsedNano = System.nanoTime() - lastPauseNs;
      if (elapsedNano > (TimeUnit.SECONDS.toNanos(pauseWaitSec))) {
        long pauseDurationSec = getPause(rand);
        log.info("PAUSING for {}s", pauseDurationSec);
        Thread.sleep(TimeUnit.SECONDS.toMillis(pauseDurationSec));
        lastPauseNs = System.nanoTime();
        pauseWaitSec = getPause(rand);
        log.info("INGESTING for {}s", pauseWaitSec);
      }
    }
  }

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {

      AccumuloClient client = env.getAccumuloClient();

      final long rowMin = env.getRowMin();
      final long rowMax = env.getRowMax();
      Preconditions.checkState(0 <= rowMin && rowMin <= rowMax,
          "Bad rowMin/rowMax, must conform to: 0 <= rowMin <= rowMax");

      String tableName = env.getAccumuloTableName();
      if (!client.tableOperations().exists(tableName)) {
        throw new TableNotFoundException(null, tableName,
            "Consult the README and create the table before starting ingest.");
      }

      byte[] ingestInstanceId = UUID.randomUUID().toString().getBytes(UTF_8);
      log.info("Ingest instance ID: {} current time: {}ms", new String(ingestInstanceId, UTF_8),
          System.currentTimeMillis());

      Properties testProps = env.getTestProperties();

      long entriesWritten = 0L;
      long entriesDeleted = 0L;
      final int flushInterval = getFlushEntries(testProps);
      log.info("A flush will occur after every {} entries written", flushInterval);
      final int maxDepth = 25;

      // always want to point back to flushed data. This way the previous item should
      // always exist in accumulo when verifying data. To do this make insert N point
      // back to the row from insert (N - flushInterval). The array below is used to keep
      // track of all inserts.
      MutationInfo[][] nodeMap = new MutationInfo[maxDepth][flushInterval];

      long lastFlushTime = System.currentTimeMillis();

      final int maxColF = env.getMaxColF();
      final int maxColQ = env.getMaxColQ();
      final boolean checksum = Boolean
          .parseBoolean(testProps.getProperty(TestProps.CI_INGEST_CHECKSUM));
      final long numEntries = Long
          .parseLong(testProps.getProperty(TestProps.CI_INGEST_CLIENT_ENTRIES));
      log.info("Total entries to be written: {}", numEntries);

      visibilities = parseVisibilities(testProps.getProperty(TestProps.CI_INGEST_VISIBILITIES));

      pauseEnabled = pauseEnabled(testProps);

      pauseMin = Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_PAUSE_WAIT_MIN));
      pauseMax = Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_PAUSE_WAIT_MAX));
      Preconditions.checkState(0 < pauseMin && pauseMin <= pauseMax,
          "Bad pause wait min/max, must conform to: 0 < min <= max");

      if (pauseEnabled) {
        lastPauseNs = System.nanoTime();
        pauseWaitSec = getPause(env.getRandom());
        log.info("PAUSING enabled");
        log.info("INGESTING for {}s", pauseWaitSec);
      }

      final float deleteProbability = getDeleteProbability(testProps);
      log.info("DELETES will occur with a probability of {}",
          String.format("%.02f", deleteProbability));

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        out: while (true) {
          ColumnVisibility cv = getVisibility(env.getRandom());

          // generate sets nodes that link to previous set of nodes
          for (int depth = 0; depth < maxDepth; depth++) {
            for (int index = 0; index < flushInterval; index++) {
              long rowLong = genLong(rowMin, rowMax, env.getRandom());

              byte[] prevRow = depth == 0 ? null : genRow(nodeMap[depth - 1][index].row);

              int cfInt = env.getRandom().nextInt(maxColF);
              int cqInt = env.getRandom().nextInt(maxColQ);

              nodeMap[depth][index] = new MutationInfo(rowLong, cfInt, cqInt);
              Mutation m = genMutation(rowLong, cfInt, cqInt, cv, ingestInstanceId, entriesWritten,
                  prevRow, checksum);
              entriesWritten++;
              bw.addMutation(m);
            }

            lastFlushTime = flush(bw, entriesWritten, entriesDeleted, lastFlushTime);
            if (entriesWritten >= numEntries)
              break out;
            pauseCheck(env.getRandom());
          }

          // random chance that the entries will be deleted
          final boolean delete = env.getRandom().nextFloat() < deleteProbability;

          // if the previously written entries are scheduled to be deleted
          if (delete) {
            log.info("Deleting last portion of written entries");
            // add delete mutations in the reverse order in which they were written
            for (int depth = nodeMap.length - 1; depth >= 0; depth--) {
              for (int index = nodeMap[depth].length - 1; index >= 0; index--) {
                MutationInfo currentNode = nodeMap[depth][index];
                Mutation m = new Mutation(genRow(currentNode.row));
                m.putDelete(genCol(currentNode.cf), genCol(currentNode.cq));
                entriesDeleted++;
                bw.addMutation(m);
              }
              lastFlushTime = flush(bw, entriesWritten, entriesDeleted, lastFlushTime);
              pauseCheck(env.getRandom());
            }
          } else {
            // create one big linked list, this makes all the first inserts point to something
            for (int index = 0; index < flushInterval - 1; index++) {
              MutationInfo firstEntry = nodeMap[0][index];
              MutationInfo lastEntry = nodeMap[maxDepth - 1][index + 1];
              Mutation m = genMutation(firstEntry.row, firstEntry.cf, firstEntry.cq, cv,
                  ingestInstanceId, entriesWritten, genRow(lastEntry.row), checksum);
              entriesWritten++;
              bw.addMutation(m);
            }
            lastFlushTime = flush(bw, entriesWritten, entriesDeleted, lastFlushTime);
          }

          if (entriesWritten >= numEntries)
            break out;
          pauseCheck(env.getRandom());
        }
      }
    }
  }

  private static class MutationInfo {

    long row;
    int cf;
    int cq;

    public MutationInfo(long row, int cf, int cq) {
      this.row = row;
      this.cf = cf;
      this.cq = cq;
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

  private static long flush(BatchWriter bw, long entriesWritten, long entriesDeleted,
      long lastFlushTime) throws MutationsRejectedException {
    long t1 = System.currentTimeMillis();
    bw.flush();
    long t2 = System.currentTimeMillis();
    log.info("FLUSH - duration: {}ms, since last flush: {}ms, total written: {}, total deleted: {}",
        (t2 - t1), (t2 - lastFlushTime), entriesWritten, entriesDeleted);
    return t2;
  }

  public static Mutation genMutation(long rowLong, int cfInt, int cqInt, ColumnVisibility cv,
      byte[] ingestInstanceId, long entriesWritten, byte[] prevRow, boolean checksum) {
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

    m.put(cfString, cqString, cv, createValue(ingestInstanceId, entriesWritten, prevRow, cksum));
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

  public static byte[] createValue(byte[] ingestInstanceId, long entriesWritten, byte[] prevRow,
      Checksum cksum) {
    int dataLen = ingestInstanceId.length + 16 + (prevRow == null ? 0 : prevRow.length) + 3;
    if (cksum != null)
      dataLen += 8;
    byte[] val = new byte[dataLen];
    System.arraycopy(ingestInstanceId, 0, val, 0, ingestInstanceId.length);
    int index = ingestInstanceId.length;
    val[index++] = ':';
    int added = FastFormat.toZeroPaddedString(val, index, entriesWritten, 16, 16, EMPTY_BYTES);
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
