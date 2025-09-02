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
package org.apache.accumulo.testing.randomwalk.bulk;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

public class BulkPlusOne extends BulkImportTest {

  public static final int LOTS = 100000;
  public static final int ZONES = 50;
  public static final int ZONE_SIZE = LOTS / ZONES;
  public static final int COLS = 10;
  public static final int HEX_SIZE = (int) Math.ceil(Math.log(LOTS) / Math.log(16));
  public static final String FMT = "r%0" + HEX_SIZE + "x";
  public static final Text CHECK_COLUMN_FAMILY = new Text("cf");
  public static final List<Column> COLNAMES =
      IntStream.range(0, COLS).mapToObj(i -> String.format("%03d", i)).map(Text::new)
          .map(t -> new Column(CHECK_COLUMN_FAMILY, t)).collect(Collectors.toList());

  public static final Text MARKER_CF = new Text("marker");

  /**
   * Inclusive start exclusive end zone range.
   */
  record BulkRange(int startZone, int endZone) {
    public BulkRange {
      Preconditions.checkArgument(startZone >= 0 && startZone < endZone && endZone <= ZONES,
          "startZone:%s endZone:%s", startZone, endZone);
    }

    static BulkRange randomRange(Random random) {
      int start = random.nextInt(ZONES);
      int end = random.nextInt(ZONES);
      if (end < start) {
        int tmp = end;
        end = start;
        start = tmp;
      }
      if (end == start) {
        end++;
      }
      return new BulkRange(start, end);
    }
  }

  /**
   * Every increment range must also be decremented and visa versa. This ensures that happens as
   * threads doing bulk imports request ranges.
   */
  static class RangeExchange {
    private Deque<BulkRange> incrementRanges = new ArrayDeque<>();
    private Deque<BulkRange> decrementRanges = new ArrayDeque<>();

    synchronized BulkRange nextIncrementRange(RandWalkEnv env) {
      var next = incrementRanges.poll();
      if (next == null) {
        next = BulkRange.randomRange(env.getRandom());
        decrementRanges.push(next);
      }
      return next;
    }

    synchronized BulkRange nextDecrementRange(RandWalkEnv env) {
      var next = decrementRanges.poll();
      if (next == null) {
        next = BulkRange.randomRange(env.getRandom());
        incrementRanges.push(next);
      }
      return next;
    }

    synchronized boolean isEmpty() {
      return incrementRanges.isEmpty() && decrementRanges.isEmpty();
    }

    synchronized void clear() {
      incrementRanges.clear();
      decrementRanges.clear();
    }
  }

  static final RangeExchange rangeExchange = new RangeExchange();
  static final AtomicLong perZoneCounters[] = new AtomicLong[ZONES];
  static {
    for (int i = 0; i < perZoneCounters.length; i++) {
      perZoneCounters[i] = new AtomicLong(0);
    }
  }

  private static final Value ONE = new Value("1".getBytes());

  /**
   * Load a plus one or minus one into a random range of the table. Overall this test should load a
   * minus one into the same range for every plus one loaded (or visa versa) and at the end of the
   * test the sum should be zero. In order to aid with debugging data loss, this test loads markers
   * along with the plus one and minus ones. These markers help pin point which bulk load operation
   * was missing. The tables row range is divided into zones and each zone has a one up counter that
   * is used to generate markers. At the end of the test each zone in the table should have a
   * contiguous set of markers. If a marker is missing, then the logging from this method should be
   * consulted to determine the corresponding bulk import operation. Once the bulk import operation
   * is found it can be followed in the Accumulo server logs. The test does analysis to find missing
   * markers and prints holes. Like if the test prints that it saw marker 97 and 99 in zone 3, then
   * that means marker 98 is missing and the bulk import operation related to 98 needs to be found
   * in the test logs. When looking for missing markers, look for the correct zone in the test logs.
   *
   * All the logging in this method includes the bulk import directory uuid. If there is a problem,
   * then this directory uuid can be used to find the corresponding fate uuid in the accumulo server
   * logs. There should be a log message in the server logs that includes the bulk directory and the
   * fate uuid.
   */
  static void bulkLoadLots(Logger log, State state, RandWalkEnv env, BulkRange bulkRange,
      Value value) throws Exception {

    long[] markers = new long[ZONES];
    Arrays.fill(markers, -1);

    // Allocate a marker for each zone and build a log message that links the marker for each zone
    // to this bulk import directory name uuid.
    StringBuilder makersBuilder = new StringBuilder("[");
    String sep = "";
    for (int z = bulkRange.startZone; z < bulkRange.endZone; z++) {
      long zoneMarker = perZoneCounters[z].incrementAndGet();
      markers[z] = zoneMarker;
      makersBuilder.append(sep).append(z).append(":").append(String.format("%07d", zoneMarker));
      sep = ",";
    }
    makersBuilder.append("]");
    String markersLog = makersBuilder.toString();

    final UUID uuid = UUID.randomUUID();
    final FileSystem fs = (FileSystem) state.get("fs");
    final Path dir = new Path(fs.getUri() + "/tmp", "bulk_" + uuid);
    log.debug("{} bulk loading {} over {} from {}", uuid, value, bulkRange, dir);
    log.debug("{} zone markers:{}", uuid, markersLog);
    final int parts = env.getRandom().nextInt(10) + 1;

    // Must mutate all rows in the zone, so expand the start and end to encompass all the rows in
    // the zone.
    final int start = bulkRange.startZone * ZONE_SIZE;
    final int end = bulkRange.endZone * ZONE_SIZE;

    // The set created below should always contain 0. So its very important that zero is first in
    // concat below.
    TreeSet<Integer> startRows = Stream
        .concat(Stream.of(start),
            Stream.generate(() -> env.getRandom().nextInt(end - start) + start))
        .distinct().limit(parts).collect(Collectors.toCollection(TreeSet::new));

    List<String> printRows =
        startRows.stream().map(row -> String.format(FMT, row)).collect(Collectors.toList());

    log.debug("{} preparing bulk files with start rows {} last row {}", uuid, printRows,
        String.format(FMT, end));

    startRows.add(end);
    List<Integer> rows = new ArrayList<>(startRows);

    long currentZone = -1;
    Text markerColumnQualifier = null;

    for (int i = 0; i < parts; i++) {
      String fileName = dir + "/" + String.format("part_%d.rf", i);

      log.trace("creating {}", fileName);
      try (RFileWriter writer = RFile.newWriter().to(fileName).withFileSystem(fs).build()) {
        writer.startDefaultLocalityGroup();
        int partStart = rows.get(i);
        int partEnd = rows.get(i + 1);
        int eCount = 0;
        for (int j = partStart; j < partEnd; j++) {
          int zone = j / ZONE_SIZE;
          if (currentZone != zone) {
            Preconditions.checkState(markers[zone] > 0, "%s %s %s", zone, j, bulkRange);
            markerColumnQualifier = new Text(String.format("%07d", markers[zone]));
            currentZone = zone;
          }

          Text row = new Text(String.format(FMT, j));
          for (Column col : COLNAMES) {
            writer.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier()), value);
            eCount++;
          }
          writer.append(new Key(row, MARKER_CF, markerColumnQualifier), ONE);
          eCount++;
        }
        log.debug("{} created {} with {} entries", uuid, fileName, eCount);
      }
    }
    env.getAccumuloClient().tableOperations().importDirectory(dir.toString())
        .to(Setup.getTableName()).tableTime(true).load();
    fs.delete(dir, true);
    log.debug("{} Finished bulk import", uuid);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void runLater(State state, RandWalkEnv env) throws Exception {
    var bulkRange = rangeExchange.nextIncrementRange(env);
    bulkLoadLots(log, state, env, bulkRange, ONE);
  }
}
