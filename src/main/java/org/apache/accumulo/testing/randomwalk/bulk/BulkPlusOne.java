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

import java.util.ArrayList;
import java.util.List;
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

public class BulkPlusOne extends BulkImportTest {

  public static final int LOTS = 100000;
  public static final int COLS = 10;
  public static final int HEX_SIZE = (int) Math.ceil(Math.log(LOTS) / Math.log(16));
  public static final String FMT = "r%0" + HEX_SIZE + "x";
  public static final Text CHECK_COLUMN_FAMILY = new Text("cf");
  public static final List<Column> COLNAMES = IntStream.range(0, COLS)
      .mapToObj(i -> String.format("%03d", i)).map(Text::new)
      .map(t -> new Column(CHECK_COLUMN_FAMILY, t)).collect(Collectors.toList());

  public static final Text MARKER_CF = new Text("marker");
  static final AtomicLong counter = new AtomicLong();

  private static final Value ONE = new Value("1".getBytes());

  static void bulkLoadLots(Logger log, State state, RandWalkEnv env, Value value) throws Exception {
    final FileSystem fs = (FileSystem) state.get("fs");
    final Path dir = new Path(fs.getUri() + "/tmp", "bulk_" + UUID.randomUUID());
    log.debug("Bulk loading from {}", dir);
    final int parts = env.getRandom().nextInt(10) + 1;

    // The set created below should always contain 0. So its very important that zero is first in
    // concat below.
    TreeSet<Integer> startRows = Stream
        .concat(Stream.of(0), Stream.generate(() -> env.getRandom().nextInt(LOTS))).distinct()
        .limit(parts).collect(Collectors.toCollection(TreeSet::new));

    List<String> printRows = startRows.stream().map(row -> String.format(FMT, row))
        .collect(Collectors.toList());

    String markerColumnQualifier = String.format("%07d", counter.incrementAndGet());
    log.debug("preparing bulk files with start rows " + printRows + " last row "
        + String.format(FMT, LOTS - 1) + " marker " + markerColumnQualifier);

    List<Integer> rows = new ArrayList<>(startRows);
    rows.add(LOTS);

    for (int i = 0; i < parts; i++) {
      String fileName = dir + "/" + String.format("part_%d.rf", i);

      log.debug("Creating {}", fileName);
      try (RFileWriter writer = RFile.newWriter().to(fileName).withFileSystem(fs).build()) {
        writer.startDefaultLocalityGroup();
        int start = rows.get(i);
        int end = rows.get(i + 1);
        for (int j = start; j < end; j++) {
          Text row = new Text(String.format(FMT, j));
          for (Column col : COLNAMES) {
            writer.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier()), value);
          }
          writer.append(new Key(row, MARKER_CF, new Text(markerColumnQualifier)), ONE);
        }
      }
    }
    env.getAccumuloClient().tableOperations().importDirectory(dir.toString())
        .to(Setup.getTableName()).tableTime(true).load();
    fs.delete(dir, true);
    log.debug("Finished bulk import, start rows " + printRows + " last row "
        + String.format(FMT, LOTS - 1) + " marker " + markerColumnQualifier);
  }

  @Override
  protected void runLater(State state, RandWalkEnv env) throws Exception {
    log.info("Incrementing");
    bulkLoadLots(log, state, env, ONE);
  }

}
