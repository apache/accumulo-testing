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
package org.apache.accumulo.testing.randomwalk.multitable;

import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class BulkImport extends Test {

  public static final Text CHECK_COLUMN_FAMILY = new Text("cf");
  public static final int ROWS = 1_000_000;
  public static final int COLS = 10;
  public static final List<Column> COLNAMES = IntStream.range(0, COLS)
      .mapToObj(i -> String.format("%03d", i)).map(Text::new)
      .map(t -> new Column(CHECK_COLUMN_FAMILY, t)).collect(Collectors.toList());

  public static final Text MARKER_CF = new Text("marker");
  static final AtomicLong counter = new AtomicLong();

  private static final Value ONE = new Value("1".getBytes());

  /**
   * Tests both the legacy (deprecated) and new bulk import methods.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  public void visit(final State state, final RandWalkEnv env, Properties props) throws Exception {
    List<String> tables = (List<String>) state.get("tableList");

    if (tables.isEmpty()) {
      log.trace("No tables to ingest into");
      return;
    }

    String tableName = tables.get(env.getRandom().nextInt(tables.size()));

    String uuid = UUID.randomUUID().toString();
    final Path dir = new Path("/tmp/bulk", uuid);
    final Path fail = new Path(dir + "_fail");
    final FileSystem fs = (FileSystem) state.get("fs");
    fs.mkdirs(fail);
    final int parts = env.getRandom().nextInt(10) + 1;
    final boolean useLegacyBulk = env.getRandom().nextBoolean();

    TreeSet<String> rows = new TreeSet<>();
    for (int i = 0; i < ROWS; i++)
      rows.add(uuid + String.format("__%06d", i));

    String markerColumnQualifier = String.format("%07d", counter.incrementAndGet());
    log.debug("Preparing {} bulk import to {}", useLegacyBulk ? "legacy" : "new", tableName);

    for (int i = 0; i < parts; i++) {
      String fileName = dir + "/" + String.format("part_%d.rf", i);
      try (RFileWriter f = RFile.newWriter().to(fileName).withFileSystem(fs).build()) {
        f.startDefaultLocalityGroup();
        for (String r : rows) {
          Text row = new Text(r);
          for (Column col : COLNAMES) {
            f.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier()), ONE);
          }
          f.append(new Key(row, MARKER_CF, new Text(markerColumnQualifier)), ONE);
        }
      }
    }
    log.debug("Starting {} bulk import to {}", useLegacyBulk ? "legacy" : "new", tableName);
    try {
      if (useLegacyBulk) {
        env.getAccumuloClient().tableOperations().importDirectory(tableName, dir.toString(),
            fail.toString(), true);
        FileStatus[] failures = fs.listStatus(fail);
        if (failures != null && failures.length > 0) {
          state.set("bulkImportSuccess", "false");
          throw new Exception(failures.length + " failure files found importing files from " + dir);
        }
      } else {
        env.getAccumuloClient().tableOperations().importDirectory(dir.toString()).to(tableName)
            .tableTime(true).load();
      }

      fs.delete(dir, true);
      fs.delete(fail, true);
      log.debug("Finished {} bulk import to {} start: {} last: {} marker: {}",
          useLegacyBulk ? "legacy" : "new", tableName, rows.first(), rows.last(),
          markerColumnQualifier);
    } catch (TableNotFoundException tnfe) {
      log.debug("Table {} was deleted", tableName);
      tables.remove(tableName);
    }
  }

}
