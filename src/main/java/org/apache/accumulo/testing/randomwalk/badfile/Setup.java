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
package org.apache.accumulo.testing.randomwalk.badfile;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Set up the Bad File RW Test. This test aims to create a bad RFile and ingest it into the cluster
 * The tableName, rows, cols and useLegacyBulk can be configured in the XML file BadFile.xml. The
 * idea is that this test can be used with the other RW tests, to introduce problems into the test
 * cluster. It will be easier to use tests that have a static table name like the Conditional test,
 * which uses "banks" or run another test, then run this test.
 *
 * The badFile is a generated R-File that isn't closed properly. This can cause problems for the
 * legacy bulk import. The new bulk import will catch the bad file at the client.
 */
public class Setup extends Test {
  public static final Text MARKER_CF = new Text("marker");
  static final AtomicLong counter = new AtomicLong();

  private static final Value ONE = new Value("1".getBytes());

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String tableName = props.getProperty("tableName", "BadFileRW");
    state.set("tableName", tableName);
    boolean useLegacyBulk = Boolean.parseBoolean(props.getProperty("useLegacyBulk", "true"));
    state.set("useLegacyBulk", useLegacyBulk);
    log.info("Setup test with table {} useLegacyBulk = {}", tableName, useLegacyBulk);

    try {
      env.getAccumuloClient().tableOperations().create(tableName);
    } catch (Exception e) {
      log.debug("Unable to create {} {}", tableName, e.getMessage());
    }

    int ROWS = Integer.parseInt(props.getProperty("rows", "1000000"));
    int COLS = Integer.parseInt(props.getProperty("cols", "10"));
    List<IteratorSetting.Column> COLNAMES = new ArrayList<>();
    for (int i = 0; i < COLS; i++) {
      var col = new IteratorSetting.Column(new Text("cf"), new Text(String.format("%03d", i)));
      COLNAMES.add(col);
    }

    Random rand = new Random();
    final Path dir = new Path("/bulk/files");
    final FileSystem fs = FileSystem.get(new Configuration());
    fs.mkdirs(dir);
    final int parts = rand.nextInt(10) + 1;
    boolean wroteBadFile = false;
    String badFile = null;
    String markerColumnQualifier = String.format("%07d", counter.incrementAndGet());

    TreeSet<String> rows = new TreeSet<>();
    for (int i = 0; i < ROWS; i++)
      rows.add(String.format("row_%06d", i));

    log.info("Creating RFiles with {} Rows", ROWS);
    for (int i = 0; i < parts; i++) {
      String fileName = dir + "/" + String.format("part_%d.rf", i);
      RFileWriter f = RFile.newWriter().to(fileName).withFileSystem(fs).build();
      f.startDefaultLocalityGroup();
      for (String r : rows) {
        Text row = new Text(r);
        for (IteratorSetting.Column col : COLNAMES) {
          f.append(new Key(row, col.getColumnFamily(), col.getColumnQualifier()), ONE);
        }
        f.append(new Key(row, MARKER_CF, new Text(markerColumnQualifier)), ONE);
      }
      if (i == parts - 1 && !wroteBadFile) {
        // skip close to corrupt file
        log.info("Skipping close on " + fileName);
        badFile = fileName;
        wroteBadFile = true;
      } else {
        f.close();
        log.debug("Wrote RFile {}", fileName);
      }
    }

    state.set("wroteBadFile", wroteBadFile);
    state.set("badFile", badFile);
    state.set("bulkDir", dir.toString());
    state.set("fs", fs);
  }
}
