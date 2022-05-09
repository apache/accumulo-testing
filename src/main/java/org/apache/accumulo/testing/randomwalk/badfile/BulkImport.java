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

import java.util.Properties;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Bulk Import the files into the configured table.
 */
public class BulkImport extends Test {

  /**
   * Tests both the legacy (deprecated) and new bulk import methods.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  public void visit(final State state, final RandWalkEnv env, Properties props) throws Exception {
    boolean useLegacyBulk = state.getBoolean("useLegacyBulk");
    boolean wroteBadFile = state.getBoolean("wroteBadFile");
    String badFile = state.getString("badFile");
    String tableName = state.getString("tableName");
    FileSystem fs = (FileSystem) state.get("fs");
    String dir = state.getString("bulkDir");

    Path fail = new Path(dir + "_fail");
    fs.mkdirs(fail);

    if (wroteBadFile) {
      log.debug("The bad file {}", badFile);
    }

    log.debug("Starting {} bulk import to {}", useLegacyBulk ? "legacy" : "new", tableName);
    try {
      if (useLegacyBulk) {
        env.getAccumuloClient().tableOperations().importDirectory(tableName, dir, fail.toString(),
            true);
        FileStatus[] failures = fs.listStatus(fail);
        if (failures != null && failures.length > 0) {
          log.info(failures.length + " failure files found importing files from " + dir);
        }
      } else {
        env.getAccumuloClient().tableOperations().importDirectory(dir).to(tableName).tableTime(true)
            .load();
      }

      // fs.delete(dir, true);
      // fs.delete(fail, true);
      log.debug("Finished {} bulk import to {}", useLegacyBulk ? "legacy" : "new", tableName);

      state.set("wroteBadFile", wroteBadFile);
    } catch (TableNotFoundException tnfe) {
      log.debug("Table {} was deleted", tableName);
      throw tnfe;
    }

    state.set("fail", fail.toString());
  }

}
