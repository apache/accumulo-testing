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
package org.apache.accumulo.testing.randomwalk.shard;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class ExportIndex extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {

    String indexTableName = state.getString("indexTableName");
    String tmpIndexTableName = indexTableName + "_tmp";

    Path exportDir = new Path("/tmp/shard_export/" + indexTableName);
    Path copyDir = new Path("/tmp/shard_export/" + tmpIndexTableName);

    FileSystem fs = FileSystem.get(env.getHadoopConfiguration());

    fs.delete(exportDir, true);
    fs.delete(copyDir, true);

    // disable spits, so that splits can be compared later w/o worrying one
    // table splitting and the other not
    env.getAccumuloClient().tableOperations().setProperty(indexTableName,
        Property.TABLE_SPLIT_THRESHOLD.getKey(), "20G");

    long t1 = System.currentTimeMillis();

    env.getAccumuloClient().tableOperations().flush(indexTableName, null, null, true);
    env.getAccumuloClient().tableOperations().offline(indexTableName);

    long t2 = System.currentTimeMillis();

    env.getAccumuloClient().tableOperations().exportTable(indexTableName, exportDir.toString());

    long t3 = System.currentTimeMillis();

    // copy files
    try (FSDataInputStream fsDataInputStream = fs.open(new Path(exportDir, "distcp.txt"));
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream, UTF_8);
        BufferedReader reader = new BufferedReader(inputStreamReader)) {
      String file;
      while ((file = reader.readLine()) != null) {
        Path src = new Path(file);
        Path dest = new Path(copyDir, src.getName());
        FileUtil.copy(fs, src, fs, dest, false, true, env.getHadoopConfiguration());
      }
    }

    long t4 = System.currentTimeMillis();

    env.getAccumuloClient().tableOperations().online(indexTableName);
    env.getAccumuloClient().tableOperations().importTable(tmpIndexTableName, copyDir.toString());

    long t5 = System.currentTimeMillis();

    fs.delete(exportDir, true);
    fs.delete(copyDir, true);

    HashSet<Text> splits1 = new HashSet<>(
        env.getAccumuloClient().tableOperations().listSplits(indexTableName));
    HashSet<Text> splits2 = new HashSet<>(
        env.getAccumuloClient().tableOperations().listSplits(tmpIndexTableName));

    if (!splits1.equals(splits2))
      throw new Exception("Splits not equals " + indexTableName + " " + tmpIndexTableName);

    HashMap<String,String> props1 = getPropsFromTable(indexTableName, env);
    HashMap<String,String> props2 = getPropsFromTable(tmpIndexTableName, env);

    if (!props1.equals(props2))
      throw new Exception("Props not equals " + indexTableName + " " + tmpIndexTableName);

    // unset the split threshold
    env.getAccumuloClient().tableOperations().removeProperty(indexTableName,
        Property.TABLE_SPLIT_THRESHOLD.getKey());
    env.getAccumuloClient().tableOperations().removeProperty(tmpIndexTableName,
        Property.TABLE_SPLIT_THRESHOLD.getKey());

    log.debug("Imported " + tmpIndexTableName + " from " + indexTableName + " flush: " + (t2 - t1)
        + "ms export: " + (t3 - t2) + "ms copy:" + (t4 - t3) + "ms import:" + (t5 - t4) + "ms");

  }

  private static HashMap<String,String> getPropsFromTable(String tableName, RandWalkEnv env)
      throws AccumuloException, TableNotFoundException {
    return new HashMap<>() {
      {
        for (var entry : env.getAccumuloClient().tableOperations().getProperties(tableName))
          put(entry.getKey(), entry.getValue());
      }
    };
  }

}
