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
package org.apache.accumulo.testing.randomwalk.multitable;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

public class CopyTable extends Test {

  private final TreeSet<Text> splits;

  public CopyTable() {
    splits = new TreeSet<>();
    for (int i = 1; i < 10; i++) {
      splits.add(new Text(Integer.toString(i)));
    }
  }

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {

    @SuppressWarnings("unchecked")
    List<String> tables = (List<String>) state.get("tableList");
    if (tables.isEmpty())
      return;

    Random rand = new Random();
    String srcTableName = tables.remove(rand.nextInt(tables.size()));

    int nextId = ((Integer) state.get("nextId")).intValue();
    String dstTableName = String.format("%s_%d", state.getString("tableNamePrefix"), nextId);

    String[] args = new String[3];
    args[0] = env.getClientPropsPath();
    args[1] = srcTableName;
    args[2] = dstTableName;

    log.debug("copying " + srcTableName + " to " + dstTableName);

    env.getAccumuloClient().tableOperations().create(dstTableName);

    env.getAccumuloClient().tableOperations().addSplits(dstTableName, splits);

    if (ToolRunner.run(env.getHadoopConfiguration(), new CopyTool(), args) != 0) {
      log.error("Failed to run map/red verify");
      return;
    }

    String tableId = env.getAccumuloClient().tableOperations().tableIdMap().get(dstTableName);
    log.debug("copied " + srcTableName + " to " + dstTableName + " (id - " + tableId + " )");

    tables.add(dstTableName);

    env.getAccumuloClient().tableOperations().delete(srcTableName);
    log.debug("dropped " + srcTableName);

    nextId++;
    state.set("nextId", Integer.valueOf(nextId));
  }
}
