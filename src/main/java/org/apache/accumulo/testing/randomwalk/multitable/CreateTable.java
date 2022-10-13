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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class CreateTable extends Test {

  private final NewTableConfiguration ntc;

  public CreateTable() {
    TreeSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 10; i++) {
      splits.add(new Text(Integer.toString(i)));
    }
    for (String s : "a b c d e f".split(" ")) {
      splits.add(new Text(s));
    }
    this.ntc = new NewTableConfiguration().withSplits(splits);
  }

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();

    int nextId = state.getInteger("nextId");
    String tableName = String.format("%s_%d", state.getString("tableNamePrefix"), nextId);
    try {
      // Create table with some splits to make the server's life easier
      client.tableOperations().create(tableName, ntc);
      String tableId = client.tableOperations().tableIdMap().get(tableName);
      log.debug("created table {} (id:{}) with {} splits", tableName, tableId,
          ntc.getSplits().size());

      @SuppressWarnings("unchecked")
      List<String> tables = (List<String>) state.get("tableList");
      tables.add(tableName);
    } catch (TableExistsException e) {
      log.warn("Failed to create " + tableName + " as it already exists");
    }

    nextId++;
    state.set("nextId", nextId);
  }
}
