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
package org.apache.accumulo.testing.randomwalk.image;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class TableOp extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {

    // choose a table
    String tableName;
    if (env.getRandom().nextInt(10) < 8) {
      tableName = state.getString("imageTableName");
    } else {
      tableName = state.getString("indexTableName");
    }

    // check if chosen table exists
    AccumuloClient client = env.getAccumuloClient();
    TableOperations tableOps = client.tableOperations();
    if (!tableOps.exists(tableName)) {
      log.error("Table " + tableName + " does not exist!");
      return;
    }

    // choose a random action
    if (env.getRandom().nextInt(10) > 6) {
      log.debug("Retrieving info for " + tableName);
      tableOps.getLocalityGroups(tableName);
      tableOps.getProperties(tableName);
      tableOps.listSplits(tableName);
      tableOps.list();
    } else {
      log.debug("Clearing locator cache for " + tableName);
      tableOps.clearLocatorCache(tableName);
    }

    if (env.getRandom().nextInt(10) < 3) {
      Map<String,Set<Text>> groups = tableOps.getLocalityGroups(state.getString("imageTableName"));

      if (groups.isEmpty()) {
        log.debug("Adding locality groups to " + state.getString("imageTableName"));
        groups = ImageFixture.getLocalityGroups();
      } else {
        log.debug("Removing locality groups from " + state.getString("imageTableName"));
        groups = new HashMap<>();
      }

      tableOps.setLocalityGroups(state.getString("imageTableName"), groups);
    }
  }
}
