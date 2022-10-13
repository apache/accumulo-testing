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
package org.apache.accumulo.testing.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class DeleteRange extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    Random rand = state.getRandom();
    String tableName = state.getRandomTableName();

    List<Text> range = new ArrayList<>();
    do {
      range.add(new Text(String.format("%016x", rand.nextLong() & 0x7fffffffffffffffL)));
      range.add(new Text(String.format("%016x", rand.nextLong() & 0x7fffffffffffffffL)));
    } while (range.get(0).equals(range.get(1)));
    Collections.sort(range);
    if (rand.nextInt(20) == 0)
      range.set(0, null);
    if (rand.nextInt(20) == 0)
      range.set(1, null);

    try {
      client.tableOperations().deleteRows(tableName, range.get(0), range.get(1));
      log.debug("deleted rows (" + range.get(0) + " -> " + range.get(1) + "] in " + tableName);
    } catch (TableNotFoundException tne) {
      log.debug("deleted rows " + tableName + " failed, table doesn't exist");
    } catch (TableOfflineException toe) {
      log.debug("deleted rows " + tableName + " failed, table offline");
    }
  }
}
