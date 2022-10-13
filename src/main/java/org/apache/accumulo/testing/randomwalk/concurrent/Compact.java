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

public class Compact extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    Random rand = state.getRandom();
    String tableName = state.getRandomTableName();
    List<Text> range = ConcurrentFixture.generateRange(rand);

    try {
      boolean wait = rand.nextBoolean();
      client.tableOperations().compact(tableName, range.get(0), range.get(1), false, wait);
      log.debug((wait ? "compacted " : "initiated compaction ") + tableName + " from "
          + range.get(0) + " to " + range.get(1));
    } catch (TableNotFoundException tne) {
      log.debug("compact " + tableName + " from " + range.get(0) + " to " + range.get(1)
          + " failed, doesnt exist");
    } catch (TableOfflineException toe) {
      log.debug("compact " + tableName + " from " + range.get(0) + " to " + range.get(1)
          + " failed, offline");
    }

  }
}
