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

import static java.util.stream.Collectors.toCollection;

import java.net.InetAddress;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.testing.randomwalk.Fixture;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class MultiTableFixture extends Fixture {

  @Override
  public void setUp(State state, RandWalkEnv env) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String prefix = String.format("multi_%s", hostname);

    Set<String> all = env.getAccumuloClient().tableOperations().list();
    List<String> tableList = all.stream().filter(s -> s.startsWith(prefix))
        .collect(toCollection(CopyOnWriteArrayList::new));

    log.debug("Existing MultiTables: {}", tableList);
    // get the max of the last ID created
    OptionalInt optionalInt = tableList.stream().mapToInt(s -> {
      String[] strArr = s.split("_");
      return Integer.parseInt(strArr[strArr.length - 1]);
    }).max();
    int nextId = optionalInt.orElse(-1) + 1;
    log.debug("Next ID started at {}", nextId);

    state.set("tableNamePrefix", prefix);
    state.set("nextId", nextId);
    state.set("numWrites", 0L);
    state.set("totalWrites", 0L);
    state.set("tableList", tableList);
    state.set("nextId", 0);
    state.set("numWrites", 0L);
    state.set("totalWrites", 0L);
    state.set("tableList", new CopyOnWriteArrayList<String>());
    state.set("fs", FileSystem.get(new Configuration()));
  }

  @Override
  public void tearDown(State state, RandWalkEnv env) throws Exception {
    // We have resources we need to clean up
    if (env.isMultiTableBatchWriterInitialized()) {
      MultiTableBatchWriter mtbw = env.getMultiTableBatchWriter();
      try {
        mtbw.close();
      } catch (MutationsRejectedException e) {
        log.error("Ignoring mutations that weren't flushed", e);
      }

      // Reset the MTBW on the state to null
      env.resetMultiTableBatchWriter();
    }

    AccumuloClient client = env.getAccumuloClient();

    @SuppressWarnings("unchecked")
    List<String> tables = (List<String>) state.get("tableList");

    for (String tableName : tables) {
      try {
        client.tableOperations().delete(tableName);
        log.debug("Dropping table " + tableName);
      } catch (TableNotFoundException e) {
        log.warn("Tried to drop table " + tableName + " but could not be found!");
      }
    }
  }
}
