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

import java.net.InetAddress;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.testing.randomwalk.Fixture;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

public class ShardFixture extends Fixture {

  static SortedSet<Text> genSplits(long max, int numTablets, String format) {

    int numSplits = numTablets - 1;
    long distance = max / numTablets;
    long split = distance;

    TreeSet<Text> splits = new TreeSet<>();

    for (int i = 0; i < numSplits; i++) {
      splits.add(new Text(String.format(format, split)));
      split += distance;
    }

    return splits;
  }

  static void createIndexTable(Logger log, State state, RandWalkEnv env, String suffix, Random rand)
      throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    String name = state.getString("indexTableName") + suffix;

    NewTableConfiguration ntc = new NewTableConfiguration();

    int numPartitions = state.getInteger("numPartitions");
    SortedSet<Text> splits = genSplits(numPartitions, rand.nextInt(numPartitions) + 1, "%06x");
    ntc.withSplits(splits);

    if ((Boolean) state.get("cacheIndex")) {
      ntc.setProperties(Map.of(Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true",
          Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true"));

      log.info("Enabling caching for table " + name);
    }

    client.tableOperations().create(name, ntc);

    String tableId = client.tableOperations().tableIdMap().get(name);
    log.info("Created index table {} (id:{}) with {} splits", name, tableId, splits.size());
  }

  @Override
  public void setUp(State state, RandWalkEnv env) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String pid = env.getPid();

    int numPartitions = env.getRandom().nextInt(90) + 10;

    state.set("indexTableName",
        String.format("ST_index_%s_%s_%d", hostname, pid, System.currentTimeMillis()));
    state.set("docTableName",
        String.format("ST_docs_%s_%s_%d", hostname, pid, System.currentTimeMillis()));
    state.set("numPartitions", numPartitions);
    state.set("cacheIndex", env.getRandom().nextBoolean());
    state.set("rand", env.getRandom());
    state.set("nextDocID", 0L);

    AccumuloClient client = env.getAccumuloClient();

    createIndexTable(this.log, state, env, "", env.getRandom());

    String docTableName = state.getString("docTableName");
    NewTableConfiguration ntc = new NewTableConfiguration();
    SortedSet<Text> splits = genSplits(0xff, env.getRandom().nextInt(32) + 1, "%02x");
    ntc.withSplits(splits);
    if (env.getRandom().nextBoolean()) {
      ntc.setProperties(Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));
      log.info("Enabling bloom filters for table {}", docTableName);
    }

    client.tableOperations().create(docTableName, ntc);
    String tableId = client.tableOperations().tableIdMap().get(docTableName);
    log.info("Created doc table {} (id:{}) with {} splits", docTableName, tableId, splits.size());
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

    log.info("Deleting index and doc tables");

    client.tableOperations().delete(state.getString("indexTableName"));
    client.tableOperations().delete(state.getString("docTableName"));

    log.debug("Exiting shard test");
  }

}
