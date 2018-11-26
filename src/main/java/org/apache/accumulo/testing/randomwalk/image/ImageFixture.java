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
package org.apache.accumulo.testing.randomwalk.image;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.Fixture;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.io.Text;

public class ImageFixture extends Fixture {

  String imageTableName;
  String indexTableName;

  @Override
  public void setUp(State state, RandWalkEnv env) throws Exception {

    AccumuloClient client = env.getAccumuloClient();

    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 256; i++) {
      splits.add(new Text(String.format("%04x", i << 8)));
    }

    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String pid = env.getPid();

    imageTableName = String.format("img_%s_%s_%d", hostname, pid, System.currentTimeMillis());
    state.set("imageTableName", imageTableName);

    indexTableName = String.format("img_ndx_%s_%s_%d", hostname, pid, System.currentTimeMillis());
    state.set("indexTableName", indexTableName);

    try {
      client.tableOperations().create(imageTableName);
      client.tableOperations().addSplits(imageTableName, splits);
      log.debug("Created table " + imageTableName + " (id:" + client.tableOperations().tableIdMap().get(imageTableName) + ")");
    } catch (TableExistsException e) {
      log.error("Table " + imageTableName + " already exists.");
      throw e;
    }

    try {
      client.tableOperations().create(indexTableName);
      log.debug("Created table " + indexTableName + " (id:" + client.tableOperations().tableIdMap().get(indexTableName) + ")");
    } catch (TableExistsException e) {
      log.error("Table " + imageTableName + " already exists.");
      throw e;
    }

    Random rand = new Random();
    if (rand.nextInt(10) < 5) {
      // setup locality groups
      Map<String,Set<Text>> groups = getLocalityGroups();

      client.tableOperations().setLocalityGroups(imageTableName, groups);
      log.debug("Configured locality groups for " + imageTableName + " groups = " + groups);
    }

    state.set("numWrites", Long.valueOf(0));
    state.set("totalWrites", Long.valueOf(0));
    state.set("verified", Integer.valueOf(0));
    state.set("lastIndexRow", new Text(""));
  }

  static Map<String,Set<Text>> getLocalityGroups() {
    Map<String,Set<Text>> groups = new HashMap<>();

    HashSet<Text> lg1 = new HashSet<>();
    lg1.add(Write.CONTENT_COLUMN_FAMILY);
    groups.put("lg1", lg1);

    HashSet<Text> lg2 = new HashSet<>();
    lg2.add(Write.META_COLUMN_FAMILY);
    groups.put("lg2", lg2);
    return groups;
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

    // Now we can safely delete the tables
    log.debug("Dropping tables: " + imageTableName + " " + indexTableName);

    AccumuloClient client = env.getAccumuloClient();

    client.tableOperations().delete(imageTableName);
    client.tableOperations().delete(indexTableName);

    log.debug("Final total of writes: " + state.getLong("totalWrites"));
  }
}
