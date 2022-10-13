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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.MessageDigest;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Write extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {

    @SuppressWarnings("unchecked")
    List<String> tables = (List<String>) state.get("tableList");

    if (tables.isEmpty()) {
      log.trace("No tables to ingest into");
      return;
    }

    String tableName = tables.get(env.getRandom().nextInt(tables.size()));

    BatchWriter bw;
    try {
      bw = env.getMultiTableBatchWriter().getBatchWriter(tableName);
    } catch (TableOfflineException e) {
      log.debug("Table " + tableName + " is offline");
      return;
    } catch (TableNotFoundException e) {
      log.debug("Table " + tableName + " not found");
      tables.remove(tableName);
      return;
    }

    Text meta = new Text("meta");
    String uuid = UUID.randomUUID().toString();

    Mutation m = new Mutation(new Text(uuid));

    // create a fake payload between 4KB and 16KB
    int numBytes = env.getRandom().nextInt(12000) + 4000;
    byte[] payloadBytes = new byte[numBytes];
    env.getRandom().nextBytes(payloadBytes);
    m.put(meta, new Text("payload"), new Value(payloadBytes));

    // store size
    m.put(meta, new Text("size"), new Value(String.format("%d", numBytes).getBytes(UTF_8)));

    // store hash
    MessageDigest alg = MessageDigest.getInstance("SHA-1");
    alg.update(payloadBytes);
    m.put(meta, new Text("sha1"), new Value(alg.digest()));

    try {
      // add mutation
      bw.addMutation(m);
      state.set("numWrites", state.getLong("numWrites") + 1);
    } catch (TableOfflineException e) {
      log.debug("BatchWrite " + tableName + " failed, offline");
    } catch (MutationsRejectedException mre) {
      if (mre.getCause() instanceof TableDeletedException) {
        log.debug("BatchWrite " + tableName + " failed, table deleted");
        tables.remove(tableName);
      } else if (mre.getCause() instanceof TableOfflineException)
        log.debug("BatchWrite " + tableName + " failed, offline");
      else
        throw mre;
    }
  }

}
