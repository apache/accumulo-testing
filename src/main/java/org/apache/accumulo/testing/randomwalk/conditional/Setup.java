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
package org.apache.accumulo.testing.randomwalk.conditional;

import java.util.Properties;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class Setup extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    state.setRandom(env.getRandom());

    int numBanks = Integer.parseInt(props.getProperty("numBanks", "1000"));
    log.debug("numBanks = " + numBanks);
    state.set("numBanks", numBanks);

    int numAccts = Integer.parseInt(props.getProperty("numAccts", "10000"));
    log.debug("numAccts = " + numAccts);
    state.set("numAccts", numAccts);

    String tableName = "banks";
    state.set("tableName", tableName);

    try {
      env.getAccumuloClient().tableOperations().create(tableName);
      log.debug("created table " + tableName);
      boolean blockCache = env.getRandom().nextBoolean();
      env.getAccumuloClient().tableOperations().setProperty(tableName,
          Property.TABLE_BLOCKCACHE_ENABLED.getKey(), blockCache + "");
      log.debug("set " + Property.TABLE_BLOCKCACHE_ENABLED.getKey() + " " + blockCache);
    } catch (TableExistsException ignored) {}

    ConditionalWriter newCW = env.getAccumuloClient().createConditionalWriter(tableName,
        new ConditionalWriterConfig().setMaxWriteThreads(1));
    ConditionalWriter previousCW = (ConditionalWriter) state.getOkIfAbsent("cw");
    state.set("cw", newCW);

    // close the previous conditional writer if there is one
    if (previousCW != null) {
      previousCW.close();
    }
  }
}
