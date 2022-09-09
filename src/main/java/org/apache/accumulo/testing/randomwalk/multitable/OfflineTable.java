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

import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class OfflineTable extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {

    @SuppressWarnings("unchecked")
    List<String> tables = (List<String>) state.get("tableList");

    if (tables.size() <= 0) {
      return;
    }

    String tableName = tables.get(env.getRandom().nextInt(tables.size()));

    env.getAccumuloClient().tableOperations().offline(tableName, env.getRandom().nextBoolean());
    log.debug("Table " + tableName + " offline ");
    env.getAccumuloClient().tableOperations().online(tableName, env.getRandom().nextBoolean());
    log.debug("Table " + tableName + " online ");
  }
}
