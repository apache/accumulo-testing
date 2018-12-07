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
package org.apache.accumulo.testing.randomwalk.conditional;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class Merge extends Test {
  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String table = state.getString("tableName");
    Random rand = (Random) state.get("rand");
    AccumuloClient client = env.getAccumuloClient();
    Text row1 = new Text(Utils.getBank(rand.nextInt((Integer) state.get("numBanks"))));
    Text row2 = new Text(Utils.getBank(rand.nextInt((Integer) state.get("numBanks"))));

    if (row1.compareTo(row2) >= 0) {
      row1 = null;
      row2 = null;
    }

    log.debug("merging " + row1 + " " + row2);
    client.tableOperations().merge(table, row1, row2);
  }

}