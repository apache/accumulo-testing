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
package org.apache.accumulo.testing.randomwalk.bulk;

import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.io.Text;

public class ConsistencyCheck extends SelectiveBulkTest {

  @Override
  protected void runLater(State state, RandWalkEnv env) throws Exception {
    Random rand = state.getRandom();
    Text row = Merge.getRandomRow(rand);
    log.info("Checking " + row);
    String user = env.getAccumuloClient().whoami();
    Authorizations auths = env.getAccumuloClient().securityOperations().getUserAuthorizations(user);
    try (Scanner scanner =
        new IsolatedScanner(env.getAccumuloClient().createScanner(Setup.getTableName(), auths))) {
      scanner.setRange(new Range(row));
      scanner.fetchColumnFamily(BulkPlusOne.CHECK_COLUMN_FAMILY);
      Value v = null;
      Key first = null;
      for (Entry<Key,Value> entry : scanner) {
        if (v == null) {
          v = entry.getValue();
          first = entry.getKey();
        }
        if (!v.equals(entry.getValue()))
          throw new RuntimeException("Inconsistent value at " + entry.getKey() + " was "
              + entry.getValue() + " should be " + v + " first read at " + first);
      }
    }
  }

}
