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
package org.apache.accumulo.testing.randomwalk.concurrent;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class CheckPermission extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    Random rand = state.getRandom();
    String userName = state.getRandomUser();
    String tableName = state.getRandomTableName();
    String namespace = state.getRandomNamespace();

    try {
      int dice = rand.nextInt(2);
      if (dice == 0) {
        log.debug("Checking systerm permission " + userName);
        client.securityOperations().hasSystemPermission(userName,
            SystemPermission.values()[rand.nextInt(SystemPermission.values().length)]);
      } else if (dice == 1) {
        log.debug("Checking table permission " + userName + " " + tableName);
        client.securityOperations().hasTablePermission(userName, tableName,
            TablePermission.values()[rand.nextInt(TablePermission.values().length)]);
      } else if (dice == 2) {
        log.debug("Checking namespace permission " + userName + " " + namespace);
        client.securityOperations().hasNamespacePermission(userName, namespace,
            NamespacePermission.values()[rand.nextInt(NamespacePermission.values().length)]);
      }

    } catch (AccumuloSecurityException ex) {
      log.debug("Unable to check permissions: " + ex.getCause());
    }
  }

}
