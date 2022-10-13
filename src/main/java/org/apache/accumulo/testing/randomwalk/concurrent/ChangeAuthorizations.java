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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class ChangeAuthorizations extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    Random rand = state.getRandom();
    String userName = state.getRandomUser();
    try {
      List<byte[]> auths = new ArrayList<>(
          client.securityOperations().getUserAuthorizations(userName).getAuthorizations());

      if (rand.nextBoolean()) {
        String authorization = String.format("a%d", rand.nextInt(5000));
        log.debug("adding authorization " + authorization);
        auths.add(authorization.getBytes(UTF_8));
      } else {
        if (auths.size() > 0) {
          log.debug("removing authorization " + new String(auths.remove(0), UTF_8));
        }
      }
      client.securityOperations().changeUserAuthorizations(userName, new Authorizations(auths));
    } catch (AccumuloSecurityException ex) {
      log.debug("Unable to change user authorizations: " + ex.getCause());
    }
  }

}
