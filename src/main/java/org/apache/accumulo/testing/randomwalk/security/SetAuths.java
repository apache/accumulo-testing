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
package org.apache.accumulo.testing.randomwalk.security;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class SetAuths extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String authsString = props.getProperty("auths", "_random");

    String targetUser = props.getProperty("system");
    String target;
    String authPrincipal;
    AuthenticationToken authToken;
    if ("table".equals(targetUser)) {
      target = WalkingSecurity.get(state, env).getTabUserName();
      authPrincipal = WalkingSecurity.get(state, env).getSysUserName();
      authToken = WalkingSecurity.get(state, env).getSysToken();
    } else {
      target = WalkingSecurity.get(state, env).getSysUserName();
      authPrincipal = env.getAccumuloUserName();
      authToken = env.getToken();
    }
    try (AccumuloClient client = env.createClient(authPrincipal, authToken)) {

      boolean exists = WalkingSecurity.get(state, env).userExists(target);
      boolean hasPermission = client.securityOperations().hasSystemPermission(authPrincipal,
          SystemPermission.ALTER_USER);

      Authorizations auths;
      if (authsString.equals("_random")) {
        String[] possibleAuths = WalkingSecurity.get(state, env).getAuthsArray();

        Random r = new Random();
        int i = r.nextInt(possibleAuths.length);
        String[] authSet = new String[i];
        int length = possibleAuths.length;
        for (int j = 0; j < i; j++) {
          int nextRand = r.nextInt(length);
          authSet[j] = possibleAuths[nextRand];
          length--;
          possibleAuths[nextRand] = possibleAuths[length];
          possibleAuths[length] = authSet[j];
        }
        auths = new Authorizations(authSet);
      } else {
        auths = new Authorizations(authsString.split(","));
      }

      try {
        client.securityOperations().changeUserAuthorizations(target, auths);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case PERMISSION_DENIED:
            if (hasPermission)
              throw new AccumuloException(
                  "Got a security exception when I should have had permission.", ae);
            else
              return;
          case USER_DOESNT_EXIST:
            if (exists)
              throw new AccumuloException(
                  "Got security exception when the user should have existed", ae);
            else
              return;
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      WalkingSecurity.get(state, env).changeAuthorizations(target, auths);
      if (!hasPermission)
        throw new AccumuloException("Didn't get Security Exception when we should have");
    }
  }

}
