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
package org.apache.accumulo.testing.randomwalk.security;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class CreateUser extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String sysPrincipal = WalkingSecurity.get(state, env).getSysUserName();
    try (AccumuloClient client = env.createClient(sysPrincipal,
        WalkingSecurity.get(state, env).getSysToken())) {

      String tableUserName = WalkingSecurity.get(state, env).getTabUserName();

      boolean exists = WalkingSecurity.get(state, env).userExists(tableUserName);
      boolean hasPermission = client.securityOperations().hasSystemPermission(sysPrincipal,
          SystemPermission.CREATE_USER);
      PasswordToken tabUserPass = new PasswordToken("Super Sekret Table User Password");
      try {
        client.securityOperations().createLocalUser(tableUserName, tabUserPass);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case PERMISSION_DENIED:
            if (hasPermission)
              throw new AccumuloException(
                  "Got a security exception when I should have had permission.", ae);
            else {
              // create user anyway for sake of state
              if (!exists) {
                env.getAccumuloClient().securityOperations().createLocalUser(tableUserName,
                    tabUserPass);
                WalkingSecurity.get(state, env).createUser(tableUserName, tabUserPass);
                Thread.sleep(1000);
              }
              return;
            }
          case USER_EXISTS:
            if (!exists)
              throw new AccumuloException(
                  "Got security exception when the user shouldn't have existed", ae);
            else
              return;
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      WalkingSecurity.get(state, env).createUser(tableUserName, tabUserPass);
      Thread.sleep(1000);
      if (!hasPermission)
        throw new AccumuloException("Didn't get Security Exception when we should have");
    }
  }
}
