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

import java.util.Arrays;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class Authenticate extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    authenticate(WalkingSecurity.get(state, env).getSysUserName(),
        WalkingSecurity.get(state, env).getSysToken(), state, env, props);
  }

  public static void authenticate(String principal, AuthenticationToken token, State state,
      RandWalkEnv env, Properties props) throws Exception {
    String targetProp = props.getProperty("target");
    boolean success = Boolean.parseBoolean(props.getProperty("valid"));

    try (AccumuloClient client = env.createClient(principal, token)) {

      String target;

      if (targetProp.equals("table")) {
        target = WalkingSecurity.get(state, env).getTabUserName();
      } else {
        target = WalkingSecurity.get(state, env).getSysUserName();
      }
      boolean exists = WalkingSecurity.get(state, env).userExists(target);
      // Copy so if failed it doesn't mess with the password stored in state
      byte[] password = Arrays.copyOf(WalkingSecurity.get(state, env).getUserPassword(target),
          WalkingSecurity.get(state, env).getUserPassword(target).length);
      boolean hasPermission = client.securityOperations().hasSystemPermission(principal,
          SystemPermission.SYSTEM) || principal.equals(target);

      if (!success)
        for (int i = 0; i < password.length; i++)
          password[i]++;

      boolean result;

      try {
        result = client.securityOperations().authenticateUser(target, new PasswordToken(password));
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case PERMISSION_DENIED:
            if (exists && hasPermission)
              throw new AccumuloException(
                  "Got a security exception when I should have had permission.", ae);
            else
              return;
          default:
            throw new AccumuloException("Unexpected exception!", ae);
        }
      }
      if (!hasPermission)
        throw new AccumuloException("Didn't get Security Exception when we should have");
      if (result != (success && exists))
        throw new AccumuloException("Authentication " + (result ? "succeeded" : "failed")
            + " when it should have " + ((success && exists) ? "succeeded" : "failed")
            + " while the user exists? " + exists);
    }
  }
}
