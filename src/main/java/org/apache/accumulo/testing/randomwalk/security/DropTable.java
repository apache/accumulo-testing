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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class DropTable extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    dropTable(state, env, props);
  }

  public static void dropTable(State state, RandWalkEnv env, Properties props) throws Exception {
    String sourceUser = props.getProperty("source", "system");
    String principal;
    AuthenticationToken token;
    boolean hasPermission = false;
    if (sourceUser.equals("table")) {
      principal = WalkingSecurity.get(state, env).getTabUserName();
      token = WalkingSecurity.get(state, env).getTabToken();
    } else {
      principal = WalkingSecurity.get(state, env).getSysUserName();
      token = WalkingSecurity.get(state, env).getSysToken();
    }
    try (AccumuloClient client = env.createClient(principal, token)) {

      String tableName = WalkingSecurity.get(state, env).getTableName();

      boolean exists = WalkingSecurity.get(state, env).getTableExists();

      try {
        hasPermission = client.securityOperations().hasTablePermission(principal, tableName,
            TablePermission.DROP_TABLE)
            || client.securityOperations().hasSystemPermission(principal,
                SystemPermission.DROP_TABLE);
        client.tableOperations().delete(tableName);
      } catch (AccumuloSecurityException ae) {
        if (ae.getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST)) {
          if (exists)
            throw new TableExistsException(null, tableName,
                "Got a TableNotFoundException but it should have existed", ae);
          else
            return;
        } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
          if (hasPermission)
            throw new AccumuloException(
                "Got a security exception when I should have had permission.", ae);
          else {
            // Drop anyway for sake of state
            env.getAccumuloClient().tableOperations().delete(tableName);
            WalkingSecurity.get(state, env).cleanTablePermissions(tableName);
            return;
          }
        } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
          if (WalkingSecurity.get(state, env).userPassTransient(client.whoami()))
            return;
        }
        throw new AccumuloException("Got unexpected ae error code", ae);
      } catch (TableNotFoundException tnfe) {
        if (exists)
          throw new TableExistsException(null, tableName,
              "Got a TableNotFoundException but it should have existed", tnfe);
        else
          return;
      }
      WalkingSecurity.get(state, env).cleanTablePermissions(tableName);
      if (!hasPermission)
        throw new AccumuloException("Didn't get Security Exception when we should have");
    }
  }
}
