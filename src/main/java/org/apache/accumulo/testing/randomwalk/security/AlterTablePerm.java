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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class AlterTablePerm extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    alter(state, env, props);
  }

  public static void alter(State state, RandWalkEnv env, Properties props) throws Exception {
    String action = props.getProperty("task", "toggle");
    String perm = props.getProperty("perm", "random");
    String sourceUserProp = props.getProperty("source", "system");
    String targetUser = props.getProperty("target", "table");

    String target;
    if ("table".equals(targetUser))
      target = WalkingSecurity.get(state, env).getTabUserName();
    else
      target = WalkingSecurity.get(state, env).getSysUserName();

    boolean userExists = WalkingSecurity.get(state, env).userExists(target);
    boolean tableExists = WalkingSecurity.get(state, env).getTableExists();

    TablePermission tabPerm;
    if (perm.equals("random")) {
      Random r = new Random();
      int i = r.nextInt(TablePermission.values().length);
      tabPerm = TablePermission.values()[i];
    } else
      tabPerm = TablePermission.valueOf(perm);
    String tableName = WalkingSecurity.get(state, env).getTableName();
    boolean hasPerm = WalkingSecurity.get(state, env).hasTablePermission(target, tableName,
        tabPerm);
    boolean canGive;
    String sourceUser;
    AuthenticationToken sourceToken;
    if ("system".equals(sourceUserProp)) {
      sourceUser = WalkingSecurity.get(state, env).getSysUserName();
      sourceToken = WalkingSecurity.get(state, env).getSysToken();
    } else if ("table".equals(sourceUserProp)) {
      sourceUser = WalkingSecurity.get(state, env).getTabUserName();
      sourceToken = WalkingSecurity.get(state, env).getTabToken();
    } else {
      sourceUser = env.getAccumuloUserName();
      sourceToken = env.getToken();
    }
    try (AccumuloClient client = env.createClient(sourceUser, sourceToken)) {
      SecurityOperations secOps = client.securityOperations();

      try {
        canGive = secOps.hasSystemPermission(sourceUser, SystemPermission.ALTER_TABLE)
            || secOps.hasTablePermission(sourceUser, tableName, TablePermission.GRANT);
      } catch (AccumuloSecurityException ae) {
        if (ae.getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST)) {
          if (tableExists)
            throw new TableExistsException(null, tableName,
                "Got a TableNotFoundException but it should exist", ae);
          else
            return;
        } else {
          throw new AccumuloException("Got unexpected ae error code", ae);
        }
      }

      // toggle
      if (!"take".equals(action) && !"give".equals(action)) {
        try {
          boolean res;
          if (hasPerm != (res = env.getAccumuloClient().securityOperations()
              .hasTablePermission(target, tableName, tabPerm)))
            throw new AccumuloException("Test framework and accumulo are out of sync for user "
                + client.whoami() + " for perm " + tabPerm.name()
                + " with local vs. accumulo being " + hasPerm + " " + res);

          if (hasPerm)
            action = "take";
          else
            action = "give";
        } catch (AccumuloSecurityException ae) {
          switch (ae.getSecurityErrorCode()) {
            case USER_DOESNT_EXIST:
              if (userExists)
                throw new AccumuloException(
                    "Framework and Accumulo are out of sync, we think user exists", ae);
              else
                return;
            case TABLE_DOESNT_EXIST:
              if (tableExists)
                throw new TableExistsException(null, tableName,
                    "Got a TableNotFoundException but it should exist", ae);
              else
                return;
            default:
              throw ae;
          }
        }
      }

      boolean trans = WalkingSecurity.get(state, env).userPassTransient(client.whoami());
      if ("take".equals(action)) {
        try {
          client.securityOperations().revokeTablePermission(target, tableName, tabPerm);
        } catch (AccumuloSecurityException ae) {
          switch (ae.getSecurityErrorCode()) {
            case GRANT_INVALID:
              throw new AccumuloException("Got a grant invalid on non-System.GRANT option", ae);
            case PERMISSION_DENIED:
              if (canGive)
                throw new AccumuloException(client.whoami() + " failed to revoke permission to "
                    + target + " when it should have worked", ae);
              return;
            case USER_DOESNT_EXIST:
              if (userExists)
                throw new AccumuloException("Table user doesn't exist and they SHOULD.", ae);
              return;
            case TABLE_DOESNT_EXIST:
              if (tableExists)
                throw new AccumuloException("Table doesn't exist but it should", ae);
              return;
            case BAD_CREDENTIALS:
              if (!trans)
                throw new AccumuloException("Bad credentials for user " + client.whoami());
              return;
            default:
              throw new AccumuloException("Got unexpected exception", ae);
          }
        }
        WalkingSecurity.get(state, env).revokeTablePermission(target, tableName, tabPerm);
      } else if ("give".equals(action)) {
        try {
          client.securityOperations().grantTablePermission(target, tableName, tabPerm);
        } catch (AccumuloSecurityException ae) {
          switch (ae.getSecurityErrorCode()) {
            case GRANT_INVALID:
              throw new AccumuloException("Got a grant invalid on non-System.GRANT option", ae);
            case PERMISSION_DENIED:
              if (canGive)
                throw new AccumuloException(client.whoami() + " failed to give permission to "
                    + target + " when it should have worked", ae);
              return;
            case USER_DOESNT_EXIST:
              if (userExists)
                throw new AccumuloException("Table user doesn't exist and they SHOULD.", ae);
              return;
            case TABLE_DOESNT_EXIST:
              if (tableExists)
                throw new AccumuloException("Table doesn't exist but it should", ae);
              return;
            case BAD_CREDENTIALS:
              if (!trans)
                throw new AccumuloException("Bad credentials for user " + client.whoami());
              return;
            default:
              throw new AccumuloException("Got unexpected exception", ae);
          }
        }
        WalkingSecurity.get(state, env).grantTablePermission(target, tableName, tabPerm);
      }

      if (!userExists)
        throw new AccumuloException("User shouldn't have existed, but apparently does");
      if (!tableExists)
        throw new AccumuloException("Table shouldn't have existed, but apparently does");
      if (!canGive)
        throw new AccumuloException(
            client.whoami() + " shouldn't have been able to grant privilege");
    }
  }
}
