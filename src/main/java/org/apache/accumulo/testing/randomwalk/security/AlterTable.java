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

import java.net.InetAddress;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class AlterTable extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String systemUser = WalkingSecurity.get(state, env).getSysUserName();
    try (AccumuloClient client = env.createClient(systemUser,
        WalkingSecurity.get(state, env).getSysToken())) {

      String tableName = WalkingSecurity.get(state, env).getTableName();

      boolean exists = WalkingSecurity.get(state, env).getTableExists();
      boolean hasPermission;
      try {
        hasPermission = client.securityOperations().hasTablePermission(systemUser, tableName,
            TablePermission.ALTER_TABLE)
            || client.securityOperations().hasSystemPermission(systemUser,
                SystemPermission.ALTER_TABLE);
      } catch (AccumuloSecurityException ae) {
        if (ae.getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST)) {
          if (exists)
            throw new TableExistsException(null, tableName,
                "Got a TableNotFoundException but it should exist", ae);
          else
            return;
        } else {
          throw new AccumuloException("Got unexpected ae error code", ae);
        }
      }
      String newTableName = String.format("security_%s_%s_%d",
          InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_"), env.getPid(),
          System.currentTimeMillis());

      renameTable(client, state, env, tableName, newTableName, hasPermission, exists);
    }
  }

  public static void renameTable(AccumuloClient client, State state, RandWalkEnv env,
      String oldName, String newName, boolean hasPermission, boolean tableExists)
      throws AccumuloSecurityException, AccumuloException, TableExistsException {
    try {
      client.tableOperations().rename(oldName, newName);
    } catch (AccumuloSecurityException ae) {
      if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (hasPermission)
          throw new AccumuloException("Got a security exception when I should have had permission.",
              ae);
        else
          return;
      } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
        if (WalkingSecurity.get(state, env).userPassTransient(client.whoami()))
          return;
      }
      throw new AccumuloException("Got unexpected ae error code", ae);
    } catch (TableNotFoundException tnfe) {
      if (tableExists)
        throw new TableExistsException(null, oldName,
            "Got a TableNotFoundException but it should exist", tnfe);
      else
        return;
    }
    WalkingSecurity.get(state, env).setTableName(newName);
    if (!hasPermission)
      throw new AccumuloException("Didn't get Security Exception when we should have");
  }
}
