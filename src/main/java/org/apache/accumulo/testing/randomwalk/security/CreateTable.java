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
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class CreateTable extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    try (AccumuloClient client = env.createClient(WalkingSecurity.get(state, env).getSysUserName(),
        WalkingSecurity.get(state, env).getSysToken())) {

      String tableName = WalkingSecurity.get(state, env).getTableName();

      boolean exists = WalkingSecurity.get(state, env).getTableExists();
      boolean hasPermission = client.securityOperations().hasSystemPermission(
          WalkingSecurity.get(state, env).getSysUserName(), SystemPermission.CREATE_TABLE);

      try {
        client.tableOperations().create(tableName);
      } catch (AccumuloSecurityException ae) {
        if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
          if (hasPermission)
            throw new AccumuloException(
                "Got a security exception when I should have had permission.", ae);
          else {
            // create table anyway for sake of state
            try {
              env.getAccumuloClient().tableOperations().create(tableName);
              WalkingSecurity.get(state, env).initTable(tableName);
            } catch (TableExistsException tee) {
              if (exists)
                return;
              else
                throw new AccumuloException("Test and Accumulo are out of sync");
            }
            return;
          }
        } else
          throw new AccumuloException("Got unexpected error", ae);
      } catch (TableExistsException tee) {
        if (!exists)
          throw new TableExistsException(null, tableName,
              "Got a TableExistsException but it shouldn't have existed", tee);
        else
          return;
      }
      WalkingSecurity.get(state, env).initTable(tableName);
      for (TablePermission tp : TablePermission.values())
        WalkingSecurity.get(state, env).grantTablePermission(client.whoami(), tableName, tp);
      if (!hasPermission)
        throw new AccumuloException("Didn't get Security Exception when we should have");
    }
  }
}
