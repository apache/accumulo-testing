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
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.Fixture;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;

public class SecurityFixture extends Fixture {

  @Override
  public void setUp(State state, RandWalkEnv env) throws Exception {
    String secTableName, systemUserName, tableUserName, secNamespaceName;
    // A best-effort sanity check to guard against not password-based auth
    if (env.getClientProps().getProperty(ClientProperty.AUTH_TYPE.getKey()).equals("kerberos")) {
      throw new IllegalStateException(
          "Security module currently cannot support Kerberos/SASL instances");
    }

    AccumuloClient client = env.getAccumuloClient();

    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");

    systemUserName = String.format("system_%s", hostname);
    tableUserName = String.format("table_%s", hostname);
    secTableName = String.format("security_%s", hostname);
    secNamespaceName = String.format("securityNs_%s", hostname);

    if (client.tableOperations().exists(secTableName))
      client.tableOperations().delete(secTableName);
    Set<String> users = client.securityOperations().listLocalUsers();
    if (users.contains(tableUserName))
      client.securityOperations().dropLocalUser(tableUserName);
    if (users.contains(systemUserName))
      client.securityOperations().dropLocalUser(systemUserName);

    PasswordToken sysUserPass = new PasswordToken("sysUser");
    client.securityOperations().createLocalUser(systemUserName, sysUserPass);

    WalkingSecurity.get(state, env).setTableName(secTableName);
    WalkingSecurity.get(state, env).setNamespaceName(secNamespaceName);
    state.set("rootUserPass", env.getToken());

    WalkingSecurity.get(state, env).setSysUserName(systemUserName);
    WalkingSecurity.get(state, env).createUser(systemUserName, sysUserPass);

    WalkingSecurity.get(state, env).changePassword(tableUserName, new PasswordToken(new byte[0]));

    WalkingSecurity.get(state, env).setTabUserName(tableUserName);

    for (TablePermission tp : TablePermission.values()) {
      WalkingSecurity.get(state, env).revokeTablePermission(systemUserName, secTableName, tp);
      WalkingSecurity.get(state, env).revokeTablePermission(tableUserName, secTableName, tp);
    }
    for (SystemPermission sp : SystemPermission.values()) {
      WalkingSecurity.get(state, env).revokeSystemPermission(systemUserName, sp);
      WalkingSecurity.get(state, env).revokeSystemPermission(tableUserName, sp);
    }
    WalkingSecurity.get(state, env).changeAuthorizations(tableUserName, new Authorizations());
  }

  @Override
  public void tearDown(State state, RandWalkEnv env) throws Exception {
    log.debug("One last validate");
    Validate.validate(state, env, log);
    AccumuloClient client = env.getAccumuloClient();

    if (WalkingSecurity.get(state, env).getTableExists()) {
      String secTableName = WalkingSecurity.get(state, env).getTableName();
      log.debug("Dropping tables: " + secTableName);

      client.tableOperations().delete(secTableName);
    }

    if (WalkingSecurity.get(state, env).getNamespaceExists()) {
      String secNamespaceName = WalkingSecurity.get(state, env).getNamespaceName();
      log.debug("Dropping namespace: " + secNamespaceName);

      client.namespaceOperations().delete(secNamespaceName);
    }

    if (WalkingSecurity.get(state, env)
        .userExists(WalkingSecurity.get(state, env).getTabUserName())) {
      String tableUserName = WalkingSecurity.get(state, env).getTabUserName();
      log.debug("Dropping user: " + tableUserName);

      client.securityOperations().dropLocalUser(tableUserName);
    }
    String systemUserName = WalkingSecurity.get(state, env).getSysUserName();
    log.debug("Dropping user: " + systemUserName);
    client.securityOperations().dropLocalUser(systemUserName);
    WalkingSecurity.clearInstance();

    // Allow user drops to propagate, in case a new security test starts
    Thread.sleep(2000);
  }
}
