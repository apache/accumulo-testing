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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class ChangePermissions extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    Random rand = state.getRandom();
    String userName = state.getRandomUser();
    String tableName = state.getRandomTableName();
    String namespace = state.getRandomNamespace();

    try {
      int dice = rand.nextInt(3);
      if (dice == 0)
        changeSystemPermission(client, rand, userName);
      else if (dice == 1)
        changeTablePermission(client, rand, userName, tableName);
      else if (dice == 2)
        changeNamespacePermission(client, rand, userName, namespace);
    } catch (AccumuloSecurityException | AccumuloException ex) {
      log.debug("Unable to change user permissions: " + ex.getCause());
    }
  }

  private void changeTablePermission(AccumuloClient client, Random rand, String userName,
      String tableName) throws AccumuloException, AccumuloSecurityException {

    EnumSet<TablePermission> perms = EnumSet.noneOf(TablePermission.class);
    for (TablePermission p : TablePermission.values()) {
      if (client.securityOperations().hasTablePermission(userName, tableName, p))
        perms.add(p);
    }

    EnumSet<TablePermission> more = EnumSet.allOf(TablePermission.class);
    more.removeAll(perms);

    if (rand.nextBoolean() && more.size() > 0) {
      List<TablePermission> moreList = new ArrayList<>(more);
      TablePermission choice = moreList.get(rand.nextInt(moreList.size()));
      log.debug("adding permission " + choice);
      client.securityOperations().grantTablePermission(userName, tableName, choice);
    } else {
      if (perms.size() > 0) {
        List<TablePermission> permList = new ArrayList<>(perms);
        TablePermission choice = permList.get(rand.nextInt(permList.size()));
        log.debug("removing permission " + choice);
        client.securityOperations().revokeTablePermission(userName, tableName, choice);
      }
    }
  }

  private void changeSystemPermission(AccumuloClient client, Random rand, String userName)
      throws AccumuloException, AccumuloSecurityException {
    EnumSet<SystemPermission> perms = EnumSet.noneOf(SystemPermission.class);
    for (SystemPermission p : SystemPermission.values()) {
      if (client.securityOperations().hasSystemPermission(userName, p))
        perms.add(p);
    }

    EnumSet<SystemPermission> more = EnumSet.allOf(SystemPermission.class);
    more.removeAll(perms);
    more.remove(SystemPermission.GRANT);

    if (rand.nextBoolean() && more.size() > 0) {
      List<SystemPermission> moreList = new ArrayList<>(more);
      SystemPermission choice = moreList.get(rand.nextInt(moreList.size()));
      log.debug("adding permission " + choice);
      client.securityOperations().grantSystemPermission(userName, choice);
    } else {
      if (perms.size() > 0) {
        List<SystemPermission> permList = new ArrayList<>(perms);
        SystemPermission choice = permList.get(rand.nextInt(permList.size()));
        log.debug("removing permission " + choice);
        client.securityOperations().revokeSystemPermission(userName, choice);
      }
    }
  }

  private void changeNamespacePermission(AccumuloClient client, Random rand, String userName,
      String namespace) throws AccumuloException, AccumuloSecurityException {

    EnumSet<NamespacePermission> perms = EnumSet.noneOf(NamespacePermission.class);
    for (NamespacePermission p : NamespacePermission.values()) {
      if (client.securityOperations().hasNamespacePermission(userName, namespace, p))
        perms.add(p);
    }

    EnumSet<NamespacePermission> more = EnumSet.allOf(NamespacePermission.class);
    more.removeAll(perms);

    if (rand.nextBoolean() && more.size() > 0) {
      List<NamespacePermission> moreList = new ArrayList<>(more);
      NamespacePermission choice = moreList.get(rand.nextInt(moreList.size()));
      log.debug("adding permission " + choice);
      client.securityOperations().grantNamespacePermission(userName, namespace, choice);
    } else {
      if (perms.size() > 0) {
        List<NamespacePermission> permList = new ArrayList<>(perms);
        NamespacePermission choice = permList.get(rand.nextInt(permList.size()));
        log.debug("removing permission " + choice);
        client.securityOperations().revokeNamespacePermission(userName, namespace, choice);
      }
    }
  }
}
