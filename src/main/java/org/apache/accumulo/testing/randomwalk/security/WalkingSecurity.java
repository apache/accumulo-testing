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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class WalkingSecurity {
  State state = null;
  RandWalkEnv env = null;
  private static final Logger log = LoggerFactory.getLogger(WalkingSecurity.class);

  private static final String tableName = "SecurityTableName";
  private static final String namespaceName = "SecurityNamespaceName";
  private static final String userName = "UserName";

  private static final String userPass = "UserPass";
  private static final String userExists = "UserExists";
  private static final String tableExists = "TableExists";
  private static final String namespaceExists = "NamespaceExists";

  private static final String connector = "UserConnection";

  private static final String authsMap = "authorizationsCountMap";
  private static final String lastKey = "lastMutationKey";
  private static final String filesystem = "securityFileSystem";

  private static WalkingSecurity instance = null;

  public WalkingSecurity(State state2, RandWalkEnv env2) {
    this.state = state2;
    this.env = env2;
  }

  public static WalkingSecurity get(State state, RandWalkEnv env) {
    if (instance == null || instance.state != state) {
      instance = new WalkingSecurity(state, env);
      state.set(tableExists, Boolean.toString(false));
      state.set(namespaceExists, Boolean.toString(false));
      state.set(authsMap, new HashMap<String,Integer>());
    }

    return instance;
  }

  public void changeAuthorizations(String user, Authorizations authorizations)
      throws AccumuloSecurityException {
    state.set(user + "_auths", authorizations);
    state.set("Auths-" + user + '-' + "time", System.currentTimeMillis());
  }

  public boolean ambiguousAuthorizations(String userName) {
    Long setTime = state.getLong("Auths-" + userName + '-' + "time");
    if (setTime == null)
      throw new RuntimeException("Auths-" + userName + '-' + "time is null");
    if (System.currentTimeMillis() < (setTime + 1000))
      return true;
    return false;
  }

  public void createUser(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    state.set(principal + userExists, Boolean.toString(true));
    changePassword(principal, token);
    cleanUser(principal);
  }

  public void dropUser(String user) throws AccumuloSecurityException {
    state.set(user + userExists, Boolean.toString(false));
    cleanUser(user);
    if (user.equals(getTabUserName()))
      state.set("table" + connector, null);
  }

  public void changePassword(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    state.set(principal + userPass, token);
    state.set(principal + userPass + "time", System.currentTimeMillis());
  }

  public boolean userExists(String user) {
    return Boolean.parseBoolean(state.getString(user + userExists));
  }

  public boolean hasSystemPermission(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    boolean res = Boolean.parseBoolean(state.getString("Sys-" + user + '-' + permission.name()));
    return res;
  }

  public boolean hasTablePermission(String user, String table, TablePermission permission)
      throws AccumuloSecurityException, TableNotFoundException {
    return Boolean.parseBoolean(state.getString("Tab-" + user + '-' + permission.name()));
  }

  public void grantSystemPermission(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    setSysPerm(state, user, permission, true);
  }

  public void revokeSystemPermission(String user, SystemPermission permission)
      throws AccumuloSecurityException {
    setSysPerm(state, user, permission, false);
  }

  public void grantTablePermission(String user, String table, TablePermission permission)
      throws AccumuloSecurityException, TableNotFoundException {
    setTabPerm(state, user, permission, table, true);
  }

  private static void setSysPerm(State state, String userName, SystemPermission tp, boolean value) {
    log.debug((value ? "Gave" : "Took") + " the system permission " + tp.name()
        + (value ? " to" : " from") + " user " + userName);
    state.set("Sys-" + userName + '-' + tp.name(), Boolean.toString(value));
  }

  private void setTabPerm(State state, String userName, TablePermission tp, String table,
      boolean value) {
    if (table.equals(userName))
      throw new RuntimeException("Something went wrong: table is equal to userName: " + userName);
    log.debug((value ? "Gave" : "Took") + " the table permission " + tp.name()
        + (value ? " to" : " from") + " user " + userName);
    state.set("Tab-" + userName + '-' + tp.name(), Boolean.toString(value));
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE))
      state.set("Tab-" + userName + '-' + tp.name() + '-' + "time", System.currentTimeMillis());
  }

  public void revokeTablePermission(String user, String table, TablePermission permission)
      throws AccumuloSecurityException, TableNotFoundException {
    setTabPerm(state, user, permission, table, false);
  }

  public void cleanTablePermissions(String table)
      throws AccumuloSecurityException, TableNotFoundException {
    for (String user : new String[] {getSysUserName(), getTabUserName()}) {
      for (TablePermission tp : TablePermission.values()) {
        revokeTablePermission(user, table, tp);
      }
    }
    state.set(tableExists, Boolean.toString(false));
  }

  public void cleanUser(String user) throws AccumuloSecurityException {
    if (getTableExists())
      for (TablePermission tp : TablePermission.values())
        try {
          revokeTablePermission(user, getTableName(), tp);
        } catch (TableNotFoundException e) {
          // ignore
        }
    for (SystemPermission sp : SystemPermission.values())
      revokeSystemPermission(user, sp);
  }

  public String getTabUserName() {
    return state.getString("table" + userName);
  }

  public String getSysUserName() {
    return state.getString("system" + userName);
  }

  public void setTabUserName(String name) {
    state.set("table" + userName, name);
    state.set(name + userExists, Boolean.toString(false));
  }

  public void setSysUserName(String name) {
    state.set("system" + userName, name);
  }

  public String getTableName() {
    return state.getString(tableName);
  }

  public String getNamespaceName() {
    return state.getString(namespaceName);
  }

  public boolean getTableExists() {
    return Boolean.parseBoolean(state.getString(tableExists));
  }

  public boolean getNamespaceExists() {
    return Boolean.parseBoolean(state.getString(namespaceExists));
  }

  public AuthenticationToken getSysToken() {
    return new PasswordToken(getSysPassword());
  }

  public AuthenticationToken getTabToken() {
    return new PasswordToken(getTabPassword());
  }

  public byte[] getUserPassword(String user) {
    Object obj = state.get(user + userPass);
    if (obj instanceof PasswordToken) {
      return ((PasswordToken) obj).getPassword();
    }
    return null;
  }

  public byte[] getSysPassword() {
    Object obj = state.get(getSysUserName() + userPass);
    if (obj instanceof PasswordToken) {
      return ((PasswordToken) obj).getPassword();
    }
    return null;
  }

  public byte[] getTabPassword() {
    Object obj = state.get(getTabUserName() + userPass);
    if (obj instanceof PasswordToken) {
      return ((PasswordToken) obj).getPassword();
    }
    return null;
  }

  public boolean userPassTransient(String user) {
    return System.currentTimeMillis() - state.getLong(user + userPass + "time") < 1000;
  }

  public void setTableName(String tName) {
    state.set(tableName, tName);
  }

  public void setNamespaceName(String nsName) {
    state.set(namespaceName, nsName);
  }

  public void initTable(String table) throws AccumuloSecurityException {
    state.set(tableExists, Boolean.toString(true));
    state.set(tableName, table);
  }

  public String[] getAuthsArray() {
    return new String[] {"Fishsticks", "PotatoSkins", "Ribs", "Asparagus", "Paper", "Towels",
        "Lint", "Brush", "Celery"};
  }

  public boolean inAmbiguousZone(String userName, TablePermission tp) {
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE)) {
      Long setTime = state.getLong("Tab-" + userName + '-' + tp.name() + '-' + "time");
      if (setTime == null)
        throw new RuntimeException("Tab-" + userName + '-' + tp.name() + '-' + "time is null");
      if (System.currentTimeMillis() < (setTime + 1000))
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public Map<String,Integer> getAuthsMap() {
    return (Map<String,Integer>) state.get(authsMap);
  }

  public String getLastKey() {
    return state.getString(lastKey);
  }

  public void increaseAuthMap(String s, int increment) {
    Integer curVal = getAuthsMap().get(s);
    if (curVal == null) {
      curVal = Integer.valueOf(0);
      getAuthsMap().put(s, curVal);
    }
    curVal += increment;
  }

  public FileSystem getFs() {
    FileSystem fs = null;
    try {
      fs = (FileSystem) state.get(filesystem);
    } catch (RuntimeException re) {}

    if (fs == null) {
      try {
        fs = FileSystem.get(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      state.set(filesystem, fs);
    }
    return fs;
  }

  public static void clearInstance() {
    instance = null;
  }

}
