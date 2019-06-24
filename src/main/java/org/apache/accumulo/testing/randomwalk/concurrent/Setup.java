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
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class Setup extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    state.setRandom(new Random());

    int numTables = Integer.parseInt(props.getProperty("numTables", "9"));
    int numNamespaces = Integer.parseInt(props.getProperty("numNamespaces", "2"));
    log.debug("numTables = " + numTables);
    log.debug("numNamespaces = " + numNamespaces);
    List<String> namespaces = new ArrayList<>();

    for (int i = 0; i < numNamespaces; i++) {
      String ns = String.format("nspc_%03d", i);
      namespaces.add(ns);
      state.addNamespace(ns);
    }

    // Make tables in the default namespace
    double tableCeil = Math.ceil((double) numTables / (numNamespaces + 1));
    for (int i = 0; i < tableCeil; i++) {
      state.addTable(String.format("ctt_%03d", i));
    }

    // Make tables in each namespace
    double tableFloor = Math.floor(numTables / (numNamespaces + 1));
    for (String n : namespaces) {
      for (int i = 0; i < tableFloor; i++) {
        state.addTable(String.format(n + ".ctt_%03d", i));
      }
    }

    int numUsers = Integer.parseInt(props.getProperty("numUsers", "5"));
    log.debug("numUsers = " + numUsers);
    for (int i = 0; i < numUsers; i++)
      state.addUser(String.format("user%03d", i));

    log.info("TABLES = " + state.getTableNames());
  }

}
