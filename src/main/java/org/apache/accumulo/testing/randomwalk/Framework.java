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
package org.apache.accumulo.testing.randomwalk;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Framework {

  private static final Logger log = LoggerFactory.getLogger(Framework.class);
  private HashMap<String,Node> nodes = new HashMap<>();
  private static final Framework INSTANCE = new Framework();

  /**
   * @return Singleton instance of Framework
   */
  public static Framework getInstance() {
    return INSTANCE;
  }

  /**
   * Run random walk framework
   *
   * @param startName
   *          Full name of starting graph or test
   */
  public int run(String startName, State state, RandWalkEnv env) {

    try {
      Node node = getNode(startName);
      node.visit(state, env, new Properties());
    } catch (Exception e) {
      log.error("Error during random walk", e);
      return -1;
    }
    return 0;
  }

  /**
   * Creates node (if it does not already exist) and inserts into map
   *
   * @param id
   *          Name of node
   * @return Node specified by id
   */
  Node getNode(String id) throws Exception {

    // check for node in nodes
    if (nodes.containsKey(id)) {
      return nodes.get(id);
    }

    // otherwise create and put in nodes
    Node node;
    if (id.endsWith(".xml")) {
      node = new Module(id);
    } else {
      node = (Test) Class.forName(id).newInstance();
    }
    nodes.put(id, node);
    return node;
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.println("Usage: Framework <testPropsPath> <clientPropsPath> <module>");
      System.exit(-1);
    }

    log.info("Running random walk test with module: " + args[2]);

    State state = new State();
    try (RandWalkEnv env = new RandWalkEnv(args[0], args[1])) {
      getInstance().run(args[2], state, env);
      log.info("Test finished");
    }
  }
}
