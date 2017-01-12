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
package org.apache.accumulo.testing.core.continuous;

import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.io.Text;

public class CreateTable {

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      System.err.println("Usage: CreateTable <propsPath>");
      System.exit(-1);
    }

    Properties props = TestProps.loadFromFile(args[0]);
    ContinuousEnv env = new ContinuousEnv(props);

    Connector conn = env.getAccumuloConnector();
    String tableName = env.getAccumuloTableName();
    if (conn.tableOperations().exists(tableName)) {
      System.err.println("ERROR: Accumulo table '"+ tableName + "' already exists");
      System.exit(-1);
    }

    int numTablets = Integer.parseInt(props.getProperty(TestProps.CI_COMMON_ACCUMULO_NUM_TABLETS));
    if (numTablets < 1) {
      System.err.println("ERROR: numTablets < 1");
      System.exit(-1);
    }
    if (env.getRowMin() >= env.getRowMax()) {
      System.err.println("ERROR: min >= max");
      System.exit(-1);
    }

    conn.tableOperations().create(tableName);

    SortedSet<Text> splits = new TreeSet<>();
    int numSplits = numTablets - 1;
    long distance = ((env.getRowMax() - env.getRowMin()) / numTablets) + 1;
    long split = distance;
    for (int i = 0; i < numSplits; i++) {
      String s = String.format("%016x", split + env.getRowMin());
      while (s.charAt(s.length() - 1) == '0') {
        s = s.substring(0, s.length() - 1);
      }
      splits.add(new Text(s));
      split += distance;
    }

    conn.tableOperations().addSplits(tableName, splits);
    System.out.println("Created Accumulo table '" + tableName + "' with " + numTablets + " tablets");
  }
}
