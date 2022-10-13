/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.continuous;

import static org.apache.accumulo.testing.TestProps.CI_COMMON_ACCUMULO_NUM_TABLETS;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTable {
  private static final Logger log = LoggerFactory.getLogger(CreateTable.class);

  public static void main(String[] args) throws Exception {

    try (ContinuousEnv env = new ContinuousEnv(args)) {
      AccumuloClient client = env.getAccumuloClient();

      String tableName = env.getAccumuloTableName();
      if (client.tableOperations().exists(tableName)) {
        log.error("Accumulo table {} already exists", tableName);
        System.exit(-1);
      }

      int numTablets = Integer.parseInt(env.getTestProperty(CI_COMMON_ACCUMULO_NUM_TABLETS));

      if (numTablets < 1) {
        log.error("numTablets < 1");
        System.exit(-1);
      }
      if (env.getRowMin() >= env.getRowMax()) {
        log.error("min >= max");
        System.exit(-1);
      }

      // retrieve and set tserver props
      Map<String,String> props = getProps(env, TestProps.CI_COMMON_ACCUMULO_SERVER_PROPS);
      try {
        for (Map.Entry<String,String> entry : props.entrySet()) {
          client.instanceOperations().setProperty(entry.getKey(), entry.getValue());
        }
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("Failed to set tserver props");
        throw new Exception(e);
      }

      SortedSet<Text> splits = new TreeSet<>();
      final int numSplits = numTablets - 1;
      final long distance = ((env.getRowMax() - env.getRowMin()) / numTablets) + 1;
      long split = distance;
      for (int i = 0; i < numSplits; i++) {
        String s = String.format("%016x", split + env.getRowMin());
        while (s.charAt(s.length() - 1) == '0') {
          s = s.substring(0, s.length() - 1);
        }
        splits.add(new Text(s));
        split += distance;
      }

      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withSplits(splits);
      ntc.setProperties(getProps(env, TestProps.CI_COMMON_ACCUMULO_TABLE_PROPS));

      client.tableOperations().create(tableName, ntc);

      log.info("Created Accumulo table {} with {} tablets", tableName, numTablets);
    }
  }

  private static Map<String,String> getProps(ContinuousEnv env, String propType) {
    String[] propArray = env.getTestProperty(propType).split(" ");
    Map<String,String> propMap = new HashMap<>();
    for (String prop : propArray) {
      log.debug("prop: {}", prop);
      String[] kv = prop.split("=");
      propMap.put(kv[0], kv[1]);
    }
    return propMap;
  }
}
