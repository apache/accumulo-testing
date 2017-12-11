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
package org.apache.accumulo.testing.core.bulk;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.io.Text;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class BulkScanner {

  public static void main(String[] args) throws Exception {

    Properties props = TestProps.loadFromFile(args[0]);
    BulkEnv env = new BulkEnv(props);

    Random r = new Random();

    long distance = 1000000000000l;

    Connector conn = env.getAccumuloConnector();
    Authorizations auths = env.getRandomAuthorizations();
    Scanner scanner = BulkUtil.createScanner(conn, env.getAccumuloTableName(), auths);
    scanner.setBatchSize(env.getScannerBatchSize());
  }
}
