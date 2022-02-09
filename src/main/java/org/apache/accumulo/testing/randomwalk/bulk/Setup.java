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
package org.apache.accumulo.testing.randomwalk.bulk;

import java.net.InetAddress;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;
import org.apache.hadoop.fs.FileSystem;

public class Setup extends Test {

  private static final int MAX_POOL_SIZE = 8;
  static String tableName = null;

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String pid = env.getPid();
    tableName = String.format("bulk_%s_%s_%d", hostname, pid, System.currentTimeMillis());
    log.info("Starting bulk test on " + tableName);

    TableOperations tableOps = env.getAccumuloClient().tableOperations();
    try {
      if (!tableOps.exists(getTableName())) {
        tableOps.create(getTableName());
        IteratorSetting is = new IteratorSetting(10, SummingCombiner.class);
        SummingCombiner.setEncodingType(is, LongCombiner.Type.STRING);
        SummingCombiner.setCombineAllColumns(is, true);
        tableOps.attachIterator(getTableName(), is);
      }
    } catch (TableExistsException ex) {
      // expected if there are multiple walkers
    }
    state.setRandom(new Random());
    state.set("fs", FileSystem.get(env.getHadoopConfiguration()));
    state.set("bulkImportSuccess", "true");
    BulkPlusOne.counter.set(0l);
    ThreadPoolExecutor e = ThreadPools.createFixedThreadPool(MAX_POOL_SIZE, "bulkImportPool",
        false);
    state.set("pool", e);
  }

  public static String getTableName() {
    return tableName;
  }

  public static ThreadPoolExecutor getThreadPool(State state) {
    return (ThreadPoolExecutor) state.get("pool");
  }

  public static void run(State state, Runnable r) {
    getThreadPool(state).submit(r);
  }

}
