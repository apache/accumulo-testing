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

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class IsolatedScan extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    String tableName = state.getRandomTableName();

    try {
      RowIterator iter = new RowIterator(
          new IsolatedScanner(client.createScanner(tableName, Authorizations.EMPTY)));

      while (iter.hasNext()) {
        PeekingIterator<Entry<Key,Value>> row = Iterators.peekingIterator(iter.next());
        Entry<Key,Value> kv = null;
        if (row.hasNext())
          kv = row.peek();
        while (row.hasNext()) {
          Entry<Key,Value> currentKV = row.next();
          if (!kv.getValue().equals(currentKV.getValue()))
            throw new Exception("values not equal " + kv + " " + currentKV);
        }
      }
      log.debug("Isolated scan " + tableName);
    } catch (TableDeletedException e) {
      log.debug("Isolated scan " + tableName + " failed, table deleted");
    } catch (TableNotFoundException e) {
      log.debug("Isolated scan " + tableName + " failed, doesnt exist");
    } catch (TableOfflineException e) {
      log.debug("Isolated scan " + tableName + " failed, offline");
    }
  }
}
