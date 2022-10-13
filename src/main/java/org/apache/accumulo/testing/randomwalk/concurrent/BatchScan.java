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
package org.apache.accumulo.testing.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class BatchScan extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    AccumuloClient client = env.getAccumuloClient();
    Random rand = state.getRandom();
    String tableName = state.getRandomTableName();

    try (BatchScanner bs = client.createBatchScanner(tableName, Authorizations.EMPTY, 3)) {
      List<Range> ranges = new ArrayList<>();
      for (int i = 0; i < rand.nextInt(2000) + 1; i++)
        ranges.add(new Range(String.format("%016x", rand.nextLong() & 0x7fffffffffffffffL)));

      bs.setRanges(ranges);

      Iterator<Entry<Key,Value>> iter = bs.iterator();
      while (iter.hasNext())
        iter.next();

      log.debug("Wrote to " + tableName);
    } catch (TableNotFoundException e) {
      log.debug("BatchScan " + tableName + " failed, doesn't exist");
    } catch (TableDeletedException tde) {
      log.debug("BatchScan " + tableName + " failed, table deleted");
    } catch (TableOfflineException e) {
      log.debug("BatchScan " + tableName + " failed, offline");
    } catch (RuntimeException e) {
      if (e.getCause() instanceof AccumuloSecurityException) {
        log.debug("BatchScan " + tableName + " failed, permission error");
      } else {
        throw e;
      }
    }
  }
}
