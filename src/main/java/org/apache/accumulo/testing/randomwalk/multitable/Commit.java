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
package org.apache.accumulo.testing.randomwalk.multitable;

import java.util.Properties;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

public class Commit extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    try {
      env.getMultiTableBatchWriter().flush();
    } catch (TableOfflineException e) {
      log.debug("Commit failed, table offline");
      return;
    } catch (MutationsRejectedException mre) {
      if (mre.getCause() instanceof TableDeletedException)
        log.debug("Commit failed, table deleted");
      else if (mre.getCause() instanceof TableOfflineException)
        log.debug("Commit failed, table offline");
      else
        throw mre;
      return;
    }

    Long numWrites = state.getLong("numWrites");
    Long totalWrites = state.getLong("totalWrites") + numWrites;

    log.debug("Committed " + numWrites + " writes.  Total writes: " + totalWrites);

    state.set("totalWrites", totalWrites);
    state.set("numWrites", 0L);
  }

}
