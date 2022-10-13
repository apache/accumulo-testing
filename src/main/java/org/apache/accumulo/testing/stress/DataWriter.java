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
package org.apache.accumulo.testing.stress;

import java.lang.ref.Cleaner.Cleanable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWriter extends Stream<Void> implements AutoCloseable {
  private final BatchWriter writer;
  private final RandomMutations mutations;
  private final Cleanable cleanable;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private static final Logger log = LoggerFactory.getLogger(DataWriter.class);

  public DataWriter(BatchWriter writer, RandomMutations mutations) {
    this.writer = writer;
    this.mutations = mutations;
    this.cleanable = CleanerUtil.unclosed(this, DataWriter.class, closed, log, writer);
  }

  @Override
  public Void next() {
    try {
      writer.addMutation(mutations.next());
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // deregister cleanable, but it won't run because it checks
      // the value of closed first, which is now true
      cleanable.clean();
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        System.err.println("Error closing batch writer.");
        e.printStackTrace();
      }
    }
  }
}
