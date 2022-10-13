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
package org.apache.accumulo.testing.randomwalk;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a point in graph of RandomFramework
 */
public abstract class Node {

  protected final Logger log = LoggerFactory.getLogger(this.getClass());
  long progress = System.currentTimeMillis();

  /**
   * Visits node
   *
   * @param state
   *          Random walk state passed between nodes
   * @param env
   *          test environment
   */
  public abstract void visit(State state, RandWalkEnv env, Properties props) throws Exception;

  @Override
  public boolean equals(Object o) {
    if (o == null)
      return false;
    return toString().equals(o.toString());
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  synchronized public void makingProgress() {
    progress = System.currentTimeMillis();
  }

  synchronized public long lastProgress() {
    return progress;
  }
}
