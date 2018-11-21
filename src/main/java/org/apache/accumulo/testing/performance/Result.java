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

package org.apache.accumulo.testing.performance;

public class Result {

  public final String id;
  public final Number data;
  public final Stats stats;
  public final String description;
  public final Purpose purpose;

  public enum Purpose {
    /**
     * Use for results that are unrelated to the main objective or are not useful for comparison.
     */
    INFORMATIONAL,
    /**
     * Use for results that related to the main objective and are useful for comparison.
     */
    COMPARISON
  }

  public Result(String id, Number data, String description, Purpose purpose) {
    this.id = id;
    this.data = data;
    this.stats = null;
    this.description = description;
    this.purpose = purpose;
  }

  public Result(String id, Stats stats, String description, Purpose purpose) {
    this.id = id;
    this.stats = stats;
    this.data = null;
    this.description = description;
    this.purpose = purpose;
  }
}
