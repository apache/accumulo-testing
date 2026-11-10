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
package org.apache.accumulo.testing.manager.stress.compactor;

import java.util.random.RandomGenerator;

enum CompactionOutcome {
  SUCCESS, FAILURE, CANCELLATION;

  static CompactionOutcome choose(StressCompactorOptions options, RandomGenerator random) {
    double total = options.successWeight + options.failureWeight + options.cancellationWeight;
    return choose(options, random.nextDouble() * total);
  }

  static CompactionOutcome choose(StressCompactorOptions options, double selection) {
    if ((selection -= options.successWeight) < 0) {
      return SUCCESS;
    }
    if ((selection -= options.failureWeight) < 0) {
      return FAILURE;
    }
    if (options.cancellationWeight > 0) {
      return CANCELLATION;
    }
    if (options.failureWeight > 0) {
      return FAILURE;
    }
    return SUCCESS;
  }
}
