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
package org.apache.accumulo.testing.manager.stress;

import java.util.random.RandomGenerator;

enum Operation {
  CREATE, DELETE, SPLIT, MERGE, AVAILABILITY, COMPACT, BULK_IMPORT;

  boolean isAsynchronous() {
    return this == AVAILABILITY || this == COMPACT;
  }

  static Operation choose(ManagerStressOptions options, RandomGenerator random) {
    double total =
        options.createWeight + options.deleteWeight + options.splitWeight + options.mergeWeight
            + options.availabilityWeight + options.compactWeight + options.bulkImportWeight;
    return choose(options, random.nextDouble() * total);
  }

  static Operation choose(ManagerStressOptions options, double selection) {
    if ((selection -= options.createWeight) < 0) {
      return CREATE;
    }
    if ((selection -= options.deleteWeight) < 0) {
      return DELETE;
    }
    if ((selection -= options.splitWeight) < 0) {
      return SPLIT;
    }
    if ((selection -= options.mergeWeight) < 0) {
      return MERGE;
    }
    if ((selection -= options.availabilityWeight) < 0) {
      return AVAILABILITY;
    }
    if ((selection -= options.compactWeight) < 0) {
      return COMPACT;
    }
    if (options.bulkImportWeight > 0) {
      return BULK_IMPORT;
    }
    // Guard floating point rounding at the final positive-weight boundary.
    if (options.availabilityWeight > 0) {
      return AVAILABILITY;
    }
    if (options.compactWeight > 0) {
      return COMPACT;
    }
    if (options.mergeWeight > 0) {
      return MERGE;
    }
    if (options.splitWeight > 0) {
      return SPLIT;
    }
    if (options.deleteWeight > 0) {
      return DELETE;
    }
    return CREATE;
  }
}
