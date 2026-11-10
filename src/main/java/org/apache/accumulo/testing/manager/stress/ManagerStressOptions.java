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

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.testing.cli.ClientOpts;

import com.beust.jcommander.Parameter;

class ManagerStressOptions extends ClientOpts {

  @Parameter(names = "--duration", converter = TimeConverter.class,
      description = "duration of the stress run (for example 10m or 1h)")
  long duration = 5 * 60 * 1000L;

  @Parameter(names = "--clients", description = "number of local worker JVMs")
  int clients = 2;

  @Parameter(names = "--tables", description = "target number of managed tables")
  int tables = 4;

  @Parameter(names = "--namespace", required = true,
      description = "new namespace used for all tables created by this run")
  String namespace;

  @Parameter(names = "--compactor-resource-group", required = true,
      description = "resource group used by the external Compactor simulators")
  String compactorResourceGroup;

  @Parameter(names = "--table-prefix", description = "prefix for tables created by this run")
  String tablePrefix;

  @Parameter(names = "--hdfs-dir", description = "shared HDFS directory for bulk import files")
  String hdfsDir;

  @Parameter(names = "--create-weight", description = "relative weight for create-table tasks")
  double createWeight = 1;

  @Parameter(names = "--delete-weight", description = "relative weight for delete-table tasks")
  double deleteWeight = 1;

  @Parameter(names = "--split-weight", description = "relative weight for split tasks")
  double splitWeight = 1;

  @Parameter(names = "--merge-weight", description = "relative weight for merge tasks")
  double mergeWeight = 1;

  @Parameter(names = "--availability-weight",
      description = "relative weight for tablet availability changes")
  double availabilityWeight = 1;

  @Parameter(names = "--compact-weight", description = "relative weight for compaction tasks")
  double compactWeight = 1;

  @Parameter(names = "--bulk-import-weight", description = "relative weight for bulk import tasks")
  double bulkImportWeight = 1;

  @Parameter(names = "--seed", description = "random seed (defaults to a per-run random seed)")
  long seed = System.nanoTime();

  @Parameter(names = "--internal-worker-id", hidden = true)
  int workerId = -1;

  @Parameter(names = "--internal-state-dir", hidden = true)
  String stateDir;

  @Parameter(names = "--internal-run-id", hidden = true)
  String runId;

  void validate(boolean coordinator) {
    if (duration <= 0) {
      throw new IllegalArgumentException("--duration must be positive");
    }
    if (clients <= 0) {
      throw new IllegalArgumentException("--clients must be positive");
    }
    if (tables <= 0) {
      throw new IllegalArgumentException("--tables must be positive");
    }
    if (namespace == null || namespace.isBlank()) {
      throw new IllegalArgumentException("--namespace is required");
    }
    if (!namespace.matches("[A-Za-z0-9_]+")) {
      throw new IllegalArgumentException("--namespace may contain only letters, digits, and _");
    }
    if (compactorResourceGroup == null || compactorResourceGroup.isBlank()) {
      throw new IllegalArgumentException("--compactor-resource-group is required");
    }
    ResourceGroupId.of(compactorResourceGroup);
    if (coordinator && workerId >= 0) {
      throw new IllegalArgumentException("--internal-worker-id is reserved for worker JVMs");
    }
    if (!coordinator && (workerId < 0 || stateDir == null || runId == null)) {
      throw new IllegalArgumentException("Missing internal worker configuration");
    }
    if (tablePrefix == null || tablePrefix.isBlank()) {
      if (!coordinator) {
        throw new IllegalArgumentException("Worker table prefix was not supplied");
      }
    } else if (!tablePrefix.matches("[A-Za-z0-9_]+")) {
      throw new IllegalArgumentException("--table-prefix may contain only letters, digits, and _");
    }
    double totalWeight = createWeight + deleteWeight + splitWeight + mergeWeight
        + availabilityWeight + compactWeight + bulkImportWeight;
    for (double weight : new double[] {createWeight, deleteWeight, splitWeight, mergeWeight,
        availabilityWeight, compactWeight, bulkImportWeight}) {
      if (!Double.isFinite(weight) || weight < 0) {
        throw new IllegalArgumentException("Operation weights must be finite and non-negative");
      }
    }
    if (!Double.isFinite(totalWeight) || totalWeight <= 0) {
      throw new IllegalArgumentException("At least one operation weight must be positive");
    }
    if (bulkImportWeight > 0 && (hdfsDir == null || hdfsDir.isBlank())) {
      throw new IllegalArgumentException("--hdfs-dir is required when bulk import is enabled");
    }
  }
}
