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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagerStressCleanupTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void controlDirectoryCleanupIsIdempotent() throws IOException {
    Path controlDirectory = temporaryDirectory.resolve("manager-stress-control");
    Path nestedFile = controlDirectory.resolve("nested").resolve("snapshot.properties");
    Files.createDirectories(nestedFile.getParent());
    Files.writeString(nestedFile, "metrics=true");

    ManagerStress.deleteRecursivelyIfExists(controlDirectory);

    assertFalse(Files.exists(controlDirectory));
    assertDoesNotThrow(() -> ManagerStress.deleteRecursivelyIfExists(controlDirectory));
  }

  @Test
  void hdfsBulkImportRunDirectoryCleanupIsRecursiveAndIdempotent() throws IOException {
    Path runDirectory = temporaryDirectory.resolve("run-1");
    Path stagedImport = runDirectory.resolve("worker-0/import-1/data.rf");
    Files.createDirectories(stagedImport.getParent());
    Files.writeString(stagedImport, "bulk import data");

    ManagerStress.deleteBulkImportRunDirectory(temporaryDirectory.toUri().toString(), "run-1");

    assertFalse(Files.exists(runDirectory));
    assertDoesNotThrow(() -> ManagerStress
        .deleteBulkImportRunDirectory(temporaryDirectory.toUri().toString(), "run-1"));
  }
}
