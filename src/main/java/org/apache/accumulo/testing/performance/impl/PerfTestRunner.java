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

package org.apache.accumulo.testing.performance.impl;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.testing.performance.Environment;
import org.apache.accumulo.testing.performance.PerformanceTest;
import org.apache.accumulo.testing.performance.Report;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PerfTestRunner {
  public static void main(String[] args) throws Exception {
    String clientProps = args[0];
    String className = args[1];
    String accumuloVersion = args[2];
    String outputDir = args[3];

    PerformanceTest perfTest = Class.forName(className).asSubclass(PerformanceTest.class)
        .newInstance();

    AccumuloClient client = Accumulo.newClient().from(clientProps).build();

    Instant start = Instant.now();

    Report result = perfTest.runTest(new Environment() {
      @Override
      public AccumuloClient getClient() {
        return client;
      }
    });

    Instant stop = Instant.now();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    ContextualReport report = new ContextualReport(className, accumuloVersion, start, stop, result);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    String time = Instant.now().atZone(ZoneId.systemDefault()).format(formatter);
    Path outputFile = Paths.get(outputDir,
        perfTest.getClass().getSimpleName() + "_" + time + ".json");

    try (Writer writer = Files.newBufferedWriter(outputFile)) {
      gson.toJson(report, writer);
    }
  }
}
