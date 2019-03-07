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

import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.testing.performance.PerformanceTest;

public class MergeSiteConfig {
  public static void main(String[] args) throws Exception {
    String className = args[0];
    Path confFile = Paths.get(args[1], "accumulo.properties");

    PerformanceTest perfTest = Class.forName(className).asSubclass(PerformanceTest.class)
        .newInstance();

    Properties props = new Properties();

    try (Reader in = Files.newBufferedReader(confFile)) {
      props.load(in);
    }

    perfTest.getSystemConfig().getAccumuloConfig().forEach((k, v) -> props.setProperty(k, v));

    try (Writer out = Files.newBufferedWriter(confFile)) {
      props.store(out, "Modified by performance test");
    }
  }
}
