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

package org.apache.accumulo.testing.core.performance.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.accumulo.testing.core.performance.PerformanceTest;
import org.apache.hadoop.conf.Configuration;

public class MergeSiteConfig {
  public static void main(String[] args) throws Exception {
    String className = args[0];
    Path confFile = Paths.get(args[1], "accumulo-site.xml");

    PerformanceTest perfTest = Class.forName(className).asSubclass(PerformanceTest.class).newInstance();

    Configuration conf = new Configuration(false);
    byte[] newConf;


    try(BufferedInputStream in = new BufferedInputStream(Files.newInputStream(confFile))){
      conf.addResource(in);
      perfTest.getConfiguration().getAccumuloSite().forEach((k,v) -> conf.set(k, v));
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      conf.writeXml(baos);
      baos.close();
      newConf = baos.toByteArray();
    }


    Files.write(confFile, newConf);
  }
}
