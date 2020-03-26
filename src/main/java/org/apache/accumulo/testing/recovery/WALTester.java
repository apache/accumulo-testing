/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.recovery;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.master.recovery.HadoopLogCloser;
import org.apache.accumulo.server.master.recovery.LogCloser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * To run the WALTester, copy accumulo-testing-shaded.jar to the Accumulo classpath, then run
 * accumulo org.apache.accumulo.testing.recovery.WALTester logCloserClass basePath [sleepSeconds]
 *
 * Example:
 * accumulo org.apache.accumulo.testing.recovery.WALTester \
 *   org.apache.accumulo.server.master.recovery.HadoopLogCloser \
 *   hdfs://localhost:8020/accumulo/file
 */
public class WALTester {
  public static final Logger log = LoggerFactory.getLogger(WALTester.class);
  private static final Text HELLO = new Text("hello");

  private AccumuloConfiguration siteConfig;
  private Configuration hadoopConfig;

  private LogCloser logCloser;
  private VolumeManager fs;

  private Path basePath;
  private int fileCount = 0;

  private String errMsg;

  public WALTester(String logCloserClass, String basePath) throws IOException {
    this.siteConfig = new SiteConfiguration();
    this.hadoopConfig = new Configuration();
    this.basePath = new Path(basePath);

    this.logCloser = ConfigurationTypeHelper.getClassInstance((String)null, logCloserClass,
        LogCloser.class, new HadoopLogCloser());

    this.fs = VolumeManagerImpl.get(siteConfig, hadoopConfig);
  }

  public static interface SyncFunc {
    void sync(FSDataOutputStream out) throws IOException;
  }

  public String getErrMsg() {
    return errMsg;
  }
  
  public boolean verifyWalOps(boolean syncable, SyncFunc syncFunc, String method,
      long sleepSeconds) throws IOException {
    fileCount++;
    errMsg = String.format("Test failure: syncable %s, sync function %s. ", syncable, method);
    Path filePath = new Path(basePath, Integer.toString(fileCount));
    boolean succeeded = true;
    FSDataOutputStream out;
    if (syncable) {
      log.info("Creating syncable file");
      out = fs.createSyncable(filePath,  0, (short) 3, 67108864);
    } else {
      log.info("Creating file");
      out = fs.create(filePath, true, 0, (short) 3, 67108864);
    }

    if (sleepSeconds > 0) {
      log.info("Sleeping for {} seconds before trying to write to file (for example to check " +
              "lease renewal)", sleepSeconds);
      try {
        Thread.sleep(sleepSeconds * 1000);
      } catch (InterruptedException e) {}
    }

    log.info("Writing to file");
    HELLO.write(out);
    syncFunc.sync(out);
    
    HELLO.write(out);
    long time = 1;
    while (time > 0) {
      log.info("Calling log closer");
      time = logCloser.close(siteConfig, hadoopConfig, fs, filePath);
      log.info("Sleeping for {}ms", time);
      try {
        Thread.sleep(time);
      } catch (InterruptedException e) {
      }
    }

    boolean gotException = false;
    try {
      log.info("Writing to file after log close");
      HELLO.write(out);
      log.info("Syncing after log close");
      syncFunc.sync(out);
    } catch (Exception e) {
      log.info("Got exception on write+sync after close as expected", e);
      gotException = true;
    }
    if (!gotException) {
      String err = "No exception on write+sync after log was closed. ";
      errMsg += err;
      log.error(err);
      succeeded = false;
    }

    try {
      if (out != null) {
        out.close();
        succeeded = false;
      }
    } catch (Exception e) {
      log.info("Got exception on close as expected", e);
    }

    log.info("Reading file");
    Text t = new Text();
    try (FSDataInputStream in = fs.open(filePath)) {
      int count = 0;
      while (in.available() > 0) {
        t.readFields(in);
        log.info("Read text " + t);
        count++;
      }
      if (count != 1) {
        String err = String.format("Expected to read 1 flushed entry from file, got %d.\n", count);
        errMsg += err;
        log.error(err);
        succeeded = false;
      }
    }

    if (succeeded) {
      errMsg = "";
    }
    return succeeded;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      throw new IllegalArgumentException("Expected <logCloserClass> <basePath> " +
          "[sleepSeconds] arguments.");
    }

    WALTester walTester = new WALTester(args[0], args[1]);
    long sleepSeconds = 0;
    if (args.length > 2) {
      sleepSeconds = Long.parseLong(args[2]);
    }

    boolean succeeded = true;
    String errMsg = "";

    succeeded &= walTester.verifyWalOps(false, FSDataOutputStream::hsync, "hsync", sleepSeconds);
    errMsg += walTester.getErrMsg();
    succeeded &= walTester.verifyWalOps(false, FSDataOutputStream::hflush, "hflush", sleepSeconds);
    errMsg += walTester.getErrMsg();
    succeeded &= walTester.verifyWalOps(true, FSDataOutputStream::hsync, "hsync", sleepSeconds);
    errMsg += walTester.getErrMsg();
    succeeded &= walTester.verifyWalOps(true, FSDataOutputStream::hflush, "hflush", sleepSeconds);
    errMsg += walTester.getErrMsg();

    if (succeeded) {
      log.info("TEST SUCCEEDED");
    } else {
      log.error("TEST FAILED\n{}", errMsg);
      System.exit(1);
    }
  }
}
