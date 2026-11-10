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

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts multiple registered Compactor simulators as threads in this JVM. */
public class StressCompactor {

  private static final Logger log = LoggerFactory.getLogger(StressCompactor.class);

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      args = new String[] {"--help"};
    }
    StressCompactorOptions options = new StressCompactorOptions();
    options.parseArgs(StressCompactor.class.getName(), args);
    if (options.host == null || options.host.isBlank()) {
      options.host = InetAddress.getLocalHost().getCanonicalHostName();
    }
    options.validate();

    List<SimulatedCompactor> compactors = new ArrayList<>(options.compactorsPerJvm);
    AtomicBoolean stopping = new AtomicBoolean();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (stopping.compareAndSet(false, true)) {
        stopCompactors(compactors);
      }
    }, "compactor-stress-shutdown"));

    try {
      for (int i = 0; i < options.compactorsPerJvm; i++) {
        SimulatedCompactor compactor = new SimulatedCompactor(options, i);
        compactors.add(compactor);
        compactor.start();
      }

      long deadline = Math.addExact(System.currentTimeMillis(), options.duration);
      compactors.forEach(SimulatedCompactor::startPolling);
      log.info("Started {} Compactor simulator(s) in JVM {} for {} ms", compactors.size(),
          ProcessHandle.current().pid(), options.duration);
      while (System.currentTimeMillis() < deadline) {
        long remaining = deadline - System.currentTimeMillis();
        Thread.sleep(Math.min(remaining, Duration.ofSeconds(1).toMillis()));
      }
    } finally {
      if (stopping.compareAndSet(false, true)) {
        stopCompactors(compactors);
      }
    }
  }

  private static void stopCompactors(List<SimulatedCompactor> compactors) {
    for (int i = compactors.size() - 1; i >= 0; i--) {
      compactors.get(i).close();
    }
    long polls = 0;
    long idle = 0;
    long jobs = 0;
    long successes = 0;
    long failures = 0;
    long cancellations = 0;
    long rpcFailures = 0;
    long outcomeReportFailures = 0;
    for (SimulatedCompactor compactor : compactors) {
      SimulatedCompactor.Counters counters = compactor.getCounters();
      polls += counters.jobRequests.sum();
      idle += counters.idleResponses.sum();
      jobs += counters.jobsReceived.sum();
      successes += counters.successes.sum();
      failures += counters.failures.sum();
      cancellations += counters.cancellations.sum();
      rpcFailures += counters.rpcFailures.sum();
      outcomeReportFailures += counters.outcomeReportFailures.sum();
    }
    log.info(
        "Compactor JVM totals: simulators={}, polls={}, idle={}, jobs={}, success={}, failure={}, "
            + "cancelled={}, rpcFailures={}, outcomeReportFailures={}",
        compactors.size(), polls, idle, jobs, successes, failures, cancellations, rpcFailures,
        outcomeReportFailures);
  }
}
