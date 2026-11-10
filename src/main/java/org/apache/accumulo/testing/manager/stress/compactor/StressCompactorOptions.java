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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.testing.cli.ClientOpts;

import com.beust.jcommander.Parameter;
import com.google.common.net.HostAndPort;

class StressCompactorOptions extends ClientOpts {

  @Parameter(names = "--duration", converter = TimeConverter.class,
      description = "duration of this simulator JVM (for example 10m or 1h)")
  long duration = 5 * 60 * 1000L;

  @Parameter(names = "--compactors-per-jvm",
      description = "number of simulated Compactor instances in this JVM")
  int compactorsPerJvm = 1;

  @Parameter(names = "--resource-group",
      description = "Compactor resource group to register and request jobs from")
  String resourceGroup = "default";

  @Parameter(names = "--host",
      description = "local host/interface reachable by the Manager for callbacks")
  String host;

  @Parameter(names = "--port-range",
      description = "Compactor callback port or inclusive range; 0 selects an ephemeral port")
  String portRange = "0";

  @Parameter(names = "--success-weight",
      description = "relative weight for successful zero-output completions")
  double successWeight = 1;

  @Parameter(names = "--failure-weight", description = "relative weight for failed jobs")
  double failureWeight = 1;

  @Parameter(names = "--cancellation-weight", description = "relative weight for cancelled jobs")
  double cancellationWeight = 1;

  @Parameter(names = "--seed", description = "random seed for outcome selection")
  long seed = System.nanoTime();

  void validate() {
    if (duration <= 0) {
      throw new IllegalArgumentException("--duration must be positive");
    }
    if (compactorsPerJvm <= 0) {
      throw new IllegalArgumentException("--compactors-per-jvm must be positive");
    }
    if (resourceGroup == null || resourceGroup.isBlank()) {
      throw new IllegalArgumentException("--resource-group must not be blank");
    }
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException(
          "--host must be a host/interface reachable by the Manager");
    }
    double totalWeight = successWeight + failureWeight + cancellationWeight;
    for (double weight : new double[] {successWeight, failureWeight, cancellationWeight}) {
      if (!Double.isFinite(weight) || weight < 0) {
        throw new IllegalArgumentException("Outcome weights must be finite and non-negative");
      }
    }
    if (!Double.isFinite(totalWeight) || totalWeight <= 0) {
      throw new IllegalArgumentException("At least one outcome weight must be positive");
    }
    if (portRange == null || !portRange.matches("(?:0|[1-9][0-9]{0,4}(?:-[1-9][0-9]{0,4})?)")) {
      throw new IllegalArgumentException("--port-range must be a port, 0, or inclusive port range");
    }
    String[] ports = portRange.split("-");
    int first = Integer.parseInt(ports[0]);
    int last = ports.length == 1 ? first : Integer.parseInt(ports[1]);
    if (first > 65535 || last > 65535 || last < first || first == 0 && last != 0) {
      throw new IllegalArgumentException("Invalid --port-range: " + portRange);
    }
    if (last - first > 4095) {
      throw new IllegalArgumentException("--port-range cannot contain more than 4096 ports");
    }
  }

  List<HostAndPort> addresses() {
    String[] ports = portRange.split("-");
    int first = Integer.parseInt(ports[0]);
    int last = ports.length == 1 ? first : Integer.parseInt(ports[1]);
    List<HostAndPort> addresses = new ArrayList<>(last - first + 1);
    for (int port = first; port <= last; port++) {
      addresses.add(HostAndPort.fromParts(host, port));
    }
    return addresses;
  }

}
