/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.testing.yarn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.ext.BundledJarRunnable;
import org.apache.twill.ext.BundledJarRunner;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class YarnAccumuloTestRunner {

  private static final Logger LOG = LoggerFactory.getLogger(YarnAccumuloTestRunner.class);

  private static final String RUNNABLE_ID = "BundledJarRunnable";

  private static class YarnTestApp implements TwillApplication {

    private TestRunnerOpts opts;
    private Properties props;

    YarnTestApp(TestRunnerOpts opts, Properties props) {
      this.opts = opts;
      this.props = props;
    }

    @Override
    public TwillSpecification configure() {

      int numCores = Integer.valueOf(props.getProperty(TestProps.YARN_CONTAINER_CORES));
      int memory = Integer.valueOf(props.getProperty(TestProps.YARN_CONTAINER_MEMORY_MB));

      ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
          .setVirtualCores(numCores).setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
          .setInstances(opts.numContainers).build();

      File jarFile = new File(opts.jarPath);
      File testProps = new File(opts.testProps);
      File log4jProps = new File(opts.logProps);

      return TwillSpecification.Builder.with()
          .setName(opts.testName)
          .withRunnable()
          .add(RUNNABLE_ID, new BundledJarRunnable(), resourceSpec)
          .withLocalFiles()
          .add(jarFile.getName(), jarFile.toURI(), false)
          .add(testProps.getName(), testProps.toURI())
          .add(log4jProps.getName(), log4jProps.toURI())
          .apply()
          .anyOrder()
          .build();
    }
  }

  private static class TestRunnerOpts {

    @Parameter(names={"--testName", "-t"}, required = true,  description = "Test name")
    String testName;

    @Parameter(names={"--numContainers", "-n"}, required = true,  description = "Test name")
    int numContainers;

    @Parameter(names={"--jar", "-j"}, required = true, description = "Bundled jar path")
    String jarPath;

    @Parameter(names={"--main", "-m"}, required = true, description = "Main class")
    String mainClass;

    @Parameter(names={"--testProps", "-p"}, required = true, description = "Test properties path")
    String testProps;

    @Parameter(names={"--logProps", "-l"}, required = true, description = "Log properties path")
    String logProps;

    @Parameter(names={"--args", "-a"}, variableArity = true, description = "Main class args")
    List<String> mainArgs = new ArrayList<>();
  }

  private static void verifyPath(String path) {
    File f = new File(path);
    Preconditions.checkState(f.exists());
    Preconditions.checkState(f.canRead());
  }

  private static int getNumRunning(TwillController controller) {
    ResourceReport report = controller.getResourceReport();
    if (report == null) {
      return 0;
    }
    Collection<TwillRunResources> resources = report.getRunnableResources(RUNNABLE_ID);
    return resources == null ? 0 : resources.size();
  }

  public static void main(String[] args) throws Exception {

    TestRunnerOpts opts = new TestRunnerOpts();
    new JCommander(opts, args);

    verifyPath(opts.jarPath);
    verifyPath(opts.testProps);
    verifyPath(opts.logProps);

    String[] mainArgs = opts.mainArgs.stream().toArray(String[]::new);
    BundledJarRunner.Arguments arguments = new BundledJarRunner.Arguments(opts.jarPath, "/lib",
                                                                          opts.mainClass, mainArgs);

    Properties props = new Properties();
    FileInputStream fis = new FileInputStream(opts.testProps);
    props.load(fis);
    fis.close();
    String zookeepers = props.getProperty(TestProps.ZOOKEEPERS);

    final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(),
                                                                      zookeepers);
    twillRunner.start();

    TwillController controller = twillRunner.prepare(
        new YarnTestApp(opts, props))
        .addJVMOptions("-Dlog4j.configuration=file:$PWD/" + new File(opts.logProps).getName())
        .withArguments("BundledJarRunnable", arguments.toArray())
        .start();

    int numRunning = getNumRunning(controller);
    while (numRunning != opts.numContainers) {
      LOG.info("{} of {} containers have started in YARN.", numRunning, opts.numContainers);
      Thread.sleep(5000);
      numRunning = getNumRunning(controller);
    }

    LOG.info("{} of {} containers have started in YARN", numRunning, opts.numContainers);
    LOG.info("{} application was successfully started in YARN", opts.testName);
  }
}