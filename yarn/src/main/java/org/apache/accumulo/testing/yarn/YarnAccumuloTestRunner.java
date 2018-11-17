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
import org.apache.accumulo.testing.core.TestEnv;
import org.apache.accumulo.testing.core.TestProps;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.ext.BundledJarRunnable;
import org.apache.twill.ext.BundledJarRunner;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class YarnAccumuloTestRunner {

  private static final Logger LOG = LoggerFactory.getLogger(YarnAccumuloTestRunner.class);

  private static final String RUNNABLE_ID = "AccumuloTest";

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

      ResourceSpecification resourceSpec = ResourceSpecification.Builder.with().setVirtualCores(numCores)
          .setMemory(memory, ResourceSpecification.SizeUnit.MEGA).setInstances(opts.numContainers).build();

      File jarFile = new File(opts.jarPath);
      File clientProps = new File(opts.clientProps);
      File testProps = new File(opts.testProps);
      File log4jProps = new File(opts.logProps);

      return TwillSpecification.Builder.with().setName(opts.testName).withRunnable().add(RUNNABLE_ID, new BundledJarRunnable(), resourceSpec).withLocalFiles()
          .add(jarFile.getName(), jarFile.toURI(), false).add(testProps.getName(), testProps.toURI()).add(clientProps.getName(), clientProps.toURI())
          .add(log4jProps.getName(), log4jProps.toURI()).apply().anyOrder().build();
    }
  }

  private static class TestRunnerOpts {

    @Parameter(names = {"--testName", "-t"}, required = true, description = "Test name")
    String testName;

    @Parameter(names = {"--numContainers", "-n"}, required = true, description = "Test name")
    int numContainers;

    @Parameter(names = {"--jar", "-j"}, required = true, description = "Bundled jar path")
    String jarPath;

    @Parameter(names = {"--main", "-m"}, required = true, description = "Main class")
    String mainClass;

    @Parameter(names = {"--clientProps", "-c"}, required = true, description = "Accumulo client properties path")
    String clientProps;

    @Parameter(names = {"--testProps", "-p"}, required = true, description = "Test properties path")
    String testProps;

    @Parameter(names = {"--logProps", "-l"}, required = true, description = "Log properties path")
    String logProps;

    @Parameter(names = {"--args", "-a"}, variableArity = true, description = "Main class args")
    List<String> mainArgs = new ArrayList<>();
  }

  private static void verifyPath(String path) {
    File f = new File(path);
    Preconditions.checkState(f.exists());
    Preconditions.checkState(f.canRead());
  }

  public static void main(String[] args) throws Exception {

    TestRunnerOpts opts = new TestRunnerOpts();
    JCommander commander = new JCommander();
    commander.addObject(opts);
    commander.parse(args);

    verifyPath(opts.jarPath);
    verifyPath(opts.testProps);
    verifyPath(opts.clientProps);
    verifyPath(opts.logProps);

    String jarFileName = Paths.get(opts.jarPath).getFileName().toString();
    String[] mainArgs = opts.mainArgs.stream().toArray(String[]::new);

    Objects.requireNonNull(jarFileName);
    Objects.requireNonNull(opts.mainClass);
    Objects.requireNonNull(mainArgs);

    BundledJarRunner.Arguments arguments =
        new BundledJarRunner.Arguments.Builder().setJarFileName(jarFileName)
            .setLibFolder("lib").setMainClassName(opts.mainClass)
            .setMainArgs(mainArgs).createArguments();

    TestEnv env = new TestEnv(opts.testProps, opts.clientProps);

    YarnConfiguration yarnConfig = new YarnConfiguration(env.getHadoopConfiguration());
    TwillRunnerService twillRunner = new YarnTwillRunnerService(yarnConfig, env.getInfo().getZooKeepers());
    twillRunner.start();

    twillRunner.prepare(new YarnTestApp(opts, env.getTestProperties()))
        .addJVMOptions("-Dlog4j.configuration=file:$PWD/" + new File(opts.logProps).getName())
        .withArguments(RUNNABLE_ID, arguments.toArray()).start();

    LOG.info("{} containers will start in YARN.", opts.numContainers);
    LOG.info("Press Ctrl-C when these containers have started.");

    while (true) {
      Thread.sleep(1000);
    }
  }
}
