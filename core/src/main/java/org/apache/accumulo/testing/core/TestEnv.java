package org.apache.accumulo.testing.core;

import static java.util.Objects.requireNonNull;

import java.lang.management.ManagementFactory;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.conf.Configuration;

public class TestEnv {

  protected final Properties testProps;
  private String clientPropsPath;
  private ClientInfo info;
  private AccumuloClient client = null;
  private Configuration hadoopConfig = null;

  public TestEnv(String testPropsPath, String clientPropsPath) {
    requireNonNull(testPropsPath);
    requireNonNull(clientPropsPath);
    this.testProps = TestProps.loadFromFile(testPropsPath);
    this.clientPropsPath = clientPropsPath;
    this.info = ClientInfo.from(TestProps.loadFromFile(clientPropsPath));
  }

  /**
   * @return a copy of the test properties
   */
  public Properties getTestProperties() {
    return new Properties(testProps);
  }

  /**
   * @return a test property value given a key
   */
  public String getTestProperty(String key) {
    return testProps.getProperty(key);
  }

  public String getClientPropsPath() {
    return clientPropsPath;
  }

  public ClientInfo getInfo() {
    return info;
  }

  /**
   * Gets the configured username.
   *
   * @return username
   */
  public String getAccumuloUserName() {
    return info.getPrincipal();
  }

  /**
   * Gets the configured password.
   *
   * @return password
   */
  public String getAccumuloPassword() {
    String authType = info.getProperties().getProperty(ClientProperty.AUTH_TYPE.getKey());
    if (authType.equals("password")) {
      return info.getProperties().getProperty(ClientProperty.AUTH_TOKEN.getKey());
    }
    return null;
  }

  /**
   * Gets this process's ID.
   *
   * @return pid
   */
  public String getPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  public Configuration getHadoopConfiguration() {
    if (hadoopConfig == null) {
      hadoopConfig = new Configuration();
      hadoopConfig.set("fs.defaultFS", getHdfsRoot());
      // Below is required due to bundled jar breaking default config.
      // See http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
      hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      hadoopConfig.set("mapreduce.framework.name", "yarn");
      hadoopConfig.set("yarn.resourcemanager.hostname", getYarnResourceManager());
    }
    return hadoopConfig;
  }

  /**
   * Gets an authentication token based on the configured password.
   */
  public AuthenticationToken getToken() {
    return info.getAuthenticationToken();
  }

  public String getHdfsRoot() {
    return testProps.getProperty(TestProps.HDFS_ROOT);
  }

  public String getYarnResourceManager() {
    return testProps.getProperty(TestProps.YARN_RESOURCE_MANAGER);
  }

  /**
   * Gets an Accumulo client. The same client is reused after the first call.
   */
  public AccumuloClient getAccumuloClient() throws AccumuloException, AccumuloSecurityException {
    if (client == null) {
      client = Accumulo.newClient().from(info).build();
    }
    return client;
  }
}
