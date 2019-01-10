package org.apache.accumulo.testing;

import static java.util.Objects.requireNonNull;

import java.lang.management.ManagementFactory;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.conf.Configuration;

public class TestEnv implements AutoCloseable {

  protected final Properties testProps;
  private String clientPropsPath;
  private final Properties clientProps;
  private AccumuloClient client = null;
  private Configuration hadoopConfig = null;

  public TestEnv(String testPropsPath, String clientPropsPath) {
    requireNonNull(testPropsPath);
    requireNonNull(clientPropsPath);
    this.testProps = TestProps.loadFromFile(testPropsPath);
    this.clientPropsPath = clientPropsPath;
    this.clientProps = Accumulo.newClientProperties().from(clientPropsPath).build();
  }

  private Properties copyProperties(Properties props) {
    Properties result = new Properties();
    props.forEach((key, value) -> result.setProperty((String) key, (String) value));
    return result;
  }

  /**
   * @return a copy of the test properties
   */
  public Properties getTestProperties() {
    return copyProperties(testProps);
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

  public Properties getClientProps() {
    return copyProperties(clientProps);
  }

  /**
   * Gets the configured username.
   *
   * @return username
   */
  public String getAccumuloUserName() {
    return ClientProperty.AUTH_PRINCIPAL.getValue(clientProps);
  }

  /**
   * Gets the configured password.
   *
   * @return password
   */
  public String getAccumuloPassword() {
    String authType = ClientProperty.AUTH_TYPE.getValue(clientProps);
    if (authType.equals("password")) {
      return ClientProperty.AUTH_TOKEN.getValue(clientProps);
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
      hadoopConfig
          .set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      hadoopConfig.set("mapreduce.framework.name", "yarn");
      hadoopConfig.set("yarn.resourcemanager.hostname", getYarnResourceManager());
      String hadoopHome = System.getenv("HADOOP_HOME");
      if (hadoopHome == null) {
        throw new IllegalArgumentException("HADOOP_HOME must be set in env");
      }
      hadoopConfig.set("yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=" + hadoopHome);
      hadoopConfig.set("mapreduce.map.env", "HADOOP_MAPRED_HOME=" + hadoopHome);
      hadoopConfig.set("mapreduce.reduce.env", "HADOOP_MAPRED_HOME=" + hadoopHome);
    }
    return hadoopConfig;
  }

  /**
   * Gets an authentication token based on the configured password.
   */
  public AuthenticationToken getToken() {
    return ClientProperty.getAuthenticationToken(clientProps);
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
  public synchronized AccumuloClient getAccumuloClient() {
    if (client == null) {
      client = Accumulo.newClient().from(clientProps).build();
    }
    return client;
  }

  public AccumuloClient createClient(String principal, AuthenticationToken token) {
    return Accumulo.newClient().from(clientProps).as(principal, token).build();
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }
}
