package org.apache.accumulo.testing.core;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.security.UserGroupInformation;

public class TestEnv {

  protected final Properties p;
  private Instance instance = null;
  private Connector connector = null;

  /**
   * Creates new test environment using provided properties
   *
   * @param p
   *          Properties
   */
  public TestEnv(Properties p) {
    requireNonNull(p);
    this.p = p;
  }

  /**
   * Gets a copy of the configuration properties.
   *
   * @return a copy of the configuration properties
   */
  public Properties copyConfigProperties() {
    return new Properties(p);
  }

  /**
   * Gets a configuration property.
   *
   * @param key
   *          key
   * @return property value
   */
  public String getConfigProperty(String key) {
    return p.getProperty(key);
  }

  /**
   * Gets the configured username.
   *
   * @return username
   */
  public String getAccumuloUserName() {
    return p.getProperty(TestProps.ACCUMULO_USERNAME);
  }

  /**
   * Gets the configured password.
   *
   * @return password
   */
  public String getAccumuloPassword() {
    return p.getProperty(TestProps.ACCUMULO_PASSWORD);
  }

  /**
   * Gets the configured keytab.
   *
   * @return path to keytab
   */
  public String getAccumuloKeytab() {
    return p.getProperty(TestProps.ACCUMULO_KEYTAB);
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
    Configuration config = new Configuration();
    config.set("mapreduce.framework.name", "yarn");
    // Setting below are required due to bundled jar breaking default
    // config.
    // See
    // http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return config;
  }

  /**
   * Gets an authentication token based on the configured password.
   */
  public AuthenticationToken getToken() {
    String password = getAccumuloPassword();
    if (null != password) {
      return new PasswordToken(getAccumuloPassword());
    }
    String keytab = getAccumuloKeytab();
    if (null != keytab) {
      File keytabFile = new File(keytab);
      if (!keytabFile.exists() || !keytabFile.isFile()) {
        throw new IllegalArgumentException("Provided keytab is not a normal file: " + keytab);
      }
      try {
        UserGroupInformation.loginUserFromKeytab(getAccumuloUserName(), keytabFile.getAbsolutePath());
        return new KerberosToken();
      } catch (IOException e) {
        throw new RuntimeException("Failed to login", e);
      }
    }
    throw new IllegalArgumentException("Must provide password or keytab in configuration");
  }

  public String getAccumuloInstanceName() {
    return p.getProperty(TestProps.ACCUMULO_INSTANCE);
  }

  public String getZookeepers() {
    return p.getProperty(TestProps.ZOOKEEPERS);
  }

  public ClientConfiguration getClientConfiguration() {
    return ClientConfiguration.loadDefault().withInstance(getAccumuloInstanceName()).withZkHosts(getZookeepers());
  }

  /**
   * Gets an Accumulo instance object. The same instance is reused after the first call.
   */
  public Instance getAccumuloInstance() {
    if (instance == null) {
      this.instance = new ZooKeeperInstance(getClientConfiguration());
    }
    return instance;
  }

  /**
   * Gets an Accumulo connector. The same connector is reused after the first call.
   */
  public Connector getAccumuloConnector() throws AccumuloException, AccumuloSecurityException {
    if (connector == null) {
      connector = getAccumuloInstance().getConnector(getAccumuloUserName(), getToken());
    }
    return connector;
  }

  public BatchWriterConfig getBatchWriterConfig() {
    int numThreads = Integer.parseInt(p.getProperty(TestProps.BW_NUM_THREADS));
    long maxLatency = Long.parseLong(p.getProperty(TestProps.BW_MAX_LATENCY_MS));
    long maxMemory = Long.parseLong(p.getProperty(TestProps.BW_MAX_MEM_BYTES));

    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxWriteThreads(numThreads);
    config.setMaxLatency(maxLatency, TimeUnit.MILLISECONDS);
    config.setMaxMemory(maxMemory);
    return config;
  }

  public int getScannerBatchSize() {
    return Integer.parseInt(p.getProperty(TestProps.SCANNER_BATCH_SIZE));
  }
}
