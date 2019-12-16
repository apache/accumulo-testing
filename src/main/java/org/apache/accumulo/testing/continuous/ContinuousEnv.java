package org.apache.accumulo.testing.continuous;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.TestEnv;
import org.apache.accumulo.testing.TestProps;
import org.apache.hadoop.mapreduce.InputFormat;

public class ContinuousEnv extends TestEnv {

  private List<Authorizations> authList;

  public ContinuousEnv(String[] args) {
    super(args);
  }

  /**
   * @return Accumulo authorizations list
   */
  private List<Authorizations> getAuthList() {
    if (authList == null) {
      String authValue = testProps.getProperty(TestProps.CI_COMMON_AUTHS);
      if (authValue == null || authValue.trim().isEmpty()) {
        authList = Collections.singletonList(Authorizations.EMPTY);
      } else {
        authList = new ArrayList<>();
        for (String a : authValue.split("\\|")) {
          authList.add(new Authorizations(a.split(",")));
        }
      }
    }
    return authList;
  }

  /**
   * @return random authorization
   */
  Authorizations getRandomAuthorizations() {
    Random r = new Random();
    return getAuthList().get(r.nextInt(getAuthList().size()));
  }

  /**
   * Provide the input format
   * 
   * @return input format specified via the configuration. Default is the threaded input format,
   *         which may not be useful in all cases.
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<TestKey,Value>> getInputFormat()
      throws ClassNotFoundException, ClassCastException {
    return (Class<? extends InputFormat<TestKey,Value>>) Class
        .forName(testProps.getProperty(TestProps.CI_COMMON_INPUT_FORMAT,
            AsyncContinousInputFormat.class.getCanonicalName()))
        .asSubclass(InputFormat.class);
  }

  /**
   * Returns the number of threads to use when async mode is selected
   *
   * @return number of threads
   */
  public long getAsyncThreadCount() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_COMMON_ASYNC_THREADS, "5"));
  }

  /**
   * Returns the number of keys to queue in async mode
   *
   * @return number of keys to queue
   */
  public long getAsyncKeysToQueue() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_COMMON_ASYNC_KEYS_QUEUE, "1000"));
  }

  public long getRowMin() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_ROW_MIN));
  }

  public long getRowMax() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_ROW_MAX));
  }

  public int getMaxColF() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_CF));
  }

  public int getMaxColQ() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_CQ));
  }

  public int getBulkMapTask() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_BULK_MAP_TASK));
  }

  public long getBulkMapNodes() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_BULK_MAP_NODES));
  }

  public int getBulkReducers() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_BULK_REDUCERS));
  }

  public String getAccumuloTableName() {
    return testProps.getProperty(TestProps.CI_COMMON_ACCUMULO_TABLE);
  }
}
