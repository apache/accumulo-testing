package org.apache.accumulo.testing.core.continuous;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.TestEnv;
import org.apache.accumulo.testing.core.TestProps;

class ContinuousEnv extends TestEnv {

  private List<Authorizations> authList;

  ContinuousEnv(String testPropsPath, String clientPropsPath) {
    super(testPropsPath, clientPropsPath);
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
        for (String a : authValue.split("|")) {
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

  long getRowMin() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_ROW_MIN));
  }

  long getRowMax() {
    return Long.parseLong(testProps.getProperty(TestProps.CI_INGEST_ROW_MAX));
  }

  int getMaxColF() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_CF));
  }

  int getMaxColQ() {
    return Integer.parseInt(testProps.getProperty(TestProps.CI_INGEST_MAX_CQ));
  }

  String getAccumuloTableName() {
    return testProps.getProperty(TestProps.CI_COMMON_ACCUMULO_TABLE);
  }
}
