/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.testing;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosHelper {
  private static final Logger log = LoggerFactory.getLogger(KerberosHelper.class);

  public static void saslLogin(Properties clientProps, Configuration conf) {
    if (Boolean.parseBoolean(ClientProperty.SASL_ENABLED.getValue(clientProps))) {
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(conf);
      try {
        UserGroupInformation.loginUserFromKeytab(
            ClientProperty.AUTH_PRINCIPAL.getValue(clientProps),
            ClientProperty.AUTH_TOKEN.getValue(clientProps));
      } catch (IOException e) {
        log.warn("Sasl login failed", e);
      }
    }
  }

  public static Properties configDelegationToken(Properties clientProps)
      throws AccumuloException, AccumuloSecurityException {
    if (Boolean.parseBoolean(ClientProperty.SASL_ENABLED.getValue(clientProps))) {
      AccumuloClient client = Accumulo.newClient().from(clientProps).build();
      DelegationToken dt = client.securityOperations().getDelegationToken(null);
      clientProps = Accumulo.newClientProperties().from(clientProps)
          .as(ClientProperty.AUTH_PRINCIPAL.getValue(clientProps), dt).build();
      client.close();
    }
    return clientProps;
  }

  public static Properties configDelegationToken(TestEnv env)
      throws AccumuloException, AccumuloSecurityException {
    Properties clientProps = env.getClientProps();
    if (Boolean.parseBoolean(ClientProperty.SASL_ENABLED.getValue(clientProps))) {
      AccumuloClient client = env.getAccumuloClient();
      clientProps = configDelegationToken(clientProps, client);
      client.close();
    }
    return clientProps;
  }

  public static Properties configDelegationToken(Properties clientProps, AccumuloClient client)
      throws AccumuloException, AccumuloSecurityException {
    if (Boolean.parseBoolean(ClientProperty.SASL_ENABLED.getValue(clientProps))) {
      DelegationToken dt = client.securityOperations().getDelegationToken(null);
      clientProps = Accumulo.newClientProperties().from(clientProps)
          .as(ClientProperty.AUTH_PRINCIPAL.getValue(clientProps), dt).build();
    }
    return clientProps;
  }
}
