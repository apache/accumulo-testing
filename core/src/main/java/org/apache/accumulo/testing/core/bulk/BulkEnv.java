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
package org.apache.accumulo.testing.core.bulk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.core.TestEnv;
import org.apache.accumulo.testing.core.TestProps;

class BulkEnv extends TestEnv {

    private List<Authorizations> authList;

    BulkEnv(Properties props) {
        super(props);
    }

    /**
     * @return Accumulo authorizations list
     */
    private List<Authorizations> getAuthList() {
        if (authList == null) {
            String authValue = p.getProperty(TestProps.BL_COMMON_AUTHS);
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

    long getRowMin() {
        return Long.parseLong(p.getProperty(TestProps.BL_INGEST_ROW_MIN));
    }

    long getRowMax() {
        return Long.parseLong(p.getProperty(TestProps.BL_INGEST_ROW_MAX));
    }

    int getMaxColF() {
        return Integer.parseInt(p.getProperty(TestProps.BL_INGEST_MAX_CF));
    }

    int getMaxColQ() {
        return Integer.parseInt(p.getProperty(TestProps.BL_INGEST_MAX_CQ));
    }

    String getAccumuloTableName() {
        return p.getProperty(TestProps.BL_COMMON_ACCUMULO_TABLE);
    }
}
