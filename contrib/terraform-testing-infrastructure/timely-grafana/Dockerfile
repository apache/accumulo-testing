#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
FROM redhat/ubi8 as builder
COPY build_output/maven /opt/apache-maven
COPY build_output/repo /opt/apache-maven/repository
COPY settings.xml /tmp/settings.xml
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV TIMELY_VERSION="3.0.0-SNAPSHOT"
ARG HADOOP_VERSION="3.3.6"

RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm && \
    dnf install -y java-17-openjdk-devel git wget collectd collectd-java && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN cd /opt && \
    git clone https://github.com/NationalSecurityAgency/datawave-utils.git datawave-utils && \
    cd datawave-utils/code-style && \
    mv pom.xml pom.xml.bak && \
    sed "s/1-SNAPSHOT/0/" pom.xml.bak > pom.xml && \
    /opt/apache-maven/bin/mvn -s /tmp/settings.xml clean install

RUN cd /opt && \
    git clone https://github.com/NationalSecurityAgency/datawave-in-memory-accumulo.git datawave-in-memory-accumulo && \
    cd datawave-in-memory-accumulo && \
    git checkout accumulo4 && \
    /opt/apache-maven/bin/mvn -s /tmp/settings.xml -Dversion.hadoop=${HADOOP_VERSION} clean install

RUN cd /opt && \
    git clone https://github.com/NationalSecurityAgency/timely.git timely && \
    cd timely && \
    git checkout accumulo4 && \
    /opt/apache-maven/bin/mvn -s /tmp/settings.xml -Dversion.hadoop=${HADOOP_VERSION} clean package -Pcollectd -DskipTests

RUN mkdir /tmp/timely-server && \
    tar zxf /opt/timely/server/target/timely-server-${TIMELY_VERSION}-dist.tar.gz --strip-components=1 -C /tmp/timely-server && \
    mkdir /tmp/timely-grafana && \
    tar zxf /opt/timely/grafana/target/timely-app-${TIMELY_VERSION}-dist.tar.gz -C /tmp/timely-grafana

FROM redhat/ubi8
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk TIMELY_VERSION="3.0.0-SNAPSHOT"

COPY --from=builder /opt/timely/collectd-timely-plugin/target/collectd-timely-plugin-${TIMELY_VERSION}.jar /opt/timely/collectd-timely-plugin.jar
COPY --from=builder /tmp/timely-server /opt/timely
COPY --from=builder /tmp/timely-grafana /var/lib/grafana/plugins/timely-app

RUN dnf install -y java-17-openjdk-devel && \
    dnf install -y https://dl.grafana.com/enterprise/release/grafana-enterprise-8.2.7-1.x86_64.rpm && \
    dnf clean all && \
    rm -rf /var/cache/dnf

COPY entrypoint.sh /opt/entrypoint.sh
EXPOSE 3000/tcp 4242/tcp 4243/tcp 4244/tcp 4245/udp
ENTRYPOINT ["/opt/entrypoint.sh"]
