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
COPY settings.xml /tmp/settings.xml
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm && \
    dnf install -y java-11-openjdk-devel git wget collectd collectd-java && \
    cd /opt && \
    wget https://dlcdn.apache.org/maven/maven-3/3.9.7/binaries/apache-maven-3.9.7-bin.tar.gz && \
    tar zxf apache-maven-3.9.7-bin.tar.gz && \
    cd /opt && \
    git clone https://github.com/NationalSecurityAgency/datawave-utils.git datawave-utils && \
    cd datawave-utils/code-style && \
    mv pom.xml pom.xml.bak && \
    sed "s/1-SNAPSHOT/0/" pom.xml.bak > pom.xml && \
    /opt/apache-maven-3.9.7/bin/mvn -s /tmp/settings.xml clean install && \
    cd /opt && \
    git clone https://github.com/NationalSecurityAgency/datawave-in-memory-accumulo.git datawave-in-memory-accumulo && \
    cd datawave-in-memory-accumulo && \
    git checkout accumulo4 && \
    /opt/apache-maven-3.9.7/bin/mvn -s /tmp/settings.xml clean install && \
    cd /opt && \
    git clone https://github.com/NationalSecurityAgency/timely.git timely && \
    cd timely && \
    git checkout accumulo4 && \
    /opt/apache-maven-3.9.7/bin/mvn -s /tmp/settings.xml clean package -Pcollectd -DskipTests && \
    dnf clean all && \
    rm -rf /var/cache/dnf

FROM redhat/ubi8
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk TIMELY_VERSION="3.0.0-SNAPSHOT"
COPY --from=builder /opt/timely/collectd-timely-plugin/target/collectd-timely-plugin-${TIMELY_VERSION}.jar /opt/timely/collectd-timely-plugin.jar
COPY --from=builder /opt/timely/server/target/timely-server-${TIMELY_VERSION}-dist.tar.gz /tmp/timely-server-dist.tar.gz
COPY --from=builder /opt/timely/grafana/target/timely-app-${TIMELY_VERSION}-dist.tar.gz /var/lib/grafana/plugins/timely-app-dist.tar.gz
RUN dnf install -y java-11-openjdk-devel && \
    dnf install -y https://dl.grafana.com/enterprise/release/grafana-enterprise-8.2.7-1.x86_64.rpm && \
    cd /var/lib/grafana/plugins/ && \
    tar zxf timely-app-dist.tar.gz && \
    rm timely-app-dist.tar.gz && \
    mkdir -p /opt/timely && \
    cd /opt/timely && \
    tar zxf /tmp/timely-server-dist.tar.gz --strip-components=1 && \
    rm /tmp/timely-server-dist.tar.gz && \
    dnf clean all && \
    rm -rf /var/cache/dnf
COPY entrypoint.sh /opt/entrypoint.sh
EXPOSE 3000/tcp 4242/tcp 4243/tcp 4244/tcp 4245/udp
ENTRYPOINT ["/opt/entrypoint.sh"]
