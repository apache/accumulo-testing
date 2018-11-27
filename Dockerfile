# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM centos:7

RUN yum install -y java-1.8.0-openjdk-devel
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk

ARG TEST_JAR_VERSION
ENV TEST_JAR_VERSION 2.0.0-SNAPSHOT
ENV TEST_JAR_PATH /opt/accumulo-testing-${TEST_JAR_VERSION}-shaded.jar
ENV ACCUMULO_CLIENT_PROPS /opt/conf/accumulo-client.properties
ENV TEST_PROPS /opt/conf/accumulo-testing.properties
ENV TEST_LOG4J /opt/conf/log4j.properties.example

RUN mkdir /opt/bin
RUN mkdir /opt/conf
RUN touch /opt/conf/env.sh

ADD ./conf/accumulo-client.properties /opt/conf/
ADD ./conf/accumulo-testing.properties /opt/conf/
ADD ./conf/log4j.properties.example /opt/conf/
ADD ./bin/cingest /opt/bin
ADD ./bin/rwalk /opt/bin
ADD ./src/main/docker/docker-entry /opt/bin
ADD ./target/accumulo-testing-${TEST_JAR_VERSION}-shaded.jar /opt/

ENTRYPOINT ["/opt/bin/docker-entry"]
CMD ["help"]
