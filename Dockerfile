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

ARG HADOOP_HOME
ARG HADOOP_USER_NAME
ENV HADOOP_HOME ${HADOOP_HOME}
ENV HADOOP_USER_NAME ${HADOOP_USER_NAME:-hadoop}

RUN yum install -y java-1.8.0-openjdk-devel
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk

ENV HADOOP_API_JAR /opt/at/hadoop-client-api.jar
ENV HADOOP_RUNTIME_JAR /opt/at/hadoop-client-runtime.jar
ENV TEST_JAR_PATH /opt/at/accumulo-testing-shaded.jar
ENV ACCUMULO_CLIENT_PROPS /opt/at/conf/accumulo-client.properties
ENV TEST_PROPS /opt/at/conf/accumulo-testing.properties
ENV TEST_LOG4J /opt/at/conf/log4j.properties

RUN mkdir /opt/at
RUN mkdir /opt/at/bin
RUN mkdir /opt/at/conf

COPY ./conf/accumulo-client.properties /opt/at/conf/
COPY ./conf/accumulo-testing.properties /opt/at/conf/
COPY ./conf/log4j.properties* /opt/at/conf/
RUN touch /opt/at/conf/env.sh

COPY ./bin/build /opt/at/bin
COPY ./bin/cingest /opt/at/bin
COPY ./bin/rwalk /opt/at/bin
COPY ./bin/gcs /opt/at/bin
COPY ./src/main/docker/docker-entry /opt/at/bin

COPY ./target/accumulo-testing-shaded.jar /opt/at/
COPY ./target/dependency/hadoop-client-api.jar /opt/at/
COPY ./target/dependency/hadoop-client-runtime.jar /opt/at/

ENTRYPOINT ["/opt/at/bin/docker-entry"]
CMD ["help"]
