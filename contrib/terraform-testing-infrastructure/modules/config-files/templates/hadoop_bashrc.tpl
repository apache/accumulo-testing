# .bashrc
if [ -f /etc/bashrc ]; then
  source /etc/bashrc
fi
export JAVA_HOME=${java_home}
export ZOOKEEPER_HOME=${software_root}/zookeeper/apache-zookeeper-${zookeeper_version}-bin
export HADOOP_HOME=${software_root}/hadoop/hadoop-${hadoop_version}
export ACCUMULO_HOME=${software_root}/accumulo/accumulo-${accumulo_version}
export ACCUMULO_LOG_DIR=${accumulo_dir}/logs
export M2_HOME=${software_root}/apache-maven/apache-maven-${maven_version}

export ACCUMULO_JAVA_OPTS="-javaagent:${software_root}/accumulo/accumulo-${accumulo_version}/lib/opentelemetry-javaagent-1.7.1.jar -Dotel.traces.exporter=jaeger -Dotel.exporter.jaeger.endpoint=http://${manager_ip}:14250 -Dtest.meter.registry.host=${manager_ip} -Dtest.meter.registry.port=8125"
