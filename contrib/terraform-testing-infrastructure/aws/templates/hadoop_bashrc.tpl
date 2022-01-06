# .bashrc
if [ -f /etc/bashrc ]; then
  source /etc/bashrc
fi
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export ZOOKEEPER_HOME=${efs_mount}/zookeeper/apache-zookeeper-${zookeeper_version}-bin
export HADOOP_HOME=${efs_mount}/hadoop/hadoop-${hadoop_version}
export ACCUMULO_HOME=${efs_mount}/accumulo/accumulo-${accumulo_version}
export ACCUMULO_LOG_DIR=${accumulo_dir}/logs
export M2_HOME=${efs_mount}/apache-maven/apache-maven-${maven_version}

export ACCUMULO_JAVA_OPTS="-javaagent:${efs_mount}/accumulo/accumulo-${accumulo_version}/lib/opentelemetry-javaagent-1.7.1.jar -Dotel.traces.exporter=jaeger -Dotel.exporter.jaeger.endpoint=http://${manager_ip}:14250 -Dtest.meter.registry.host=${manager_ip} -Dtest.meter.registry.port=8125"
