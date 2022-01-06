# This is the main configuration file for Apache Accumulo. Available configuration properties can be
# found at https://accumulo.apache.org/docs/2.x/configuration/server-properties

## Sets location in HDFS where Accumulo will store data
instance.volumes=hdfs://${manager_ip}:8000/accumulo

## Sets location of Zookeepers
instance.zookeeper.host=${manager_ip}:2181

## Change secret before initialization. All Accumulo servers must have same secret
instance.secret=DEFAULT

## Set to false if 'accumulo-util build-native' fails
tserver.memory.maps.native.enabled=true

# Run multiple compactors per node
compactor.port.search=true

# OpenTelemetry settings
general.opentelemetry.enabled=true

#Micrometer settings
general.micrometer.enabled=true
general.micrometer.jvm.metrics.enabled=true
general.micrometer.factory=org.apache.accumulo.test.metrics.TestStatsDRegistryFactory
