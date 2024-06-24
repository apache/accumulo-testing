# Timely-Grafana Stack

This Docker image is designed to communicate with a locally running Apache Accumulo
instance for Grafana dashboard development purposes. This Docker image contains
Timely and Grafana and is configured to connect to the host network which will make
all Timely and Grafana ports available. An agent, like Telegraf, can be used to
get metrics from the Accumulo processes in a StatsD format and send them to Timely
using the OpenTSDB output plugin.

## Deployment

1. Run `build-image.sh`, after this runs the timely jar files will be copied out
   of the Docker image and put into the `build_output` directory.
2. Copy the timely jar files from the `build_output` directory to `$ACCUMULO_HOME/lib`
3. Add the following to `accumulo.properties':
```
general.micrometer.enabled=true
general.micrometer.jvm.metrics.enabled=true
general.micrometer.factory=org.apache.accumulo.test.metrics.TestStatsDRegistryFactory
```
4. Add the following to the `JAVA_OPTS` variable in `accumulo-env.sh`:
```
  "-Dtest.meter.registry.host=localhost"
  "-Dtest.meter.registry.port=8125"
```
5. Start ZooKeeper, Hadoop, and Accumulo
6. Execute `run-container.sh` to start Timely and Grafana

