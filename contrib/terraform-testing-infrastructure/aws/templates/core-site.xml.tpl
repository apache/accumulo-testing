<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${manager_ip}:8000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>${hadoop_dir}</value>
  </property>
</configuration>

