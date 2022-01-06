[Unit]
Description=Hadoop DataNode start/stop
After=remote-fs.target

[Service]
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk
Environment=HADOOP_HOME=${efs_mount}/hadoop/hadoop-${hadoop_version}
Environment=HADOOP_LOG_DIR=${hadoop_dir}/logs
User=hadoop
Group=hadoop
Type=oneshot
ExecStart=${efs_mount}/hadoop/hadoop-${hadoop_version}/bin/hdfs --daemon start datanode
ExecStop=${efs_mount}/hadoop/hadoop-${hadoop_version}/bin/hdfs --daemon stop datanode
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target

