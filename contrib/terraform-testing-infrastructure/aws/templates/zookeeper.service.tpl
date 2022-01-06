[Unit]
Description=ZooKeeper start/stop
After=remote-fs.target

[Service]
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk
Environment=ZOOKEEPER_HOME=${efs_mount}/zookeeper/apache-zookeeper-${zookeeper_version}-bin
Environment=ZOO_LOG_DIR=${zookeeper_dir}/logs
User=hadoop
Group=hadoop
Type=oneshot
ExecStart=${efs_mount}/zookeeper/apache-zookeeper-${zookeeper_version}-bin/bin/zkServer.sh start
ExecStop=${efs_mount}/zookeeper/apache-zookeeper-${zookeeper_version}-bin/bin/zkServer.sh start
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target

