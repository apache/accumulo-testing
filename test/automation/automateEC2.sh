#! /usr/bin/env bash

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


# reads all git repositories and properties from cluster_props.sh
source cluster_props.sh
while true; do

	# check for null properties
	[ -z "$ACCUMULO_REPO" ] && echo "Check accumulo in cluster_props.sh" && break
	[ -z "$ACCUMULO_BRANCH" ] && echo "Check accumulo branch in cluster_props.sh" && break
	[ -z "$ACCUMULO_TESTING_REPO" ] && echo "Check accumulo-testing in cluster_props.sh" && break
	[ -z "$ACCUMULO_TESTING_BRANCH" ] && echo "Check accumulo-testing branch in cluster_props.sh" && break
	[ -z "$FLUO_MUCHOS_REPO" ] && echo "Check fluo-muchos in cluster_props.sh" && break
	[ -z "$FLUO_MUCHOS_BRANCH" ] && echo "Check fluo-muchos branch in cluster_props.sh" && break
	[[ -z "$MUCHOS_PROPS" || ! -f "$MUCHOS_PROPS" ]] && echo "Check muchos.props in cluster_props.sh" && break



	# builds Accumulo tarball and installs fluo-muchos in a temporary directory
	TMPDIR=`mktemp -d`
	echo "Directory created: $TMPDIR " && cd $TMPDIR
	git clone --single-branch --branch $FLUO_MUCHOS_BRANCH $FLUO_MUCHOS_REPO 
	git clone --single-branch --branch $ACCUMULO_BRANCH $ACCUMULO_REPO && cd accumulo
	mvn clean package -DskipFormat -PskipQA

	# copies the tarball to the given muchos directory
	cp ./assemble/target/*.gz $TMPDIR/fluo-muchos/conf/upload/
	if [ $? -eq 0 ]; then
		echo "Accumulo tarball copied to fluo-muchos"
	else
		break
	fi

	# sets up the cluster
	cd $TMPDIR/fluo-muchos || (echo "Could not find Fluo-Muchos" && break)
	cp conf/muchos.props.example conf/muchos.props
	cp $MUCHOS_PROPS ./conf/muchos.props || (echo Could not use custom config. Check path in cluster_props.sh)

	./bin/muchos launch -c "$USER-cluster" && echo "Setting up cluster.."
	# repeat setup until all nodes are intialized
	until ./bin/muchos setup; do
		echo "Script will resume once nodes are intialized."
		echo "Retrying in 20 seconds..."
		sleep 20
	done


	if [ $? -eq 0 ]; then
                echo "EC2 cluster setup as $USER-cluster"
        else
		echo "Terminating cluster"
		./bin/muchos terminate -c $USER-cluster
                break
        fi

	CLUSTERUSER=`./bin/muchos config -p cluster_user`
	PROXYIP=`./bin/muchos config -p proxy.public.ip`
	M2='/home/centos/install/apache-maven*/bin'
	
	# clones and builds accumulo and  accumulo-testing to EC2
	ssh $CLUSTERUSER@$PROXYIP "git clone --single-branch --branch $ACCUMULO_BRANCH $ACCUMULO_REPO"
	ssh $CLUSTERUSER@$PROXYIP "cd accumulo && $M2/mvn clean install -PskipQA && cd .."
	ssh $CLUSTERUSER@$PROXYIP "git clone --single-branch --branch $ACCUMULO_TESTING_BRANCH $ACCUMULO_TESTING_REPO"
	ssh $CLUSTERUSER@$PROXYIP "cd accumulo-testing && $M2/mvn clean package"

	break
done
