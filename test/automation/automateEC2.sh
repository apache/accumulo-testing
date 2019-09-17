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


while true; do

	# pulls arguments from the text file
	ARGSFILE=$1
	I=0
	while IFS= read -r line; do
		ARG[$I]="$line";
		I=$((I+1));
	done <"$ARGSFILE"


	if [ $I != 7 ]; then
		echo 'Missing arguments in args.txt. Check README.md'
		break;
	fi

	ACCUMULO_REPO=${ARG[0]}
	ACCUMULO_BR=${ARG[1]}

	ACCUMULO_TESTING_REPO=${ARG[2]}
	ACCUMULO_TESTING_BR=${ARG[3]}

	MUCHOS_REPO=${ARG[4]}
	MUCHOS_BR=${ARG[5]}
	MUCHOS_CONFIG=${ARG[6]} # overwrite muchos.props in cloned repository



	# builds Accumulo tarball and installs fluo-muchos in a temporary directory
	TMPDIR=`mktemp -d`
	echo "Directory created: $TMPDIR " && cd $TMPDIR
	git clone --single-branch --branch $MUCHOS_BR $MUCHOS_REPO 
	cd fluo-muchos && cd ..
	git clone --single-branch --branch $ACCUMULO_BR $ACCUMULO_REPO && cd accumulo
	mvn clean package -DskipFormat -PskipQA

	# copies the tarball to the given muchos directory
	cp ./assemble/target/*.gz ~/fluo-muchos/conf/upload/
	if [ $? -eq 0 ]; then
		echo "Accumulo tarball copied to fluo-muchos"
	else
		break
	fi

	# sets up the cluster
	cd $TMPDIR/fluo-muchos || (echo "Could not find Fluo-Muchos" && break)
	cp conf/muchos.props.example conf/muchos.props
	cp $MUCHOS_CONFIG ./conf/muchos.props || (echo Could not use custom config. Check paths in args.txt)

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
	ssh $CLUSTERUSER@$PROXYIP "git clone --single-branch --branch $ACCUMULO_BR $ACCUMULO_REPO &"
	ssh $CLUSTERUSER@$PROXYIP "cd accumulo && $M2/mvn clean install -PskipQA && cd .. &"
	ssh $CLUSTERUSER@$PROXYIP "git clone --single-branch --branch $ACCUMULO_TESTING_BR $ACCUMULO_TESTING_REPO &"
	ssh $CLUSTERUSER@$PROXYIP "cd accumulo-testing && $M2/mvn clean package &"

	break
done
