#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

set -ev

HOME_DIR=`pwd`
TRAVIS_DIR=`dirname $0`
BASE_DIR=$1
BACKEND=$2
BIN=$BASE_DIR/bin
CONF=$BASE_DIR/conf/graphs/hugegraph.properties
REST_CONF=$BASE_DIR/conf/rest-server.properties
GREMLIN_CONF=$BASE_DIR/conf/gremlin-server.yaml

declare -A backend_serializer_map=(["memory"]="text" ["rocksdb"]="binary")

SERIALIZER=${backend_serializer_map[$BACKEND]}

# Set backend and serializer
sed -i "s/backend=.*/backend=$BACKEND/" $CONF
sed -i "s/serializer=.*/serializer=$SERIALIZER/" $CONF

# Append schema.sync_deletion=true to config file
echo "schema.sync_deletion=true" >> $CONF

echo "k8s.api=false" >> $REST_CONF
echo "server.use_k8s=false" >> $REST_CONF
echo "graph.load_from_local_config=true" >> $REST_CONF

$BIN/init-store.sh

AGENT_JAR=${HOME_DIR}/${TRAVIS_DIR}/jacocoagent.jar
$BIN/start-hugegraph.sh -j "-javaagent:${AGENT_JAR}=includes=*,port=36320,destfile=jacoco-it.exec,output=tcpserver" -v
