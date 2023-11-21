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

TRAVIS_DIR=`dirname $0`

if [ $# -ne 1 ]; then
    echo "Must pass base branch name of pull request"
    exit 1
fi

CLIENT_BRANCH=$1
HUGEGRAPH_BRANCH=$CLIENT_BRANCH

HUGEGRAPH_GIT_URL="https://github.com/hugegraph/hugegraph.git"

git clone $HUGEGRAPH_GIT_URL

cd hugegraph

git checkout $HUGEGRAPH_BRANCH

mvn package -DskipTests

mv hugegraph-*.tar.gz ../

cd ../

rm -rf hugegraph

tar -zxvf hugegraph-*.tar.gz

HTTPS_SERVER_DIR="hugegraph_https"

mkdir $HTTPS_SERVER_DIR

cp -r hugegraph-*/. $HTTPS_SERVER_DIR

cd hugegraph-*

cp ../$TRAVIS_DIR/conf/* conf

echo -e "pa" | bin/init-store.sh

bin/start-hugegraph.sh

cd ../

cd $HTTPS_SERVER_DIR

REST_SERVER_CONFIG="conf/rest-server.properties"

GREMLIN_SERVER_CONFIG="conf/gremlin-server.yaml"

sed -i "s?http://127.0.0.1:8080?https://127.0.0.1:8443?g" "$REST_SERVER_CONFIG"

sed -i "s/#port: 8182/port: 8282/g" "$GREMLIN_SERVER_CONFIG"

echo "ssl.keystore_password=hugegraph" >> $REST_SERVER_CONFIG

echo "ssl.keystore_file=conf/hugegraph-server.keystore" >> $REST_SERVER_CONFIG

echo "gremlinserver.url=http://127.0.0.1:8282" >> $REST_SERVER_CONFIG

bin/init-store.sh

bin/start-hugegraph.sh

cd ../