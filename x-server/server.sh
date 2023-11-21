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

function rename()
{
    cfilelist=$(find  -maxdepth 1 -type d -printf '%f\n' )
    for cfilename in $cfilelist
    do
        if [[ $cfilename =~ SNAPSHOT ]]
        then
           mv $cfilename ${cfilename/-?.?.?-SNAPSHOT/}
        fi
    done
}

wget -q -O output.tar.gz  $AGILE_PRODUCT_HTTP_URL
tar -zxf output.tar.gz
cd output
rm -rf hugegraph-pd
rm -rf hugegraph-store
rm -rf hugegraph
find . -name "*.tar.gz" -exec tar -zxf {} \;
rename

# start store
pushd hugegraph-store
print log
mkdir -p logs
touch logs/hugegraph-store.log
tail -f logs/hugegraph-store.log | awk '{print "------store: " $0}' &

sed -i 's#local os=`uname`#local os=Linux#g' bin/util.sh
# sed -i 's/export LD_PRELOAD/#export LD_PRELOAD/' bin/start-hugegraph-store.sh
sed -i 's/# Find Java/export JAVA_HOME=$ORACLEJDK_11_0_7_HOME/' bin/start-hugegraph-store.sh
bin/start-hugegraph-store.sh
popd
jps
sleep 10

# start pd
pushd hugegraph-pd
print log
mkdir -p logs
touch logs/hugegraph-pd.log
tail -f logs/hugegraph-pd.log | awk '{print "------pd: " $0}' &

sed -i 's/initial-store-list:.*/initial-store-list: 127.0.0.1:8500\n  initial-store-count: 1/' conf/application.yml
sed -i 's/,127.0.0.1:8611,127.0.0.1:8612//' conf/application.yml
sed -i 's/# Find Java/export JAVA_HOME=$ORACLEJDK_11_0_7_HOME/' bin/start-hugegraph-pd.sh
bin/start-hugegraph-pd.sh
popd
jps
sleep 30

# start server
pushd hugegraph
# print server log
mkdir -p logs
touch logs/hugegraph-server.log
tail -f logs/hugegraph-server.log | awk '{print "==server==: " $0}' &

sed -i 's#META_SERVERS="http://127.0.0.1:2379"#META_SERVERS="127.0.0.1:8686"#' bin/start-hugegraph.sh
sed -i 's/# Find Java/export JAVA_HOME=$ORACLEJDK_11_0_7_HOME/' bin/hugegraph-server.sh

bin/start-hugegraph.sh

jps
sleep 30
# create graph
curl --location --request POST '127.0.0.1:8080/graphspaces/DEFAULT/graphs/hugegraph' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
  "backend": "hstore",
  "serializer": "binary",
  "store": "hugegraph",
  "vertex.cache_capacity": "0",
  "edge.cache_capacity": "0"
}'

sleep 5
curl --location --request POST '127.0.0.1:8080/graphspaces/DEFAULT/graphs/hugegraphapi' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
  "backend": "hstore",
  "serializer": "binary",
  "store": "hugegraphapi",
  "vertex.cache_capacity": "0",
  "edge.cache_capacity": "0"
}'
sleep 5

# GremlinApiTest will init, clear and truncate backend independently
curl --location --request POST '127.0.0.1:8080/graphspaces/DEFAULT/graphs/gremlin' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
  "backend": "hstore",
  "serializer": "binary",
  "store": "gremlin",
  "vertex.cache_capacity": "0",
  "edge.cache_capacity": "0"
}'
sleep 5
bin/stop-hugegraph.sh
popd
ls -l
cp -r hugegraph/* ../hugegraph-test/
ls ../hugegraph-test/

