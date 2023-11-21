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

abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$(cd -P "$(dirname "$SOURCE")" && pwd)"
}

if [[ $# -lt 12 ]]; then
    echo "USAGE: $0 GREMLIN_SERVER_CONF REST_SERVER_CONF OPEN_SECURITY_CHECK"
    echo " e.g.: $0 conf/gremlin-server.yaml conf/rest-server.properties true"
    exit 1;
fi

BIN=$(abs_path)
TOP="$(cd $BIN/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
EXT="$TOP/ext"
PLUGINS="$TOP/plugins"
LOGS="$TOP/logs"
GC_LOGS="$TOP/gclogs"
OUTPUT=${LOGS}/hugegraph-server.log
## set jmx_export port for prometheus, 0 or unset represents not start jmx_export
JMX_EXPORT_PORT=0

export HUGEGRAPH_HOME="$TOP"
. ${BIN}/util.sh

GREMLIN_SERVER_CONF="$1"
REST_SERVER_CONF="$2"
GRAPH_SPACE="$3"
SERVICE_ID="$4"
META_SERVERS="$5"
CLUSTER="$6"
PD_PEERS="$7"
WITH_CA="${8}"
CA_FILE="${9}"
CLIENT_CA="${10}"
CLIENT_KEY="${11}"
OPEN_SECURITY_CHECK="${12}"

if [[ $# -eq 12 ]]; then
    USER_OPTION=""
    GC_OPTION=""
elif [[ $# -eq 13 ]]; then
    USER_OPTION="${13}"
    GC_OPTION=""
elif [[ $# -eq 14 ]]; then
    USER_OPTION="${13}"
    GC_OPTION="${14}"
fi

ensure_path_writable $LOGS
ensure_path_writable $PLUGINS

# The maximum and minimum heap memory that service can use
MAX_MEM=$((16 * 1024))
MIN_MEM=$((1 * 512))
EXPECT_JDK_VERSION=11

# Add the slf4j-log4j12 binding
CP=$(find -L $LIB -name 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')

# Add the jars in lib that start with "hugegraph"
CP="$CP":$(find -L $LIB -name 'hugegraph*.jar' | sort | tr '\n' ':')
# Add the remaining jars in lib.
CP="$CP":$(find -L $LIB -name '*.jar' \
                \! -name 'hugegraph*' \
                \! -name 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')
# Add the jars in ext (at any subdirectory depth)
CP="$CP":$(find -L $EXT -name '*.jar' | sort | tr '\n' ':')
# Add the jars in plugins (at any subdirectory depth)
CP="$CP":$(find -L $PLUGINS -name '*.jar' | sort | tr '\n' ':')

# (Cygwin only) Use ; classpath separator and reformat paths for Windows ("C:\foo")
[[ $(uname) = CYGWIN* ]] && CP="$(cygpath -p -w "$CP")"

export CLASSPATH="${CLASSPATH:-}:$CP"

# Change to $BIN's parent
cd ${TOP} || exit

# Find Java
# 若其他组件需要 JDK8 环境启动，则可以在此处设置环境变量
# Mac 的 JAVA_HOME 一般如下
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.17.jdk/Contents/Home
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

JAVA_VERSION=$($JAVA -version 2>&1 | awk 'NR==1{gsub(/"/,""); print $3}' \
              | awk -F'_' '{print $1}')
if [[ $? -ne 0 || $JAVA_VERSION < $EXPECT_JDK_VERSION ]]; then
    echo "Please make sure that the JDK is installed and the version >= $EXPECT_JDK_VERSION" \
         >> ${OUTPUT}
    exit 1
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphServer, requires at least ${MIN_MEM}m free memory" \
             >> ${OUTPUT}
        exit 1
    fi
    JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS} ${USER_OPTION}"

    # Rolling out detailed GC logs (gc+heap info )
    JAVA_OPTIONS="${JAVA_OPTIONS} -Xlog:gc+heap=info:file=${GC_LOGS}/gc-%t.log:level,tags,time,uptime,pid:filesize=10M,filecount=3"
fi

# Using G1GC as the default garbage collector (Recommended for large memory machines)
case "$GC_OPTION" in
    g1)
        echo "Using G1GC as the default garbage collector"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseG1GC -XX:+ParallelRefProcEnabled \
                      -XX:InitiatingHeapOccupancyPercent=50 -XX:G1RSetUpdatingPauseTimePercent=5"
        ;;
    "") ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', only support 'g1' now" >> ${OUTPUT}
        exit 1
esac

JVM_OPTIONS="-Dlog4j.configurationFile=${CONF}/log4j2.xml"
if [ "${JMX_EXPORT_PORT}" != "" ] && [ ${JMX_EXPORT_PORT} -ne 0 ] ; then
  JAVA_OPTIONS="${JAVA_OPTIONS} -javaagent:${LIB}/jmx_prometheus_javaagent-0.16.1.jar=${JMX_EXPORT_PORT}:${CONF}/jmx_exporter.yml"
fi

if [[ ${OPEN_SECURITY_CHECK} == "true" ]]; then
    JVM_OPTIONS="${JVM_OPTIONS} -Djava.security.manager=import org.apache.hugegraph.security.HugeSecurityManager"
fi
# add exports to use Reflection in HugeFactoryAuthProxy
JVM_OPTIONS="${JVM_OPTIONS} --add-exports java.base/jdk.internal.reflect=ALL-UNNAMED"

# Turn on security check
exec ${JAVA} -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} \
     -cp ${CLASSPATH} org.apache.hugegraph.dist.HugeGraphServer ${GREMLIN_SERVER_CONF} \
     ${REST_SERVER_CONF} ${GRAPH_SPACE} ${SERVICE_ID} ${META_SERVERS} ${CLUSTER} ${PD_PEERS} \
     ${WITH_CA} ${CA_FILE} ${CLIENT_CA} ${CLIENT_KEY} >> ${OUTPUT} 2>&1
