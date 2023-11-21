FROM centos:7

LABEL maintainer="HugeGraph Docker Maintainers"

# 1. Install needed dependencies of GraphServer & RocksDB
# RUN set -x \
#    && apt-get -q update \
#    && apt-get -q install -y --no-install-recommends --no-install-suggests \
#       curl \
#       lsof \
#       g++ \
#       gcc \
#    && apt-get clean

# 2. Init HugeGraph Sever
ADD ./hugegraph-3.6.4-SNAPSHOT.tar.gz /root/hugegraph-server

ADD ./jdk-11.0.17_linux-x64_bin.tar.gz /root/jdk
ENV JAVA_HOME=/root/jdk/jdk-11.0.17
ENV JRE_HOME=${JAVA_HOME}/jre
ENV CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
ENV PATH=${JAVA_HOME}/bin:$PATH

RUN cd /root/hugegraph-server/hugegraph-3.6.4-SNAPSHOT

EXPOSE 8080
WORKDIR /root
VOLUME /root

ENTRYPOINT ["sh","-c","/root/hugegraph-server/hugegraph-3.6.4-SNAPSHOT/bin/k8s-deployment-hugegraph.sh && tail -f /root/hugegraph-server/hugegraph-3.6.4-SNAPSHOT/logs/hugegraph-server.log" ]
