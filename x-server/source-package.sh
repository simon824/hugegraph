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

############### 注意 ##################
############### 注意 ##################
############### 注意 ##################
## 注意：请替换 PD 和 STORE 地址

# sudo yum install -y maven git
RELEASE_DIR="R.$(date "+%Y%m%d.%H%M%S")"
mkdir -p ${RELEASE_DIR}

cd ${RELEASE_DIR}
MD5_FILE=./md5.txt

# PACKAGE COMMON
echo "[dis] Start package common ......"
#git clone https://github.com/starhugegraph/hugegraph-common.git
git clone git@github.com:starhugegraph/hugegraph-common.git
cd hugegraph-common
git checkout -b gh-dis-release origin/gh-dis-release
git pull --rebase
COMMIT_ID=`git rev-parse HEAD`
mvn clean install -Dmaven.test.skip=true
MD5=`md5 ./target/hugegraph-common-1.8.10.jar | cut -d ' ' -f1`
M2_MD5=`md5 ~/.m2/repository/com/baidu/hugegraph/hugegraph-common/1.8.10/hugegraph-common-1.8.10.jar | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./target/hugegraph-common-1.8.10.jar | cut -d ' ' -f1`
cp ./target/hugegraph-common-1.8.10.jar ../
cp ./pom.xml ../common-pom.xml
cd ..
rm -rf hugegraph-common

echo "## hugegraph-common-1.8.10.jar" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt


echo "[dis] Start package hugegraph client ......"
#git clone https://github.com/starhugegraph/hugegraph-client.git
git clone git@github.com:starhugegraph/hugegraph-client.git
cd hugegraph-client
git checkout -b gh-dis-release origin/gh-dis-release
git pull --rebase
COMMIT_ID=`git rev-parse HEAD`
mvn clean install -Dmaven.test.skip=true
MD5=`md5 ./target/hugegraph-client-3.0.0.jar | cut -d ' ' -f1`
M2_MD5=`md5 ~/.m2/repository/com/baidu/hugegraph/hugegraph-client/3.0.0/hugegraph-client-3.0.0.jar | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./target/hugegraph-client-3.0.0.jar | cut -d ' ' -f1`
cp ./target/hugegraph-client-3.0.0.jar ../
cp ./pom.xml ../client-pom.xml
cd ..
rm -rf hugegraph-client

echo "## hugegraph-client-3.0.0.jar" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt

# PACKAGE HUGEGRAPH
echo "[dis] package hugegraph ......"
#git clone https://github.com/starhugegraph/hugegraph.git
git clone git@github.com:starhugegraph/hugegraph.git
cd hugegraph
git checkout -b gh-dis-release origin/gh-dis-release
git pull --rebase
TRAVIS_DIR=./hugegraph-dist/src/assembly/travis/lib
PLUGIN_DIR=./hugegraph-dist/src/assembly/plugin
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-client -Dversion=3.0.0 -Dpackaging=jar  -DpomFile=$TRAVIS_DIR/hg-pd-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-common-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-common -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-pd-common-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-grpc-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-grpc -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-pd-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-client -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-grpc-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-grpc -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-term-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-term -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-term-pom.xml
mvn install:install-file -Dfile=$PLUGIN_DIR/lib/hugegraph-plugin-1.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-plugin -Dversion=1.0.0 -Dpackaging=jar -DpomFile=$PLUGIN_DIR/pom.xml

mvn install:install-file -Dfile=$TRAVIS_DIR/hugegraph-computer-0.1.1.pom -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-computer -Dversion=0.1.1 -Dpackaging=pom
mvn install:install-file -Dfile=$TRAVIS_DIR/computer-driver-0.1.1.jar -DgroupId=com.baidu.hugegraph -DartifactId=computer-driver -Dversion=0.1.1 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/computer-driver-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/computer-k8s-0.1.1.jar -DgroupId=com.baidu.hugegraph -DartifactId=computer-k8s -Dversion=0.1.1 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/computer-k8s-pom.xml

COMMIT_ID=`git rev-parse HEAD`
mvn clean package -Dmaven.test.skip=true
MD5=`md5 ./hugegraph-3.0.0.tar.gz | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./hugegraph-3.0.0.tar.gz | cut -d ' ' -f1`
cp ./hugegraph-3.0.0.tar.gz ../
cd ..
rm -rf hugegraph

echo "## hugegraph-3.0.0.tar.gz" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt

# PACKAGE LOADER
#git clone https://github.com/starhugegraph/hugegraph-loader.git
git clone git@github.com:starhugegraph/hugegraph-loader.git
cd hugegraph-loader
git checkout -b gh-dis-release origin/gh-dis-release
git pull --rebase
STATIC_DIR=./assembly/static
mvn install:install-file -Dfile=$STATIC_DIR/lib/ojdbc8-12.2.0.1.jar -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar
COMMIT_ID=`git rev-parse HEAD`

# replace latest client
rm -rf ${STATIC_DIR}/lib/hugegraph-common-1.8.10.jar
rm -rf ${STATIC_DIR}/lib/hugegraph-client-3.0.0.jar
rm -rf ${STATIC_DIR}/lib/common-pom.xml
rm -rf ${STATIC_DIR}/lib/client-pom.xml
cp ../hugegraph-common-1.8.10.jar ${STATIC_DIR}/lib/hugegraph-common-1.8.10.jar
cp ../hugegraph-client-3.0.0.jar ${STATIC_DIR}/lib/hugegraph-client-3.0.0.jar
cp ../common-pom.xml ${STATIC_DIR}/lib/common-pom.xml
cp ../client-pom.xml ${STATIC_DIR}/lib/client-pom.xml
mvn clean install -Dmaven.test.skip=true
MD5=`md5 ./hugegraph-loader-3.0.0.tar.gz | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./hugegraph-loader-3.0.0.tar.gz | cut -d ' ' -f1`

MD5_JAR=`md5 ./target/hugegraph-loader-3.0.0.jar | cut -d ' ' -f1`
SHA256_JAR=`openssl dgst -sha256 ./target/hugegraph-loader-3.0.0.jar | cut -d ' ' -f1`
cp ./target/hugegraph-loader-3.0.0.jar ../
cp ./pom.xml ../loader-pom.xml
cp ./hugegraph-loader-3.0.0.tar.gz ../
cd ..
rm -rf hugegraph-loader

#echo "## hugegraph-loader-3.0.0.jar" >> ./md5.txt
#echo "commit: ${COMMIT_ID}" >> ./md5.txt
#echo "   md5: ${MD5_JAR}" >> ./md5.txt
#echo "sha256: ${SHA256_JAR}" >> ./md5.txt
#echo "" >> ./md5.txt

echo "## hugegraph-loader-3.0.0.tar.gz" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt


# PACKAGE HUGEGRAPH TOOLS
#git clone https://github.com/starhugegraph/hugegraph-tools.git
git clone git@github.com:starhugegraph/hugegraph-tools.git
cd hugegraph-tools
git checkout -b gh-dis-release origin/gh-dis-release
git pull --rebase
COMMIT_ID=`git rev-parse HEAD`
mvn clean package -Dmaven.test.skip=true
MD5=`md5 ./hugegraph-tools-3.0.0.tar.gz | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./hugegraph-tools-3.0.0.tar.gz | cut -d ' ' -f1`
cp ./hugegraph-tools-3.0.0.tar.gz ../
cd ..
rm -rf hugegraph-tools

echo "## hugegraph-tools-3.0.0.tar.gz" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt

#PACKAGE HUBBLE
#git clone https://github.com/starhugegraph/hugegraph-hubble.git
git clone git@github.com:starhugegraph/hugegraph-hubble.git
cd hugegraph-hubble
git checkout -b gh-dis-release origin/gh-dis-release
git pull --rebase
COMMIT_ID=`git rev-parse HEAD`
mvn clean package -Dmaven.test.skip=true
MD5=`md5 ./hugegraph-hubble-3.0.0.tar.gz | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./hugegraph-hubble-3.0.0.tar.gz | cut -d ' ' -f1`
cp ./hugegraph-hubble-3.0.0.tar.gz ../
cd ..
rm -rf hugegraph-hubble

echo "## hugegraph-hubble-3.0.0.tar.gz" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt

# PACKAGE PD
git clone ssh://zhangyi51@icode.baidu.com:8235/baidu/starhugegraph/hugegraph-pd baidu/starhugegraph/hugegraph-pd && curl -s http://icode.baidu.com/tools/hooks/commit-msg > baidu/starhugegraph/hugegraph-pd/.git/hooks/commit-msg && chmod u+x baidu/starhugegraph/hugegraph-pd/.git/hooks/commit-msg && git config -f baidu/starhugegraph/hugegraph-pd/.git/config user.name zhangyi51 && git config -f baidu/starhugegraph/hugegraph-pd/.git/config user.email zhangyi51@baidu.com
cd baidu/starhugegraph/hugegraph-pd
git checkout master
git pull --rebase
COMMIT_ID=`git rev-parse HEAD`
chmod +x ./local-release.sh
./local-release.sh
tar zcvf ./dist/hugegraph-pd-3.0.0.tar.gz ./dist/hugegraph-pd-3.0.0
MD5=`md5 ./dist/hugegraph-pd-3.0.0.tar.gz | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./dist/hugegraph-pd-3.0.0.tar.gz | cut -d ' ' -f1`
cp ./dist/hugegraph-pd-3.0.0.tar.gz ../../../
cd ../../../

rm -rf baidu/starhugegraph/hugegraph-pd
echo "## hugegraph-pd-3.0.0.tar.gz" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt

# PACKAGE STORE
git clone ssh://zhangyi51@icode.baidu.com:8235/baidu/starhugegraph/hugegraph-store baidu/starhugegraph/hugegraph-store && curl -s http://icode.baidu.com/tools/hooks/commit-msg > baidu/starhugegraph/hugegraph-store/.git/hooks/commit-msg && chmod u+x baidu/starhugegraph/hugegraph-store/.git/hooks/commit-msg && git config -f baidu/starhugegraph/hugegraph-store/.git/config user.name zhangyi51 && git config -f baidu/starhugegraph/hugegraph-store/.git/config user.email zhangyi51@baidu.com
cd baidu/starhugegraph/hugegraph-store
git checkout master
git pull --rebase
COMMIT_ID=`git rev-parse HEAD`
chmod +x ./local-release.sh
./local-release.sh
tar zcvf ./dist/hugegraph-store-3.0.0.tar.gz ./dist/hugegraph-store-3.0.0
MD5=`md5 ./dist/hugegraph-store-3.0.0.tar.gz | cut -d ' ' -f1`
SHA256=`openssl dgst -sha256 ./dist/hugegraph-store-3.0.0.tar.gz | cut -d ' ' -f1`
cp ./dist/hugegraph-store-3.0.0.tar.gz ../../../
cd ../../../

rm -rf baidu
echo "## hugegraph-store-3.0.0.tar.gz" >> ./md5.txt
echo "commit: ${COMMIT_ID}" >> ./md5.txt
echo "   md5: ${MD5}" >> ./md5.txt
echo "sha256: ${SHA256}" >> ./md5.txt
echo "" >> ./md5.txt

## sha256
openssl dgst -sha256 hugegraph-client-3.0.0.jar hugegraph-3.0.0.tar.gz \
	hugegraph-loader-3.0.0.tar.gz hugegraph-tools-3.0.0.tar.gz \
	hugegraph-hubble-3.0.0.tar.gz hugegraph-pd-3.0.0.tar.gz \
	hugegraph-store-3.0.0.tar.gz > tgz.sha256

cd ..
echo "OUTPUT: ${RELEASE_DIR}"
tar czvf ${RELEASE_DIR}.tar.gz ${RELEASE_DIR}
rm -rf ${RELEASE_DIR}

