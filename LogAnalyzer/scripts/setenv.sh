#!/bin/bash
#export JAVA_HOME=/opt/huawei/Bigdata/jdk
export JAVA_HOME=/opt/huawei/Bigdata/jdk
export PATH=$PATH:$JAVA_HOME/bin
export JAVA_OPTS="$JAVA_OPTS -Dlog.conf.path=${CATALINA_HOME}/logconf/"
export JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true -server"
export JAVA_OPTS="$JAVA_OPTS -XX:MaxPermSize=4G -Xss8m -XX:MaxHeapSize=10G -Xms12G -Xmx12G -DIgnoreReplayReqDetect -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled" 
export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:${CATALINA_HOME}/logconf/log4j.properties"
export JAVA_OPTS="$JAVA_OPTS -Dzookeeper.authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider"


export JAVA_OPTS="$JAVA_OPTS -Dapp.log=${CATALINA_HOME}/logs"
export JAVA_OPTS="$JAVA_OPTS -DappName=LogAnalyzer"
export JAVA_OPTS="$JAVA_OPTS -Dexport.file.path=${CATALINA_HOME}/webapps/LogAnalyzer/download"

export JAVA_OPTS="$JAVA_OPTS -Dexportfile.path=${CATALINA_HOME}/webapps/LogAnalyzer/download"
