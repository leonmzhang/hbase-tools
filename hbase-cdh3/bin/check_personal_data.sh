#!/usr/bin/env bash

bin=$(dirname $0)
bin=$(cd $bin>/dev/null; pwd)

JAVA=${JAVA_HOME}/bin/java

SUSPEND=y

CLASSPATH=${CLASSPATH}:${bin}/../hbase-tools-cdh3-0.1.0-SNAPSHOT-jar-with-dependencies.jar:${bin}/../conf

JVM_OPTS=" -Dbase.dir=${bin}/../"

JVM_OPTS="${JVM_OPTS} -Xdebug -Xrunjdwp:transport=dt_socket,suspend=${SUSPEND},server=y,address=3389 "

CLASS=com.iflytek.hbase.tools.CheckPersonalData

${JAVA} -classpath ${CLASSPATH} ${JVM_OPTS} ${CLASS} $@
