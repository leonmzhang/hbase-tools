#!/usr/bin/env bash

bin=$(dirname $0)
bin=$(cd $bin>/dev/null; pwd)

CLASSPATH=${CLASSPATH}:${bin}/../hbase-tools-cdh3-0.1.0-SNAPSHOT-jar-with-dependencies.jar:${bin}/../conf
JAVA=${JAVA_HOME}/bin/java

DEBUG_FLAG=$1
if [ ${DEBUG_FLAG} = "debug" ]; then
  JVM_OPTS=" -Xdebug -Xrunjdwp:transport=dt_socket,suspend=y,server=y,address=3389 "
  shift
fi

${JAVA} -classpath ${CLASSPATH} ${JVM_OPTS} ${CLASS} ${bin} $@ &
