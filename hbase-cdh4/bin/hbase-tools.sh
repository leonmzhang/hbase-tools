#!/usr/bin/env bash

bin=$(dirname $0)
bin=$(cd $bin>/dev/null; pwd)

if [ $# = 0 ]; then
  echo "Usage: hbase-tools [--config confdir] COMMAND"
  echo "where command is one of:"
  echo "    scan    scan"
  echo "    get     get"
  echo "    put     put"
  
  exit 1
fi

CLASSPATH=${CLASSPATH}:${bin}/../hbase-tools-cdh4-0.1.0-SNAPSHOT-jar-with-dependencies.jar
JAVA=${JAVA_HOME}/bin/java

COMMAND=$1
shift

if [ ${COMMAND} = "scan" ]; then
  CLASS="com.iflytek.hbase.ScanTool"
fi

${JAVA} -classpath ${CLASSPATH} ${JVM_OPTS} ${CLASS}

