#!/usr/bin/env bash

if [ $# = 0 ]; then
  echo "Usage: hbase-tools [--config confdir] COMMAND"
  echo "where command is one of:"
  echo "    scan    scan"
  echo "    get     get"
  echo "    put     put"
fi

CLASSPATH=${CLASSPATH}:
JAVA=${JAVA_HOME}/bin/java

COMMAND=$1
shift

if [ ${COMMAND} = "scan" ]; then
  CLASS="com.iflytek.hbase.ScanTool"
fi

${JAVA} -classpath ${CLASSPATH} ${JVM_OPTS} ${CLASS}

