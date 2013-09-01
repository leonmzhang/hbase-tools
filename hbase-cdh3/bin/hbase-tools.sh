#!/usr/bin/env bash

bin=$(dirname $0)
bin=$(cd $bin>/dev/null; pwd)

if [ $# = 0 ]; then
  echo "Usage: hbase-tools [--config confdir] COMMAND"
  echo "where command is one of:"
  echo "    scan    scan"
  echo "    get     get"
  echo "    put     put"
  echo "    sync    sync"
  echo "    trans   trans"
  
  exit 1
fi

CLASSPATH=${CLASSPATH}:${bin}/../hbase-tools-cdh3-0.1.0-SNAPSHOT-jar-with-dependencies.jar:${bin}/../conf
JAVA=${JAVA_HOME}/bin/java

JVM_OPTS=" -Dbase.dir=${bin}/../ "

DEBUG_FLAG=$1
if [ ${DEBUG_FLAG} = "debug" ]; then
  JVM_OPTS="${JVM_OPTS} -Xdebug -Xrunjdwp:transport=dt_socket,suspend=y,server=y,address=3389 "
  shift
fi

COMMAND=$1
shift

if [ "${COMMAND}" = "scan" ]; then
  CLASS="com.iflytek.hbase.ScanTool"
elif [ "${COMMAND}" = "get" ]; then
  CLASS="com.iflytek.hbase.GetTool"
elif [ "${COMMAND}" = "sync" ]; then
  CLASS="com.iflytek.hbase.SyncTable"
elif [ "${COMMAND}" = "trans" ]; then
  CLASS="com.iflytek.hbase.TransTool"
elif [ "${COMMAND}" = "app" ]; then
  CLASS="com.iflytek.hbase.main.CheckAppData"
elif [ "${COMMAND}" = "hdfs" ]; then
  CLASS="com.iflytek.hbase.main.HdfsToHbase"
fi

${JAVA} -classpath ${CLASSPATH} ${JVM_OPTS} ${CLASS} $@

