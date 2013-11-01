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

CLASSPATH=${CLASSPATH}:${bin}/../hbase-tools-cdh4-0.1.0-SNAPSHOT-jar-with-dependencies.jar:${bin}/../conf
JAVA=${JAVA_HOME}/bin/java

JVM_OPTS=" -Dbase.dir=$bin/../ "

COMMAND=$1
shift

if [ "${COMMAND}" = "scan" ]; then
  CLASS="com.iflytek.hbase.ScanTool"
elif [ "${COMMAND}" = "get" ]; then
  CLASS="com.iflytek.hbase.GetTool"
elif [ "${COMMAND}" = "put" ]; then
  CLASS="com.iflytek.hbase.PutTool"
elif [ "${COMMAND}" = "sync_uid_list" ]; then
  CLASS="com.iflytek.hbase.SyncUidList"
elif [ "${COMMAND}" = "get_tmp" ]; then
  CLASS="com.iflytek.hbase.GetTmp"
elif [ "${COMMAND}" = "tv" ]; then
  CLASS="com.iflytek.hbase.GetTvWord"
elif [ "${COMMAND}" = "nlp" ]; then
  CLASS="com.iflytek.hbase.ExportNlp"
fi

${JAVA} -classpath ${CLASSPATH} ${JVM_OPTS} ${CLASS} $@ &

