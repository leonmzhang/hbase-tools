#!/usr/bin/env bash

bin=$(dirname "$0")
bin=$(cd "${bin}">/dev/null; pwd)

mvn clean install assembly:assembly
cp target/hbase-tools-cdh4-0.1.0-SNAPSHOT-jar-with-dependencies.jar ./
