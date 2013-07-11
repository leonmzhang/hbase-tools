#!/usr/bin/env bash

if [ $# = 0 ]; then
  echo "Usage: hbase-tools [--config confdir] COMMAND"
  echo "where command is one of:"
  echo "    scan    scan"
  echo "    get     get"
  echo "    put     put"
fi
