#!/usr/bin/env bash
set -euo pipefail

hdfs dfs -mkdir -p /cyber_logs
hdfs dfs -mkdir -p /cyber_logs_checkpoint
hdfs dfs -ls /

echo "HDFS directories initialized."
