#!/usr/bin/env bash
set -euo pipefail

/opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/project/spark/spark_streaming.py
