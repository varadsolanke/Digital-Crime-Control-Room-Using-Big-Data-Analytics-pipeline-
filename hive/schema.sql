CREATE DATABASE IF NOT EXISTS cyber_security;
USE cyber_security;

CREATE EXTERNAL TABLE IF NOT EXISTS cyber_logs (
    user_id STRING,
    activity_type STRING,
    timestamp STRING,
    ip_address STRING,
    status STRING,
    event_ts TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/cyber_logs';

MSCK REPAIR TABLE cyber_logs;
