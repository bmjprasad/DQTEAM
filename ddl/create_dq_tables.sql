-- Databricks SQL DDL to create DQ system tables
CREATE DATABASE IF NOT EXISTS dq_system;

CREATE TABLE IF NOT EXISTS dq_system.dq_profiles (
  run_id STRING,
  run_ts TIMESTAMP,
  catalog_name STRING,
  schema_name STRING,
  table_name STRING,
  column_name STRING,
  data_type STRING,
  row_count BIGINT,
  null_count BIGINT,
  null_ratio DOUBLE,
  distinct_count BIGINT,
  min_value STRING,
  max_value STRING,
  p01 STRING,
  p99 STRING,
  mean DOUBLE,
  stddev DOUBLE,
  min_length INT,
  max_length INT,
  profile_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS dq_system.dq_rules (
  rule_id STRING,
  created_ts TIMESTAMP,
  catalog_name STRING,
  schema_name STRING,
  table_name STRING,
  column_name STRING,
  rule_type STRING,
  rule_expression STRING,
  severity STRING,
  source STRING,
  profile_run_id STRING,
  metadata_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS dq_system.dq_anomalies (
  anomaly_id STRING,
  run_id STRING,
  run_ts TIMESTAMP,
  catalog_name STRING,
  schema_name STRING,
  table_name STRING,
  metric_name STRING,
  metric_value DOUBLE,
  score DOUBLE,
  is_anomaly BOOLEAN,
  model_version STRING,
  details_json STRING
) USING DELTA;