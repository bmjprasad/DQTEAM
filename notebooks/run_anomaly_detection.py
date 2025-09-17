# Databricks notebook source
# MAGIC %python

# COMMAND ----------
# DBTITLE 1,Parameters
try:
    dbutils.widgets.text("dq_database", "dq_system")
    dbutils.widgets.text("lookback_days", "30")
    dbutils.widgets.text("contamination", "0.05")
    dbutils.widgets.text("model_version", "v1")
except NameError:
    pass

dq_database = dbutils.widgets.get("dq_database") if 'dbutils' in globals() else "dq_system"
lookback_days = int(dbutils.widgets.get("lookback_days")) if 'dbutils' in globals() else 30
contamination = float(dbutils.widgets.get("contamination")) if 'dbutils' in globals() else 0.05
model_version = dbutils.widgets.get("model_version") if 'dbutils' in globals() else "v1"

# COMMAND ----------
# DBTITLE 1,Imports
from dq.config import DQConfig
from dq.delta_tables import ensure_database, create_dq_tables
from dq.anomaly_detector import detect_anomalies

# COMMAND ----------
# DBTITLE 1,Init
cfg = DQConfig(database_name=dq_database)
ensure_database(spark, cfg.database_name, cfg.base_location)
create_dq_tables(spark, cfg.database_name)

# COMMAND ----------
# DBTITLE 1,Run Anomaly Detection
anomalies_df = detect_anomalies(
    spark,
    config=cfg,
    lookback_days=lookback_days,
    contamination=contamination,
    model_version=model_version,
)

print(f"Anomaly detection completed. Wrote {anomalies_df.count()} records to {cfg.database_name}.dq_anomalies")