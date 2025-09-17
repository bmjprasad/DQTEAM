# Databricks notebook source
# MAGIC %python

# COMMAND ----------
# DBTITLE 1,Parameters
try:
    dbutils.widgets.text("full_table_name", "")
    dbutils.widgets.text("dq_database", "dq_system")
    dbutils.widgets.text("base_location", "")
except NameError:
    pass

full_table_name = dbutils.widgets.get("full_table_name") if 'dbutils' in globals() else ""
if not full_table_name:
    raise ValueError("Parameter 'full_table_name' is required, e.g. 'catalog.schema.table' or 'schema.table'")

dq_database = dbutils.widgets.get("dq_database") if 'dbutils' in globals() else "dq_system"
base_location = dbutils.widgets.get("base_location") if 'dbutils' in globals() else ""
base_location = base_location or None

# COMMAND ----------
# DBTITLE 1,Imports
from dq.config import DQConfig
from dq.delta_tables import ensure_database, create_dq_tables
from dq.profiler import profile_table
from dq.rule_generator import generate_rules_from_profiles

# COMMAND ----------
# DBTITLE 1,Init
cfg = DQConfig(database_name=dq_database, base_location=base_location)
ensure_database(spark, cfg.database_name, cfg.base_location)
create_dq_tables(spark, cfg.database_name)

# COMMAND ----------
# DBTITLE 1,Profile and Generate Rules
profiles_df = profile_table(spark, full_table_name, config=cfg, save_to_table=True)
_ = generate_rules_from_profiles(spark, profiles_df, config=cfg)

print(f"Profiled {full_table_name} and generated rules into {cfg.database_name}.dq_rules")