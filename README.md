# Data Quality (DQ) Framework for Azure Databricks (PySpark)

This workspace contains a lightweight DQ framework that:
- Profiles tables and stores per-column metrics in `dq_system.dq_profiles`.
- Generates DQ rules automatically into `dq_system.dq_rules` based on the latest profile.
- Runs anomaly detection separately and stores results in `dq_system.dq_anomalies`.

## Components
- `dq/` Python package with profiling, rule generation, anomaly detection.
- `ddl/create_dq_tables.sql` DDL for Delta tables.
- `notebooks/run_rule_generation.py` Notebook: profile a table and generate rules.
- `notebooks/run_anomaly_detection.py` Notebook: run anomaly detection across recent runs.

## Quick start (Databricks)
1. Import the repo as a Databricks Repo, or copy files into Workspace.
2. Open `ddl/create_dq_tables.sql` in a SQL notebook or run via `%sql` to create the Delta tables (optional, notebooks create them if missing).
3. Open `notebooks/run_rule_generation.py` and set widget `full_table_name` to your target (e.g. `catalog.schema.table` or `schema.table`). Run the notebook to:
   - Ensure database and tables.
   - Compute and save profiles.
   - Generate rules.
4. Open `notebooks/run_anomaly_detection.py` and run it to train/score anomalies. It writes to `dq_system.dq_anomalies`.

## Jobs
Create two Databricks Jobs for separation of concerns:
- Job 1: Rule generation (runs `notebooks/run_rule_generation.py` per table on a schedule or triggered on data arrival).
- Job 2: Anomaly detection (runs `notebooks/run_anomaly_detection.py` daily/hourly).

## Configuration
`dq/config.py` exposes thresholds (null ratio, uniqueness ratio, quantiles) and anomaly settings. You can override by passing widget values to the notebooks or editing defaults.