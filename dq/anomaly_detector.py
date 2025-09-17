from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import json
import uuid
from datetime import timedelta

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import DQConfig


FEATURE_COLUMNS = [
    "row_count",
    "avg_null_ratio",
    "max_null_ratio",
    "avg_distinct_ratio",
    "num_columns",
]


def _build_feature_frame(spark, cfg: DQConfig, lookback_days: int) -> DataFrame:
    profiles = spark.table(f"{cfg.database_name}.dq_profiles")

    lower_ts = F.current_timestamp() - F.expr(f"INTERVAL {int(lookback_days)} DAYS")
    filtered = profiles.where(F.col("run_ts") >= lower_ts)

    agg = (
        filtered.groupBy("run_id", "catalog_name", "schema_name", "table_name")
        .agg(
            F.max("run_ts").alias("run_ts"),
            F.max("row_count").alias("row_count"),
            F.avg("null_ratio").alias("avg_null_ratio"),
            F.max("null_ratio").alias("max_null_ratio"),
            F.avg(
                F.when(F.col("row_count") > 0, F.col("distinct_count") / F.col("row_count")).otherwise(F.lit(0.0))
            ).alias("avg_distinct_ratio"),
            F.countDistinct("column_name").alias("num_columns"),
        )
    )

    return agg


def detect_anomalies(
    spark,
    config: Optional[DQConfig] = None,
    lookback_days: int = 30,
    contamination: Optional[float] = None,
    model_version: str = "v1",
) -> DataFrame:
    """Train an IsolationForest on recent table profile features and flag anomalies on the latest run per table.

    Writes results to cfg.database_name.dq_anomalies.
    """
    from sklearn.ensemble import IsolationForest  # imported here to avoid cluster import issues at module load

    cfg = config or DQConfig()
    feat_df = _build_feature_frame(spark, cfg, lookback_days)

    # Identify the latest run per table
    w = F.window("run_ts", "365 days")  # dummy window to allow max by grouping keys
    latest_per_table = (
        feat_df
        .withColumn("rn", F.row_number().over(
            F.Window.partitionBy("catalog_name", "schema_name", "table_name").orderBy(F.col("run_ts").desc())
        ))
    )

    # Collect feature history to pandas for model training
    history_pdf = feat_df.select(
        "catalog_name",
        "schema_name",
        "table_name",
        "run_id",
        "run_ts",
        *FEATURE_COLUMNS,
    ).fillna(0).toPandas()

    if history_pdf.shape[0] < max(5, cfg.min_history_runs_for_ml):
        # Not enough history; simple z-score on row_count as fallback
        grouped = history_pdf.groupby(["catalog_name", "schema_name", "table_name"], as_index=False)
        summary = grouped["row_count"].agg([np.mean, np.std]).reset_index()
        latest_pdf = (
            history_pdf.sort_values(["catalog_name", "schema_name", "table_name", "run_ts"]).groupby(["catalog_name", "schema_name", "table_name"], as_index=False).tail(1)
        )
        latest_with_stats = latest_pdf.merge(
            summary,
            on=["catalog_name", "schema_name", "table_name"],
            how="left",
        )
        latest_with_stats["z"] = (latest_with_stats["row_count"] - latest_with_stats["mean"]) / latest_with_stats["std"].replace(0, np.nan)
        latest_with_stats["z"].fillna(0.0, inplace=True)
        latest_with_stats["is_anomaly"] = latest_with_stats["z"].abs() > 3.0
        latest_with_stats["score"] = latest_with_stats["z"].abs()

        anomalies_rows = []
        for _, r in latest_with_stats.iterrows():
            anomalies_rows.append({
                "anomaly_id": str(uuid.uuid4()),
                "run_id": r["run_id"],
                "run_ts": r["run_ts"],
                "catalog_name": r.get("catalog_name"),
                "schema_name": r["schema_name"],
                "table_name": r["table_name"],
                "metric_name": "composite",
                "metric_value": float(r["row_count"]),
                "score": float(r["score"]),
                "is_anomaly": bool(r["is_anomaly"]),
                "model_version": model_version + "-zscore",
                "details_json": json.dumps({c: (None if np.isnan(r[c]) else float(r[c])) if isinstance(r[c], (int, float, np.floating)) else r[c] for c in FEATURE_COLUMNS}),
            })

        result_schema = T.StructType([
            T.StructField("anomaly_id", T.StringType(), False),
            T.StructField("run_id", T.StringType(), False),
            T.StructField("run_ts", T.TimestampType(), False),
            T.StructField("catalog_name", T.StringType(), True),
            T.StructField("schema_name", T.StringType(), False),
            T.StructField("table_name", T.StringType(), False),
            T.StructField("metric_name", T.StringType(), False),
            T.StructField("metric_value", T.DoubleType(), False),
            T.StructField("score", T.DoubleType(), False),
            T.StructField("is_anomaly", T.BooleanType(), False),
            T.StructField("model_version", T.StringType(), False),
            T.StructField("details_json", T.StringType(), True),
        ])
        anomalies_df = spark.createDataFrame(anomalies_rows, schema=result_schema)
        anomalies_df.write.mode("append").format("delta").saveAsTable(f"{cfg.database_name}.dq_anomalies")
        return anomalies_df

    # Train IsolationForest on history
    X = history_pdf[FEATURE_COLUMNS].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).values
    iso = IsolationForest(
        contamination=(contamination if contamination is not None else cfg.isolation_forest_contamination),
        random_state=42,
        n_estimators=200,
        max_samples="auto",
        n_jobs=-1,
    )
    iso.fit(X)

    # Score only the latest per table
    latest_pdf = (
        history_pdf.sort_values(["catalog_name", "schema_name", "table_name", "run_ts"]).groupby(["catalog_name", "schema_name", "table_name"], as_index=False).tail(1)
    )
    X_latest = latest_pdf[FEATURE_COLUMNS].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).values
    preds = iso.predict(X_latest)  # -1 anomaly, 1 normal
    scores = -iso.score_samples(X_latest)  # higher => more anomalous

    anomalies_rows = []
    for i, r in latest_pdf.reset_index(drop=True).iterrows():
        is_anom = preds[i] == -1
        score = float(scores[i])
        details = {c: float(r[c]) if not isinstance(r[c], (str,)) else r[c] for c in FEATURE_COLUMNS}
        anomalies_rows.append({
            "anomaly_id": str(uuid.uuid4()),
            "run_id": r["run_id"],
            "run_ts": r["run_ts"],
            "catalog_name": r.get("catalog_name"),
            "schema_name": r["schema_name"],
            "table_name": r["table_name"],
            "metric_name": "composite",
            "metric_value": float(r["row_count"]),
            "score": score,
            "is_anomaly": bool(is_anom),
            "model_version": model_version,
            "details_json": json.dumps(details),
        })

    result_schema = T.StructType([
        T.StructField("anomaly_id", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("run_ts", T.TimestampType(), False),
        T.StructField("catalog_name", T.StringType(), True),
        T.StructField("schema_name", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("metric_name", T.StringType(), False),
        T.StructField("metric_value", T.DoubleType(), False),
        T.StructField("score", T.DoubleType(), False),
        T.StructField("is_anomaly", T.BooleanType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("details_json", T.StringType(), True),
    ])
    anomalies_df = spark.createDataFrame(anomalies_rows, schema=result_schema)
    anomalies_df.write.mode("append").format("delta").saveAsTable(f"{cfg.database_name}.dq_anomalies")
    return anomalies_df