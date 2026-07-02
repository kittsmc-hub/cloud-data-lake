"""
log_pipeline_dag.py

Orchestrates the end-to-end pipeline:
  generate_synthetic_logs -> ingest_raw_to_bronze -> bronze_to_silver
    -> dbt_deps -> dbt_run -> dbt_test -> refresh_dashboard_cache

Design notes:
- Each task is independently retryable. bronze_to_silver and dbt steps are
  idempotent (MERGE-based / full-refresh-safe), so retries never duplicate
  data.
- bronze_to_silver failing on the data-quality gate is a real task failure
  (non-zero exit from the Spark job), not a warning — the DAG stops there
  and does not run dbt against a quarantined batch.
- SLA is set on the whole run so a stuck pipeline pages instead of silently
  going stale.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "raw")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "silver")
QUARANTINE_BUCKET = os.environ.get("QUARANTINE_BUCKET", "quality-quarantine")

SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="log_pipeline_dag",
    description="Raw logs -> bronze -> silver -> dbt marts -> dashboard",
    default_args=default_args,
    schedule="*/15 * * * *",   # every 15 minutes, simulating near-real-time ingestion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=None,
    max_active_runs=1,          # never run two ingests concurrently against the same watermark
    tags=["data-lake", "portfolio"],
) as dag:

    generate_synthetic_logs = BashOperator(
        task_id="generate_synthetic_logs",
        bash_command=(
            "python /opt/data-generator/generate_logs.py "
            "--batches 1 --rows-per-batch 3000 --upload "
            f"--bucket {RAW_BUCKET} --key-prefix events"
        ),
        env={
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "AWS_ACCESS_KEY_ID": "{{ var.value.get('minio_access_key', 'minioadmin') }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.get('minio_secret_key', 'minioadmin123') }}",
        },
        sla=timedelta(minutes=5),
    )

    ingest_raw_to_bronze = BashOperator(
        task_id="ingest_raw_to_bronze",
        bash_command=(
            "spark-submit --master " + SPARK_MASTER +
            " --packages " + SPARK_PACKAGES +
            " /opt/spark-jobs/ingest_raw_to_bronze.py"
            f" --raw-path s3a://{RAW_BUCKET}/events/"
            f" --bronze-path s3a://{BRONZE_BUCKET}/raw_events"
            f" --minio-endpoint {MINIO_ENDPOINT}"
        ),
        sla=timedelta(minutes=10),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit --master " + SPARK_MASTER +
            " --packages " + SPARK_PACKAGES +
            " /opt/spark-jobs/bronze_to_silver.py"
            f" --bronze-path s3a://{BRONZE_BUCKET}/raw_events"
            f" --silver-path s3a://{SILVER_BUCKET}/events"
            f" --quarantine-path s3a://{QUARANTINE_BUCKET}/events"
            f" --minio-endpoint {MINIO_ENDPOINT}"
            " --null-rate-threshold 0.01"
        ),
        sla=timedelta(minutes=15),
        # Non-zero exit (quality gate failure) fails this task and the DAG
        # stops here by default trigger rule (all_success) on downstream tasks.
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/dbt/log_analytics && dbt deps --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt/log_analytics && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt/log_analytics && dbt test --profiles-dir .",
    )

    def _touch_refresh_marker(**context):
        """Deterministic, zero-dependency 'cache refresh' signal: writes a
        timestamp file the dashboard polls to know new data is ready,
        instead of the dashboard guessing based on wall-clock time."""
        marker_path = "/opt/warehouse/_last_successful_run.txt"
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w") as f:
            f.write(context["ts"])

    refresh_dashboard_cache = PythonOperator(
        task_id="refresh_dashboard_cache",
        python_callable=_touch_refresh_marker,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        generate_synthetic_logs
        >> ingest_raw_to_bronze
        >> bronze_to_silver
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> refresh_dashboard_cache
    )
