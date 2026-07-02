"""
Raw -> Bronze ingestion job.

Reads newly-arrived JSON-lines log batches from `raw/`, appends them to the
`bronze.raw_events` Delta table, and evolves the table schema when new
(optional) fields show up. This is an append-only, no-transform layer: the
point of bronze is to preserve exactly what arrived, so any downstream bug
can always be traced back to the untouched source data.

Idempotency: each source file's path is recorded in the `_source_file`
column. Re-running against files already ingested is a no-op because the
Airflow task only passes newly-listed keys (see airflow/dags/log_pipeline_dag.py),
and as a second line of defense this job anti-joins against files already
present in bronze before writing.

Usage:
    spark-submit ingest_raw_to_bronze.py \
        --raw-path s3a://raw/events/ \
        --bronze-path s3a://bronze/raw_events \
        --minio-endpoint http://minio:9000
"""
from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession, functions as F

sys.path.append("/opt/spark-jobs")
from utils.schema import evolve_and_log, SchemaEvolutionError  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest_raw_to_bronze")

RAW_SCHEMA_REQUIRED_COLS = [
    "event_id", "event_time", "endpoint", "method", "status_code",
    "latency_ms", "user_id", "country", "ingest_batch_id",
]


def build_spark(app_name: str, minio_endpoint: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    return builder.getOrCreate()


def get_existing_schema(spark: SparkSession, bronze_path: str) -> dict | None:
    try:
        existing = spark.read.format("delta").load(bronze_path)
        return {f.name: f.dataType.simpleString() for f in existing.schema.fields}
    except Exception:
        return None  # table doesn't exist yet


def read_new_batches(spark: SparkSession, raw_path: str):
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .json(raw_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("event_time"))
    )
    return df


def validate_required_columns(df) -> None:
    missing = [c for c in RAW_SCHEMA_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"raw batch is missing required columns: {missing}")


def dedupe_already_ingested(spark: SparkSession, df, bronze_path: str):
    """Anti-join against files already present in bronze, keyed on
    _source_file, so a re-triggered DAG run never double-ingests a file."""
    try:
        existing_files = (
            spark.read.format("delta").load(bronze_path)
            .select("_source_file").distinct()
        )
        return df.join(existing_files, on="_source_file", how="left_anti")
    except Exception:
        return df  # table doesn't exist yet, nothing to dedupe against


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    args = parser.parse_args()

    spark = build_spark("ingest_raw_to_bronze", args.minio_endpoint)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("reading new batches from %s", args.raw_path)
    df = read_new_batches(spark, args.raw_path)
    validate_required_columns(df)

    row_count_before_dedupe = df.count()
    existing_schema = get_existing_schema(spark, args.bronze_path)

    try:
        classification = evolve_and_log(df, existing_schema, table_name="bronze.raw_events")
    except SchemaEvolutionError:
        logger.error("aborting ingestion due to breaking schema change; "
                      "a human needs to decide the migration path")
        spark.stop()
        sys.exit(1)

    df = dedupe_already_ingested(spark, df, args.bronze_path)
    new_row_count = df.count()
    logger.info("rows read=%d, new after file-level dedupe=%d, schema_change=%s",
                row_count_before_dedupe, new_row_count, classification)

    if new_row_count == 0:
        logger.info("nothing new to ingest, exiting cleanly")
        spark.stop()
        return

    writer = (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("ingest_date")
    )
    writer.save(args.bronze_path)
    logger.info("wrote %d rows to %s", new_row_count, args.bronze_path)

    spark.stop()


if __name__ == "__main__":
    main()
