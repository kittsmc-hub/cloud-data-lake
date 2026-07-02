"""
Bronze -> Silver incremental transform job.

Reads only bronze rows ingested since the last successful watermark, applies
light typing/cleanup, runs the data-quality gate, and MERGEs the result into
`silver.events` keyed on `event_id`. This makes re-runs idempotent: running
this job twice on the same bronze data produces the same silver table,
because MERGE upserts rather than appends, and late-arriving/duplicate
events with a previously-seen event_id update the existing row instead of
creating a new one.

Watermarking: the watermark is *not* an external state store. It's read from
Delta's own commit history on the silver table (the max `_ingested_at` value
already present in silver), so there's nothing extra to keep consistent and
no separate "checkpoint db" that can drift from reality.

On a quality gate failure, the batch is written to `quality-quarantine/`
instead of silver, and the job exits non-zero so Airflow marks the task
failed rather than silently proceeding with bad data.

Usage:
    spark-submit bronze_to_silver.py \
        --bronze-path s3a://bronze/raw_events \
        --silver-path s3a://silver/events \
        --quarantine-path s3a://quality-quarantine/events \
        --minio-endpoint http://minio:9000
"""
from __future__ import annotations

import argparse
import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

sys.path.append("/opt/spark-jobs")
from utils.quality_checks import run_checks, compute_stats_from_df  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_to_silver")


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


def get_watermark(spark: SparkSession, silver_path: str):
    try:
        existing = spark.read.format("delta").load(silver_path)
        row = existing.select(F.max("_ingested_at").alias("wm")).collect()[0]
        return row["wm"]
    except Exception:
        return None  # silver table doesn't exist yet -> process everything


def clean_and_type(df):
    """Light, deterministic cleanup — casts, trims, derived columns. No
    business logic here beyond what's needed to make the data queryable;
    business logic belongs in dbt marts, not in the ETL layer."""
    return (
        df.withColumn("status_code", F.col("status_code").cast("int"))
        .withColumn("latency_ms", F.col("latency_ms").cast("int"))
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("endpoint", F.trim(F.col("endpoint")))
        .withColumn("method", F.upper(F.trim(F.col("method"))))
        .dropDuplicates(["event_id"])  # last-write-wins within the batch itself
    )


def merge_into_silver(spark: SparkSession, df, silver_path: str):
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("silver table doesn't exist yet, creating via initial write")
        df.write.format("delta").mode("overwrite").save(silver_path)
        return

    target = DeltaTable.forPath(spark, silver_path)
    (
        target.alias("t")
        .merge(df.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--quarantine-path", required=True)
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--null-rate-threshold", type=float, default=0.01)
    args = parser.parse_args()

    spark = build_spark("bronze_to_silver", args.minio_endpoint)
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(args.bronze_path)
    watermark = get_watermark(spark, args.silver_path)

    if watermark is not None:
        logger.info("incremental run, watermark=%s", watermark)
        incremental = bronze.where(F.col("_ingested_at") > F.lit(watermark))
    else:
        logger.info("no watermark found, processing full bronze table (first run)")
        incremental = bronze

    row_count = incremental.count()
    logger.info("candidate rows for silver: %d", row_count)
    if row_count == 0:
        logger.info("nothing new since last watermark, exiting cleanly")
        spark.stop()
        return

    cleaned = clean_and_type(incremental)

    stats = compute_stats_from_df(cleaned)
    report = run_checks(stats, null_rate_threshold=args.null_rate_threshold)
    logger.info("quality report:\n%s", report.summary())

    if not report.passed:
        logger.error("quality gate FAILED: %d check(s) failed, quarantining batch",
                     len(report.failures))
        (
            cleaned.write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .save(args.quarantine_path)
        )
        spark.stop()
        sys.exit(1)

    merge_into_silver(spark, cleaned, args.silver_path)
    logger.info("merged %d rows into %s", row_count, args.silver_path)

    spark.stop()


if __name__ == "__main__":
    main()
