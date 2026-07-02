# Cloud Data Lake — Terabyte-Scale Log Analytics Platform

[![CI](https://github.com/kittsmc-hub/cloud-data-lake/actions/workflows/ci.yml/badge.svg)](https://github.com/kittsmc-hub/cloud-data-lake/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/delta--lake-3.2-blue)
![dbt](https://img.shields.io/badge/dbt-duckdb--1.8-ff694b)
![License](https://img.shields.io/badge/license-MIT-green)

A runnable, end-to-end simulation of a company-grade lakehouse: synthetic
web-server logs land in object storage, get processed by Spark into a
schema-evolving, upsert-capable Delta Lake, get modeled by dbt into a DuckDB
warehouse, and get visualized on a live dashboard — orchestrated by Airflow,
reproducible with a single `docker compose up`.

Built to demonstrate production data engineering patterns, not tutorial
patterns: incremental processing, schema evolution, data-quality gating,
idempotent pipelines, and transformation logic that's actually unit tested.


---

## Table of contents

- [Architecture](#architecture)
- [Stack and why](#stack-and-why)
- [Engineering decisions](#engineering-decisions-the-part-worth-reading)
- [Repo layout](#repo-layout)
- [Quickstart](#quickstart)
- [Testing strategy](#testing-strategy)
- [Observability](#observability)
- [Security](#security)
- [Failure modes and how they're handled](#failure-modes-and-how-theyre-handled)
- [Scaling to real terabytes](#scaling-to-real-terabytes)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)

## Architecture

```
                 ┌──────────────┐
  synthetic      │   Users /    │
  log events ───▶│  App Servers │
                 └──────┬───────┘
                        │ JSON lines, batched
                        ▼
                 ┌──────────────┐
                 │  MinIO (S3)  │  raw/  bronze/  silver/  quality-quarantine/
                 │ object store │
                 └──────┬───────┘
                        │ read (s3a://)
                        ▼
                 ┌──────────────┐
                 │    Spark     │  ingest_raw_to_bronze.py  (append, schema evolution)
                 │ (Delta Lake) │  bronze_to_silver.py      (incremental MERGE, quality gate)
                 └──────┬───────┘
                        ▼
                 ┌──────────────┐
                 │  Delta Lake  │  bronze.raw_events  (append-only, replay source)
                 │   tables     │  silver.events      (deduped, typed, quality-gated)
                 └──────┬───────┘
                        │ dbt (delta_scan via DuckDB)
                        ▼
                 ┌──────────────┐
                 │   DuckDB     │  staging (stg_logs) → marts
                 │  warehouse   │  (fct_requests_daily, dim_endpoints)
                 └──────┬───────┘
                        ▼
                 ┌──────────────┐
                 │  Streamlit   │  traffic, error rate, latency percentiles,
                 │  dashboard   │  top endpoints, pipeline freshness
                 └──────────────┘

  Orchestration: Airflow DAG (log_pipeline_dag) runs every 15 minutes:
  generate → ingest_raw_to_bronze → bronze_to_silver → dbt deps → dbt run → dbt test
  → refresh_dashboard_cache, with retries, SLAs, and a hard stop on
  data-quality gate failure (bad data never reaches dbt or the dashboard).
```

## Stack and why

| Layer          | Tool                        | Why this and not the obvious alternative |
|----------------|------------------------------|-------------------------------------------|
| Object storage | MinIO (S3-compatible)        | Identical API to real S3, runs locally, zero cloud bill. Swapping to real S3 is an endpoint config change, not a rewrite. |
| Processing     | Apache Spark + Delta Lake    | Delta gives ACID `MERGE`, schema evolution, and time travel — the actual reasons lakehouses exist instead of raw Parquet files. |
| Orchestration  | Apache Airflow               | Industry-standard DAG scheduler; retries, SLAs, and task-level observability out of the box. |
| Transformation | dbt (dbt-duckdb adapter)     | SQL transformation logic should be version-controlled, tested, and documented independent of the engine that runs it. |
| Warehouse      | DuckDB                       | In-process, zero infra, reads Delta/Parquet directly. Increasingly the real choice for mid-size companies whose data fits on one box — and it's what makes this whole stack runnable on a laptop. |
| Dashboard      | Streamlit                    | Queries DuckDB directly, no API layer needed for a project this size. |
| Infra          | Docker Compose               | Single-command reproducibility for a portfolio reviewer. |

## Engineering decisions (the part worth reading)

This section exists because "I used Spark and Airflow" doesn't tell a
reviewer anything. These are the actual decisions and the trade-offs behind
them.

**1. Schema diffing is pure Python, separate from the Spark write path.**
`spark-jobs/utils/schema.py` classifies an incoming batch's schema change as
`none` / `additive` / `widening` / `breaking` using plain
`{field_name: type_name}` dicts — zero Spark dependency. `evolve_and_log` is
a five-line wrapper that pulls a real schema off a DataFrame and calls into
that pure logic. Breaking changes (a dropped column, an incompatible type
change) raise and stop the write; `mergeSchema=true` only runs after the
diff says it's safe. Trade-off: an extra read of the existing table's schema
on every ingest run. Worth it — a silent breaking merge is a worse outage
than one extra metadata read.

**2. The watermark lives in the data, not a separate state store.**
`bronze_to_silver.py` reads `max(_ingested_at)` already present in the
silver table as its own watermark, instead of an external checkpoint (Redis
key, Airflow XCom, a tracking table). Trade-off: can't process bronze and
silver fully in parallel across runs. Worth it — there's no external state
that can drift from what's actually in the table, and a failed run doesn't
leave a stale checkpoint behind.

**3. `MERGE`, not `INSERT`, into silver.** Silver is upserted on `event_id`,
so re-running the job (retry, backfill, manual replay) is idempotent by
construction. The log generator deliberately produces late-arriving,
out-of-order events to prove this: duplicates land as updates, not extra
rows, and the daily traffic numbers in `fct_requests_daily` don't
double-count.

**4. Data quality is a gate, not a report.** A failed quality check
(`spark-jobs/utils/quality_checks.py`) quarantines the batch and fails the
Spark job with a non-zero exit — Airflow marks the task failed and
everything downstream stops. This is deliberately stricter than "log a
warning and continue." A dashboard fed by silently-passed bad data is worse
than a delayed pipeline run. dbt schema tests + a singular freshness test
add a second, independent quality layer at the modeling boundary.

**5. DuckDB over a cloud warehouse.** The SQL dialect is close enough to
Snowflake/BigQuery that the migration path is a `profiles.yml` target
change, not a model rewrite — dbt's adapter pattern is what makes that true.
Chose this specifically so the project needs no paid infrastructure to run
or demo.

## Repo layout

```
cloud-data-lake/
├── docker-compose.yml          # MinIO, Postgres, Airflow, Spark master/worker, dashboard
├── Makefile                    # make up / test-unit / dbt-run / trigger / clean
├── .github/workflows/ci.yml    # unit tests + dbt parse + docker build, on every push
├── data-generator/
│   └── generate_logs.py        # synthetic logs: skewed traffic, error spikes,
│                                #   late arrivals, deliberate schema drift over time
├── spark-jobs/
│   ├── ingest_raw_to_bronze.py # raw -> bronze: append-only, schema evolution, file-level dedupe
│   ├── bronze_to_silver.py     # bronze -> silver: incremental MERGE, quality gate
│   ├── utils/
│   │   ├── schema.py           # pure schema-diff/classification logic (unit tested)
│   │   └── quality_checks.py   # pure quality-gate pass/fail logic (unit tested)
│   ├── tests/                  # gate tests — no Spark cluster required to run these
│   └── requirements.txt
├── airflow/
│   ├── dags/log_pipeline_dag.py
│   └── Dockerfile              # Airflow image + spark-submit client
├── dbt/log_analytics/
│   ├── models/staging/         # stg_logs.sql — typed passthrough of silver
│   ├── models/marts/           # fct_requests_daily, dim_endpoints + schema.yml tests
│   ├── tests/                  # assert_silver_data_is_fresh.sql (SLA enforcement)
│   └── profiles.yml            # duckdb adapter, delta_scan against MinIO
├── dashboard/
│   └── app.py                  # Streamlit, queries the DuckDB warehouse directly
├── infra/
│   └── minio-init.sh           # idempotent bucket bootstrap
└── blog/
    └── building-a-terabyte-scale-data-lake.md
```

## Quickstart

```bash
git clone <this repo> && cd cloud-data-lake
cp .env.example .env
docker compose up -d --build        # MinIO, Postgres, Airflow, Spark, dashboard

./infra/minio-init.sh               # bootstrap buckets (idempotent)

# Airflow UI: http://localhost:8080  (admin/admin)
# unpause + trigger log_pipeline_dag, or:
make trigger

# Spark master UI: http://localhost:8082   (worker UI: http://localhost:8083)
# MinIO console:   http://localhost:9001  (minioadmin/minioadmin123)

# once a run completes:
# Dashboard: http://localhost:8501
```

### Running pieces independently (no Docker required)

```bash
# unit tests for schema evolution + quality-gate logic — pure Python, <2s, no cluster
make test-unit

# generate a batch of synthetic logs locally, inspect the JSON directly
cd data-generator && python generate_logs.py --batches 5 --rows-per-batch 5000 --out ./local_output
```

## Testing strategy

Two lanes, deliberately different cost/speed profiles:

- **Gate tests** (`spark-jobs/tests/`) — pure Python, no Spark cluster, run in
  under two seconds, run on every push via CI (`.github/workflows/ci.yml`).
  These cover the actual decision logic: every schema-change classification
  branch (`none`/`additive`/`widening`/`breaking`) and every quality-check
  failure mode (null rate, bad status code, unknown method, latency outlier,
  duplicate `event_id`s, empty batch). This is the logic a reviewer should
  read first — `test_schema_evolution.py` and `test_quality_checks.py`.
- **dbt tests** — schema tests (`unique`, `not_null`, `accepted_values`,
  `dbt_utils.accepted_range`) on every mart column that matters, plus a
  singular freshness SLA test that fails the run if silver hasn't seen new
  data in 2 hours.
- **CI** also runs `dbt parse` against an in-memory DuckDB target so
  Jinja/SQL errors in the models are caught without needing live MinIO/Spark
  services in the runner, and builds both Docker images to catch Dockerfile
  breakage before it reaches a real deploy.

## Observability

- Every Spark job logs the quality report (`QualityReport.summary()`) and
  the schema-change classification on every run — visible directly in
  Airflow task logs, no separate log aggregation needed for a project this
  size.
- `refresh_dashboard_cache` writes a timestamp marker
  (`/opt/warehouse/_last_successful_run.txt`) that the dashboard reads and
  displays, so "is this data fresh" is answered by the pipeline itself, not
  inferred from wall-clock time.
- The dbt freshness test is the automated version of the same check — it
  fails loudly in CI/orchestration instead of relying on someone noticing a
  stale chart.

## Security

- No secrets in the repo. `.env` is gitignored; `.env.example` documents
  every variable a fresh clone needs.
- MinIO/Postgres/Airflow credentials are dev defaults meant for local demo
  only — a real deployment would source these from a secrets manager
  (AWS Secrets Manager / Vault / SSM Parameter Store) and never bake them
  into `docker-compose.yml`.
- Airflow's `AIRFLOW__WEBSERVER__EXPOSE_CONFIG` is on for local demo
  convenience; that's a setting a production deployment turns off.

## Failure modes and how they're handled

| Failure                                   | Handling |
|--------------------------------------------|----------|
| New optional field appears in raw logs      | `mergeSchema` applies automatically after the diff classifies it `additive`. |
| A field's type narrows/disappears           | `SchemaEvolutionError` raised, ingestion job exits non-zero, Airflow task fails, no partial write. |
| Batch has an unacceptable null rate         | Quality gate fails, batch quarantined to `quality-quarantine/`, silver untouched. |
| Duplicate `event_id` from a retried request | `MERGE ... whenMatchedUpdateAll` — upsert, not a duplicate row. |
| Late-arriving event (clock skew, buffering) | Picked up by the next incremental run via the `_ingested_at` watermark; MERGE handles it whether it lands as a new row or an update. |
| Pipeline silently stops running             | dbt freshness test fails on the next scheduled `dbt test`, well before a human would notice a stale dashboard. |
| Re-run after a partial failure              | Every write is idempotent (append with file-level dedupe in bronze, MERGE in silver), so re-running a failed DAG run is always safe. |

## Scaling to real terabytes

The project is sized for a laptop, but every interface matches what a
production version would use:

- **MinIO → S3/GCS**: change `fs.s3a.endpoint`. No code changes.
- **Local Spark → EMR/Databricks/Dataproc**: point `spark-submit` at a real
  cluster master, raise `spark.sql.shuffle.partitions`.
- **Airflow LocalExecutor → CeleryExecutor/KubernetesExecutor**: for
  multi-worker orchestration once one box can't run all tasks.
- **DuckDB → Snowflake/BigQuery/Redshift**: change the dbt `profiles.yml`
  target. The SQL in `models/` doesn't change — that's the point of using
  dbt instead of hand-written DuckDB-specific queries.

## Troubleshooting

- **`spark-submit` fails to download Delta/Hadoop packages**: the Airflow
  container resolves `--packages io.delta:delta-spark_2.12:3.2.0,...` from
  Maven Central at submit time, which needs outbound internet access from
  wherever `docker compose` is running. If you're behind a restrictive
  network, pre-fetch the jars into a local Maven repo and mount it, or bake
  them into the `airflow/Dockerfile` image instead of resolving them per-run.
- **Port already in use** (8080, 9000, 9001, 7077, 8082, 8083, 8501): another
  local service (or a previous run of this project) is holding the port.
  `docker compose down` first, or remap the host side of the port in
  `docker-compose.yml`.
- **`docker.io/bitnami/spark` image not found**: Bitnami retired free public
  tags for this image in 2025 — it's now a paid "Bitnami Secure Images"
  product. This repo already uses the official `apache/spark:3.5.1` image
  instead; if you see this error you're on an older copy of the compose
  file.
- **Airflow webserver healthy but DAG doesn't appear**: give the scheduler
  a minute to parse `airflow/dags/log_pipeline_dag.py` on first boot, or
  check `docker compose logs airflow-scheduler` for a Jinja/import error.

## Known limitations

- DuckDB is genuinely single-node; this project doesn't pretend otherwise,
  it's an explicit, stated trade-off for local demoability (see [Scaling](#scaling-to-real-terabytes)).
- The Airflow DAG uses `LocalExecutor` — fine for a 15-minute-interval demo
  pipeline, not sized for high task-parallelism.
- No automated backfill CLI yet (see Roadmap) — backfills currently mean
  re-running the DAG with an adjusted date range by hand.

## Roadmap

- [ ] Backfill CLI: re-run `bronze_to_silver` for an arbitrary historical
      window without relying on the live watermark.
- [ ] Great Expectations or Soda integration as a second quality layer on
      top of the custom gate, for checks that benefit from a standard
      library (distribution drift, anomaly detection).
- [ ] Terraform module to stand this up against real AWS S3 + MWAA + EMR
      Serverless, so the "scaling to real terabytes" section has a working
      example, not just a description.
- [ ] dbt exposures wired to the Streamlit dashboard for documented
      lineage from raw source to dashboard panel.

## License

MIT — use this as a template for your own portfolio.
