#!/usr/bin/env bash
# Idempotently creates the buckets the pipeline expects. Safe to re-run.
set -euo pipefail

MINIO_ALIAS="local"
MINIO_HOST="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin123}"

BUCKETS=("${RAW_BUCKET:-raw}" "${BRONZE_BUCKET:-bronze}" "${SILVER_BUCKET:-silver}" "${QUARANTINE_BUCKET:-quality-quarantine}")

if ! command -v mc >/dev/null 2>&1; then
  echo "MinIO client (mc) not found locally; running bootstrap inside the minio container instead."
  for b in "${BUCKETS[@]}"; do
    docker compose exec -T minio mc alias set "$MINIO_ALIAS" "http://localhost:9000" "$MINIO_USER" "$MINIO_PASS" >/dev/null
    docker compose exec -T minio mc mb --ignore-existing "$MINIO_ALIAS/$b"
    echo "ensured bucket: $b"
  done
  exit 0
fi

mc alias set "$MINIO_ALIAS" "$MINIO_HOST" "$MINIO_USER" "$MINIO_PASS" >/dev/null
for b in "${BUCKETS[@]}"; do
  mc mb --ignore-existing "$MINIO_ALIAS/$b"
  echo "ensured bucket: $b"
done

echo "all buckets ready: ${BUCKETS[*]}"
