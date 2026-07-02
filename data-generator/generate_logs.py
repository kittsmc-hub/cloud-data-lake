"""
Synthetic web-server log generator.

Simulates a company's application servers emitting request logs. Produces
JSON-lines batches with realistic distributions (skewed endpoint popularity,
occasional error spikes, late-arriving events, and a slow schema drift where
new fields get added over time) so the downstream pipeline has something
non-trivial to actually process.

Usage:
    python generate_logs.py --batches 5 --rows-per-batch 5000 --out ./local_output
    python generate_logs.py --batches 5 --rows-per-batch 5000 --upload --bucket raw
"""
from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ENDPOINTS = [
    ("/api/v1/products", 0.28),
    ("/api/v1/products/{id}", 0.20),
    ("/api/v1/cart", 0.15),
    ("/api/v1/checkout", 0.07),
    ("/api/v1/search", 0.18),
    ("/api/v1/users/{id}", 0.06),
    ("/api/v1/recommendations", 0.06),
]

METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE"]

STATUS_WEIGHTS = [(200, 0.90), (201, 0.03), (400, 0.02), (401, 0.01),
                   (404, 0.02), (500, 0.015), (503, 0.005)]

COUNTRIES = ["US", "GB", "DE", "IN", "BR", "JP", "KE", "NG", "CA", "AU"]


def weighted_choice(pairs):
    r = random.random()
    upto = 0.0
    for item, weight in pairs:
        upto += weight
        if r <= upto:
            return item
    return pairs[-1][0]


def make_event(event_time: datetime, schema_version: int) -> dict:
    endpoint = weighted_choice(ENDPOINTS)
    status = weighted_choice(STATUS_WEIGHTS)
    latency_ms = max(1, int(random.gauss(120, 60)))
    if status >= 500:
        latency_ms += random.randint(200, 2000)  # errors tend to be slow

    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time.isoformat(),
        "endpoint": endpoint,
        "method": weighted_choice([(m, 1 / len(METHODS)) for m in METHODS]),
        "status_code": status,
        "latency_ms": latency_ms,
        "user_id": f"u_{random.randint(1, 50000)}",
        "country": random.choice(COUNTRIES),
        "ingest_batch_id": None,  # filled in by caller
    }

    # Simulate schema evolution: newer batches include additional fields
    # that older batches never had. This exercises mergeSchema downstream.
    if schema_version >= 2:
        event["user_agent"] = random.choice(
            ["Mozilla/5.0", "okhttp/4.9", "PostmanRuntime/7.32", "curl/8.4"]
        )
    if schema_version >= 3:
        event["session_id"] = str(uuid.uuid4())
        event["is_authenticated"] = random.random() > 0.3

    return event


def generate_batch(rows: int, batch_id: str, schema_version: int,
                    late_arrival_pct: float = 0.03) -> list[dict]:
    now = datetime.now(timezone.utc)
    events = []
    for _ in range(rows):
        # Most events are "now", a small fraction are late-arriving
        # (simulating network delay / buffered client uploads) to exercise
        # the incremental-merge / dedup logic downstream.
        if random.random() < late_arrival_pct:
            event_time = now - timedelta(minutes=random.randint(5, 240))
        else:
            event_time = now - timedelta(seconds=random.randint(0, 30))
        event = make_event(event_time, schema_version)
        event["ingest_batch_id"] = batch_id
        events.append(event)
    return events


def write_batch_local(events: list[dict], out_dir: Path, batch_id: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"events_{batch_id}.jsonl"
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return path


def upload_batch_to_minio(path: Path, bucket: str, key_prefix: str):
    import boto3  # local import so `--out` mode has zero extra deps

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    date_partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"{key_prefix}/{date_partition}/{path.name}"
    s3.upload_file(str(path), bucket, key)
    print(f"uploaded s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batches", type=int, default=3)
    parser.add_argument("--rows-per-batch", type=int, default=1000)
    parser.add_argument("--out", type=str, default="./local_output")
    parser.add_argument("--upload", action="store_true",
                         help="upload batches to MinIO instead of only writing locally")
    parser.add_argument("--bucket", type=str, default=os.environ.get("RAW_BUCKET", "raw"))
    parser.add_argument("--key-prefix", type=str, default="events")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    out_dir = Path(args.out)
    for i in range(args.batches):
        batch_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{i:03d}"
        # schema drifts upward over the run to simulate a real evolving app
        schema_version = 1 + min(2, i // max(1, args.batches // 3))
        events = generate_batch(args.rows_per_batch, batch_id, schema_version)
        path = write_batch_local(events, out_dir, batch_id)
        print(f"wrote {len(events)} rows -> {path} (schema_version={schema_version})")

        if args.upload:
            upload_batch_to_minio(path, args.bucket, args.key_prefix)


if __name__ == "__main__":
    main()
