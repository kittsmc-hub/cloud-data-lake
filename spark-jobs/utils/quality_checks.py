"""
Data quality gate.

Design: quality checks are expressed as pure functions over a plain dict of
pre-computed stats (`run_checks`), so the pass/fail logic is unit-testable
without spinning up Spark. `compute_stats_from_df` is the only function that
touches a live DataFrame — it just aggregates numbers and hands them to the
pure checker.

A failed gate should cause the caller to quarantine the batch rather than
write it to silver. This is intentionally strict: shipping a "mostly fine"
batch to the warehouse is worse than a delayed pipeline run.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


@dataclass
class QualityReport:
    results: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failures(self) -> list:
        return [r for r in self.results if not r.passed]

    def summary(self) -> str:
        lines = [f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}" for r in self.results]
        return "\n".join(lines)


VALID_STATUS_CODES = set(range(100, 600))
VALID_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}


def run_checks(stats: dict, *, null_rate_threshold: float = 0.01,
                max_latency_ms: int = 30_000) -> QualityReport:
    """Evaluate a batch's aggregated stats against quality rules.

    Expected `stats` keys (all producible from a single Spark aggregation
    pass, see `compute_stats_from_df`):
        row_count: int
        null_counts: dict[str, int]   # per required column
        distinct_status_codes: set[int]
        distinct_methods: set[str]
        min_event_time: str (ISO)
        max_event_time: str (ISO)
        max_latency_ms: int
        duplicate_event_ids: int
    """
    report = QualityReport()
    row_count = stats.get("row_count", 0)

    # 1. Non-empty batch
    report.results.append(CheckResult(
        name="non_empty_batch",
        passed=row_count > 0,
        detail=f"row_count={row_count}",
    ))
    if row_count == 0:
        return report  # remaining checks are meaningless on an empty batch

    # 2. Null rate on required columns stays under threshold
    required_cols = ["event_id", "event_time", "endpoint", "status_code", "user_id"]
    for col in required_cols:
        nulls = stats.get("null_counts", {}).get(col, 0)
        rate = nulls / row_count
        report.results.append(CheckResult(
            name=f"null_rate::{col}",
            passed=rate <= null_rate_threshold,
            detail=f"{nulls}/{row_count} = {rate:.4f} (threshold {null_rate_threshold})",
        ))

    # 3. Status codes are in a valid HTTP range
    bad_codes = {c for c in stats.get("distinct_status_codes", set()) if c not in VALID_STATUS_CODES}
    report.results.append(CheckResult(
        name="status_code_range",
        passed=not bad_codes,
        detail=f"invalid codes found={sorted(bad_codes)}" if bad_codes else "all codes in 100-599",
    ))

    # 4. HTTP methods are from a known set
    bad_methods = {m for m in stats.get("distinct_methods", set()) if m not in VALID_METHODS}
    report.results.append(CheckResult(
        name="method_whitelist",
        passed=not bad_methods,
        detail=f"unknown methods={sorted(bad_methods)}" if bad_methods else "all methods known",
    ))

    # 5. No absurd latency values (sensor/serialization bug guard)
    max_latency = stats.get("max_latency_ms", 0)
    report.results.append(CheckResult(
        name="latency_sanity",
        passed=max_latency <= max_latency_ms,
        detail=f"max_latency_ms={max_latency} (limit {max_latency_ms})",
    ))

    # 6. No duplicate event_ids within the batch itself
    dupes = stats.get("duplicate_event_ids", 0)
    report.results.append(CheckResult(
        name="no_duplicate_event_ids",
        passed=dupes == 0,
        detail=f"duplicate_event_ids={dupes}",
    ))

    return report


def compute_stats_from_df(df) -> dict:
    """Spark-facing wrapper: aggregate a bronze batch DataFrame into the
    stats dict `run_checks` expects. Single pass, one Spark job."""
    from pyspark.sql import functions as F

    row_count = df.count()

    required_cols = ["event_id", "event_time", "endpoint", "status_code", "user_id"]
    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required_cols]
    null_row = df.select(*null_exprs).collect()[0].asDict()

    distinct_status = {r["status_code"] for r in df.select("status_code").distinct().collect()}
    distinct_methods = {r["method"] for r in df.select("method").distinct().collect()}

    max_latency = df.select(F.max("latency_ms")).collect()[0][0] or 0

    dup_count = (
        df.groupBy("event_id").count()
        .where(F.col("count") > 1)
        .count()
    )

    time_bounds = df.select(F.min("event_time"), F.max("event_time")).collect()[0]

    return {
        "row_count": row_count,
        "null_counts": null_row,
        "distinct_status_codes": distinct_status,
        "distinct_methods": distinct_methods,
        "max_latency_ms": max_latency,
        "duplicate_event_ids": dup_count,
        "min_event_time": str(time_bounds[0]),
        "max_event_time": str(time_bounds[1]),
    }
