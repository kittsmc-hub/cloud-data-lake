"""
Unit tests for utils/quality_checks.py. Pure Python, no SparkSession
required. These cover the actual pass/fail decision logic that gates
whether a batch reaches silver or gets quarantined.
"""
from utils.quality_checks import run_checks


def _good_stats(row_count=1000):
    return {
        "row_count": row_count,
        "null_counts": {
            "event_id": 0, "event_time": 0, "endpoint": 0,
            "status_code": 0, "user_id": 0,
        },
        "distinct_status_codes": {200, 201, 404, 500},
        "distinct_methods": {"GET", "POST", "PUT"},
        "max_latency_ms": 4500,
        "duplicate_event_ids": 0,
    }


def test_clean_batch_passes_all_checks():
    report = run_checks(_good_stats())
    assert report.passed
    assert report.failures == []


def test_empty_batch_fails_immediately_and_skips_rest():
    report = run_checks(_good_stats(row_count=0))
    assert not report.passed
    assert len(report.results) == 1
    assert report.results[0].name == "non_empty_batch"


def test_high_null_rate_fails():
    stats = _good_stats(row_count=1000)
    stats["null_counts"]["event_id"] = 50  # 5% > 1% threshold
    report = run_checks(stats, null_rate_threshold=0.01)
    assert not report.passed
    failed_names = {r.name for r in report.failures}
    assert "null_rate::event_id" in failed_names


def test_null_rate_within_threshold_passes():
    stats = _good_stats(row_count=1000)
    stats["null_counts"]["event_id"] = 5  # 0.5% < 1% threshold
    report = run_checks(stats, null_rate_threshold=0.01)
    assert report.passed


def test_invalid_status_code_fails():
    stats = _good_stats()
    stats["distinct_status_codes"] = {200, 9999}
    report = run_checks(stats)
    assert not report.passed
    failed_names = {r.name for r in report.failures}
    assert "status_code_range" in failed_names


def test_unknown_http_method_fails():
    stats = _good_stats()
    stats["distinct_methods"] = {"GET", "FETCH"}  # FETCH isn't a real HTTP verb
    report = run_checks(stats)
    assert not report.passed
    failed_names = {r.name for r in report.failures}
    assert "method_whitelist" in failed_names


def test_absurd_latency_fails():
    stats = _good_stats()
    stats["max_latency_ms"] = 999_999  # clearly a serialization bug
    report = run_checks(stats, max_latency_ms=30_000)
    assert not report.passed
    failed_names = {r.name for r in report.failures}
    assert "latency_sanity" in failed_names


def test_duplicate_event_ids_within_batch_fails():
    stats = _good_stats()
    stats["duplicate_event_ids"] = 3
    report = run_checks(stats)
    assert not report.passed
    failed_names = {r.name for r in report.failures}
    assert "no_duplicate_event_ids" in failed_names


def test_multiple_simultaneous_failures_are_all_reported():
    stats = _good_stats()
    stats["duplicate_event_ids"] = 3
    stats["distinct_status_codes"] = {9999}
    report = run_checks(stats)
    failed_names = {r.name for r in report.failures}
    assert "no_duplicate_event_ids" in failed_names
    assert "status_code_range" in failed_names
    assert len(report.failures) >= 2


def test_summary_output_is_human_readable():
    report = run_checks(_good_stats())
    summary = report.summary()
    assert "PASS" in summary
    assert "non_empty_batch" in summary
