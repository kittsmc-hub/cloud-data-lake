"""
Unit tests for utils/schema.py. Pure Python, no SparkSession required —
these are the "gate tests" that should run on every commit.
"""
import pytest

from utils.schema import (
    SchemaEvolutionError,
    classify_schema_change,
    diff_field_sets,
)


def test_diff_no_changes():
    schema = {"a": "string", "b": "int"}
    diff = diff_field_sets(schema, schema)
    assert not diff.has_changes
    assert classify_schema_change(diff) == "none"


def test_diff_detects_added_field():
    existing = {"a": "string", "b": "int"}
    incoming = {"a": "string", "b": "int", "c": "string"}
    diff = diff_field_sets(existing, incoming)
    assert diff.added_fields == {"c": "string"}
    assert not diff.removed_fields
    assert not diff.type_changes
    assert classify_schema_change(diff) == "additive"


def test_diff_detects_removed_field_as_breaking():
    existing = {"a": "string", "b": "int"}
    incoming = {"a": "string"}
    diff = diff_field_sets(existing, incoming)
    assert diff.removed_fields == {"b": "int"}
    assert classify_schema_change(diff) == "breaking"


def test_safe_widening_int_to_long():
    existing = {"latency_ms": "integer"}
    incoming = {"latency_ms": "long"}
    diff = diff_field_sets(existing, incoming)
    assert diff.type_changes == {"latency_ms": ("integer", "long")}
    assert classify_schema_change(diff) == "widening"


def test_unsafe_type_change_is_breaking():
    existing = {"status_code": "integer"}
    incoming = {"status_code": "string"}
    diff = diff_field_sets(existing, incoming)
    assert classify_schema_change(diff) == "breaking"


def test_added_and_removed_together_is_breaking_not_additive():
    existing = {"a": "string", "b": "int"}
    incoming = {"a": "string", "c": "string"}
    diff = diff_field_sets(existing, incoming)
    assert diff.added_fields == {"c": "string"}
    assert diff.removed_fields == {"b": "int"}
    assert classify_schema_change(diff) == "breaking"


class _FakeDataType:
    def __init__(self, name):
        self._name = name

    def simpleString(self):
        return self._name


class _FakeField:
    def __init__(self, name, type_name):
        self.name = name
        self.dataType = _FakeDataType(type_name)


class _FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class _FakeDataFrame:
    def __init__(self, field_types: dict):
        self.schema = _FakeSchema([_FakeField(n, t) for n, t in field_types.items()])


def test_evolve_and_log_raises_on_breaking_change():
    from utils.schema import evolve_and_log

    existing_schema = {"event_id": "string", "status_code": "integer"}
    incoming_df = _FakeDataFrame({"event_id": "string"})  # dropped status_code

    with pytest.raises(SchemaEvolutionError):
        evolve_and_log(incoming_df, existing_schema, table_name="bronze.raw_events")


def test_evolve_and_log_passes_on_additive_change():
    from utils.schema import evolve_and_log

    existing_schema = {"event_id": "string"}
    incoming_df = _FakeDataFrame({"event_id": "string", "user_agent": "string"})

    result = evolve_and_log(incoming_df, existing_schema, table_name="bronze.raw_events")
    assert result == "additive"


def test_evolve_and_log_first_write_is_none():
    from utils.schema import evolve_and_log

    incoming_df = _FakeDataFrame({"event_id": "string"})
    result = evolve_and_log(incoming_df, None, table_name="bronze.raw_events")
    assert result == "none"
