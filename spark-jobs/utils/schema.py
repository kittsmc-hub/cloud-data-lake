"""
Schema evolution utilities.

The pure functions here (`diff_field_sets`, `classify_schema_change`) operate
on plain dicts of {field_name: type_name} and have zero Spark dependency, so
they're unit-testable without a cluster. `evolve_and_log` is the thin Spark
wrapper that pulls a real schema off a DataFrame and calls into the pure
logic — that's the only part that needs a live SparkSession.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("schema_evolution")


class SchemaEvolutionError(Exception):
    """Raised when an incoming batch changes a field's type incompatibly
    (e.g. a column that used to be an int now arrives as a struct) — this is
    NOT something mergeSchema should silently paper over."""


@dataclass
class SchemaDiff:
    added_fields: dict = field(default_factory=dict)     # name -> type
    removed_fields: dict = field(default_factory=dict)    # name -> type (present in existing, absent in incoming)
    type_changes: dict = field(default_factory=dict)      # name -> (old_type, new_type)

    @property
    def is_additive_only(self) -> bool:
        return not self.removed_fields and not self.type_changes

    @property
    def has_changes(self) -> bool:
        return bool(self.added_fields or self.removed_fields or self.type_changes)


# Types that are safe to widen automatically (narrow -> wide is compatible,
# matches Spark/Delta's own widening rules for the common numeric cases).
SAFE_WIDENING = {
    ("integer", "long"),
    ("integer", "double"),
    ("long", "double"),
    ("float", "double"),
}


def diff_field_sets(existing_schema: dict, incoming_schema: dict) -> SchemaDiff:
    """Compare two flat {field_name: type_name} schemas.

    `existing_schema` is the schema of the table as it stands today.
    `incoming_schema` is the schema of the new batch about to be written.
    """
    existing_fields = set(existing_schema)
    incoming_fields = set(incoming_schema)

    added = {f: incoming_schema[f] for f in incoming_fields - existing_fields}
    removed = {f: existing_schema[f] for f in existing_fields - incoming_fields}

    type_changes = {}
    for f in existing_fields & incoming_fields:
        old_t, new_t = existing_schema[f], incoming_schema[f]
        if old_t != new_t:
            type_changes[f] = (old_t, new_t)

    return SchemaDiff(added_fields=added, removed_fields=removed, type_changes=type_changes)


def classify_schema_change(diff: SchemaDiff) -> str:
    """Classify a diff as one of: 'none', 'additive', 'widening', 'breaking'.

    - additive: new optional columns only -> safe, mergeSchema handles it
    - widening: existing column's type got wider (int -> long etc) -> safe
    - breaking: removed a column the table has, or an incompatible type
      change -> must not be auto-applied, needs a human decision
    """
    if not diff.has_changes:
        return "none"

    if diff.removed_fields:
        return "breaking"

    for old_t, new_t in diff.type_changes.values():
        if (old_t, new_t) not in SAFE_WIDENING:
            return "breaking"

    if diff.type_changes:
        return "widening"

    return "additive"


def evolve_and_log(spark_df, existing_table_schema: dict | None, table_name: str) -> str:
    """Spark-facing wrapper: extract incoming schema from a DataFrame, diff
    it against the existing table schema, log the outcome, and raise on
    breaking changes so a bad batch never gets auto-merged.

    Returns the classification string. Callers should only proceed with
    `mergeSchema=true` writes when this returns 'none', 'additive', or
    'widening'.
    """
    incoming_schema = {f.name: f.dataType.simpleString() for f in spark_df.schema.fields}

    if existing_table_schema is None:
        logger.info("table %s does not exist yet; initial schema has %d fields",
                    table_name, len(incoming_schema))
        return "none"

    diff = diff_field_sets(existing_table_schema, incoming_schema)
    classification = classify_schema_change(diff)

    if classification == "none":
        return classification

    if classification == "additive":
        logger.info("schema evolution on %s: additive, new fields=%s",
                    table_name, sorted(diff.added_fields))
    elif classification == "widening":
        logger.info("schema evolution on %s: type widening=%s",
                    table_name, diff.type_changes)
    else:
        logger.error(
            "BREAKING schema change on %s: removed=%s type_changes=%s",
            table_name, sorted(diff.removed_fields), diff.type_changes,
        )
        raise SchemaEvolutionError(
            f"breaking schema change on {table_name}: "
            f"removed={sorted(diff.removed_fields)} type_changes={diff.type_changes}"
        )

    return classification
