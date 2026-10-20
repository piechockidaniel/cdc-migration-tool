"""Failure pattern mining.

Takes the failure groups from the audit store (same error signature, same
table, same operation) and classifies them into *actionable* categories:

* ``fixable``      - a mapping change solves it (widen/cast/default/upsert...)
* ``schema_drift`` - source or destination schema changed under us
* ``deprecated``   - the destination/source object no longer exists; candidate
                     for removing the mapping from the next iteration
* ``transient``    - deadlocks, timeouts, connection resets -> retry
* ``ordering``     - FK / dependency problems -> defer or reorder mappings
* ``unknown``      - needs a human

Every classified pattern carries a concrete suggestion, and - where it can
be expressed as a mapping change - a machine readable ``proposed_change``
that the UI/API can apply with one click (strangler-fig iteration loop:
observe -> classify -> adjust mapping -> replay DLQ).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# (regex, category, suggestion, proposed_change-factory)
_CLASSIFIERS = [
    (re.compile(r"string or binary data would be truncated|value too long for type|data too long for column|ORA-12899", re.I),
     "fixable", "Destination column is narrower than the source value. Widen the destination column or add a "
                "truncating transformation (e.g. LEFT(col, n)).",
     lambda g: {"action": "add_transformation", "hint": "LEFT({column}, <n>)"}),
    (re.compile(r"cannot insert the value null|null value in column .* violates not-null|column .* cannot be null|ORA-01400|NOT NULL constraint failed", re.I),
     "fixable", "Source delivers NULL for a NOT NULL destination column. Set a default_value on the column mapping "
                "or relax the constraint.",
     lambda g: {"action": "set_default_value", "hint": "default_value on the affected column"}),
    (re.compile(r"violation of primary key|duplicate key value violates unique|duplicate entry .* for key|ORA-00001|unique constraint|E11000 duplicate key", re.I),
     "fixable", "Row already exists in the destination (replayed event or a snapshot overlap). Switch the mapping to "
                "upsert semantics (sync_mode='upsert').",
     lambda g: {"action": "set_sync_mode", "value": "upsert"}),
    (re.compile(r"invalid object name|relation .* does not exist|table .* doesn't exist|ORA-00942|no such table", re.I),
     "deprecated", "The referenced table does not exist (anymore). If the source/destination object was retired, "
                   "remove the mapping from the working set; otherwise fix the schema/table name.",
     lambda g: {"action": "disable_mapping", "reason": "missing table"}),
    (re.compile(r"invalid column name|column .* does not exist|unknown column|ORA-00904|no column named|has no column", re.I),
     "schema_drift", "A column referenced by the mapping does not exist in the destination: schema drift. Re-scan the "
                     "destination and regenerate the column mapping.",
     lambda g: {"action": "rescan_columns"}),
    (re.compile(r"arithmetic overflow|out of range|numeric value out of range|ORA-01438|value out of range", re.I),
     "fixable", "Numeric precision/scale of the destination column is too small. Widen the type or cast in a transformation.",
     lambda g: {"action": "widen_type"}),
    (re.compile(r"conversion failed|invalid input syntax|incorrect .* value|could not convert|ORA-01722|ORA-01861|datatype mismatch", re.I),
     "fixable", "Type conversion failed. Add an explicit CAST/TRY_CAST transformation or fix the destination type.",
     lambda g: {"action": "add_transformation", "hint": "TRY_CAST({column} AS <type>)"}),
    (re.compile(r"deadlock|lock request time out|lock wait timeout|could not obtain lock|ORA-00060|timeout expired|connection reset|"
                r"communication link failure|server closed the connection|ORA-03113|database is locked", re.I),
     "transient", "Transient infrastructure error. Safe to replay the failed events.",
     lambda g: {"action": "replay"}),
    (re.compile(r"foreign key constraint|violates foreign key|ORA-02291|ORA-02292|a foreign key constraint fails|FOREIGN KEY constraint failed", re.I),
     "ordering", "Referential integrity: the parent row is not (yet) in the destination. Order the mappings parent -> "
                 "child, or replay after the parent mapping caught up.",
     lambda g: {"action": "reorder_mappings"}),
    (re.compile(r"permission denied|the .* permission was denied|access denied|ORA-01031|insufficient privilege", re.I),
     "fixable", "Destination login lacks privileges on the target object.",
     lambda g: {"action": "grant_privileges"}),
    (re.compile(r"key columns .* are not produced by mapping|no primary key found", re.I),
     "fixable", "The mapping does not deliver the key columns needed to route UPDATE/DELETE. Add key_columns to the mapping.",
     lambda g: {"action": "set_key_columns"}),
]


@dataclass
class FailurePattern:
    signature: str
    dest_table: Optional[str]
    source_table: Optional[str]
    operation: Optional[str]
    count: int
    first_seen: str
    last_seen: str
    sample_error: str
    category: str
    suggestion: str
    proposed_change: Dict[str, Any] = field(default_factory=dict)
    systemic: bool = False       # persisted over a long window -> not a one-off
    share: float = 0.0           # share of all failures in the window

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__.copy()


def classify(error: str) -> tuple[str, str, Dict[str, Any]]:
    for rx, category, suggestion, change in _CLASSIFIERS:
        if rx.search(error or ""):
            return category, suggestion, change({})
    return "unknown", "No known remedy; inspect the sample failure payload.", {}


def mine_patterns(groups: List[Dict[str, Any]], systemic_after_hours: float = 6.0) -> List[FailurePattern]:
    total = sum(int(g.get("count", 0)) for g in groups) or 1
    out: List[FailurePattern] = []
    for g in groups:
        category, suggestion, change = classify(g.get("sample_error") or "")
        try:
            first = datetime.fromisoformat(g["first_seen"])
            last = datetime.fromisoformat(g["last_seen"])
            span_h = (last - first).total_seconds() / 3600.0
        except Exception:
            span_h = 0.0
        out.append(FailurePattern(
            signature=g.get("signature") or "", dest_table=g.get("dest_table"), source_table=g.get("source_table"),
            operation=g.get("operation"), count=int(g.get("count", 0)), first_seen=g.get("first_seen", ""),
            last_seen=g.get("last_seen", ""), sample_error=(g.get("sample_error") or "")[:500],
            category=category, suggestion=suggestion, proposed_change=change,
            systemic=span_h >= systemic_after_hours and int(g.get("count", 0)) >= 3,
            share=round(int(g.get("count", 0)) / total, 4),
        ))
    out.sort(key=lambda p: (-p.count, p.dest_table or ""))
    return out


def summarize(patterns: List[FailurePattern]) -> Dict[str, Any]:
    by_cat: Dict[str, int] = {}
    for p in patterns:
        by_cat[p.category] = by_cat.get(p.category, 0) + p.count
    deprecated = sorted({p.dest_table for p in patterns if p.category == "deprecated" and p.dest_table})
    return {
        "patterns": len(patterns),
        "failures": sum(p.count for p in patterns),
        "by_category": by_cat,
        "deprecated_candidates": deprecated,
        "replayable": sum(p.count for p in patterns if p.category == "transient"),
        "systemic": [p.signature for p in patterns if p.systemic],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Applying a proposed change to a mapping (closes the observe -> adjust loop)
# ---------------------------------------------------------------------------
_COLUMN_RX = [
    re.compile(r"column '(?P<col>[^']+)'", re.I),                       # SQL Server: "... into column 'name', table ..."
    re.compile(r'column "(?P<col>[^"]+)"', re.I),                       # PostgreSQL
    re.compile(r"Column '(?P<col>[^']+)' cannot be null", re.I),         # MySQL
    re.compile(r"constraint failed: [\w.]+\.(?P<col>\w+)", re.I),     # SQLite
    re.compile(r'\("(?P<schema>[^"]+)"\."(?P<table>[^"]+)"\."(?P<col>[^"]+)"\)', re.I),  # Oracle ORA-01400
]


def column_from_error(error: str) -> Optional[str]:
    for rx in _COLUMN_RX:
        m = rx.search(error or "")
        if m:
            return m.group("col")
    return None


def apply_proposed_change(mapping, action: str, error: str = "", **params: Any) -> Dict[str, Any]:
    """Mutate ``mapping`` (a ``Mapping`` model) according to a pattern's proposed change.

    Returns a description of what changed. Raises ``ValueError`` when the action
    needs parameters that were not supplied (column, value, expression, key_columns).
    """
    from backend.models.schemas import ColumnMapping
    column = params.get("column") or column_from_error(error)
    if action == "disable_mapping":
        mapping.enabled = False
        return {"mapping_id": mapping.id, "enabled": False}
    if action == "set_sync_mode":
        mapping.sync_mode = params.get("value", "upsert")
        return {"mapping_id": mapping.id, "sync_mode": mapping.sync_mode}
    if action == "set_key_columns":
        keys = params.get("key_columns")
        if not keys:
            raise ValueError("key_columns required")
        mapping.key_columns = list(keys)
        return {"mapping_id": mapping.id, "key_columns": mapping.key_columns}
    if action in ("set_default_value", "add_transformation"):
        if not column:
            raise ValueError("column could not be derived from the error; pass 'column'")
        target = next((cm for cm in mapping.column_mappings if cm.destination_column.lower() == column.lower()), None)
        if target is None:
            target = ColumnMapping(source_column=column, destination_column=column)
            mapping.column_mappings.append(target)
        if action == "set_default_value":
            if "value" not in params:
                raise ValueError("value required for set_default_value")
            target.default_value = params["value"]
            return {"mapping_id": mapping.id, "column": target.destination_column, "default_value": params["value"]}
        expression = params.get("expression")
        if not expression:
            raise ValueError("expression required for add_transformation, e.g. LEFT(\"col\", 50)")
        target.transformation = expression
        target.transformation_type = "custom"
        return {"mapping_id": mapping.id, "column": target.destination_column, "transformation": expression}
    raise ValueError(f"action {action!r} cannot be applied automatically")
