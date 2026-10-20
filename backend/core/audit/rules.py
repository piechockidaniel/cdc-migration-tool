"""Rule engine for alerting.

A *rule* is a boolean expression over a context of health facts
(``pipeline.lag_seconds``, ``failures.rate_1h``, ``source.retention_margin_minutes``,
``snapshot.stalled_seconds`` ...) plus severity, cooldown and the *rule set* it
belongs to. Rule sets bundle the rules for a situation ("mssql-cdc",
"data-loss-guard", "snapshot", ...) so an operator enables a set, not 40 rules.

Expressions are evaluated by a tiny AST-whitelisting evaluator - no ``eval``.
"""
from __future__ import annotations

import ast
import operator
from dataclasses import dataclass, field, fields
from typing import Any, Callable, Dict, List, Optional

_BIN_OPS: Dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv,
    ast.Mod: operator.mod, ast.FloorDiv: operator.floordiv, ast.Pow: operator.pow,
}
_CMP_OPS: Dict[type, Callable[[Any, Any], bool]] = {
    ast.Gt: operator.gt, ast.GtE: operator.ge, ast.Lt: operator.lt, ast.LtE: operator.le,
    ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.In: lambda a, b: a in b, ast.NotIn: lambda a, b: a not in b,
}
FUNCTIONS = {"max": max, "min": min, "abs": abs, "len": len, "sum": sum, "any": any, "all": all, "round": round,
          "float": float, "int": int, "str": str}


class ExpressionError(ValueError):
    pass


# top-level fact groups a condition may reference; anything else is a typo
KNOWN_FACT_GROUPS = frozenset({"pipeline", "source", "failures", "snapshot", "pipelines", "bus", "global"})


def referenced_names(expression: str) -> set:
    tree = ast.parse(expression, mode="eval")
    return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}


class SafeEvaluator:
    """Evaluate ``a.b > 3 and not c`` style expressions against nested dicts."""

    def __init__(self, context: Dict[str, Any]):
        self.context = context

    def evaluate(self, expression: str) -> Any:
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as exc:
            raise ExpressionError(f"invalid expression {expression!r}: {exc}") from exc
        return self._eval(tree.body)

    def _lookup(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Name):
            if node.id in self.context:
                return self.context[node.id]
            if node.id in ("True", "False", "None"):
                return {"True": True, "False": False, "None": None}[node.id]
            if node.id in KNOWN_FACT_GROUPS:
                return None  # fact group absent in this scope (e.g. 'pipeline' in the global context) -> never fires
            raise ExpressionError(f"unknown name {node.id!r}")
        if isinstance(node, ast.Attribute):
            base = self._lookup(node.value)
            if isinstance(base, dict):
                return base.get(node.attr)
            if hasattr(base, node.attr) and not node.attr.startswith("_"):
                return getattr(base, node.attr)
            return None
        if isinstance(node, ast.Subscript):
            base = self._eval(node.value)
            idx = self._eval(node.slice)
            try:
                return base[idx]
            except (KeyError, IndexError, TypeError):
                return None
        raise ExpressionError(f"unsupported lookup {ast.dump(node)}")

    def _eval(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, (ast.Name, ast.Attribute, ast.Subscript)):
            return self._lookup(node)
        if isinstance(node, ast.BoolOp):
            vals = [self._eval(v) for v in node.values]
            return all(vals) if isinstance(node.op, ast.And) else any(vals)
        if isinstance(node, ast.UnaryOp):
            v = self._eval(node.operand)
            if isinstance(node.op, ast.Not):
                return not v
            if isinstance(node.op, ast.USub):
                return -v
            if isinstance(node.op, ast.UAdd):
                return +v
        if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
            left, right = self._eval(node.left), self._eval(node.right)
            if left is None or right is None:
                return None
            try:
                return _BIN_OPS[type(node.op)](left, right)
            except ZeroDivisionError:
                return None
        if isinstance(node, ast.Compare):
            left = self._eval(node.left)
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval(comparator)
                if (left is None or right is None) and not isinstance(op, (ast.Eq, ast.NotEq, ast.Is, ast.IsNot)):
                    return False  # missing facts never fire an ordering comparison
                if isinstance(op, (ast.Is, ast.IsNot)):
                    ok = (left is right) if isinstance(op, ast.Is) else (left is not right)
                    if not ok:
                        return False
                    left = right
                    continue
                try:
                    if not _CMP_OPS[type(op)](left, right):
                        return False
                except TypeError:
                    return False
                left = right
            return True
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in FUNCTIONS:
            args = [self._eval(a) for a in node.args]
            if any(a is None for a in args):
                return None
            return FUNCTIONS[node.func.id](*args)
        if isinstance(node, (ast.List, ast.Tuple)):
            return [self._eval(e) for e in node.elts]
        if isinstance(node, ast.IfExp):
            return self._eval(node.body) if self._eval(node.test) else self._eval(node.orelse)
        raise ExpressionError(f"unsupported syntax: {ast.dump(node)[:80]}")


@dataclass
class Rule:
    id: str
    name: str
    condition: str
    severity: str = "warning"          # info | warning | error | critical
    description: str = ""
    rule_set: str = "custom"
    cooldown_seconds: int = 900        # do not re-notify a firing alert more often than this
    for_seconds: int = 0               # condition must hold this long before firing (flap protection)
    enabled: bool = True
    scope: str = "global"              # global | per_table | per_pipeline | per_snapshot
    labels: Dict[str, str] = field(default_factory=dict)
    message: str = ""                  # optional template, e.g. "lag {pipeline.lag_seconds}s"

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__.copy()

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Rule":
        names = {f.name for f in fields(cls)}
        known = {k: v for k, v in data.items() if k in names}
        return cls(**known)


# ---------------------------------------------------------------------------
# Built-in rule sets - the "most frequent issues" catalogue
# ---------------------------------------------------------------------------
BUILTIN_RULES: List[Rule] = [
    # pipeline health
    Rule("pipeline_not_running", "Pipeline stopped unexpectedly", "pipeline.expected_running and not pipeline.running",
         "critical", "A pipeline that should be running is not.", "pipeline-health", scope="per_pipeline"),
    Rule("pipeline_lag_high", "Replication lag high", "pipeline.lag_seconds > 300", "warning",
         "Source commit -> destination apply lag over 5 minutes.", "pipeline-health", scope="per_pipeline"),
    Rule("pipeline_lag_critical", "Replication lag critical", "pipeline.lag_seconds > 1800", "critical",
         "Lag over 30 minutes: destination is stale.", "pipeline-health", scope="per_pipeline"),
    Rule("pipeline_queue_backlog", "Apply queue backlog", "pipeline.queue_depth > 50000", "warning",
         "Reader is far ahead of the applier: destination too slow or blocked.", "pipeline-health", scope="per_pipeline"),
    Rule("pipeline_silent_source", "No changes received for a long time", "pipeline.running and pipeline.idle_seconds > 3600 and pipeline.expect_traffic",
         "warning", "A normally busy source produced nothing for an hour - capture job dead? network?", "pipeline-health",
         scope="per_pipeline"),
    Rule("pipeline_connection_errors", "Connection errors", "pipeline.connection_errors_5m >= 3", "error",
         "Repeated connection failures in the last 5 minutes.", "pipeline-health", scope="per_pipeline", cooldown_seconds=300),
    # failures
    Rule("failure_rate_high", "Failure rate high", "failures.total_1h >= 10 and failures.rate_1h > 0.01", "error",
         "More than 1% of changes failed in the last hour.", "failure-patterns"),
    Rule("failure_burst", "Failure burst", "failures.total_5m >= 50", "critical",
         "50+ failures in 5 minutes - probably schema drift or destination down.", "failure-patterns", cooldown_seconds=300),
    Rule("failure_new_signature", "New failure pattern", "failures.new_signatures_1h > 0", "warning",
         "A failure signature that was never seen before appeared.", "failure-patterns", cooldown_seconds=1800),
    Rule("failure_systemic", "Systemic failure pattern", "failures.systemic_patterns > 0", "error",
         "The same failure keeps recurring for hours - needs a mapping change.", "failure-patterns", cooldown_seconds=3600),
    Rule("dlq_growing", "Dead-letter queue growing", "failures.unresolved > 1000", "warning",
         "Unresolved failed events are piling up.", "failure-patterns", cooldown_seconds=3600),
    # data-loss guard (the most important set)
    Rule("retention_margin_low", "CDC retention margin low",
         "source.retention_margin_minutes != None and source.retention_margin_minutes < 120", "critical",
         "Committed position is close to being purged by the source retention (CDC cleanup / binlog expire / WAL / oplog). "
         "Silent data loss is imminent.", "data-loss-guard", scope="per_pipeline", cooldown_seconds=600),
    Rule("position_behind_min", "Committed position already purged",
         "source.position_before_min == True", "critical",
         "The committed position is older than the oldest available change at the source - data was lost; "
         "a new snapshot is required.", "data-loss-guard", scope="per_pipeline", cooldown_seconds=600),
    Rule("capture_job_errors", "Source capture reports errors", "source.capture_errors > 0", "error",
         "The source capture mechanism (CDC capture job / slot / LogMiner) reports errors.", "data-loss-guard",
         scope="per_pipeline"),
    Rule("slot_wal_retention", "Replication slot retaining too much WAL", "source.slot_lag_bytes > 5368709120", "error",
         "PostgreSQL slot retains > 5 GB of WAL - disk-full risk on the source.", "data-loss-guard", scope="per_pipeline"),
    # snapshots
    Rule("snapshot_failed", "Snapshot job failed", "snapshot.state == 'failed'", "error", "", "snapshot", scope="per_snapshot"),
    Rule("snapshot_stalled", "Snapshot job stalled", "snapshot.state == 'running' and snapshot.stalled_seconds > 600", "warning",
         "No chunk completed for 10 minutes.", "snapshot", scope="per_snapshot"),
    Rule("snapshot_slow_chunks", "Snapshot chunks slow", "snapshot.state == 'running' and snapshot.avg_chunk_seconds > 60", "warning",
         "Average chunk takes > 60 s: reduce chunk size or check source load.", "snapshot", scope="per_snapshot"),
    Rule("snapshot_error_rate", "Snapshot chunk errors", "snapshot.chunk_errors > 5", "error", "", "snapshot", scope="per_snapshot"),
    # unsafe configuration
    Rule("unsafe_key_heuristic", "Mapping relies on guessed key columns", "pipeline.unsafe_key_tables > 0", "warning",
         "UPDATE/DELETE routing uses a guessed key - define key_columns.", "configuration", scope="per_pipeline",
         cooldown_seconds=86400),
    Rule("prerequisite_failed", "Prerequisite check failed", "source.prerequisites_failed > 0", "error",
         "A prerequisite for CDC on the source is not met.", "configuration", scope="per_pipeline", cooldown_seconds=3600),
]

RULE_SETS: Dict[str, str] = {
    "pipeline-health": "Liveness and throughput of running pipelines",
    "failure-patterns": "Failed changes, DLQ growth and recurring error signatures",
    "data-loss-guard": "Source retention vs. committed position, capture errors",
    "snapshot": "Initial load jobs",
    "configuration": "Unsafe or incomplete configuration",
}


def builtin_rules_by_set(rule_set: Optional[str] = None) -> List[Rule]:
    return [r for r in BUILTIN_RULES if rule_set is None or r.rule_set == rule_set]
