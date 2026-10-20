import time

import pytest

from backend.connectors.base import ChangeEvent, Operation, Position
from backend.core.audit.alerting import Alert, AlertManager, BusChannel, EmailChannel, LogChannel, WebhookChannel
from backend.core.audit.patterns import classify, mine_patterns, summarize
from backend.core.audit.rules import BUILTIN_RULES, ExpressionError, Rule, SafeEvaluator, builtin_rules_by_set
from backend.core.audit.store import AuditStore
from backend.core.event_bus import EventBus


def ev(i=1, op=Operation.INSERT):
    return ChangeEvent(source="src", schema="dbo", table="T", operation=op, position=Position("fake", i),
                       after={"id": i}, key={"id": i}, event_id=f"e{i}")


# ---------------------------------------------------------------------------
# audit store
# ---------------------------------------------------------------------------
def test_audit_store_records_queries_and_stats(audit_store: AuditStore):
    audit_store.record("pipeline", "pipeline", "started", severity="info", source="src")
    audit_store.record_batch("src", "dbo.T", "INSERT", applied=90, failed=10, duration_s=0.5, last_position={"v": 1})
    audit_store.record_batch("src", "dbo.T", "DELETE", applied=5, failed=0, duration_s=0.1)
    fid = audit_store.record_failure(ev(1), "dbo.T", "Cannot insert the value NULL into column 'name'", "sig-null")
    assert fid >= 1
    rows = audit_store.query(category="batch")
    assert len(rows) == 2 and rows[0]["payload"]["applied"] == 5
    assert audit_store.query(severity="warning")[0]["status"] == "partial"
    st = audit_store.stats(hours=1)
    assert st["applied"] == 95 and st["failed"] == 10 and st["failures_total"] == 1
    assert {t["operation"] for t in st["tables"]} == {"INSERT", "DELETE"}
    assert audit_store.query(table="dbo.T", limit=1)


def test_audit_store_event_level_and_buffering(tmp_path):
    s = AuditStore(tmp_path / "a.sqlite3", audit_level="all", flush_interval_s=100, buffer_size=1000)
    s.record_event(ev(1), "processed", "dbo.T")
    assert s.query(category="cdc_event")  # query flushes
    s2 = AuditStore(tmp_path / "b.sqlite3", audit_level="summary", flush_interval_s=100)
    s2.record_event(ev(1), "processed", "dbo.T")
    s2.record_event(ev(2), "failed", "dbo.T", error="x")
    assert len(s2.query(category="cdc_event")) == 1
    s.close(); s2.close()


def test_dlq_grouping_resolution_and_purge(audit_store: AuditStore):
    for i in range(5):
        audit_store.record_failure(ev(i), "dbo.T", "Violation of PRIMARY KEY constraint", "sig-pk")
    for i in range(2):
        audit_store.record_failure(ev(10 + i, Operation.UPDATE), "dbo.U", "deadlock victim", "sig-dl")
    groups = audit_store.failure_groups(hours=1)
    assert groups[0]["signature"] == "sig-pk" and groups[0]["count"] == 5 and groups[1]["count"] == 2
    assert len(audit_store.failures(table="dbo.U")) == 2
    f = audit_store.failure(groups[1]["sample_id"])
    assert f["payload"]["operation"] == "UPDATE"
    assert audit_store.resolve_failures(signature="sig-pk", resolution="switched to upsert") == 5
    assert len(audit_store.failures(resolved=False)) == 2
    audit_store.bump_attempt(f["id"], "still failing")
    assert audit_store.failure(f["id"])["attempts"] == 2
    assert audit_store.purge(audit_days=0, failures_days=0)["failures_deleted"] == 5


# ---------------------------------------------------------------------------
# pattern mining
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("msg,category", [
    ("String or binary data would be truncated in table 'x'", "fixable"),
    ("Cannot insert the value NULL into column 'n'", "fixable"),
    ("Violation of PRIMARY KEY constraint 'PK'", "fixable"),
    ("Invalid object name 'dbo.Old'", "deprecated"),
    ("relation \"old_table\" does not exist", "deprecated"),
    ("Invalid column name 'NewCol'", "schema_drift"),
    ("Transaction (Process ID 5) was deadlocked", "transient"),
    ("The INSERT statement conflicted with the FOREIGN KEY constraint", "ordering"),
    ("ORA-12899: value too large for column", "fixable"),
    ("something nobody has seen", "unknown"),
])
def test_classify(msg, category):
    assert classify(msg)[0] == category


def test_mine_patterns_and_summary():
    groups = [
        {"signature": "a", "dest_table": "dbo.T", "source_table": "s.T", "operation": "INSERT", "count": 90,
         "first_seen": "2024-01-01T00:00:00+00:00", "last_seen": "2024-01-01T12:00:00+00:00",
         "sample_error": "Violation of PRIMARY KEY constraint"},
        {"signature": "b", "dest_table": "dbo.Old", "source_table": "s.Old", "operation": "UPDATE", "count": 10,
         "first_seen": "2024-01-01T00:00:00+00:00", "last_seen": "2024-01-01T00:10:00+00:00",
         "sample_error": "Invalid object name 'dbo.Old'"},
    ]
    patterns = mine_patterns(groups)
    assert patterns[0].category == "fixable" and patterns[0].systemic and patterns[0].share == 0.9
    assert patterns[0].proposed_change == {"action": "set_sync_mode", "value": "upsert"}
    assert patterns[1].category == "deprecated" and not patterns[1].systemic
    s = summarize(patterns)
    assert s["deprecated_candidates"] == ["dbo.Old"] and s["by_category"] == {"fixable": 90, "deprecated": 10}
    assert s["systemic"] == ["a"]


# ---------------------------------------------------------------------------
# rule expressions
# ---------------------------------------------------------------------------
def test_safe_evaluator_semantics():
    ctx = {"pipeline": {"lag_seconds": 400, "running": True, "tags": ["a"]}, "failures": {"rate_1h": 0.02}, "x": None}
    e = SafeEvaluator(ctx)
    assert e.evaluate("pipeline.lag_seconds > 300 and pipeline.running")
    assert not e.evaluate("pipeline.lag_seconds > 500")
    assert e.evaluate("failures.rate_1h * 100 > 1")
    assert e.evaluate("'a' in pipeline.tags") and e.evaluate("max(1, pipeline.lag_seconds) == 400")
    assert e.evaluate("pipeline['lag_seconds'] >= 400")
    assert not e.evaluate("pipeline.missing > 1")          # missing fact never fires
    assert not e.evaluate("x > 1")
    assert e.evaluate("x == None")
    with pytest.raises(ExpressionError):
        e.evaluate("__import__('os').system('x')")
    with pytest.raises(ExpressionError):
        e.evaluate("pipeline.lag_seconds >")
    with pytest.raises(ExpressionError):
        e.evaluate("open('/etc/passwd')")


def test_builtin_rules_are_valid_expressions():
    ctx = {"pipeline": {}, "source": {}, "failures": {}, "snapshot": {}}
    for r in BUILTIN_RULES:
        SafeEvaluator(ctx).evaluate(r.condition)   # must not raise
    assert builtin_rules_by_set("data-loss-guard")
    assert len({r.id for r in BUILTIN_RULES}) == len(BUILTIN_RULES)


# ---------------------------------------------------------------------------
# alert manager
# ---------------------------------------------------------------------------
class Recorder:
    name = "rec"

    def __init__(self):
        self.sent = []

    def send(self, alert, resolved=False):
        self.sent.append((alert.rule_id, alert.scope_key, resolved))


def test_alert_lifecycle_cooldown_and_resolution(sqlite_store):
    rec = Recorder()
    rules = [Rule("lag", "lag", "pipeline.lag_seconds > 10", "warning", scope="per_pipeline", cooldown_seconds=60),
             Rule("glob", "global", "failures.total > 5", "error", scope="global", cooldown_seconds=0)]
    am = AlertManager(rules, channels=[rec], state_store=sqlite_store)
    ctx = {"global": {"failures": {"total": 1}}, "pipeline:p1": {"pipeline": {"lag_seconds": 50}}, "pipeline:p2": {"pipeline": {"lag_seconds": 1}}}
    fired = am.evaluate(ctx)
    assert [a.key for a in fired] == ["lag|pipeline:p1"]
    am.evaluate(ctx)                                   # within cooldown -> no repeat
    assert len(rec.sent) == 1
    ctx["pipeline:p1"]["pipeline"]["lag_seconds"] = 0
    am.evaluate(ctx)
    assert rec.sent[-1] == ("lag", "pipeline:p1", True) and not am.active
    assert am.history[-1].status == "resolved"
    # global rule fires each evaluation with cooldown 0
    ctx["global"]["failures"]["total"] = 10
    am.evaluate(ctx); am.evaluate(ctx)
    assert sum(1 for s in rec.sent if s[0] == "glob" and not s[2]) == 2
    # state persisted & reloaded
    am2 = AlertManager(rules, channels=[rec], state_store=sqlite_store)
    assert "glob|global" in am2.active


def test_alert_for_seconds_and_rule_sets_and_errors():
    rec = Recorder()
    rules = [Rule("flap", "flap", "pipeline.lag_seconds > 1", scope="per_pipeline", for_seconds=3600),
             Rule("off", "off", "True", scope="global", rule_set="snapshot"),
             Rule("bad", "bad", "pipeline.lag_seconds >>> 1", scope="per_pipeline")]
    am = AlertManager(rules, channels=[rec], enabled_rule_sets=["custom"])
    am.evaluate({"global": {}, "pipeline:p": {"pipeline": {"lag_seconds": 5}}})
    assert am.active["flap|pipeline:p"].status == "pending" and rec.sent == []
    assert "off|global" not in am.active
    assert "bad" in am.evaluation_errors
    snap = am.snapshot()
    assert snap["enabled_rule_sets"] == ["custom"] and snap["active"][0]["status"] == "pending"
    am.upsert_rule(Rule("new", "new", "True", scope="global", rule_set="custom"))
    am.evaluate({"global": {}})
    assert ("new", "global", False) in rec.sent
    assert am.remove_rule("new") and not am.remove_rule("nope")


def test_channels():
    bus = EventBus()
    sub = bus.subscribe("alerts")
    a = Alert("r", "global", "error", "name", "msg")
    BusChannel(bus).send(a)
    assert sub.get(0.1).payload["rule_id"] == "r"
    LogChannel().send(a, resolved=True)
    calls = []
    WebhookChannel("http://x", opener=lambda req, timeout: calls.append((req.full_url, req.data))).send(a)
    assert calls and b'"status": "firing"' in calls[0][1]

    class SMTP:
        sent = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def starttls(self):
            pass

        def login(self, u, p):
            pass

        def send_message(self, msg):
            SMTP.sent.append(msg["Subject"])
    EmailChannel("h", 25, "a@b", ["c@d"], username="u", password="p", smtp_factory=SMTP).send(a)
    assert SMTP.sent and "[ERROR]" in SMTP.sent[0]


def test_alert_manager_background_loop():
    rec = Recorder()
    am = AlertManager([Rule("always", "always", "True", scope="global", cooldown_seconds=0)], channels=[rec])
    am.start(lambda: {"global": {}}, interval_s=0.05)
    time.sleep(0.3)
    am.stop()
    assert len(rec.sent) >= 2 and am.last_evaluation


def test_apply_proposed_change_mutates_mapping():
    from backend.core.audit.patterns import apply_proposed_change, column_from_error
    from tests.fakes import make_mapping
    assert column_from_error("Cannot insert the value NULL into column 'name', table 'db.dbo.t'") == "name"
    assert column_from_error('null value in column "email" violates not-null constraint') == "email"
    assert column_from_error("NOT NULL constraint failed: customers.city") == "city"
    m = make_mapping()
    assert apply_proposed_change(m, "set_default_value", "Cannot insert the value NULL into column 'name'", value="n/a")["column"] == "name"
    assert next(cm for cm in m.column_mappings if cm.destination_column == "name").default_value == "n/a"
    assert apply_proposed_change(m, "set_sync_mode")["sync_mode"] == "upsert"
    assert apply_proposed_change(m, "add_transformation", "", column="email", expression='LEFT("Email", 5)')["transformation"]
    assert apply_proposed_change(m, "set_key_columns", key_columns=["id"])["key_columns"] == ["id"]
    assert apply_proposed_change(m, "disable_mapping")["enabled"] is False and m.enabled is False
    with pytest.raises(ValueError):
        apply_proposed_change(m, "set_default_value", "no column here")
    with pytest.raises(ValueError):
        apply_proposed_change(m, "replay")


def test_missing_fact_group_never_fires_but_typos_are_errors():
    e = SafeEvaluator({"failures": {"total_1h": 1}})
    assert e.evaluate("pipeline.lag_seconds > 1") is False          # 'pipeline' absent in the global scope
    assert e.evaluate("pipeline == None") is True
    with pytest.raises(ExpressionError):
        e.evaluate("pipline.lag_seconds > 1")                         # typo
