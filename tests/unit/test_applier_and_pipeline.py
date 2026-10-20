import time

import pytest

from backend.connectors.base import ChangeEvent, Operation, Position
from backend.core.applier import RowApplier, error_signature
from backend.core.duckdb_processor import DuckDBProcessor
from backend.core.event_bus import EventBus, KafkaTransport
from backend.core.pipeline import ChangePipeline, PipelineRegistry
from backend.models.schemas import AutoGenerateMode, ColumnMapping, Mapping, MappingType
from tests.fakes import FakeConnector, make_mapping, sqlite_destination


class MemoryPositions:
    def __init__(self):
        self.data = {}

    def get_position(self, key):
        return self.data.get(key)

    def set_position(self, key, kind, value, extra=None):
        self.data[key] = {"kind": kind, "value": value, **(extra or {})}


def ev(op, after=None, before=None, pos=1, table="Customers"):
    return ChangeEvent(source="src", schema="dbo", table=table, operation=op, position=Position("fake", pos),
                       after=after, before=before, key={"CustomerID": (after or before or {}).get("CustomerID")})


# ---------------------------------------------------------------------------
# error signatures
# ---------------------------------------------------------------------------
def test_error_signature_normalises_variable_parts():
    a = error_signature("String or binary data would be truncated in table 'db.dbo.customers', column 'name'. Truncated value: 'abcdef'.")
    b = error_signature("String or binary data would be truncated in table 'db.dbo.customers', column 'name'. Truncated value: 'zzz'.")
    assert a == b
    assert error_signature("Violation of PRIMARY KEY constraint 'PK_x'. The duplicate key value is (42).") == \
        error_signature("Violation of PRIMARY KEY constraint 'PK_x'. The duplicate key value is (7).")


# ---------------------------------------------------------------------------
# applier
# ---------------------------------------------------------------------------
def test_applier_resolves_keys_from_destination_pk_and_upserts():
    dest = sqlite_destination()
    applier = RowApplier(dest)
    applier.register_mappings([make_mapping()])
    r = applier.apply([
        ev(Operation.INSERT, {"CustomerID": 1, "Name": "Ann", "Email": "a@x", "City": "Oslo"}, pos=1),
        ev(Operation.INSERT, {"CustomerID": 2, "Name": "Bob", "Email": "b@x", "City": "Rome"}, pos=2),
        ev(Operation.UPDATE, {"CustomerID": 1, "Name": "Ann2", "Email": "a@x", "City": "Oslo"}, pos=3),
        ev(Operation.DELETE, before={"CustomerID": 2, "Name": "Bob", "Email": "b@x", "City": "Rome"}, pos=4),
        ev(Operation.INSERT, {"CustomerID": 1, "Name": "Ann2", "Email": "a@x", "City": "Oslo"}, pos=5),  # replay -> idempotent
    ])
    assert r.applied == 5 and r.failed == 0
    rt = applier.runtime_for_key("dbo.Customers")
    assert rt.key_columns == ["id"] and rt.key_source == "destination_pk"
    rows = dest.query("SELECT id, name FROM customers ORDER BY id")
    assert rows == [{"id": 1, "name": "Ann2"}]


def test_applier_uses_mapping_key_columns_and_composite_keys():
    dest = sqlite_destination()
    m = Mapping(id="o", name="o", mapping_type=MappingType.TABLE, source_schema="dbo", source_table="Orders",
                destination_schema="", destination_table="orders", key_columns=["order_id", "line"],
                column_mappings=[ColumnMapping(source_column="OrderId", destination_column="order_id"),
                                 ColumnMapping(source_column="Line", destination_column="line"),
                                 ColumnMapping(source_column="Amount", destination_column="amount")])
    applier = RowApplier(dest)
    applier.register_mappings([m])
    r = applier.apply([ev(Operation.INSERT, {"OrderId": 1, "Line": 1, "Amount": 5.0}, table="Orders"),
                       ev(Operation.INSERT, {"OrderId": 1, "Line": 2, "Amount": 6.0}, table="Orders", pos=2),
                       ev(Operation.UPDATE, {"OrderId": 1, "Line": 2, "Amount": 7.5}, table="Orders", pos=3),
                       ev(Operation.DELETE, before={"OrderId": 1, "Line": 1, "Amount": 5.0}, table="Orders", pos=4)])
    assert r.applied == 4
    assert dest.query("SELECT order_id, line, amount FROM orders") == [{"order_id": 1, "line": 2, "amount": 7.5}]


def test_applier_column_controls_defaults_ignore_autogenerate_and_duckdb_expressions():
    dest = sqlite_destination()
    m = make_mapping(transformations=True)
    m.column_mappings[2] = ColumnMapping(source_column="Email", destination_column="email", default_value="n/a")
    m.column_mappings.append(ColumnMapping(destination_column="updated", auto_generate=AutoGenerateMode.ON_INSERT,
                                           auto_generate_expression="'gen'"))
    applier = RowApplier(dest, duckdb=DuckDBProcessor())
    applier.register_mappings([m])
    r = applier.apply([ev(Operation.INSERT, {"CustomerID": 1, "Name": "Ann", "Email": None, "City": "oslo"})])
    assert r.failed == 0, r.failures
    row = dest.query("SELECT * FROM customers")[0]
    assert row["city"] == "OSLO" and row["email"] == "n/a" and row["updated"] == "gen"


def test_applier_isolates_poison_rows_and_records_failures(audit_store):
    dest = sqlite_destination("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT);")
    applier = RowApplier(dest, audit=audit_store)
    applier.register_mappings([make_mapping()])
    events = [ev(Operation.INSERT, {"CustomerID": i, "Name": None if i == 2 else f"n{i}", "Email": "", "City": ""}, pos=i) for i in range(1, 5)]
    r = applier.apply(events)
    assert r.applied == 3 and r.failed == 1
    assert r.failures[0].event.after["CustomerID"] == 2
    assert "NOT NULL" in r.failures[0].error
    assert dest.row_count("", "customers") == 3
    dlq = audit_store.failures()
    assert len(dlq) == 1 and dlq[0]["dest_table"] == "main.customers" and dlq[0]["payload"]["after"]["CustomerID"] == 2


def test_applier_skips_unmapped_and_disabled_operations():
    dest = sqlite_destination()
    m = make_mapping(sync_deletes=False)
    applier = RowApplier(dest)
    applier.register_mappings([m])
    r = applier.apply([ev(Operation.DELETE, before={"CustomerID": 1}), ev(Operation.INSERT, {"CustomerID": 1}, table="Unknown")])
    assert r.applied == 0 and r.skipped == 2


def test_applier_heuristic_key_is_flagged(audit_store):
    dest = sqlite_destination("CREATE TABLE customers (id INTEGER, name TEXT, email TEXT, city TEXT);")  # no PK
    applier = RowApplier(dest, audit=audit_store)
    applier.register_mappings([make_mapping()])
    rt = applier.runtime_for_key("dbo.Customers")
    assert applier.resolve_keys(rt) == ["id"] and rt.key_source == "heuristic"
    assert "main.customers" in applier.unsafe_key_warnings
    assert audit_store.query(category="warning")


# ---------------------------------------------------------------------------
# event bus
# ---------------------------------------------------------------------------
def test_event_bus_patterns_backpressure_and_observers():
    bus = EventBus(default_maxsize=2)
    sub = bus.subscribe("changes.*", name="s")
    seen = []
    bus.on("changes.p1", lambda m: seen.append(m.payload))
    assert bus.publish("changes.p1", 1) == 1
    assert bus.publish("other", 2) == 0
    bus.publish("changes.p1", 3)
    assert bus.publish("changes.p1", 4, block=False) == 0   # queue full -> dropped, not blocked
    assert sub.dropped == 1 and seen == [1, 3, 4]
    batch = sub.get_batch(10, timeout=0.1)
    assert [m.payload for m in batch] == [1, 3]
    stats = bus.stats()
    assert stats["subscriptions"][0]["delivered"] == 2
    sub.close()
    assert bus.stats()["subscriptions"] == []


def test_kafka_transport_serialises_events():
    class P:
        def __init__(self):
            self.msgs = []

        def produce(self, topic, key=None, value=None, headers=None):
            self.msgs.append((topic, key, value))

        def poll(self, t):
            pass

        def flush(self, t=None):
            pass
    p = P()
    bus = EventBus(transports=[KafkaTransport("b:9092", producer=p)])
    bus.subscribe("changes.*")
    bus.publish("changes.x", ev(Operation.INSERT, {"CustomerID": 1}), key="dbo.Customers")
    bus.publish("alerts", {"a": 1})  # not mirrored (pattern)
    assert len(p.msgs) == 1 and p.msgs[0][0] == "cdcsync.changes.x" and b'"operation": "INSERT"' in p.msgs[0][2]


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------
def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


def test_pipeline_end_to_end_commits_positions_after_apply(audit_store):
    src = FakeConnector({"id": "src"})
    dest = sqlite_destination()
    positions = MemoryPositions()
    bus = EventBus()
    p = ChangePipeline("p1", src, dest, [make_mapping()], positions, audit=audit_store, bus=bus, batch_size=10, batch_wait_s=0.05)
    statuses = []
    p.add_listener(lambda e, s, err: statuses.append(s))
    p.start()
    assert src.stream_started.wait(2)
    for i in range(1, 26):
        src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": i, "Name": f"n{i}", "Email": "", "City": ""})
    src.script("dbo", "Customers", Operation.DELETE, before={"CustomerID": 3})
    assert wait_for(lambda: p.events_applied == 26)
    assert dest.row_count("", "customers") == 24
    assert positions.data["p1:__source__"]["value"] == 26          # committed only after apply
    assert src.confirmed and src.confirmed[-1].value == 26
    assert wait_for(lambda: statuses.count("processed") == 26)   # listeners run after the counters are updated
    st = p.status()
    assert st["state"] == "running" and st["events_read"] == 26 and st["batches"] >= 1
    p.stop()
    assert p.state == "stopped"
    assert audit_store.query(category="batch")
    assert audit_store.stats(1)["applied"] == 26


def test_pipeline_resumes_from_committed_position():
    src = FakeConnector({"id": "src"})
    dest = sqlite_destination()
    positions = MemoryPositions()
    positions.set_position("p2:__source__", "fake", 2)
    for i in range(1, 5):
        src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": i, "Name": "", "Email": "", "City": ""})
    p = ChangePipeline("p2", src, dest, [make_mapping()], positions, bus=EventBus(), batch_wait_s=0.05)
    p.start()
    assert wait_for(lambda: p.events_applied == 2)
    assert [r["id"] for r in dest.query("SELECT id FROM customers ORDER BY id")] == [3, 4]
    p.stop()


def test_pipeline_pause_resume_and_failures_do_not_block_stream(audit_store):
    src = FakeConnector({"id": "src"})
    dest = sqlite_destination("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT);")
    positions = MemoryPositions()
    p = ChangePipeline("p3", src, dest, [make_mapping()], positions, audit=audit_store, bus=EventBus(), batch_wait_s=0.05)
    p.start()
    p.pause()
    assert p.state == "paused"
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 1, "Name": None, "Email": "", "City": ""})
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 2, "Name": "ok", "Email": "", "City": ""})
    time.sleep(0.3)
    assert p.events_applied == 0
    p.resume()
    assert wait_for(lambda: p.events_applied + p.events_failed == 2)
    assert p.events_failed == 1 and positions.data["p3:__source__"]["value"] == 2  # failed event went to DLQ, position advanced
    assert len(audit_store.failures()) == 1
    ctx = p.health_context()
    assert ctx["pipeline"]["running"] and ctx["pipeline"]["events_failed"] == 1
    p.stop()


def test_pipeline_registry_rejects_duplicate_running():
    reg = PipelineRegistry()
    src = FakeConnector({"id": "src"})
    p = ChangePipeline("dup", src, sqlite_destination(), [make_mapping()], MemoryPositions(), bus=EventBus())
    reg.add(p)
    p.start()
    with pytest.raises(RuntimeError):
        reg.add(ChangePipeline("dup", src, sqlite_destination(), [make_mapping()], MemoryPositions(), bus=EventBus()))
    reg.stop_all()
    assert p.state == "stopped"


def test_pipeline_start_failure_sets_error_state():
    src = FakeConnector({"id": "src"})
    src.fail_connect = True
    p = ChangePipeline("bad", src, sqlite_destination(), [make_mapping()], MemoryPositions(), bus=EventBus())
    with pytest.raises(RuntimeError):
        p.start()
    assert p.state == "error" and "cannot connect" in p.last_error


def test_pipeline_adopts_legacy_lsn_position():
    class MSSQLLike(FakeConnector):
        engine = "mssql"
    src = MSSQLLike({"id": "ms"})
    positions = MemoryPositions()
    positions.set_position("dbo.Customers", "mssql_lsn", "00aa")          # 1.x lsn_state.json layout
    p = ChangePipeline("ws_1", src, sqlite_destination(), [make_mapping()], positions, bus=EventBus())
    p.load_positions()
    assert p.positions["dbo.Customers"].value == "00aa"
    assert p.position_scope == "table"


def test_applier_runs_duckdb_script_per_batch():
    dest = sqlite_destination()
    m = make_mapping()
    m.use_duckdb_transformation = True
    m.duckdb_script_content = """
        -- keep only customers with an email, upper-case the city
        CREATE OR REPLACE TEMP TABLE t AS SELECT "CustomerID", "Name", "Email", upper("City") AS "City" FROM {source_table} WHERE "Email" <> '';
        SELECT * FROM t;
    """
    applier = RowApplier(dest, duckdb=DuckDBProcessor())
    applier.register_mappings([m])
    r = applier.apply([ev(Operation.INSERT, {"CustomerID": 1, "Name": "a", "Email": "a@x", "City": "oslo"}, pos=1),
                       ev(Operation.INSERT, {"CustomerID": 2, "Name": "b", "Email": "", "City": "rome"}, pos=2)])
    assert r.failed == 0
    assert dest.query("SELECT id, city FROM customers ORDER BY id") == [{"id": 1, "city": "OSLO"}]   # row 2 filtered by the script
