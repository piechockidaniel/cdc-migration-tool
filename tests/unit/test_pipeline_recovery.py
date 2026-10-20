"""Pipeline failure/recovery paths and the DuckDB staging processor."""
from datetime import date, datetime
from decimal import Decimal

import pytest

from backend.connectors.base import ConnectorError, Operation
from backend.core.duckdb_processor import DuckDBProcessor, infer_duckdb_type, quote_ident
from backend.core.event_bus import EventBus
from backend.core.pipeline import ChangePipeline
from backend.models.schemas import ColumnMapping, Mapping, MappingType
from tests.fakes import FakeConnector, make_mapping, sqlite_destination
from tests.unit.test_applier_and_pipeline import MemoryPositions, wait_for


# ---------------------------------------------------------------------------
# DuckDB processor
# ---------------------------------------------------------------------------
def test_infer_duckdb_types_and_quoting():
    assert infer_duckdb_type(None) == "VARCHAR"
    assert infer_duckdb_type(True) == "BOOLEAN"
    assert infer_duckdb_type(3) == "BIGINT"
    assert infer_duckdb_type(1.5) == "DOUBLE"
    assert infer_duckdb_type(Decimal("1.5")) == "DECIMAL(38, 10)"
    assert infer_duckdb_type(datetime.now()) == "TIMESTAMP"
    assert infer_duckdb_type(date.today()) == "DATE"
    assert infer_duckdb_type(b"x") == "BLOB"
    assert infer_duckdb_type("s") == "VARCHAR"
    assert quote_ident('a"b') == '"a""b"'
    assert DuckDBProcessor.quote("x") == '"x"'


def test_duckdb_processor_stage_transform_and_sql_generators():
    p = DuckDBProcessor()
    p.create_staging_table("stg", {"id": "BIGINT", "first": "VARCHAR", "last": "VARCHAR", "amount": "DOUBLE"})
    p.load_data("stg", [])  # no-op
    p.load_data("stg", [{"id": 1, "first": "Ann", "last": "Lee", "amount": 2.5},
                        {"id": 2, "first": "Bob", "last": "Ray", "amount": 4.0}])
    m = Mapping(id="m", name="m", mapping_type=MappingType.TABLE, destination_schema="dbo", destination_table="t",
                source_schema="dbo", source_table="stg",
                column_mappings=[ColumnMapping(source_column="id", destination_column="id"),
                                 ColumnMapping(source_column="first", destination_column="full_name",
                                               transformation="first || ' ' || last"),
                                 ColumnMapping(source_column="amount", destination_column="doubled", transformation="amount * 2")])
    rows = p.transform_data("stg", m)
    assert rows == [{"id": 1, "full_name": "Ann Lee", "doubled": 5.0}, {"id": 2, "full_name": "Bob Ray", "doubled": 8.0}]
    assert p.execute_query("SELECT count(*) AS n FROM stg") == [{"n": 2}]
    assert p.execute_query("CREATE TABLE other (x INT)") == []
    with pytest.raises(Exception):
        p.execute_query("SELECT * FROM missing_table")
    with pytest.raises(Exception):
        p.transform_data("missing", m)
    with pytest.raises(Exception):
        p.load_data("missing", [{"a": 1}])
    with pytest.raises(Exception):
        p.create_staging_table("bad", {"x": "NOT A TYPE"})

    sql, params = DuckDBProcessor.generate_insert_sql("s", "t", {"a": 1, "b": "x"})
    assert sql == "INSERT INTO s.t (a, b) VALUES (?, ?)" and params == (1, "x")
    sql, params = DuckDBProcessor.generate_update_sql("s", "t", {"a": 1, "b": "x"}, ["a"])
    assert sql == "UPDATE s.t SET b = ? WHERE a = ?" and params == ("x", 1)
    sql, params = DuckDBProcessor.generate_delete_sql("s", "t", {"a": 1, "b": "x"}, ["a"])
    assert sql == "DELETE FROM s.t WHERE a = ?" and params == (1,)
    p.close()


# ---------------------------------------------------------------------------
# pipeline recovery
# ---------------------------------------------------------------------------
class FlakyConnector(FakeConnector):
    """Stream fails once (ConnectorError) then recovers; then ends once to exercise the reconnect branch."""

    def __init__(self, config):
        super().__init__(config)
        self.failures_left = 1
        self.end_once = True
        self.disconnects = 0

    def disconnect(self):
        self.disconnects += 1
        super().disconnect()

    def stream_changes(self, tables, from_position, stop_event):
        if self.failures_left > 0:
            self.failures_left -= 1
            raise ConnectorError("position purged")
        if self.end_once:
            self.end_once = False
            return iter(())  # generator ended without stop -> reconnect with backoff
        return super().stream_changes(tables, from_position, stop_event)


def test_pipeline_reader_failure_is_audited_and_recovers(audit_store, monkeypatch):
    src = FlakyConnector({"id": "src"})
    dest = sqlite_destination()
    positions = MemoryPositions()
    p = ChangePipeline("rec", src, dest, [make_mapping()], positions, audit=audit_store, bus=EventBus(), batch_wait_s=0.05)
    # shrink the backoff so the test stays fast
    orig_wait = p._stop.wait
    monkeypatch.setattr(p._stop, "wait", lambda t=None: orig_wait(min(t or 0, 0.05)))
    p.start()
    assert wait_for(lambda: p.state == "error", timeout=2)
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 1, "Name": "n", "Email": "", "City": ""})
    assert wait_for(lambda: p.events_applied == 1, timeout=3)
    assert p.state == "running" and src.disconnects >= 1
    msgs = [r["message"] for r in audit_store.query(category="pipeline")]
    assert any("reader failed" in m for m in msgs) and any("recovered" in m for m in msgs)
    assert "position purged" in (p.last_error or "")
    p.stop()


class CrashingDestination:
    """Wraps the sqlite destination; the first apply crashes as if the destination went away."""

    def __init__(self, inner):
        self._inner = inner
        self.crashes_left = 1

    def __getattr__(self, item):
        return getattr(self._inner, item)


def test_pipeline_apply_crash_requeues_and_does_not_commit(audit_store):
    src = FakeConnector({"id": "src"})
    dest = sqlite_destination()
    positions = MemoryPositions()
    p = ChangePipeline("crash", src, dest, [make_mapping()], positions, audit=audit_store, bus=EventBus(),
                       batch_size=10, batch_wait_s=0.05)
    real_apply = p.applier.apply
    state = {"crashes": 1}

    def flaky_apply(events):
        if state["crashes"]:
            state["crashes"] -= 1
            raise RuntimeError("destination gone")
        return real_apply(events)

    p.applier.apply = flaky_apply
    p.start()
    assert src.stream_started.wait(2)
    for i in range(1, 4):
        src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": i, "Name": "n", "Email": "", "City": ""})
    assert wait_for(lambda: "destination gone" in (p.last_error or ""), timeout=2)
    assert positions.data.get("crash:__source__") is None           # nothing committed while the batch is in flight
    assert wait_for(lambda: p.events_applied == 3, timeout=5)       # requeued batch applied on retry, in order
    assert [r["id"] for r in dest.query("SELECT id FROM customers ORDER BY id")] == [1, 2, 3]
    assert positions.data["crash:__source__"]["value"] == 3
    p.stop()


def test_pipeline_position_commit_failure_is_reported():
    class BadPositions(MemoryPositions):
        def set_position(self, *a, **k):
            raise RuntimeError("disk full")

    src = FakeConnector({"id": "src"})
    p = ChangePipeline("pc", src, sqlite_destination(), [make_mapping()], BadPositions(), bus=EventBus(), batch_wait_s=0.05)
    p.start()
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 1, "Name": "n", "Email": "", "City": ""})
    assert wait_for(lambda: p.events_applied == 1)
    assert "position commit failed" in p.last_error
    p.stop()


def test_pipeline_table_scoped_positions_for_mssql_like_sources():
    src = FakeConnector({"id": "src"})
    src.engine = "mssql"
    positions = MemoryPositions()
    positions.set_position("dbo.Customers", "mssql_lsn", "0x01")  # legacy 1.x key
    p = ChangePipeline("tbl", src, sqlite_destination(), [make_mapping()], positions, bus=EventBus(), batch_wait_s=0.05,
                       initial_position="latest")
    assert p.position_scope == "table"
    p.load_positions()
    assert p.positions["dbo.Customers"].kind == "mssql_lsn"
    init = p._initial_positions()
    assert init["dbo.Customers"].value == "0x01"
    p.positions["dbo.Customers"] = None
    assert p._initial_positions()["dbo.Customers"] == src.current_position()
    positions.data.clear()
    p.start()
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 1, "Name": "n", "Email": "", "City": ""})
    assert wait_for(lambda: p.events_applied == 1)
    assert positions.data["tbl:dbo.Customers"]["value"] == 1
    p.stop()


def test_pipeline_source_scope_latest_and_confirm_failure_is_tolerated():
    class NoConfirm(FakeConnector):
        def confirm_position(self, position):
            raise RuntimeError("nope")

    src = NoConfirm({"id": "src"})
    p = ChangePipeline("cf", src, sqlite_destination(), [make_mapping()], MemoryPositions(), bus=EventBus(),
                       batch_wait_s=0.05, initial_position="latest")
    assert p._initial_positions() == src.current_position()
    p.start()
    src.script("dbo", "Customers", Operation.INSERT, {"CustomerID": 1, "Name": "n", "Email": "", "City": ""})
    assert wait_for(lambda: p.events_applied == 1)
    p.stop()


# ---------------------------------------------------------------------------
# workflow -> mapping compiler (node types)
# ---------------------------------------------------------------------------
def test_workflow_converter_node_types():
    from backend.core.workflow_converter import WorkflowConverter
    from backend.models.workflow_schemas import NodePosition, VisualWorkflow, WorkflowEdge, WorkflowNode

    def node(i, t, **cfg):
        return WorkflowNode(id=i, type=t, position=NodePosition(x=0, y=0), config=cfg)

    wf = VisualWorkflow(id="wf", name="wf", nodes=[
        node("sdb", "source_database", server="s", database="d"),
        node("st", "source_table", schema="dbo", table="Customers", columns=["id", "first", "last", "meta", "x"]),
        node("tj", "transform_json", source_columns=["meta", "x"], destination_column="meta_json"),
        node("tc", "transform_concat", source_columns=["first", "last"], destination_column="full_name", separator="' "),
        node("ts", "transform_sql", source_columns=["id"], destination_column="id2", expression="id * 2"),
        node("tf", "transform_filter", condition="id > 0"),      # no column mapping
        node("tj0", "transform_json", source_columns=[], destination_column=""),  # ignored: incomplete
        node("tc0", "transform_concat", source_columns=[]),
        node("ts0", "transform_sql", destination_column="y"),
        node("td", "transform_duckdb", script_content="SELECT * FROM {source_table}"),
        node("dm", "dest_mapper", mappings=[{"source": "id", "destination": "customer_id"}, {"source": "extra", "destination": "extra_out"}, "junk"]),
        node("ddb", "dest_database", server="s", database="d"),
        node("dt", "dest_table", schema="out", table="customers"),
    ], edges=[WorkflowEdge(id="e1", source="st", target="dt"), WorkflowEdge(id="e2", source="ghost", target="dt")])
    ok, mapping, errors = WorkflowConverter().compile_workflow(wf)
    assert ok and not errors, errors
    by_dest = {cm.destination_column: cm for cm in mapping.column_mappings}
    assert by_dest["meta_json"].source_columns == ["meta", "x"] and by_dest["meta_json"].transformation_type == "json"
    assert by_dest["full_name"].transformation == "concat_ws(''' ', \"first\", \"last\")"
    assert by_dest["id2"].source_column == "id" and by_dest["id2"].transformation == "id * 2"
    assert by_dest["customer_id"].source_column == "id"           # explicit mapper entry wins
    assert by_dest["extra_out"].source_column == "extra"          # mapper-only column still emitted
    assert "y" not in by_dest and "" not in by_dest
    assert mapping.use_duckdb_transformation and mapping.duckdb_script_content.startswith("SELECT")
    assert mapping.source_table == "Customers" and mapping.destination_table == "customers" and mapping.destination_schema == "out"

    # script by name + missing nodes
    wf2 = VisualWorkflow(id="w2", name="w2", nodes=[node("td", "transform_duckdb", script_name="basic")])
    ok, mapping, errors = WorkflowConverter().compile_workflow(wf2)
    assert not ok and any("Source Table" in e for e in errors) and any("Destination Table" in e for e in errors)
    wf3 = VisualWorkflow(id="w3", name="w3", nodes=[node("st", "source_table", table="t"), node("dt", "dest_table", table="t")])
    ok, mapping, errors = WorkflowConverter().compile_workflow(wf3)
    assert not ok and any("no column mappings" in e for e in errors)
    wf4 = VisualWorkflow(id="w4", name="w4", nodes=[node("st", "source_table", table="t", columns=["a"]), node("dt", "dest_table", table="t"),
                                                    node("td", "transform_duckdb", script_name="basic")])
    ok, mapping, errors = WorkflowConverter().compile_workflow(wf4)
    assert ok and mapping.duckdb_script_name == "basic"


def test_workflow_converter_reports_node_errors(monkeypatch):
    from backend.core.workflow_converter import WorkflowConverter
    from backend.models.workflow_schemas import NodePosition, VisualWorkflow, WorkflowNode
    conv = WorkflowConverter()
    monkeypatch.setattr(conv, "_node_to_column_mapping", lambda n: (_ for _ in ()).throw(ValueError("bad node")))
    wf = VisualWorkflow(id="w", name="w", nodes=[
        WorkflowNode(id="st", type="source_table", position=NodePosition(x=0, y=0), config={"table": "t", "columns": ["a"]}),
        WorkflowNode(id="dt", type="dest_table", position=NodePosition(x=0, y=0), config={"table": "t"}),
        WorkflowNode(id="tx", type="transform_sql", label="my sql", position=NodePosition(x=0, y=0), config={}),
    ])
    ok, _, errors = conv.compile_workflow(wf)
    assert not ok and "my sql" in errors[0] and "bad node" in errors[0]
