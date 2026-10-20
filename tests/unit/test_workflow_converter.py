"""Regression: the visual workflow compiler raised 'NoneType' object is not callable (stray `Mappping=None` parameter)."""
from backend.core.workflow_converter import WorkflowConverter
from backend.models.workflow_schemas import NodePosition, VisualWorkflow, WorkflowEdge, WorkflowNode


def _node(nid, ntype, config):
    return WorkflowNode(id=nid, type=ntype, position=NodePosition(x=0, y=0), config=config)


def test_compile_workflow_produces_table_mapping():
    wf = VisualWorkflow(id="wf1", name="wf", nodes=[
        _node("src", "source_table", {"schema": "dbo", "table": "Customers", "columns": ["CustomerID", "Name"]}),
        _node("concat", "transform_concat", {"source_columns": ["FirstName", "LastName"], "destination_column": "full_name", "separator": " "}),
        _node("dst", "dest_table", {"schema": "dbo", "table": "customers", "columns": ["id", "name", "full_name"]}),
    ], edges=[WorkflowEdge(id="e1", source="src", target="concat"), WorkflowEdge(id="e2", source="concat", target="dst")])
    ok, mapping, errors = WorkflowConverter().compile_workflow(wf)
    assert ok, errors
    assert mapping.id == "mapping_wf1" and mapping.source_table == "Customers" and mapping.destination_table == "customers"
    concat = next((cm for cm in mapping.column_mappings if cm.destination_column == "full_name"), None)
    assert concat is not None and concat.transformation.startswith("concat_ws(' ', ") and concat.source_columns == ["FirstName", "LastName"]


def test_compile_workflow_direct_columns_and_column_mapper():
    wf = VisualWorkflow(id="wf2", name="wf", nodes=[
        _node("src", "source_table", {"schema": "dbo", "table": "Customers", "columns": ["CustomerID", "Name", "City"]}),
        _node("map", "dest_mapper", {"mappings": [{"source": "CustomerID", "destination": "id"}]}),
        _node("dst", "dest_table", {"schema": "dbo", "table": "customers"}),
    ])
    ok, mapping, errors = WorkflowConverter().compile_workflow(wf)
    assert ok, errors
    pairs = {cm.source_column: cm.destination_column for cm in mapping.column_mappings}
    assert pairs == {"CustomerID": "id", "Name": "Name", "City": "City"}
    empty = VisualWorkflow(id="wf3", name="wf", nodes=[_node("src", "source_table", {"schema": "dbo", "table": "T"}), _node("dst", "dest_table", {"schema": "dbo", "table": "U"})])
    ok, mapping, errors = WorkflowConverter().compile_workflow(empty)
    assert not ok and "no column mappings" in errors[0]
