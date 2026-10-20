"""Pydantic models for visual workflow designer."""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class NodePosition(BaseModel):
    """Node position on canvas."""
    x: float
    y: float


class WorkflowNode(BaseModel):
    """Visual workflow node."""
    id: str = Field(..., description="Unique node ID")
    type: str = Field(..., description="Node type (start, source_table, transform_json, etc.)")
    position: NodePosition = Field(..., description="Position on canvas")
    config: Dict[str, Any] = Field(default_factory=dict, description="Node-specific configuration")
    label: Optional[str] = Field(None, description="Custom label for the node")


class WorkflowEdge(BaseModel):
    """Connection between workflow nodes."""
    id: str = Field(..., description="Unique edge ID")
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    source_port: Optional[int] = Field(None, description="Source port index")
    target_port: Optional[int] = Field(None, description="Target port index")


class VisualWorkflow(BaseModel):
    """Complete visual workflow definition."""
    id: str = Field(..., description="Unique workflow ID")
    name: str = Field(..., description="Workflow name")
    description: Optional[str] = Field(None, description="Workflow description")
    nodes: List[WorkflowNode] = Field(default_factory=list, description="List of nodes in the workflow")
    edges: List[WorkflowEdge] = Field(default_factory=list, description="List of connections")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    compiled_mapping_id: Optional[str] = Field(None, description="ID of compiled Mapping")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class WorkflowValidationResult(BaseModel):
    """Result of workflow validation."""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    node_count: int = 0
    edge_count: int = 0


class WorkflowCompileResult(BaseModel):
    """Result of workflow compilation."""
    success: bool
    mapping_id: Optional[str] = None
    message: str = ""
    errors: List[str] = Field(default_factory=list)


class WorkflowListItem(BaseModel):
    """Summary of workflow for listing."""
    id: str
    name: str
    description: Optional[str] = None
    node_count: int
    edge_count: int
    compiled_mapping_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime

