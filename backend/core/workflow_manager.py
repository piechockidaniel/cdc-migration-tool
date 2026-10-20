"""
Workflow Manager

Manages visual workflows - CRUD operations, persistence, and registry.
"""

import json
import logging
import os
from typing import List, Optional, Tuple
from pathlib import Path
from datetime import datetime
from backend.models.workflow_schemas import (
    VisualWorkflow, WorkflowNode,
    WorkflowListItem, WorkflowValidationResult
)

logger = logging.getLogger(__name__)

# Workflow storage paths
WORKFLOWS_BASE_DIR = Path(os.environ.get("CDCSYNC_WORKFLOWS_DIR", "workflows"))
VISUAL_DIR = WORKFLOWS_BASE_DIR / "visual"
COMPILED_DIR = WORKFLOWS_BASE_DIR / "compiled"


class WorkflowManager:
    """Manager for visual workflows."""
    
    def __init__(self):
        """Initialize workflow manager."""
        self._ensure_directories()
        self._workflows_cache = {}
        self._load_workflows()
    
    def _ensure_directories(self):
        """Ensure workflow directories exist."""
        for directory in [WORKFLOWS_BASE_DIR, VISUAL_DIR, COMPILED_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")
    
    def _load_workflows(self):
        """Load all workflows from disk into cache."""
        self._workflows_cache = {}
        
        if not VISUAL_DIR.exists():
            return
        
        for workflow_file in VISUAL_DIR.glob("*.json"):
            try:
                with open(workflow_file, 'r') as f:
                    data = json.load(f)
                    workflow = VisualWorkflow(**data)
                    self._workflows_cache[workflow.id] = workflow
                    logger.info(f"Loaded workflow: {workflow.id}")
            except Exception as e:
                logger.error(f"Error loading workflow {workflow_file}: {e}")
    
    def create_workflow(self, workflow: VisualWorkflow) -> Tuple[bool, str]:
        """Create a new visual workflow.
        
        Args:
            workflow: Workflow to create
            
        Returns:
            Tuple of (success, message)
        """
        try:
            if workflow.id in self._workflows_cache:
                return False, f"Workflow '{workflow.id}' already exists"
            
            # Set timestamps
            workflow.created_at = datetime.now()
            workflow.updated_at = datetime.now()
            
            # Save to disk
            workflow_path = VISUAL_DIR / f"{workflow.id}.json"
            with open(workflow_path, 'w') as f:
                json.dump(workflow.model_dump(mode='json'), f, indent=2, default=str)
            
            # Add to cache
            self._workflows_cache[workflow.id] = workflow
            
            logger.info(f"Created workflow: {workflow.id}")
            return True, f"Workflow '{workflow.name}' created successfully"
            
        except Exception as e:
            logger.error(f"Error creating workflow: {e}")
            return False, f"Error creating workflow: {str(e)}"
    
    def get_workflow(self, workflow_id: str) -> Optional[VisualWorkflow]:
        """Get a workflow by ID.
        
        Args:
            workflow_id: Workflow ID
            
        Returns:
            Workflow or None if not found
        """
        return self._workflows_cache.get(workflow_id)
    
    def list_workflows(self) -> List[WorkflowListItem]:
        """List all workflows with summary information.
        
        Returns:
            List of workflow summaries
        """
        workflows = []
        
        for workflow in self._workflows_cache.values():
            workflows.append(WorkflowListItem(
                id=workflow.id,
                name=workflow.name,
                description=workflow.description,
                node_count=len(workflow.nodes),
                edge_count=len(workflow.edges),
                compiled_mapping_id=workflow.compiled_mapping_id,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at
            ))
        
        return sorted(workflows, key=lambda w: w.updated_at, reverse=True)
    
    def update_workflow(self, workflow: VisualWorkflow) -> Tuple[bool, str]:
        """Update an existing workflow.
        
        Args:
            workflow: Updated workflow
            
        Returns:
            Tuple of (success, message)
        """
        try:
            if workflow.id not in self._workflows_cache:
                return False, f"Workflow '{workflow.id}' not found"
            
            # Update timestamp
            workflow.updated_at = datetime.now()
            
            # Save to disk
            workflow_path = VISUAL_DIR / f"{workflow.id}.json"
            with open(workflow_path, 'w') as f:
                json.dump(workflow.model_dump(mode='json'), f, indent=2, default=str)
            
            # Update cache
            self._workflows_cache[workflow.id] = workflow
            
            logger.info(f"Updated workflow: {workflow.id}")
            return True, f"Workflow '{workflow.name}' updated successfully"
            
        except Exception as e:
            logger.error(f"Error updating workflow: {e}")
            return False, f"Error updating workflow: {str(e)}"
    
    def delete_workflow(self, workflow_id: str) -> Tuple[bool, str]:
        """Delete a workflow.
        
        Args:
            workflow_id: Workflow ID to delete
            
        Returns:
            Tuple of (success, message)
        """
        try:
            if workflow_id not in self._workflows_cache:
                return False, f"Workflow '{workflow_id}' not found"
            
            # Delete file
            workflow_path = VISUAL_DIR / f"{workflow_id}.json"
            if workflow_path.exists():
                workflow_path.unlink()
            
            # Remove from cache
            workflow_name = self._workflows_cache[workflow_id].name
            del self._workflows_cache[workflow_id]
            
            logger.info(f"Deleted workflow: {workflow_id}")
            return True, f"Workflow '{workflow_name}' deleted successfully"
            
        except Exception as e:
            logger.error(f"Error deleting workflow: {e}")
            return False, f"Error deleting workflow: {str(e)}"
    
    def validate_workflow(self, workflow: VisualWorkflow) -> WorkflowValidationResult:
        """Validate a workflow structure.
        
        Args:
            workflow: Workflow to validate
            
        Returns:
            Validation result
        """
        errors = []
        warnings = []
        
        # Check for nodes
        if not workflow.nodes or len(workflow.nodes) == 0:
            errors.append("Workflow must have at least one node")
            return WorkflowValidationResult(
                valid=False,
                errors=errors,
                node_count=0,
                edge_count=0
            )
        
        # Check for Start node
        start_nodes = [n for n in workflow.nodes if n.type == 'start']
        if len(start_nodes) == 0:
            errors.append("Workflow must have a Start node")
        elif len(start_nodes) > 1:
            warnings.append("Workflow has multiple Start nodes (only one will be used)")
        
        # Check for end nodes (Stop or Destination)
        end_node_types = ['stop', 'dest_mapper', 'dest_table']
        end_nodes = [n for n in workflow.nodes if n.type in end_node_types]
        if len(end_nodes) == 0:
            errors.append("Workflow must have at least one Stop node or Destination")
        
        # Check for disconnected nodes
        connected_nodes = set()
        for edge in workflow.edges:
            connected_nodes.add(edge.source)
            connected_nodes.add(edge.target)
        
        disconnected = []
        for node in workflow.nodes:
            if node.id not in connected_nodes and node.type != 'start':
                disconnected.append(node.label or node.id)
        
        if disconnected:
            warnings.append(f"Disconnected nodes: {', '.join(disconnected)}")
        
        # Check for cycles (DAG validation)
        if self._has_cycle(workflow):
            errors.append("Workflow contains cycles (circular dependencies not allowed)")
        
        # Validate node configurations
        for node in workflow.nodes:
            node_errors = self._validate_node_config(node)
            errors.extend(node_errors)
        
        return WorkflowValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            node_count=len(workflow.nodes),
            edge_count=len(workflow.edges)
        )
    
    def _has_cycle(self, workflow: VisualWorkflow) -> bool:
        """Check if workflow has cycles using DFS.
        
        Args:
            workflow: Workflow to check
            
        Returns:
            True if cycle detected
        """
        # Build adjacency list
        graph = {}
        for node in workflow.nodes:
            graph[node.id] = []
        
        for edge in workflow.edges:
            if edge.source in graph:
                graph[edge.source].append(edge.target)
        
        # DFS cycle detection
        visited = set()
        rec_stack = set()
        
        def has_cycle_util(node_id):
            visited.add(node_id)
            rec_stack.add(node_id)
            
            for neighbor in graph.get(node_id, []):
                if neighbor not in visited:
                    if has_cycle_util(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node_id)
            return False
        
        for node_id in graph:
            if node_id not in visited:
                if has_cycle_util(node_id):
                    return True
        
        return False
    
    def _validate_node_config(self, node: WorkflowNode) -> List[str]:
        """Validate individual node configuration.
        
        Args:
            node: Node to validate
            
        Returns:
            List of error messages
        """
        errors = []
        
        # Type-specific validation
        if node.type in ['source_database', 'dest_database']:
            if not node.config.get('server'):
                errors.append(f"Node '{node.label or node.id}': Server is required")
            if not node.config.get('database'):
                errors.append(f"Node '{node.label or node.id}': Database is required")
        
        elif node.type in ['source_table', 'dest_table']:
            if not node.config.get('table'):
                errors.append(f"Node '{node.label or node.id}': Table name is required")
        
        elif node.type in ['transform_json', 'transform_concat']:
            if not node.config.get('source_columns') or len(node.config.get('source_columns', [])) == 0:
                errors.append(f"Node '{node.label or node.id}': Source columns required")
            if not node.config.get('destination_column'):
                errors.append(f"Node '{node.label or node.id}': Destination column required")
        
        elif node.type == 'transform_sql':
            if not node.config.get('expression'):
                errors.append(f"Node '{node.label or node.id}': SQL expression required")
        
        elif node.type == 'transform_filter':
            if not node.config.get('condition'):
                errors.append(f"Node '{node.label or node.id}': Filter condition required")
        
        elif node.type == 'transform_dedupe':
            if not node.config.get('unique_columns') or len(node.config.get('unique_columns', [])) == 0:
                errors.append(f"Node '{node.label or node.id}': Unique columns required")
        
        return errors
    
    def set_compiled_mapping(self, workflow_id: str, mapping_id: str) -> Tuple[bool, str]:
        """Associate a compiled mapping with a workflow.
        
        Args:
            workflow_id: Workflow ID
            mapping_id: Compiled mapping ID
            
        Returns:
            Tuple of (success, message)
        """
        workflow = self.get_workflow(workflow_id)
        if not workflow:
            return False, "Workflow not found"
        
        workflow.compiled_mapping_id = mapping_id
        return self.update_workflow(workflow)


# Global instance
workflow_manager = WorkflowManager()

