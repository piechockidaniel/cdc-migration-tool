"""
Visual Workflow Designer - Examples

This script demonstrates how to create, manage, and compile visual workflows via API.
"""

import requests
import json

API_BASE = "http://localhost:8000/api"


def create_simple_customer_workflow():
    """
    Example 1: Simple customer sync workflow
    
    Flow: Start → Source DB → Source Table → JSON Transform → Dest DB → Dest Table → Column Mapper
    """
    
    workflow = {
        "id": "workflow_customer_simple",
        "name": "Customer Sync - Simple",
        "description": "Basic customer synchronization with JSON aggregation",
        "nodes": [
            {
                "id": "node_start",
                "type": "start",
                "position": {"x": 50, "y": 200},
                "config": {},
                "label": "Start"
            },
            {
                "id": "node_src_db",
                "type": "source_database",
                "position": {"x": 200, "y": 200},
                "config": {
                    "server": "localhost",
                    "database": "OLTP_Customers",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Source DB"
            },
            {
                "id": "node_src_table",
                "type": "source_table",
                "position": {"x": 400, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "Customers",
                    "columns": []
                },
                "label": "Customer Table"
            },
            {
                "id": "node_json_transform",
                "type": "transform_json",
                "position": {"x": 600, "y": 200},
                "config": {
                    "source_columns": ["FirstName", "LastName", "Email", "Phone"],
                    "destination_column": "CustomerData",
                    "json_structure": "object"
                },
                "label": "JSON Aggregation"
            },
            {
                "id": "node_dest_db",
                "type": "dest_database",
                "position": {"x": 800, "y": 200},
                "config": {
                    "server": "localhost",
                    "database": "DataWarehouse",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Dest DB"
            },
            {
                "id": "node_dest_table",
                "type": "dest_table",
                "position": {"x": 1000, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "DimCustomers"
                },
                "label": "DimCustomers"
            },
            {
                "id": "node_mapper",
                "type": "dest_mapper",
                "position": {"x": 1200, "y": 200},
                "config": {
                    "mappings": []
                },
                "label": "Column Mapper"
            }
        ],
        "edges": [
            {"id": "edge_1", "source": "node_start", "target": "node_src_db"},
            {"id": "edge_2", "source": "node_src_db", "target": "node_src_table"},
            {"id": "edge_3", "source": "node_src_table", "target": "node_json_transform"},
            {"id": "edge_4", "source": "node_json_transform", "target": "node_dest_db"},
            {"id": "edge_5", "source": "node_dest_db", "target": "node_dest_table"},
            {"id": "edge_6", "source": "node_dest_table", "target": "node_mapper"}
        ],
        "metadata": {
            "created_by": "API Example",
            "use_case": "Customer synchronization"
        }
    }
    
    response = requests.post(f"{API_BASE}/admin/workflow/visual/create", json=workflow)
    print("Customer Simple Workflow:", json.dumps(response.json(), indent=2))
    return workflow["id"]


def create_filtered_orders_workflow():
    """
    Example 2: Orders workflow with filtering and logging
    
    Flow: Start → Source → Filter → Logger → Destination
    """
    
    workflow = {
        "id": "workflow_orders_filtered",
        "name": "Orders - Filtered with Logging",
        "description": "Sync only completed orders with logging",
        "nodes": [
            {
                "id": "node_start",
                "type": "start",
                "position": {"x": 50, "y": 200},
                "config": {},
                "label": "Start"
            },
            {
                "id": "node_src_db",
                "type": "source_database",
                "position": {"x": 200, "y": 200},
                "config": {
                    "server": "localhost",
                    "database": "OLTP_Orders",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Source DB"
            },
            {
                "id": "node_src_table",
                "type": "source_table",
                "position": {"x": 400, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "Orders"
                },
                "label": "Orders Table"
            },
            {
                "id": "node_filter",
                "type": "transform_filter",
                "position": {"x": 600, "y": 200},
                "config": {
                    "condition": "status = 'COMPLETED' AND order_date >= '2024-01-01'",
                    "filter_type": "where"
                },
                "label": "Filter Completed"
            },
            {
                "id": "node_logger",
                "type": "op_logger",
                "position": {"x": 800, "y": 200},
                "config": {
                    "log_level": "info",
                    "message": "Processing completed order",
                    "log_data": True
                },
                "label": "Order Logger"
            },
            {
                "id": "node_dest_db",
                "type": "dest_database",
                "position": {"x": 1000, "y": 200},
                "config": {
                    "server": "localhost",
                    "database": "DataWarehouse",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "DW DB"
            },
            {
                "id": "node_dest_table",
                "type": "dest_table",
                "position": {"x": 1200, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "FactOrders"
                },
                "label": "FactOrders"
            }
        ],
        "edges": [
            {"id": "edge_1", "source": "node_start", "target": "node_src_db"},
            {"id": "edge_2", "source": "node_src_db", "target": "node_src_table"},
            {"id": "edge_3", "source": "node_src_table", "target": "node_filter"},
            {"id": "edge_4", "source": "node_filter", "target": "node_logger"},
            {"id": "edge_5", "source": "node_logger", "target": "node_dest_db"},
            {"id": "edge_6", "source": "node_dest_db", "target": "node_dest_table"}
        ]
    }
    
    response = requests.post(f"{API_BASE}/admin/workflow/visual/create", json=workflow)
    print("\nOrders Filtered Workflow:", json.dumps(response.json(), indent=2))
    return workflow["id"]


def create_conditional_routing_workflow():
    """
    Example 3: Conditional routing based on transaction amount
    
    Flow: Start → Source → Condition → [High/Low] → Different Destinations
    """
    
    workflow = {
        "id": "workflow_transactions_conditional",
        "name": "Transactions - Conditional Routing",
        "description": "Route transactions to different tables based on amount",
        "nodes": [
            {
                "id": "node_start",
                "type": "start",
                "position": {"x": 50, "y": 300},
                "config": {},
                "label": "Start"
            },
            {
                "id": "node_src_db",
                "type": "source_database",
                "position": {"x": 200, "y": 300},
                "config": {
                    "server": "localhost",
                    "database": "OLTP_Transactions",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Source DB"
            },
            {
                "id": "node_src_table",
                "type": "source_table",
                "position": {"x": 400, "y": 300},
                "config": {
                    "schema": "dbo",
                    "table": "Transactions"
                },
                "label": "Transactions"
            },
            {
                "id": "node_condition",
                "type": "op_condition",
                "position": {"x": 600, "y": 300},
                "config": {
                    "condition": "amount > 10000",
                    "condition_type": "sql"
                },
                "label": "Amount Check"
            },
            {
                "id": "node_dest_high",
                "type": "dest_table",
                "position": {"x": 800, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "HighValueTransactions"
                },
                "label": "High Value"
            },
            {
                "id": "node_dest_low",
                "type": "dest_table",
                "position": {"x": 800, "y": 400},
                "config": {
                    "schema": "dbo",
                    "table": "StandardTransactions"
                },
                "label": "Standard"
            }
        ],
        "edges": [
            {"id": "edge_1", "source": "node_start", "target": "node_src_db"},
            {"id": "edge_2", "source": "node_src_db", "target": "node_src_table"},
            {"id": "edge_3", "source": "node_src_table", "target": "node_condition"},
            {"id": "edge_4", "source": "node_condition", "target": "node_dest_high"},
            {"id": "edge_5", "source": "node_condition", "target": "node_dest_low"}
        ]
    }
    
    response = requests.post(f"{API_BASE}/admin/workflow/visual/create", json=workflow)
    print("\nConditional Routing Workflow:", json.dumps(response.json(), indent=2))
    return workflow["id"]


def create_multi_transform_workflow():
    """
    Example 4: Multiple transformations chained together
    
    Flow: Start → Source → Filter → Dedupe → JSON → Concat → Destination
    """
    
    workflow = {
        "id": "workflow_multi_transform",
        "name": "Multi-Transform Pipeline",
        "description": "Demonstrates chaining multiple transformations",
        "nodes": [
            {
                "id": "node_start",
                "type": "start",
                "position": {"x": 50, "y": 250},
                "config": {},
                "label": "Start"
            },
            {
                "id": "node_src_db",
                "type": "source_database",
                "position": {"x": 200, "y": 250},
                "config": {
                    "server": "localhost",
                    "database": "OLTP",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Source"
            },
            {
                "id": "node_src_table",
                "type": "source_table",
                "position": {"x": 350, "y": 250},
                "config": {
                    "schema": "dbo",
                    "table": "Employees"
                },
                "label": "Employees"
            },
            {
                "id": "node_filter",
                "type": "transform_filter",
                "position": {"x": 500, "y": 250},
                "config": {
                    "condition": "status = 'ACTIVE'"
                },
                "label": "Active Only"
            },
            {
                "id": "node_dedupe",
                "type": "transform_dedupe",
                "position": {"x": 650, "y": 250},
                "config": {
                    "unique_columns": ["EmployeeID"],
                    "keep_strategy": "last"
                },
                "label": "Remove Dupes"
            },
            {
                "id": "node_json",
                "type": "transform_json",
                "position": {"x": 800, "y": 200},
                "config": {
                    "source_columns": ["FirstName", "LastName", "Email"],
                    "destination_column": "PersonalInfo"
                },
                "label": "Personal Info JSON"
            },
            {
                "id": "node_concat",
                "type": "transform_concat",
                "position": {"x": 800, "y": 300},
                "config": {
                    "source_columns": ["Department", "JobTitle", "Level"],
                    "destination_column": "JobInfo",
                    "separator": " - "
                },
                "label": "Job Info Concat"
            },
            {
                "id": "node_dest_db",
                "type": "dest_database",
                "position": {"x": 1000, "y": 250},
                "config": {
                    "server": "localhost",
                    "database": "DataWarehouse",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Destination"
            },
            {
                "id": "node_dest_table",
                "type": "dest_table",
                "position": {"x": 1150, "y": 250},
                "config": {
                    "schema": "dbo",
                    "table": "DimEmployees"
                },
                "label": "DimEmployees"
            }
        ],
        "edges": [
            {"id": "e1", "source": "node_start", "target": "node_src_db"},
            {"id": "e2", "source": "node_src_db", "target": "node_src_table"},
            {"id": "e3", "source": "node_src_table", "target": "node_filter"},
            {"id": "e4", "source": "node_filter", "target": "node_dedupe"},
            {"id": "e5", "source": "node_dedupe", "target": "node_json"},
            {"id": "e6", "source": "node_dedupe", "target": "node_concat"},
            {"id": "e7", "source": "node_json", "target": "node_dest_db"},
            {"id": "e8", "source": "node_concat", "target": "node_dest_db"},
            {"id": "e9", "source": "node_dest_db", "target": "node_dest_table"}
        ]
    }
    
    response = requests.post(f"{API_BASE}/admin/workflow/visual/create", json=workflow)
    print("\nMulti-Transform Workflow:", json.dumps(response.json(), indent=2))
    return workflow["id"]


def create_duckdb_transform_workflow():
    """
    Example 5: Using DuckDB transformation
    
    Flow: Start → Source → DuckDB Transform → Destination
    """
    
    workflow = {
        "id": "workflow_duckdb_aggregation",
        "name": "Sales Aggregation - DuckDB",
        "description": "Aggregate sales data using DuckDB",
        "nodes": [
            {
                "id": "node_start",
                "type": "start",
                "position": {"x": 50, "y": 200},
                "config": {},
                "label": "Start"
            },
            {
                "id": "node_src_db",
                "type": "source_database",
                "position": {"x": 200, "y": 200},
                "config": {
                    "server": "localhost",
                    "database": "Sales_DB",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Sales DB"
            },
            {
                "id": "node_src_table",
                "type": "source_table",
                "position": {"x": 400, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "DailySales"
                },
                "label": "Daily Sales"
            },
            {
                "id": "node_duckdb",
                "type": "transform_duckdb",
                "position": {"x": 600, "y": 200},
                "config": {
                    "script_name": "aggregation",
                    "script_content": """
                        -- Monthly sales aggregation
                        SELECT 
                            DATE_TRUNC('month', sale_date) as month,
                            customer_id,
                            COUNT(*) as sale_count,
                            SUM(amount) as total_amount,
                            AVG(amount) as avg_amount
                        FROM {source_table}
                        GROUP BY DATE_TRUNC('month', sale_date), customer_id
                    """
                },
                "label": "Monthly Aggregation"
            },
            {
                "id": "node_dest_db",
                "type": "dest_database",
                "position": {"x": 850, "y": 200},
                "config": {
                    "server": "localhost",
                    "database": "Analytics_DB",
                    "port": 1433,
                    "use_windows_auth": True
                },
                "label": "Analytics DB"
            },
            {
                "id": "node_dest_table",
                "type": "dest_table",
                "position": {"x": 1050, "y": 200},
                "config": {
                    "schema": "dbo",
                    "table": "MonthlySales"
                },
                "label": "Monthly Sales"
            }
        ],
        "edges": [
            {"id": "e1", "source": "node_start", "target": "node_src_db"},
            {"id": "e2", "source": "node_src_db", "target": "node_src_table"},
            {"id": "e3", "source": "node_src_table", "target": "node_duckdb"},
            {"id": "e4", "source": "node_duckdb", "target": "node_dest_db"},
            {"id": "e5", "source": "node_dest_db", "target": "node_dest_table"}
        ]
    }
    
    response = requests.post(f"{API_BASE}/admin/workflow/visual/create", json=workflow)
    print("\nDuckDB Transform Workflow:", json.dumps(response.json(), indent=2))
    return workflow["id"]


def list_all_workflows():
    """List all visual workflows"""
    response = requests.get(f"{API_BASE}/admin/workflow/visual/list")
    workflows = response.json()
    
    print("\n" + "="*80)
    print("VISUAL WORKFLOWS")
    print("="*80)
    
    for wf in workflows:
        print(f"\n📊 {wf['name']}")
        print(f"   ID: {wf['id']}")
        print(f"   Nodes: {wf['node_count']}, Edges: {wf['edge_count']}")
        if wf.get('compiled_mapping_id'):
            print(f"   Compiled: {wf['compiled_mapping_id']}")
        print(f"   Created: {wf['created_at']}")


def validate_workflow(workflow_id):
    """Validate a workflow"""
    response = requests.post(f"{API_BASE}/admin/workflow/validate/{workflow_id}")
    result = response.json()
    
    print(f"\n=== Validation: {workflow_id} ===")
    print(f"Valid: {result['valid']}")
    print(f"Nodes: {result['node_count']}, Edges: {result['edge_count']}")
    
    if result.get('errors'):
        print("\nErrors:")
        for error in result['errors']:
            print(f"  ❌ {error}")
    
    if result.get('warnings'):
        print("\nWarnings:")
        for warning in result['warnings']:
            print(f"  ⚠️  {warning}")
    
    if result['valid']:
        print("\n✅ Workflow is valid!")


def compile_workflow(workflow_id):
    """Compile workflow to Mapping"""
    response = requests.post(f"{API_BASE}/admin/workflow/compile/{workflow_id}")
    result = response.json()
    
    print(f"\n=== Compilation: {workflow_id} ===")
    print(f"Success: {result['success']}")
    
    if result['success']:
        print(f"✅ Compiled to mapping: {result['mapping_id']}")
        print(f"Message: {result['message']}")
    else:
        print(f"❌ {result['message']}")
        if result.get('errors'):
            for error in result['errors']:
                print(f"  Error: {error}")


def get_workflow_details(workflow_id):
    """Get detailed workflow information"""
    response = requests.get(f"{API_BASE}/admin/workflow/visual/{workflow_id}")
    workflow = response.json()
    
    print(f"\n=== Workflow Details: {workflow_id} ===")
    print(f"Name: {workflow['name']}")
    print(f"Description: {workflow.get('description', 'N/A')}")
    print(f"\nNodes ({len(workflow['nodes'])}):")
    for node in workflow['nodes']:
        print(f"  - {node['label'] or node['id']} ({node['type']})")
    
    print(f"\nEdges ({len(workflow['edges'])}):")
    for edge in workflow['edges']:
        print(f"  - {edge['source']} → {edge['target']}")
    
    if workflow.get('compiled_mapping_id'):
        print(f"\nCompiled Mapping: {workflow['compiled_mapping_id']}")


def delete_workflow(workflow_id):
    """Delete a workflow"""
    response = requests.delete(f"{API_BASE}/admin/workflow/visual/{workflow_id}")
    result = response.json()
    
    if result['success']:
        print(f"✅ {result['message']}")
    else:
        print(f"❌ Failed to delete workflow")


def main():
    """Run all visual workflow examples"""
    print("=" * 80)
    print("VISUAL WORKFLOW DESIGNER - API EXAMPLES")
    print("=" * 80)
    
    workflows_created = []
    
    # Create workflows
    print("\n1. Creating Simple Customer Workflow")
    print("-" * 80)
    try:
        wf_id = create_simple_customer_workflow()
        workflows_created.append(wf_id)
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n2. Creating Filtered Orders Workflow")
    print("-" * 80)
    try:
        wf_id = create_filtered_orders_workflow()
        workflows_created.append(wf_id)
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n3. Creating Conditional Routing Workflow")
    print("-" * 80)
    try:
        wf_id = create_conditional_routing_workflow()
        workflows_created.append(wf_id)
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n4. Creating DuckDB Transform Workflow")
    print("-" * 80)
    try:
        wf_id = create_duckdb_transform_workflow()
        workflows_created.append(wf_id)
    except Exception as e:
        print(f"Error: {e}")
    
    # List all workflows
    try:
        list_all_workflows()
    except Exception as e:
        print(f"Error listing workflows: {e}")
    
    # Validate first workflow
    if workflows_created:
        try:
            validate_workflow(workflows_created[0])
        except Exception as e:
            print(f"Error validating: {e}")
        
        # Get details
        try:
            get_workflow_details(workflows_created[0])
        except Exception as e:
            print(f"Error getting details: {e}")
        
        # Compile
        try:
            compile_workflow(workflows_created[0])
        except Exception as e:
            print(f"Error compiling: {e}")
    
    print("\n" + "=" * 80)
    print("✅ Visual Workflow Examples Complete!")
    print("=" * 80)
    print(f"\nCreated {len(workflows_created)} workflows")
    print("Open admin UI → Visual Designer tab to view and edit visually")


if __name__ == "__main__":
    main()

