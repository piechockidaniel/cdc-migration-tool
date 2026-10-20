"""Example script demonstrating API usage for MSSQL CDC Synchronizer.

This script shows how to programmatically:
1. Configure connections
2. Enable CDC
3. Create mappings
4. Create working sets
5. Start synchronization

Prerequisites:
- Application must be running on http://localhost:8000
- Source and destination SQL Servers must be accessible
- CDC must be supported on source server
"""

import time
import requests

API_BASE = "http://localhost:8000/api"


def test_connection(name, server, database, use_windows_auth=True):
    """Test database connection."""
    print(f"\n{'='*60}")
    print(f"Testing {name} connection...")
    print(f"{'='*60}")
    
    config = {
        "name": name,
        "server": server,
        "port": 1433,
        "database": database,
        "use_windows_auth": use_windows_auth,
        "username": None,
        "password": None
    }
    
    response = requests.post(f"{API_BASE}/admin/connect/test", json=config)
    result = response.json()
    
    if result["success"]:
        print(f"✓ Connection successful: {result['message']}")
        return config
    else:
        print(f"✗ Connection failed: {result['message']}")
        return None


def set_connection(connection_type, config):
    """Set source or destination connection."""
    print(f"\nSetting {connection_type} connection...")
    
    response = requests.post(
        f"{API_BASE}/admin/connect/set",
        params={"connection_type": connection_type},
        json=config
    )
    
    result = response.json()
    
    if result["success"]:
        print(f"✓ {connection_type} connection configured")
        return True
    else:
        print(f"✗ Failed to set {connection_type} connection")
        return False


def enable_database_cdc():
    """Enable CDC on source database."""
    print(f"\n{'='*60}")
    print("Enabling CDC on source database...")
    print(f"{'='*60}")
    
    response = requests.post(
        f"{API_BASE}/admin/cdc/enable-db",
        json={"connection_type": "source"}
    )
    
    result = response.json()
    
    if response.status_code == 200 and result.get("success"):
        print(f"✓ {result['message']}")
        return True
    else:
        print(f"✗ Failed: {result.get('detail', 'Unknown error')}")
        return False


def enable_table_cdc(schema, table):
    """Enable CDC on a specific table."""
    print(f"\nEnabling CDC on {schema}.{table}...")
    
    response = requests.post(
        f"{API_BASE}/admin/cdc/enable-table",
        json={
            "connection_type": "source",
            "schema_name": schema,
            "table_name": table
        }
    )
    
    result = response.json()
    
    if response.status_code == 200 and result.get("success"):
        print(f"✓ {result['message']}")
        return True
    else:
        print(f"✗ Failed: {result.get('detail', 'Unknown error')}")
        return False


def create_table_mapping(mapping_id, source_schema, source_table, 
                        dest_schema, dest_table, column_mappings):
    """Create a table mapping."""
    print(f"\n{'='*60}")
    print(f"Creating mapping: {source_schema}.{source_table} → {dest_schema}.{dest_table}")
    print(f"{'='*60}")
    
    mapping = {
        "id": mapping_id,
        "source_schema": source_schema,
        "source_table": source_table,
        "destination_schema": dest_schema,
        "destination_table": dest_table,
        "column_mappings": column_mappings,
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": True
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    
    if response.status_code == 200:
        result = response.json()
        if result.get("success"):
            print(f"✓ Mapping created successfully")
            print(f"  Columns mapped: {len(column_mappings)}")
            return True
    
    print(f"✗ Failed to create mapping")
    return False


def create_working_set(workset_id, name, source_config, dest_config, mapping_ids):
    """Create a working set."""
    print(f"\n{'='*60}")
    print(f"Creating working set: {name}")
    print(f"{'='*60}")
    
    from datetime import datetime
    
    workset = {
        "id": workset_id,
        "name": name,
        "description": f"Created via API example at {datetime.now().isoformat()}",
        "source_connection": source_config,
        "destination_connection": dest_config,
        "table_mappings": mapping_ids,
        "is_active": False,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    response = requests.post(f"{API_BASE}/admin/workset/create", json=workset)
    
    if response.status_code == 200:
        result = response.json()
        if result.get("success"):
            print(f"✓ Working set created")
            print(f"  Mappings: {len(mapping_ids)}")
            return True
    
    print(f"✗ Failed to create working set")
    return False


def activate_working_set(workset_id):
    """Activate a working set."""
    print(f"\nActivating working set: {workset_id}...")
    
    response = requests.put(f"{API_BASE}/admin/workset/activate/{workset_id}")
    
    if response.status_code == 200:
        result = response.json()
        if result.get("success"):
            print(f"✓ Working set activated")
            return True
    
    print(f"✗ Failed to activate working set")
    return False


def start_synchronization():
    """Start synchronization."""
    print(f"\n{'='*60}")
    print("Starting synchronization...")
    print(f"{'='*60}")
    
    response = requests.post(f"{API_BASE}/operations/start")
    
    if response.status_code == 200:
        result = response.json()
        if result.get("success"):
            print(f"✓ {result['message']}")
            return True
    
    print(f"✗ Failed to start synchronization")
    return False


def get_status():
    """Get synchronization status."""
    response = requests.get(f"{API_BASE}/operations/status")
    
    if response.status_code == 200:
        return response.json()
    
    return None


def monitor_sync(duration=30):
    """Monitor synchronization for specified duration."""
    print(f"\n{'='*60}")
    print(f"Monitoring synchronization for {duration} seconds...")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    while time.time() - start_time < duration:
        status = get_status()
        
        if status:
            print(f"\rStatus: {status['status'].upper()} | "
                  f"Events: {status['events_processed']} | "
                  f"Errors: {status['errors_count']}", end="", flush=True)
        
        time.sleep(2)
    
    print("\n\nMonitoring complete!")


def main():
    """Main execution function."""
    print("="*60)
    print("MSSQL CDC Synchronizer - API Example")
    print("="*60)
    
    # Configuration - MODIFY THESE VALUES FOR YOUR ENVIRONMENT
    source_server = "localhost"
    source_database = "SourceDB"
    dest_server = "localhost"
    dest_database = "DestDB"
    
    # Test table configuration
    schema = "dbo"
    table = "TestTable"
    
    # Column mappings - MODIFY FOR YOUR TABLE STRUCTURE
    column_mappings = [
        {
            "source_column": "ID",
            "destination_column": "ID",
            "transformation": None
        },
        {
            "source_column": "Name",
            "destination_column": "Name",
            "transformation": None
        },
        {
            "source_column": "CreatedDate",
            "destination_column": "CreatedDate",
            "transformation": None
        }
    ]
    
    try:
        # Step 1: Test and configure connections
        source_config = test_connection("Source", source_server, source_database)
        if not source_config:
            print("\n✗ Source connection failed. Exiting.")
            return
        
        dest_config = test_connection("Destination", dest_server, dest_database)
        if not dest_config:
            print("\n✗ Destination connection failed. Exiting.")
            return
        
        # Step 2: Set connections in application
        if not set_connection("source", source_config):
            return
        
        if not set_connection("destination", dest_config):
            return
        
        # Step 3: Enable CDC
        enable_database_cdc()
        time.sleep(2)  # Give CDC time to initialize
        
        enable_table_cdc(schema, table)
        time.sleep(2)
        
        # Step 4: Create table mapping
        mapping_id = f"mapping_{schema}_{table}"
        if not create_table_mapping(
            mapping_id, 
            schema, table,
            schema, table,
            column_mappings
        ):
            return
        
        # Step 5: Create working set
        workset_id = "example_workset"
        if not create_working_set(
            workset_id,
            "Example Working Set",
            source_config,
            dest_config,
            [mapping_id]
        ):
            return
        
        # Step 6: Activate working set
        if not activate_working_set(workset_id):
            return
        
        # Step 7: Start synchronization
        if not start_synchronization():
            return
        
        # Step 8: Monitor for a while
        monitor_sync(duration=30)
        
        print("\n" + "="*60)
        print("Example completed successfully!")
        print("="*60)
        print("\nNext steps:")
        print("1. Open http://localhost:8000 in your browser")
        print("2. Go to Monitoring tab to see live events")
        print("3. Make changes in source database to test synchronization")
        print("4. Use Operations tab to stop synchronization when done")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()








