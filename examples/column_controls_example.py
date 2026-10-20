"""
Column Control Features Example

This example demonstrates:
1. Ignore Changes - Skip synchronization for specific columns
2. Auto-Generate (On Insert) - Generate values on every insert
3. Auto-Generate (On Init) - Generate values only on first insert
"""

import requests
import json

API_BASE = "http://localhost:8000/api"


def example_audit_trail_with_auto_generate():
    """
    Example: Customer sync with auto-generated audit fields
    
    Features:
    - Regular 1:1 column mappings
    - Auto-generated GUID for each sync
    - Auto-generated timestamps (first and last sync)
    - Auto-generated source label
    """
    
    mapping = {
        "id": "customers_with_audit_trail",
        "source_schema": "dbo",
        "source_table": "Customers",
        "destination_schema": "dbo",
        "destination_table": "CustomerAudit",
        "column_mappings": [
            # Regular synced columns
            {
                "source_column": "CustomerID",
                "destination_column": "CustomerID"
            },
            {
                "source_column": "CustomerName",
                "destination_column": "CustomerName"
            },
            {
                "source_column": "Email",
                "destination_column": "Email"
            },
            
            # Auto-generated audit fields
            {
                "destination_column": "SyncGUID",
                "auto_generate": "on_insert",
                "auto_generate_expression": "NEWID()",
                "comment": "New GUID for each sync event"
            },
            {
                "destination_column": "FirstSyncedAt",
                "auto_generate": "on_init",
                "auto_generate_expression": "GETDATE()",
                "comment": "Timestamp of initial sync (never changes)"
            },
            {
                "destination_column": "LastSyncedAt",
                "auto_generate": "on_insert",
                "auto_generate_expression": "GETDATE()",
                "comment": "Timestamp of most recent sync"
            },
            {
                "destination_column": "SyncSource",
                "auto_generate": "on_init",
                "auto_generate_expression": "'CDC_REALTIME'",
                "comment": "Label identifying sync source"
            },
            {
                "destination_column": "SyncUser",
                "auto_generate": "on_insert",
                "auto_generate_expression": "CURRENT_USER",
                "comment": "Database user performing sync"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": True
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print("Audit Trail Mapping:", json.dumps(response.json(), indent=2))


def example_preserve_manual_adjustments():
    """
    Example: Product pricing with manual override protection
    
    Features:
    - Standard fields sync normally
    - Pricing fields with ignore_changes to preserve manual edits
    - Notes field with ignore_changes
    """
    
    mapping = {
        "id": "products_with_manual_overrides",
        "source_schema": "dbo",
        "source_table": "Products",
        "destination_schema": "dbo",
        "destination_table": "ProductCatalog",
        "column_mappings": [
            # Normal sync fields
            {
                "source_column": "ProductID",
                "destination_column": "ProductID"
            },
            {
                "source_column": "ProductName",
                "destination_column": "ProductName"
            },
            {
                "source_column": "Description",
                "destination_column": "Description"
            },
            
            # Standard price syncs normally
            {
                "source_column": "StandardPrice",
                "destination_column": "StandardPrice"
            },
            
            # Actual price - ignore changes to preserve manual adjustments
            {
                "source_column": "StandardPrice",
                "destination_column": "ActualPrice",
                "ignore_changes": True,
                "comment": "Gets initial price but preserves manual adjustments"
            },
            
            # Discount percentage - ignore changes
            {
                "source_column": "DiscountPercent",
                "destination_column": "DiscountPercent",
                "ignore_changes": True,
                "comment": "May be manually adjusted in destination"
            },
            
            # Notes field - ignore changes
            {
                "source_column": "Notes",
                "destination_column": "Notes",
                "ignore_changes": True,
                "comment": "Destination-specific notes, don't overwrite"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": False  # Don't delete products
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print("\nManual Overrides Mapping:", json.dumps(response.json(), indent=2))


def example_skip_large_binary_data():
    """
    Example: Document sync with ignored binary content
    
    Features:
    - Metadata syncs normally
    - Large binary content only synced on initial insert
    - Content hash synced for change detection
    """
    
    mapping = {
        "id": "documents_skip_binary",
        "source_schema": "dbo",
        "source_table": "Documents",
        "destination_schema": "dbo",
        "destination_table": "DocumentArchive",
        "column_mappings": [
            # Metadata fields
            {
                "source_column": "DocumentID",
                "destination_column": "DocumentID"
            },
            {
                "source_column": "DocumentName",
                "destination_column": "DocumentName"
            },
            {
                "source_column": "DocumentType",
                "destination_column": "DocumentType"
            },
            {
                "source_column": "FileSize",
                "destination_column": "FileSize"
            },
            
            # Large binary content - ignore changes (only initial sync)
            {
                "source_column": "DocumentContent",
                "destination_column": "DocumentContent",
                "ignore_changes": True,
                "comment": "Large binary - only sync on initial insert"
            },
            
            # Content hash for change detection
            {
                "source_column": "ContentHash",
                "destination_column": "ContentHash",
                "comment": "Hash changes when content changes"
            },
            
            # Thumbnail - ignore changes
            {
                "source_column": "Thumbnail",
                "destination_column": "Thumbnail",
                "ignore_changes": True,
                "comment": "Binary thumbnail - skip updates"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": False
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print("\nSkip Binary Data Mapping:", json.dumps(response.json(), indent=2))


def example_data_warehouse_with_tracking():
    """
    Example: Data warehouse dimension with SCD Type 2 tracking
    
    Features:
    - Auto-generated surrogate key
    - Auto-generated effective dates
    - Auto-generated tracking flags
    - JSON aggregation of source data
    """
    
    mapping = {
        "id": "dim_customer_scd2",
        "source_schema": "dbo",
        "source_table": "Customers",
        "destination_schema": "dw",
        "destination_table": "DimCustomer",
        "column_mappings": [
            # Surrogate key (auto-generated sequence)
            {
                "destination_column": "CustomerKey",
                "auto_generate": "on_insert",
                "auto_generate_expression": "NEXT VALUE FOR dw.DimCustomerSeq",
                "comment": "Data warehouse surrogate key"
            },
            
            # Business key
            {
                "source_column": "CustomerID",
                "destination_column": "CustomerBusinessKey"
            },
            
            # Customer data as JSON
            {
                "source_columns": ["FirstName", "LastName", "Email", "Phone"],
                "destination_column": "CustomerData",
                "transformation_type": "json",
                "comment": "All customer details as JSON"
            },
            
            # SCD Type 2 tracking fields
            {
                "destination_column": "EffectiveDate",
                "auto_generate": "on_init",
                "auto_generate_expression": "GETDATE()",
                "comment": "When this version became effective"
            },
            {
                "destination_column": "ExpirationDate",
                "auto_generate": "on_init",
                "auto_generate_expression": "'9999-12-31'",
                "comment": "When this version expires (initially far future)"
            },
            {
                "destination_column": "IsCurrent",
                "auto_generate": "on_init",
                "auto_generate_expression": "1",
                "comment": "Flag indicating current version"
            },
            
            # Audit fields
            {
                "destination_column": "LoadDate",
                "auto_generate": "on_insert",
                "auto_generate_expression": "GETDATE()",
                "comment": "ETL load timestamp"
            },
            {
                "destination_column": "SourceSystem",
                "auto_generate": "on_init",
                "auto_generate_expression": "'OLTP_CDC'",
                "comment": "Source system identifier"
            },
            {
                "destination_column": "ETLBatchID",
                "auto_generate": "on_insert",
                "auto_generate_expression": "CONVERT(VARCHAR(20), GETDATE(), 112)",
                "comment": "ETL batch identifier (YYYYMMDD)"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": False
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print("\nData Warehouse SCD2 Mapping:", json.dumps(response.json(), indent=2))


def example_combined_features():
    """
    Example: Complex mapping using all features together
    
    Features:
    - Regular mappings
    - Transformations (JSON, concat)
    - Ignore changes
    - Auto-generate (both modes)
    """
    
    mapping = {
        "id": "employees_full_featured",
        "source_schema": "hr",
        "source_table": "Employees",
        "destination_schema": "analytics",
        "destination_table": "EmployeeView",
        "column_mappings": [
            # Regular sync
            {
                "source_column": "EmployeeID",
                "destination_column": "EmployeeID"
            },
            
            # Concatenation
            {
                "source_columns": ["FirstName", "MiddleName", "LastName"],
                "destination_column": "FullName",
                "transformation_type": "concat"
            },
            
            # JSON aggregation
            {
                "source_columns": ["Email", "Phone", "Mobile", "WorkEmail"],
                "destination_column": "ContactInfo",
                "transformation_type": "json"
            },
            
            # Salary - ignore changes (preserve manual adjustments)
            {
                "source_column": "BaseSalary",
                "destination_column": "AdjustedSalary",
                "ignore_changes": True,
                "comment": "Gets initial value but preserves manual adjustments"
            },
            
            # Photo - ignore changes (large binary)
            {
                "source_column": "EmployeePhoto",
                "destination_column": "EmployeePhoto",
                "ignore_changes": True,
                "comment": "Large binary - skip updates"
            },
            
            # Auto-generated tracking fields
            {
                "destination_column": "ViewGUID",
                "auto_generate": "on_insert",
                "auto_generate_expression": "NEWID()"
            },
            {
                "destination_column": "FirstViewedAt",
                "auto_generate": "on_init",
                "auto_generate_expression": "GETDATE()"
            },
            {
                "destination_column": "LastViewedAt",
                "auto_generate": "on_insert",
                "auto_generate_expression": "GETDATE()"
            },
            {
                "destination_column": "DataSource",
                "auto_generate": "on_init",
                "auto_generate_expression": "'HR_SYSTEM'"
            },
            {
                "destination_column": "ViewVersion",
                "auto_generate": "on_insert",
                "auto_generate_expression": "1"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": True
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print("\nCombined Features Mapping:", json.dumps(response.json(), indent=2))


def view_mapping_details(mapping_id):
    """View detailed information about a mapping"""
    response = requests.get(f"{API_BASE}/admin/mapping/{mapping_id}")
    if response.status_code == 200:
        mapping = response.json()
        print(f"\n=== Mapping: {mapping_id} ===")
        print(f"Source: {mapping['source_schema']}.{mapping['source_table']}")
        print(f"Destination: {mapping['destination_schema']}.{mapping['destination_table']}")
        print(f"\nColumn Mappings ({len(mapping['column_mappings'])}):")
        
        for idx, cm in enumerate(mapping['column_mappings'], 1):
            print(f"\n  {idx}. ", end="")
            
            # Source
            if cm.get('source_columns'):
                print(f"[{', '.join(cm['source_columns'])}]", end="")
            elif cm.get('source_column'):
                print(cm['source_column'], end="")
            else:
                print("(no source)", end="")
            
            print(f" → {cm['destination_column']}")
            
            # Features
            features = []
            if cm.get('transformation_type'):
                features.append(f"Transform: {cm['transformation_type']}")
            if cm.get('ignore_changes'):
                features.append("Ignore Changes: ON")
            if cm.get('auto_generate') and cm['auto_generate'] != 'none':
                features.append(f"Auto-Gen: {cm['auto_generate']}")
                if cm.get('auto_generate_expression'):
                    features.append(f"  Expression: {cm['auto_generate_expression']}")
            
            if features:
                for feature in features:
                    print(f"     {feature}")


def list_all_mappings():
    """List all configured mappings with feature summary"""
    response = requests.get(f"{API_BASE}/admin/mapping/list")
    mappings = response.json()
    
    print("\n" + "="*80)
    print("CONFIGURED MAPPINGS")
    print("="*80)
    
    for mapping in mappings:
        print(f"\n📋 {mapping['id']}")
        print(f"   {mapping['source_schema']}.{mapping['source_table']} → "
              f"{mapping['destination_schema']}.{mapping['destination_table']}")
        
        # Count features
        total_mappings = len(mapping['column_mappings'])
        ignored_count = sum(1 for cm in mapping['column_mappings'] if cm.get('ignore_changes'))
        autogen_count = sum(1 for cm in mapping['column_mappings'] 
                          if cm.get('auto_generate') and cm['auto_generate'] != 'none')
        transformed_count = sum(1 for cm in mapping['column_mappings'] 
                               if cm.get('transformation_type'))
        
        print(f"   📊 {total_mappings} mappings: ", end="")
        features = []
        if ignored_count:
            features.append(f"{ignored_count} ignored")
        if autogen_count:
            features.append(f"{autogen_count} auto-generated")
        if transformed_count:
            features.append(f"{transformed_count} transformed")
        
        if features:
            print(", ".join(features))
        else:
            print("all direct")
        
        print(f"   {'✅ Enabled' if mapping['enabled'] else '❌ Disabled'}")


def main():
    """Run all column control examples"""
    print("=" * 80)
    print("COLUMN CONTROL FEATURES - EXAMPLES")
    print("=" * 80)
    
    print("\n1. Audit Trail with Auto-Generated Fields")
    print("-" * 80)
    try:
        example_audit_trail_with_auto_generate()
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n2. Preserve Manual Adjustments")
    print("-" * 80)
    try:
        example_preserve_manual_adjustments()
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n3. Skip Large Binary Data")
    print("-" * 80)
    try:
        example_skip_large_binary_data()
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n4. Data Warehouse with SCD Type 2 Tracking")
    print("-" * 80)
    try:
        example_data_warehouse_with_tracking()
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n5. Combined Features (All Together)")
    print("-" * 80)
    try:
        example_combined_features()
    except Exception as e:
        print(f"Error: {e}")
    
    # List all mappings
    try:
        list_all_mappings()
    except Exception as e:
        print(f"Error listing mappings: {e}")
    
    print("\n" + "=" * 80)
    print("✅ Examples completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()

