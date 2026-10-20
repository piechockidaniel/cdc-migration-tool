"""
Advanced Column Mapping Example

This example demonstrates how to create table mappings with:
- Multiple source columns mapped to a single destination column
- JSON transformation for data aggregation
- Concatenation for text fields
- Custom SQL transformations
"""

import requests
import json

API_BASE = "http://localhost:8000/api"


def create_user_profile_mapping():
    """
    Example: Map user data with JSON aggregation
    
    Source: Users table with FirstName, LastName, Email, Phone, Address, City, State
    Destination: UserProfiles table with UserID, PersonalInfo (JSON), ContactInfo (JSON), Location (TEXT)
    """
    
    mapping = {
        "id": "map_users_to_profiles_json",
        "source_schema": "dbo",
        "source_table": "Users",
        "destination_schema": "dbo",
        "destination_table": "UserProfiles",
        "column_mappings": [
            # Simple 1:1 mapping
            {
                "source_column": "UserID",
                "destination_column": "UserID"
            },
            # Multiple columns to JSON (personal info)
            {
                "source_columns": ["FirstName", "LastName", "DateOfBirth"],
                "destination_column": "PersonalInfo",
                "transformation": "JSON_OBJECT('FirstName', FirstName, 'LastName', LastName, 'DateOfBirth', DateOfBirth)",
                "transformation_type": "json"
            },
            # Multiple columns to JSON (contact info)
            {
                "source_columns": ["Email", "Phone", "MobilePhone"],
                "destination_column": "ContactInfo",
                "transformation": "JSON_OBJECT('Email', Email, 'Phone', Phone, 'MobilePhone', MobilePhone)",
                "transformation_type": "json"
            },
            # Concatenation for location
            {
                "source_columns": ["Address", "City", "State", "ZipCode"],
                "destination_column": "Location",
                "transformation": "CONCAT(Address, ', ', City, ', ', State, ' ', ZipCode)",
                "transformation_type": "concat"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": True
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print(f"User Profile Mapping: {response.json()}")


def create_product_pricing_mapping():
    """
    Example: Calculate final price using custom transformation
    
    Source: Products table with BasePrice, TaxRate, DiscountPercent, ShippingCost
    Destination: ProductPricing table with ProductID, FinalPrice
    """
    
    mapping = {
        "id": "map_products_with_pricing",
        "source_schema": "dbo",
        "source_table": "Products",
        "destination_schema": "dbo",
        "destination_table": "ProductPricing",
        "column_mappings": [
            # Simple 1:1 mapping
            {
                "source_column": "ProductID",
                "destination_column": "ProductID"
            },
            {
                "source_column": "ProductName",
                "destination_column": "ProductName"
            },
            # Custom calculation: (BasePrice * (1 + TaxRate) * (1 - DiscountPercent/100)) + ShippingCost
            {
                "source_columns": ["BasePrice", "TaxRate", "DiscountPercent", "ShippingCost"],
                "destination_column": "FinalPrice",
                "transformation": "(BasePrice * (1 + TaxRate) * (1 - DiscountPercent/100)) + ShippingCost",
                "transformation_type": "custom"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": False  # Don't delete pricing records
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print(f"Product Pricing Mapping: {response.json()}")


def create_audit_trail_mapping():
    """
    Example: Create comprehensive audit trail with metadata
    
    Source: Transactions table with various transaction details
    Destination: AuditLog table with TransactionID, AuditData (JSON), Summary (TEXT)
    """
    
    mapping = {
        "id": "map_transactions_to_audit",
        "source_schema": "dbo",
        "source_table": "Transactions",
        "destination_schema": "dbo",
        "destination_table": "AuditLog",
        "column_mappings": [
            {
                "source_column": "TransactionID",
                "destination_column": "TransactionID"
            },
            # Comprehensive JSON for audit data
            {
                "source_columns": [
                    "UserID", 
                    "Amount", 
                    "TransactionType", 
                    "Status", 
                    "CreatedDate",
                    "ModifiedDate",
                    "IPAddress",
                    "DeviceInfo"
                ],
                "destination_column": "AuditData",
                "transformation": (
                    "JSON_OBJECT("
                    "'UserID', UserID, "
                    "'Amount', Amount, "
                    "'TransactionType', TransactionType, "
                    "'Status', Status, "
                    "'CreatedDate', CreatedDate, "
                    "'ModifiedDate', ModifiedDate, "
                    "'IPAddress', IPAddress, "
                    "'DeviceInfo', DeviceInfo"
                    ")"
                ),
                "transformation_type": "json"
            },
            # Human-readable summary
            {
                "source_columns": ["TransactionType", "Amount", "Status", "CreatedDate"],
                "destination_column": "Summary",
                "transformation": "CONCAT(TransactionType, ' of $', CAST(Amount AS VARCHAR), ' - ', Status, ' on ', CAST(CreatedDate AS VARCHAR))",
                "transformation_type": "concat"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": False
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print(f"Audit Trail Mapping: {response.json()}")


def create_customer_360_mapping():
    """
    Example: Create a 360-degree customer view with aggregated data
    
    Source: Multiple normalized tables
    Destination: Customer360 with aggregated JSON data
    """
    
    mapping = {
        "id": "map_customer_360",
        "source_schema": "dbo",
        "source_table": "Customers",
        "destination_schema": "dbo",
        "destination_table": "Customer360",
        "column_mappings": [
            # Identity
            {
                "source_column": "CustomerID",
                "destination_column": "CustomerID"
            },
            # Full name concatenation
            {
                "source_columns": ["FirstName", "MiddleName", "LastName", "Suffix"],
                "destination_column": "FullName",
                "transformation": "CONCAT(FirstName, ' ', ISNULL(MiddleName + ' ', ''), LastName, ISNULL(' ' + Suffix, ''))",
                "transformation_type": "custom"
            },
            # Demographics as JSON
            {
                "source_columns": ["Age", "Gender", "MaritalStatus", "EducationLevel"],
                "destination_column": "Demographics",
                "transformation": "JSON_OBJECT('Age', Age, 'Gender', Gender, 'MaritalStatus', MaritalStatus, 'EducationLevel', EducationLevel)",
                "transformation_type": "json"
            },
            # Preferences as JSON
            {
                "source_columns": ["PreferredLanguage", "PreferredContactMethod", "NewsletterSubscribed"],
                "destination_column": "Preferences",
                "transformation": "JSON_OBJECT('Language', PreferredLanguage, 'ContactMethod', PreferredContactMethod, 'Newsletter', NewsletterSubscribed)",
                "transformation_type": "json"
            }
        ],
        "enabled": True,
        "sync_inserts": True,
        "sync_updates": True,
        "sync_deletes": True
    }
    
    response = requests.post(f"{API_BASE}/admin/mapping/create", json=mapping)
    print(f"Customer 360 Mapping: {response.json()}")


def list_all_mappings():
    """List all configured mappings"""
    response = requests.get(f"{API_BASE}/admin/mapping/list")
    mappings = response.json()
    
    print("\n=== Configured Mappings ===")
    for mapping in mappings:
        print(f"\nMapping ID: {mapping['id']}")
        print(f"  Source: {mapping['source_schema']}.{mapping['source_table']}")
        print(f"  Destination: {mapping['destination_schema']}.{mapping['destination_table']}")
        print(f"  Column Mappings: {len(mapping['column_mappings'])}")
        print(f"  Enabled: {mapping['enabled']}")
        
        # Show transformation details
        for cm in mapping['column_mappings']:
            if cm.get('source_columns') and len(cm['source_columns']) > 1:
                print(f"    - {', '.join(cm['source_columns'])} → {cm['destination_column']}")
                if cm.get('transformation_type'):
                    print(f"      Type: {cm['transformation_type']}")
            else:
                src = cm.get('source_column') or cm.get('source_columns', ['?'])[0]
                print(f"    - {src} → {cm['destination_column']}")


def get_mapping_details(mapping_id):
    """Get detailed information about a specific mapping"""
    response = requests.get(f"{API_BASE}/admin/mapping/{mapping_id}")
    mapping = response.json()
    
    print(f"\n=== Mapping Details: {mapping_id} ===")
    print(json.dumps(mapping, indent=2))


def main():
    """Run all examples"""
    print("Creating Advanced Column Mappings...\n")
    
    # Create different types of mappings
    try:
        create_user_profile_mapping()
    except Exception as e:
        print(f"Error creating user profile mapping: {e}")
    
    try:
        create_product_pricing_mapping()
    except Exception as e:
        print(f"Error creating product pricing mapping: {e}")
    
    try:
        create_audit_trail_mapping()
    except Exception as e:
        print(f"Error creating audit trail mapping: {e}")
    
    try:
        create_customer_360_mapping()
    except Exception as e:
        print(f"Error creating customer 360 mapping: {e}")
    
    # List all mappings
    try:
        list_all_mappings()
    except Exception as e:
        print(f"Error listing mappings: {e}")
    
    print("\n✓ Advanced mapping examples completed!")


if __name__ == "__main__":
    main()

