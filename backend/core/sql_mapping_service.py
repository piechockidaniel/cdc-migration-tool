"""SQL Mapping execution service."""
import logging
from typing import List, Dict, Any, Tuple
from backend.models.schemas import Mapping, MappingType
from backend.db.mssql_manager import MSSQLConnection

logger = logging.getLogger(__name__)


class SQLMappingService:
    """Service for executing SQL-based mappings."""
    
    def __init__(self):
        """Initialize SQL mapping service."""
        pass
    
    def execute_mapping(
        self,
        mapping: Mapping,
        source_conn: MSSQLConnection,
        dest_conn: MSSQLConnection
    ) -> Tuple[bool, str, int]:
        """Execute a SQL mapping synchronization.
        
        Args:
            mapping: Unified mapping (must be SQL type)
            source_conn: Source database connection
            dest_conn: Destination database connection
            
        Returns:
            Tuple of (success, message, rows_processed)
        """
        # Validate mapping type
        if mapping.mapping_type != MappingType.SQL:
            return False, f"Mapping {mapping.id} is not a SQL mapping (type: {mapping.mapping_type})", 0
        
        if not mapping.source_query:
            return False, f"SQL mapping {mapping.id} has no source query", 0
        
        try:
            # Ensure connections are active
            if not source_conn.is_connected():
                source_conn.connect()
            if not dest_conn.is_connected():
                dest_conn.connect()
            
            # Execute source query
            logger.info(f"Executing source query for SQL mapping: {mapping.id}")
            source_data = source_conn.execute_query(mapping.source_query)
            
            if not source_data:
                logger.info(f"No data returned from source query for mapping: {mapping.id}")
                return True, "No data to synchronize", 0
            
            rows_processed = len(source_data)
            logger.info(f"Retrieved {rows_processed} rows from source for mapping: {mapping.id}")
            
            # Determine sync mode and execute accordingly
            if mapping.sync_mode == "full":
                success, message = self._execute_full_sync(mapping, source_data, dest_conn)
            elif mapping.sync_mode == "incremental":
                success, message = self._execute_incremental_sync(mapping, source_data, dest_conn)
            elif mapping.sync_mode == "upsert":
                success, message = self._execute_upsert_sync(mapping, source_data, dest_conn)
            else:
                return False, f"Unknown sync mode: {mapping.sync_mode}", 0
            
            if success:
                return True, message, rows_processed
            else:
                return False, message, 0
                
        except Exception as e:
            logger.error(f"Error executing SQL mapping {mapping.id}: {e}")
            return False, str(e), 0
    
    def _execute_full_sync(
        self,
        mapping: Mapping,
        source_data: List[Dict[str, Any]],
        dest_conn: MSSQLConnection
    ) -> Tuple[bool, str]:
        """Execute full sync (truncate and insert all data).
        
        Args:
            mapping: Unified mapping (SQL type)
            source_data: Data from source query
            dest_conn: Destination database connection
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Truncate destination table
            truncate_sql = f"TRUNCATE TABLE [{mapping.destination_schema}].[{mapping.destination_table}]"
            dest_conn.execute_query(truncate_sql)
            logger.info(f"Truncated destination table: {mapping.destination_schema}.{mapping.destination_table}")
            
            # Insert all data
            if source_data:
                rows_inserted = dest_conn.bulk_insert(
                    mapping.destination_schema,
                    mapping.destination_table,
                    source_data,
                    batch_size=mapping.batch_size
                )
                return True, f"Full sync completed: {rows_inserted} rows inserted"
            else:
                return True, "Full sync completed: table truncated (no data to insert)"
                
        except Exception as e:
            logger.error(f"Error in full sync: {e}")
            return False, f"Full sync failed: {str(e)}"
    
    def _execute_incremental_sync(
        self,
        mapping: Mapping,
        source_data: List[Dict[str, Any]],
        dest_conn: MSSQLConnection
    ) -> Tuple[bool, str]:
        """Execute incremental sync (append only).
        
        Args:
            mapping: Unified mapping (SQL type)
            source_data: Data from source query
            dest_conn: Destination database connection
            
        Returns:
            Tuple of (success, message)
        """
        try:
            if not source_data:
                return True, "Incremental sync completed: no new data"
            
            rows_inserted = dest_conn.bulk_insert(
                mapping.destination_schema,
                mapping.destination_table,
                source_data,
                batch_size=mapping.batch_size
            )
            
            return True, f"Incremental sync completed: {rows_inserted} rows inserted"
            
        except Exception as e:
            logger.error(f"Error in incremental sync: {e}")
            return False, f"Incremental sync failed: {str(e)}"
    
    def _execute_upsert_sync(
        self,
        mapping: Mapping,
        source_data: List[Dict[str, Any]],
        dest_conn: MSSQLConnection
    ) -> Tuple[bool, str]:
        """Execute upsert sync (merge/update or insert).
        
        Args:
            mapping: Unified mapping (SQL type)
            source_data: Data from source query
            dest_conn: Destination database connection
            
        Returns:
            Tuple of (success, message)
        """
        try:
            if not source_data:
                return True, "Upsert sync completed: no data to process"
            
            if not mapping.key_columns:
                return False, "Upsert sync requires key_columns to be specified"
            
            # Build MERGE statement for SQL Server
            # This is a simplified version - in production, you'd want more robust handling
            rows_updated = 0
            rows_inserted = 0
            
            # Process in batches
            for i in range(0, len(source_data), mapping.batch_size):
                batch = source_data[i:i + mapping.batch_size]
                
                for row in batch:
                    # Check if row exists based on key columns
                    key_conditions = " AND ".join([
                        f"[{key_col}] = ?" for key_col in mapping.key_columns
                    ])
                    
                    check_sql = f"""
                        SELECT COUNT(*) as cnt
                        FROM [{mapping.destination_schema}].[{mapping.destination_table}]
                        WHERE {key_conditions}
                    """
                    
                    key_values = tuple(row.get(key_col) for key_col in mapping.key_columns)
                    result = dest_conn.execute_query(check_sql, params=key_values)
                    
                    if result and result[0].get('cnt', 0) > 0:
                        # Update existing row
                        if mapping.update_query:
                            # Use custom update query if provided
                            # Note: Custom queries should use proper parameterization
                            update_sql = mapping.update_query
                            # For custom queries, we assume they're properly parameterized
                            # In production, you'd want more robust handling
                            dest_conn.execute_non_query(update_sql)
                        else:
                            # Auto-generate UPDATE statement
                            set_clauses = ", ".join([
                                f"[{col}] = ?" for col in row.keys() if col not in mapping.key_columns
                            ])
                            if set_clauses:  # Only update if there are non-key columns
                                update_sql = f"""
                                    UPDATE [{mapping.destination_schema}].[{mapping.destination_table}]
                                    SET {set_clauses}
                                    WHERE {key_conditions}
                                """
                                update_params = tuple(
                                    v for k, v in row.items() if k not in mapping.key_columns
                                ) + key_values
                                dest_conn.execute_non_query(update_sql, params=update_params)
                        rows_updated += 1
                    else:
                        # Insert new row
                        if mapping.insert_query:
                            # Use custom insert query if provided
                            # Note: Custom queries should use proper parameterization
                            insert_sql = mapping.insert_query
                            dest_conn.execute_non_query(insert_sql)
                        else:
                            # Use bulk insert
                            dest_conn.bulk_insert(
                                mapping.destination_schema,
                                mapping.destination_table,
                                [row],
                                batch_size=1
                            )
                        rows_inserted += 1
            
            return True, f"Upsert sync completed: {rows_inserted} inserted, {rows_updated} updated"
            
        except Exception as e:
            logger.error(f"Error in upsert sync: {e}")
            return False, f"Upsert sync failed: {str(e)}"


# Global SQL mapping service instance
sql_mapping_service = SQLMappingService()

