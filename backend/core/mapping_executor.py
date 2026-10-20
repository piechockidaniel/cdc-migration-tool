"""Mapping execution service with transaction support."""
import logging
from typing import List, Dict, Any
from collections import defaultdict
from backend.models.schemas import Mapping, MappingType
from backend.db.mssql_manager import MSSQLConnection
from backend.core.sql_mapping_service import sql_mapping_service

logger = logging.getLogger(__name__)


class MappingExecutor:
    """Service for executing multiple mappings in transactions."""
    
    def __init__(self):
        """Initialize mapping executor."""
        pass
    
    def execute_mappings_transactionally(
        self,
        mappings: List[Mapping],
        source_conn: MSSQLConnection,
        dest_conn: MSSQLConnection
    ) -> Dict[str, Dict[str, Any]]:
        """Execute multiple mappings in transaction groups.
        
        Groups mappings by destination table to handle NOT NULL constraints.
        Each group executes in a single transaction.
        
        Args:
            mappings: List of mappings to execute
            source_conn: Source database connection
            dest_conn: Destination database connection
            
        Returns:
            Dictionary mapping mapping_id -> execution result
        """
        results = {}
        
        if not mappings:
            return results
        
        # Group mappings by destination table
        groups = self._group_mappings_by_destination(mappings)
        
        logger.info(f"Executing {len(mappings)} mappings in {len(groups)} transaction groups")
        
        # Execute each group in a transaction
        for group_key, group_mappings in groups.items():
            logger.info(f"Executing group '{group_key}' with {len(group_mappings)} mappings")
            group_results = self._execute_mapping_group(
                group_mappings,
                source_conn,
                dest_conn
            )
            results.update(group_results)
        
        return results
    
    def _group_mappings_by_destination(self, mappings: List[Mapping]) -> Dict[str, List[Mapping]]:
        """Group mappings by destination table.
        
        Args:
            mappings: List of mappings
            
        Returns:
            Dictionary mapping destination_table -> list of mappings
        """
        groups = defaultdict(list)
        
        for mapping in mappings:
            if not mapping.enabled:
                continue
            
            # Create group key from destination schema and table
            group_key = f"{mapping.destination_schema}.{mapping.destination_table}"
            groups[group_key].append(mapping)
        
        return dict(groups)
    
    def _execute_mapping_group(
        self,
        mappings: List[Mapping],
        source_conn: MSSQLConnection,
        dest_conn: MSSQLConnection
    ) -> Dict[str, Dict[str, Any]]:
        """Execute a group of mappings targeting the same destination table in a single transaction.
        
        Args:
            mappings: List of mappings in the group
            source_conn: Source database connection
            dest_conn: Destination database connection
            
        Returns:
            Dictionary mapping mapping_id -> execution result
        """
        results = {}
        
        # Ensure connections are active
        if not source_conn.is_connected():
            source_conn.connect()
        if not dest_conn.is_connected():
            dest_conn.connect()
        
        # Start transaction - disable autocommit and get cursor directly
        original_autocommit = dest_conn.connection.autocommit
        dest_conn.connection.autocommit = False
        cursor = dest_conn.connection.cursor()
        
        try:
            # Execute each mapping in the group
            for mapping in mappings:
                try:
                    if mapping.mapping_type == MappingType.SQL:
                        # Execute SQL mapping
                        success, message, rows_processed = sql_mapping_service.execute_mapping(
                            mapping,
                            source_conn,
                            dest_conn
                        )
                        results[mapping.id] = {
                            "success": success,
                            "message": message,
                            "rows_processed": rows_processed if rows_processed else 0,
                            "mapping_type": "SQL"
                        }
                        
                        # If mapping failed, rollback entire transaction
                        if not success:
                            raise Exception(f"Mapping {mapping.id} failed: {message}")
                            
                    elif mapping.mapping_type == MappingType.TABLE:
                        # Table mappings are handled by CDC sync engine
                        # For manual execution, we'd need snapshot service
                        # For now, mark as skipped (they're handled by CDC monitor)
                        results[mapping.id] = {
                            "success": True,
                            "message": "Table mapping handled by CDC monitor",
                            "rows_processed": 0,
                            "mapping_type": "TABLE",
                            "skipped": True
                        }
                    else:
                        results[mapping.id] = {
                            "success": False,
                            "message": f"Unknown mapping type: {mapping.mapping_type}",
                            "rows_processed": 0,
                            "mapping_type": str(mapping.mapping_type)
                        }
                        raise Exception(f"Unknown mapping type: {mapping.mapping_type}")
                    
                except Exception as e:
                    logger.error(f"Error executing mapping {mapping.id}: {e}")
                    results[mapping.id] = {
                        "success": False,
                        "message": str(e),
                        "rows_processed": 0,
                        "mapping_type": str(mapping.mapping_type),
                        "error": str(e)
                    }
                    # Rollback transaction on error
                    dest_conn.connection.rollback()
                    raise
            
            # Commit transaction if all mappings succeeded
            dest_conn.connection.commit()
            logger.info(f"Successfully executed {len(mappings)} mappings in transaction")
            
        except Exception as e:
            logger.error(f"Transaction failed for mapping group: {e}")
            dest_conn.connection.rollback()
            # Mark all mappings in group as failed
            for mapping in mappings:
                if mapping.id not in results:
                    results[mapping.id] = {
                        "success": False,
                        "message": f"Transaction failed: {str(e)}",
                        "rows_processed": 0,
                        "mapping_type": str(mapping.mapping_type),
                        "error": str(e)
                    }
        finally:
            # Restore autocommit and close cursor
            cursor.close()
            dest_conn.connection.autocommit = original_autocommit
        
        return results
    
# Global mapping executor instance
mapping_executor = MappingExecutor()
