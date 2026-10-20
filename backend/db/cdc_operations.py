"""CDC (Change Data Capture) operations for SQL Server."""
import logging
from typing import List, Dict, Any, Optional, Tuple

from backend.db.mssql_manager import MSSQLConnection

logger = logging.getLogger(__name__)


class CDCOperations:
    """CDC operations manager."""
    
    def __init__(self, connection: MSSQLConnection):
        """Initialize CDC operations.
        
        Args:
            connection: MSSQL connection instance
        """
        self.connection = connection
    
    def is_cdc_enabled_on_database(self) -> bool:
        """Check if CDC is enabled on the database.
        
        Returns:
            True if CDC is enabled, False otherwise
        """
        try:
            query = "SELECT is_cdc_enabled FROM sys.databases WHERE name = ?"
            results = self.connection.execute_query(query, (self.connection.config.database,))
            return bool(results[0]['is_cdc_enabled']) if results else False
        except Exception as e:
            logger.error(f"Error checking CDC status: {e}")
            return False
    
    def enable_cdc_on_database(self) -> Tuple[bool, str]:
        """Enable CDC on the database.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            if self.is_cdc_enabled_on_database():
                return True, "CDC is already enabled on this database"
            
            query = "EXEC sys.sp_cdc_enable_db"
            self.connection.execute_non_query(query)
            
            # Verify CDC was enabled
            if self.is_cdc_enabled_on_database():
                logger.info(f"CDC enabled on database {self.connection.config.database}")
                return True, "CDC enabled successfully on database"
            else:
                return False, "CDC enable command executed but verification failed"
                
        except Exception as e:
            error_msg = f"Error enabling CDC on database: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def is_cdc_enabled_on_table(self, schema_name: str, table_name: str) -> bool:
        """Check if CDC is enabled on a specific table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            True if CDC is enabled on table, False otherwise
        """
        try:
            query = """
            SELECT is_tracked_by_cdc 
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ? AND t.name = ?
            """
            results = self.connection.execute_query(query, (schema_name, table_name))
            return bool(results[0]['is_tracked_by_cdc']) if results else False
        except Exception as e:
            logger.error(f"Error checking CDC status for table {schema_name}.{table_name}: {e}")
            return False
    
    def enable_cdc_on_table(
        self, 
        schema_name: str, 
        table_name: str, 
        capture_instance: Optional[str] = None,
        role_name: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Enable CDC on a specific table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            capture_instance: Optional capture instance name
            role_name: Optional role name for access control
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Check if database CDC is enabled first
            if not self.is_cdc_enabled_on_database():
                return False, "CDC must be enabled on database first"
            
            # Check if already enabled
            if self.is_cdc_enabled_on_table(schema_name, table_name):
                return True, f"CDC is already enabled on table {schema_name}.{table_name}"
            
            # Build the enable command
            if not capture_instance:
                capture_instance = f"{schema_name}_{table_name}"
            
            query = """
            EXEC sys.sp_cdc_enable_table
                @source_schema = ?,
                @source_name = ?,
                @role_name = ?,
                @capture_instance = ?
            """
            
            self.connection.execute_non_query(
                query, 
                (schema_name, table_name, role_name, capture_instance)
            )
            
            # Verify CDC was enabled
            if self.is_cdc_enabled_on_table(schema_name, table_name):
                logger.info(f"CDC enabled on table {schema_name}.{table_name}")
                return True, f"CDC enabled successfully on table {schema_name}.{table_name}"
            else:
                return False, "CDC enable command executed but verification failed"
                
        except Exception as e:
            error_msg = f"Error enabling CDC on table {schema_name}.{table_name}: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def get_cdc_enabled_tables(self) -> List[Dict[str, Any]]:
        """Get list of tables with CDC enabled.
        
        Returns:
            List of CDC-enabled tables with their capture instances
        """
        try:
            query = """
            SELECT 
                ct.source_schema,
                ct.source_table,
                ct.capture_instance,
                ct.start_lsn,
                ct.create_date,
                ct.role_name
            FROM cdc.change_tables ct
            ORDER BY ct.source_schema, ct.source_table
            """
            return self.connection.execute_query(query)
        except Exception as e:
            logger.error(f"Error getting CDC-enabled tables: {e}")
            return []
    
    def get_cdc_changes(
        self,
        schema_name: str,
        table_name: str,
        from_lsn: Optional[bytes] = None,
        to_lsn: Optional[bytes] = None,
        row_filter: str = "all"
    ) -> List[Dict[str, Any]]:
        """Get CDC changes for a table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            from_lsn: Starting LSN (None for minimum LSN)
            to_lsn: Ending LSN (None for maximum LSN)
            row_filter: Row filter option ('all', 'all update old', 'all update new')
            
        Returns:
            List of CDC changes
        """
        try:
            capture_instance = f"{schema_name}_{table_name}"
            
            # Get LSN bounds if not provided
            if from_lsn is None:
                lsn_query = "SELECT sys.fn_cdc_get_min_lsn(?) as min_lsn"
                result = self.connection.execute_query(lsn_query, (capture_instance,))
                from_lsn = result[0]['min_lsn'] if result else None
            
            if to_lsn is None:
                lsn_query = "SELECT sys.fn_cdc_get_max_lsn() as max_lsn"
                result = self.connection.execute_query(lsn_query)
                to_lsn = result[0]['max_lsn'] if result else None
            
            if from_lsn is None or to_lsn is None:
                return []
            
            # Query CDC changes
            query = f"""
            SELECT *
            FROM cdc.fn_cdc_get_all_changes_{capture_instance}(?, ?, ?)
            ORDER BY __$start_lsn, __$seqval
            """
            
            return self.connection.execute_query(query, (from_lsn, to_lsn, row_filter))
            
        except Exception as e:
            logger.error(f"Error getting CDC changes for {schema_name}.{table_name}: {e}")
            return []
    
    def get_min_lsn(self, capture_instance: str) -> Optional[bytes]:
        """Get minimum LSN for a capture instance.
        
        Args:
            capture_instance: Capture instance name
            
        Returns:
            Minimum LSN or None
        """
        try:
            query = "SELECT sys.fn_cdc_get_min_lsn(?) as min_lsn"
            result = self.connection.execute_query(query, (capture_instance,))
            return result[0]['min_lsn'] if result else None
        except Exception as e:
            logger.error(f"Error getting min LSN: {e}")
            return None
    
    def get_max_lsn(self) -> Optional[bytes]:
        """Get maximum LSN for the database.
        
        Returns:
            Maximum LSN or None
        """
        try:
            query = "SELECT sys.fn_cdc_get_max_lsn() as max_lsn"
            result = self.connection.execute_query(query)
            return result[0]['max_lsn'] if result else None
        except Exception as e:
            logger.error(f"Error getting max LSN: {e}")
            return None
    
    def increment_lsn(self, lsn: bytes) -> Optional[bytes]:
        """Increment LSN to get the next value.
        
        Args:
            lsn: Current LSN
            
        Returns:
            Next LSN or None
        """
        try:
            query = "SELECT sys.fn_cdc_increment_lsn(?) as next_lsn"
            result = self.connection.execute_query(query, (lsn,))
            return result[0]['next_lsn'] if result else None
        except Exception as e:
            logger.error(f"Error incrementing LSN: {e}")
            return None








