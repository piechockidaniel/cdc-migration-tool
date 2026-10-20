"""MSSQL database connection manager with connection pooling."""
import pyodbc
from typing import List, Dict, Any, Iterator, Optional, Tuple
from contextlib import contextmanager
import logging
from backend.models.schemas import ConnectionConfig, DatabaseInfo, TableInfo, ColumnInfo

logger = logging.getLogger(__name__)


class MSSQLConnection:
    """MSSQL connection manager with pooling support."""
    
    def __init__(self, config: ConnectionConfig):
        """Initialize connection manager.
        
        Args:
            config: Connection configuration
        """
        self.config = config
        self.connection: Optional[pyodbc.Connection] = None
        self._connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build ODBC connection string.
        
        Returns:
            Connection string
        """
        parts = [
            f"DRIVER={{{self.config.driver}}}"
        ]

        if self.config.port:
            parts.append(f"SERVER={self.config.server},{self.config.port}")
        else:
            parts.append(f"SERVER={self.config.server}")

        parts.append(f"DATABASE={self.config.database}")
        parts.append("Encrypt=Optional")
        
        if self.config.use_windows_auth:
            parts.append("Trusted_Connection=yes")
        else:
            if self.config.username:
                parts.append(f"UID={self.config.username}")
            if self.config.password:
                parts.append(f"PWD={self.config.password}")
        
        # Additional settings for better compatibility
        parts.extend([
            "TrustServerCertificate=Yes",
            # 0 = wait forever; a hung server would block the caller indefinitely
            f"Connection Timeout={int(getattr(self.config, 'login_timeout', 15) or 15)}",
            "APP=cdcsync",
        ])
        # NOTE: never log/print the connection string - it contains the password
        return ";".join(parts)
    
    def connect(self) -> bool:
        """Establish database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connection = pyodbc.connect(self._connection_string)
            logger.info(f"Connected to {self.config.server}/{self.config.database}")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info(f"Disconnected from {self.config.server}/{self.config.database}")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
            finally:
                self.connection = None
    
    def is_connected(self) -> bool:
        """Check if connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        if not self.connection:
            return False
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as ex:
            logger.warning(f"Connection check failed: {ex}")
            return False
    
    @contextmanager
    def get_cursor(self) -> Iterator[Any]:
        """Get database cursor as context manager.

        Yields:
            Database cursor
        """
        if not self.connection:
            if not self.connect():
                raise Exception("Failed to establish database connection")
        
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Query error: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [column[0] for column in cursor.description] if cursor.description else []
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
    
    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """Execute a non-query statement (INSERT, UPDATE, DELETE).
        
        Args:
            query: SQL statement
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount
    
    def bulk_insert(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """Bulk insert data into a table using fast_executemany for performance.
        
        Args:
            schema: Schema name
            table: Table name
            data: List of dictionaries containing row data
            batch_size: Number of rows to insert per batch
            
        Returns:
            Total number of rows inserted
        """
        if not data:
            return 0
        
        try:
            # Get column names from first row
            columns = list(data[0].keys())
            column_str = ', '.join([f"[{col}]" for col in columns])
            placeholders = ', '.join(['?' for _ in columns])
            
            insert_sql = f"INSERT INTO [{schema}].[{table}] ({column_str}) VALUES ({placeholders})"
            
            # Prepare data as list of tuples
            values_list = [tuple(row[col] for col in columns) for row in data]
            
            # Use fast_executemany for better performance
            with self.get_cursor() as cursor:
                # Enable fast_executemany (pyodbc feature for bulk inserts)
                cursor.fast_executemany = True
                
                # Insert in batches
                total_inserted = 0
                for i in range(0, len(values_list), batch_size):
                    batch = values_list[i:i + batch_size]
                    cursor.executemany(insert_sql, batch)
                    total_inserted += len(batch)
                
                return total_inserted
                
        except Exception as e:
            logger.error(f"Error in bulk insert: {e}")
            raise
    
    def get_server_version(self) -> str:
        """Get SQL Server version.
        
        Returns:
            Server version string
        """
        try:
            results = self.execute_query("SELECT @@VERSION AS version")
            return results[0]['version'] if results else "Unknown"
        except Exception as e:
            logger.error(f"Error getting server version: {e}")
            return "Unknown"
    
    def get_databases(self) -> List[DatabaseInfo]:
        """Get list of databases on server.
        
        Returns:
            List of database information
        """
        query = """
        SELECT 
            name,
            CASE WHEN is_cdc_enabled = 1 THEN 1 ELSE 0 END as cdc_enabled
        FROM sys.databases
        WHERE database_id > 4  -- Exclude system databases
        ORDER BY name
        """
        try:
            results = self.execute_query(query)
            return [
                DatabaseInfo(
                    name=row['name'],
                    cdc_enabled=bool(row['cdc_enabled'])
                )
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error getting databases: {e}")
            return []
    
    def get_tables(self, database: Optional[str] = None) -> List[TableInfo]:
        """Get list of tables in database.
        
        Args:
            database: Database name (uses current if None)
            
        Returns:
            List of table information
        """
        query = """
        SELECT
            s.name as schema_name,
            t.name as table_name,
            s.name + '.' + t.name as full_name,
            -- BUGFIX: sys.change_tracking_tables is *Change Tracking*, not CDC.
            CASE WHEN t.is_tracked_by_cdc = 1 THEN 1 ELSE 0 END as cdc_enabled,
            SUM(p.rows) as row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
        WHERE t.is_ms_shipped = 0 AND s.name <> 'cdc'
        GROUP BY s.name, t.name, t.is_tracked_by_cdc
        ORDER BY s.name, t.name
        """
        try:
            if database and database != self.config.database:
                # Switch database temporarily
                original_db = self.config.database
                self.config.database = database
                self.disconnect()
                self.connect()
                results = self.execute_query(query)
                self.config.database = original_db
                self.disconnect()
                self.connect()
            else:
                results = self.execute_query(query)
            
            return [
                TableInfo(
                    schema_name=row['schema_name'],
                    table_name=row['table_name'],
                    full_name=row['full_name'],
                    cdc_enabled=bool(row['cdc_enabled']),
                    row_count=row.get('row_count', 0)
                )
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get list of columns in a table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            List of column information
        """
        query = """
        SELECT 
            c.name as column_name,
            t.name as data_type,
            c.is_nullable,
            c.max_length,
            CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END as is_primary_key
        FROM sys.columns c
        INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
        INNER JOIN sys.tables tb ON c.object_id = tb.object_id
        INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id
        LEFT JOIN (
            SELECT ic.object_id, ic.column_id
            FROM sys.index_columns ic
            INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE i.is_primary_key = 1
        ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
        WHERE s.name = ? AND tb.name = ?
        ORDER BY c.column_id
        """
        try:
            results = self.execute_query(query, (schema_name, table_name))
            return [
                ColumnInfo(
                    column_name=row['column_name'],
                    data_type=row['data_type'],
                    is_nullable=bool(row['is_nullable']),
                    max_length=row.get('max_length'),
                    is_primary_key=bool(row['is_primary_key'])
                )
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error getting columns for {schema_name}.{table_name}: {e}")
            return []
    
    def test_connection(self) -> Tuple[bool, str]:
        """Test database connection.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            if self.connect():
                version = self.get_server_version()
                self.disconnect()
                return True, f"Connected successfully. Server version: {version[:50]}..."
            else:
                return False, "Failed to establish connection"
        except Exception as e:
            return False, str(e)


class ConnectionPool:
    """Connection pool manager for source and destination connections."""
    
    def __init__(self):
        """Initialize connection pool."""
        self.source: Optional[MSSQLConnection] = None
        self.destination: Optional[MSSQLConnection] = None
    
    def set_source(self, config: ConnectionConfig) -> MSSQLConnection:
        """Set source connection.
        
        Args:
            config: Connection configuration
            
        Returns:
            MSSQL connection instance
        """
        if self.source:
            self.source.disconnect()
        self.source = MSSQLConnection(config)
        return self.source
    
    def set_destination(self, config: ConnectionConfig) -> MSSQLConnection:
        """Set destination connection.
        
        Args:
            config: Connection configuration
            
        Returns:
            MSSQL connection instance
        """
        if self.destination:
            self.destination.disconnect()
        self.destination = MSSQLConnection(config)
        return self.destination
    
    def get_source(self) -> Optional[MSSQLConnection]:
        """Get source connection.
        
        Returns:
            Source connection or None
        """
        return self.source
    
    def get_destination(self) -> Optional[MSSQLConnection]:
        """Get destination connection.
        
        Returns:
            Destination connection or None
        """
        return self.destination
    
    def close_all(self):
        """Close all connections."""
        if self.source:
            self.source.disconnect()
            self.source = None
        if self.destination:
            self.destination.disconnect()
            self.destination = None


# Global connection pool instance
connection_pool = ConnectionPool()





