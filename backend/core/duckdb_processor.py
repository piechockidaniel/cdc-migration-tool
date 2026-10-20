"""DuckDB processor for data staging and transformation."""
import logging
import threading
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Any

import duckdb

from backend.models.schemas import Mapping

logger = logging.getLogger(__name__)


def infer_duckdb_type(sample: Any) -> str:
    """Map a Python value to a DuckDB column type (keeps numbers/dates typed)."""
    if sample is None:
        return "VARCHAR"
    if isinstance(sample, bool):
        return "BOOLEAN"
    if isinstance(sample, int):
        return "BIGINT"
    if isinstance(sample, float):
        return "DOUBLE"
    if isinstance(sample, Decimal):
        return "DECIMAL(38, 10)"
    if isinstance(sample, datetime):
        return "TIMESTAMP"
    if isinstance(sample, date):
        return "DATE"
    if isinstance(sample, (bytes, bytearray, memoryview)):
        return "BLOB"
    return "VARCHAR"


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class DuckDBProcessor:
    """DuckDB processor for in-memory data operations."""
    
    def __init__(self):
        """Initialize DuckDB processor with in-memory database."""
        self.conn = duckdb.connect(':memory:')
        self._lock = threading.RLock()
        logger.info("Initialized DuckDB in-memory database")

    @staticmethod
    def quote(name: str) -> str:
        return quote_ident(name)
    
    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")
    
    def create_staging_table(self, table_name: str, columns: Dict[str, str]):
        """Create a staging table in DuckDB.
        
        Args:
            table_name: Name for the staging table
            columns: Dictionary of column names to data types
        """
        try:
            with self._lock:
                # Drop table if exists
                self.conn.execute(f"DROP TABLE IF EXISTS {quote_ident(table_name)}")

                # Create table
                column_defs = [f"{quote_ident(name)} {dtype}" for name, dtype in columns.items()]
                create_sql = f"CREATE TABLE {quote_ident(table_name)} ({', '.join(column_defs)})"
                self.conn.execute(create_sql)
            
            logger.debug(f"Created staging table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating staging table {table_name}: {e}")
            raise
    
    def load_data(self, table_name: str, data: List[Dict[str, Any]]):
        """Load data into a staging table.
        
        Args:
            table_name: Name of the staging table
            data: List of data rows as dictionaries
        """
        try:
            if not data:
                return
            
            # Insert data
            columns = list(data[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = (f"INSERT INTO {quote_ident(table_name)} ({', '.join(quote_ident(c) for c in columns)}) "
                          f"VALUES ({placeholders})")

            values = [tuple(row.get(col) for col in columns) for row in data]
            with self._lock:
                self.conn.executemany(insert_sql, values)
            
            logger.debug(f"Loaded {len(data)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            raise
    
    def transform_data(
        self, 
        source_table: str,
        mapping: Mapping
    ) -> List[Dict[str, Any]]:
        """Transform data according to column mappings.
        
        Args:
            source_table: Name of the source staging table
            mapping: Table mapping configuration
            
        Returns:
            List of transformed data rows
        """
        try:
            # Build SELECT statement with column mappings
            select_columns = []
            for col_mapping in mapping.column_mappings:
                if col_mapping.transformation:
                    # Use transformation expression
                    select_columns.append(
                        f"{col_mapping.transformation} AS {col_mapping.destination_column}"
                    )
                else:
                    # Direct mapping
                    select_columns.append(
                        f"{col_mapping.source_column} AS {col_mapping.destination_column}"
                    )
            
            query = f"SELECT {', '.join(select_columns)} FROM {quote_ident(source_table)}"
            with self._lock:
                result = self.conn.execute(query).fetchall()
            
            # Convert to list of dictionaries
            columns = [col_mapping.destination_column for col_mapping in mapping.column_mappings]
            transformed_data = [dict(zip(columns, row)) for row in result]
            
            logger.debug(f"Transformed {len(transformed_data)} rows")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise
    
    @staticmethod
    def generate_insert_sql(
            schema: str,
        table: str,
        data: Dict[str, Any]
    ) -> tuple[str, tuple]:
        """Generate INSERT SQL statement.
        
        Args:
            schema: Destination schema
            table: Destination table
            data: Data row as dictionary
            
        Returns:
            Tuple of (SQL statement, parameters)
        """
        columns = list(data.keys())
        column_str = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        
        sql = f"INSERT INTO {schema}.{table} ({column_str}) VALUES ({placeholders})"
        params = tuple(data[col] for col in columns)
        
        return sql, params
    
    @staticmethod
    def generate_update_sql(
            schema: str,
        table: str,
        data: Dict[str, Any],
        primary_keys: List[str]
    ) -> tuple[str, tuple]:
        """Generate UPDATE SQL statement.
        
        Args:
            schema: Destination schema
            table: Destination table
            data: Data row as dictionary
            primary_keys: List of primary key column names
            
        Returns:
            Tuple of (SQL statement, parameters)
        """
        # SET clause columns (exclude primary keys)
        set_columns = [col for col in data.keys() if col not in primary_keys]
        set_clause = ', '.join([f"{col} = ?" for col in set_columns])
        
        # WHERE clause
        where_clause = ' AND '.join([f"{pk} = ?" for pk in primary_keys])
        
        sql = f"UPDATE {schema}.{table} SET {set_clause} WHERE {where_clause}"
        
        # Parameters: set values + primary key values
        params = tuple(data[col] for col in set_columns) + tuple(data[pk] for pk in primary_keys)
        
        return sql, params
    
    @staticmethod
    def generate_delete_sql(
            schema: str,
        table: str,
        data: Dict[str, Any],
        primary_keys: List[str]
    ) -> tuple[str, tuple]:
        """Generate DELETE SQL statement.
        
        Args:
            schema: Destination schema
            table: Destination table
            data: Data row as dictionary (must contain primary key values)
            primary_keys: List of primary key column names
            
        Returns:
            Tuple of (SQL statement, parameters)
        """
        where_clause = ' AND '.join([f"{pk} = ?" for pk in primary_keys])
        sql = f"DELETE FROM {schema}.{table} WHERE {where_clause}"
        params = tuple(data[pk] for pk in primary_keys)
        
        return sql, params
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of result rows as dictionaries
        """
        try:
            with self._lock:
                cur = self.conn.execute(query)
                result = cur.fetchall()
                if not cur.description:
                    return []
                columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in result]
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
# Global DuckDB processor instance
duckdb_processor = DuckDBProcessor()








