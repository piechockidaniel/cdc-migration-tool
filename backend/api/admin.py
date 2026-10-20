"""Administration API endpoints."""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
import logging
from pydantic import BaseModel, Field
from backend.models.schemas import (
    ConnectionConfig, ConnectionTestResponse, DatabaseInfo,
    TableInfo, ColumnInfo, CDCEnableRequest, CDCStatusResponse,
    Mapping, MappingType, WorkingSet, ConnectionType,
    IngestionAnalysisRequest, PatternTestRequest, PatternTestResult
)
from backend.db.mssql_manager import MSSQLConnection, connection_pool
from backend.db.cdc_operations import CDCOperations
from backend.core.config_manager import config_manager
from backend.core.mapping_manager import mapping_manager
from backend.core.duckdb_script_manager import duckdb_script_manager
from backend.core.workflow_manager import workflow_manager
from backend.core.workflow_converter import workflow_converter
from backend.models.workflow_schemas import (
    VisualWorkflow, WorkflowListItem, WorkflowValidationResult,
    WorkflowCompileResult
)

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response models for DuckDB scripts
class ScriptValidationRequest(BaseModel):
    content: str


class ScriptValidationResponse(BaseModel):
    valid: bool
    message: str


# Connection Management

@router.post("/connect/test")
async def test_connection(config: ConnectionConfig) -> ConnectionTestResponse:
    """Test a database connection.
    
    Args:
        config: Connection configuration
        
    Returns:
        Connection test response
    """
    try:
        if config.engine.lower() not in ("mssql", "sqlserver"):
            # non SQL Server engines go through the connector layer (blocking driver call off the loop)
            import asyncio
            from backend.core import runtime
            connector = runtime.connector_from_config(config)
            try:
                success, message = await asyncio.to_thread(connector.test_connection)
                version = await asyncio.to_thread(connector.server_version) if success else None
            finally:
                await asyncio.to_thread(connector.disconnect)
            return ConnectionTestResponse(success=success, message=message, server_version=version)

        conn = MSSQLConnection(config)
        success, message = conn.test_connection()

        return ConnectionTestResponse(
            success=success,
            message=message,
            server_version=conn.get_server_version() if success else None
        )
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        return ConnectionTestResponse(
            success=False,
            message=str(e)
        )


@router.post("/connect/set")
async def set_connection(connection_type: ConnectionType, config: ConnectionConfig):
    """Set source or destination connection.
    
    Args:
        connection_type: Type of connection (source/destination)
        config: Connection configuration
        
    Returns:
        Success message
    """
    try:
        if connection_type == ConnectionType.SOURCE:
            connection_pool.set_source(config)
            logger.info(f"Set source connection: {config.name}")
        else:
            connection_pool.set_destination(config)
            logger.info(f"Set destination connection: {config.name}")
        
        return {"success": True, "message": f"{connection_type.value} connection set successfully"}
    except Exception as e:
        logger.error(f"Error setting connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Database Scanning

@router.get("/scan/databases")
async def get_databases(connection_type: ConnectionType) -> List[DatabaseInfo]:
    """Get list of databases.
    
    Args:
        connection_type: Type of connection to scan
        
    Returns:
        List of databases
    """
    try:
        conn = connection_pool.source if connection_type == ConnectionType.SOURCE else connection_pool.destination
        if not conn:
            raise HTTPException(status_code=400, detail=f"{connection_type.value} connection not set")
        
        if not conn.is_connected():
            conn.connect()
        
        return conn.get_databases()
    except Exception as e:
        logger.error(f"Error getting databases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/tables")
async def get_tables(connection_type: ConnectionType, database: Optional[str] = None) -> List[TableInfo]:
    """Get list of tables in database.
    
    Args:
        connection_type: Type of connection to scan
        database: Database name (optional)
        
    Returns:
        List of tables
    """
    try:
        conn = connection_pool.source if connection_type == ConnectionType.SOURCE else connection_pool.destination
        if not conn:
            raise HTTPException(status_code=400, detail=f"{connection_type.value} connection not set")
        
        if not conn.is_connected():
            conn.connect()
        
        return conn.get_tables(database)
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scan/columns")
async def get_columns(
    connection_type: ConnectionType,
    schema_name: str,
    table_name: str
) -> List[ColumnInfo]:
    """Get list of columns in table.
    
    Args:
        connection_type: Type of connection to scan
        schema_name: Schema name
        table_name: Table name
        
    Returns:
        List of columns
    """
    try:
        conn = connection_pool.source if connection_type == ConnectionType.SOURCE else connection_pool.destination
        if not conn:
            raise HTTPException(status_code=400, detail=f"{connection_type.value} connection not set")
        
        if not conn.is_connected():
            conn.connect()
        
        return conn.get_columns(schema_name, table_name)
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# CDC Management

@router.post("/cdc/enable-db")
async def enable_cdc_database(request: CDCEnableRequest):
    """Enable CDC on database.
    
    Args:
        request: CDC enable request
        
    Returns:
        Success response
    """
    try:
        conn = connection_pool.source if request.connection_type == ConnectionType.SOURCE else connection_pool.destination
        if not conn:
            raise HTTPException(status_code=400, detail=f"{request.connection_type.value} connection not set")
        
        if not conn.is_connected():
            conn.connect()
        
        cdc_ops = CDCOperations(conn)
        success, message = cdc_ops.enable_cdc_on_database()
        
        if not success:
            raise HTTPException(status_code=500, detail=message)
        
        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling CDC on database: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cdc/enable-table")
async def enable_cdc_table(request: CDCEnableRequest):
    """Enable CDC on table.
    
    Args:
        request: CDC enable request
        
    Returns:
        Success response
    """
    try:
        if not request.schema_name or not request.table_name:
            raise HTTPException(status_code=400, detail="schema_name and table_name are required")
        
        conn = connection_pool.source if request.connection_type == ConnectionType.SOURCE else connection_pool.destination
        if not conn:
            raise HTTPException(status_code=400, detail=f"{request.connection_type.value} connection not set")
        
        if not conn.is_connected():
            conn.connect()
        
        cdc_ops = CDCOperations(conn)
        success, message = cdc_ops.enable_cdc_on_table(
            request.schema_name,
            request.table_name,
            request.capture_instance
        )
        
        if not success:
            raise HTTPException(status_code=500, detail=message)
        
        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling CDC on table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cdc/status")
async def get_cdc_status(connection_type: ConnectionType) -> CDCStatusResponse:
    """Get CDC status for database.
    
    Args:
        connection_type: Type of connection
        
    Returns:
        CDC status response
    """
    try:
        conn = connection_pool.source if connection_type == ConnectionType.SOURCE else connection_pool.destination
        if not conn:
            raise HTTPException(status_code=400, detail=f"{connection_type.value} connection not set")
        
        if not conn.is_connected():
            conn.connect()
        
        cdc_ops = CDCOperations(conn)
        cdc_enabled = cdc_ops.is_cdc_enabled_on_database()
        tables = cdc_ops.get_cdc_enabled_tables() if cdc_enabled else []
        
        return CDCStatusResponse(
            database=conn.config.database,
            cdc_enabled=cdc_enabled,
            tables=tables
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting CDC status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# CDC Operations with Connection Config (for connection library)

class CDCConnectionRequest(BaseModel):
    """Request model for CDC operations with connection details."""
    connection: ConnectionConfig

class CDCTableRequest(BaseModel):
    """Request model for CDC table operations with connection details."""
    connection: ConnectionConfig
    schema_name: str
    table_name: str
    capture_instance: Optional[str] = None


@router.post("/cdc/check-status")
async def check_cdc_status_with_connection(request: CDCConnectionRequest):
    """Check CDC status for a specific connection.

    Args:
        request: Request with connection details

    Returns:
        CDC status response
    """
    temp_conn = None
    try:
        temp_conn = MSSQLConnection(request.connection)
        success, message = temp_conn.test_connection()
        if not success:
            raise HTTPException(status_code=400, detail=f"Connection failed: {message}")

        if not temp_conn.is_connected():
            temp_conn.connect()

        cdc_ops = CDCOperations(temp_conn)
        cdc_enabled = cdc_ops.is_cdc_enabled_on_database()
        tables = cdc_ops.get_cdc_enabled_tables() if cdc_enabled else []

        return {
            "database": request.connection.database,
            "cdc_enabled": cdc_enabled,
            "tables": tables
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking CDC status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_conn:
            temp_conn.disconnect()


@router.post("/cdc/enable-database")
async def enable_cdc_on_database_with_connection(request: CDCConnectionRequest):
    """Enable CDC on database using provided connection.

    Args:
        request: Request with connection details

    Returns:
        Success response
    """
    temp_conn = None
    try:
        temp_conn = MSSQLConnection(request.connection)
        success, message = temp_conn.test_connection()
        if not success:
            raise HTTPException(status_code=400, detail=f"Connection failed: {message}")

        if not temp_conn.is_connected():
            temp_conn.connect()

        cdc_ops = CDCOperations(temp_conn)
        success, message = cdc_ops.enable_cdc_on_database()

        if not success:
            raise HTTPException(status_code=500, detail=message)

        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling CDC on database: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_conn:
            temp_conn.disconnect()


@router.post("/cdc/enable-table-with-connection")
async def enable_cdc_on_table_with_connection(request: CDCTableRequest):
    """Enable CDC on table using provided connection.

    Args:
        request: Request with connection and table details

    Returns:
        Success response
    """
    temp_conn = None
    try:
        if not request.schema_name or not request.table_name:
            raise HTTPException(status_code=400, detail="schema_name and table_name are required")

        temp_conn = MSSQLConnection(request.connection)
        success, message = temp_conn.test_connection()
        if not success:
            raise HTTPException(status_code=400, detail=f"Connection failed: {message}")

        if not temp_conn.is_connected():
            temp_conn.connect()

        cdc_ops = CDCOperations(temp_conn)
        success, message = cdc_ops.enable_cdc_on_table(
            request.schema_name,
            request.table_name,
            request.capture_instance
        )

        if not success:
            raise HTTPException(status_code=500, detail=message)

        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling CDC on table: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_conn:
            temp_conn.disconnect()


# Unified Mapping Management

@router.post("/mapping/create")
async def create_mapping(mapping: Mapping):
    """Create a new unified mapping (table or SQL based).
    
    Args:
        mapping: Unified mapping configuration
        
    Returns:
        Success response
    """
    try:
        success, message = mapping_manager.create_mapping(mapping)
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {"success": True, "message": message, "mapping_id": mapping.id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mapping/list")
async def list_mappings(mapping_type: Optional[str] = None) -> List[Mapping]:
    """Get all unified mappings, optionally filtered by type.
    
    Args:
        mapping_type: Optional filter by mapping type ('table' or 'sql')
        
    Returns:
        List of unified mappings
    """
    try:
        filter_type = None
        if mapping_type:
            try:
                filter_type = MappingType(mapping_type.lower())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid mapping_type: {mapping_type}. Must be 'table' or 'sql'")
        
        return mapping_manager.list_mappings(filter_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mapping/{mapping_id}")
async def get_mapping(mapping_id: str) -> Mapping:
    """Get a unified mapping by ID.
    
    Args:
        mapping_id: Mapping ID
        
    Returns:
        Unified mapping
    """
    try:
        mapping = mapping_manager.get_mapping(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mapping/{mapping_id}/history")
async def mapping_history(mapping_id: str, limit: int = 20):
    """Previous versions of a mapping (who changed what, when)."""
    if config_manager.get_mapping(mapping_id) is None:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return {"mapping_id": mapping_id, "version": config_manager.get_mapping_version(mapping_id),
            "history": config_manager.get_mapping_history(mapping_id, limit)}


@router.put("/mapping/update")
async def update_mapping(mapping: Mapping):
    """Update a unified mapping.
    
    Args:
        mapping: Updated unified mapping
        
    Returns:
        Success response
    """
    try:
        success, message = mapping_manager.update_mapping(mapping)
        if not success:
            raise HTTPException(status_code=400, detail=message)
        
        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/mapping/{mapping_id}")
async def delete_mapping(mapping_id: str):
    """Delete a unified mapping.
    
    Args:
        mapping_id: Mapping ID to delete
        
    Returns:
        Success response
    """
    try:
        success, message = mapping_manager.delete_mapping(mapping_id)
        if not success:
            raise HTTPException(status_code=404, detail=message)
        
        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mapping/{mapping_id}/test")
async def test_mapping(mapping_id: str):
    """Test a mapping by executing the source query (for SQL) or validating configuration (for TABLE).
    
    Args:
        mapping_id: Mapping ID to test
        
    Returns:
        Test results
    """
    try:
        mapping = mapping_manager.get_mapping(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        if mapping.mapping_type == MappingType.SQL:
            # Test SQL mapping query
            active_workset = config_manager.get_active_workset()
            if not active_workset:
                raise HTTPException(status_code=400, detail="No active working set")
            
            source_conn = connection_pool.set_source(active_workset.source_connection)
            if not source_conn.is_connected():
                source_conn.connect()
            
            test_query = f"SELECT TOP 10 * FROM ({mapping.source_query}) AS subquery"
            try:
                results = source_conn.execute_query(test_query)
                return {
                    "success": True,
                    "row_count": len(results),
                    "sample_data": results[:5],
                    "columns": list(results[0].keys()) if results else []
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e)
                }
        else:
            # For TABLE mappings, just validate configuration
            is_valid, errors = mapping_manager.validate_mapping(mapping)
            return {
                "success": is_valid,
                "errors": errors if not is_valid else [],
                "message": "Configuration valid" if is_valid else "Configuration has errors"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class QueryTestRequest(BaseModel):
    """Request model for testing a query with connection details."""
    connection: ConnectionConfig
    query: str
    limit: int = 10


@router.post("/mapping/test-query")
async def test_query_with_connection(request: QueryTestRequest):
    """Test a SQL query with provided connection details.

    Used by the mapping wizard to test queries before creating the mapping.

    Args:
        request: Query test request with connection and query

    Returns:
        Test results with sample data
    """
    try:
        # Create temporary connection
        temp_conn = MSSQLConnection(request.connection)

        # Test connection first
        success, message = temp_conn.test_connection()
        if not success:
            return {
                "success": False,
                "error": f"Connection failed: {message}"
            }

        # Connect and execute query
        if not temp_conn.is_connected():
            temp_conn.connect()

        # Wrap query in SELECT TOP to limit results
        test_query = f"SELECT TOP {request.limit} * FROM ({request.query}) AS test_subquery"

        try:
            results = temp_conn.execute_query(test_query)
            return {
                "success": True,
                "row_count": len(results),
                "sample_data": results[:5] if results else [],
                "columns": list(results[0].keys()) if results and len(results) > 0 else [],
                "message": f"Query executed successfully. Returned {len(results)} row(s)."
            }
        except Exception as query_error:
            return {
                "success": False,
                "error": f"Query execution failed: {str(query_error)}"
            }
        finally:
            # Clean up connection
            temp_conn.disconnect()

    except Exception as e:
        logger.error(f"Error testing query: {e}")
        return {
            "success": False,
            "error": f"Test failed: {str(e)}"
        }


# Multi-Source Mapping Support

class MergePreviewRequest(BaseModel):
    """Request model for previewing a multi-source merge."""
    mapping: Mapping
    connection_id: Optional[str] = Field(None, description="Connection ID to use for preview (will use first source connection if not provided)")
    limit: int = Field(default=10, description="Number of preview rows to return")


@router.post("/mapping/preview-merge")
async def preview_multi_source_merge(request: MergePreviewRequest):
    """Preview the results of a multi-source merge pattern.

    Args:
        request: Merge preview request

    Returns:
        Preview results with sample data
    """
    try:
        from backend.core.multi_source_executor import MergePatternExecutor

        # Validate it's a multi-source mapping
        if not request.mapping.is_multi_source:
            return {
                "success": False,
                "error": "Mapping is not multi-source"
            }

        # Get connection to use (first source if not specified)
        connection_id = request.connection_id
        if not connection_id and request.mapping.sources:
            connection_id = request.mapping.sources[0].connection_id

        if not connection_id:
            return {
                "success": False,
                "error": "No connection specified and no sources configured"
            }

        # Load connection
        conn_data = config_manager.get_connection(connection_id)
        if not conn_data:
            return {
                "success": False,
                "error": f"Connection '{connection_id}' not found"
            }

        # Create connection config
        conn_config = ConnectionConfig(**conn_data)
        temp_conn = MSSQLConnection(conn_config)

        try:
            # Test connection
            success, message = temp_conn.test_connection()
            if not success:
                return {
                    "success": False,
                    "error": f"Connection failed: {message}"
                }

            # Execute merge preview
            executor = MergePatternExecutor({})
            result = executor.execute_merge_preview(
                request.mapping,
                temp_conn,
                limit=request.limit
            )

            return result

        finally:
            temp_conn.disconnect()

    except Exception as e:
        logger.error(f"Error previewing multi-source merge: {e}")
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/mapping/validate-merge")
async def validate_merge_pattern(mapping: Mapping):
    """Validate a multi-source merge pattern.

    Args:
        mapping: Mapping to validate

    Returns:
        Validation result
    """
    try:
        from backend.core.multi_source_executor import MergePatternExecutor

        executor = MergePatternExecutor({})
        is_valid, message = executor.validate_merge_pattern(mapping)

        return {
            "success": is_valid,
            "message": message,
            "is_valid": is_valid
        }

    except Exception as e:
        logger.error(f"Error validating merge pattern: {e}")
        return {
            "success": False,
            "is_valid": False,
            "message": str(e)
        }


# Connection Library Management

@router.get("/connection/list")
async def list_connections():
    """Get all saved connections from the library.

    Returns:
        List of saved connections
    """
    try:
        from backend.storage.secrets import redact
        return [redact(c) for c in config_manager.get_all_connections()]   # never expose secrets
    except Exception as e:
        logger.error(f"Error listing connections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connection/{connection_id}")
async def get_connection(connection_id: str):
    """Get a specific connection by ID.

    Args:
        connection_id: Connection ID

    Returns:
        Connection data
    """
    try:
        from backend.storage.secrets import redact
        connection = config_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        return redact(connection)   # the UI sends '***' back unchanged -> existing secret is kept
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/connection/save")
async def save_connection(connection: ConnectionConfig):
    """Save a connection to the library.

    Args:
        connection: Connection configuration

    Returns:
        Success response
    """
    try:
        connection_data = connection.model_dump(mode='json')
        # Use connection name as ID if not provided (BUGFIX: the model had no 'id' field, so a custom id was silently ignored)
        connection_id = connection_data.get('id') or connection.name.lower().replace(' ', '_')
        connection_data['id'] = connection_id

        success, message = config_manager.save_connection(connection_id, connection_data)

        if not success:
            raise HTTPException(status_code=400, detail=message)

        return {"success": True, "message": message, "connection_id": connection_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/connection/update")
async def update_connection(connection: ConnectionConfig):
    """Update an existing connection.

    Args:
        connection: Updated connection configuration

    Returns:
        Success response
    """
    try:
        connection_data = connection.model_dump(mode='json')
        connection_id = connection_data.get('id') or connection.name.lower().replace(' ', '_')
        connection_data['id'] = connection_id

        # a redacted password coming back from the edit form means "keep the stored one"
        existing = config_manager.get_connection(connection_id) or {}
        for key in ("password",):
            if connection_data.get(key) == "***":
                connection_data[key] = existing.get(key)

        success, message = config_manager.update_connection(connection_id, connection_data)

        if not success:
            raise HTTPException(status_code=400, detail=message)

        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/connection/{connection_id}")
async def delete_connection(connection_id: str):
    """Delete a connection from the library.

    Args:
        connection_id: Connection ID to delete

    Returns:
        Success response
    """
    try:
        success, message = config_manager.delete_connection(connection_id)

        if not success:
            raise HTTPException(status_code=404, detail=message)

        return {"success": True, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Working Set Management

@router.post("/workset/create")
async def create_workset(workset: WorkingSet):
    """Create a new working set.
    
    Args:
        workset: Working set configuration
        
    Returns:
        Success response
    """
    try:
        try:
            config_manager.resolve_workset(workset)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if config_manager.create_workset(workset):
            return {"success": True, "message": "Working set created", "workset_id": workset.id}
        else:
            raise HTTPException(status_code=400, detail="Working set ID already exists")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating working set: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/workset/update")
async def update_workset(workset: WorkingSet):
    """Update an existing working set."""
    try:
        try:
            config_manager.resolve_workset(workset)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if not config_manager.update_workset(workset):
            raise HTTPException(status_code=404, detail="Working set not found")
        return {"success": True, "message": "Working set updated", "workset_id": workset.id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating working set: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workset/list")
async def list_worksets() -> List[WorkingSet]:
    """Get all working sets.
    
    Returns:
        List of working sets
    """
    try:
        out = []
        for ws in config_manager.get_all_worksets():
            for side in ("source_connection", "destination_connection"):
                c = getattr(ws, side)
                if c is not None and c.password and not str(c.password).startswith("${"):
                    c.password = "***"
            out.append(ws)
        return out
    except Exception as e:
        logger.error(f"Error listing working sets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workset/active")
async def get_active_workset():
    # NOTE: declared before /workset/{workset_id} - the 1.x order made "active" match as an id (always 404).
    """Get active working set configuration.
    
    Returns:
        Active working set or None
    """
    try:
        active_workset = config_manager.get_active_workset()
        if not active_workset:
            return None
        
        return {
            "id": active_workset.id,
            "name": active_workset.name,
            "source_connection": active_workset.source_connection.model_dump(),
            "destination_connection": active_workset.destination_connection.model_dump()
        }
    except Exception as e:
        logger.error(f"Error getting active workset: {e}")
        return None


# Legacy SQL Mapping endpoints (deprecated - use unified /mapping/* endpoints)
# Kept for backward compatibility during transition


@router.get("/workset/{workset_id}")
async def get_workset(workset_id: str) -> WorkingSet:
    """Get a working set by ID.
    
    Args:
        workset_id: Working set ID
        
    Returns:
        Working set
    """
    try:
        workset = config_manager.get_workset(workset_id)
        if not workset:
            raise HTTPException(status_code=404, detail="Working set not found")
        return workset
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting working set: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/workset/activate/{workset_id}")
async def activate_workset(workset_id: str):
    """Activate a working set.
    
    Args:
        workset_id: Working set ID to activate
        
    Returns:
        Success response
    """
    try:
        if config_manager.set_active_workset(workset_id):
            return {"success": True, "message": "Working set activated"}
        else:
            raise HTTPException(status_code=404, detail="Working set not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error activating working set: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/workset/{workset_id}")
async def delete_workset(workset_id: str):
    """Delete a working set.
    
    Args:
        workset_id: Working set ID to delete
        
    Returns:
        Success response
    """
    try:
        if config_manager.delete_workset(workset_id):
            return {"success": True, "message": "Working set deleted"}
        else:
            raise HTTPException(status_code=404, detail="Working set not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting working set: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# DuckDB Script Management

@router.get("/duckdb/scripts/list")
async def list_duckdb_scripts(category: Optional[str] = None):
    """List available DuckDB transformation scripts.
    
    Args:
        category: Filter by category ('template' or 'custom')
        
    Returns:
        List of available scripts
    """
    try:
        scripts = duckdb_script_manager.list_scripts(category)
        return scripts
    except Exception as e:
        logger.error(f"Error listing DuckDB scripts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/duckdb/scripts/{script_name}")
async def get_duckdb_script(script_name: str):
    """Get DuckDB script content by name.
    
    Args:
        script_name: Name of the script
        
    Returns:
        Script content
    """
    try:
        content = duckdb_script_manager.get_script(script_name)
        if content:
            return {"success": True, "name": script_name, "content": content}
        else:
            raise HTTPException(status_code=404, detail=f"Script '{script_name}' not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting DuckDB script: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/duckdb/scripts/validate")
async def validate_duckdb_script(request: ScriptValidationRequest) -> ScriptValidationResponse:
    """Validate DuckDB script syntax.
    
    Args:
        request: Script validation request with content
        
    Returns:
        Validation result
    """
    try:
        is_valid, message = duckdb_script_manager.validate_script_syntax(request.content)
        return ScriptValidationResponse(valid=is_valid, message=message)
    except Exception as e:
        logger.error(f"Error validating DuckDB script: {e}")
        return ScriptValidationResponse(valid=False, message=str(e))


@router.post("/duckdb/scripts/save")
async def save_duckdb_script(
    script_name: str,
    content: str,
    category: str = 'custom',
    description: Optional[str] = None,
    overwrite: bool = False
):
    """Save a DuckDB transformation script.
    
    Args:
        script_name: Name for the script
        content: Script content
        category: 'template' or 'custom'
        description: Script description
        overwrite: Allow overwriting existing script
        
    Returns:
        Success response
    """
    try:
        success, message = duckdb_script_manager.save_script(
            script_name, content, category, description, overwrite
        )
        if success:
            return {"success": True, "message": message}
        else:
            raise HTTPException(status_code=400, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving DuckDB script: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/duckdb/scripts/{script_name}")
async def delete_duckdb_script(script_name: str, category: str = 'custom'):
    """Delete a DuckDB script.
    
    Args:
        script_name: Name of the script to delete
        category: 'custom' only (templates cannot be deleted)
        
    Returns:
        Success response
    """
    try:
        success, message = duckdb_script_manager.delete_script(script_name, category)
        if success:
            return {"success": True, "message": message}
        else:
            raise HTTPException(status_code=400, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting DuckDB script: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Visual Workflow Management

@router.get("/workflow/visual/list")
async def list_visual_workflows() -> List[WorkflowListItem]:
    """List all visual workflows.
    
    Returns:
        List of workflow summaries
    """
    try:
        return workflow_manager.list_workflows()
    except Exception as e:
        logger.error(f"Error listing workflows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflow/visual/{workflow_id}")
async def get_visual_workflow(workflow_id: str) -> VisualWorkflow:
    """Get a visual workflow by ID.
    
    Args:
        workflow_id: Workflow ID
        
    Returns:
        Visual workflow
    """
    try:
        workflow = workflow_manager.get_workflow(workflow_id)
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
        return workflow
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflow/visual/create")
async def create_visual_workflow(workflow: VisualWorkflow):
    """Create a new visual workflow.
    
    Args:
        workflow: Visual workflow to create
        
    Returns:
        Success response
    """
    try:
        success, message = workflow_manager.create_workflow(workflow)
        if success:
            return {"success": True, "message": message, "workflow_id": workflow.id}
        else:
            raise HTTPException(status_code=400, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/workflow/visual/update")
async def update_visual_workflow(workflow: VisualWorkflow):
    """Update an existing visual workflow.
    
    Args:
        workflow: Updated visual workflow
        
    Returns:
        Success response
    """
    try:
        success, message = workflow_manager.update_workflow(workflow)
        if success:
            return {"success": True, "message": message}
        else:
            raise HTTPException(status_code=400, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/workflow/visual/{workflow_id}")
async def delete_visual_workflow(workflow_id: str):
    """Delete a visual workflow.
    
    Args:
        workflow_id: Workflow ID to delete
        
    Returns:
        Success response
    """
    try:
        success, message = workflow_manager.delete_workflow(workflow_id)
        if success:
            return {"success": True, "message": message}
        else:
            raise HTTPException(status_code=404, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflow/validate/{workflow_id}")
async def validate_workflow(workflow_id: str) -> WorkflowValidationResult:
    """Validate a visual workflow.
    
    Args:
        workflow_id: Workflow ID to validate
        
    Returns:
        Validation result
    """
    try:
        workflow = workflow_manager.get_workflow(workflow_id)
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        return workflow_manager.validate_workflow(workflow)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error validating workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflow/compile/{workflow_id}")
async def compile_workflow_to_mapping(workflow_id: str) -> WorkflowCompileResult:
    """Compile visual workflow into Mapping.
    
    Args:
        workflow_id: Workflow ID to compile
        
    Returns:
        Compilation result with mapping ID
    """
    try:
        workflow = workflow_manager.get_workflow(workflow_id)
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        # Compile workflow
        success, table_mapping, errors = workflow_converter.compile_workflow(workflow)
        
        if not success:
            return WorkflowCompileResult(
                success=False,
                message="Compilation failed",
                errors=errors
            )
        
        # Save the compiled mapping
        mapping_success, mapping_message = mapping_manager.create_mapping(table_mapping)
        
        if mapping_success:
            # Associate compiled mapping with workflow
            workflow_manager.set_compiled_mapping(workflow_id, table_mapping.id)
            
            return WorkflowCompileResult(
                success=True,
                mapping_id=table_mapping.id,
                message=f"Workflow compiled successfully to mapping '{table_mapping.id}'"
            )
        else:
            return WorkflowCompileResult(
                success=False,
                message=f"Workflow compiled but mapping creation failed: {mapping_message}",
                errors=[mapping_message]
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error compiling workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Data Ingestion Management

@router.get("/ingestion/patterns")
async def get_ingestion_patterns():
    """Get all available ingestion patterns.

    Returns:
        List of ingestion patterns
    """
    try:
        from backend.core.data_ingestion_engine import ingestion_engine
        patterns = ingestion_engine.get_patterns()
        return {"success": True, "patterns": [p.model_dump() for p in patterns]}
    except Exception as e:
        logger.error(f"Error getting ingestion patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ingestion/patterns/reload")
async def reload_ingestion_patterns():
    """Reload ingestion patterns from configuration.

    Returns:
        Success response
    """
    try:
        from backend.core.data_ingestion_engine import ingestion_engine
        ingestion_engine.reload_patterns()
        patterns = ingestion_engine.get_patterns()
        return {
            "success": True,
            "message": f"Reloaded {len(patterns)} patterns",
            "patterns": [p.model_dump() for p in patterns]
        }
    except Exception as e:
        logger.error(f"Error reloading ingestion patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ingestion/analyze")
async def analyze_ingestion_data(request: IngestionAnalysisRequest):
    """Analyze data from multiple sources and detect mismatches.

    Args:
        request: Analysis request with sources and configuration

    Returns:
        Analysis result with matched/unmatched records
    """
    try:
        from backend.core.data_ingestion_engine import ingestion_engine

        result = ingestion_engine.analyze_sources(
            sources=request.sources,
            column_mappings=request.column_mappings,
            join_keys=request.join_keys,
            apply_patterns=request.apply_patterns,
            max_records=request.max_records
        )

        return {"success": True, "result": result.model_dump()}
    except Exception as e:
        logger.error(f"Error analyzing ingestion data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ingestion/test-pattern")
async def test_pattern(request: PatternTestRequest):
    """Test a pattern on sample data.

    Args:
        request: Pattern test request

    Returns:
        Pattern test result
    """
    try:
        from backend.core.data_ingestion_engine import ingestion_engine

        if request.pattern_id not in ingestion_engine.detectors:
            raise HTTPException(status_code=404, detail=f"Pattern '{request.pattern_id}' not found")

        detector = ingestion_engine.detectors[request.pattern_id]
        matches_found = 0
        sample_results = []

        for sample in request.sample_data:
            matched, fix = detector.detect(sample)
            if matched:
                matches_found += 1

            sample_results.append({
                "input": sample,
                "matched": matched,
                "suggested_fix": fix
            })

        return {
            "success": True,
            "result": PatternTestResult(
                pattern_id=request.pattern_id,
                matches_found=matches_found,
                sample_results=sample_results,
                success=True,
                message=f"Pattern tested on {len(request.sample_data)} samples"
            ).model_dump()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing pattern: {e}")
        raise HTTPException(status_code=500, detail=str(e))






