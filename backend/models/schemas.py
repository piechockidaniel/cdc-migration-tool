"""Pydantic models for API requests and responses."""
import warnings
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

# ``SourceConfig.schema`` is part of the public JSON contract used by the UI;
# pydantic warns that it shadows the (deprecated) ``BaseModel.schema`` method.
warnings.filterwarnings("ignore", message='Field name "schema" in "SourceConfig" shadows')


class ConnectionType(str, Enum):
    """Connection type enum."""
    SOURCE = "source"
    DESTINATION = "destination"


class ConnectionConfig(BaseModel):
    """Database connection configuration."""
    id: Optional[str] = Field(default=None, description="Connection library id (defaults to a slug of the name)")
    name: str = Field(..., description="Connection name")
    server: str = Field(..., description="Server address")
    database: str = Field(..., description="Database name")
    username: Optional[str] = Field(default=None, description="Username for authentication")
    password: Optional[str] = Field(default=None, description="Password for authentication")
    use_windows_auth: bool = Field(default=True, description="Use Windows authentication")
    port: Optional[int] = Field(default=None, description="Server port")
    driver: str = Field(default="ODBC Driver 18 for SQL Server", description="ODBC driver name")
    engine: str = Field(default="mssql", description="Database engine: mssql | postgresql | mariadb | mongodb | oracle")
    login_timeout: int = Field(default=15, description="Connection timeout in seconds (0 = wait forever is NOT allowed)")
    read_only_intent: bool = Field(default=False, description="Connect with ApplicationIntent=ReadOnly (AG readable secondary)")
    options: Dict[str, Any] = Field(default_factory=dict, description="Engine specific options (slot_name, plugin, server_id, service_name, ...)")


class ConnectionTestResponse(BaseModel):
    """Response for connection test."""
    success: bool
    message: str
    server_version: Optional[str] = None


class DatabaseInfo(BaseModel):
    """Database information."""
    name: str
    cdc_enabled: bool = False


class TableInfo(BaseModel):
    """Table information."""
    schema_name: str
    table_name: str
    full_name: str
    cdc_enabled: bool = False
    row_count: Optional[int] = None


class ColumnInfo(BaseModel):
    """Column information."""
    column_name: str
    data_type: str
    is_nullable: bool
    max_length: Optional[int] = None
    is_primary_key: bool = False


class AutoGenerateMode(str, Enum):
    """Auto-generate mode for columns."""
    NONE = "none"  # No auto-generation
    ON_INSERT = "on_insert"  # Generate on every insert
    ON_INIT = "on_init"  # Generate only on initial insert (once)


class TransformationEngine(str, Enum):
    """Transformation engine type."""
    NONE = "none"  # No transformation
    SQL = "sql"  # SQL Server native transformation
    DUCKDB = "duckdb"  # DuckDB in-memory transformation


class MappingType(str, Enum):
    """Mapping type enum."""
    TABLE = "table"  # Table-to-table mapping with CDC
    SQL = "sql"  # SQL query-based mapping


class MergeOperationType(str, Enum):
    """Types of merge operations for multi-source mappings."""
    JOIN = "join"           # SQL JOIN (INNER, LEFT, RIGHT, FULL)
    UNION = "union"         # Vertical stacking (UNION or UNION ALL)
    INTERSECT = "intersect" # Common rows only
    EXCEPT = "except"       # Rows in first but not second
    CUSTOM = "custom"       # Custom SQL expression


class ConflictResolutionStrategy(str, Enum):
    """Strategy for resolving conflicts in multi-source merges."""
    LATEST_WINS = "latest_wins"           # Most recent update wins
    SOURCE_PRIORITY = "source_priority"   # Prioritize specific source
    CUSTOM_RULE = "custom_rule"           # User-defined resolution


class ColumnMapping(BaseModel):
    """Column-to-column mapping.
    
    Supports:
    - Simple 1:1 mapping (source_column -> destination_column)
    - Many-to-one mapping (source_columns list -> destination_column with transformation)
    - Column-level controls (ignore, auto-generate, default values)
    - DuckDB in-memory transformations
    """
    source_column: Optional[str] = None  # For single column mapping (backward compatible)
    source_columns: Optional[List[str]] = None  # For multiple columns (e.g., JSON aggregation)
    destination_column: str
    transformation: Optional[str] = None  # SQL expression for transformation (e.g., 'JSON_OBJECT', 'CONCAT')
    transformation_type: Optional[str] = None  # Helper: 'json', 'concat', 'custom', 'duckdb', None for direct mapping
    transformation_engine: TransformationEngine = TransformationEngine.SQL  # Engine to use for transformation
    
    # Column control options
    ignore_changes: bool = False  # If True, column changes are not synced (skipped)
    auto_generate: AutoGenerateMode = AutoGenerateMode.NONE  # Auto-generate value for destination column
    auto_generate_expression: Optional[str] = None  # SQL expression for auto-generation (e.g., 'NEWID()', 'GETDATE()')
    default_value: Optional[str] = None  # Default value if source is NULL or missing (can be SQL expression)


# Multi-Source Mapping Models

class SourceConfig(BaseModel):
    """Configuration for a single source in multi-source mapping."""
    id: str = Field(..., description="Unique source identifier within mapping")
    connection_id: str = Field(..., description="Reference to saved connection ID")
    alias: str = Field(..., description="Alias used in merge operations (e.g., 'crm', 'erp')")
    schema: str = Field(..., description="Schema name")
    table: str = Field(..., description="Table name")

    # CDC tracking (per source)
    enable_cdc: bool = True
    cdc_capture_instance: Optional[str] = None

    # Filtering
    where_clause: Optional[str] = Field(default=None, description="Optional WHERE condition for this source")


class OutputColumnMapping(BaseModel):
    """Maps source columns to destination columns in merge result."""
    source_alias: str = Field(..., description="Source alias (e.g., 'crm', 'erp', or 'merged')")
    source_column: str = Field(..., description="Column name in the source")
    destination_column: str = Field(..., description="Column name in destination table")
    transformation: Optional[str] = Field(default=None, description="Optional SQL transformation expression")


class MergeOperation(BaseModel):
    """Single merge operation in a pipeline."""
    type: MergeOperationType
    left_source: str = Field(..., description="Left source alias or 'previous_result'")
    right_source: str = Field(..., description="Right source alias")

    # For JOIN operations
    join_type: Optional[str] = Field(default=None, description="INNER, LEFT, RIGHT, FULL")
    on_condition: Optional[str] = Field(default=None, description="JOIN condition (e.g., 'left.id = right.customer_id')")

    # For UNION operations
    union_all: bool = True  # True = UNION ALL, False = UNION (distinct)

    # Custom SQL
    custom_expression: Optional[str] = None


class MergePattern(BaseModel):
    """Defines how multiple sources are merged."""
    operations: List[MergeOperation] = Field(..., description="Ordered list of merge operations")
    output_columns: List[OutputColumnMapping] = Field(..., description="Final column selection and mapping")


class Mapping(BaseModel):
    """Unified mapping configuration supporting both table-based and SQL-based mappings."""
    id: str = Field(..., description="Unique mapping ID")
    name: str = Field(..., description="Mapping name/description")
    mapping_type: MappingType = Field(..., description="Type of mapping: TABLE or SQL")
    
    # Common fields
    destination_schema: str = Field(..., description="Destination schema name")
    destination_table: str = Field(..., description="Destination table name")
    enabled: bool = True
    assigned_worksets: List[str] = Field(default_factory=list, description="List of working set IDs this mapping is assigned to")
    
    # Sync options (common)
    sync_mode: str = Field(default="full", description="Sync mode: 'full' (replace all), 'incremental' (append only), 'upsert' (merge)")
    sync_schedule: Optional[str] = Field(default=None, description="Cron expression for scheduled sync (optional)")
    batch_size: int = Field(default=1000, description="Batch size for processing records")
    timeout_seconds: int = Field(default=300, description="Query timeout in seconds")
    
    # Multi-source support
    is_multi_source: bool = False
    sources: List[SourceConfig] = Field(default_factory=list, description="Multiple source configurations")
    merge_pattern: Optional[MergePattern] = Field(default=None, description="Pattern for merging multiple sources")
    conflict_resolution: ConflictResolutionStrategy = ConflictResolutionStrategy.LATEST_WINS
    source_priority: List[str] = Field(default_factory=list, description="Ordered list of source IDs (highest priority first)")

    # Table-specific fields (when mapping_type == TABLE and is_multi_source == False)
    # DEPRECATED when is_multi_source == True (use sources instead)
    source_schema: Optional[str] = Field(default=None, description="Source schema name (for TABLE type, single source)")
    source_table: Optional[str] = Field(default=None, description="Source table name (for TABLE type, single source)")
    column_mappings: List[ColumnMapping] = Field(default_factory=list, description="Column mappings (for TABLE type)")
    sync_deletes: bool = True  # For TABLE type
    sync_updates: bool = True  # For TABLE type
    sync_inserts: bool = True  # For TABLE type
    
    # DuckDB transformation options (for TABLE type)
    use_duckdb_transformation: bool = False
    duckdb_script_name: Optional[str] = None
    duckdb_script_content: Optional[str] = None
    
    # Initial snapshot options (for TABLE type)
    perform_initial_snapshot: bool = False
    snapshot_completed_at: Optional[datetime] = None
    
    # SQL-specific fields (when mapping_type == SQL)
    source_query: Optional[str] = Field(default=None, description="SQL query to extract data (for SQL type)")
    insert_query: Optional[str] = Field(default=None, description="Custom INSERT query template (for SQL type)")
    update_query: Optional[str] = Field(default=None, description="Custom UPDATE query template (for SQL type)")
    delete_query: Optional[str] = Field(default=None, description="Custom DELETE query template (for SQL type)")
    key_columns: Optional[List[str]] = Field(default=None, description="Key columns for upsert/update/delete (for SQL type)")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None  # success, failed, running
    last_run_error: Optional[str] = None



class WorkingSet(BaseModel):
    """Working set configuration."""
    id: str = Field(..., description="Unique working set ID")
    name: str = Field(..., description="Working set name")
    description: Optional[str] = None
    # 2.0: reference connections from the library by id (secrets stay in one place) ...
    source_connection_id: Optional[str] = Field(default=None, description="Connection library id of the source")
    destination_connection_id: Optional[str] = Field(default=None, description="Connection library id of the destination")
    # ... or embed them (1.x). At least one of the two forms is required.
    source_connection: Optional[ConnectionConfig] = None
    destination_connection: Optional[ConnectionConfig] = None
    mappings: List[str] = Field(default_factory=list, description="Unified list of mapping IDs (table and SQL)")
    # Legacy fields for backward compatibility during migration
    table_mappings: List[str] = Field(default_factory=list, description="DEPRECATED: List of table mapping IDs")
    sql_mappings: List[str] = Field(default_factory=list, description="DEPRECATED: List of SQL mapping IDs")
    is_active: bool = False
    data_ingestion_check: bool = Field(default=False, description="Enable data ingestion quality check before sync")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class CDCEnableRequest(BaseModel):
    """Request to enable CDC."""
    connection_type: ConnectionType
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    capture_instance: Optional[str] = None


class CDCStatusResponse(BaseModel):
    """CDC status response."""
    database: str
    cdc_enabled: bool
    tables: List[Dict[str, Any]] = Field(default_factory=list)


class OperationStatus(str, Enum):
    """Operation status enum."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    ERROR = "error"


class SyncStatus(BaseModel):
    """Synchronization status."""
    status: OperationStatus
    working_set_id: Optional[str] = None
    working_set_name: Optional[str] = None
    start_time: Optional[datetime] = None
    events_processed: int = 0
    errors_count: int = 0
    current_lsn: Optional[str] = None
    last_event_time: Optional[datetime] = None


class CDCOperation(str, Enum):
    """CDC operation types."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class CDCEvent(BaseModel):
    """CDC event."""
    id: str
    timestamp: datetime
    source_table: str
    operation: CDCOperation
    lsn: str
    seqval: str
    data: Dict[str, Any]
    status: str = "pending"  # pending, processed, failed
    error: Optional[str] = None
    source_change_time: Optional[datetime] = None  # Original timestamp from source table (for latency tracking)


class SyncStatistics(BaseModel):
    """Synchronization statistics."""
    total_events: int = 0
    successful_events: int = 0
    failed_events: int = 0
    events_per_second: float = 0.0
    average_latency_ms: float = 0.0
    uptime_seconds: float = 0.0


class VerificationRequest(BaseModel):
    """Data quality verification request."""
    mapping_id: str
    sample_size: int = Field(default=100, description="Number of random records to verify")
    check_row_counts: bool = True
    check_reverse_mapping: bool = True


class VerificationResult(BaseModel):
    """Data quality verification result."""
    mapping_id: str
    timestamp: datetime
    source_row_count: int
    destination_row_count: int
    row_count_match: bool
    samples_checked: int
    samples_matched: int
    reverse_mapping_success_rate: float
    mismatches: List[Dict[str, Any]] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


# Data Ingestion Models

class PatternType(str, Enum):
    """Pattern detection type enum."""
    COLUMN_SWAP = "column_swap"
    CHARACTER_NORMALIZATION = "character_normalization"
    CASE_NORMALIZATION = "case_normalization"
    WHITESPACE_NORMALIZATION = "whitespace_normalization"


class IngestionPattern(BaseModel):
    """Pattern configuration for data ingestion."""
    id: str = Field(..., description="Unique pattern ID")
    name: str = Field(..., description="Pattern name")
    description: str = Field(..., description="Pattern description")
    type: PatternType = Field(..., description="Pattern type")
    enabled: bool = Field(default=True, description="Whether pattern is enabled")
    priority: int = Field(default=1, description="Pattern priority (lower runs first)")
    config: Dict[str, Any] = Field(default_factory=dict, description="Pattern-specific configuration")


class DataSourceConfig(BaseModel):
    """Configuration for a data source in ingestion analysis."""
    connection_id: str = Field(..., description="Connection ID from connection library")
    schema_name: str = Field(..., description="Schema name")
    table_name: str = Field(..., description="Table name")
    alias: str = Field(..., description="Alias for this source (e.g., 'source1', 'source2')")
    columns: List[str] = Field(default_factory=list, description="Columns to compare")
    where_clause: Optional[str] = Field(default=None, description="Optional WHERE clause for filtering")


class IngestionColumnMapping(BaseModel):
    """Mapping between columns across sources (data-ingestion analysis).

    NOTE: this used to be called ``ColumnMapping`` which shadowed the table
    ``ColumnMapping`` model defined above and broke ``auto_map_columns`` and
    the workflow compiler.
    """
    name: str = Field(..., description="Logical column name")
    source_columns: Dict[str, str] = Field(..., description="Map of source alias to column name")


class IngestionAnalysisRequest(BaseModel):
    """Request to analyze data from multiple sources."""
    sources: List[DataSourceConfig] = Field(..., description="List of data sources to compare")
    column_mappings: List[IngestionColumnMapping] = Field(..., description="Column mappings across sources")
    join_keys: List[str] = Field(..., description="Columns to use for joining/matching records")
    apply_patterns: List[str] = Field(default_factory=list, description="Pattern IDs to apply")
    max_records: int = Field(default=10000, description="Maximum records to analyze")


class MismatchDetail(BaseModel):
    """Details about a data mismatch."""
    row_id: str = Field(..., description="Unique identifier for the row")
    column_name: str = Field(..., description="Column with mismatch")
    values: Dict[str, Any] = Field(..., description="Values from each source")
    detected_patterns: List[str] = Field(default_factory=list, description="Patterns that could resolve this mismatch")
    suggested_fix: Optional[str] = Field(default=None, description="Suggested fix for the mismatch")


class IngestionAnalysisResult(BaseModel):
    """Result of data ingestion analysis."""
    total_records: int = Field(..., description="Total records processed")
    matched_records: int = Field(..., description="Number of matched records")
    unmatched_records: int = Field(..., description="Number of unmatched records")
    match_percentage: float = Field(..., description="Percentage of matched records")
    mismatches: List[MismatchDetail] = Field(default_factory=list, description="Details of mismatched records")
    patterns_applied: List[str] = Field(default_factory=list, description="Patterns that were applied")
    statistics: Dict[str, Any] = Field(default_factory=dict, description="Additional statistics")
    execution_time_ms: float = Field(..., description="Execution time in milliseconds")
    timestamp: datetime = Field(default_factory=datetime.now, description="Analysis timestamp")


class PatternTestRequest(BaseModel):
    """Request to test a pattern on sample data."""
    pattern_id: str = Field(..., description="Pattern ID to test")
    sample_data: List[Dict[str, Any]] = Field(..., description="Sample data to test pattern on")


class PatternTestResult(BaseModel):
    """Result of pattern testing."""
    pattern_id: str
    matches_found: int
    sample_results: List[Dict[str, Any]] = Field(default_factory=list)
    success: bool
    message: str





