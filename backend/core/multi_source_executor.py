"""Multi-Source Merge Pattern Executor.

This module executes merge patterns to combine data from multiple sources.
"""
import logging
from typing import List, Dict, Any, Optional
from backend.models.schemas import Mapping, MergeOperationType
from backend.db.mssql_manager import MSSQLConnection

logger = logging.getLogger(__name__)


class MergePatternExecutor:
    """Executes merge patterns to combine data from multiple sources."""

    def __init__(self, connections_map: Dict[str, MSSQLConnection]):
        """Initialize executor with connection map.

        Args:
            connections_map: Map of source_id -> MSSQLConnection
        """
        self.connections_map = connections_map

    def validate_merge_pattern(self, mapping: Mapping) -> tuple[bool, str]:
        """Validate a multi-source mapping configuration.

        Args:
            mapping: Multi-source mapping to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not mapping.is_multi_source:
            return False, "Mapping is not multi-source"

        if len(mapping.sources) < 2:
            return False, "Multi-source mapping must have at least 2 sources"

        if not mapping.merge_pattern:
            return False, "Multi-source mapping must have a merge pattern"

        if len(mapping.merge_pattern.operations) == 0:
            return False, "Merge pattern must have at least one operation"

        # Validate source aliases are unique
        aliases = [s.alias for s in mapping.sources]
        if len(aliases) != len(set(aliases)):
            return False, "Source aliases must be unique"

        # Validate operations reference valid sources
        source_aliases = {s.alias for s in mapping.sources}
        for i, op in enumerate(mapping.merge_pattern.operations):
            if op.left_source != "previous_result" and op.left_source not in source_aliases:
                return False, f"Operation {i}: left_source '{op.left_source}' not found in sources"

            if op.right_source not in source_aliases:
                return False, f"Operation {i}: right_source '{op.right_source}' not found in sources"

            # Validate operation-specific fields
            if op.type == MergeOperationType.JOIN:
                if not op.join_type:
                    return False, f"Operation {i}: JOIN operation requires join_type"
                if not op.on_condition:
                    return False, f"Operation {i}: JOIN operation requires on_condition"

        # Validate output columns reference valid sources
        for i, col in enumerate(mapping.merge_pattern.output_columns):
            if col.source_alias not in source_aliases and col.source_alias != "merged":
                return False, f"Output column {i}: source_alias '{col.source_alias}' not found in sources"

        return True, "Validation successful"

    def build_merge_query(self, mapping: Mapping, limit: Optional[int] = None) -> str:
        """Build SQL query to execute merge pattern.

        Args:
            mapping: Multi-source mapping
            limit: Optional row limit for preview

        Returns:
            SQL query string
        """
        if not mapping.merge_pattern:
            raise ValueError("Mapping has no merge pattern")

        # Build CTEs for each source
        cte_parts = []
        for source in mapping.sources:
            where_clause = f"WHERE {source.where_clause}" if source.where_clause else ""
            cte = f"""
{source.alias} AS (
    SELECT *
    FROM [{source.schema}].[{source.table}]
    {where_clause}
)"""
            cte_parts.append(cte)

        # Build operation pipeline
        operations_sql = []
        for i, op in enumerate(mapping.merge_pattern.operations):
            if op.type == MergeOperationType.JOIN:
                left = op.left_source if op.left_source != "previous_result" else f"step{i-1}"
                right = op.right_source

                join_sql = f"""
step{i} AS (
    SELECT *
    FROM {left}
    {op.join_type} JOIN {right}
    ON {op.on_condition}
)"""
                operations_sql.append(join_sql)

            elif op.type == MergeOperationType.UNION:
                left = op.left_source if op.left_source != "previous_result" else f"step{i-1}"
                right = op.right_source
                union_type = "UNION ALL" if op.union_all else "UNION"

                union_sql = f"""
step{i} AS (
    SELECT * FROM {left}
    {union_type}
    SELECT * FROM {right}
)"""
                operations_sql.append(union_sql)

            elif op.type == MergeOperationType.INTERSECT:
                left = op.left_source if op.left_source != "previous_result" else f"step{i-1}"
                right = op.right_source

                intersect_sql = f"""
step{i} AS (
    SELECT * FROM {left}
    INTERSECT
    SELECT * FROM {right}
)"""
                operations_sql.append(intersect_sql)

            elif op.type == MergeOperationType.EXCEPT:
                left = op.left_source if op.left_source != "previous_result" else f"step{i-1}"
                right = op.right_source

                except_sql = f"""
step{i} AS (
    SELECT * FROM {left}
    EXCEPT
    SELECT * FROM {right}
)"""
                operations_sql.append(except_sql)

            elif op.type == MergeOperationType.CUSTOM:
                if not op.custom_expression:
                    raise ValueError(f"Operation {i}: CUSTOM type requires custom_expression")
                custom_sql = f"""
step{i} AS (
    {op.custom_expression}
)"""
                operations_sql.append(custom_sql)

        # Build final SELECT with column mappings
        final_step = f"step{len(mapping.merge_pattern.operations) - 1}"

        # Build column list
        column_list = []
        for col_map in mapping.merge_pattern.output_columns:
            if col_map.transformation:
                column_list.append(f"{col_map.transformation} AS [{col_map.destination_column}]")
            else:
                # If source_alias is specified, qualify the column
                if col_map.source_alias != "merged":
                    column_list.append(f"{col_map.source_alias}.[{col_map.source_column}] AS [{col_map.destination_column}]")
                else:
                    column_list.append(f"[{col_map.source_column}] AS [{col_map.destination_column}]")

        columns_str = ",\n    ".join(column_list)

        # Combine all parts
        limit_clause = f"TOP {limit}" if limit else ""

        query = f"""
WITH
{','.join(cte_parts)},
{','.join(operations_sql)}
SELECT {limit_clause}
    {columns_str}
FROM {final_step}
"""
        return query

    def execute_merge_preview(
        self,
        mapping: Mapping,
        connection: MSSQLConnection,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Execute merge pattern and return preview results.

        Args:
            mapping: Multi-source mapping
            connection: Database connection to use
            limit: Number of rows to return

        Returns:
            Dict with columns and sample data
        """
        query: Optional[str] = None  # defined before the try so the error branch can always report it
        try:
            # Validate mapping
            is_valid, error_msg = self.validate_merge_pattern(mapping)
            if not is_valid:
                raise ValueError(f"Invalid merge pattern: {error_msg}")

            # Build query
            query = self.build_merge_query(mapping, limit=limit)
            logger.info(f"Executing merge preview query:\n{query}")

            # Execute query
            if not connection.is_connected():
                connection.connect()

            results = connection.execute_query(query)

            # Extract columns
            columns = []
            if results and len(results) > 0:
                columns = list(results[0].keys())

            return {
                "success": True,
                "columns": columns,
                "row_count": len(results),
                "sample_data": results[:limit],
                "query": query
            }

        except Exception as e:
            logger.error(f"Error executing merge preview: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query
            }

    def execute_merge_for_sync(
        self,
        mapping: Mapping,
        connection: MSSQLConnection
    ) -> List[Dict[str, Any]]:
        """Execute merge pattern for full sync (no limit).

        Args:
            mapping: Multi-source mapping
            connection: Database connection to use

        Returns:
            List of result rows
        """
        # Validate mapping
        is_valid, error_msg = self.validate_merge_pattern(mapping)
        if not is_valid:
            raise ValueError(f"Invalid merge pattern: {error_msg}")

        # Build query
        query = self.build_merge_query(mapping, limit=None)
        logger.info("Executing merge query for sync")

        # Execute query
        if not connection.is_connected():
            connection.connect()

        results = connection.execute_query(query)
        logger.info(f"Merge query returned {len(results)} rows")

        return results
