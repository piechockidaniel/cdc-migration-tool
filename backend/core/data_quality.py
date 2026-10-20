"""Data quality verificator for comparing source and destination data."""
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from backend.core.duckdb_processor import DuckDBProcessor
from backend.db.mssql_manager import MSSQLConnection
from backend.models.schemas import VerificationResult, Mapping

logger = logging.getLogger(__name__)


def _get_random_samples(
        connection: MSSQLConnection,
    schema: str,
    table: str,
    sample_size: int
) -> List[Dict[str, Any]]:
    """Get random samples from a table.

    Args:
        connection: Database connection
        schema: Schema name
        table: Table name
        sample_size: Number of samples to retrieve

    Returns:
        List of sample records
    """
    # ``ORDER BY NEWID()`` sorts the WHOLE table (a full scan + sort on a
    # multi-TB table). TABLESAMPLE is page based and nearly free; it may
    # return nothing for tiny tables, in which case we fall back to TOP n.
    try:
        query = f"""
            SELECT TOP ({int(sample_size)}) *
            FROM [{schema}].[{table}] TABLESAMPLE SYSTEM (1 PERCENT)
        """
        rows = connection.execute_query(query)
        if rows:
            return rows
    except Exception as e:
        logger.debug(f"TABLESAMPLE not available ({e}); falling back to TOP")
    try:
        return connection.execute_query(f"SELECT TOP ({int(sample_size)}) * FROM [{schema}].[{table}]")
    except Exception as e:
        logger.error(f"Error getting samples: {e}")
        return []


class DataQualityVerificator:
    """Data quality verificator for CDC synchronization."""
    
    def __init__(
        self,
        source_connection: MSSQLConnection,
        destination_connection: MSSQLConnection,
        duckdb_processor: DuckDBProcessor
    ):
        """Initialize verificator.
        
        Args:
            source_connection: Source database connection
            destination_connection: Destination database connection
            duckdb_processor: DuckDB processor for comparisons
        """
        self.source = source_connection
        self.destination = destination_connection
        self.duckdb = duckdb_processor
    
    def verify_mapping(
        self,
        mapping: Mapping,
        sample_size: int = 100,
        check_row_counts: bool = True,
        check_reverse_mapping: bool = True
    ) -> VerificationResult:
        """Verify data quality for a table mapping.
        
        Args:
            mapping: Table mapping to verify
            sample_size: Number of random records to verify
            check_row_counts: Whether to check row counts
            check_reverse_mapping: Whether to check reverse mapping
            
        Returns:
            Verification result
        """
        try:
            errors = []
            mismatches = []
            
            source_count = 0
            dest_count = 0
            samples_checked = 0
            samples_matched = 0
            reverse_success_rate = 0.0
            
            # Check row counts
            if check_row_counts:
                source_count = self._get_row_count(
                    self.source,
                    mapping.source_schema,
                    mapping.source_table
                )
                dest_count = self._get_row_count(
                    self.destination,
                    mapping.destination_schema,
                    mapping.destination_table
                )
                
                logger.info(f"Row counts - Source: {source_count}, Destination: {dest_count}")
            
            # Sample and verify records
            if sample_size > 0:
                samples = _get_random_samples(
                    self.source,
                    mapping.source_schema,
                    mapping.source_table,
                    sample_size
                )
                
                samples_checked = len(samples)
                
                for sample in samples:
                    try:
                        # Transform source data according to mapping
                        transformed = self._transform_sample(sample, mapping)
                        
                        # Find corresponding record in destination
                        dest_record = self._find_destination_record(
                            mapping,
                            transformed
                        )
                        
                        if dest_record:
                            # Compare values
                            match, diffs = self._compare_records(transformed, dest_record)
                            if match:
                                samples_matched += 1
                            else:
                                mismatches.append({
                                    'source_data': sample,
                                    'transformed_data': transformed,
                                    'destination_data': dest_record,
                                    'differences': diffs
                                })
                        else:
                            mismatches.append({
                                'source_data': sample,
                                'transformed_data': transformed,
                                'destination_data': None,
                                'error': 'Record not found in destination'
                            })
                            
                    except Exception as e:
                        errors.append(f"Error verifying sample: {str(e)}")
            
            # Check reverse mapping
            if check_reverse_mapping and sample_size > 0:
                try:
                    reverse_success_rate = self._verify_reverse_mapping(
                        mapping,
                        min(sample_size, 10)  # Limit reverse checks
                    )
                except Exception as e:
                    errors.append(f"Error checking reverse mapping: {str(e)}")
            
            return VerificationResult(
                mapping_id=mapping.id,
                timestamp=datetime.now(),
                source_row_count=source_count,
                destination_row_count=dest_count,
                row_count_match=(source_count == dest_count),
                samples_checked=samples_checked,
                samples_matched=samples_matched,
                reverse_mapping_success_rate=reverse_success_rate,
                mismatches=mismatches[:10],  # Limit to first 10 mismatches
                errors=errors
            )
            
        except Exception as e:
            logger.error(f"Error verifying mapping: {e}")
            return VerificationResult(
                mapping_id=mapping.id,
                timestamp=datetime.now(),
                source_row_count=0,
                destination_row_count=0,
                row_count_match=False,
                samples_checked=0,
                samples_matched=0,
                reverse_mapping_success_rate=0.0,
                mismatches=[],
                errors=[str(e)]
            )
    
    @staticmethod
    def _get_row_count(
            connection: MSSQLConnection,
        schema: str,
        table: str
    ) -> int:
        """Get row count for a table.
        
        Args:
            connection: Database connection
            schema: Schema name
            table: Table name
            
        Returns:
            Row count
        """
        try:
            query = f"SELECT COUNT_BIG(*) as count FROM [{schema}].[{table}]"
            result = connection.execute_query(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0

    @staticmethod
    def _transform_sample(
            sample: Dict[str, Any],
        mapping: Mapping
    ) -> Dict[str, Any]:
        """Transform a sample record according to mapping.
        
        Args:
            sample: Source record
            mapping: Table mapping
            
        Returns:
            Transformed record
        """
        transformed = {}
        for col_mapping in mapping.column_mappings:
            source_value = sample.get(col_mapping.source_column)
            
            if col_mapping.transformation:
                # For now, simple transformations - would need SQL evaluation
                # This is simplified; in production, would use DuckDB for transformation
                transformed[col_mapping.destination_column] = source_value
            else:
                transformed[col_mapping.destination_column] = source_value
        
        return transformed
    
    def _find_destination_record(
        self,
        mapping: Mapping,
        transformed_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Find corresponding record in destination table.
        
        Args:
            mapping: Table mapping
            transformed_data: Transformed source data
            
        Returns:
            Destination record or None
        """
        try:
            # Real key columns: mapping.key_columns, then the destination PK,
            # then (last resort, flagged) the old "contains id" heuristic.
            pk_candidates = list(mapping.key_columns or [])
            if not pk_candidates:
                try:
                    pk_candidates = [c.column_name for c in self.destination.get_columns(
                        mapping.destination_schema, mapping.destination_table) if c.is_primary_key]
                except Exception:
                    pk_candidates = []
            pk_candidates = [c for c in pk_candidates if c in transformed_data]
            if not pk_candidates:
                logger.warning("No key columns known for %s.%s - falling back to name heuristic",
                               mapping.destination_schema, mapping.destination_table)
                pk_candidates = [col for col in transformed_data.keys() if col.lower() == 'id' or col.lower().endswith('id')]

            if not pk_candidates:
                # Use first column as fallback
                pk_candidates = [list(transformed_data.keys())[0]] if transformed_data else []
            
            if not pk_candidates:
                return None
            
            where_clauses = []
            params = []
            for pk in pk_candidates:
                if transformed_data[pk] is not None:
                    where_clauses.append(f"{pk} = ?")
                    params.append(transformed_data[pk])
            
            if not where_clauses:
                return None
            
            query = f"""
                SELECT * FROM [{mapping.destination_schema}].[{mapping.destination_table}]
                WHERE {' AND '.join(where_clauses)}
            """
            
            results = self.destination.execute_query(query, tuple(params))
            return results[0] if results else None
            
        except Exception as e:
            logger.error(f"Error finding destination record: {e}")
            return None
    
    @staticmethod
    def _compare_records(
            record1: Dict[str, Any],
        record2: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """Compare two records.
        
        Args:
            record1: First record
            record2: Second record
            
        Returns:
            Tuple of (match, differences)
        """
        differences = []
        
        for key in record1.keys():
            if key not in record2:
                differences.append(f"Column {key} missing in destination")
                continue
            
            val1 = record1[key]
            val2 = record2[key]
            
            # Handle None/NULL
            if val1 is None and val2 is None:
                continue
            
            # Convert to string for comparison (handles type differences)
            if str(val1).strip() != str(val2).strip():
                differences.append(f"{key}: {val1} != {val2}")
        
        return len(differences) == 0, differences
    
    def _verify_reverse_mapping(
        self,
        mapping: Mapping,
        sample_size: int
    ) -> float:
        """Verify reverse mapping capability.
        
        Args:
            mapping: Table mapping
            sample_size: Number of samples to check
            
        Returns:
            Success rate (0.0 to 1.0)
        """
        try:
            # Get samples from destination
            dest_samples = _get_random_samples(
                self.destination,
                mapping.destination_schema,
                mapping.destination_table,
                sample_size
            )
            
            if not dest_samples:
                return 0.0
            
            successes = 0
            
            for dest_sample in dest_samples:
                # Try to find corresponding source record
                # Build reverse mapping
                reverse_data = {}
                for col_mapping in mapping.column_mappings:
                    dest_value = dest_sample.get(col_mapping.destination_column)
                    reverse_data[col_mapping.source_column] = dest_value
                
                # Search for source record
                pk_candidates = [
                    col for col in reverse_data.keys()
                    if 'id' in col.lower()
                ]
                
                if not pk_candidates:
                    pk_candidates = [list(reverse_data.keys())[0]] if reverse_data else []
                
                if pk_candidates:
                    where_clauses = []
                    params = []
                    for pk in pk_candidates:
                        if reverse_data[pk] is not None:
                            where_clauses.append(f"{pk} = ?")
                            params.append(reverse_data[pk])
                    
                    if where_clauses:
                        query = f"""
                            SELECT * FROM {mapping.source_schema}.{mapping.source_table}
                            WHERE {' AND '.join(where_clauses)}
                        """
                        results = self.source.execute_query(query, tuple(params))
                        if results:
                            successes += 1
            
            return successes / len(dest_samples) if dest_samples else 0.0
            
        except Exception as e:
            logger.error(f"Error verifying reverse mapping: {e}")
            return 0.0








