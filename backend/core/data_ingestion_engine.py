"""Data ingestion and pattern detection engine."""
import logging
import unicodedata
import re
from typing import List, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod
from backend.models.schemas import (
    IngestionPattern, PatternType, MismatchDetail,
    IngestionAnalysisResult, DataSourceConfig, IngestionColumnMapping
)
from backend.db.mssql_manager import MSSQLConnection
from backend.core.config_manager import config_manager

logger = logging.getLogger(__name__)


_NON_DECOMPOSABLE = {"Ł": "L", "ł": "l", "Ø": "O", "ø": "o", "Đ": "D", "đ": "d", "ß": "ss", "Æ": "AE", "æ": "ae",
                     "Œ": "OE", "œ": "oe", "Þ": "Th", "þ": "th", "ı": "i"}


class PatternDetectorBase(ABC):
    """Base class for pattern detectors."""

    def __init__(self, pattern: IngestionPattern):
        self.pattern = pattern
        self.config = pattern.config

    @abstractmethod
    def detect(self, values: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Detect if pattern matches the given values.

        Args:
            values: Dictionary of source alias to value

        Returns:
            Tuple of (pattern_matched, suggested_fix)
        """
        pass

    @abstractmethod
    def normalize(self, value: Any) -> Any:
        """Normalize a value according to this pattern.

        Args:
            value: Value to normalize

        Returns:
            Normalized value
        """
        pass


class VerticalShiftDetector(PatternDetectorBase):
    """Detects when columns are swapped (vertical shift)."""

    def detect(self, values: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Detect column swap pattern."""
        if len(values) < 2:
            return False, None

        sources = list(values.keys())
        val1 = str(values[sources[0]]) if values[sources[0]] else ""
        val2 = str(values[sources[1]]) if values[sources[1]] else ""

        if not val1 or not val2:
            return False, None

        val1_lower = val1.lower()
        val2_lower = val2.lower()

        if val1_lower == val2_lower:
            return False, None

        threshold = self.config.get('threshold', 0.7)

        if self._is_likely_swapped(val1, val2, threshold):
            return True, f"Columns may be swapped: '{val1}' <-> '{val2}'"

        return False, None

    @staticmethod
    def _is_likely_swapped(val1: str, val2: str, threshold: float) -> bool:
        """Same words, different order (e.g. 'John Smith' vs 'Smith John'): the classic first/last name swap.

        ``threshold`` is the minimum share of shared words (0..1). The 1.x version only compared
        capitalisation, which flagged case differences as swaps and missed real swaps.
        """
        w1, w2 = val1.lower().split(), val2.lower().split()
        if len(w1) < 2 or len(w2) < 2 or w1 == w2:
            return False
        shared = len(set(w1) & set(w2)) / max(len(set(w1) | set(w2)), 1)
        return shared >= threshold

    def normalize(self, value: Any) -> Any:
        """No normalization for vertical shift."""
        return value


class DiacriticMismatchDetector(PatternDetectorBase):
    """Detects diacritic character mismatches."""

    def detect(self, values: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Detect diacritic mismatch pattern."""
        if len(values) < 2:
            return False, None

        sources = list(values.keys())
        val1 = str(values[sources[0]]) if values[sources[0]] else ""
        val2 = str(values[sources[1]]) if values[sources[1]] else ""

        if not val1 or not val2:
            return False, None

        normalized1 = self.normalize(val1)
        normalized2 = self.normalize(val2)

        if normalized1 == normalized2 and val1 != val2:
            return True, f"Diacritic mismatch detected: '{val1}' vs '{val2}' (normalized: '{normalized1}')"

        return False, None

    def normalize(self, value: Any) -> Any:
        """Normalize by removing diacritics."""
        if not isinstance(value, str):
            return value

        # letters without a Unicode decomposition (NFD cannot strip them) + user mappings
        char_mappings = {**_NON_DECOMPOSABLE, **self.config.get('character_mappings', {})}

        result = value
        for original, replacement in char_mappings.items():
            result = result.replace(original, replacement)

        if self.config.get('normalize_unicode', True):
            nfd = unicodedata.normalize('NFD', result)
            result = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

        return result


class CaseMismatchDetector(PatternDetectorBase):
    """Detects case sensitivity mismatches."""

    def detect(self, values: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Detect case mismatch pattern."""
        if len(values) < 2:
            return False, None

        sources = list(values.keys())
        val1 = str(values[sources[0]]) if values[sources[0]] else ""
        val2 = str(values[sources[1]]) if values[sources[1]] else ""

        if not val1 or not val2:
            return False, None

        if val1.lower() == val2.lower() and val1 != val2:
            return True, f"Case mismatch: '{val1}' vs '{val2}'"

        return False, None

    def normalize(self, value: Any) -> Any:
        """Normalize by converting to lowercase."""
        if isinstance(value, str):
            return value.lower()
        return value


class WhitespaceMismatchDetector(PatternDetectorBase):
    """Detects whitespace variation mismatches."""

    def detect(self, values: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Detect whitespace mismatch pattern."""
        if len(values) < 2:
            return False, None

        sources = list(values.keys())
        val1 = str(values[sources[0]]) if values[sources[0]] else ""
        val2 = str(values[sources[1]]) if values[sources[1]] else ""

        if not val1 or not val2:
            return False, None

        normalized1 = self.normalize(val1)
        normalized2 = self.normalize(val2)

        if normalized1 == normalized2 and val1 != val2:
            return True, f"Whitespace mismatch: '{val1}' vs '{val2}'"

        return False, None

    def normalize(self, value: Any) -> Any:
        """Normalize by trimming and normalizing internal whitespace."""
        if not isinstance(value, str):
            return value

        result = value

        if self.config.get('trim', True):
            result = result.strip()

        if self.config.get('normalize_internal', True):
            result = re.sub(r'\s+', ' ', result)

        return result


class DataIngestionEngine:
    """Engine for analyzing multi-source data and detecting patterns."""

    def __init__(self):
        self.detectors: Dict[str, PatternDetectorBase] = {}
        self._load_patterns()

    def _load_patterns(self):
        """Load patterns from configuration."""
        try:
            import json
            from pathlib import Path

            import os
            config_path = Path(os.environ.get("CDCSYNC_CONFIG_DIR", "config")) / "ingestion_patterns.json"
            if not config_path.exists():
                logger.warning("Ingestion patterns configuration not found")
                return

            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            for pattern_data in config.get('patterns', []):
                if not pattern_data.get('enabled', True):
                    continue

                pattern = IngestionPattern(**pattern_data)
                detector = self._create_detector(pattern)
                if detector:
                    self.detectors[pattern.id] = detector

            logger.info(f"Loaded {len(self.detectors)} pattern detectors")
        except Exception as e:
            logger.error(f"Error loading patterns: {e}")

    def _create_detector(self, pattern: IngestionPattern) -> Optional[PatternDetectorBase]:
        """Create a detector instance for a pattern."""
        detector_map = {
            PatternType.COLUMN_SWAP: VerticalShiftDetector,
            PatternType.CHARACTER_NORMALIZATION: DiacriticMismatchDetector,
            PatternType.CASE_NORMALIZATION: CaseMismatchDetector,
            PatternType.WHITESPACE_NORMALIZATION: WhitespaceMismatchDetector
        }

        detector_class = detector_map.get(pattern.type)
        if detector_class:
            return detector_class(pattern)

        logger.warning(f"No detector implementation for pattern type: {pattern.type}")
        return None

    def analyze_sources(
        self,
        sources: List[DataSourceConfig],
        column_mappings: List[IngestionColumnMapping],
        join_keys: List[str],
        apply_patterns: List[str] = None,
        max_records: int = 10000
    ) -> IngestionAnalysisResult:
        """Analyze data from multiple sources and detect mismatches.

        Args:
            sources: List of data source configurations
            column_mappings: Column mappings across sources
            join_keys: Columns to use for joining
            apply_patterns: Pattern IDs to apply (None = all enabled)
            max_records: Maximum records to analyze

        Returns:
            Analysis result with matched/unmatched records
        """
        import time
        start_time = time.time()

        try:
            data_by_source = self._fetch_data_from_sources(sources, max_records)

            matched, mismatches = self._compare_records(
                data_by_source,
                sources,
                column_mappings,
                join_keys,
                apply_patterns
            )

            execution_time = (time.time() - start_time) * 1000

            total = matched + len(mismatches)
            match_percentage = (matched / total * 100) if total > 0 else 0

            return IngestionAnalysisResult(
                total_records=total,
                matched_records=matched,
                unmatched_records=len(mismatches),
                match_percentage=round(match_percentage, 2),
                mismatches=mismatches,
                patterns_applied=apply_patterns or list(self.detectors.keys()),
                statistics={
                    'sources_analyzed': len(sources),
                    'columns_compared': len(column_mappings)
                },
                execution_time_ms=round(execution_time, 2)
            )
        except Exception as e:
            logger.error(f"Error analyzing sources: {e}")
            raise

    def _fetch_data_from_sources(
        self,
        sources: List[DataSourceConfig],
        max_records: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch data from all sources."""
        data_by_source = {}

        for source in sources:
            connection_config = config_manager.get_connection(source.connection_id)
            if not connection_config:
                raise ValueError(f"Connection not found: {source.connection_id}")

            if isinstance(connection_config, dict):  # library documents are dicts (same 1.x bug as multi-source)
                from backend.core.config_manager import connection_config_from_document
                connection_config = connection_config_from_document(connection_config)
            conn = MSSQLConnection(connection_config)

            columns_str = ', '.join(source.columns) if source.columns else '*'
            query = f"SELECT TOP {max_records} {columns_str} FROM {source.schema_name}.{source.table_name}"

            if source.where_clause:
                query += f" WHERE {source.where_clause}"

            results = conn.execute_query(query)
            data_by_source[source.alias] = results

            logger.info(f"Fetched {len(results)} records from source '{source.alias}'")

        return data_by_source

    def _compare_records(
        self,
        data_by_source: Dict[str, List[Dict[str, Any]]],
        sources: List[DataSourceConfig],
        column_mappings: List[IngestionColumnMapping],
        join_keys: List[str],
        apply_patterns: List[str] = None
    ) -> Tuple[int, List[MismatchDetail]]:
        """Compare records and detect mismatches."""
        matched_count = 0
        mismatches = []

        if len(sources) < 2:
            return matched_count, mismatches

        source_aliases = [s.alias for s in sources]
        primary_source = source_aliases[0]
        primary_data = data_by_source.get(primary_source, [])

        detectors_to_use = self._get_detectors_to_use(apply_patterns)

        for primary_record in primary_data:
            join_key_values = {k: primary_record.get(k) for k in join_keys if k in primary_record}

            matching_records = {primary_source: primary_record}
            for alias in source_aliases[1:]:
                matching_record = self._find_matching_record(
                    data_by_source.get(alias, []),
                    join_key_values
                )
                if matching_record:
                    matching_records[alias] = matching_record

            if len(matching_records) != len(sources):
                continue

            record_matched = True
            for col_mapping in column_mappings:
                values = {}
                for source_alias, col_name in col_mapping.source_columns.items():
                    if source_alias in matching_records:
                        values[source_alias] = matching_records[source_alias].get(col_name)

                if not self._values_match(values):
                    record_matched = False

                    detected_patterns = []
                    suggested_fix = None

                    for detector in detectors_to_use:
                        matched, fix = detector.detect(values)
                        if matched:
                            detected_patterns.append(detector.pattern.id)
                            if not suggested_fix:
                                suggested_fix = fix

                    row_id = str(join_key_values)
                    mismatches.append(MismatchDetail(
                        row_id=row_id,
                        column_name=col_mapping.name,
                        values=values,
                        detected_patterns=detected_patterns,
                        suggested_fix=suggested_fix
                    ))

            if record_matched:
                matched_count += 1

        return matched_count, mismatches

    def _get_detectors_to_use(self, apply_patterns: List[str] = None) -> List[PatternDetectorBase]:
        """Get list of detectors to use."""
        if apply_patterns:
            return [self.detectors[pid] for pid in apply_patterns if pid in self.detectors]
        return list(self.detectors.values())

    def _find_matching_record(
        self,
        records: List[Dict[str, Any]],
        join_key_values: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Find matching record based on join keys."""
        for record in records:
            match = True
            for key, value in join_key_values.items():
                if record.get(key) != value:
                    match = False
                    break
            if match:
                return record
        return None

    def _values_match(self, values: Dict[str, Any]) -> bool:
        """Check if all values match."""
        if len(values) < 2:
            return True

        unique_values = set(str(v) for v in values.values() if v is not None)
        return len(unique_values) <= 1

    def get_patterns(self) -> List[IngestionPattern]:
        """Get all loaded patterns."""
        return [detector.pattern for detector in self.detectors.values()]

    def reload_patterns(self):
        """Reload patterns from configuration."""
        self.detectors.clear()
        self._load_patterns()


ingestion_engine = DataIngestionEngine()
