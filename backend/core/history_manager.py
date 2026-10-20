"""History logging manager for CDC events.

Storage format changed from one JSON *array* per day (which had to be fully
loaded, extended and rewritten on every flush - O(n^2) over a day and a
guaranteed outage at bank-grade volumes) to **append-only JSON Lines**
(``cdc_events_YYYYMMDD.jsonl``). Old ``.json`` files are still readable.

The authoritative audit trail is ``backend.core.audit.AuditStore``; this
module only feeds the legacy monitoring endpoints.
"""
import json
import logging
import os
import threading
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from backend.models.schemas import CDCEvent

logger = logging.getLogger(__name__)

HISTORY_DIR = Path(os.environ.get("CDCSYNC_HISTORY_DIR", "history"))


class HistoryManager:
    """Manager for logging CDC events to JSON files."""
    
    def __init__(self):
        """Initialize history manager."""
        self._ensure_history_dir()
        self.events_buffer: List[Dict[str, Any]] = []
        self.buffer_size = 100
        self._lock = threading.Lock()
    
    @staticmethod
    def _ensure_history_dir():
        """Ensure history directory exists."""
        HISTORY_DIR.mkdir(exist_ok=True)
    
    @staticmethod
    def _get_log_file_path(log_date: date = None) -> Path:
        """Get log file path for a specific date.
        
        Args:
            log_date: Date for log file (defaults to today)
            
        Returns:
            Path to log file
        """
        if log_date is None:
            log_date = date.today()
        
        filename = f"cdc_events_{log_date.strftime('%Y%m%d')}.jsonl"
        return HISTORY_DIR / filename

    @staticmethod
    def _read_log_file(log_file: Path) -> List[Dict[str, Any]]:
        """Read a day file (JSON Lines, or the legacy JSON array format)."""
        events: List[Dict[str, Any]] = []
        legacy = log_file.with_suffix('.json')
        if legacy.exists():
            try:
                with open(legacy, 'r', encoding='utf-8') as f:
                    events.extend(json.load(f))
            except Exception as e:
                logger.error(f"Error loading legacy log file {legacy}: {e}")
        if log_file.exists():
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            logger.warning(f"Skipping corrupt line in {log_file}")
        return events
    
    def log_event(
        self,
        event: CDCEvent,
        status: str,
        error: Optional[str] = None,
        affected_rows: int = 1
    ):
        """Log a CDC event to history.
        
        Args:
            event: CDC event
            status: Event status (processed, failed)
            error: Error message if failed
            affected_rows: Number of affected rows
        """
        try:
            log_entry = {
                'event_id': event.id,
                'timestamp': datetime.now().isoformat(),
                'source_table': event.source_table,
                'operation': event.operation.value,
                'lsn': event.lsn,
                'seqval': event.seqval,
                'status': status,
                'affected_rows': affected_rows,
                'error': error,
                'data_keys': list(event.data.keys()) if event.data else []
            }
            
            with self._lock:
                self.events_buffer.append(log_entry)
                should_flush = len(self.events_buffer) >= self.buffer_size

            # Flush buffer if it reaches the buffer size
            if should_flush:
                self.flush_buffer()
                
        except Exception as e:
            logger.error(f"Error logging event to history: {e}")
    
    def flush_buffer(self):
        """Append buffered events to today's JSON Lines file (O(batch), not O(day))."""
        with self._lock:
            entries, self.events_buffer = self.events_buffer, []
        if not entries:
            return
        try:
            log_file = self._get_log_file_path()
            self._ensure_history_dir()
            with open(log_file, 'a', encoding='utf-8') as f:
                for entry in entries:
                    f.write(json.dumps(entry, default=str) + "\n")
            logger.debug(f"Flushed {len(entries)} events to {log_file}")
        except Exception as e:
            logger.error(f"Error flushing events buffer: {e}")
            with self._lock:
                self.events_buffer = entries + self.events_buffer
    
    def query_events(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        source_table: Optional[str] = None,
        operation: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query historical events.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            source_table: Source table filter
            operation: Operation type filter
            status: Status filter
            limit: Maximum number of results
            
        Returns:
            List of matching events
        """
        # Flush any pending events
        self.flush_buffer()
        
        results = []
        
        # Default to today if no dates specified
        if start_date is None and end_date is None:
            start_date = date.today()
            end_date = date.today()
        elif start_date is None:
            start_date = end_date
        elif end_date is None:
            end_date = start_date
        
        try:
            # Iterate through date range
            current_date = start_date
            while current_date <= end_date:
                log_file = self._get_log_file_path(current_date)

                if log_file.exists() or log_file.with_suffix('.json').exists():
                    events = self._read_log_file(log_file)

                    # Apply filters
                    for event in events:
                        if source_table and event.get('source_table') != source_table:
                            continue
                        if operation and event.get('operation') != operation:
                            continue
                        if status and event.get('status') != status:
                            continue
                        
                        results.append(event)
                        
                        if len(results) >= limit:
                            return results[:limit]
                
                # Move to next day
                from datetime import timedelta
                current_date += timedelta(days=1)
            
            return results[:limit]
            
        except Exception as e:
            logger.error(f"Error querying events: {e}")
            return []
    
    def get_statistics(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """Get statistics for historical events.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Statistics dictionary
        """
        events = self.query_events(start_date, end_date, limit=100000)
        
        total_events = len(events)
        successful = sum(1 for e in events if e.get('status') == 'processed')
        failed = sum(1 for e in events if e.get('status') == 'failed')
        
        operations = {}
        for event in events:
            op = event.get('operation', 'unknown')
            operations[op] = operations.get(op, 0) + 1
        
        tables = {}
        for event in events:
            table = event.get('source_table', 'unknown')
            tables[table] = tables.get(table, 0) + 1
        
        return {
            'total_events': total_events,
            'successful_events': successful,
            'failed_events': failed,
            'success_rate': round(successful / total_events * 100, 2) if total_events > 0 else 0,
            'operations': operations,
            'tables': tables
        }
    
    def get_recent_errors(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent error events.
        
        Args:
            limit: Maximum number of errors to return
            
        Returns:
            List of error events
        """
        return self.query_events(status='failed', limit=limit)
    
# Global history manager instance
history_manager = HistoryManager()








