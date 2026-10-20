"""Multi-source synchronization engine that applies merged CDC changes to destination."""
import asyncio
import logging
from typing import Optional, Dict, Any, List, Tuple, Callable
from datetime import datetime
from backend.db.mssql_manager import MSSQLConnection
from backend.models.schemas import Mapping, ConflictResolutionStrategy
from backend.core.multi_source_executor import MergePatternExecutor

logger = logging.getLogger(__name__)


class ConflictResolver:
    """Handles conflict resolution for multi-source data."""

    @staticmethod
    def resolve(
        mapping: Mapping,
        source_id: str,
        existing_data: Optional[Dict[str, Any]],
        new_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Resolve conflicts between existing and new data.

        Args:
            mapping: Multi-source mapping
            source_id: Source that triggered the change
            existing_data: Existing row in destination (or None if new)
            new_data: New merged data from merge pattern

        Returns:
            Resolved data dictionary
        """
        if not existing_data:
            # No conflict - this is a new row
            return new_data

        strategy = mapping.conflict_resolution

        if strategy == ConflictResolutionStrategy.LATEST_WINS:
            # Most recent update wins (always use new data)
            return new_data

        elif strategy == ConflictResolutionStrategy.SOURCE_PRIORITY:
            # Use source priority list to determine which source wins
            if not mapping.source_priority:
                logger.warning("SOURCE_PRIORITY strategy but no priority list - using LATEST_WINS")
                return new_data

            # Check if the source that changed has higher priority than existing
            # For simplicity, we'll use LATEST_WINS here
            # In production, track which source last updated each column
            return new_data

        elif strategy == ConflictResolutionStrategy.CUSTOM_RULE:
            # Custom resolution - would need to be implemented per use case
            logger.warning("CUSTOM_RULE not implemented - using LATEST_WINS")
            return new_data

        else:
            return new_data


class MultiSourceSyncEngine:
    """Synchronization engine for multi-source data replication."""

    def __init__(self):
        """Initialize multi-source sync engine."""
        self.is_running = False
        self.destination_connection: Optional[MSSQLConnection] = None
        self.mapping: Optional[Mapping] = None
        self.executor: Optional[MergePatternExecutor] = None
        self.sync_task: Optional[asyncio.Task] = None
        self.event_queue: Optional[asyncio.Queue] = None

        # Statistics
        self.events_processed = 0
        self.errors_count = 0
        self.inserts_count = 0
        self.updates_count = 0
        self.deletes_count = 0
        self.start_time: Optional[datetime] = None
        self.last_event_time: Optional[datetime] = None

        # Event listeners
        self.event_listeners: List[Callable] = []

    def set_mapping(self, mapping: Mapping):
        """Set multi-source mapping.

        Args:
            mapping: Multi-source mapping configuration
        """
        if not mapping.is_multi_source:
            raise ValueError("Mapping must be multi-source")

        self.mapping = mapping
        # MergePatternExecutor doesn't need connections_map for execute_merge_for_sync
        # Pass empty dict to satisfy constructor
        self.executor = MergePatternExecutor({})
        logger.info(f"Multi-source sync engine configured for mapping: {mapping.name}")

    def set_destination_connection(self, connection: MSSQLConnection):
        """Set destination database connection.

        Args:
            connection: MSSQL connection
        """
        self.destination_connection = connection
        logger.info("Multi-source sync engine destination connection set")

    def set_event_queue(self, event_queue: asyncio.Queue):
        """Set event queue to consume from.

        Args:
            event_queue: Event queue from CDC monitor
        """
        self.event_queue = event_queue

    def add_event_listener(self, listener: Callable):
        """Add event listener for sync events.

        Args:
            listener: Callable that accepts (event, status, error) parameters
        """
        self.event_listeners.append(listener)

    async def _notify_listeners(self, event: Dict[str, Any], status: str, error: Optional[str] = None):
        """Notify event listeners.

        Args:
            event: Event dictionary
            status: Event status (processed, failed)
            error: Error message if failed
        """
        for listener in self.event_listeners:
            try:
                if asyncio.iscoroutinefunction(listener):
                    await listener(event, status, error)
                else:
                    listener(event, status, error)
            except Exception as e:
                logger.error(f"Error notifying listener: {e}")

    def _get_primary_key_condition(self, data: Dict[str, Any]) -> Tuple[str, list]:
        """Build a *parameterised* WHERE condition for the key.

        BUGFIX: the original interpolated values into SQL (injection + quoting
        bugs). Key columns: mapping.key_columns, else the first output column.

        Returns:
            (where clause with ? placeholders, parameters)
        """
        key_cols = list(self.mapping.key_columns or [])
        if not key_cols:
            if not self.mapping.merge_pattern or not self.mapping.merge_pattern.output_columns:
                return "1=0", []
            key_cols = [self.mapping.merge_pattern.output_columns[0].destination_column]
        clauses, params = [], []
        for col in key_cols:
            value = data.get(col)
            if value is None:
                return "1=0", []
            clauses.append(f"[{col}] = ?")
            params.append(value)
        return " AND ".join(clauses), params

    async def _get_existing_row(self, key_condition) -> Optional[Dict[str, Any]]:
        """Get existing row from destination.

        Args:
            key_condition: (where clause, params) tuple from _get_primary_key_condition

        Returns:
            Existing row data or None
        """
        try:
            where, params = key_condition
            query = f"""
                SELECT TOP 1 *
                FROM [{self.mapping.destination_schema}].[{self.mapping.destination_table}]
                WHERE {where}
            """

            cursor = self.destination_connection.connection.cursor()
            cursor.execute(query, params)

            row = cursor.fetchone()
            if row:
                # Convert to dictionary
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))

            return None

        except Exception as e:
            logger.error(f"Error getting existing row: {e}")
            return None

    async def _apply_merged_change(
        self,
        event_dict: Dict[str, Any],
        join_key: Any
    ) -> Tuple[bool, Optional[str]]:
        """Apply a merged change to destination.

        This re-executes the merge pattern for the affected join key
        and updates the destination accordingly.

        Args:
            event_dict: Event dictionary with source info
            join_key: Key that identifies affected row

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Re-execute merge pattern for this specific join key
            # We need to build a filtered query that only returns the affected row(s)

            source_alias = event_dict.get('_source_alias')
            operation = event_dict.get('operation')

            logger.debug(f"Applying merged change: source={source_alias}, op={operation}, key={join_key}")

            # NOTE (review finding 1.10): the merge pattern is re-executed for ALL rows on every event;
            # a key-filtered merge query is the planned fix. See docs/developer-guide/07-history.md.

            # Execute merge pattern
            result = self.executor.execute_merge_for_sync(
                self.mapping,
                self.destination_connection
            )

            if not result:
                return False, "Merge pattern execution failed"

            # Result is a list of rows from execute_merge_for_sync
            # Filter to only affected rows (simplified - would need better key matching)
            affected_rows = result  # In production, filter by join_key

            if not affected_rows:
                logger.debug("No rows affected by merge")
                return True, None

            # Process each affected row
            for row_data in affected_rows:
                # Determine if this is INSERT or UPDATE
                key_condition = self._get_primary_key_condition(row_data)
                existing_row = await self._get_existing_row(key_condition)

                # Resolve conflicts
                resolved_data = ConflictResolver.resolve(
                    self.mapping,
                    event_dict.get('_source_id'),
                    existing_row,
                    row_data
                )

                # Apply to destination
                if existing_row:
                    # UPDATE
                    await self._update_row(resolved_data, key_condition)
                    self.updates_count += 1
                else:
                    # INSERT
                    await self._insert_row(resolved_data)
                    self.inserts_count += 1

                # Record latency
                self._record_latency(event_dict, resolved_data)

            return True, None

        except Exception as e:
            logger.error(f"Error applying merged change: {e}")
            return False, str(e)

    async def _insert_row(self, data: Dict[str, Any]):
        """Insert row into destination.

        Args:
            data: Row data to insert
        """
        try:
            columns = list(data.keys())
            values = list(data.values())

            # Build INSERT statement
            cols_str = ', '.join(columns)
            placeholders = ', '.join(['?' for _ in values])

            query = f"""
                INSERT INTO {self.mapping.destination_schema}.{self.mapping.destination_table}
                ({cols_str})
                VALUES ({placeholders})
            """

            cursor = self.destination_connection.connection.cursor()
            cursor.execute(query, values)
            self.destination_connection.connection.commit()

            logger.debug(f"Inserted row into {self.mapping.destination_schema}.{self.mapping.destination_table}")

        except Exception as e:
            logger.error(f"Error inserting row: {e}")
            raise

    async def _update_row(self, data: Dict[str, Any], key_condition: Tuple[str, list]):
        """Update row in destination.

        Args:
            data: Row data to update
            key_condition: WHERE clause for the row
        """
        try:
            # Build SET clause
            set_parts = []
            values = []

            for col, val in data.items():
                set_parts.append(f"[{col}] = ?")
                values.append(val)

            set_clause = ', '.join(set_parts)

            where, where_params = key_condition
            query = f"""
                UPDATE [{self.mapping.destination_schema}].[{self.mapping.destination_table}]
                SET {set_clause}
                WHERE {where}
            """

            cursor = self.destination_connection.connection.cursor()
            cursor.execute(query, values + list(where_params))
            self.destination_connection.connection.commit()

            logger.debug(f"Updated row in {self.mapping.destination_schema}.{self.mapping.destination_table}")

        except Exception as e:
            logger.error(f"Error updating row: {e}")
            raise

    def _record_latency(self, event_dict: Dict[str, Any], data: Dict[str, Any]):
        """Record latency metrics.

        Args:
            event_dict: Event dictionary
            data: Processed data
        """
        try:
            # Extract source timestamp if available
            source_timestamp = event_dict.get('timestamp')
            if isinstance(source_timestamp, str):
                source_timestamp = datetime.fromisoformat(source_timestamp)

            if source_timestamp:
                # BUGFIX: the original called record_latency with a non-existent signature
                from backend.core import metrics
                latency_s = (datetime.now() - source_timestamp).total_seconds()
                metrics.END_TO_END_LAG.labels(
                    table=f"{self.mapping.destination_schema}.{self.mapping.destination_table}"
                ).set(max(latency_s, 0.0))
        except Exception as e:
            logger.error(f"Error recording latency: {e}")

    async def _process_event_batch(self, events: List[Dict[str, Any]]):
        """Process a batch of events.

        Args:
            events: List of event dictionaries
        """
        for event_dict in events:
            try:
                # Join key: first configured key column (was hard-coded to 'customer_id')
                key_cols = list(self.mapping.key_columns or [])
                if not key_cols and self.mapping.merge_pattern and self.mapping.merge_pattern.output_columns:
                    key_cols = [self.mapping.merge_pattern.output_columns[0].source_column]
                join_key = event_dict.get('data', {}).get(key_cols[0]) if key_cols else None

                success, error = await self._apply_merged_change(event_dict, join_key)

                if success:
                    self.events_processed += 1
                    await self._notify_listeners(event_dict, "processed")
                else:
                    self.errors_count += 1
                    await self._notify_listeners(event_dict, "failed", error)

                self.last_event_time = datetime.now()

            except Exception as e:
                logger.error(f"Error processing event: {e}")
                self.errors_count += 1
                await self._notify_listeners(event_dict, "failed", str(e))

    async def _sync_loop(self):
        """Main synchronization loop."""
        logger.info("Multi-source sync engine loop started")

        batch = []
        batch_timeout = 1.0  # seconds

        while self.is_running:
            try:
                # Get event from queue
                try:
                    event_dict = await asyncio.wait_for(
                        self.event_queue.get(),
                        timeout=batch_timeout
                    )
                    batch.append(event_dict)

                except asyncio.TimeoutError:
                    # Process accumulated batch
                    if batch:
                        await self._process_event_batch(batch)
                        batch.clear()
                    continue

                # Process batch if full
                if len(batch) >= 10:  # Batch size
                    await self._process_event_batch(batch)
                    batch.clear()

            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                await asyncio.sleep(1)

        # Process remaining events
        if batch:
            await self._process_event_batch(batch)

        logger.info("Multi-source sync engine loop stopped")

    async def start(self):
        """Start synchronization engine."""
        if self.is_running:
            logger.warning("Multi-source sync engine already running")
            return

        if not self.destination_connection:
            raise Exception("Destination connection not set")

        if not self.mapping:
            raise Exception("Mapping not set")

        if not self.event_queue:
            raise Exception("Event queue not set")

        # Ensure connection is active
        if not self.destination_connection.is_connected():
            self.destination_connection.connect()

        self.is_running = True
        self.start_time = datetime.now()
        self.events_processed = 0
        self.errors_count = 0
        self.inserts_count = 0
        self.updates_count = 0
        self.deletes_count = 0

        # Start sync task
        self.sync_task = asyncio.create_task(self._sync_loop())

        logger.info("Multi-source sync engine started")

    async def stop(self):
        """Stop synchronization engine."""
        if not self.is_running:
            logger.warning("Multi-source sync engine not running")
            return

        logger.info("Stopping multi-source sync engine...")
        self.is_running = False

        # Wait for sync task to complete
        if self.sync_task:
            try:
                await asyncio.wait_for(self.sync_task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Sync task did not stop gracefully, cancelling...")
                self.sync_task.cancel()

        logger.info("Multi-source sync engine stopped")

    def get_statistics(self) -> Dict[str, Any]:
        """Get synchronization statistics.

        Returns:
            Statistics dictionary
        """
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0

        return {
            'is_running': self.is_running,
            'mapping_name': self.mapping.name if self.mapping else None,
            'events_processed': self.events_processed,
            'inserts': self.inserts_count,
            'updates': self.updates_count,
            'deletes': self.deletes_count,
            'errors': self.errors_count,
            'runtime_seconds': runtime,
            'events_per_second': self.events_processed / runtime if runtime > 0 else 0,
            'last_event_time': self.last_event_time.isoformat() if self.last_event_time else None
        }
