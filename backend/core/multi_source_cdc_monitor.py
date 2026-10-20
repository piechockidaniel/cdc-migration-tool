"""Multi-source CDC monitoring service for parallel change tracking across multiple databases."""
import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from backend.db.mssql_manager import MSSQLConnection
from backend.db.cdc_operations import CDCOperations
from backend.models.schemas import CDCEvent, CDCOperation, Mapping, SourceConfig
from backend.core.config_manager import config_manager
import uuid

logger = logging.getLogger(__name__)


class SourceCDCMonitor:
    """Monitors CDC events for a single source in a multi-source mapping."""

    def __init__(self, source: SourceConfig, connection: MSSQLConnection):
        """Initialize source monitor.

        Args:
            source: Source configuration
            connection: Database connection for this source
        """
        self.source = source
        self.connection = connection
        self.cdc_ops = CDCOperations(connection)
        self.last_lsn: Optional[bytes] = None
        self.is_active = source.enable_cdc

    def get_lsn_key(self) -> str:
        """Get LSN state key for this source."""
        return f"multi_{self.source.id}_{self.source.schema}.{self.source.table}"

    def load_lsn_state(self):
        """Load last processed LSN from configuration."""
        lsn_key = self.get_lsn_key()
        last_lsn_hex = config_manager.get_last_lsn(lsn_key)

        if last_lsn_hex:
            try:
                self.last_lsn = bytes.fromhex(last_lsn_hex)
                logger.info(f"Loaded LSN for source {self.source.alias}: {last_lsn_hex}")
            except Exception as e:
                logger.error(f"Error loading LSN for source {self.source.alias}: {e}")
                self.last_lsn = None

    def save_lsn_state(self, lsn: bytes):
        """Save last processed LSN.

        Args:
            lsn: LSN as bytes
        """
        try:
            lsn_hex = lsn.hex() if lsn else None
            if lsn_hex:
                lsn_key = self.get_lsn_key()
                config_manager.set_last_lsn(lsn_key, lsn_hex)
                self.last_lsn = lsn
        except Exception as e:
            logger.error(f"Error saving LSN for source {self.source.alias}: {e}")

    async def poll_changes(self) -> List[Dict[str, Any]]:
        """Poll CDC changes for this source.

        Returns:
            List of CDC change records with source_id tag
        """
        if not self.is_active:
            return []

        try:
            # Determine capture instance
            capture_instance = self.source.cdc_capture_instance or f"{self.source.schema}_{self.source.table}"

            # Get LSN bounds
            from_lsn = self.last_lsn
            if from_lsn is None:
                from_lsn = self.cdc_ops.get_min_lsn(capture_instance)
                if from_lsn is None:
                    logger.debug(f"No LSN available for source {self.source.alias}")
                    return []
            else:
                from_lsn = self.cdc_ops.increment_lsn(from_lsn) or from_lsn  # BUGFIX: avoid re-reading last LSN

            to_lsn = self.cdc_ops.get_max_lsn()
            if to_lsn is None or to_lsn <= from_lsn:
                return []  # No new changes

            # Get changes
            changes = self.cdc_ops.get_cdc_changes(
                self.source.schema,
                self.source.table,
                from_lsn,
                to_lsn,
                "all"
            )

            if changes:
                logger.info(f"Source {self.source.alias}: Found {len(changes)} CDC changes")

                # Tag each change with source information
                for change in changes:
                    change['_source_id'] = self.source.id
                    change['_source_alias'] = self.source.alias

                # Update LSN state
                self.save_lsn_state(to_lsn)

            return changes

        except Exception as e:
            logger.error(f"Error polling changes for source {self.source.alias}: {e}")
            return []


class MultiSourceCDCMonitor:
    """Monitors CDC events from multiple sources simultaneously."""

    def __init__(self, mapping: Mapping):
        """Initialize multi-source CDC monitor.

        Args:
            mapping: Multi-source mapping configuration
        """
        if not mapping.is_multi_source:
            raise ValueError("Mapping must be multi-source")

        self.mapping = mapping
        self.source_monitors: Dict[str, SourceCDCMonitor] = {}
        self.connections: Dict[str, MSSQLConnection] = {}
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.is_running = False
        self.is_paused = False
        self.poll_interval = 5  # seconds
        self.monitor_tasks: List[asyncio.Task] = []
        self.batch_size = 100

    def _create_connections(self):
        """Create database connections for all sources."""
        logger.info("Creating connections for multi-source monitoring...")

        for source in self.mapping.sources:
            try:
                # Get connection config from connection library
                conn_config = config_manager.get_connection(source.connection_id)
                if not conn_config:
                    logger.error(f"Connection {source.connection_id} not found for source {source.alias}")
                    continue

                # Create MSSQL connection (BUGFIX: the library stores dicts, MSSQLConnection expects ConnectionConfig)
                if isinstance(conn_config, dict):
                    from backend.core.config_manager import connection_config_from_document
                    conn_config = connection_config_from_document(conn_config)
                connection = MSSQLConnection(conn_config)
                if not connection.connect():
                    raise RuntimeError(f"cannot connect to {conn_config.server}/{conn_config.database}")
                self.connections[source.id] = connection

                # Create source monitor
                monitor = SourceCDCMonitor(source, connection)
                monitor.load_lsn_state()
                self.source_monitors[source.id] = monitor

                logger.info(f"Created monitor for source {source.alias} ({source.schema}.{source.table})")

            except Exception as e:
                logger.error(f"Error creating connection for source {source.alias}: {e}")

    def _close_connections(self):
        """Close all database connections."""
        for source_id, connection in self.connections.items():
            try:
                connection.disconnect()  # BUGFIX: MSSQLConnection has no close()
            except Exception as e:
                logger.error(f"Error closing connection for source {source_id}: {e}")

        self.connections.clear()
        self.source_monitors.clear()

    @staticmethod
    def _get_operation_type(operation_code: int) -> CDCOperation:
        """Convert CDC operation code to CDCOperation enum."""
        if operation_code == 1:
            return CDCOperation.DELETE
        elif operation_code == 2:
            return CDCOperation.INSERT
        elif operation_code in (3, 4):
            return CDCOperation.UPDATE
        else:
            return CDCOperation.INSERT

    async def _monitor_source(self, source_id: str):
        """Monitor a single source and queue events.

        Args:
            source_id: Source identifier
        """
        monitor = self.source_monitors.get(source_id)
        if not monitor:
            logger.warning(f"No monitor found for source {source_id}")
            return

        logger.info(f"Started monitoring source {monitor.source.alias}")

        while self.is_running:
            try:
                if not self.is_paused:
                    changes = await monitor.poll_changes()

                    for change in changes:
                        # Convert to CDCEvent
                        operation_code = change.get('__$operation', 2)
                        operation = self._get_operation_type(operation_code)

                        # Extract data (exclude CDC metadata)
                        data = {
                            k: v for k, v in change.items()
                            if not k.startswith('__$') and not k.startswith('_source')
                        }

                        event = CDCEvent(
                            id=str(uuid.uuid4()),
                            timestamp=datetime.now(),
                            source_table=f"{monitor.source.schema}.{monitor.source.table}",
                            operation=operation,
                            lsn=change['__$start_lsn'].hex() if '__$start_lsn' in change else '',
                            seqval=change['__$seqval'].hex() if '__$seqval' in change else '',
                            data=data,
                            status="pending"
                        )

                        # Add source metadata
                        event_dict = event.model_dump()
                        event_dict['_source_id'] = change['_source_id']
                        event_dict['_source_alias'] = change['_source_alias']

                        await self.event_queue.put(event_dict)

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"Error monitoring source {source_id}: {e}")
                await asyncio.sleep(self.poll_interval)

        logger.info(f"Stopped monitoring source {monitor.source.alias}")

    async def start(self):
        """Start multi-source CDC monitoring."""
        if self.is_running:
            logger.warning("Multi-source CDC monitor already running")
            return

        logger.info(f"Starting multi-source CDC monitor for mapping: {self.mapping.name}")

        # Create connections and monitors
        self._create_connections()

        if not self.source_monitors:
            raise Exception("No source monitors created - check connection configurations")

        self.is_running = True
        self.is_paused = False

        # Start monitoring task for each source
        for source_id in self.source_monitors.keys():
            task = asyncio.create_task(self._monitor_source(source_id))
            self.monitor_tasks.append(task)

        logger.info(f"Multi-source CDC monitor started with {len(self.source_monitors)} sources (direct queue mode)")

    async def stop(self):
        """Stop multi-source CDC monitoring."""
        if not self.is_running:
            logger.warning("Multi-source CDC monitor not running")
            return

        logger.info("Stopping multi-source CDC monitor...")
        self.is_running = False

        # Wait for all tasks to complete
        for task in self.monitor_tasks:
            try:
                await asyncio.wait_for(task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Monitor task did not stop gracefully, cancelling...")
                task.cancel()

        self.monitor_tasks.clear()

        # Close connections
        self._close_connections()

        logger.info("Multi-source CDC monitor stopped")

    def pause(self):
        """Pause CDC monitoring."""
        if not self.is_running:
            logger.warning("Multi-source CDC monitor not running")
            return

        self.is_paused = True
        logger.info("Multi-source CDC monitor paused")

    def resume(self):
        """Resume CDC monitoring."""
        if not self.is_running:
            logger.warning("Multi-source CDC monitor not running")
            return

        self.is_paused = False
        logger.info("Multi-source CDC monitor resumed")

    def get_status(self) -> Dict[str, Any]:
        """Get current monitor status.

        Returns:
            Status dictionary
        """
        return {
            'is_running': self.is_running,
            'is_paused': self.is_paused,
            'mapping_name': self.mapping.name,
            'source_count': len(self.source_monitors),
            'sources': [
                {
                    'alias': monitor.source.alias,
                    'schema': monitor.source.schema,
                    'table': monitor.source.table,
                    'cdc_enabled': monitor.is_active
                }
                for monitor in self.source_monitors.values()
            ],
            'poll_interval': self.poll_interval,
            'queue_size': self.event_queue.qsize(),
        }
