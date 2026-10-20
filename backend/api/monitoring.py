"""Monitoring API endpoints with WebSocket support."""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from typing import List, Dict, Any, Optional
import logging
from datetime import date
from backend.models.schemas import SyncStatistics, VerificationRequest, VerificationResult
from backend.core.history_manager import history_manager
from backend.core.data_quality import DataQualityVerificator
from backend.db.mssql_manager import connection_pool
from backend.core.duckdb_processor import duckdb_processor
from backend.core.config_manager import config_manager

logger = logging.getLogger(__name__)

router = APIRouter()


# WebSocket connection manager
class ConnectionManager:
    """Manages WebSocket connections for live event streaming."""
    
    def __init__(self):
        """Initialize connection manager."""
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        """Accept and store a new WebSocket connection.
        
        Args:
            websocket: WebSocket connection
        """
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Active connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection.
        
        Args:
            websocket: WebSocket connection
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Active connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Broadcast a message to all connected clients.
        
        Args:
            message: Message to broadcast
        """
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error sending message to WebSocket: {e}")
                disconnected.append(connection)
        
        # Clean up disconnected connections
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


# Event listener for broadcasting CDC events
async def broadcast_event(event, status, error):
    """Broadcast CDC event to WebSocket clients.
    
    Args:
        event: CDC event
        status: Event status
        error: Error message if failed
    """
    message = {
        'type': 'cdc_event',
        'event_id': event.id,
        'timestamp': event.timestamp.isoformat(),
        'source_table': event.source_table,
        'operation': event.operation.value,
        'status': status,
        'error': error
    }
    await manager.broadcast(message)



def attach_bus_broadcaster():
    """Forward pipeline events and alerts from the event bus to WebSocket clients.

    The bus observers run on worker threads; the coroutine is scheduled on the
    running loop with ``run_coroutine_threadsafe``.
    """
    import asyncio
    from backend.core.event_bus import bus

    loop = asyncio.get_running_loop()

    def on_legacy(msg):
        payload = msg.payload
        asyncio.run_coroutine_threadsafe(broadcast_event(payload["event"], payload["status"], payload["error"]), loop)

    def on_alert(msg):
        asyncio.run_coroutine_threadsafe(manager.broadcast({"type": "alert", **msg.payload}), loop)

    bus.on("events.legacy", on_legacy)
    bus.on("alerts", on_alert)


@router.websocket("/events")
async def websocket_events(websocket: WebSocket):
    """WebSocket endpoint for live CDC event streaming.
    
    Args:
        websocket: WebSocket connection
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            data = await websocket.receive_text()
            
            # Echo back for ping/pong
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


@router.get("/statistics")
async def get_statistics() -> SyncStatistics:
    """Synchronization statistics aggregated over all running pipelines."""
    try:
        from backend.core import metrics as m
        from backend.core.pipeline import pipelines
        active = pipelines.all()
        applied = sum(p.events_applied for p in active)
        failed = sum(p.events_failed for p in active)
        uptime = max((p.status()["uptime_seconds"] for p in active), default=0.0)
        avg_ms = 0.0
        apply_hist = m.registry.get("apply_latency_seconds")
        if apply_hist is not None:
            snaps = [child.snapshot() for _, child in apply_hist.iter_series()]
            total = sum(s["count"] for s in snaps)
            avg_ms = (sum(s["sum"] for s in snaps) / total * 1000.0) if total else 0.0
        return SyncStatistics(total_events=applied + failed, successful_events=applied, failed_events=failed,
                              events_per_second=round(applied / uptime, 2) if uptime else 0.0,
                              average_latency_ms=round(avg_ms, 2), uptime_seconds=uptime)
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/errors")
async def get_recent_errors(limit: int = 50) -> List[Dict[str, Any]]:
    """Get recent error events.
    
    Args:
        limit: Maximum number of errors to return
        
    Returns:
        List of error events
    """
    try:
        return history_manager.get_recent_errors(limit)
    except Exception as e:
        logger.error(f"Error getting recent errors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def query_history(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source_table: Optional[str] = None,
    operation: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Query historical CDC events.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        source_table: Source table filter
        operation: Operation type filter
        status: Status filter
        limit: Maximum number of results
        
    Returns:
        List of historical events
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None
        
        return history_manager.query_events(
            start_date=start,
            end_date=end,
            source_table=source_table,
            operation=operation,
            status=status,
            limit=limit
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error querying history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history/statistics")
async def get_history_statistics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """Get statistics for historical events.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Historical statistics
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None
        
        return history_manager.get_statistics(start, end)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error getting history statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify")
async def verify_data_quality(request: VerificationRequest) -> VerificationResult:
    """Run data quality verification for a mapping.
    
    Args:
        request: Verification request
        
    Returns:
        Verification result
    """
    try:
        # Get mapping
        mapping = config_manager.get_mapping(request.mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        # Get connections
        source_conn = connection_pool.get_source()
        dest_conn = connection_pool.get_destination()
        
        if not source_conn or not dest_conn:
            raise HTTPException(status_code=400, detail="Source and destination connections must be configured")
        
        # Ensure connections are active
        if not source_conn.is_connected():
            source_conn.connect()
        if not dest_conn.is_connected():
            dest_conn.connect()
        
        # Create verificator
        verificator = DataQualityVerificator(source_conn, dest_conn, duckdb_processor)
        
        # Run verification
        result = verificator.verify_mapping(
            mapping,
            sample_size=request.sample_size,
            check_row_counts=request.check_row_counts,
            check_reverse_mapping=request.check_reverse_mapping
        )
        
        logger.info(f"Verification completed for mapping {request.mapping_id}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error verifying data quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/verify/results/{mapping_id}")
async def get_verification_results(mapping_id: str) -> Dict[str, Any]:
    """Get cached verification results for a mapping.
    
    Note: This is a placeholder - in production, you'd want to cache results
    
    Args:
        mapping_id: Mapping ID
        
    Returns:
        Cached verification results
    """
    # For now, return empty results
    # In production, implement caching mechanism
    return {
        "mapping_id": mapping_id,
        "message": "No cached results available. Run verification first."
    }


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check for monitoring system.
    
    Returns:
        Health status
    """
    from backend.core.pipeline import pipelines
    return {
        "status": "healthy",
        "websocket_connections": len(manager.active_connections),
        "sync_engine_running": any(p.state in ("running", "paused") for p in pipelines.all()),
        "pipelines": {p.id: p.state for p in pipelines.all()},
    }





