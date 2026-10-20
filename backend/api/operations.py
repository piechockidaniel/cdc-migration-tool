"""Operations API endpoints for CDC monitoring control."""
from fastapi import APIRouter, HTTPException
import logging
from typing import Dict, Any, Optional, List
from backend.models.schemas import SyncStatus, OperationStatus
from backend.core.history_manager import history_manager
from backend.core.config_manager import config_manager
from backend.db.mssql_manager import connection_pool
from backend.core.sql_mapping_service import sql_mapping_service
from backend.core.mapping_executor import mapping_executor
from backend.models.schemas import MappingType
from backend.core.multi_source_cdc_monitor import MultiSourceCDCMonitor
from backend.core.multi_source_sync_engine import MultiSourceSyncEngine
from backend.core import metrics, runtime
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)

router = APIRouter()

# Global multi-source monitor and sync engine instances
multi_source_monitor: Optional[MultiSourceCDCMonitor] = None
multi_source_sync_engine: Optional[MultiSourceSyncEngine] = None


def _workset_pipeline_id(workset_id: str) -> str:
    return f"ws_{workset_id}"


def _legacy_status(pipeline) -> dict:
    """Shape used by the original UI (sync_engine.get_statistics())."""
    if pipeline is None:
        return {"total_events": 0, "successful_events": 0, "failed_events": 0, "events_per_second": 0.0,
                "uptime_seconds": 0.0, "start_time": None, "last_event_time": None}
    st = pipeline.status()
    uptime = st["uptime_seconds"] or 0
    return {
        "total_events": st["events_applied"] + st["events_failed"],
        "successful_events": st["events_applied"],
        "failed_events": st["events_failed"],
        "events_per_second": round(st["events_applied"] / uptime, 2) if uptime else 0.0,
        "uptime_seconds": uptime,
        "start_time": st["started_at"],
        "last_event_time": st["last_event_at"],
    }


def _active_pipeline():
    active_workset = config_manager.get_active_workset()
    if not active_workset:
        return None, None
    return active_workset, runtime.pipelines.get(_workset_pipeline_id(active_workset.id))


@router.post("/start")
async def start_synchronization():
    """Start CDC synchronisation for the active working set.

    Implementation note: this now builds an event-driven ``ChangePipeline``
    (``backend/core/pipeline.py``) instead of the legacy polling monitor:
    watermark-only polling with adaptive back-off (or Service Broker push),
    at-least-once position commits, non-blocking API. Initial snapshots run
    as background *snapshot jobs* (chunked, resumable) and no longer block the
    request.
    """
    try:
        active_workset = config_manager.get_active_workset()
        if not active_workset:
            raise HTTPException(status_code=400, detail="No active working set configured")

        all_mappings = config_manager.get_mappings_for_workset(active_workset.id)
        if not all_mappings:
            raise HTTPException(status_code=400, detail="No mappings configured in working set")
        mappings = [m for m in all_mappings if m.mapping_type == MappingType.TABLE and not m.is_multi_source]
        if not mappings:
            raise HTTPException(status_code=400, detail="No table mappings configured in working set (CDC requires TABLE type)")

        # keep the legacy connection pool in sync for the admin/scan endpoints
        connection_pool.set_source(active_workset.source_connection)
        connection_pool.set_destination(active_workset.destination_connection)

        pipeline_id = _workset_pipeline_id(active_workset.id)
        existing = runtime.pipelines.get(pipeline_id)
        if existing and existing.state in ("running", "paused", "starting"):
            raise HTTPException(status_code=409, detail="Synchronization is already running")

        # pipelines/snapshots reference connections by library id: use the workset's ids when it has them,
        # otherwise register copies of the embedded (1.x) connection objects
        src_id = active_workset.source_connection_id or f"{pipeline_id}_source"
        dst_id = active_workset.destination_connection_id or f"{pipeline_id}_destination"
        if not active_workset.source_connection_id:
            config_manager.save_connection(src_id, {**active_workset.source_connection.model_dump(mode="json"), "id": src_id})
        if not active_workset.destination_connection_id:
            config_manager.save_connection(dst_id, {**active_workset.destination_connection.model_dump(mode="json"), "id": dst_id})

        # initial snapshots -> background jobs (chunked, resumable); CDC starts from the snapshot position
        snapshot_results = []
        for mapping in mappings:
            if mapping.perform_initial_snapshot and not mapping.snapshot_completed_at:
                try:
                    job = await asyncio.to_thread(lambda m=mapping: runtime.snapshot_manager.create_job(m, src_id, dst_id, pipeline_id=pipeline_id))
                    runtime.snapshot_manager.start(job.id)
                    snapshot_results.append({"mapping_id": mapping.id, "success": True, "job_id": job.id,
                                             "message": f"snapshot job {job.id} started in background ({job.estimated_rows or '?'} rows)"})
                except Exception as exc:
                    snapshot_results.append({"mapping_id": mapping.id, "success": False, "rows_processed": 0, "message": str(exc)})
                    logger.warning(f"Snapshot job creation failed for mapping {mapping.id}: {exc}")

        runtime.save_pipeline_definition(pipeline_id, {
            "id": pipeline_id, "source_connection_id": src_id, "destination_connection_id": dst_id,
            "workset_id": active_workset.id, "mapping_ids": [], "auto_start": False, "options": {},
        })
        pipeline = await asyncio.to_thread(runtime.start_pipeline_from_definition, pipeline_id)

        logger.info(f"Started synchronization for working set: {active_workset.name}")
        response = {
            "success": True,
            "message": f"Synchronization started for working set: {active_workset.name}",
            "workset_id": active_workset.id,
            "pipeline_id": pipeline.id,
        }
        if snapshot_results:
            response["snapshot_results"] = snapshot_results
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting synchronization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop")
async def stop_synchronization():
    """Stop CDC monitoring and synchronization."""
    try:
        _, pipeline = _active_pipeline()
        if pipeline is not None:
            await asyncio.to_thread(pipeline.stop)
        history_manager.flush_buffer()
        logger.info("Stopped synchronization")
        return {"success": True, "message": "Synchronization stopped"}
    except Exception as e:
        logger.error(f"Error stopping synchronization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pause")
async def pause_synchronization():
    """Pause CDC processing (events are held, positions are not advanced)."""
    try:
        _, pipeline = _active_pipeline()
        if pipeline is None:
            raise HTTPException(status_code=400, detail="Synchronization is not running")
        pipeline.pause()
        return {"success": True, "message": "Synchronization paused"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing synchronization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/resume")
async def resume_synchronization():
    """Resume CDC processing."""
    try:
        _, pipeline = _active_pipeline()
        if pipeline is None:
            raise HTTPException(status_code=400, detail="Synchronization is not running")
        pipeline.resume()
        return {"success": True, "message": "Synchronization resumed"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming synchronization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_status() -> SyncStatus:
    """Get current synchronization status."""
    try:
        active_workset, pipeline = _active_pipeline()
        state = pipeline.state if pipeline else "stopped"
        status = {"running": OperationStatus.RUNNING, "paused": OperationStatus.PAUSED, "starting": OperationStatus.STARTING,
                  "stopping": OperationStatus.STOPPING, "error": OperationStatus.ERROR}.get(state, OperationStatus.STOPPED)
        stats = _legacy_status(pipeline)
        current_lsn = None
        if pipeline:
            positions = [v for v in pipeline.status()["positions"].values() if v]
            if positions:
                current_lsn = str(positions[-1]["value"])

        return SyncStatus(
            status=status,
            working_set_id=active_workset.id if active_workset else None,
            working_set_name=active_workset.name if active_workset else None,
            start_time=datetime.fromisoformat(stats['start_time']) if stats['start_time'] else None,
            events_processed=stats['successful_events'],
            errors_count=stats['failed_events'],
            current_lsn=current_lsn,
            last_event_time=datetime.fromisoformat(stats['last_event_time']) if stats['last_event_time'] else None
        )

    except Exception as e:
        logger.error(f"Error getting status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_statistics() -> Dict[str, Any]:
    """Get detailed synchronization statistics."""
    try:
        _, pipeline = _active_pipeline()
        sync_stats = _legacy_status(pipeline)
        monitor_status = {
            "is_running": pipeline.state in ("running", "paused") if pipeline else False,
            "is_paused": pipeline.state == "paused" if pipeline else False,
            "active_mappings": len(pipeline.mappings) if pipeline else 0,
            "poll_interval": "adaptive",
            "queue_size": pipeline.status()["queue_depth"] if pipeline else 0,
        }
        latency_stats = {}
        if pipeline:
            latency_stats = {"lag_seconds": pipeline.lag_seconds,
                             "apply_latency": {k: v for k, v in metrics.registry.to_dict()["metrics"].get("apply_latency_seconds", {}).items() if k == "series"}}
        return {**sync_stats, "monitor_status": monitor_status, "latency_stats": latency_stats,
                "pipeline": pipeline.status() if pipeline else None}
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/latency/stats")
async def get_latency_statistics(mapping_id: Optional[str] = None, hours: int = 24) -> Dict[str, Any]:
    """Latency statistics (2.0: from the in-process metrics instead of a table in the destination)."""
    hist = metrics.registry.get("apply_latency_seconds")
    lag = metrics.registry.get("replication_lag_seconds")
    series = [child.snapshot() for _, child in hist.iter_series()] if hist else []
    total = sum(s["count"] for s in series)
    lags = [child.value for _, child in lag.iter_series()] if lag else []
    return {
        "total_records": total,
        "avg_latency_ms": round(sum(s["sum"] for s in series) / total * 1000.0, 2) if total else 0,
        "min_latency_ms": round(min(s["min"] for s in series) * 1000.0, 2) if series else None,
        "max_latency_ms": round(max(s["max"] for s in series) * 1000.0, 2) if series else None,
        "median_latency_ms": round(max((s["p50"] for s in series), default=0) * 1000.0, 2),
        "p95_latency_ms": round(max((s["p95"] for s in series), default=0) * 1000.0, 2),
        "p99_latency_ms": round(max((s["p99"] for s in series), default=0) * 1000.0, 2),
        "replication_lag_seconds": max(lags) if lags else None,
        "mapping_count": len(series), "hours": hours, "mapping_id": mapping_id,
    }


@router.get("/latency/records")
async def get_latency_records(mapping_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    """Per-table lag gauges (the 1.x per-row latency table was removed)."""
    lag = metrics.registry.get("replication_lag_seconds")
    records = [{"destination_table": dict(labels).get("table"), "latency_ms": round(child.value * 1000.0, 1)}
               for labels, child in lag.iter_series()] if lag else []
    return {"records": records[:limit], "count": len(records)}


@router.post("/mapping/{mapping_id}/execute")
async def execute_mapping(mapping_id: str) -> Dict[str, Any]:
    """Execute a mapping synchronization (SQL type only - TABLE mappings use CDC).
    
    Args:
        mapping_id: Mapping ID to execute
        
    Returns:
        Execution result with success status and row count
    """
    try:
        # Get unified mapping
        mapping = config_manager.get_mapping(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        if not mapping.enabled:
            raise HTTPException(status_code=400, detail="Mapping is disabled")
        
        if mapping.mapping_type != MappingType.SQL:
            raise HTTPException(status_code=400, detail=f"Mapping type {mapping.mapping_type} cannot be executed manually (use CDC for TABLE type)")
        
        # Get active workset for connections
        active_workset = config_manager.get_active_workset()
        if not active_workset:
            raise HTTPException(status_code=400, detail="No active working set configured")
        
        # Set up connections
        source_conn = connection_pool.set_source(active_workset.source_connection)
        dest_conn = connection_pool.set_destination(active_workset.destination_connection)
        
        # Execute SQL mapping
        success, message, rows_processed = sql_mapping_service.execute_mapping(
            mapping,
            source_conn,
            dest_conn
        )
        
        if success:
            logger.info(f"Mapping {mapping_id} executed successfully: {rows_processed} rows processed")
            return {
                "success": True,
                "message": message,
                "rows_processed": rows_processed,
                "mapping_id": mapping_id
            }
        else:
            logger.error(f"Mapping {mapping_id} execution failed: {message}")
            return {
                "success": False,
                "message": message,
                "rows_processed": 0,
                "mapping_id": mapping_id
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mapping/execute-transactional")
async def execute_mappings_transactionally(mapping_ids: List[str]) -> Dict[str, Any]:
    """Execute multiple mappings in transactions (grouped by destination table).
    
    Args:
        mapping_ids: List of mapping IDs to execute
        
    Returns:
        Dictionary with execution results for each mapping
    """
    try:
        # Get active workset for connections
        active_workset = config_manager.get_active_workset()
        if not active_workset:
            raise HTTPException(status_code=400, detail="No active working set configured")
        
        # Set up connections
        source_conn = connection_pool.set_source(active_workset.source_connection)
        dest_conn = connection_pool.set_destination(active_workset.destination_connection)
        
        # Get mappings
        mappings = [config_manager.get_mapping(mid) for mid in mapping_ids]
        mappings = [m for m in mappings if m is not None and m.enabled]
        
        if not mappings:
            return {
                "success": True,
                "message": "No valid enabled mappings found",
                "results": {}
            }
        
        # Execute using transaction executor
        results = mapping_executor.execute_mappings_transactionally(
            mappings,
            source_conn,
            dest_conn
        )
        
        total_rows = sum(r.get("rows_processed", 0) for r in results.values())
        successful = sum(1 for r in results.values() if r.get("success", False))
        
        return {
            "success": True,
            "message": f"Executed {successful}/{len(results)} mappings successfully",
            "total_rows_processed": total_rows,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing mappings transactionally: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mapping/execute-all-sql")
async def execute_all_sql_mappings() -> Dict[str, Any]:
    """Execute all enabled SQL mappings in transactions.
    
    Returns:
        Dictionary with execution results for each mapping
    """
    try:
        # Get active workset for connections
        active_workset = config_manager.get_active_workset()
        if not active_workset:
            raise HTTPException(status_code=400, detail="No active working set configured")
        
        # Set up connections
        source_conn = connection_pool.set_source(active_workset.source_connection)
        dest_conn = connection_pool.set_destination(active_workset.destination_connection)
        
        # Get all enabled SQL mappings
        all_mappings = config_manager.get_all_mappings()
        sql_mappings = [m for m in all_mappings if m.mapping_type == MappingType.SQL and m.enabled]
        
        if not sql_mappings:
            return {
                "success": True,
                "message": "No enabled SQL mappings found",
                "results": {}
            }
        
        # Execute using transaction executor
        results = mapping_executor.execute_mappings_transactionally(
            sql_mappings,
            source_conn,
            dest_conn
        )
        
        total_rows = sum(r.get("rows_processed", 0) for r in results.values())
        successful = sum(1 for r in results.values() if r.get("success", False))
        
        return {
            "success": True,
            "message": f"Executed {successful}/{len(results)} SQL mappings successfully",
            "total_rows_processed": total_rows,
            "results": results
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing SQL mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== MULTI-SOURCE SYNC ENDPOINTS ====================

@router.post("/multi-source/start")
async def start_multi_source_sync(mapping_id: str, destination_connection_id: Optional[str] = None):
    """Start multi-source CDC monitoring and synchronization for a specific mapping.

    Args:
        mapping_id: ID of the multi-source mapping to start

    Returns:
        Success response
    """
    global multi_source_monitor, multi_source_sync_engine

    try:
        # Get the mapping
        mapping = config_manager.get_mapping(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")

        if not mapping.is_multi_source:
            raise HTTPException(status_code=400, detail="Mapping is not a multi-source mapping")

        # Stop existing sync if running
        if multi_source_monitor and multi_source_monitor.is_running:
            logger.info("Stopping existing multi-source sync...")
            await multi_source_monitor.stop()
            if multi_source_sync_engine:
                await multi_source_sync_engine.stop()

        # Destination: explicit library connection, else the active working set's destination.
        # (BUGFIX: 1.x read ``mapping.destination_connection`` which does not exist on the model -> 500 on every start)
        dest_config = None
        if destination_connection_id:
            doc = config_manager.get_connection(destination_connection_id)
            if doc is None:
                raise HTTPException(status_code=404, detail=f"connection {destination_connection_id!r} not found")
            from backend.core.config_manager import connection_config_from_document
            dest_config = connection_config_from_document(doc)
        else:
            active_workset = config_manager.get_active_workset()
            if active_workset:
                dest_config = active_workset.destination_connection
            else:
                raise HTTPException(status_code=400, detail="No destination connection configured (pass destination_connection_id or activate a working set)")

        dest_conn = connection_pool.set_destination(dest_config)

        # Create multi-source CDC monitor
        multi_source_monitor = MultiSourceCDCMonitor(mapping)

        # Create multi-source sync engine
        multi_source_sync_engine = MultiSourceSyncEngine()
        multi_source_sync_engine.set_mapping(mapping)
        multi_source_sync_engine.set_destination_connection(dest_conn)
        multi_source_sync_engine.set_event_queue(multi_source_monitor.event_queue)

        # Add history logging listener
        def log_to_history(event, status, error):
            history_manager.log_event(event, status, error)

        multi_source_sync_engine.add_event_listener(log_to_history)

        # Start monitor
        await multi_source_monitor.start()

        # Start sync engine
        await multi_source_sync_engine.start()

        logger.info(f"Started multi-source sync for mapping: {mapping.name}")

        return {
            "success": True,
            "message": f"Multi-source synchronization started for: {mapping.name}",
            "mapping_id": mapping_id,
            "source_count": len(mapping.sources)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting multi-source sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/multi-source/stop")
async def stop_multi_source_sync():
    """Stop multi-source CDC monitoring and synchronization.

    Returns:
        Success response
    """

    try:
        if not multi_source_monitor or not multi_source_monitor.is_running:
            return {
                "success": True,
                "message": "Multi-source sync is not running"
            }

        # Stop sync engine
        if multi_source_sync_engine:
            await multi_source_sync_engine.stop()

        # Stop CDC monitor
        await multi_source_monitor.stop()

        # Flush history
        history_manager.flush_buffer()

        logger.info("Stopped multi-source synchronization")

        return {
            "success": True,
            "message": "Multi-source synchronization stopped"
        }

    except Exception as e:
        logger.error(f"Error stopping multi-source sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/multi-source/pause")
async def pause_multi_source_sync():
    """Pause multi-source CDC monitoring.

    Returns:
        Success response
    """

    try:
        if not multi_source_monitor or not multi_source_monitor.is_running:
            raise HTTPException(status_code=400, detail="Multi-source sync is not running")

        multi_source_monitor.pause()

        logger.info("Paused multi-source synchronization")

        return {
            "success": True,
            "message": "Multi-source synchronization paused"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing multi-source sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/multi-source/resume")
async def resume_multi_source_sync():
    """Resume multi-source CDC monitoring.

    Returns:
        Success response
    """

    try:
        if not multi_source_monitor or not multi_source_monitor.is_running:
            raise HTTPException(status_code=400, detail="Multi-source sync is not running")

        multi_source_monitor.resume()

        logger.info("Resumed multi-source synchronization")

        return {
            "success": True,
            "message": "Multi-source synchronization resumed"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming multi-source sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/multi-source/status")
async def get_multi_source_status():
    """Get current multi-source synchronization status.

    Returns:
        Status information
    """

    try:
        if not multi_source_monitor:
            return {
                "is_running": False,
                "message": "Multi-source sync not initialized"
            }

        monitor_status = multi_source_monitor.get_status()

        sync_stats = {}
        if multi_source_sync_engine:
            sync_stats = multi_source_sync_engine.get_statistics()

        return {
            "is_running": monitor_status.get('is_running', False),
            "is_paused": monitor_status.get('is_paused', False),
            "mapping_name": monitor_status.get('mapping_name'),
            "source_count": monitor_status.get('source_count', 0),
            "sources": monitor_status.get('sources', []),
            "queue_size": monitor_status.get('queue_size', 0),
            "pending_batches": monitor_status.get('pending_batches', 0),
            "sync_statistics": sync_stats
        }

    except Exception as e:
        logger.error(f"Error getting multi-source status: {e}")
        raise HTTPException(status_code=500, detail=str(e))



