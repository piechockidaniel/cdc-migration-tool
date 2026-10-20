"""Pipeline + snapshot job API."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.core import runtime
from backend.core.config_manager import config_manager
from backend.core.pipeline import pipelines
from backend.core.snapshot_manager import JOB_STATES

logger = logging.getLogger(__name__)
router = APIRouter()
snapshot_router = APIRouter()


class PipelineDefinition(BaseModel):
    id: str
    source_connection_id: str
    destination_connection_id: str
    mapping_ids: List[str] = Field(default_factory=list)
    workset_id: Optional[str] = None
    auto_start: bool = False
    options: Dict[str, Any] = Field(default_factory=dict, description="batch_size, batch_wait_s, initial_position (earliest|latest), insert_mode (upsert|insert), expect_traffic")


@router.get("")
async def list_pipelines():
    running = {p.id: p.status() for p in pipelines.all()}
    out = []
    for pid, definition in runtime.pipeline_definitions().items():
        out.append({"definition": definition, "runtime": running.get(pid)})
    for pid, st in running.items():
        if pid not in runtime.pipeline_definitions():
            out.append({"definition": None, "runtime": st})
    return out


@router.post("")
async def define_pipeline(definition: PipelineDefinition):
    for cid in (definition.source_connection_id, definition.destination_connection_id):
        if config_manager.get_connection(cid) is None:
            raise HTTPException(status_code=404, detail=f"connection {cid!r} not found")
    if not definition.mapping_ids and not definition.workset_id:
        raise HTTPException(status_code=400, detail="mapping_ids or workset_id required")
    runtime.save_pipeline_definition(definition.id, definition.model_dump())
    runtime.audit.record("config", "pipelines", f"pipeline {definition.id} defined", payload=definition.model_dump())
    return {"success": True, "id": definition.id}


@router.delete("/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    p = pipelines.get(pipeline_id)
    if p and p.state not in ("stopped", "error"):
        raise HTTPException(status_code=409, detail="stop the pipeline first")
    pipelines.remove(pipeline_id)
    config_manager.store.delete(runtime.PIPELINES, pipeline_id)
    return {"success": True}


@router.post("/{pipeline_id}/start")
async def start_pipeline(pipeline_id: str):
    try:
        p = await asyncio.to_thread(runtime.start_pipeline_from_definition, pipeline_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return p.status()


@router.post("/{pipeline_id}/stop")
async def stop_pipeline(pipeline_id: str):
    p = pipelines.get(pipeline_id)
    if p is None:
        raise HTTPException(status_code=404, detail="pipeline not running")
    await asyncio.to_thread(p.stop)
    return p.status()


@router.post("/{pipeline_id}/pause")
async def pause_pipeline(pipeline_id: str):
    p = pipelines.get(pipeline_id)
    if p is None:
        raise HTTPException(status_code=404, detail="pipeline not running")
    p.pause()
    return p.status()


@router.post("/{pipeline_id}/resume")
async def resume_pipeline(pipeline_id: str):
    p = pipelines.get(pipeline_id)
    if p is None:
        raise HTTPException(status_code=404, detail="pipeline not running")
    p.resume()
    return p.status()


@router.get("/{pipeline_id}/status")
async def pipeline_status(pipeline_id: str):
    p = pipelines.get(pipeline_id)
    if p is None:
        definition = runtime.pipeline_definitions().get(pipeline_id)
        if definition is None:
            raise HTTPException(status_code=404, detail="pipeline not found")
        return {"id": pipeline_id, "state": "stopped", "definition": definition}
    return p.status()


@router.get("/{pipeline_id}/health")
async def pipeline_health(pipeline_id: str):
    p = pipelines.get(pipeline_id)
    if p is None:
        raise HTTPException(status_code=404, detail="pipeline not running")
    return await asyncio.to_thread(p.health_context)


@router.post("/{pipeline_id}/positions/reset")
async def reset_positions(pipeline_id: str, table: Optional[str] = None):
    """Forget committed positions (next start re-reads from 'earliest'/'latest'). Use with care."""
    p = pipelines.get(pipeline_id)
    if p and p.state not in ("stopped", "error"):
        raise HTTPException(status_code=409, detail="stop the pipeline first")
    removed = []
    for key in list(config_manager.get_all_positions()):
        if key.startswith(f"{pipeline_id}:") and (table is None or key == f"{pipeline_id}:{table}"):
            config_manager.clear_position(key)
            removed.append(key)
    runtime.audit.record("config", "pipelines", f"positions reset for {pipeline_id}: {removed}", severity="warning")
    return {"removed": removed}


@router.get("/bus/stats")
async def bus_stats():
    return runtime.bus.stats()


# ---------------------------------------------------------------------------
# snapshots
# ---------------------------------------------------------------------------
class SnapshotRequest(BaseModel):
    mapping_id: str
    source_connection_id: str
    destination_connection_id: str
    pipeline_id: Optional[str] = None
    key_columns: Optional[List[str]] = None
    where: Optional[str] = None
    chunk_size: int = 5000
    min_chunk_size: int = 500
    max_chunk_size: int = 50000
    max_chunk_seconds: float = 20.0
    max_rows_per_second: Optional[int] = None
    pause_between_chunks_ms: int = 0
    max_retries: int = 5
    windows: List[Dict[str, Any]] = Field(default_factory=list, description='[{"start":"22:00","end":"06:00","days":[0,1,2,3,4]}]')
    start: bool = True


@snapshot_router.get("")
async def list_snapshots(state: Optional[str] = None):
    jobs = runtime.snapshot_manager.list()
    if state:
        if state not in JOB_STATES:
            raise HTTPException(status_code=400, detail=f"state must be one of {JOB_STATES}")
        jobs = [j for j in jobs if j.state == state]
    return [j.to_json() for j in jobs]


@snapshot_router.post("")
async def create_snapshot(req: SnapshotRequest):
    mapping = config_manager.get_mapping(req.mapping_id)
    if mapping is None:
        raise HTTPException(status_code=404, detail="mapping not found")
    opts = req.model_dump(exclude={"mapping_id", "source_connection_id", "destination_connection_id", "pipeline_id", "start"})
    try:
        job = await asyncio.to_thread(lambda: runtime.snapshot_manager.create_job(
            mapping, req.source_connection_id, req.destination_connection_id, pipeline_id=req.pipeline_id, **opts))
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if req.start:
        runtime.snapshot_manager.start(job.id)
    return runtime.snapshot_manager.get(job.id).to_json()


@snapshot_router.get("/{job_id}")
async def get_snapshot(job_id: str):
    job = runtime.snapshot_manager.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job.to_json()


def _control(job_id: str, action: str):
    mgr = runtime.snapshot_manager
    try:
        job = getattr(mgr, action)(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return job.to_json() if job is not None else {"success": True}


@snapshot_router.post("/{job_id}/start")
async def start_snapshot(job_id: str):
    return _control(job_id, "start")


@snapshot_router.post("/{job_id}/pause")
async def pause_snapshot(job_id: str):
    return _control(job_id, "pause")


@snapshot_router.post("/{job_id}/resume")
async def resume_snapshot(job_id: str):
    return _control(job_id, "resume")


@snapshot_router.post("/{job_id}/cancel")
async def cancel_snapshot(job_id: str):
    return _control(job_id, "cancel")


@snapshot_router.post("/{job_id}/reset")
async def reset_snapshot(job_id: str):
    return _control(job_id, "reset")


@snapshot_router.delete("/{job_id}")
async def delete_snapshot(job_id: str):
    try:
        if not runtime.snapshot_manager.delete(job_id):
            raise HTTPException(status_code=404, detail="job not found")
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"success": True}
