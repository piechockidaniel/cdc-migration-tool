"""Audit, dead-letter queue, failure patterns, alert rules and metrics endpoints."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from backend.connectors.base import ChangeEvent, Operation, Position
from backend.core import metrics, runtime
from backend.core.audit import RULE_SETS, Rule, mine_patterns, summarize
from backend.core.config_manager import config_manager
from backend.core.pipeline import pipelines
from backend.storage.base import ALERT_RULES

logger = logging.getLogger(__name__)
audit_router = APIRouter()
alerts_router = APIRouter()
metrics_router = APIRouter()


# ---------------------------------------------------------------------------
# audit trail
# ---------------------------------------------------------------------------
@audit_router.get("/events")
async def audit_events(category: Optional[str] = None, table: Optional[str] = None, severity: Optional[str] = None,
                       status: Optional[str] = None, hours: float = 24, limit: int = 200, offset: int = 0):
    return await asyncio.to_thread(lambda: runtime.audit.query(category, table, severity, status, hours, min(limit, 2000), offset))


@audit_router.get("/stats")
async def audit_stats(hours: float = 24):
    return await asyncio.to_thread(runtime.audit.stats, hours)


@audit_router.post("/purge")
async def audit_purge(audit_days: int = 30, failures_days: int = 90):
    return await asyncio.to_thread(runtime.audit.purge, audit_days, failures_days)


# ---------------------------------------------------------------------------
# dead-letter queue + patterns
# ---------------------------------------------------------------------------
@audit_router.get("/failures")
async def failures(resolved: Optional[bool] = False, table: Optional[str] = None, signature: Optional[str] = None,
                   hours: Optional[float] = None, limit: int = 200, offset: int = 0):
    return await asyncio.to_thread(lambda: runtime.audit.failures(resolved, table, signature, hours, min(limit, 2000), offset))


@audit_router.get("/failures/{failure_id}")
async def failure_detail(failure_id: int):
    f = runtime.audit.failure(failure_id)
    if f is None:
        raise HTTPException(status_code=404, detail="failure not found")
    return f


@audit_router.get("/patterns")
async def failure_patterns(hours: float = 24, include_resolved: bool = False):
    groups = await asyncio.to_thread(runtime.audit.failure_groups, hours, include_resolved)
    patterns = mine_patterns(groups)
    return {"summary": summarize(patterns), "patterns": [p.to_json() for p in patterns]}


class ApplyPatternRequest(BaseModel):
    signature: str
    action: str = Field(..., description="disable_mapping | set_sync_mode | set_default_value | add_transformation | set_key_columns")
    mapping_id: Optional[str] = Field(None, description="defaults to the mapping whose destination matches the pattern")
    params: Dict[str, Any] = Field(default_factory=dict, description="column, value, expression, key_columns ...")
    resolve: bool = Field(True, description="mark the pattern's failures as resolved after applying")


@audit_router.post("/patterns/apply")
async def apply_pattern(req: ApplyPatternRequest):
    """Apply a proposed mapping change derived from a failure pattern (observe -> classify -> adjust)."""
    from backend.core.audit.patterns import apply_proposed_change
    groups = [g for g in runtime.audit.failure_groups(hours=None, include_resolved=True) if g["signature"] == req.signature]
    if not groups:
        raise HTTPException(status_code=404, detail="unknown signature")
    group = groups[0]
    if req.mapping_id:
        mapping = config_manager.get_mapping(req.mapping_id)
    else:
        candidates = [m for m in config_manager.get_all_mappings()
                      if f"{m.destination_schema}.{m.destination_table}" == group["dest_table"]
                      and f"{m.source_schema}.{m.source_table}" == group["source_table"]]
        if len(candidates) > 1:
            raise HTTPException(status_code=409, detail=f"several mappings match this pattern: {[m.id for m in candidates]}; pass mapping_id")
        mapping = candidates[0] if candidates else None
    if mapping is None:
        raise HTTPException(status_code=404, detail="no mapping matches this pattern; pass mapping_id")
    try:
        change = apply_proposed_change(mapping, req.action, group.get("sample_error") or "", **req.params)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    config_manager.update_mapping(mapping)
    resolved = runtime.audit.resolve_failures(signature=req.signature, resolution=f"mapping change: {req.action}") if req.resolve else 0
    runtime.audit.record("config", "patterns", f"applied {req.action} to mapping {mapping.id} for signature {req.signature[:60]}",
                         severity="warning", table=group["dest_table"], payload={"change": change, "resolved": resolved})
    running = [p.id for p in pipelines.all() if any(m.id == mapping.id for m in p.mappings) and p.state in ("running", "paused")]
    return {"change": change, "resolved_failures": resolved,
            "note": f"restart pipelines {running} to pick up the new mapping" if running else "no running pipeline uses this mapping"}


class ResolveRequest(BaseModel):
    ids: Optional[List[int]] = None
    signature: Optional[str] = None
    resolution: str = "manual"


@audit_router.post("/failures/resolve")
async def resolve_failures(req: ResolveRequest):
    n = runtime.audit.resolve_failures(req.ids, req.signature, req.resolution)
    runtime.audit.record("config", "dlq", f"{n} failures resolved ({req.resolution})")
    return {"resolved": n}


class ReplayRequest(BaseModel):
    ids: Optional[List[int]] = None
    signature: Optional[str] = None
    pipeline_id: str = Field(..., description="running pipeline whose applier/destination should be used")
    limit: int = 1000


@audit_router.post("/failures/replay")
async def replay_failures(req: ReplayRequest):
    """Re-apply failed events through a running pipeline's applier (after fixing the cause)."""
    p = pipelines.get(req.pipeline_id)
    if p is None or p.state not in ("running", "paused"):
        raise HTTPException(status_code=409, detail="pipeline must be running (or paused)")
    rows = runtime.audit.failures(resolved=False, signature=req.signature, limit=req.limit) if req.signature else \
        [runtime.audit.failure(i) for i in (req.ids or [])]
    rows = [r for r in rows if r]
    events = []
    for r in rows:
        payload = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        events.append((r["id"], ChangeEvent(
            source=payload["source"], schema=payload["schema"], table=payload["table"], operation=Operation(payload["operation"]),
            position=Position.from_json(payload["position"]), key=payload.get("key") or {}, after=payload.get("after"),
            before=payload.get("before"), event_id=payload.get("event_id"), metadata=payload.get("metadata") or {})))

    def _replay():
        result = p.applier.apply([e for _, e in events], record_failures=False)
        failed_ids = {id(f.event) for f in result.failures}
        ok_ids = [fid for fid, e in events if id(e) not in failed_ids]
        if ok_ids:
            runtime.audit.resolve_failures(ok_ids, resolution="replayed")
        for fid, e in events:
            if id(e) in failed_ids:
                err = next(f.error for f in result.failures if f.event is e)
                runtime.audit.bump_attempt(fid, err)
        return {"replayed": len(ok_ids), "still_failing": len(failed_ids)}
    out = await asyncio.to_thread(_replay)
    runtime.audit.record("config", "dlq", f"replay: {out}", severity="warning" if out["still_failing"] else "info")
    return out


# ---------------------------------------------------------------------------
# alerts & rules
# ---------------------------------------------------------------------------
@alerts_router.get("")
async def alerts_snapshot():
    return runtime.alert_manager.snapshot()


@alerts_router.post("/evaluate")
async def evaluate_now():
    contexts = await asyncio.to_thread(runtime.health_contexts)
    fired = runtime.alert_manager.evaluate(contexts)
    return {"fired": [a.to_json() for a in fired], "contexts": contexts}


@alerts_router.get("/context")
async def alert_context():
    return await asyncio.to_thread(runtime.health_contexts)


@alerts_router.get("/rule-sets")
async def rule_sets():
    enabled = runtime.alert_manager.enabled_rule_sets
    return [{"id": k, "description": v, "enabled": enabled is None or k in enabled,
             "rules": [r.id for r in runtime.alert_manager.rules.values() if r.rule_set == k]} for k, v in RULE_SETS.items()]


class RuleSetsRequest(BaseModel):
    enabled: Optional[List[str]] = Field(None, description="null = all rule sets enabled")


@alerts_router.put("/rule-sets")
async def set_rule_sets(req: RuleSetsRequest):
    config_manager.set_setting("alerting.enabled_rule_sets", req.enabled)
    runtime.reload_alerting()
    return {"enabled": req.enabled}


@alerts_router.get("/rules")
async def list_rules():
    return [r.to_json() for r in runtime.alert_manager.rules.values()]


class RuleModel(BaseModel):
    id: str
    name: str
    condition: str
    severity: str = "warning"
    description: str = ""
    rule_set: str = "custom"
    cooldown_seconds: int = 900
    for_seconds: int = 0
    enabled: bool = True
    scope: str = "global"
    labels: Dict[str, str] = Field(default_factory=dict)
    message: str = ""


@alerts_router.put("/rules")
async def upsert_rule(rule: RuleModel):
    from backend.core.audit.rules import SafeEvaluator, ExpressionError
    try:
        SafeEvaluator({"pipeline": {}, "source": {}, "failures": {}, "snapshot": {}}).evaluate(rule.condition)
    except ExpressionError as exc:
        raise HTTPException(status_code=400, detail=f"invalid condition: {exc}")
    config_manager.store.put(ALERT_RULES, rule.id, rule.model_dump())
    runtime.alert_manager.upsert_rule(Rule.from_json(rule.model_dump()))
    runtime.audit.record("config", "alerting", f"rule {rule.id} saved", payload=rule.model_dump())
    return rule.model_dump()


@alerts_router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str):
    config_manager.store.delete(ALERT_RULES, rule_id)
    runtime.reload_alerting()
    return {"success": True, "builtin_restored": any(r.id == rule_id for r in runtime.alert_manager.rules.values())}


class ChannelsRequest(BaseModel):
    webhook_url: Optional[str] = None
    email: Optional[Dict[str, Any]] = Field(None, description="{host, port, sender, recipients[], username, password, use_tls}")


@alerts_router.put("/channels")
async def set_channels(req: ChannelsRequest):
    config_manager.set_setting("alerting.webhook_url", req.webhook_url)
    config_manager.set_setting("alerting.email", req.email)
    runtime.reload_alerting()
    return {"channels": [c.name for c in runtime.alert_manager.channels]}


@alerts_router.post("/test")
async def test_alert():
    from backend.core.audit.alerting import Alert
    a = Alert("test", "global", "info", "Test alert", "This is a test notification from cdcsync")
    runtime.alert_manager._notify(a, resolved=False)
    return {"sent_to": [c.name for c in runtime.alert_manager.channels]}


# ---------------------------------------------------------------------------
# metrics
# ---------------------------------------------------------------------------
@metrics_router.get("/metrics", include_in_schema=False)
async def prometheus_metrics():
    return Response(content=metrics.registry.to_prometheus(), media_type="text/plain; version=0.0.4")


@metrics_router.get("/api/metrics")
async def json_metrics():
    return metrics.registry.to_dict()


@metrics_router.get("/api/metrics/history")
async def metrics_history(minutes: float = 60, prefix: Optional[str] = None, max_points: int = 400):
    """Sampled time series (rate.applied, rate.failed, lag.<table>, queue.<pipeline>, apply_p95.<table> ...)."""
    from backend.core.metrics_history import history
    return {"interval_s": history.interval_s, "series": history.series(minutes, prefix, min(max_points, 2000))}


class ValidateRuleRequest(BaseModel):
    condition: str


@alerts_router.post("/rules/validate")
async def validate_rule(req: ValidateRuleRequest):
    from backend.core.audit.rules import KNOWN_FACT_GROUPS, SafeEvaluator, ExpressionError, referenced_names, FUNCTIONS
    contexts = await asyncio.to_thread(runtime.health_contexts)
    results = {}
    try:
        unknown = referenced_names(req.condition) - KNOWN_FACT_GROUPS - set(FUNCTIONS) - {"True", "False", "None"}
        if unknown:
            return {"valid": False, "error": f"unknown name(s) {sorted(unknown)}; facts are {sorted(KNOWN_FACT_GROUPS - {'global'})}"}
        for scope, ctx in contexts.items():
            merged = dict(contexts.get("global", {}))
            merged.update(ctx)
            results[scope] = bool(SafeEvaluator(merged).evaluate(req.condition))
    except (ExpressionError, SyntaxError) as exc:
        return {"valid": False, "error": str(exc)}
    except Exception as exc:  # pragma: no cover
        return {"valid": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"valid": True, "results": results}


# ---------------------------------------------------------------------------
# settings / housekeeping
# ---------------------------------------------------------------------------
settings_router = APIRouter()


@settings_router.get("/info")
async def settings_info():
    import os
    import platform
    import sys
    store = config_manager.store
    return {
        "version": "2.0.0",
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "config_backend": store.backend_name,
        "config_dir": str(config_manager.config_dir),
        "history_dir": str(runtime.HISTORY_DIR),
        "audit_level": runtime.audit.audit_level,
        "collections": store.stats()["collections"],
        "kafka_mirror": bool(os.environ.get("CDCSYNC_KAFKA_BOOTSTRAP")),
        "alert_interval_s": float(os.environ.get("CDCSYNC_ALERT_INTERVAL", "30")),
        "max_parallel_snapshots": int(os.environ.get("CDCSYNC_MAX_PARALLEL_SNAPSHOTS", "2")),
    }


@settings_router.get("/export")
async def settings_export(include_secrets: bool = False):
    """Full configuration as JSON (connections redacted unless include_secrets=true)."""
    from backend.storage.secrets import redact
    connections = {c.get("id", i): (c if include_secrets else redact(c)) for i, c in enumerate(config_manager.get_all_connections())}
    return {
        "exported_at": __import__("datetime").datetime.now().isoformat(),
        "worksets": {ws.id: ws.model_dump(mode="json") for ws in config_manager.get_all_worksets()},
        "mappings": {m.id: m.model_dump(mode="json") for m in config_manager.get_all_mappings()},
        "connections": connections,
        "positions": config_manager.get_all_positions(),
        "pipelines": runtime.pipeline_definitions(),
        "alert_rules": {d.key: d.body for d in config_manager.store.get_all(ALERT_RULES)},
    }


class ImportRequest(BaseModel):
    data: Dict[str, Any]
    overwrite: bool = False


@settings_router.post("/import")
async def settings_import(req: ImportRequest):
    from backend.models.schemas import Mapping, WorkingSet
    from backend.storage.base import CONNECTIONS, MAPPINGS, POSITIONS, WORKSETS, Document
    docs, skipped = [], 0
    data = req.data

    def add(collection, key, body):
        nonlocal skipped
        if not req.overwrite and config_manager.store.get(collection, key) is not None:
            skipped += 1
            return
        docs.append(Document(collection, key, body, updated_by="import"))
    try:
        for k, v in (data.get("connections") or {}).items():
            if isinstance(v.get("password"), str) and v["password"] == "***":
                v = {**v, "password": (config_manager.get_connection(k) or {}).get("password")}
            add(CONNECTIONS, str(k), v)
        for k, v in (data.get("mappings") or {}).items():
            add(MAPPINGS, k, Mapping(**v).model_dump(mode="json"))
        for k, v in (data.get("worksets") or {}).items():
            add(WORKSETS, k, WorkingSet(**v).model_dump(mode="json"))
        for k, v in (data.get("positions") or {}).items():
            add(POSITIONS, k, v)
        for k, v in (data.get("pipelines") or {}).items():
            add(runtime.PIPELINES, k, v)
        for k, v in (data.get("alert_rules") or {}).items():
            add(ALERT_RULES, k, v)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid import document: {exc}")
    config_manager.store.put_many(docs)
    runtime.reload_alerting()
    runtime.audit.record("config", "settings", f"imported {len(docs)} documents ({skipped} skipped)", severity="warning")
    return {"imported": len(docs), "skipped": skipped}
