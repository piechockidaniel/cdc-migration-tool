"""Process-wide runtime: wires store, audit, bus, alerting, snapshots and pipelines together.

Everything here is lazily constructed from the configuration store so the
API layer only deals with ids. ``CDCSYNC_DISABLE_BACKGROUND=1`` keeps the
background alert loop off (tests).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from backend.connectors import SourceConnector, create_connector
from backend.core.audit import (
    AlertChannel, AlertManager, AuditChannel, AuditStore, BusChannel, EmailChannel, LogChannel, Rule, WebhookChannel,
    BUILTIN_RULES, mine_patterns,
)
from backend.core.config_manager import config_manager
from backend.core.event_bus import KafkaTransport, bus
from backend.core.pipeline import ChangePipeline, pipelines
from backend.core.snapshot_manager import SnapshotManager
from backend.destinations import SqlDestination, create_destination
from backend.models.schemas import ConnectionConfig, Mapping
from backend.storage.base import ALERT_RULES
from backend.storage.secrets import resolve_secrets

logger = logging.getLogger(__name__)

HISTORY_DIR = Path(os.environ.get("CDCSYNC_HISTORY_DIR", "history"))
PIPELINES = "pipelines"  # store collection for pipeline definitions

# ---------------------------------------------------------------------------
# audit store
# ---------------------------------------------------------------------------
audit = AuditStore(HISTORY_DIR / "audit.sqlite3", audit_level=os.environ.get("CDCSYNC_AUDIT_LEVEL", "summary"))

# ---------------------------------------------------------------------------
# event bus transports (Kafka / Confluent mirror)
# ---------------------------------------------------------------------------
if os.environ.get("CDCSYNC_KAFKA_BOOTSTRAP"):
    try:  # pragma: no cover - needs confluent-kafka
        bus.transports.append(KafkaTransport(os.environ["CDCSYNC_KAFKA_BOOTSTRAP"],
                                             topic_prefix=os.environ.get("CDCSYNC_KAFKA_PREFIX", "cdcsync.")))
        logger.info("Kafka mirror enabled -> %s", os.environ["CDCSYNC_KAFKA_BOOTSTRAP"])
    except Exception as err:
        logger.error("Kafka transport unavailable: %s", err)


# ---------------------------------------------------------------------------
# connection factories
# ---------------------------------------------------------------------------
def connection_document(connection_id: str) -> Dict[str, Any]:
    doc = config_manager.get_connection(connection_id)
    if doc is None:
        raise KeyError(f"connection {connection_id!r} not found in the connection library")
    return doc


def connector_config_from(config: ConnectionConfig | Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a ConnectionConfig (or library document) into a connector config dict."""
    data = config.model_dump() if isinstance(config, ConnectionConfig) else dict(config)
    options = data.pop("options", None) or {}
    data.update(options)
    data.setdefault("engine", "mssql")
    data.setdefault("id", data.get("name"))
    return resolve_secrets(data)


def connector_for(connection_id: str) -> SourceConnector:
    return create_connector(connector_config_from(connection_document(connection_id)))


def connector_from_config(config: ConnectionConfig | Dict[str, Any]) -> SourceConnector:
    return create_connector(connector_config_from(config))


def destination_for(connection_id: str) -> SqlDestination:
    cfg = connector_config_from(connection_document(connection_id))
    return create_destination(cfg, name=connection_id)


def destination_from_config(config: ConnectionConfig | Dict[str, Any], name: str = "destination") -> SqlDestination:
    return create_destination(connector_config_from(config), name=name)


# ---------------------------------------------------------------------------
# snapshot manager
# ---------------------------------------------------------------------------
def _duckdb():
    from backend.core.duckdb_processor import duckdb_processor
    return duckdb_processor


snapshot_manager = SnapshotManager(
    config_manager.store,
    connector_factory=connector_for,
    destination_factory=destination_for,
    mapping_lookup=config_manager.get_mapping,
    position_store=config_manager,
    audit=audit,
    duckdb=_duckdb(),
    max_parallel_jobs=int(os.environ.get("CDCSYNC_MAX_PARALLEL_SNAPSHOTS", "2")),
)


# ---------------------------------------------------------------------------
# alerting
# ---------------------------------------------------------------------------
def load_rules() -> List[Rule]:
    rules = {r.id: r for r in BUILTIN_RULES}
    for doc in config_manager.store.get_all(ALERT_RULES):
        try:
            rules[doc.key] = Rule.from_json(doc.body)
        except Exception as err:
            logger.error("invalid stored rule %s: %s", doc.key, err)
    return list(rules.values())


def build_channels() -> List[AlertChannel]:
    channels: List[AlertChannel] = [LogChannel(), AuditChannel(audit), BusChannel(bus)]
    webhook = config_manager.get_setting("alerting.webhook_url") or os.environ.get("CDCSYNC_ALERT_WEBHOOK")
    if webhook:
        channels.append(WebhookChannel(webhook))
    email = config_manager.get_setting("alerting.email")
    if email and email.get("host") and email.get("recipients"):
        channels.append(EmailChannel(email["host"], int(email.get("port", 25)), email.get("sender", "cdcsync@localhost"),
                                     email["recipients"], email.get("username"), email.get("password"), email.get("use_tls", True)))
    return channels


alert_manager = AlertManager(
    rules=load_rules(),
    channels=build_channels(),
    state_store=config_manager.store,
    enabled_rule_sets=config_manager.get_setting("alerting.enabled_rule_sets"),
)


def reload_alerting() -> None:
    alert_manager.rules = {r.id: r for r in load_rules()}
    alert_manager.channels = build_channels()
    sets = config_manager.get_setting("alerting.enabled_rule_sets")
    alert_manager.enabled_rule_sets = set(sets) if sets is not None else None


def failure_facts() -> Dict[str, Any]:
    """``failures.*`` facts for the rule engine, derived from the audit store."""
    stats_1h = audit.stats(hours=1)
    failures_5m = len(audit.failures(resolved=None, hours=5 / 60, limit=10_000))
    groups_1h = audit.failure_groups(hours=1, include_resolved=True)
    groups_24h = audit.failure_groups(hours=24, include_resolved=True)
    known_before = {g["signature"] for g in groups_24h} - {g["signature"] for g in groups_1h}
    recent_new = [g for g in groups_1h if g["signature"] not in known_before and g["first_seen"] >= _iso_hours_ago(1)]
    patterns = mine_patterns(groups_24h)
    unresolved = len(audit.failures(resolved=False, limit=100_000))
    total_1h = stats_1h["failed"]
    applied_1h = stats_1h["applied"]
    return {
        "total_1h": total_1h,
        "rate_1h": (total_1h / (total_1h + applied_1h)) if (total_1h + applied_1h) else 0.0,
        "total_5m": failures_5m,
        "new_signatures_1h": len(recent_new),
        "systemic_patterns": sum(1 for p in patterns if p.systemic),
        "unresolved": unresolved,
    }


def _iso_hours_ago(hours: float) -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def health_contexts() -> Dict[str, Dict[str, Any]]:
    try:
        failures = failure_facts()
    except Exception as err:  # pragma: no cover
        logger.error("failure facts unavailable: %s", err)
        failures = {}
    ctx: Dict[str, Dict[str, Any]] = {"global": {"failures": failures, "pipelines": len(pipelines.all()),
                                                 "bus": bus.stats()}}
    ctx.update(pipelines.health_contexts())
    ctx.update(snapshot_manager.health_contexts())
    return ctx


# ---------------------------------------------------------------------------
# pipelines
# ---------------------------------------------------------------------------
def build_pipeline(pipeline_id: str, source: ConnectionConfig | Dict[str, Any] | str, destination: ConnectionConfig | Dict[str, Any] | str,
                   mappings: List[Mapping], **options: Any) -> ChangePipeline:
    connector = connector_for(source) if isinstance(source, str) else connector_from_config(source)
    dest = destination_for(destination) if isinstance(destination, str) else destination_from_config(destination, name=pipeline_id + ":dest")
    p = ChangePipeline(
        pipeline_id, connector, dest, mappings, position_store=config_manager, audit=audit, bus=bus, duckdb=_duckdb(),
        batch_size=int(options.get("batch_size", 500)), batch_wait_s=float(options.get("batch_wait_s", 0.5)),
        initial_position=options.get("initial_position", "earliest"), insert_mode=options.get("insert_mode", "upsert"),
        expect_traffic=bool(options.get("expect_traffic", False)),
    )
    return p


def save_pipeline_definition(pipeline_id: str, definition: Dict[str, Any]) -> None:
    config_manager.store.put(PIPELINES, pipeline_id, definition)


def pipeline_definitions() -> Dict[str, Dict[str, Any]]:
    return config_manager.store.get_all_bodies(PIPELINES)


def start_pipeline_from_definition(pipeline_id: str) -> ChangePipeline:
    definition = pipeline_definitions().get(pipeline_id)
    if definition is None:
        raise KeyError(f"pipeline {pipeline_id} not defined")
    mappings = [m for m in (config_manager.get_mapping(mid) for mid in definition.get("mapping_ids", [])) if m]
    if definition.get("workset_id"):
        mappings += config_manager.get_mappings_for_workset(definition["workset_id"])
    existing = pipelines.get(pipeline_id)
    if existing and existing.state not in ("stopped", "error"):
        return existing
    p = build_pipeline(pipeline_id, definition["source_connection_id"], definition["destination_connection_id"], mappings,
                       **definition.get("options", {}))
    p.add_listener(_legacy_history_listener)
    pipelines.add(p)
    p.start()
    return p


def _legacy_history_listener(event, status, error):
    """Feed the legacy history/WebSocket consumers (monitoring page)."""
    try:
        from backend.core.history_manager import history_manager
        from backend.models.schemas import CDCEvent, CDCOperation
        op = CDCOperation(event.operation.value) if event.operation.value in CDCOperation.__members__ else CDCOperation.INSERT
        legacy = CDCEvent(id=event.event_id or "", timestamp=event.received_at, source_table=event.table_key, operation=op,
                          lsn=str(event.position.value), seqval=str(event.position.extra.get("seqval", "")) if event.position.extra else "",
                          data=event.data, status=status, error=error, source_change_time=event.source_ts)
        history_manager.log_event(legacy, status, error)
        bus.publish("events.legacy", {"event": legacy, "status": status, "error": error}, block=False)
    except Exception as err:  # pragma: no cover
        logger.debug("legacy listener failed: %s", err)


# ---------------------------------------------------------------------------
# background / lifecycle
# ---------------------------------------------------------------------------
_started = False


def start_background() -> None:
    global _started
    if _started or os.environ.get("CDCSYNC_DISABLE_BACKGROUND") == "1":
        return
    _started = True
    from backend.core.metrics_history import history
    history.start()
    alert_manager.start(health_contexts, interval_s=float(os.environ.get("CDCSYNC_ALERT_INTERVAL", "30")))
    for pid, definition in pipeline_definitions().items():
        if definition.get("auto_start"):
            try:
                start_pipeline_from_definition(pid)
            except Exception as err:
                logger.error("auto-start of pipeline %s failed: %s", pid, err)
    logger.info("background services started")


def shutdown() -> None:
    from backend.core.metrics_history import history
    history.stop()
    pipelines.stop_all()
    snapshot_manager.stop_all()
    alert_manager.stop()
    audit.close()
    bus.close()
