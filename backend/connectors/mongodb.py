"""MongoDB connector - change streams (native push, resume tokens as positions).

* ``schema`` = database name, ``table`` = collection name.
* Positions are resume tokens (``{"_data": ...}``) which survive restarts as
  long as the oplog still contains them - the prerequisite check reports the
  oplog window so operators can size it against the expected outage.
* Before images require ``changeStreamPreAndPostImages`` (MongoDB 6.0+);
  ``enable_capture_table`` switches it on with ``collMod``.
* Snapshots use keyset pagination on ``_id`` (any BSON-comparable type).
"""
from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from backend.connectors.base import (
    Capability, ChangeEvent, ColumnRef, ConnectorError, Operation, Position,
    PrerequisiteCheck, SourceConnector, TableRef,
)
from backend.core import metrics

logger = logging.getLogger(__name__)

POSITION_KIND = "mongo_token"
_OP_MAP = {"insert": Operation.INSERT, "update": Operation.UPDATE, "replace": Operation.UPDATE, "delete": Operation.DELETE}


try:  # bson ships with pymongo; keep the connector importable without it
    from bson import Decimal128 as _Decimal128, ObjectId as _ObjectId, Timestamp as _Timestamp
except ImportError:  # pragma: no cover
    _Decimal128 = _ObjectId = _Timestamp = None  # type: ignore[assignment,misc]


def _plain(value: Any) -> Any:
    """Convert BSON specific types into JSON friendly Python values."""
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if _ObjectId is not None and isinstance(value, _ObjectId):
        return str(value)
    if _Decimal128 is not None and isinstance(value, _Decimal128):
        return str(value.to_decimal())
    if _Timestamp is not None and isinstance(value, _Timestamp):
        return {"t": value.time, "i": value.inc}
    return value


class MongoConnector(SourceConnector):
    engine = "mongodb"
    capabilities = frozenset({
        Capability.NATIVE_CDC, Capability.PUSH_EVENTS, Capability.KEYSET_SNAPSHOT, Capability.BEFORE_IMAGE,
    })

    def __init__(self, config: Dict[str, Any], client_factory=None):
        super().__init__(config)
        self._client = None
        self._client_factory = client_factory
        self.database: str = config.get("database", "")
        self.full_document = config.get("full_document", "updateLookup")
        self.full_document_before_change = config.get("full_document_before_change", "whenAvailable")
        self.max_await_ms = int(config.get("max_await_ms", 1000))

    # ------------------------------------------------------------------
    def uri(self, redact: bool = False) -> str:
        c = self.config
        if c.get("uri"):
            return "mongodb://***" if redact else c["uri"]
        auth = ""
        if c.get("username"):
            auth = f"{c['username']}:{'***' if redact else c.get('password', '')}@"
        host = c.get("server", c.get("host", "localhost"))
        port = c.get("port") or 27017
        opts = c.get("options", "")
        return f"mongodb://{auth}{host}:{port}/{c.get('auth_database', 'admin')}{('?' + opts) if opts else ''}"

    def connect(self) -> None:
        with self._lock:
            if self._client is not None:
                return
            try:
                if self._client_factory is not None:
                    self._client = self._client_factory(self)
                else:
                    from pymongo import MongoClient
                    self._client = MongoClient(self.uri(), serverSelectionTimeoutMS=int(self.config.get("login_timeout", 15)) * 1000,
                                               appname=self.config.get("application_name", "cdcsync"))
                    self._client.admin.command("ping")
            except Exception as exc:
                metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                raise ConnectorError(f"MongoDB connect failed ({self.uri(redact=True)}): {exc}") from exc

    def disconnect(self) -> None:
        with self._lock:
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:  # pragma: no cover
                    pass
                self._client = None

    def is_connected(self) -> bool:
        with self._lock:
            if self._client is None:
                return False
            try:
                with metrics.timed_query(self.name, "ping"):
                    self._client.admin.command("ping")
                return True
            except Exception:
                return False

    def _db(self, name: Optional[str] = None):
        if self._client is None:
            self.connect()
        return self._client[name or self.database]

    def server_version(self) -> str:
        with metrics.timed_query(self.name, "meta"):
            info = self._client.server_info() if self._client else self._db().client.server_info()
        return str(info.get("version", "unknown"))

    def quote(self, identifier: str) -> str:
        return identifier

    # ------------------------------------------------------------------
    def list_tables(self) -> List[TableRef]:
        db = self._db()
        out = []
        with metrics.timed_query(self.name, "meta"):
            for info in db.list_collections():
                if info.get("type") == "view" or info["name"].startswith("system."):
                    continue
                try:
                    stats = db.command("collStats", info["name"])
                    count, size = int(stats.get("count", 0)), int(stats.get("storageSize", 0))
                except Exception:
                    count, size = None, None
                pre = bool((info.get("options") or {}).get("changeStreamPreAndPostImages", {}).get("enabled"))
                out.append(TableRef(db.name, info["name"], True, count, size, {"pre_post_images": pre}))
        return sorted(out, key=lambda t: t.table)

    def get_columns(self, schema: str, table: str) -> List[ColumnRef]:
        """Sample documents to infer fields (schemaless source)."""
        coll = self._db(schema)[table]
        fields: Dict[str, str] = {}
        with metrics.timed_query(self.name, "meta"):
            for doc in coll.find({}, limit=int(self.config.get("schema_sample", 100))):
                for k, v in doc.items():
                    fields.setdefault(k, type(v).__name__)
        cols = [ColumnRef("_id", fields.get("_id", "ObjectId"), False, True, 0)]
        cols += [ColumnRef(k, t, True, False, i + 1) for i, (k, t) in enumerate(sorted(fields.items())) if k != "_id"]
        return cols

    def get_primary_key(self, schema: str, table: str) -> List[str]:
        return ["_id"]

    # ------------------------------------------------------------------
    def check_prerequisites(self, tables=None) -> List[PrerequisiteCheck]:
        checks: List[PrerequisiteCheck] = []

        def add(name, passed, detail="", fix="", severity="error"):
            checks.append(PrerequisiteCheck(name, bool(passed), detail, fix, severity))

        try:
            self.connect()
            version = self.server_version()
            add("connection", True, version)
        except Exception as exc:
            add("connection", False, str(exc))
            return checks
        try:
            hello = self._client.admin.command("hello")
            is_rs = bool(hello.get("setName")) or hello.get("msg") == "isdbgrid"
            add("replica_set_or_sharded", is_rs, f"setName={hello.get('setName')}, msg={hello.get('msg')}",
                "Change streams need a replica set (even single-node: rs.initiate()) or a sharded cluster")
            major = int(str(version).split(".")[0]) if version and version[0].isdigit() else 0
            add("server_version", major >= 4, version, "MongoDB 4.0+ required (6.0+ for before images)")
            add("pre_post_images_supported", major >= 6, version,
                "Before images (DELETE/UPDATE old values) need MongoDB 6.0+", "warning" if major < 6 else "info")
        except Exception as exc:
            add("topology", False, str(exc))
        try:
            self._client.admin.command("replSetGetStatus")  # raises on standalone
            oplog = self._client["local"]["oplog.rs"]
            newest = oplog.find_one(sort=[("$natural", -1)])
            oldest = oplog.find_one(sort=[("$natural", 1)])
            if newest and oldest:
                window_h = (newest["ts"].time - oldest["ts"].time) / 3600.0
                add("oplog_window", window_h >= 24, f"oplog window ~{window_h:.1f} h",
                    "Increase oplog size (replSetResizeOplog) so resume tokens survive the longest outage", "warning")
        except Exception as exc:
            add("oplog_window", True, f"cannot inspect oplog ({exc}); needs clusterMonitor role", severity="info")
        try:
            roles = self._client[self.config.get("auth_database", "admin")].command("connectionStatus")
            add("user_roles", True, str(roles.get("authInfo", {}).get("authenticatedUserRoles", []))[:300],
                "User needs read + changeStream on the database (e.g. role 'read')", "info")
        except Exception:
            pass
        for schema, table in tables or []:
            try:
                info = list(self._db(schema).list_collections(filter={"name": table}))
                if not info:
                    add(f"collection_exists:{schema}.{table}", False, "collection not found")
                    continue
                pre = bool((info[0].get("options") or {}).get("changeStreamPreAndPostImages", {}).get("enabled"))
                add(f"pre_post_images:{schema}.{table}", pre, f"changeStreamPreAndPostImages={pre}",
                    f"db.runCommand({{collMod: '{table}', changeStreamPreAndPostImages: {{enabled: true}}}})", "warning")
            except Exception as exc:
                add(f"collection:{schema}.{table}", False, str(exc))
        return checks

    def capture_status(self) -> Dict[str, Any]:
        try:
            hello = self._client.admin.command("hello") if self._client else {}
        except Exception:
            hello = {}
        return {"database": self.database, "cdc_enabled": bool(hello.get("setName")) or hello.get("msg") == "isdbgrid",
                "replica_set": hello.get("setName"), "primary": hello.get("primary")}

    def enable_capture_database(self) -> Tuple[bool, str]:
        status = self.capture_status()
        if status["cdc_enabled"]:
            return True, "change streams available (replica set / sharded cluster)"
        return False, "server is standalone: initiate a replica set (rs.initiate()) to enable change streams"

    def enable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        try:
            self._db(schema).command("collMod", table, changeStreamPreAndPostImages={"enabled": True})
            return True, f"pre/post images enabled on {schema}.{table}"
        except Exception as exc:
            return False, f"collMod failed (MongoDB 6.0+ and dbAdmin needed): {exc}"

    def disable_capture_table(self, schema, table, **options) -> Tuple[bool, str]:
        try:
            self._db(schema).command("collMod", table, changeStreamPreAndPostImages={"enabled": False})
            return True, f"pre/post images disabled on {schema}.{table}"
        except Exception as exc:
            return False, str(exc)

    # ------------------------------------------------------------------
    def current_position(self) -> Optional[Position]:
        try:
            hello = self._client.admin.command("hello")
            lw = hello.get("lastWrite", {})
            ts = lw.get("opTime", {}).get("ts")
            if ts is not None:
                return Position("mongo_optime", {"t": ts.time, "i": ts.inc})
        except Exception:
            pass
        return None

    @staticmethod
    def _pipeline(tables: Sequence[Tuple[str, str]]):
        match: Dict[str, Any] = {"operationType": {"$in": list(_OP_MAP.keys())}}
        colls = sorted({t for _, t in tables})
        if colls:
            match["ns.coll"] = {"$in": colls}
        return [{"$match": match}]

    def _open_stream(self, tables, from_position):
        kwargs: Dict[str, Any] = {"pipeline": self._pipeline(tables), "full_document": self.full_document,
                                  "max_await_time_ms": self.max_await_ms}
        if self.full_document_before_change:
            kwargs["full_document_before_change"] = self.full_document_before_change
        if from_position and from_position.value:
            kwargs["resume_after"] = from_position.value
        try:
            return self._db().watch(**kwargs)
        except Exception as exc:
            metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
            raise ConnectorError(f"change stream could not be opened ({self.uri(redact=True)}): {exc}") from exc

    def _to_event(self, change: Dict[str, Any]) -> Optional[ChangeEvent]:
        op = _OP_MAP.get(change.get("operationType"))
        if op is None:
            return None
        ns = change.get("ns", {})
        key = _plain(change.get("documentKey") or {})
        after = _plain(change.get("fullDocument")) if change.get("fullDocument") is not None else None
        before = _plain(change.get("fullDocumentBeforeChange")) if change.get("fullDocumentBeforeChange") is not None else None
        if op == Operation.UPDATE and after is None:
            after = None  # document vanished before lookup - applier treats as key-only update
        cluster_ts = change.get("clusterTime")
        ts = datetime.fromtimestamp(cluster_ts.time) if cluster_ts is not None and hasattr(cluster_ts, "time") else None
        ev = ChangeEvent(source=self.name, schema=ns.get("db", self.database), table=ns.get("coll", ""), operation=op,
                         position=Position(POSITION_KIND, _plain(change.get("_id"))), key=key, after=after, before=before,
                         source_ts=ts, tx_id=str(change.get("txnNumber")) if change.get("txnNumber") is not None else None,
                         event_id=str(uuid.uuid4()),
                         metadata={"update_description": _plain(change.get("updateDescription"))} if change.get("updateDescription") else {})
        metrics.EVENTS_READ.labels(source=self.name, table=ev.table_key, op=op.value).inc()
        return ev

    def read_changes(self, tables, from_position, to_position=None, limit=10_000):
        events: List[ChangeEvent] = []
        last = from_position
        with self._open_stream(tables, from_position) as stream:
            while len(events) < limit:
                change = stream.try_next()
                if change is None:
                    break
                ev = self._to_event(change)
                if ev:
                    events.append(ev)
                    last = ev.position
            token = stream.resume_token
            if token is not None:
                last = Position(POSITION_KIND, _plain(token))
        metrics.POLL_COUNT.labels(source=self.name, outcome="ok").inc()
        return events, last

    def stream_changes(self, tables, from_position, stop_event: threading.Event) -> Iterator[ChangeEvent]:
        with self._open_stream(tables, from_position) as stream:
            while not stop_event.is_set():
                try:
                    change = stream.try_next()  # blocks up to max_await_ms server-side (no client polling)
                except Exception as exc:
                    metrics.CONNECTION_ERRORS.labels(connection=self.name).inc()
                    raise ConnectorError(f"change stream failed: {exc}") from exc
                if change is None:
                    metrics.WATERMARK_CHECKS.labels(source=self.name, outcome="idle").inc()
                    continue
                ev = self._to_event(change)
                if ev:
                    yield ev

    def retention_status(self, positions: Dict[str, Optional[Position]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"position_before_min": False, "capture_errors": 0, "retention_margin_minutes": None}
        try:
            oplog = self._client["local"]["oplog.rs"]
            oldest = oplog.find_one(sort=[("$natural", 1)])
            newest = oplog.find_one(sort=[("$natural", -1)])
            if oldest and newest:
                out["retention_minutes"] = (newest["ts"].time - oldest["ts"].time) / 60.0
                pos = positions.get("__source__")
                token_ts = None
                if pos and isinstance(pos.value, dict) and pos.value.get("_data"):
                    # resume token: first 8 hex chars after the 0x82 prefix encode the timestamp
                    data = str(pos.value["_data"])
                    try:
                        token_ts = int(data[2:10], 16)
                    except ValueError:
                        token_ts = None
                if token_ts:
                    out["position_before_min"] = token_ts < oldest["ts"].time
                    out["retention_margin_minutes"] = round((token_ts - oldest["ts"].time) / 60.0, 1)
        except Exception:
            pass
        return out

    # ------------------------------------------------------------------
    def count_rows(self, schema, table, approximate=True):
        coll = self._db(schema)[table]
        with metrics.timed_query(self.name, "count"):
            return int(coll.estimated_document_count() if approximate else coll.count_documents({}))

    def read_snapshot_chunk(self, schema, table, columns, key_columns, after_key, chunk_size, where=None):
        coll = self._db(schema)[table]
        key = key_columns[0] if key_columns else "_id"
        flt: Dict[str, Any] = {}
        if after_key is not None:
            flt[key] = {"$gt": after_key[0]}
        if where:
            import json
            flt.update(json.loads(where))
        projection = {c: 1 for c in columns} if columns else None
        if projection is not None:
            projection.setdefault(key, 1)
        with metrics.timed_query(self.name, "snapshot_chunk"):
            docs = list(coll.find(flt, projection).sort(key, 1).limit(int(chunk_size)))
        return [_plain(d) for d in docs]

    def key_bounds(self, schema, table, key_columns):
        coll = self._db(schema)[table]
        key = key_columns[0] if key_columns else "_id"
        lo = coll.find_one({}, {key: 1}, sort=[(key, 1)])
        hi = coll.find_one({}, {key: 1}, sort=[(key, -1)])
        return (_plain(lo[key]),) if lo else None, (_plain(hi[key]),) if hi else None
