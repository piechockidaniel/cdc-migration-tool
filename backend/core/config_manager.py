"""Configuration management on top of the transactional ``ConfigStore``.

The public API is intentionally identical to the original JSON-file version so
the API layer and the UI keep working, but:

* every write is transactional / crash-safe (see ``backend/storage``),
* documents are versioned (``ConflictError`` on stale updates),
* CDC positions are generic (LSN, SCN, binlog coordinates, resume tokens...),
* the legacy ``config/*.json`` files are migrated automatically on first start.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from backend.models.schemas import Mapping, WorkingSet
from backend.storage import base as collections
from backend.storage.base import ConfigStore, ConflictError, Document
from backend.storage.factory import open_store

if TYPE_CHECKING:  # pragma: no cover
    from backend.models.schemas import ConnectionConfig

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(os.environ.get("CDCSYNC_CONFIG_DIR", "config"))


def connection_config_from_document(doc: Dict[str, Any]) -> "ConnectionConfig":
    """Build a ``ConnectionConfig`` from a library document (secrets resolved, ``name`` defaults to ``id``)."""
    from backend.models.schemas import ConnectionConfig
    from backend.storage.secrets import resolve_secrets
    data = {k: v for k, v in resolve_secrets(doc).items() if k in ConnectionConfig.model_fields}
    data.setdefault("name", data.get("id") or "connection")
    return ConnectionConfig(**data)


class ConfigManager:
    """Facade over the store with typed accessors for the domain objects."""

    def __init__(self, store: Optional[ConfigStore] = None, config_dir: Path = CONFIG_DIR):
        self.config_dir = Path(config_dir)
        self.store: ConfigStore = store or open_store(self.config_dir)
        self._migrate_legacy_workset_lists()

    # ------------------------------------------------------------------
    # Working sets
    # ------------------------------------------------------------------
    def _migrate_legacy_workset_lists(self) -> None:
        """Merge deprecated ``table_mappings``/``sql_mappings`` into ``mappings``."""
        for ws in self.get_all_worksets():
            if not ws.mappings and (ws.table_mappings or ws.sql_mappings):
                ws.mappings = list(dict.fromkeys(ws.table_mappings + ws.sql_mappings))
                self.store.put(collections.WORKSETS, ws.id, ws.model_dump(mode="json"), updated_by="migration")

    def create_workset(self, workset: WorkingSet) -> bool:
        try:
            self.store.put(collections.WORKSETS, workset.id, workset.model_dump(mode="json"), expected_version=0)
            logger.info("Created working set: %s", workset.name)
            return True
        except ConflictError:
            logger.warning("Working set %s already exists", workset.id)
            return False

    def update_workset(self, workset: WorkingSet) -> bool:
        if self.store.get(collections.WORKSETS, workset.id) is None:
            logger.warning("Working set %s does not exist", workset.id)
            return False
        workset.updated_at = datetime.now()
        self.store.put(collections.WORKSETS, workset.id, workset.model_dump(mode="json"))
        return True

    def delete_workset(self, workset_id: str) -> bool:
        return self.store.delete(collections.WORKSETS, workset_id)

    def get_workset(self, workset_id: str) -> Optional[WorkingSet]:
        body = self.store.get_body(collections.WORKSETS, workset_id)
        return WorkingSet(**body) if body else None

    def get_all_worksets(self) -> List[WorkingSet]:
        out = []
        for doc in self.store.get_all(collections.WORKSETS):
            try:
                out.append(WorkingSet(**doc.body))
            except Exception as exc:
                logger.error("Skipping invalid working set %s: %s", doc.key, exc)
        return out

    def get_active_workset(self) -> Optional[WorkingSet]:
        for ws in self.get_all_worksets():
            if ws.is_active:
                return self.resolve_workset(ws)
        return None

    def resolve_workset(self, ws: WorkingSet) -> WorkingSet:
        """Fill ``source_connection``/``destination_connection`` from the library when only ids are given."""
        for side in ("source", "destination"):
            cid = getattr(ws, f"{side}_connection_id")
            if cid:
                doc = self.get_connection(cid)
                if doc is None:
                    raise ValueError(f"working set {ws.id}: connection {cid!r} not found in the library")
                setattr(ws, f"{side}_connection", connection_config_from_document(doc))
            elif getattr(ws, f"{side}_connection") is None:
                raise ValueError(f"working set {ws.id}: {side}_connection or {side}_connection_id is required")
        return ws

    def set_active_workset(self, workset_id: str) -> bool:
        worksets = self.get_all_worksets()
        if not any(ws.id == workset_id for ws in worksets):
            return False
        docs = []
        for ws in worksets:
            desired = ws.id == workset_id
            if ws.is_active != desired:
                ws.is_active = desired
                docs.append(Document(collections.WORKSETS, ws.id, ws.model_dump(mode="json")))
        if docs:
            self.store.put_many(docs)  # atomic: never two active worksets
        logger.info("Activated working set: %s", workset_id)
        return True

    # ------------------------------------------------------------------
    # Mappings
    # ------------------------------------------------------------------
    def create_mapping(self, mapping: Mapping) -> bool:
        try:
            mapping.updated_at = datetime.now()
            self.store.put(collections.MAPPINGS, mapping.id, mapping.model_dump(mode="json"), expected_version=0)
            logger.info("Created mapping: %s (type: %s)", mapping.id, mapping.mapping_type.value)
            return True
        except ConflictError:
            logger.warning("Mapping %s already exists", mapping.id)
            return False

    def update_mapping(self, mapping: Mapping, expected_version: Optional[int] = None) -> bool:
        if self.store.get(collections.MAPPINGS, mapping.id) is None:
            logger.warning("Mapping %s does not exist", mapping.id)
            return False
        mapping.updated_at = datetime.now()
        self.store.put(collections.MAPPINGS, mapping.id, mapping.model_dump(mode="json"), expected_version=expected_version)
        return True

    def delete_mapping(self, mapping_id: str) -> bool:
        return self.store.delete(collections.MAPPINGS, mapping_id)

    def get_mapping(self, mapping_id: str) -> Optional[Mapping]:
        body = self.store.get_body(collections.MAPPINGS, mapping_id)
        return Mapping(**body) if body else None

    def get_mapping_version(self, mapping_id: str) -> Optional[int]:
        doc = self.store.get(collections.MAPPINGS, mapping_id)
        return doc.version if doc else None

    def get_mapping_history(self, mapping_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        return [
            {"version": d.version, "updated_at": d.updated_at, "updated_by": d.updated_by, "body": d.body}
            for d in self.store.history(collections.MAPPINGS, mapping_id, limit)
        ]

    def get_all_mappings(self) -> List[Mapping]:
        out = []
        for doc in self.store.get_all(collections.MAPPINGS):
            try:
                out.append(Mapping(**doc.body))
            except Exception as exc:
                logger.error("Skipping invalid mapping %s: %s", doc.key, exc)
        return out

    def get_mappings_for_workset(self, workset_id: str) -> List[Mapping]:
        workset = self.get_workset(workset_id)
        if not workset:
            return []
        ids = workset.mappings or (workset.table_mappings + workset.sql_mappings)
        result = []
        for mapping_id in ids:
            m = self.get_mapping(mapping_id)
            if m:
                result.append(m)
        return result

    # ------------------------------------------------------------------
    # CDC positions (generic: LSN / SCN / binlog / resume token)
    # ------------------------------------------------------------------
    def get_position(self, key: str) -> Optional[Dict[str, Any]]:
        return self.store.get_body(collections.POSITIONS, key)

    def set_position(self, key: str, kind: str, value: Any, extra: Optional[Dict[str, Any]] = None) -> None:
        body = {"kind": kind, "value": value, "committed_at": datetime.now().isoformat()}
        if extra:
            body.update(extra)
        self.store.put(collections.POSITIONS, key, body)

    def clear_position(self, key: Optional[str] = None) -> None:
        if key:
            self.store.delete(collections.POSITIONS, key)
        else:
            for doc in self.store.get_all(collections.POSITIONS):
                self.store.delete(collections.POSITIONS, doc.key)

    def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        return self.store.get_all_bodies(collections.POSITIONS)

    # Legacy LSN helpers (MSSQL hex string)
    def get_last_lsn(self, table_key: str) -> Optional[str]:
        body = self.get_position(table_key)
        return body.get("value") if body else None

    def set_last_lsn(self, table_key: str, lsn: str) -> None:
        self.set_position(table_key, "mssql_lsn", lsn)

    # ------------------------------------------------------------------
    # Connection library
    # ------------------------------------------------------------------
    def get_all_connections(self) -> List[dict]:
        return [d.body for d in self.store.get_all(collections.CONNECTIONS)]

    def get_connection(self, connection_id: str) -> Optional[dict]:
        return self.store.get_body(collections.CONNECTIONS, connection_id)

    def save_connection(self, connection_id: str, connection_data: dict) -> tuple[bool, str]:
        try:
            self.store.put(collections.CONNECTIONS, connection_id, connection_data)
            return True, f"Connection '{connection_id}' saved successfully"
        except Exception as exc:
            logger.error("Error saving connection: %s", exc)
            return False, str(exc)

    def update_connection(self, connection_id: str, connection_data: dict) -> tuple[bool, str]:
        if self.store.get(collections.CONNECTIONS, connection_id) is None:
            return False, f"Connection '{connection_id}' not found"
        return self.save_connection(connection_id, connection_data)

    def delete_connection(self, connection_id: str) -> tuple[bool, str]:
        if self.store.delete(collections.CONNECTIONS, connection_id):
            return True, f"Connection '{connection_id}' deleted successfully"
        return False, f"Connection '{connection_id}' not found"

    # ------------------------------------------------------------------
    # Generic settings
    # ------------------------------------------------------------------
    def get_setting(self, key: str, default: Any = None) -> Any:
        body = self.store.get_body(collections.SETTINGS, key)
        return body.get("value", default) if body else default

    def set_setting(self, key: str, value: Any) -> None:
        self.store.put(collections.SETTINGS, key, {"value": value})

# Global configuration manager instance
config_manager = ConfigManager()
