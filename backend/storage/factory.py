"""Backend selection + one-time migration from the legacy ``config/*.json`` layout."""
from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from backend.storage import base
from backend.storage.base import ConfigStore, Document
from backend.storage.json_store import JsonFileConfigStore
from backend.storage.sqlite_store import SqliteConfigStore

logger = logging.getLogger(__name__)

LEGACY_FILES = {
    "connections.json": base.CONNECTIONS,
    "mappings.json": base.MAPPINGS,
    "worksets.json": base.WORKSETS,
}
LEGACY_LSN_FILE = "lsn_state.json"


def migrate_legacy_json(config_dir: Path, store: ConfigStore) -> int:
    """Import the original flat JSON files into ``store`` (idempotent).

    Files are renamed to ``*.migrated-<timestamp>`` afterwards so the import
    never runs twice and the originals are kept as a backup.
    """
    imported = 0
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    docs = []
    for filename, collection in LEGACY_FILES.items():
        path = config_dir / filename
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8").strip()
            data = json.loads(content) if content else {}
        except Exception as exc:  # corrupted file - leave it for the operator
            logger.error("Cannot parse legacy %s: %s", path, exc)
            continue
        for key, body in data.items():
            if store.get(collection, key) is None:
                docs.append(Document(collection, key, body, updated_by="legacy-migration"))
                imported += 1
        shutil.move(str(path), str(path.with_suffix(f".json.migrated-{stamp}")))
    lsn_path = config_dir / LEGACY_LSN_FILE
    if lsn_path.exists():
        try:
            data = json.loads(lsn_path.read_text(encoding="utf-8") or "{}")
            for table_key, lsn_hex in data.items():
                if store.get(base.POSITIONS, table_key) is None:
                    docs.append(Document(base.POSITIONS, table_key,
                                         {"kind": "mssql_lsn", "value": lsn_hex}, updated_by="legacy-migration"))
                    imported += 1
            shutil.move(str(lsn_path), str(lsn_path.with_suffix(f".json.migrated-{stamp}")))
        except Exception as exc:
            logger.error("Cannot migrate %s: %s", lsn_path, exc)
    if docs:
        store.put_many(docs)
        logger.info("Migrated %d legacy configuration documents into %s", imported, store.backend_name)
    return imported


def open_store(config_dir: str | Path = "config", backend: Optional[str] = None, migrate: bool = True) -> ConfigStore:
    """Open the configured store.

    Backend is selected by argument, then ``CDCSYNC_CONFIG_BACKEND`` env var,
    default ``sqlite``.
    """
    config_dir = Path(config_dir)
    config_dir.mkdir(parents=True, exist_ok=True)
    backend = (backend or os.environ.get("CDCSYNC_CONFIG_BACKEND") or "sqlite").lower()
    if backend == "sqlite":
        store: ConfigStore = SqliteConfigStore(config_dir / "state.sqlite3")
    elif backend == "json":
        store = JsonFileConfigStore(config_dir / "store")
    else:
        raise ValueError(f"unknown config backend {backend!r} (use 'sqlite' or 'json')")
    if migrate:
        migrate_legacy_json(config_dir, store)
    return store
