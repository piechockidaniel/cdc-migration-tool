"""Persistent storage for instance state (connections, mappings, positions, jobs).

See ``docs/developer-guide/03-storage.md`` for the reasoning behind the two backends.
"""
from backend.storage.base import ConfigStore, ConflictError, Document
from backend.storage.json_store import JsonFileConfigStore
from backend.storage.sqlite_store import SqliteConfigStore
from backend.storage.factory import open_store

__all__ = [
    "ConfigStore",
    "ConflictError",
    "Document",
    "JsonFileConfigStore",
    "SqliteConfigStore",
    "open_store",
]
