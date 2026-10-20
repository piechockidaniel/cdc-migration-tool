"""Destination (sink) abstraction."""
from backend.destinations.sql_destination import Dialect, SqlDestination, DIALECTS, create_destination

__all__ = ["Dialect", "SqlDestination", "DIALECTS", "create_destination"]
