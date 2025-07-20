"""
State Management Module

Provides SQLite-based state persistence for RedditDL sessions,
replacing the previous JSON-based state management.
"""

from .manager import StateManager
from .migrations import migrate_json_to_sqlite

__all__ = ["StateManager", "migrate_json_to_sqlite"]