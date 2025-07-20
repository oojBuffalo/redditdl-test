"""
RedditDL Plugin System

This module provides the plugin architecture for RedditDL, enabling third-party
extensions through a pluggy-based system. The plugin system supports:

- Content handlers for processing different media types
- Filters for post filtering and selection
- Exporters for data export in various formats  
- Scrapers for additional data sources

Key Components:
- PluginManager: Central plugin management and lifecycle
- Hook specifications: Interfaces for plugin types
- Plugin discovery: Automatic loading and validation
- Sandboxing: Safe execution environment
"""

from .manager import PluginManager
from .hooks import (
    ContentHandlerHooks,
    FilterHooks, 
    ExporterHooks,
    ScraperHooks
)

__all__ = [
    'PluginManager',
    'ContentHandlerHooks',
    'FilterHooks',
    'ExporterHooks', 
    'ScraperHooks'
]