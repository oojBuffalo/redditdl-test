"""
CLI Observers Module for RedditDL

This module provides CLI-specific observer implementations that extend
the base observer system with enhanced user interface capabilities.
"""

from .progress import CLIProgressObserver

__all__ = ['CLIProgressObserver']