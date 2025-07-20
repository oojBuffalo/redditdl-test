"""
Core Cache Module

Provides intelligent caching capabilities for RedditDL including:
- Multi-level caching (memory, disk)
- TTL-based expiration
- LRU eviction policies
- Cache warming and preloading
"""

from .manager import CacheManager

__all__ = [
    'CacheManager'
]