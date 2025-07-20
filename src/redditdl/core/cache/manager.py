"""
Cache Manager

Intelligent caching system with multiple cache levels, automatic eviction,
and performance optimization for frequently accessed data.
"""

import asyncio
import hashlib
import json
import logging
import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar
from dataclasses import dataclass, field
import threading
import tempfile
from cachetools import TTLCache, LRUCache

from redditdl.core.monitoring.metrics import get_metrics_collector, time_operation


T = TypeVar('T')
logger = logging.getLogger(__name__)


@dataclass
class CacheConfig:
    """Configuration for cache management."""
    memory_cache_size: int = 1000
    disk_cache_size_mb: int = 100
    default_ttl: float = 3600.0  # 1 hour
    enable_disk_cache: bool = True
    cache_dir: Optional[Path] = None
    compression_enabled: bool = True
    metrics_enabled: bool = True


@dataclass
class CacheStats:
    """Cache performance statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    disk_reads: int = 0
    disk_writes: int = 0
    total_size_mb: float = 0.0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class CacheEntry:
    """Container for cached data with metadata."""
    
    def __init__(self, data: Any, ttl: Optional[float] = None):
        """
        Initialize cache entry.
        
        Args:
            data: Data to cache
            ttl: Time-to-live in seconds
        """
        self.data = data
        self.created_at = time.time()
        self.expires_at = self.created_at + ttl if ttl else None
        self.access_count = 0
        self.last_accessed = self.created_at
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def touch(self) -> None:
        """Update access statistics."""
        self.access_count += 1
        self.last_accessed = time.time()
    
    def get_data(self) -> Any:
        """Get cached data and update access stats."""
        if self.is_expired():
            raise ValueError("Cache entry has expired")
        
        self.touch()
        return self.data


class CacheManager:
    """
    Multi-level cache manager with intelligent eviction and performance optimization.
    
    Features:
    - Memory cache with LRU eviction
    - Persistent disk cache
    - TTL-based expiration
    - Automatic cache warming
    - Performance metrics
    - Thread-safe operations
    """
    
    def __init__(self, config: Optional[CacheConfig] = None):
        """
        Initialize cache manager.
        
        Args:
            config: Cache configuration
        """
        self.config = config or CacheConfig()
        self.stats = CacheStats()
        
        # Initialize memory cache
        self._memory_cache: TTLCache = TTLCache(
            maxsize=self.config.memory_cache_size,
            ttl=self.config.default_ttl
        )
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Disk cache setup
        self._disk_cache_dir: Optional[Path] = None
        if self.config.enable_disk_cache:
            self._setup_disk_cache()
        
        # Metrics
        self._metrics_collector = get_metrics_collector() if self.config.metrics_enabled else None
        if self._metrics_collector:
            self._setup_metrics()
        
        # Cache warming task
        self._warm_task: Optional[asyncio.Task] = None
        self._warming_enabled = False
    
    def _setup_disk_cache(self) -> None:
        """Setup disk cache directory."""
        if self.config.cache_dir:
            self._disk_cache_dir = self.config.cache_dir
        else:
            self._disk_cache_dir = Path(tempfile.gettempdir()) / "redditdl_cache"
        
        self._disk_cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Disk cache directory: {self._disk_cache_dir}")
    
    def _setup_metrics(self) -> None:
        """Setup cache metrics."""
        if self._metrics_collector:
            self._metrics_collector.counter("cache.hits", "Cache hits")
            self._metrics_collector.counter("cache.misses", "Cache misses") 
            self._metrics_collector.counter("cache.evictions", "Cache evictions")
            self._metrics_collector.gauge("cache.size", "Cache size")
            self._metrics_collector.timer("cache.operation_time", "Cache operation time")
    
    def _get_cache_key(self, key: str) -> str:
        """Generate consistent cache key."""
        # Hash key to ensure valid filename and consistent length
        return hashlib.sha256(key.encode()).hexdigest()
    
    def _get_disk_path(self, cache_key: str) -> Path:
        """Get disk cache file path."""
        if not self._disk_cache_dir:
            raise ValueError("Disk cache not enabled")
        
        # Organize files in subdirectories based on first 2 chars of hash
        subdir = cache_key[:2]
        cache_subdir = self._disk_cache_dir / subdir
        cache_subdir.mkdir(exist_ok=True)
        
        return cache_subdir / f"{cache_key}.cache"
    
    def get(self, key: str, default: Optional[T] = None) -> Optional[T]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            default: Default value if not found
            
        Returns:
            Cached value or default
        """
        with self._lock:
            cache_key = self._get_cache_key(key)
            
            # Check memory cache first
            try:
                entry = self._memory_cache.get(cache_key)
                if entry and not entry.is_expired():
                    data = entry.get_data()
                    self.stats.hits += 1
                    
                    if self._metrics_collector:
                        self._metrics_collector.increment("cache.hits")
                    
                    return data
            except (KeyError, ValueError):
                pass
            
            # Check disk cache
            if self.config.enable_disk_cache:
                disk_data = self._get_from_disk(cache_key)
                if disk_data is not None:
                    # Promote to memory cache
                    self._memory_cache[cache_key] = CacheEntry(disk_data)
                    self.stats.hits += 1
                    
                    if self._metrics_collector:
                        self._metrics_collector.increment("cache.hits")
                    
                    return disk_data
            
            # Cache miss
            self.stats.misses += 1
            if self._metrics_collector:
                self._metrics_collector.increment("cache.misses")
            
            return default
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
        """
        with self._lock:
            cache_key = self._get_cache_key(key)
            entry_ttl = ttl or self.config.default_ttl
            
            # Create cache entry
            entry = CacheEntry(value, entry_ttl)
            
            # Store in memory cache
            self._memory_cache[cache_key] = entry
            
            # Store in disk cache if enabled
            if self.config.enable_disk_cache:
                self._set_to_disk(cache_key, value, entry_ttl)
            
            # Update metrics
            if self._metrics_collector:
                self._metrics_collector.set_gauge("cache.size", len(self._memory_cache))
    
    def delete(self, key: str) -> bool:
        """
        Delete value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key was found and deleted
        """
        with self._lock:
            cache_key = self._get_cache_key(key)
            found = False
            
            # Remove from memory cache
            if cache_key in self._memory_cache:
                del self._memory_cache[cache_key]
                found = True
            
            # Remove from disk cache
            if self.config.enable_disk_cache:
                disk_path = self._get_disk_path(cache_key)
                if disk_path.exists():
                    disk_path.unlink()
                    found = True
            
            return found
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            # Clear memory cache
            self._memory_cache.clear()
            
            # Clear disk cache
            if self.config.enable_disk_cache and self._disk_cache_dir:
                import shutil
                if self._disk_cache_dir.exists():
                    shutil.rmtree(self._disk_cache_dir)
                    self._disk_cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Reset stats
            self.stats = CacheStats()
            
            logger.info("Cache cleared")
    
    def _get_from_disk(self, cache_key: str) -> Optional[Any]:
        """Get value from disk cache."""
        if not self._disk_cache_dir:
            return None
        
        try:
            disk_path = self._get_disk_path(cache_key)
            if not disk_path.exists():
                return None
            
            with time_operation("cache.disk_read"):
                with open(disk_path, 'rb') as f:
                    cache_data = pickle.load(f)
                
                # Check expiration
                if cache_data.get('expires_at') and time.time() > cache_data['expires_at']:
                    disk_path.unlink()
                    return None
                
                self.stats.disk_reads += 1
                return cache_data['data']
        
        except Exception as e:
            logger.warning(f"Failed to read from disk cache: {e}")
            return None
    
    def _set_to_disk(self, cache_key: str, value: Any, ttl: float) -> None:
        """Set value to disk cache."""
        if not self._disk_cache_dir:
            return
        
        try:
            disk_path = self._get_disk_path(cache_key)
            
            cache_data = {
                'data': value,
                'created_at': time.time(),
                'expires_at': time.time() + ttl,
                'ttl': ttl
            }
            
            with time_operation("cache.disk_write"):
                with open(disk_path, 'wb') as f:
                    pickle.dump(cache_data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            self.stats.disk_writes += 1
        
        except Exception as e:
            logger.warning(f"Failed to write to disk cache: {e}")
    
    def cached(self, key: Optional[str] = None, ttl: Optional[float] = None):
        """
        Decorator for caching function results.
        
        Args:
            key: Custom cache key (uses function name and args if None)
            ttl: Time-to-live for cached result
            
        Returns:
            Decorated function
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            def wrapper(*args, **kwargs) -> T:
                # Generate cache key
                if key:
                    cache_key = key
                else:
                    # Create key from function name and arguments
                    func_name = f"{func.__module__}.{func.__name__}"
                    args_key = str(hash((args, tuple(sorted(kwargs.items())))))
                    cache_key = f"{func_name}:{args_key}"
                
                # Try to get from cache
                cached_result = self.get(cache_key)
                if cached_result is not None:
                    return cached_result
                
                # Execute function and cache result
                result = func(*args, **kwargs)
                self.set(cache_key, result, ttl)
                
                return result
            
            return wrapper
        return decorator
    
    async def cached_async(self, key: Optional[str] = None, ttl: Optional[float] = None):
        """
        Decorator for caching async function results.
        
        Args:
            key: Custom cache key (uses function name and args if None)
            ttl: Time-to-live for cached result
            
        Returns:
            Decorated async function
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            async def wrapper(*args, **kwargs) -> T:
                # Generate cache key
                if key:
                    cache_key = key
                else:
                    func_name = f"{func.__module__}.{func.__name__}"
                    args_key = str(hash((args, tuple(sorted(kwargs.items())))))
                    cache_key = f"{func_name}:{args_key}"
                
                # Try to get from cache
                cached_result = self.get(cache_key)
                if cached_result is not None:
                    return cached_result
                
                # Execute function and cache result
                result = await func(*args, **kwargs)
                self.set(cache_key, result, ttl)
                
                return result
            
            return wrapper
        return decorator
    
    async def warm_cache(self, warm_functions: List[Callable] = None) -> None:
        """
        Warm cache with frequently accessed data.
        
        Args:
            warm_functions: List of functions to call for cache warming
        """
        if not warm_functions:
            return
        
        self._warming_enabled = True
        logger.info("Starting cache warming...")
        
        try:
            for func in warm_functions:
                if not self._warming_enabled:
                    break
                
                try:
                    if asyncio.iscoroutinefunction(func):
                        await func()
                    else:
                        func()
                    
                    # Small delay to prevent overwhelming
                    await asyncio.sleep(0.1)
                
                except Exception as e:
                    logger.warning(f"Cache warming function failed: {e}")
        
        except Exception as e:
            logger.error(f"Cache warming error: {e}")
        
        logger.info("Cache warming completed")
    
    def stop_warming(self) -> None:
        """Stop cache warming process."""
        self._warming_enabled = False
        
        if self._warm_task:
            self._warm_task.cancel()
    
    def cleanup_expired(self) -> int:
        """
        Clean up expired cache entries.
        
        Returns:
            Number of entries cleaned up
        """
        with self._lock:
            cleaned_count = 0
            
            # Memory cache cleanup (TTLCache handles this automatically)
            # But we'll check disk cache
            if self.config.enable_disk_cache and self._disk_cache_dir:
                for cache_file in self._disk_cache_dir.rglob("*.cache"):
                    try:
                        with open(cache_file, 'rb') as f:
                            cache_data = pickle.load(f)
                        
                        if cache_data.get('expires_at') and time.time() > cache_data['expires_at']:
                            cache_file.unlink()
                            cleaned_count += 1
                    
                    except Exception:
                        # Remove corrupted cache files
                        cache_file.unlink()
                        cleaned_count += 1
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} expired cache entries")
            
            return cleaned_count
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get detailed cache information."""
        with self._lock:
            info = {
                "config": {
                    "memory_cache_size": self.config.memory_cache_size,
                    "disk_cache_enabled": self.config.enable_disk_cache,
                    "default_ttl": self.config.default_ttl,
                    "disk_cache_dir": str(self._disk_cache_dir) if self._disk_cache_dir else None
                },
                "stats": {
                    "hits": self.stats.hits,
                    "misses": self.stats.misses,
                    "hit_rate": self.stats.hit_rate,
                    "evictions": self.stats.evictions,
                    "disk_reads": self.stats.disk_reads,
                    "disk_writes": self.stats.disk_writes
                },
                "memory_cache": {
                    "current_size": len(self._memory_cache),
                    "max_size": self._memory_cache.maxsize,
                    "utilization": len(self._memory_cache) / self._memory_cache.maxsize
                }
            }
            
            # Disk cache info
            if self.config.enable_disk_cache and self._disk_cache_dir:
                try:
                    disk_files = list(self._disk_cache_dir.rglob("*.cache"))
                    total_size = sum(f.stat().st_size for f in disk_files if f.exists())
                    
                    info["disk_cache"] = {
                        "file_count": len(disk_files),
                        "total_size_mb": total_size / 1024 / 1024,
                        "max_size_mb": self.config.disk_cache_size_mb
                    }
                except Exception:
                    info["disk_cache"] = {"error": "Unable to calculate disk cache size"}
            
            return info


# Global cache manager instance
_global_cache = CacheManager()


def get_cache_manager() -> CacheManager:
    """Get global cache manager instance."""
    return _global_cache


def cached(key: Optional[str] = None, ttl: Optional[float] = None):
    """Convenience decorator for caching function results."""
    return _global_cache.cached(key, ttl)


async def cached_async(key: Optional[str] = None, ttl: Optional[float] = None):
    """Convenience decorator for caching async function results."""
    return await _global_cache.cached_async(key, ttl)