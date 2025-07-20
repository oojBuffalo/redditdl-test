"""
Concurrent Rate Limiters

Advanced rate limiting for concurrent operations with different strategies
for various Reddit API endpoints and public scraping modes.
"""

import asyncio
import time
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from redditdl.core.monitoring.metrics import get_metrics_collector, time_operation


class LimiterType(Enum):
    """Rate limiter types for different operation modes."""
    API = "api"           # Reddit API operations
    PUBLIC = "public"     # Public/web scraping
    DOWNLOADS = "downloads"  # Media downloads
    DATABASE = "database"    # Database operations


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 1.0
    burst_limit: int = 5
    max_concurrent: int = 10
    backoff_factor: float = 2.0
    max_backoff: float = 60.0


class ConcurrentRateLimiter:
    """
    Advanced rate limiter for concurrent operations.
    
    Provides:
    - Token bucket algorithm for smooth rate limiting
    - Configurable burst capacity
    - Exponential backoff on rate limit violations
    - Per-operation-type rate limiting
    - Thread-safe concurrent access
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        """
        Initialize concurrent rate limiter.
        
        Args:
            config: Rate limiting configuration
        """
        self.config = config or RateLimitConfig()
        
        # Token bucket state
        self._tokens = float(self.config.burst_limit)
        self._last_update = time.time()
        
        # Concurrency control
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._lock = asyncio.Lock()
        
        # Backoff tracking
        self._consecutive_violations = 0
        self._backoff_until = 0.0
        
        # Statistics
        self._total_requests = 0
        self._violations = 0
        self._total_wait_time = 0.0
    
    async def acquire(self) -> None:
        """
        Acquire permission to proceed with an operation.
        
        Blocks until rate limit allows the operation.
        """
        async with self._semaphore:
            await self._wait_for_token()
    
    async def _wait_for_token(self) -> None:
        """Wait for a token to become available."""
        async with self._lock:
            now = time.time()
            
            # Check if we're in backoff period
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                self._total_wait_time += wait_time
                await asyncio.sleep(wait_time)
                now = time.time()
            
            # Refill tokens based on elapsed time
            elapsed = now - self._last_update
            tokens_to_add = elapsed * self.config.requests_per_second
            self._tokens = min(
                self.config.burst_limit,
                self._tokens + tokens_to_add
            )
            self._last_update = now
            
            # Check if we have a token available
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                self._consecutive_violations = 0
                self._total_requests += 1
            else:
                # Rate limit violation - apply backoff
                self._violations += 1
                self._consecutive_violations += 1
                
                backoff_time = min(
                    self.config.max_backoff,
                    (self.config.backoff_factor ** self._consecutive_violations) * 0.1
                )
                
                self._backoff_until = now + backoff_time
                self._total_wait_time += backoff_time
                
                await asyncio.sleep(backoff_time)
                
                # Recursively try again after backoff
                await self._wait_for_token()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            'total_requests': self._total_requests,
            'violations': self._violations,
            'total_wait_time': self._total_wait_time,
            'current_tokens': self._tokens,
            'consecutive_violations': self._consecutive_violations,
            'is_in_backoff': time.time() < self._backoff_until
        }
    
    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._total_requests = 0
        self._violations = 0
        self._total_wait_time = 0.0


class MultiLimiter:
    """
    Manages multiple rate limiters for different operation types.
    
    Provides centralized rate limiting for API calls, downloads,
    database operations, etc. with appropriate limits for each.
    """
    
    def __init__(self):
        """Initialize multi-limiter with default configurations."""
        self._limiters: Dict[LimiterType, ConcurrentRateLimiter] = {}
        
        # Initialize default limiters
        self._limiters[LimiterType.API] = ConcurrentRateLimiter(
            RateLimitConfig(
                requests_per_second=1.4,  # Reddit API: ~1 req/sec with buffer
                burst_limit=3,
                max_concurrent=5,
                backoff_factor=2.0,
                max_backoff=30.0
            )
        )
        
        self._limiters[LimiterType.PUBLIC] = ConcurrentRateLimiter(
            RateLimitConfig(
                requests_per_second=0.16,  # Public scraping: ~6 sec intervals
                burst_limit=2,
                max_concurrent=3,
                backoff_factor=3.0,
                max_backoff=60.0
            )
        )
        
        self._limiters[LimiterType.DOWNLOADS] = ConcurrentRateLimiter(
            RateLimitConfig(
                requests_per_second=2.0,  # Downloads: more aggressive
                burst_limit=10,
                max_concurrent=15,
                backoff_factor=1.5,
                max_backoff=20.0
            )
        )
        
        self._limiters[LimiterType.DATABASE] = ConcurrentRateLimiter(
            RateLimitConfig(
                requests_per_second=10.0,  # Database: high throughput
                burst_limit=50,
                max_concurrent=20,
                backoff_factor=1.2,
                max_backoff=5.0
            )
        )
    
    async def acquire(self, limiter_type: LimiterType) -> None:
        """
        Acquire permission for specific operation type.
        
        Args:
            limiter_type: Type of operation to rate limit
        """
        if limiter_type not in self._limiters:
            raise ValueError(f"Unknown limiter type: {limiter_type}")
        
        await self._limiters[limiter_type].acquire()
    
    def get_limiter(self, limiter_type: LimiterType) -> ConcurrentRateLimiter:
        """Get specific rate limiter for manual use."""
        return self._limiters[limiter_type]
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all rate limiters."""
        return {
            limiter_type.value: limiter.get_stats()
            for limiter_type, limiter in self._limiters.items()
        }
    
    def reset_all_stats(self) -> None:
        """Reset statistics for all rate limiters."""
        for limiter in self._limiters.values():
            limiter.reset_stats()


# Global multi-limiter instance
_global_limiter = MultiLimiter()


async def rate_limit(limiter_type: LimiterType) -> None:
    """
    Convenience function for rate limiting operations.
    
    Args:
        limiter_type: Type of operation to rate limit
    """
    await _global_limiter.acquire(limiter_type)


def get_rate_limiter(limiter_type: LimiterType) -> ConcurrentRateLimiter:
    """Get specific rate limiter instance."""
    return _global_limiter.get_limiter(limiter_type)


def get_rate_limit_stats() -> Dict[str, Dict[str, Any]]:
    """Get rate limiting statistics for all operation types."""
    return _global_limiter.get_all_stats()