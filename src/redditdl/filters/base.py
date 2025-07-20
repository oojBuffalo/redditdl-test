"""
Abstract Filter Base Classes

Defines the core interfaces and utilities for implementing Reddit post filters.
All filters implement the Filter abstract base class and return FilterResult
objects with detailed information about the filtering operation.
"""

import hashlib
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union
from redditdl.scrapers import PostMetadata


class FilterComposition(Enum):
    """How to combine multiple filters."""
    AND = "and"  # All filters must pass
    OR = "or"    # At least one filter must pass


@dataclass
class FilterResult:
    """
    Result of applying a filter to a post.
    
    Attributes:
        passed: Whether the post passed the filter
        reason: Human-readable reason for pass/fail
        metadata: Additional filter-specific metadata
        execution_time: Time taken to apply filter (seconds)
        error: Error message if filter execution failed
    """
    passed: bool
    reason: str = ""
    metadata: Dict[str, Any] = None
    execution_time: float = 0.0
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class FilterPerformanceStats:
    """Performance statistics for filter operations."""
    total_executions: int = 0
    total_time: float = 0.0
    avg_time: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    
    def record_execution(self, execution_time: float, cache_hit: bool = False):
        """Record a filter execution."""
        self.total_executions += 1
        self.total_time += execution_time
        self.avg_time = self.total_time / self.total_executions
        
        if cache_hit:
            self.cache_hits += 1
        else:
            self.cache_misses += 1
    
    @property
    def cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio."""
        total_requests = self.cache_hits + self.cache_misses
        return self.cache_hits / total_requests if total_requests > 0 else 0.0


class OptimizedFilter:
    """
    Performance-optimized wrapper for filters with caching and monitoring.
    
    This wrapper adds result caching, performance monitoring, and optimization
    features to any Filter implementation.
    """
    
    def __init__(self, filter_instance: 'Filter', enable_cache: bool = True, cache_size: int = 1000):
        self.filter = filter_instance
        self.enable_cache = enable_cache
        self.cache_size = cache_size
        self.stats = FilterPerformanceStats()
        self._cache = {}
        
        # Estimated costs for filter ordering (lower = faster)
        self._estimated_cost = self._estimate_filter_cost()
    
    def _estimate_filter_cost(self) -> float:
        """Estimate the computational cost of this filter."""
        # Cost estimates based on filter complexity
        filter_costs = {
            'score': 1.0,     # Very fast - simple numeric comparison
            'nsfw': 1.5,      # Fast - simple boolean check
            'media_type': 2.0, # Fast - simple string/list operations
            'date': 3.0,      # Medium - date parsing and comparison
            'domain': 4.0,    # Medium - URL parsing and regex
            'keyword': 5.0,   # Slower - text search and regex
        }
        return filter_costs.get(self.filter.name, 3.0)
    
    def _generate_cache_key(self, post: PostMetadata) -> str:
        """Generate a cache key for the post."""
        if not self.enable_cache:
            return None
        
        # Create a cache key based on relevant post attributes
        key_data = {
            'id': getattr(post, 'id', ''),
            'title': getattr(post, 'title', ''),
            'score': getattr(post, 'score', 0),
            'is_nsfw': getattr(post, 'is_nsfw', False),
            'url': getattr(post, 'url', ''),
            'created_utc': getattr(post, 'created_utc', 0),
            'subreddit': getattr(post, 'subreddit', ''),
            'selftext': getattr(post, 'selftext', ''),
        }
        
        # Create hash of key data combined with filter config
        cache_key = f"{self.filter.name}:{hashlib.md5(str(key_data).encode()).hexdigest()}"
        return cache_key
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """Apply the filter with caching and performance monitoring."""
        start_time = time.time()
        
        # Check cache first
        cache_key = self._generate_cache_key(post)
        if cache_key and cache_key in self._cache:
            cached_result = self._cache[cache_key]
            execution_time = time.time() - start_time
            self.stats.record_execution(execution_time, cache_hit=True)
            
            # Return cached result with updated execution time
            return FilterResult(
                passed=cached_result.passed,
                reason=f"[CACHED] {cached_result.reason}",
                metadata=cached_result.metadata,
                execution_time=execution_time,
                error=cached_result.error
            )
        
        # Execute filter
        try:
            result = self.filter.apply(post)
            execution_time = time.time() - start_time
            result.execution_time = execution_time
            
            # Cache result if enabled
            if cache_key and self.enable_cache:
                # Manage cache size
                if len(self._cache) >= self.cache_size:
                    # Remove oldest entries (simple FIFO)
                    oldest_key = next(iter(self._cache))
                    del self._cache[oldest_key]
                
                self._cache[cache_key] = result
            
            self.stats.record_execution(execution_time, cache_hit=False)
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_result = FilterResult(
                passed=False,
                reason=f"Filter execution error: {e}",
                execution_time=execution_time,
                error=str(e)
            )
            self.stats.record_execution(execution_time, cache_hit=False)
            return error_result
    
    @property
    def name(self) -> str:
        """Get the filter name."""
        return self.filter.name
    
    @property
    def description(self) -> str:
        """Get the filter description."""
        return self.filter.description
    
    @property
    def estimated_cost(self) -> float:
        """Get the estimated computational cost."""
        return self._estimated_cost
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return {
            'total_executions': self.stats.total_executions,
            'total_time': self.stats.total_time,
            'avg_time': self.stats.avg_time,
            'cache_hits': self.stats.cache_hits,
            'cache_misses': self.stats.cache_misses,
            'cache_hit_ratio': self.stats.cache_hit_ratio,
            'estimated_cost': self._estimated_cost,
        }
    
    def clear_cache(self):
        """Clear the result cache."""
        self._cache.clear()


class Filter(ABC):
    """
    Abstract base class for all Reddit post filters.
    
    Filters are composable components that evaluate whether a Reddit post
    meets specific criteria. Each filter should be stateless and thread-safe.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the filter with configuration.
        
        Args:
            config: Filter configuration dictionary
        """
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name of the filter."""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        pass
    
    @abstractmethod
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        pass
    
    def validate_config(self) -> List[str]:
        """
        Validate the filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        return []
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for this filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {},
            "additionalProperties": False
        }
    
    def __str__(self) -> str:
        """String representation of the filter."""
        return f"{self.name}: {self.description}"
    
    def __repr__(self) -> str:
        """Detailed string representation of the filter."""
        return f"{self.__class__.__name__}(config={self.config})"


class FilterChain:
    """
    Chains multiple filters together with AND/OR logic.
    
    This class allows combining multiple filters using logical operators
    and provides optimized execution with early termination.
    """
    
    def __init__(self, filters: List[Filter], composition: FilterComposition = FilterComposition.AND):
        """
        Initialize the filter chain.
        
        Args:
            filters: List of filters to chain together
            composition: How to combine filter results (AND/OR)
        """
        self.filters = filters
        self.composition = composition
        self.logger = logging.getLogger(__name__)
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply all filters in the chain to a post with performance optimization.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the chain
        """
        import time
        start_time = time.time()
        
        if not self.filters:
            return FilterResult(
                passed=True,
                reason="No filters in chain",
                execution_time=time.time() - start_time
            )
        
        # Create optimized filters and sort by estimated cost (fastest first)
        optimized_filters = []
        for filter_instance in self.filters:
            if isinstance(filter_instance, OptimizedFilter):
                optimized_filters.append(filter_instance)
            else:
                optimized_filters.append(OptimizedFilter(filter_instance))
        
        # Sort filters by estimated cost for optimal execution order
        optimized_filters.sort(key=lambda f: f.estimated_cost)
        
        results = []
        total_execution_time = 0.0
        
        for filter_instance in optimized_filters:
            try:
                result = filter_instance.apply(post)
                results.append(result)
                total_execution_time += result.execution_time
                
                # Early termination optimization
                if self.composition == FilterComposition.AND and not result.passed:
                    # In AND mode, if any filter fails, the whole chain fails
                    return FilterResult(
                        passed=False,
                        reason=f"Failed {filter_instance.name}: {result.reason}",
                        metadata={
                            "filter_chain": self.composition.value,
                            "failed_filter": filter_instance.name,
                            "filters_executed": len(results),
                            "total_filters": len(self.filters),
                            "individual_results": [
                                {
                                    "filter": f.name,
                                    "passed": r.passed,
                                    "reason": r.reason,
                                    "execution_time": r.execution_time
                                }
                                for f, r in zip(self.filters[:len(results)], results)
                            ]
                        },
                        execution_time=time.time() - start_time
                    )
                elif self.composition == FilterComposition.OR and result.passed:
                    # In OR mode, if any filter passes, the whole chain passes
                    return FilterResult(
                        passed=True,
                        reason=f"Passed {filter_instance.name}: {result.reason}",
                        metadata={
                            "filter_chain": self.composition.value,
                            "passed_filter": filter_instance.name,
                            "filters_executed": len(results),
                            "total_filters": len(self.filters),
                            "individual_results": [
                                {
                                    "filter": f.name,
                                    "passed": r.passed,
                                    "reason": r.reason,
                                    "execution_time": r.execution_time
                                }
                                for f, r in zip(self.filters[:len(results)], results)
                            ]
                        },
                        execution_time=time.time() - start_time
                    )
                        
            except Exception as e:
                self.logger.error(f"Error applying filter {filter_instance.name}: {e}")
                error_result = FilterResult(
                    passed=False,  # Fail-safe: errors cause filter to fail
                    reason=f"Filter error: {e}",
                    error=str(e),
                    execution_time=time.time() - start_time
                )
                results.append(error_result)
                
                if self.composition == FilterComposition.AND:
                    # In AND mode, errors cause the whole chain to fail
                    return error_result
        
        # Determine final result based on composition
        if self.composition == FilterComposition.AND:
            # All filters must pass
            all_passed = all(r.passed for r in results)
            return FilterResult(
                passed=all_passed,
                reason="All filters passed" if all_passed else "One or more filters failed",
                metadata={
                    "filter_chain": self.composition.value,
                    "filters_executed": len(results),
                    "total_filters": len(self.filters),
                    "individual_results": [
                        {
                            "filter": f.name,
                            "passed": r.passed,
                            "reason": r.reason,
                            "execution_time": r.execution_time
                        }
                        for f, r in zip(self.filters, results)
                    ]
                },
                execution_time=time.time() - start_time
            )
        else:
            # At least one filter must pass
            any_passed = any(r.passed for r in results)
            return FilterResult(
                passed=any_passed,
                reason="At least one filter passed" if any_passed else "All filters failed",
                metadata={
                    "filter_chain": self.composition.value,
                    "filters_executed": len(results),
                    "total_filters": len(self.filters),
                    "individual_results": [
                        {
                            "filter": f.name,
                            "passed": r.passed,
                            "reason": r.reason,
                            "execution_time": r.execution_time
                        }
                        for f, r in zip(self.filters, results)
                    ]
                },
                execution_time=time.time() - start_time
            )
    
    def validate_config(self) -> List[str]:
        """
        Validate all filters in the chain.
        
        Returns:
            List of validation error messages from all filters
        """
        errors = []
        for filter_instance in self.filters:
            filter_errors = filter_instance.validate_config()
            errors.extend([f"{filter_instance.name}: {error}" for error in filter_errors])
        return errors
    
    def __len__(self) -> int:
        """Return the number of filters in the chain."""
        return len(self.filters)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics for all filters in the chain.
        
        Returns:
            Dictionary containing performance metrics for each filter
        """
        stats = {}
        for i, filter_instance in enumerate(self.filters):
            if isinstance(filter_instance, OptimizedFilter):
                stats[filter_instance.name] = filter_instance.get_stats()
            else:
                stats[f"filter_{i}"] = {
                    "name": getattr(filter_instance, 'name', f'unnamed_filter_{i}'),
                    "total_executions": 0,
                    "total_time": 0.0,
                    "avg_time": 0.0,
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "cache_hit_ratio": 0.0,
                    "estimated_cost": 3.0
                }
        return stats
    
    def clear_all_caches(self):
        """Clear caches for all optimized filters in the chain."""
        for filter_instance in self.filters:
            if isinstance(filter_instance, OptimizedFilter):
                filter_instance.clear_cache()
    
    def optimize_filter_order(self):
        """
        Reorder filters based on their actual performance statistics.
        
        This method sorts filters by their average execution time to optimize
        future filtering operations.
        """
        def get_actual_cost(filter_instance):
            if isinstance(filter_instance, OptimizedFilter) and filter_instance.stats.total_executions > 0:
                return filter_instance.stats.avg_time
            elif isinstance(filter_instance, OptimizedFilter):
                return filter_instance.estimated_cost
            else:
                return 3.0  # Default cost for non-optimized filters
        
        self.filters.sort(key=get_actual_cost)
    
    def __str__(self) -> str:
        """String representation of the filter chain."""
        filter_names = [f.name for f in self.filters]
        composition_str = " AND " if self.composition == FilterComposition.AND else " OR "
        return f"FilterChain({composition_str.join(filter_names)})"