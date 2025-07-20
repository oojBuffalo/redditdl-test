"""
Resource Profiler

Advanced profiling capabilities for identifying performance bottlenecks,
resource usage patterns, and optimization opportunities.
"""

import asyncio
import cProfile
import io
import logging
import pstats
import time
import tracemalloc
from typing import Dict, List, Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from contextlib import contextmanager
from pathlib import Path
import psutil
import threading
import functools

from redditdl.core.monitoring.metrics import MetricsCollector, MetricSummary


T = TypeVar('T')
logger = logging.getLogger(__name__)


@dataclass
class ProfileResult:
    """Container for profiling results."""
    name: str
    duration: float
    cpu_time: float
    memory_peak_mb: float
    memory_delta_mb: float
    function_stats: Dict[str, Any] = field(default_factory=dict)
    memory_trace: Optional[List[Any]] = None


@dataclass
class BottleneckInfo:
    """Information about identified performance bottlenecks."""
    function_name: str
    filename: str
    line_number: int
    call_count: int
    total_time: float
    per_call_time: float
    percentage_of_total: float


class ResourceProfiler:
    """
    Advanced resource profiler for performance analysis.
    
    Provides:
    - CPU profiling with function-level statistics
    - Memory profiling with allocation tracking
    - Automatic bottleneck identification
    - Profile comparison and trend analysis
    """
    
    def __init__(self):
        """Initialize resource profiler."""
        self._profiles: Dict[str, List[ProfileResult]] = {}
        self._active_profiles: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        
        # Memory tracking state
        self._memory_tracking = False
        self._memory_snapshots: Dict[str, Any] = {}
    
    def profile_function(self, name: Optional[str] = None):
        """
        Decorator for profiling individual functions.
        
        Args:
            name: Optional custom name for the profile
            
        Returns:
            Decorated function
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            profile_name = name or f"{func.__module__}.{func.__name__}"
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> T:
                with self.profile_context(profile_name):
                    return func(*args, **kwargs)
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> T:
                with self.profile_context(profile_name):
                    return await func(*args, **kwargs)
            
            # Return appropriate wrapper based on function type
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return wrapper
        
        return decorator
    
    @contextmanager
    def profile_context(self, name: str):
        """
        Context manager for profiling code blocks.
        
        Args:
            name: Profile name for identification
        """
        # Start profiling
        start_time = time.time()
        start_cpu = time.process_time()
        
        # Memory tracking
        memory_before = self._get_memory_usage()
        memory_snapshot_before = None
        
        if self._memory_tracking:
            memory_snapshot_before = tracemalloc.take_snapshot()
        
        # CPU profiling
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            yield
        finally:
            # Stop profiling
            profiler.disable()
            
            end_time = time.time()
            end_cpu = time.process_time()
            memory_after = self._get_memory_usage()
            
            # Calculate metrics
            duration = end_time - start_time
            cpu_time = end_cpu - start_cpu
            memory_delta = memory_after - memory_before
            
            # Memory peak tracking
            memory_peak = memory_after  # Simplified - could track actual peak
            
            # Extract function statistics
            stats_io = io.StringIO()
            stats = pstats.Stats(profiler, stream=stats_io)
            stats.sort_stats('cumulative')
            
            function_stats = self._extract_function_stats(stats)
            
            # Memory trace (if tracking enabled)
            memory_trace = None
            if self._memory_tracking and memory_snapshot_before:
                memory_snapshot_after = tracemalloc.take_snapshot()
                memory_trace = self._analyze_memory_diff(
                    memory_snapshot_before, 
                    memory_snapshot_after
                )
            
            # Create profile result
            result = ProfileResult(
                name=name,
                duration=duration,
                cpu_time=cpu_time,
                memory_peak_mb=memory_peak,
                memory_delta_mb=memory_delta,
                function_stats=function_stats,
                memory_trace=memory_trace
            )
            
            # Store result
            with self._lock:
                if name not in self._profiles:
                    self._profiles[name] = []
                self._profiles[name].append(result)
            
            logger.debug(
                f"Profile '{name}': {duration:.3f}s wall, {cpu_time:.3f}s CPU, "
                f"{memory_delta:.1f}MB memory delta"
            )
    
    def start_memory_tracking(self) -> None:
        """Start detailed memory allocation tracking."""
        if not self._memory_tracking:
            tracemalloc.start()
            self._memory_tracking = True
            logger.info("Started memory tracking")
    
    def stop_memory_tracking(self) -> None:
        """Stop memory allocation tracking."""
        if self._memory_tracking:
            tracemalloc.stop()
            self._memory_tracking = False
            logger.info("Stopped memory tracking")
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0
    
    def _extract_function_stats(self, stats: pstats.Stats) -> Dict[str, Any]:
        """Extract function statistics from cProfile stats."""
        try:
            # Get top functions by cumulative time
            stats_dict = {}
            
            # Convert stats to dictionary format
            for func, (cc, nc, tt, ct, callers) in stats.stats.items():
                filename, line_num, func_name = func
                
                stats_dict[f"{filename}:{line_num}({func_name})"] = {
                    'call_count': cc,
                    'native_calls': nc,
                    'total_time': tt,
                    'cumulative_time': ct,
                    'per_call_total': tt / cc if cc > 0 else 0,
                    'per_call_cumulative': ct / cc if cc > 0 else 0
                }
            
            # Sort by cumulative time and get top 20
            sorted_funcs = sorted(
                stats_dict.items(),
                key=lambda x: x[1]['cumulative_time'],
                reverse=True
            )[:20]
            
            return dict(sorted_funcs)
        
        except Exception as e:
            logger.warning(f"Failed to extract function stats: {e}")
            return {}
    
    def _analyze_memory_diff(self, before: Any, after: Any) -> List[Dict[str, Any]]:
        """Analyze memory allocation differences."""
        try:
            top_stats = after.compare_to(before, 'lineno')
            
            memory_trace = []
            for stat in top_stats[:10]:  # Top 10 allocations
                memory_trace.append({
                    'filename': stat.traceback.format()[0] if stat.traceback else 'unknown',
                    'size_mb': stat.size / 1024 / 1024,
                    'count': stat.count,
                    'size_diff_mb': stat.size_diff / 1024 / 1024 if hasattr(stat, 'size_diff') else 0
                })
            
            return memory_trace
        
        except Exception as e:
            logger.warning(f"Failed to analyze memory diff: {e}")
            return []
    
    def get_profile_results(self, name: str) -> List[ProfileResult]:
        """Get all profile results for a given name."""
        with self._lock:
            return self._profiles.get(name, []).copy()
    
    def get_all_profiles(self) -> Dict[str, List[ProfileResult]]:
        """Get all profile results."""
        with self._lock:
            return {
                name: results.copy() 
                for name, results in self._profiles.items()
            }
    
    def identify_bottlenecks(self, name: str, min_time: float = 0.001) -> List[BottleneckInfo]:
        """
        Identify performance bottlenecks from profile results.
        
        Args:
            name: Profile name to analyze
            min_time: Minimum time threshold for bottleneck identification
            
        Returns:
            List of identified bottlenecks
        """
        results = self.get_profile_results(name)
        if not results:
            return []
        
        # Use the most recent result
        latest_result = results[-1]
        function_stats = latest_result.function_stats
        
        if not function_stats:
            return []
        
        # Calculate total time for percentage calculation
        total_time = sum(
            stats['cumulative_time'] 
            for stats in function_stats.values()
        )
        
        bottlenecks = []
        for func_key, stats in function_stats.items():
            cumulative_time = stats['cumulative_time']
            
            if cumulative_time < min_time:
                continue
            
            # Parse function key
            parts = func_key.split(':', 1)
            if len(parts) != 2:
                continue
            
            filename = parts[0]
            func_part = parts[1]
            
            # Extract line number and function name
            line_num = 0
            func_name = func_part
            if '(' in func_part and ')' in func_part:
                line_str = func_part.split('(')[0]
                func_name = func_part.split('(')[1].rstrip(')')
                try:
                    line_num = int(line_str)
                except ValueError:
                    pass
            
            percentage = (cumulative_time / total_time) * 100 if total_time > 0 else 0
            
            bottleneck = BottleneckInfo(
                function_name=func_name,
                filename=filename,
                line_number=line_num,
                call_count=stats['call_count'],
                total_time=cumulative_time,
                per_call_time=stats['per_call_cumulative'],
                percentage_of_total=percentage
            )
            
            bottlenecks.append(bottleneck)
        
        # Sort by total time descending
        bottlenecks.sort(key=lambda x: x.total_time, reverse=True)
        
        return bottlenecks[:10]  # Top 10 bottlenecks
    
    def compare_profiles(self, name: str, count: int = 2) -> Dict[str, Any]:
        """
        Compare recent profile results to identify trends.
        
        Args:
            name: Profile name to compare
            count: Number of recent profiles to compare
            
        Returns:
            Comparison analysis
        """
        results = self.get_profile_results(name)
        if len(results) < count:
            return {"error": f"Need at least {count} profile results"}
        
        # Get the most recent profiles
        recent_results = results[-count:]
        
        comparison = {
            "profile_count": len(recent_results),
            "time_trend": [],
            "memory_trend": [],
            "performance_change": {}
        }
        
        # Analyze trends
        for i, result in enumerate(recent_results):
            comparison["time_trend"].append({
                "index": i,
                "duration": result.duration,
                "cpu_time": result.cpu_time
            })
            
            comparison["memory_trend"].append({
                "index": i,
                "peak_mb": result.memory_peak_mb,
                "delta_mb": result.memory_delta_mb
            })
        
        # Calculate performance change
        if len(recent_results) >= 2:
            first = recent_results[0]
            last = recent_results[-1]
            
            duration_change = ((last.duration - first.duration) / first.duration) * 100
            memory_change = ((last.memory_peak_mb - first.memory_peak_mb) / 
                           first.memory_peak_mb) * 100 if first.memory_peak_mb > 0 else 0
            
            comparison["performance_change"] = {
                "duration_change_percent": duration_change,
                "memory_change_percent": memory_change,
                "trend": "improving" if duration_change < -5 else "degrading" if duration_change > 5 else "stable"
            }
        
        return comparison
    
    def export_profile_report(self, output_file: Path, name: Optional[str] = None) -> None:
        """
        Export detailed profile report to file.
        
        Args:
            output_file: Output file path
            name: Optional specific profile name (exports all if None)
        """
        import json
        
        profiles_to_export = {}
        
        if name:
            profiles_to_export[name] = self.get_profile_results(name)
        else:
            profiles_to_export = self.get_all_profiles()
        
        report_data = {
            "export_timestamp": time.time(),
            "profiles": {}
        }
        
        for profile_name, results in profiles_to_export.items():
            profile_data = {
                "result_count": len(results),
                "results": [],
                "bottlenecks": [],
                "trends": {}
            }
            
            # Add individual results
            for result in results[-10:]:  # Last 10 results
                result_data = {
                    "name": result.name,
                    "duration": result.duration,
                    "cpu_time": result.cpu_time,
                    "memory_peak_mb": result.memory_peak_mb,
                    "memory_delta_mb": result.memory_delta_mb,
                    "top_functions": list(result.function_stats.items())[:5]
                }
                profile_data["results"].append(result_data)
            
            # Add bottleneck analysis
            bottlenecks = self.identify_bottlenecks(profile_name)
            profile_data["bottlenecks"] = [
                {
                    "function": b.function_name,
                    "file": b.filename,
                    "line": b.line_number,
                    "calls": b.call_count,
                    "total_time": b.total_time,
                    "per_call": b.per_call_time,
                    "percentage": b.percentage_of_total
                }
                for b in bottlenecks
            ]
            
            # Add trend analysis
            if len(results) >= 2:
                profile_data["trends"] = self.compare_profiles(profile_name)
            
            report_data["profiles"][profile_name] = profile_data
        
        # Write report
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"Exported profile report to {output_file}")
    
    def clear_profiles(self, name: Optional[str] = None) -> None:
        """
        Clear profile results.
        
        Args:
            name: Specific profile to clear (clears all if None)
        """
        with self._lock:
            if name:
                self._profiles.pop(name, None)
                logger.info(f"Cleared profiles for '{name}'")
            else:
                self._profiles.clear()
                logger.info("Cleared all profiles")


# Global profiler instance
_global_profiler = ResourceProfiler()


def get_profiler() -> ResourceProfiler:
    """Get global resource profiler instance."""
    return _global_profiler


def profile_function(name: Optional[str] = None):
    """Convenience decorator for profiling functions."""
    return _global_profiler.profile_function(name)


def profile_context(name: str):
    """Convenience context manager for profiling code blocks."""
    return _global_profiler.profile_context(name)