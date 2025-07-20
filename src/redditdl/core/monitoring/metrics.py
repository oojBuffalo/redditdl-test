"""
Performance Metrics Collection

Comprehensive metrics collection system for tracking performance,
resource usage, and bottleneck identification across all RedditDL components.
"""

import asyncio
import logging
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import psutil
import json
from pathlib import Path


logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics that can be collected."""
    COUNTER = "counter"           # Incrementing values
    GAUGE = "gauge"              # Current values
    HISTOGRAM = "histogram"      # Distribution of values
    TIMER = "timer"              # Timing measurements


@dataclass
class MetricValue:
    """Container for a metric value with metadata."""
    value: float
    timestamp: float = field(default_factory=time.time)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSummary:
    """Summary statistics for a metric."""
    count: int = 0
    sum: float = 0.0
    min: float = float('inf')
    max: float = float('-inf')
    avg: float = 0.0
    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0


class Metric:
    """
    Base metric class for collecting and aggregating performance data.
    """
    
    def __init__(self, name: str, metric_type: MetricType, 
                 description: str = "", max_samples: int = 10000):
        """
        Initialize metric.
        
        Args:
            name: Metric name
            metric_type: Type of metric
            description: Human-readable description
            max_samples: Maximum number of samples to keep
        """
        self.name = name
        self.type = metric_type
        self.description = description
        self.max_samples = max_samples
        
        # Storage for metric values
        self._values = deque(maxlen=max_samples)
        self._lock = threading.Lock()
        
        # Cached summary (invalidated on new values)
        self._cached_summary: Optional[MetricSummary] = None
        self._summary_dirty = True
    
    def record(self, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Record a new metric value.
        
        Args:
            value: Metric value to record
            tags: Optional tags for the metric
        """
        with self._lock:
            metric_value = MetricValue(
                value=value,
                tags=tags or {}
            )
            self._values.append(metric_value)
            self._summary_dirty = True
    
    def increment(self, amount: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Increment counter metric.
        
        Args:
            amount: Amount to increment by
            tags: Optional tags for the metric
        """
        if self.type != MetricType.COUNTER:
            raise ValueError("increment() only valid for COUNTER metrics")
        
        # For counters, we track total increments
        current_total = self._values[-1].value if self._values else 0.0
        self.record(current_total + amount, tags)
    
    def set(self, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Set gauge metric value.
        
        Args:
            value: Value to set
            tags: Optional tags for the metric
        """
        if self.type != MetricType.GAUGE:
            raise ValueError("set() only valid for GAUGE metrics")
        
        self.record(value, tags)
    
    def time_block(self, tags: Optional[Dict[str, str]] = None):
        """
        Context manager for timing code blocks.
        
        Args:
            tags: Optional tags for the timing metric
            
        Returns:
            Timer context manager
        """
        if self.type != MetricType.TIMER:
            raise ValueError("time_block() only valid for TIMER metrics")
        
        return TimerContext(self, tags)
    
    def get_summary(self, force_refresh: bool = False) -> MetricSummary:
        """
        Get summary statistics for the metric.
        
        Args:
            force_refresh: Force recalculation of summary
            
        Returns:
            Metric summary statistics
        """
        with self._lock:
            if self._summary_dirty or force_refresh or self._cached_summary is None:
                self._cached_summary = self._calculate_summary()
                self._summary_dirty = False
            
            return self._cached_summary
    
    def _calculate_summary(self) -> MetricSummary:
        """Calculate summary statistics from current values."""
        if not self._values:
            return MetricSummary()
        
        values = [v.value for v in self._values]
        values.sort()
        
        count = len(values)
        total = sum(values)
        
        summary = MetricSummary(
            count=count,
            sum=total,
            min=min(values),
            max=max(values),
            avg=total / count if count > 0 else 0.0
        )
        
        # Calculate percentiles
        if count > 0:
            summary.p50 = self._percentile(values, 0.5)
            summary.p95 = self._percentile(values, 0.95)
            summary.p99 = self._percentile(values, 0.99)
        
        return summary
    
    def _percentile(self, sorted_values: List[float], percentile: float) -> float:
        """Calculate percentile from sorted values."""
        if not sorted_values:
            return 0.0
        
        index = percentile * (len(sorted_values) - 1)
        lower_index = int(index)
        upper_index = min(lower_index + 1, len(sorted_values) - 1)
        
        if lower_index == upper_index:
            return sorted_values[lower_index]
        
        # Linear interpolation
        weight = index - lower_index
        return (sorted_values[lower_index] * (1 - weight) + 
                sorted_values[upper_index] * weight)
    
    def get_recent_values(self, count: int = 100) -> List[MetricValue]:
        """Get recent metric values."""
        with self._lock:
            return list(self._values)[-count:]
    
    def clear(self) -> None:
        """Clear all metric values."""
        with self._lock:
            self._values.clear()
            self._cached_summary = None
            self._summary_dirty = True


class TimerContext:
    """Context manager for timing operations."""
    
    def __init__(self, metric: Metric, tags: Optional[Dict[str, str]] = None):
        self.metric = metric
        self.tags = tags
        self.start_time: Optional[float] = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is not None:
            duration = time.time() - self.start_time
            self.metric.record(duration, self.tags)


class MetricsCollector:
    """
    Central metrics collection system for RedditDL performance monitoring.
    
    Provides:
    - Automatic system resource monitoring
    - Custom application metrics
    - Metric aggregation and summarization
    - Export capabilities for analysis
    """
    
    def __init__(self):
        """Initialize metrics collector."""
        self._metrics: Dict[str, Metric] = {}
        self._system_monitor_task: Optional[asyncio.Task] = None
        self._monitoring = False
        self._monitor_interval = 5.0  # seconds
        
        # Initialize built-in system metrics
        self._init_system_metrics()
    
    def _init_system_metrics(self) -> None:
        """Initialize system resource metrics."""
        self.create_metric(
            "system.cpu_percent", 
            MetricType.GAUGE,
            "System CPU usage percentage"
        )
        
        self.create_metric(
            "system.memory_percent",
            MetricType.GAUGE,
            "System memory usage percentage"
        )
        
        self.create_metric(
            "system.memory_mb",
            MetricType.GAUGE,
            "Process memory usage in MB"
        )
        
        self.create_metric(
            "system.disk_io_read_mb",
            MetricType.COUNTER,
            "Cumulative disk read in MB"
        )
        
        self.create_metric(
            "system.disk_io_write_mb",
            MetricType.COUNTER,
            "Cumulative disk write in MB"
        )
        
        self.create_metric(
            "system.network_io_recv_mb",
            MetricType.COUNTER,
            "Cumulative network received in MB"
        )
        
        self.create_metric(
            "system.network_io_sent_mb",
            MetricType.COUNTER,
            "Cumulative network sent in MB"
        )
    
    def create_metric(self, name: str, metric_type: MetricType, 
                     description: str = "") -> Metric:
        """
        Create a new metric.
        
        Args:
            name: Metric name (use dots for namespacing)
            metric_type: Type of metric
            description: Human-readable description
            
        Returns:
            Created metric instance
        """
        if name in self._metrics:
            return self._metrics[name]
        
        metric = Metric(name, metric_type, description)
        self._metrics[name] = metric
        
        logger.debug(f"Created metric: {name} ({metric_type.value})")
        return metric
    
    def get_metric(self, name: str) -> Optional[Metric]:
        """Get metric by name."""
        return self._metrics.get(name)
    
    def counter(self, name: str, description: str = "") -> Metric:
        """Create or get a counter metric."""
        return self.create_metric(name, MetricType.COUNTER, description)
    
    def gauge(self, name: str, description: str = "") -> Metric:
        """Create or get a gauge metric."""
        return self.create_metric(name, MetricType.GAUGE, description)
    
    def histogram(self, name: str, description: str = "") -> Metric:
        """Create or get a histogram metric."""
        return self.create_metric(name, MetricType.HISTOGRAM, description)
    
    def timer(self, name: str, description: str = "") -> Metric:
        """Create or get a timer metric."""
        return self.create_metric(name, MetricType.TIMER, description)
    
    def record(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Record a metric value by name.
        
        Args:
            name: Metric name
            value: Value to record
            tags: Optional tags
        """
        metric = self._metrics.get(name)
        if metric:
            metric.record(value, tags)
        else:
            logger.warning(f"Attempted to record unknown metric: {name}")
    
    def increment(self, name: str, amount: float = 1.0, 
                 tags: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric by name.
        
        Args:
            name: Counter metric name
            amount: Amount to increment
            tags: Optional tags
        """
        metric = self._metrics.get(name)
        if metric and metric.type == MetricType.COUNTER:
            metric.increment(amount, tags)
        else:
            logger.warning(f"Attempted to increment unknown or non-counter metric: {name}")
    
    def set_gauge(self, name: str, value: float, 
                 tags: Optional[Dict[str, str]] = None) -> None:
        """
        Set a gauge metric value by name.
        
        Args:
            name: Gauge metric name
            value: Value to set
            tags: Optional tags
        """
        metric = self._metrics.get(name)
        if metric and metric.type == MetricType.GAUGE:
            metric.set(value, tags)
        else:
            logger.warning(f"Attempted to set unknown or non-gauge metric: {name}")
    
    def time_operation(self, name: str, tags: Optional[Dict[str, str]] = None):
        """
        Time an operation using context manager.
        
        Args:
            name: Timer metric name
            tags: Optional tags
            
        Returns:
            Timer context manager
        """
        metric = self.timer(name, f"Timer for {name}")
        return metric.time_block(tags)
    
    async def start_monitoring(self, interval: float = 5.0) -> None:
        """
        Start automatic system monitoring.
        
        Args:
            interval: Monitoring interval in seconds
        """
        if self._monitoring:
            return
        
        self._monitor_interval = interval
        self._monitoring = True
        self._system_monitor_task = asyncio.create_task(self._monitor_system())
        
        logger.info(f"Started system monitoring (interval: {interval}s)")
    
    async def stop_monitoring(self) -> None:
        """Stop automatic system monitoring."""
        if not self._monitoring:
            return
        
        self._monitoring = False
        
        if self._system_monitor_task:
            self._system_monitor_task.cancel()
            try:
                await self._system_monitor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Stopped system monitoring")
    
    async def _monitor_system(self) -> None:
        """Monitor system resources periodically."""
        try:
            while self._monitoring:
                await self._collect_system_metrics()
                await asyncio.sleep(self._monitor_interval)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in system monitoring: {e}")
    
    async def _collect_system_metrics(self) -> None:
        """Collect current system metrics."""
        try:
            process = psutil.Process()
            
            # CPU usage
            cpu_percent = process.cpu_percent()
            self.set_gauge("system.cpu_percent", cpu_percent)
            
            # Memory usage
            memory_info = process.memory_info()
            memory_percent = process.memory_percent()
            memory_mb = memory_info.rss / 1024 / 1024
            
            self.set_gauge("system.memory_percent", memory_percent)
            self.set_gauge("system.memory_mb", memory_mb)
            
            # I/O counters
            try:
                io_counters = process.io_counters()
                self.set_gauge("system.disk_io_read_mb", io_counters.read_bytes / 1024 / 1024)
                self.set_gauge("system.disk_io_write_mb", io_counters.write_bytes / 1024 / 1024)
            except Exception:
                # I/O counters may not be available on all platforms
                pass
            
            # Network I/O (system-wide)
            try:
                net_io = psutil.net_io_counters()
                if net_io:
                    self.set_gauge("system.network_io_recv_mb", net_io.bytes_recv / 1024 / 1024)
                    self.set_gauge("system.network_io_sent_mb", net_io.bytes_sent / 1024 / 1024)
            except Exception:
                pass
            
        except Exception as e:
            logger.warning(f"Failed to collect system metrics: {e}")
    
    def get_all_summaries(self) -> Dict[str, MetricSummary]:
        """Get summaries for all metrics."""
        return {
            name: metric.get_summary()
            for name, metric in self._metrics.items()
        }
    
    def export_metrics(self, output_file: Optional[Path] = None) -> Dict[str, Any]:
        """
        Export all metrics to a dictionary or file.
        
        Args:
            output_file: Optional file to write metrics to
            
        Returns:
            Dictionary containing all metric data
        """
        export_data = {
            "timestamp": time.time(),
            "metrics": {}
        }
        
        for name, metric in self._metrics.items():
            summary = metric.get_summary()
            recent_values = metric.get_recent_values(50)  # Last 50 values
            
            export_data["metrics"][name] = {
                "type": metric.type.value,
                "description": metric.description,
                "summary": {
                    "count": summary.count,
                    "sum": summary.sum,
                    "min": summary.min if summary.min != float('inf') else None,
                    "max": summary.max if summary.max != float('-inf') else None,
                    "avg": summary.avg,
                    "p50": summary.p50,
                    "p95": summary.p95,
                    "p99": summary.p99
                },
                "recent_values": [
                    {
                        "value": v.value,
                        "timestamp": v.timestamp,
                        "tags": v.tags
                    }
                    for v in recent_values
                ]
            }
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(export_data, f, indent=2)
            logger.info(f"Exported metrics to {output_file}")
        
        return export_data
    
    def clear_all(self) -> None:
        """Clear all metrics."""
        for metric in self._metrics.values():
            metric.clear()
        
        logger.info("Cleared all metrics")


# Global metrics collector instance
_global_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance."""
    return _global_collector


def record_metric(name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
    """Convenience function for recording metrics."""
    _global_collector.record(name, value, tags)


def increment_counter(name: str, amount: float = 1.0, 
                     tags: Optional[Dict[str, str]] = None) -> None:
    """Convenience function for incrementing counters."""
    _global_collector.increment(name, amount, tags)


def set_gauge(name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
    """Convenience function for setting gauges."""
    _global_collector.set_gauge(name, value, tags)


def time_operation(name: str, tags: Optional[Dict[str, str]] = None):
    """Convenience function for timing operations."""
    return _global_collector.time_operation(name, tags)