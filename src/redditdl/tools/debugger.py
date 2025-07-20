#!/usr/bin/env python3
"""
RedditDL Plugin Debugger

Advanced debugging tools for plugin development and troubleshooting.
Provides tracing, profiling, inspection, and diagnostic capabilities
for RedditDL plugins.

Features:
- Plugin execution tracing
- Performance profiling
- State inspection
- Error analysis
- Interactive debugging
- Plugin isolation testing

Author: RedditDL Plugin Development Kit
License: MIT
Version: 1.0.0
"""

import sys
import time
import inspect
import logging
import traceback
import cProfile
import pstats
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from pathlib import Path
from contextlib import contextmanager
from functools import wraps
import json
import io

logger = logging.getLogger(__name__)

@dataclass
class PluginExecutionTrace:
    """Represents a single plugin execution trace."""
    plugin_name: str
    method: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    input_data: Any = None
    output_data: Any = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    memory_usage: Optional[int] = None
    call_stack: List[str] = field(default_factory=list)

@dataclass
class PluginPerformanceMetrics:
    """Performance metrics for a plugin."""
    plugin_name: str
    total_execution_time: float = 0.0
    call_count: int = 0
    average_execution_time: float = 0.0
    max_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    memory_peak: int = 0
    errors_count: int = 0
    warnings_count: int = 0

class PluginDebugger:
    """
    Advanced plugin debugging and profiling tool.
    
    Provides comprehensive debugging capabilities for RedditDL plugins
    including execution tracing, performance profiling, and state inspection.
    """
    
    def __init__(self, enable_profiling: bool = True, trace_level: str = "INFO"):
        """
        Initialize the plugin debugger.
        
        Args:
            enable_profiling: Enable performance profiling
            trace_level: Logging level for traces (DEBUG, INFO, WARNING, ERROR)
        """
        self.enable_profiling = enable_profiling
        self.trace_level = getattr(logging, trace_level.upper())
        
        # Storage for debugging data
        self.traces: List[PluginExecutionTrace] = []
        self.performance_metrics: Dict[str, PluginPerformanceMetrics] = {}
        self.plugin_states: Dict[str, Dict[str, Any]] = {}
        self.error_history: List[Dict[str, Any]] = []
        
        # Profiling data
        self.profiler: Optional[cProfile.Profile] = None
        self.profiling_stats: Optional[pstats.Stats] = None
        
        # Setup logging
        self._setup_debug_logging()
        
        logger.info("PluginDebugger initialized")
    
    def _setup_debug_logging(self) -> None:
        """Setup debug-specific logging configuration."""
        # Create debug logger
        self.debug_logger = logging.getLogger('redditdl.plugin_debugger')
        self.debug_logger.setLevel(self.trace_level)
        
        # Create console handler if not exists
        if not self.debug_logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '[PLUGIN_DEBUG] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.debug_logger.addHandler(handler)
    
    def trace_plugin_execution(self, plugin_name: str):
        """
        Decorator to trace plugin method execution.
        
        Args:
            plugin_name: Name of the plugin being traced
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await self._execute_with_trace(
                    func, plugin_name, func.__name__, True, *args, **kwargs
                )
            
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                return self._execute_with_trace(
                    func, plugin_name, func.__name__, False, *args, **kwargs
                )
            
            # Return appropriate wrapper based on function type
            if inspect.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
        
        return decorator
    
    async def _execute_with_trace(
        self, 
        func: Callable, 
        plugin_name: str, 
        method_name: str, 
        is_async: bool,
        *args, 
        **kwargs
    ) -> Any:
        """Execute function with comprehensive tracing."""
        trace = PluginExecutionTrace(
            plugin_name=plugin_name,
            method=method_name,
            start_time=time.time(),
            call_stack=self._get_call_stack()
        )
        
        # Capture input data (with size limits)
        trace.input_data = self._serialize_data(args, kwargs)
        
        # Start memory tracking
        initial_memory = self._get_memory_usage()
        
        try:
            # Start profiling if enabled
            if self.enable_profiling:
                self._start_profiling()
            
            self.debug_logger.info(f"Starting {plugin_name}.{method_name}")
            
            # Execute function
            if is_async:
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Capture output data
            trace.output_data = self._serialize_data(result)
            
            self.debug_logger.info(f"Completed {plugin_name}.{method_name}")
            
            return result
            
        except Exception as e:
            # Capture error information
            error_msg = f"{type(e).__name__}: {str(e)}"
            trace.errors.append(error_msg)
            
            # Log detailed error information
            error_info = {
                'plugin_name': plugin_name,
                'method': method_name,
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc(),
                'timestamp': time.time()
            }
            self.error_history.append(error_info)
            
            self.debug_logger.error(f"Error in {plugin_name}.{method_name}: {error_msg}")
            
            raise
            
        finally:
            # Complete trace
            trace.end_time = time.time()
            trace.duration = trace.end_time - trace.start_time
            trace.memory_usage = self._get_memory_usage() - initial_memory
            
            # Stop profiling
            if self.enable_profiling:
                self._stop_profiling()
            
            # Store trace
            self.traces.append(trace)
            
            # Update performance metrics
            self._update_performance_metrics(trace)
    
    def _execute_with_trace_sync(
        self, 
        func: Callable, 
        plugin_name: str, 
        method_name: str, 
        *args, 
        **kwargs
    ) -> Any:
        """Synchronous version of trace execution."""
        trace = PluginExecutionTrace(
            plugin_name=plugin_name,
            method=method_name,
            start_time=time.time(),
            call_stack=self._get_call_stack()
        )
        
        trace.input_data = self._serialize_data(args, kwargs)
        initial_memory = self._get_memory_usage()
        
        try:
            if self.enable_profiling:
                self._start_profiling()
            
            self.debug_logger.info(f"Starting {plugin_name}.{method_name}")
            
            result = func(*args, **kwargs)
            trace.output_data = self._serialize_data(result)
            
            self.debug_logger.info(f"Completed {plugin_name}.{method_name}")
            
            return result
            
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            trace.errors.append(error_msg)
            
            error_info = {
                'plugin_name': plugin_name,
                'method': method_name,
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc(),
                'timestamp': time.time()
            }
            self.error_history.append(error_info)
            
            self.debug_logger.error(f"Error in {plugin_name}.{method_name}: {error_msg}")
            
            raise
            
        finally:
            trace.end_time = time.time()
            trace.duration = trace.end_time - trace.start_time
            trace.memory_usage = self._get_memory_usage() - initial_memory
            
            if self.enable_profiling:
                self._stop_profiling()
            
            self.traces.append(trace)
            self._update_performance_metrics(trace)
    
    def _get_call_stack(self) -> List[str]:
        """Get current call stack."""
        stack = []
        frame = inspect.currentframe()
        
        try:
            # Skip debugger frames
            while frame and len(stack) < 10:
                filename = frame.f_code.co_filename
                function_name = frame.f_code.co_name
                line_number = frame.f_lineno
                
                if 'debugger.py' not in filename:
                    stack.append(f"{Path(filename).name}:{function_name}:{line_number}")
                
                frame = frame.f_back
        finally:
            del frame
        
        return stack
    
    def _serialize_data(self, *data) -> Dict[str, Any]:
        """Serialize data for storage with size limits."""
        try:
            serialized = {}
            for i, item in enumerate(data):
                # Convert to JSON-serializable format
                if hasattr(item, '__dict__'):
                    item_data = {
                        'type': type(item).__name__,
                        'data': str(item)[:1000]  # Limit size
                    }
                elif isinstance(item, (dict, list, tuple)):
                    item_str = str(item)
                    item_data = item_str[:1000] if len(item_str) > 1000 else item
                else:
                    item_data = str(item)[:1000]
                
                serialized[f'arg_{i}'] = item_data
            
            return serialized
        except Exception:
            return {'serialization_error': 'Failed to serialize data'}
    
    def _get_memory_usage(self) -> int:
        """Get current memory usage in bytes."""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return process.memory_info().rss
        except ImportError:
            return 0
    
    def _start_profiling(self) -> None:
        """Start performance profiling."""
        if not self.profiler:
            self.profiler = cProfile.Profile()
        self.profiler.enable()
    
    def _stop_profiling(self) -> None:
        """Stop performance profiling."""
        if self.profiler:
            self.profiler.disable()
    
    def _update_performance_metrics(self, trace: PluginExecutionTrace) -> None:
        """Update performance metrics with trace data."""
        plugin_name = trace.plugin_name
        
        if plugin_name not in self.performance_metrics:
            self.performance_metrics[plugin_name] = PluginPerformanceMetrics(plugin_name)
        
        metrics = self.performance_metrics[plugin_name]
        
        if trace.duration:
            metrics.total_execution_time += trace.duration
            metrics.call_count += 1
            metrics.average_execution_time = metrics.total_execution_time / metrics.call_count
            metrics.max_execution_time = max(metrics.max_execution_time, trace.duration)
            metrics.min_execution_time = min(metrics.min_execution_time, trace.duration)
        
        if trace.memory_usage:
            metrics.memory_peak = max(metrics.memory_peak, trace.memory_usage)
        
        if trace.errors:
            metrics.errors_count += len(trace.errors)
        
        if trace.warnings:
            metrics.warnings_count += len(trace.warnings)
    
    def capture_plugin_state(self, plugin_name: str, plugin_instance: Any) -> None:
        """Capture current state of a plugin."""
        try:
            state = {}
            
            # Capture basic attributes
            for attr_name in dir(plugin_instance):
                if not attr_name.startswith('_'):
                    try:
                        attr_value = getattr(plugin_instance, attr_name)
                        if not callable(attr_value):
                            state[attr_name] = str(attr_value)[:500]  # Limit size
                    except Exception:
                        state[attr_name] = '<unable_to_access>'
            
            # Capture configuration if available
            if hasattr(plugin_instance, 'config'):
                state['_config'] = str(plugin_instance.config)[:1000]
            
            self.plugin_states[plugin_name] = state
            
        except Exception as e:
            logger.warning(f"Failed to capture state for {plugin_name}: {e}")
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of plugin execution traces."""
        summary = {
            'total_traces': len(self.traces),
            'unique_plugins': len(set(trace.plugin_name for trace in self.traces)),
            'total_errors': sum(len(trace.errors) for trace in self.traces),
            'total_execution_time': sum(trace.duration or 0 for trace in self.traces),
            'plugins': {}
        }
        
        # Group traces by plugin
        for trace in self.traces:
            plugin_name = trace.plugin_name
            if plugin_name not in summary['plugins']:
                summary['plugins'][plugin_name] = {
                    'call_count': 0,
                    'total_time': 0,
                    'errors': 0,
                    'methods': set()
                }
            
            plugin_summary = summary['plugins'][plugin_name]
            plugin_summary['call_count'] += 1
            plugin_summary['total_time'] += trace.duration or 0
            plugin_summary['errors'] += len(trace.errors)
            plugin_summary['methods'].add(trace.method)
        
        # Convert sets to lists for JSON serialization
        for plugin_data in summary['plugins'].values():
            plugin_data['methods'] = list(plugin_data['methods'])
        
        return summary
    
    def get_performance_report(self) -> str:
        """Generate detailed performance report."""
        if not self.performance_metrics:
            return "No performance data available."
        
        report_lines = [
            "Plugin Performance Report",
            "=" * 50,
            ""
        ]
        
        # Sort plugins by total execution time
        sorted_plugins = sorted(
            self.performance_metrics.items(),
            key=lambda x: x[1].total_execution_time,
            reverse=True
        )
        
        for plugin_name, metrics in sorted_plugins:
            report_lines.extend([
                f"Plugin: {plugin_name}",
                f"  Total Execution Time: {metrics.total_execution_time:.4f}s",
                f"  Call Count: {metrics.call_count}",
                f"  Average Time: {metrics.average_execution_time:.4f}s",
                f"  Max Time: {metrics.max_execution_time:.4f}s",
                f"  Min Time: {metrics.min_execution_time:.4f}s",
                f"  Memory Peak: {metrics.memory_peak} bytes",
                f"  Errors: {metrics.errors_count}",
                f"  Warnings: {metrics.warnings_count}",
                ""
            ])
        
        return "\n".join(report_lines)
    
    def get_error_analysis(self) -> Dict[str, Any]:
        """Analyze plugin errors and provide insights."""
        if not self.error_history:
            return {"message": "No errors recorded"}
        
        analysis = {
            'total_errors': len(self.error_history),
            'errors_by_plugin': {},
            'errors_by_type': {},
            'most_recent_errors': self.error_history[-5:],  # Last 5 errors
            'error_patterns': []
        }
        
        # Group errors by plugin and type
        for error in self.error_history:
            plugin_name = error['plugin_name']
            error_type = error['error_type']
            
            analysis['errors_by_plugin'][plugin_name] = analysis['errors_by_plugin'].get(plugin_name, 0) + 1
            analysis['errors_by_type'][error_type] = analysis['errors_by_type'].get(error_type, 0) + 1
        
        # Identify patterns
        if analysis['errors_by_type']:
            most_common_error = max(analysis['errors_by_type'].items(), key=lambda x: x[1])
            analysis['error_patterns'].append(f"Most common error: {most_common_error[0]} ({most_common_error[1]} occurrences)")
        
        if analysis['errors_by_plugin']:
            most_problematic_plugin = max(analysis['errors_by_plugin'].items(), key=lambda x: x[1])
            analysis['error_patterns'].append(f"Most problematic plugin: {most_problematic_plugin[0]} ({most_problematic_plugin[1]} errors)")
        
        return analysis
    
    def export_debug_data(self, output_path: str) -> None:
        """Export all debug data to a file."""
        debug_data = {
            'summary': self.get_execution_summary(),
            'performance_metrics': {
                name: {
                    'plugin_name': metrics.plugin_name,
                    'total_execution_time': metrics.total_execution_time,
                    'call_count': metrics.call_count,
                    'average_execution_time': metrics.average_execution_time,
                    'max_execution_time': metrics.max_execution_time,
                    'min_execution_time': metrics.min_execution_time,
                    'memory_peak': metrics.memory_peak,
                    'errors_count': metrics.errors_count,
                    'warnings_count': metrics.warnings_count
                }
                for name, metrics in self.performance_metrics.items()
            },
            'plugin_states': self.plugin_states,
            'error_analysis': self.get_error_analysis(),
            'traces': [
                {
                    'plugin_name': trace.plugin_name,
                    'method': trace.method,
                    'duration': trace.duration,
                    'errors': trace.errors,
                    'warnings': trace.warnings,
                    'memory_usage': trace.memory_usage
                }
                for trace in self.traces[-100:]  # Last 100 traces
            ]
        }
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, indent=2, default=str)
        
        logger.info(f"Debug data exported to: {output_path}")
    
    def generate_profiling_report(self) -> str:
        """Generate profiling report from cProfile data."""
        if not self.profiler:
            return "No profiling data available."
        
        # Get stats from profiler
        stats_stream = io.StringIO()
        stats = pstats.Stats(self.profiler, stream=stats_stream)
        stats.sort_stats('cumulative')
        stats.print_stats(20)  # Top 20 functions
        
        return stats_stream.getvalue()
    
    @contextmanager
    def plugin_isolation(self, plugin_name: str):
        """Context manager for isolated plugin testing."""
        old_traces_count = len(self.traces)
        old_errors_count = len(self.error_history)
        
        self.debug_logger.info(f"Starting isolated test for plugin: {plugin_name}")
        
        try:
            yield self
        finally:
            new_traces = self.traces[old_traces_count:]
            new_errors = self.error_history[old_errors_count:]
            
            self.debug_logger.info(
                f"Isolated test complete for {plugin_name}: "
                f"{len(new_traces)} traces, {len(new_errors)} errors"
            )

# Utility functions for debugging
def debug_plugin_method(plugin_name: str, debugger: PluginDebugger = None):
    """Convenience decorator for debugging plugin methods."""
    if debugger is None:
        debugger = PluginDebugger()
    
    return debugger.trace_plugin_execution(plugin_name)

def create_debug_session(output_dir: str = "./debug_output") -> PluginDebugger:
    """Create a new debug session with output directory."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    debugger = PluginDebugger(enable_profiling=True)
    
    # Setup session logging
    session_log = Path(output_dir) / f"debug_session_{int(time.time())}.log"
    handler = logging.FileHandler(session_log)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    debugger.debug_logger.addHandler(handler)
    
    logger.info(f"Debug session started. Logs: {session_log}")
    
    return debugger

# Example usage
if __name__ == "__main__":
    # Demonstrate debugging capabilities
    debugger = create_debug_session()
    
    # Example plugin class for testing
    class ExamplePlugin:
        def __init__(self):
            self.name = "example_plugin"
            self.config = {"setting": "value"}
        
        @debug_plugin_method("example_plugin", debugger)
        def process_data(self, data):
            """Example method that processes data."""
            # Simulate processing
            time.sleep(0.1)
            return {"processed": data, "timestamp": time.time()}
        
        @debug_plugin_method("example_plugin", debugger)
        def process_with_error(self, data):
            """Example method that demonstrates error handling."""
            if data == "error":
                raise ValueError("Simulated error for testing")
            return {"result": "success"}
    
    # Test the plugin
    plugin = ExamplePlugin()
    
    print("Testing plugin debugging...")
    
    # Test successful execution
    result1 = plugin.process_data("test_data")
    print(f"Result 1: {result1}")
    
    # Test error handling
    try:
        plugin.process_with_error("error")
    except ValueError:
        pass  # Expected error
    
    # Capture plugin state
    debugger.capture_plugin_state("example_plugin", plugin)
    
    # Generate reports
    print("\nPerformance Report:")
    print(debugger.get_performance_report())
    
    print("\nError Analysis:")
    print(json.dumps(debugger.get_error_analysis(), indent=2))
    
    # Export debug data
    debugger.export_debug_data("./debug_output/debug_data.json")
    
    print("\nDebug session complete!")