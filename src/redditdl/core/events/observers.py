"""
Standard Observers for RedditDL Event System

Provides a collection of standard observers for different output formats
and monitoring needs in the RedditDL pipeline.
"""

import json
import logging
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO, Union

try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn
    from rich.table import Table
    from rich.live import Live
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from redditdl.core.events.types import (
    BaseEvent, EventType, PostDiscoveredEvent, DownloadStartedEvent,
    DownloadProgressEvent, DownloadCompletedEvent, PostProcessedEvent,
    FilterAppliedEvent, PipelineStageEvent, ErrorEvent, StatisticsEvent
)


class Observer(ABC):
    """
    Abstract base class for all event observers.
    
    Observers receive events from the EventEmitter and process them
    according to their specific purpose (logging, progress tracking, etc.).
    """
    
    def __init__(self, name: str):
        """Initialize observer with a name for identification."""
        self.name = name
        self.enabled = True
        self.statistics = {
            'events_received': 0,
            'events_processed': 0,
            'events_errored': 0,
            'last_event_time': None
        }
    
    @abstractmethod
    def handle_event(self, event: EventType) -> None:
        """
        Handle an incoming event.
        
        Args:
            event: Event instance to process
        """
        pass
    
    def __call__(self, event: EventType) -> None:
        """
        Allow observer to be called directly as a function.
        
        This enables easy subscription to EventEmitter.
        """
        if not self.enabled:
            return
        
        try:
            self.statistics['events_received'] += 1
            self.statistics['last_event_time'] = time.time()
            
            self.handle_event(event)
            
            self.statistics['events_processed'] += 1
            
        except Exception as e:
            self.statistics['events_errored'] += 1
            logging.error(f"Observer {self.name} error: {e}")
    
    def enable(self) -> None:
        """Enable this observer."""
        self.enabled = True
    
    def disable(self) -> None:
        """Disable this observer."""
        self.enabled = False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get observer statistics."""
        return {
            'name': self.name,
            'enabled': self.enabled,
            **self.statistics
        }


class ConsoleObserver(Observer):
    """
    Observer that outputs events to console with optional rich formatting.
    
    Provides enhanced console output with progress indicators and colored text.
    """
    
    def __init__(self, 
                 name: str = "console",
                 verbose: bool = True,
                 use_rich: bool = None,
                 show_timestamps: bool = True):
        """
        Initialize console observer.
        
        Args:
            name: Observer name
            verbose: Show detailed event information
            use_rich: Use rich formatting if available (auto-detect if None)
            show_timestamps: Include timestamps in output
        """
        super().__init__(name)
        self.verbose = verbose
        self.show_timestamps = show_timestamps
        
        # Auto-detect rich availability
        if use_rich is None:
            use_rich = RICH_AVAILABLE
        
        self.use_rich = use_rich and RICH_AVAILABLE
        
        if self.use_rich:
            self.console = Console()
        else:
            self.console = None
    
    def handle_event(self, event: EventType) -> None:
        """Handle event by printing to console."""
        if isinstance(event, PostDiscoveredEvent):
            self._handle_post_discovered(event)
        elif isinstance(event, DownloadStartedEvent):
            self._handle_download_started(event)
        elif isinstance(event, DownloadProgressEvent):
            self._handle_download_progress(event)
        elif isinstance(event, DownloadCompletedEvent):
            self._handle_download_completed(event)
        elif isinstance(event, PostProcessedEvent):
            self._handle_post_processed(event)
        elif isinstance(event, FilterAppliedEvent):
            self._handle_filter_applied(event)
        elif isinstance(event, PipelineStageEvent):
            self._handle_pipeline_stage(event)
        elif isinstance(event, ErrorEvent):
            self._handle_error(event)
        elif isinstance(event, StatisticsEvent):
            self._handle_statistics(event)
        else:
            self._handle_generic(event)
    
    def _handle_post_discovered(self, event: PostDiscoveredEvent) -> None:
        """Handle post discovery events."""
        message = f"📥 Discovered {event.post_count} posts from {event.source_type}: {event.target}"
        self._print(message, style="blue")
    
    def _handle_download_started(self, event: DownloadStartedEvent) -> None:
        """Handle download start events."""
        size_info = f" ({event.expected_size} bytes)" if event.expected_size else ""
        message = f"⬇️  Starting download: {event.filename}{size_info}"
        if self.verbose:
            message += f"\n   URL: {event.url}"
        self._print(message, style="cyan")
    
    def _handle_download_progress(self, event: DownloadProgressEvent) -> None:
        """Handle download progress events."""
        if not self.verbose:
            return  # Skip progress events in non-verbose mode
        
        progress_info = ""
        if event.progress_percentage:
            progress_info = f" ({event.progress_percentage:.1f}%)"
        
        speed_info = ""
        if event.download_speed > 0:
            speed_info = f" @ {event.download_speed / 1024:.1f} KB/s"
        
        message = f"📊 {event.filename}: {event.bytes_downloaded} bytes{progress_info}{speed_info}"
        self._print(message, style="dim")
    
    def _handle_download_completed(self, event: DownloadCompletedEvent) -> None:
        """Handle download completion events."""
        if event.success:
            speed_info = f" @ {event.average_speed / 1024:.1f} KB/s" if event.average_speed > 0 else ""
            message = f"✅ Downloaded: {event.filename} ({event.file_size} bytes){speed_info}"
            self._print(message, style="green")
        else:
            message = f"❌ Download failed: {event.filename}"
            if event.error_message:
                message += f" - {event.error_message}"
            self._print(message, style="red")
    
    def _handle_post_processed(self, event: PostProcessedEvent) -> None:
        """Handle post processing events."""
        if event.success:
            ops_info = f" ({', '.join(event.operations_performed)})" if event.operations_performed else ""
            message = f"🔄 Processed: {event.post_title[:50]}...{ops_info}"
            self._print(message, style="green")
        else:
            message = f"❌ Processing failed: {event.post_title[:50]}..."
            if event.error_message:
                message += f" - {event.error_message}"
            self._print(message, style="red")
    
    def _handle_filter_applied(self, event: FilterAppliedEvent) -> None:
        """Handle filter application events."""
        message = f"🔍 Filter '{event.filter_type}': {event.posts_before} → {event.posts_after} posts ({event.posts_filtered} filtered)"
        self._print(message, style="yellow")
    
    def _handle_pipeline_stage(self, event: PipelineStageEvent) -> None:
        """Handle pipeline stage events."""
        if event.stage_status == 'started':
            message = f"🚀 Starting stage: {event.stage_name}"
            self._print(message, style="blue")
        elif event.stage_status == 'completed':
            message = f"✅ Completed stage: {event.stage_name} ({event.execution_time:.2f}s)"
            if event.posts_processed > 0:
                message += f" - {event.posts_successful}/{event.posts_processed} posts successful"
            self._print(message, style="green")
        elif event.stage_status == 'failed':
            message = f"❌ Stage failed: {event.stage_name} - {event.error_message}"
            self._print(message, style="red")
        elif event.stage_status == 'skipped':
            message = f"⏭️  Skipped stage: {event.stage_name}"
            self._print(message, style="dim")
    
    def _handle_error(self, event: ErrorEvent) -> None:
        """Handle error events."""
        message = f"🚨 {event.error_type}: {event.error_message}"
        if event.error_context:
            message += f" (in {event.error_context})"
        if event.recoverable:
            message += " [Recoverable]"
        self._print(message, style="red bold")
    
    def _handle_statistics(self, event: StatisticsEvent) -> None:
        """Handle statistics events."""
        if not self.verbose:
            return  # Skip statistics in non-verbose mode
        
        message = (f"📈 Stats: {event.posts_processed}/{event.total_posts} posts, "
                  f"{event.downloads_completed} downloads, "
                  f"{event.success_rate:.1f}% success rate")
        self._print(message, style="blue")
    
    def _handle_generic(self, event: BaseEvent) -> None:
        """Handle generic/unknown events."""
        if self.verbose:
            message = f"📋 {event.event_type}: {event.to_dict()}"
            self._print(message, style="dim")
    
    def _print(self, message: str, style: Optional[str] = None) -> None:
        """Print message with optional styling and timestamp."""
        if self.show_timestamps:
            timestamp = datetime.now().strftime("%H:%M:%S")
            message = f"[{timestamp}] {message}"
        
        if self.use_rich and self.console:
            self.console.print(message, style=style)
        else:
            print(message)


class LoggingObserver(Observer):
    """
    Observer that writes events to log files with structured formatting.
    
    Provides detailed logging of all events for debugging and auditing.
    """
    
    def __init__(self, 
                 name: str = "logging",
                 log_file: Optional[Union[str, Path]] = None,
                 log_level: int = logging.INFO,
                 format_string: str = None):
        """
        Initialize logging observer.
        
        Args:
            name: Observer name
            log_file: Path to log file (None for default logger)
            log_level: Minimum log level to record
            format_string: Custom log format string
        """
        super().__init__(name)
        self.log_file = Path(log_file) if log_file else None
        self.log_level = log_level
        
        # Set up logger
        self.logger = logging.getLogger(f"redditdl.events.{name}")
        self.logger.setLevel(log_level)
        
        # Add file handler if log file specified
        if self.log_file:
            handler = logging.FileHandler(self.log_file)
            formatter = logging.Formatter(
                format_string or 
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def handle_event(self, event: EventType) -> None:
        """Handle event by writing to log."""
        # Determine log level based on event type
        if isinstance(event, ErrorEvent):
            level = logging.ERROR
        elif isinstance(event, (DownloadCompletedEvent, PostProcessedEvent)) and not event.success:
            level = logging.WARNING
        elif isinstance(event, DownloadProgressEvent):
            level = logging.DEBUG  # Progress events are verbose
        else:
            level = logging.INFO
        
        if level >= self.log_level:
            message = self._format_event_message(event)
            self.logger.log(level, message)
    
    def _format_event_message(self, event: EventType) -> str:
        """Format event as a log message."""
        base_info = f"[{event.event_type}] {event.event_id}"
        
        if isinstance(event, PostDiscoveredEvent):
            return f"{base_info} Discovered {event.post_count} posts from {event.target}"
        elif isinstance(event, DownloadStartedEvent):
            return f"{base_info} Starting download: {event.filename} from {event.url}"
        elif isinstance(event, DownloadProgressEvent):
            progress = event.progress_percentage or 0
            return f"{base_info} Download progress: {event.filename} {progress:.1f}%"
        elif isinstance(event, DownloadCompletedEvent):
            status = "SUCCESS" if event.success else "FAILED"
            return f"{base_info} Download {status}: {event.filename} ({event.file_size} bytes)"
        elif isinstance(event, PostProcessedEvent):
            status = "SUCCESS" if event.success else "FAILED"
            return f"{base_info} Processing {status}: {event.post_title}"
        elif isinstance(event, FilterAppliedEvent):
            return f"{base_info} Filter applied: {event.filter_type} - {event.posts_filtered} posts filtered"
        elif isinstance(event, PipelineStageEvent):
            return f"{base_info} Stage {event.stage_status}: {event.stage_name}"
        elif isinstance(event, ErrorEvent):
            return f"{base_info} ERROR: {event.error_message} (in {event.error_context})"
        else:
            return f"{base_info} Generic event: {event.to_dict()}"


class StatisticsObserver(Observer):
    """
    Observer that collects and aggregates statistics about pipeline execution.
    
    Provides real-time metrics and performance monitoring.
    """
    
    def __init__(self, name: str = "statistics"):
        """Initialize statistics observer."""
        super().__init__(name)
        self.stats = {
            'session_start': time.time(),
            'posts_discovered': 0,
            'downloads_started': 0,
            'downloads_completed': 0,
            'downloads_failed': 0,
            'posts_processed': 0,
            'posts_successful': 0,
            'posts_failed': 0,
            'filters_applied': 0,
            'total_bytes_downloaded': 0,
            'total_download_time': 0.0,
            'stages_completed': 0,
            'stages_failed': 0,
            'errors_occurred': 0
        }
        
        # Detailed tracking
        self.download_speeds = []
        self.processing_times = []
        self.error_types = defaultdict(int)
        self.stage_times = {}
        
        # Thread safety
        self._lock = threading.RLock()
    
    def handle_event(self, event: EventType) -> None:
        """Handle event by updating statistics."""
        with self._lock:
            if isinstance(event, PostDiscoveredEvent):
                self.stats['posts_discovered'] += event.post_count
            
            elif isinstance(event, DownloadStartedEvent):
                self.stats['downloads_started'] += 1
            
            elif isinstance(event, DownloadCompletedEvent):
                if event.success:
                    self.stats['downloads_completed'] += 1
                    self.stats['total_bytes_downloaded'] += event.file_size
                    self.stats['total_download_time'] += event.duration_seconds
                    if event.average_speed > 0:
                        self.download_speeds.append(event.average_speed)
                else:
                    self.stats['downloads_failed'] += 1
            
            elif isinstance(event, PostProcessedEvent):
                self.stats['posts_processed'] += 1
                if event.success:
                    self.stats['posts_successful'] += 1
                    self.processing_times.append(event.processing_time)
                else:
                    self.stats['posts_failed'] += 1
            
            elif isinstance(event, FilterAppliedEvent):
                self.stats['filters_applied'] += 1
            
            elif isinstance(event, PipelineStageEvent):
                if event.stage_status == 'completed':
                    self.stats['stages_completed'] += 1
                    self.stage_times[event.stage_name] = event.execution_time
                elif event.stage_status == 'failed':
                    self.stats['stages_failed'] += 1
            
            elif isinstance(event, ErrorEvent):
                self.stats['errors_occurred'] += 1
                self.error_types[event.error_type] += 1
    
    def get_current_statistics(self) -> Dict[str, Any]:
        """Get current statistics snapshot."""
        with self._lock:
            elapsed_time = time.time() - self.stats['session_start']
            
            # Calculate derived statistics
            avg_download_speed = (
                sum(self.download_speeds) / len(self.download_speeds)
                if self.download_speeds else 0
            )
            
            avg_processing_time = (
                sum(self.processing_times) / len(self.processing_times)
                if self.processing_times else 0
            )
            
            success_rate = 0
            if self.stats['posts_processed'] > 0:
                success_rate = (self.stats['posts_successful'] / self.stats['posts_processed']) * 100
            
            return {
                **self.stats,
                'session_duration': elapsed_time,
                'average_download_speed': avg_download_speed,
                'average_processing_time': avg_processing_time,
                'success_rate': success_rate,
                'downloads_per_minute': (self.stats['downloads_completed'] / elapsed_time * 60) if elapsed_time > 0 else 0,
                'error_types': dict(self.error_types),
                'stage_times': dict(self.stage_times)
            }


class JSONObserver(Observer):
    """
    Observer that outputs events as JSON for machine-readable consumption.
    
    Useful for integration with external tools and monitoring systems.
    """
    
    def __init__(self, 
                 name: str = "json",
                 output_file: Optional[Union[str, Path, TextIO]] = None,
                 pretty_print: bool = False):
        """
        Initialize JSON observer.
        
        Args:
            name: Observer name
            output_file: File path or file object to write JSON (None for stdout)
            pretty_print: Format JSON with indentation
        """
        super().__init__(name)
        self.pretty_print = pretty_print
        
        if isinstance(output_file, (str, Path)):
            self.output_file = open(output_file, 'w')
            self.should_close_file = True
        else:
            self.output_file = output_file or sys.stdout
            self.should_close_file = False
        
        # Thread safety for file writing
        self._lock = threading.RLock()
    
    def handle_event(self, event: EventType) -> None:
        """Handle event by writing JSON to output."""
        try:
            event_data = event.to_dict()
            
            if self.pretty_print:
                json_str = json.dumps(event_data, indent=2, default=str)
            else:
                json_str = json.dumps(event_data, default=str)
            
            with self._lock:
                self.output_file.write(json_str + '\n')
                self.output_file.flush()
                
        except Exception as e:
            logging.error(f"JSONObserver error: {e}")
    
    def __del__(self):
        """Cleanup file handle when observer is destroyed."""
        if self.should_close_file and hasattr(self, 'output_file'):
            try:
                self.output_file.close()
            except:
                pass


class ProgressObserver(Observer):
    """
    Observer that provides rich progress tracking with multiple progress bars.
    
    Uses the Rich library for enhanced terminal output with real-time updates.
    """
    
    def __init__(self, 
                 name: str = "progress",
                 show_overall: bool = True,
                 show_individual: bool = True,
                 max_individual_bars: int = 5):
        """
        Initialize progress observer.
        
        Args:
            name: Observer name
            show_overall: Show overall pipeline progress
            show_individual: Show individual download progress bars
            max_individual_bars: Maximum number of individual progress bars
        """
        super().__init__(name)
        
        if not RICH_AVAILABLE:
            raise ImportError("Rich library required for ProgressObserver")
        
        self.show_overall = show_overall
        self.show_individual = show_individual
        self.max_individual_bars = max_individual_bars
        
        # Progress tracking
        self.console = Console()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        
        # Task tracking
        self.overall_task = None
        self.download_tasks = {}  # post_id -> task_id
        self.stage_task = None
        
        # Statistics
        self.total_posts = 0
        self.processed_posts = 0
        
        # Start progress display
        self.live = Live(self.progress, console=self.console, refresh_per_second=4)
        self.live.start()
    
    def handle_event(self, event: EventType) -> None:
        """Handle event by updating progress displays."""
        try:
            if isinstance(event, PostDiscoveredEvent):
                self._handle_posts_discovered(event)
            elif isinstance(event, DownloadStartedEvent):
                self._handle_download_started(event)
            elif isinstance(event, DownloadProgressEvent):
                self._handle_download_progress(event)
            elif isinstance(event, DownloadCompletedEvent):
                self._handle_download_completed(event)
            elif isinstance(event, PostProcessedEvent):
                self._handle_post_processed(event)
            elif isinstance(event, PipelineStageEvent):
                self._handle_pipeline_stage(event)
                
        except Exception as e:
            logging.error(f"ProgressObserver error: {e}")
    
    def _handle_posts_discovered(self, event: PostDiscoveredEvent) -> None:
        """Handle post discovery to set up overall progress."""
        self.total_posts = event.post_count
        if self.show_overall and self.overall_task is None:
            self.overall_task = self.progress.add_task(
                f"Processing {self.total_posts} posts",
                total=self.total_posts
            )
    
    def _handle_download_started(self, event: DownloadStartedEvent) -> None:
        """Handle download start to create individual progress bar."""
        if self.show_individual and len(self.download_tasks) < self.max_individual_bars:
            total = event.expected_size if event.expected_size else None
            task_id = self.progress.add_task(
                f"📥 {event.filename}",
                total=total
            )
            self.download_tasks[event.post_id] = task_id
    
    def _handle_download_progress(self, event: DownloadProgressEvent) -> None:
        """Handle download progress updates."""
        if event.post_id in self.download_tasks:
            task_id = self.download_tasks[event.post_id]
            self.progress.update(
                task_id,
                completed=event.bytes_downloaded,
                description=f"📥 {event.filename} @ {event.download_speed/1024:.1f} KB/s"
            )
    
    def _handle_download_completed(self, event: DownloadCompletedEvent) -> None:
        """Handle download completion."""
        if event.post_id in self.download_tasks:
            task_id = self.download_tasks[event.post_id]
            if event.success:
                self.progress.update(
                    task_id,
                    description=f"✅ {event.filename}",
                    completed=event.file_size
                )
            else:
                self.progress.update(
                    task_id,
                    description=f"❌ {event.filename} - {event.error_message}"
                )
            
            # Remove task after a short delay to show completion
            self.progress.remove_task(task_id)
            del self.download_tasks[event.post_id]
    
    def _handle_post_processed(self, event: PostProcessedEvent) -> None:
        """Handle post processing completion."""
        self.processed_posts += 1
        if self.overall_task is not None:
            self.progress.update(
                self.overall_task,
                completed=self.processed_posts,
                description=f"Processing posts ({self.processed_posts}/{self.total_posts})"
            )
    
    def _handle_pipeline_stage(self, event: PipelineStageEvent) -> None:
        """Handle pipeline stage events."""
        if event.stage_status == 'started':
            if self.stage_task is not None:
                self.progress.remove_task(self.stage_task)
            self.stage_task = self.progress.add_task(
                f"🚀 {event.stage_name}",
                total=None  # Indeterminate progress
            )
        elif event.stage_status in ['completed', 'failed', 'skipped']:
            if self.stage_task is not None:
                status_emoji = {'completed': '✅', 'failed': '❌', 'skipped': '⏭️'}
                self.progress.update(
                    self.stage_task,
                    description=f"{status_emoji.get(event.stage_status, '?')} {event.stage_name}"
                )
                self.progress.remove_task(self.stage_task)
                self.stage_task = None
    
    def shutdown(self) -> None:
        """Shutdown progress display."""
        try:
            self.live.stop()
        except:
            pass
    
    def __del__(self):
        """Cleanup when observer is destroyed."""
        self.shutdown()