"""
CLI Progress Observer for RedditDL

Provides enhanced progress tracking with rich UI components, ETA calculations,
and multiple output modes for CLI usage.
"""

import json
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO, Union

try:
    from rich.console import Console
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        Progress, SpinnerColumn, TextColumn, BarColumn, 
        TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn,
        MofNCompleteColumn, FileSizeColumn, TransferSpeedColumn
    )
    from rich.table import Table
    from rich.text import Text
    from rich.layout import Layout
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from redditdl.core.events.observers import Observer
from redditdl.core.events.types import (
    BaseEvent, PostDiscoveredEvent, DownloadStartedEvent,
    DownloadProgressEvent, DownloadCompletedEvent, PostProcessedEvent,
    FilterAppliedEvent, PipelineStageEvent, ErrorEvent, StatisticsEvent
)


class OutputMode(Enum):
    """Output mode enumeration."""
    NORMAL = "normal"
    QUIET = "quiet"
    VERBOSE = "verbose"
    JSON = "json"


class ProgressDisplay(Enum):
    """Progress display mode enumeration."""
    RICH = "rich"
    TQDM = "tqdm"
    SIMPLE = "simple"
    NONE = "none"


class CLIProgressObserver(Observer):
    """
    Enhanced CLI progress observer with multiple display modes and output options.
    
    Features:
    - Rich progress bars with ETA and speed metrics
    - Fallback to tqdm or simple progress display
    - Multiple output modes (quiet, verbose, JSON)
    - Live statistics and status updates
    - Customizable progress bar appearance
    """
    
    def __init__(self,
                 name: str = "cli_progress",
                 output_mode: OutputMode = OutputMode.NORMAL,
                 progress_display: Optional[ProgressDisplay] = None,
                 show_individual: bool = True,
                 max_individual_bars: int = 5,
                 show_eta: bool = True,
                 show_speed: bool = True,
                 show_statistics: bool = True,
                 quiet_mode: bool = False,
                 json_output: Optional[Union[str, Path, TextIO]] = None):
        """
        Initialize CLI progress observer.
        
        Args:
            name: Observer name
            output_mode: Output verbosity mode
            progress_display: Progress display type (auto-detect if None)
            show_individual: Show individual download progress bars
            max_individual_bars: Maximum number of concurrent progress bars
            show_eta: Show estimated time of arrival
            show_speed: Show download/processing speeds
            show_statistics: Show live statistics
            quiet_mode: Suppress most output
            json_output: File or stream for JSON output
        """
        super().__init__(name)
        
        # Configuration
        self.output_mode = output_mode
        self.show_individual = show_individual and not quiet_mode
        self.max_individual_bars = max_individual_bars
        self.show_eta = show_eta
        self.show_speed = show_speed
        self.show_statistics = show_statistics and not quiet_mode
        self.quiet_mode = quiet_mode
        
        # Auto-detect progress display if not specified
        if progress_display is None:
            if quiet_mode or output_mode == OutputMode.JSON:
                progress_display = ProgressDisplay.NONE
            elif RICH_AVAILABLE:
                progress_display = ProgressDisplay.RICH
            elif TQDM_AVAILABLE:
                progress_display = ProgressDisplay.TQDM
            else:
                progress_display = ProgressDisplay.SIMPLE
        
        self.progress_display = progress_display
        
        # Initialize display components
        self._setup_display()
        self._setup_json_output(json_output)
        
        # State tracking
        self.session_start = time.time()
        self.statistics = {
            'posts_discovered': 0,
            'posts_processed': 0,
            'downloads_started': 0,
            'downloads_completed': 0,
            'downloads_failed': 0,
            'bytes_downloaded': 0,
            'errors_occurred': 0,
            'current_stage': None,
            'stages_completed': [],
            'stages_failed': []
        }
        
        # Progress tracking
        self.overall_task = None
        self.download_tasks = {}  # post_id -> task info
        self.stage_task = None
        self.total_posts = 0
        
        # Performance tracking
        self.download_speeds = []
        self.stage_times = {}
        self.eta_calculator = ETACalculator()
        
        # Thread safety
        self._lock = threading.RLock()
    
    def _setup_display(self) -> None:
        """Set up appropriate display components."""
        if self.progress_display == ProgressDisplay.RICH and RICH_AVAILABLE:
            self._setup_rich_display()
        elif self.progress_display == ProgressDisplay.TQDM and TQDM_AVAILABLE:
            self._setup_tqdm_display()
        elif self.progress_display == ProgressDisplay.SIMPLE:
            self._setup_simple_display()
        else:
            self.console = None
            self.progress = None
            self.live = None
    
    def _setup_rich_display(self) -> None:
        """Set up Rich-based progress display."""
        self.console = Console()
        
        # Create progress bars with custom columns
        progress_columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
        ]
        
        if self.show_speed:
            progress_columns.append(TransferSpeedColumn())
        
        if self.show_eta:
            progress_columns.extend([
                TimeElapsedColumn(),
                TimeRemainingColumn()
            ])
        
        self.progress = Progress(*progress_columns, console=self.console)
        
        if self.show_statistics:
            # Create layout for progress + statistics
            self.layout = Layout()
            self.layout.split_column(
                Layout(self.progress, name="progress", ratio=3),
                Layout(name="stats", ratio=1)
            )
            self.live = Live(self.layout, console=self.console, refresh_per_second=4)
        else:
            self.live = Live(self.progress, console=self.console, refresh_per_second=4)
        
        self.live.start()
    
    def _setup_tqdm_display(self) -> None:
        """Set up tqdm-based progress display."""
        self.console = None
        self.progress = None
        self.tqdm_bars = {}
        self.live = None
    
    def _setup_simple_display(self) -> None:
        """Set up simple text-based progress display."""
        self.console = None
        self.progress = None
        self.live = None
        self.last_update = 0
    
    def _setup_json_output(self, json_output: Optional[Union[str, Path, TextIO]]) -> None:
        """Set up JSON output stream."""
        if json_output is None:
            self.json_file = None
            self.should_close_json = False
        elif isinstance(json_output, (str, Path)):
            self.json_file = open(json_output, 'w')
            self.should_close_json = True
        else:
            self.json_file = json_output
            self.should_close_json = False
    
    def handle_event(self, event: BaseEvent) -> None:
        """Handle event by updating appropriate display."""
        with self._lock:
            # Update statistics
            self._update_statistics(event)
            
            # Handle JSON output
            if self.output_mode == OutputMode.JSON:
                self._handle_json_output(event)
                return
            
            # Handle quiet mode
            if self.quiet_mode:
                self._handle_quiet_mode(event)
                return
            
            # Handle event based on display type
            if self.progress_display == ProgressDisplay.RICH:
                self._handle_rich_event(event)
            elif self.progress_display == ProgressDisplay.TQDM:
                self._handle_tqdm_event(event)
            elif self.progress_display == ProgressDisplay.SIMPLE:
                self._handle_simple_event(event)
    
    def _update_statistics(self, event: BaseEvent) -> None:
        """Update internal statistics based on event."""
        if isinstance(event, PostDiscoveredEvent):
            self.statistics['posts_discovered'] = event.post_count
            self.total_posts = event.post_count
        elif isinstance(event, DownloadStartedEvent):
            self.statistics['downloads_started'] += 1
        elif isinstance(event, DownloadCompletedEvent):
            if event.success:
                self.statistics['downloads_completed'] += 1
                self.statistics['bytes_downloaded'] += event.file_size
                if event.average_speed > 0:
                    self.download_speeds.append(event.average_speed)
            else:
                self.statistics['downloads_failed'] += 1
        elif isinstance(event, PostProcessedEvent):
            self.statistics['posts_processed'] += 1
        elif isinstance(event, PipelineStageEvent):
            if event.stage_status == 'started':
                self.statistics['current_stage'] = event.stage_name
            elif event.stage_status == 'completed':
                self.statistics['stages_completed'].append(event.stage_name)
                self.stage_times[event.stage_name] = event.execution_time
                self.statistics['current_stage'] = None
            elif event.stage_status == 'failed':
                self.statistics['stages_failed'].append(event.stage_name)
                self.statistics['current_stage'] = None
        elif isinstance(event, ErrorEvent):
            self.statistics['errors_occurred'] += 1
    
    def _handle_json_output(self, event: BaseEvent) -> None:
        """Handle JSON output mode."""
        event_data = {
            'timestamp': time.time(),
            'event': event.to_dict(),
            'statistics': self.get_current_statistics()
        }
        
        if self.json_file:
            json.dump(event_data, self.json_file, default=str)
            self.json_file.write('\n')
            self.json_file.flush()
        else:
            print(json.dumps(event_data, default=str))
    
    def _handle_quiet_mode(self, event: BaseEvent) -> None:
        """Handle quiet mode with minimal output."""
        if isinstance(event, ErrorEvent):
            print(f"Error: {event.error_message}", file=sys.stderr)
        elif isinstance(event, PipelineStageEvent) and event.stage_status == 'failed':
            print(f"Stage failed: {event.stage_name}", file=sys.stderr)
    
    def _handle_rich_event(self, event: BaseEvent) -> None:
        """Handle event with Rich display."""
        if isinstance(event, PostDiscoveredEvent):
            self._rich_posts_discovered(event)
        elif isinstance(event, DownloadStartedEvent):
            self._rich_download_started(event)
        elif isinstance(event, DownloadProgressEvent):
            self._rich_download_progress(event)
        elif isinstance(event, DownloadCompletedEvent):
            self._rich_download_completed(event)
        elif isinstance(event, PostProcessedEvent):
            self._rich_post_processed(event)
        elif isinstance(event, PipelineStageEvent):
            self._rich_pipeline_stage(event)
        elif isinstance(event, ErrorEvent):
            self._rich_error(event)
        
        # Update statistics display
        if self.show_statistics and hasattr(self, 'layout'):
            self._update_rich_statistics()
    
    def _rich_posts_discovered(self, event: PostDiscoveredEvent) -> None:
        """Handle post discovery with Rich display."""
        if self.overall_task is None:
            self.overall_task = self.progress.add_task(
                f"Processing {event.post_count} posts",
                total=event.post_count
            )
    
    def _rich_download_started(self, event: DownloadStartedEvent) -> None:
        """Handle download start with Rich display."""
        if self.show_individual and len(self.download_tasks) < self.max_individual_bars:
            total = event.expected_size if event.expected_size else None
            task_id = self.progress.add_task(
                f"📥 {event.filename}",
                total=total
            )
            self.download_tasks[event.post_id] = {
                'task_id': task_id,
                'filename': event.filename,
                'start_time': time.time()
            }
    
    def _rich_download_progress(self, event: DownloadProgressEvent) -> None:
        """Handle download progress with Rich display."""
        if event.post_id in self.download_tasks:
            task_info = self.download_tasks[event.post_id]
            task_id = task_info['task_id']
            
            description = f"📥 {event.filename}"
            if self.show_speed and event.download_speed > 0:
                description += f" @ {event.download_speed / 1024:.1f} KB/s"
            
            self.progress.update(
                task_id,
                completed=event.bytes_downloaded,
                description=description
            )
    
    def _rich_download_completed(self, event: DownloadCompletedEvent) -> None:
        """Handle download completion with Rich display."""
        if event.post_id in self.download_tasks:
            task_info = self.download_tasks[event.post_id]
            task_id = task_info['task_id']
            
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
            
            # Remove task after short delay
            self.progress.remove_task(task_id)
            del self.download_tasks[event.post_id]
    
    def _rich_post_processed(self, event: PostProcessedEvent) -> None:
        """Handle post processing with Rich display."""
        if self.overall_task is not None:
            processed = self.statistics['posts_processed']
            self.progress.update(
                self.overall_task,
                completed=processed,
                description=f"Processing posts ({processed}/{self.total_posts})"
            )
    
    def _rich_pipeline_stage(self, event: PipelineStageEvent) -> None:
        """Handle pipeline stage events with Rich display."""
        if event.stage_status == 'started':
            if self.stage_task is not None:
                self.progress.remove_task(self.stage_task)
            self.stage_task = self.progress.add_task(
                f"🚀 {event.stage_name}",
                total=None
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
    
    def _rich_error(self, event: ErrorEvent) -> None:
        """Handle error events with Rich display."""
        if self.output_mode == OutputMode.VERBOSE:
            self.console.print(f"[red]🚨 {event.error_type}: {event.error_message}[/red]")
    
    def _update_rich_statistics(self) -> None:
        """Update Rich statistics panel."""
        if not hasattr(self, 'layout'):
            return
        
        stats = self.get_current_statistics()
        
        # Create statistics table
        table = Table(title="Session Statistics", show_header=True, header_style="bold blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Posts Discovered", str(stats['posts_discovered']))
        table.add_row("Posts Processed", f"{stats['posts_processed']}/{stats['posts_discovered']}")
        table.add_row("Downloads Completed", str(stats['downloads_completed']))
        table.add_row("Downloads Failed", str(stats['downloads_failed']))
        table.add_row("Bytes Downloaded", f"{stats['bytes_downloaded'] / (1024*1024):.1f} MB")
        table.add_row("Average Speed", f"{stats.get('average_speed', 0) / 1024:.1f} KB/s")
        table.add_row("Session Duration", f"{stats['session_duration']:.1f}s")
        table.add_row("Success Rate", f"{stats.get('success_rate', 0):.1f}%")
        
        if stats['errors_occurred'] > 0:
            table.add_row("Errors", str(stats['errors_occurred']))
        
        # Update layout
        self.layout["stats"].update(Panel(table, border_style="blue"))
    
    def _handle_tqdm_event(self, event: BaseEvent) -> None:
        """Handle event with tqdm display."""
        # Implementation for tqdm-based progress bars
        if isinstance(event, PostDiscoveredEvent):
            if not hasattr(self, 'overall_bar'):
                self.overall_bar = tqdm(
                    total=event.post_count,
                    desc="Processing posts",
                    unit="post"
                )
        elif isinstance(event, DownloadStartedEvent):
            if self.show_individual and len(self.tqdm_bars) < self.max_individual_bars:
                pbar = tqdm(
                    total=event.expected_size,
                    desc=f"📥 {event.filename}",
                    unit="B",
                    unit_scale=True
                )
                self.tqdm_bars[event.post_id] = pbar
        elif isinstance(event, DownloadProgressEvent):
            if event.post_id in self.tqdm_bars:
                pbar = self.tqdm_bars[event.post_id]
                pbar.update(event.bytes_downloaded - pbar.n)
        elif isinstance(event, DownloadCompletedEvent):
            if event.post_id in self.tqdm_bars:
                pbar = self.tqdm_bars[event.post_id]
                pbar.close()
                del self.tqdm_bars[event.post_id]
        elif isinstance(event, PostProcessedEvent):
            if hasattr(self, 'overall_bar'):
                self.overall_bar.update(1)
    
    def _handle_simple_event(self, event: BaseEvent) -> None:
        """Handle event with simple text display."""
        current_time = time.time()
        
        # Rate limit simple output
        if current_time - self.last_update < 1.0:
            return
        
        self.last_update = current_time
        
        if isinstance(event, PostDiscoveredEvent):
            print(f"📥 Discovered {event.post_count} posts")
        elif isinstance(event, DownloadCompletedEvent):
            if event.success:
                print(f"✅ Downloaded: {event.filename}")
            else:
                print(f"❌ Download failed: {event.filename}")
        elif isinstance(event, PostProcessedEvent):
            processed = self.statistics['posts_processed']
            total = self.statistics['posts_discovered']
            print(f"🔄 Processed {processed}/{total} posts")
        elif isinstance(event, PipelineStageEvent):
            if event.stage_status == 'started':
                print(f"🚀 Starting: {event.stage_name}")
            elif event.stage_status == 'completed':
                print(f"✅ Completed: {event.stage_name}")
            elif event.stage_status == 'failed':
                print(f"❌ Failed: {event.stage_name}")
    
    def get_current_statistics(self) -> Dict[str, Any]:
        """Get current session statistics."""
        with self._lock:
            current_time = time.time()
            session_duration = current_time - self.session_start
            
            # Calculate derived statistics
            average_speed = 0
            if self.download_speeds:
                average_speed = sum(self.download_speeds) / len(self.download_speeds)
            
            success_rate = 0
            total_downloads = self.statistics['downloads_completed'] + self.statistics['downloads_failed']
            if total_downloads > 0:
                success_rate = (self.statistics['downloads_completed'] / total_downloads) * 100
            
            downloads_per_minute = 0
            if session_duration > 0:
                downloads_per_minute = (self.statistics['downloads_completed'] / session_duration) * 60
            
            return {
                **self.statistics,
                'session_duration': session_duration,
                'average_speed': average_speed,
                'success_rate': success_rate,
                'downloads_per_minute': downloads_per_minute,
                'stage_times': dict(self.stage_times)
            }
    
    def shutdown(self) -> None:
        """Shutdown progress display and cleanup resources."""
        try:
            # Close Rich display
            if hasattr(self, 'live') and self.live:
                self.live.stop()
            
            # Close tqdm bars
            if hasattr(self, 'tqdm_bars'):
                for pbar in self.tqdm_bars.values():
                    pbar.close()
            
            if hasattr(self, 'overall_bar'):
                self.overall_bar.close()
            
            # Close JSON output
            if self.should_close_json and hasattr(self, 'json_file') and self.json_file:
                self.json_file.close()
                
        except Exception:
            pass  # Ignore cleanup errors
    
    def __del__(self):
        """Cleanup when observer is destroyed."""
        self.shutdown()


class ETACalculator:
    """
    Helper class for calculating estimated time of arrival (ETA) for operations.
    
    Uses exponential smoothing to provide more accurate ETA estimates.
    """
    
    def __init__(self, smoothing_factor: float = 0.1):
        """
        Initialize ETA calculator.
        
        Args:
            smoothing_factor: Exponential smoothing factor (0.0 to 1.0)
        """
        self.smoothing_factor = smoothing_factor
        self.start_time = None
        self.last_update = None
        self.smoothed_rate = None
        self.total_expected = None
    
    def start(self, total_expected: int) -> None:
        """Start ETA calculation for a new operation."""
        self.start_time = time.time()
        self.last_update = self.start_time
        self.smoothed_rate = None
        self.total_expected = total_expected
    
    def update(self, completed: int) -> Optional[float]:
        """
        Update ETA calculation with current progress.
        
        Args:
            completed: Amount of work completed
            
        Returns:
            Estimated seconds remaining, or None if not enough data
        """
        if self.start_time is None or self.total_expected is None:
            return None
        
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        if elapsed <= 0 or completed <= 0:
            return None
        
        # Calculate current rate (items per second)
        current_rate = completed / elapsed
        
        # Apply exponential smoothing
        if self.smoothed_rate is None:
            self.smoothed_rate = current_rate
        else:
            self.smoothed_rate = (
                self.smoothing_factor * current_rate + 
                (1 - self.smoothing_factor) * self.smoothed_rate
            )
        
        # Calculate ETA
        remaining = self.total_expected - completed
        if self.smoothed_rate > 0:
            eta_seconds = remaining / self.smoothed_rate
            return eta_seconds
        
        return None
    
    def get_eta_string(self, completed: int) -> str:
        """
        Get formatted ETA string.
        
        Args:
            completed: Amount of work completed
            
        Returns:
            Formatted ETA string (e.g., "5m 30s remaining")
        """
        eta_seconds = self.update(completed)
        
        if eta_seconds is None:
            return "calculating..."
        
        if eta_seconds < 60:
            return f"{eta_seconds:.0f}s remaining"
        elif eta_seconds < 3600:
            minutes = int(eta_seconds // 60)
            seconds = int(eta_seconds % 60)
            return f"{minutes}m {seconds}s remaining"
        else:
            hours = int(eta_seconds // 3600)
            minutes = int((eta_seconds % 3600) // 60)
            return f"{hours}h {minutes}m remaining"


def create_cli_progress_observer(config: Dict[str, Any]) -> CLIProgressObserver:
    """
    Factory function to create CLI progress observer from configuration.
    
    Args:
        config: Configuration dictionary with observer settings
        
    Returns:
        Configured CLIProgressObserver instance
    """
    # Extract configuration options
    output_mode = OutputMode(config.get('output_mode', 'normal'))
    
    progress_display = None
    if 'progress_display' in config:
        progress_display = ProgressDisplay(config['progress_display'])
    
    return CLIProgressObserver(
        name=config.get('name', 'cli_progress'),
        output_mode=output_mode,
        progress_display=progress_display,
        show_individual=config.get('show_individual', True),
        max_individual_bars=config.get('max_individual_bars', 5),
        show_eta=config.get('show_eta', True),
        show_speed=config.get('show_speed', True),
        show_statistics=config.get('show_statistics', True),
        quiet_mode=config.get('quiet_mode', False),
        json_output=config.get('json_output')
    )