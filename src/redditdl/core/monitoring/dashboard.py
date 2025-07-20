"""
Performance Dashboard

Real-time performance monitoring dashboard with live metrics display,
trend analysis, and bottleneck identification.
"""

import asyncio
import logging
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.text import Text
from rich.align import Align

from redditdl.core.monitoring.metrics import MetricsCollector, MetricSummary
from redditdl.core.monitoring.profiler import ResourceProfiler


logger = logging.getLogger(__name__)


@dataclass
class DashboardConfig:
    """Configuration for performance dashboard."""
    refresh_interval: float = 1.0
    max_history_points: int = 100
    show_system_metrics: bool = True
    show_application_metrics: bool = True
    show_bottlenecks: bool = True
    compact_mode: bool = False


class PerformanceDashboard:
    """
    Real-time performance monitoring dashboard.
    
    Provides:
    - Live system resource monitoring
    - Application-specific metrics display
    - Performance trend visualization
    - Bottleneck identification and alerts
    """
    
    def __init__(self, 
                 metrics_collector: Optional[MetricsCollector] = None,
                 profiler: Optional[ResourceProfiler] = None,
                 config: Optional[DashboardConfig] = None):
        """
        Initialize performance dashboard.
        
        Args:
            metrics_collector: Metrics collector instance
            profiler: Resource profiler instance
            config: Dashboard configuration
        """
        self.metrics_collector = metrics_collector
        self.profiler = profiler
        self.config = config or DashboardConfig()
        
        self.console = Console()
        self._running = False
        self._live: Optional[Live] = None
        
        # Dashboard state
        self._metric_history: Dict[str, List[float]] = {}
        self._last_update = time.time()
        self._update_count = 0
    
    async def start(self) -> None:
        """Start the performance dashboard."""
        if self._running:
            return
        
        self._running = True
        
        # Create layout
        layout = self._create_layout()
        
        # Start live display
        self._live = Live(
            layout,
            console=self.console,
            refresh_per_second=1.0 / self.config.refresh_interval,
            screen=True
        )
        
        # Run dashboard update loop
        await self._run_dashboard()
    
    def stop(self) -> None:
        """Stop the performance dashboard."""
        if not self._running:
            return
        
        self._running = False
        
        if self._live:
            self._live.stop()
        
        logger.info("Performance dashboard stopped")
    
    def _create_layout(self) -> Layout:
        """Create the dashboard layout."""
        layout = Layout()
        
        if self.config.compact_mode:
            # Compact layout for smaller terminals
            layout.split_column(
                Layout(name="header", size=3),
                Layout(name="main")
            )
            
            layout["main"].split_row(
                Layout(name="metrics", ratio=2),
                Layout(name="status", ratio=1)
            )
        else:
            # Full layout with multiple sections
            layout.split_column(
                Layout(name="header", size=3),
                Layout(name="main"),
                Layout(name="footer", size=5)
            )
            
            layout["main"].split_row(
                Layout(name="left", ratio=2),
                Layout(name="right", ratio=1)
            )
            
            layout["left"].split_column(
                Layout(name="system", ratio=1),
                Layout(name="application", ratio=1)
            )
            
            layout["right"].split_column(
                Layout(name="bottlenecks", ratio=1),
                Layout(name="trends", ratio=1)
            )
        
        return layout
    
    async def _run_dashboard(self) -> None:
        """Main dashboard update loop."""
        layout = self._create_layout()
        
        with Live(
            layout,
            console=self.console,
            refresh_per_second=1.0 / self.config.refresh_interval,
            screen=True
        ) as live:
            
            self._live = live
            
            try:
                while self._running:
                    # Update dashboard content
                    await self._update_dashboard(layout)
                    
                    # Update live display
                    live.update(layout)
                    
                    # Wait for next update
                    await asyncio.sleep(self.config.refresh_interval)
            
            except KeyboardInterrupt:
                self._running = False
            except Exception as e:
                logger.error(f"Dashboard error: {e}")
            finally:
                self._live = None
    
    async def _update_dashboard(self, layout: Layout) -> None:
        """Update dashboard content."""
        self._update_count += 1
        current_time = time.time()
        
        # Update header
        self._update_header(layout)
        
        if self.config.compact_mode:
            # Compact mode updates
            if "metrics" in layout:
                layout["metrics"].update(self._create_metrics_table())
            if "status" in layout:
                layout["status"].update(self._create_status_panel())
        else:
            # Full mode updates
            if "system" in layout:
                layout["system"].update(self._create_system_metrics_panel())
            if "application" in layout:
                layout["application"].update(self._create_application_metrics_panel())
            if "bottlenecks" in layout:
                layout["bottlenecks"].update(self._create_bottlenecks_panel())
            if "trends" in layout:
                layout["trends"].update(self._create_trends_panel())
            if "footer" in layout:
                layout["footer"].update(self._create_footer())
        
        self._last_update = current_time
    
    def _update_header(self, layout: Layout) -> None:
        """Update dashboard header."""
        uptime = time.time() - (self._last_update if hasattr(self, '_start_time') else time.time())
        
        header_text = Text("RedditDL Performance Dashboard", style="bold cyan")
        header_text.append(f" | Uptime: {uptime:.0f}s", style="dim")
        header_text.append(f" | Updates: {self._update_count}", style="dim")
        
        if "header" in layout:
            layout["header"].update(
                Panel(
                    Align.center(header_text),
                    style="bright_blue"
                )
            )
    
    def _create_system_metrics_panel(self) -> Panel:
        """Create system metrics panel."""
        if not self.metrics_collector:
            return Panel("No metrics collector available", title="System Metrics")
        
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Current", justify="right")
        table.add_column("Trend", justify="center")
        
        # System metrics to display
        system_metrics = [
            ("CPU %", "system.cpu_percent"),
            ("Memory %", "system.memory_percent"),
            ("Memory MB", "system.memory_mb"),
            ("Disk Read MB", "system.disk_io_read_mb"),
            ("Disk Write MB", "system.disk_io_write_mb"),
            ("Net Recv MB", "system.network_io_recv_mb"),
            ("Net Sent MB", "system.network_io_sent_mb")
        ]
        
        for display_name, metric_name in system_metrics:
            metric = self.metrics_collector.get_metric(metric_name)
            if metric:
                summary = metric.get_summary()
                current_value = summary.avg if summary.count > 0 else 0
                
                # Update history for trend calculation
                if metric_name not in self._metric_history:
                    self._metric_history[metric_name] = []
                
                self._metric_history[metric_name].append(current_value)
                if len(self._metric_history[metric_name]) > self.config.max_history_points:
                    self._metric_history[metric_name].pop(0)
                
                # Calculate trend
                trend = self._calculate_trend(self._metric_history[metric_name])
                trend_symbol = self._get_trend_symbol(trend)
                
                # Format value based on metric type
                if "percent" in metric_name:
                    value_str = f"{current_value:.1f}%"
                elif "mb" in metric_name.lower():
                    value_str = f"{current_value:.1f} MB"
                else:
                    value_str = f"{current_value:.2f}"
                
                table.add_row(display_name, value_str, trend_symbol)
        
        return Panel(table, title="System Metrics", border_style="blue")
    
    def _create_application_metrics_panel(self) -> Panel:
        """Create application-specific metrics panel."""
        if not self.metrics_collector:
            return Panel("No metrics collector available", title="Application Metrics")
        
        table = Table(show_header=True, header_style="bold green")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right")
        table.add_column("Avg", justify="right")
        table.add_column("P95", justify="right")
        
        # Get all non-system metrics
        all_summaries = self.metrics_collector.get_all_summaries()
        app_metrics = {
            name: summary for name, summary in all_summaries.items()
            if not name.startswith("system.")
        }
        
        # Sort by count descending
        sorted_metrics = sorted(
            app_metrics.items(),
            key=lambda x: x[1].count,
            reverse=True
        )
        
        for metric_name, summary in sorted_metrics[:10]:  # Top 10
            if summary.count > 0:
                table.add_row(
                    metric_name.split(".")[-1],  # Show only last part
                    str(summary.count),
                    f"{summary.avg:.3f}",
                    f"{summary.p95:.3f}"
                )
        
        return Panel(table, title="Application Metrics", border_style="green")
    
    def _create_bottlenecks_panel(self) -> Panel:
        """Create bottlenecks identification panel."""
        if not self.profiler:
            return Panel("No profiler available", title="Bottlenecks")
        
        table = Table(show_header=True, header_style="bold red")
        table.add_column("Function", style="cyan")
        table.add_column("Time %", justify="right")
        table.add_column("Calls", justify="right")
        
        # Get bottlenecks from most recent profiles
        all_profiles = self.profiler.get_all_profiles()
        
        # Collect all bottlenecks
        all_bottlenecks = []
        for profile_name in all_profiles:
            bottlenecks = self.profiler.identify_bottlenecks(profile_name)
            all_bottlenecks.extend(bottlenecks)
        
        # Sort by percentage and take top 10
        all_bottlenecks.sort(key=lambda x: x.percentage_of_total, reverse=True)
        top_bottlenecks = all_bottlenecks[:10]
        
        for bottleneck in top_bottlenecks:
            # Truncate function name if too long
            func_name = bottleneck.function_name
            if len(func_name) > 20:
                func_name = func_name[:17] + "..."
            
            table.add_row(
                func_name,
                f"{bottleneck.percentage_of_total:.1f}%",
                str(bottleneck.call_count)
            )
        
        if not top_bottlenecks:
            table.add_row("No bottlenecks detected", "", "")
        
        return Panel(table, title="Performance Bottlenecks", border_style="red")
    
    def _create_trends_panel(self) -> Panel:
        """Create performance trends panel."""
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Metric", style="cyan")
        table.add_column("Trend", justify="center")
        table.add_column("Change", justify="right")
        
        # Analyze trends for key metrics
        key_metrics = ["system.cpu_percent", "system.memory_percent", "system.memory_mb"]
        
        for metric_name in key_metrics:
            if metric_name in self._metric_history:
                history = self._metric_history[metric_name]
                if len(history) >= 2:
                    trend = self._calculate_trend(history)
                    trend_symbol = self._get_trend_symbol(trend)
                    
                    # Calculate percentage change from first to last
                    if history[0] != 0:
                        change_percent = ((history[-1] - history[0]) / history[0]) * 100
                        change_str = f"{change_percent:+.1f}%"
                    else:
                        change_str = "N/A"
                    
                    display_name = metric_name.replace("system.", "").replace("_", " ").title()
                    table.add_row(display_name, trend_symbol, change_str)
        
        return Panel(table, title="Performance Trends", border_style="yellow")
    
    def _create_footer(self) -> Panel:
        """Create dashboard footer with summary information."""
        if not self.metrics_collector:
            footer_text = "Dashboard running without metrics collector"
        else:
            all_summaries = self.metrics_collector.get_all_summaries()
            total_metrics = len(all_summaries)
            active_metrics = sum(1 for s in all_summaries.values() if s.count > 0)
            
            footer_text = f"Tracking {active_metrics}/{total_metrics} metrics"
            
            if self.profiler:
                all_profiles = self.profiler.get_all_profiles()
                total_profiles = sum(len(results) for results in all_profiles.values())
                footer_text += f" | {len(all_profiles)} profile types | {total_profiles} total profiles"
        
        footer_text += f" | Press Ctrl+C to exit"
        
        return Panel(
            Align.center(Text(footer_text, style="dim")),
            style="dim"
        )
    
    def _create_metrics_table(self) -> Table:
        """Create compact metrics table for compact mode."""
        table = Table(show_header=True, header_style="bold")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        table.add_column("Trend", justify="center")
        
        if self.metrics_collector:
            # Show top system metrics in compact mode
            key_metrics = [
                ("CPU %", "system.cpu_percent"),
                ("Memory %", "system.memory_percent"),
                ("Memory MB", "system.memory_mb")
            ]
            
            for display_name, metric_name in key_metrics:
                metric = self.metrics_collector.get_metric(metric_name)
                if metric:
                    summary = metric.get_summary()
                    current_value = summary.avg if summary.count > 0 else 0
                    
                    # Update history
                    if metric_name not in self._metric_history:
                        self._metric_history[metric_name] = []
                    
                    self._metric_history[metric_name].append(current_value)
                    if len(self._metric_history[metric_name]) > 20:  # Shorter history in compact mode
                        self._metric_history[metric_name].pop(0)
                    
                    # Calculate trend
                    trend = self._calculate_trend(self._metric_history[metric_name])
                    trend_symbol = self._get_trend_symbol(trend)
                    
                    # Format value
                    if "percent" in metric_name:
                        value_str = f"{current_value:.1f}%"
                    elif "mb" in metric_name.lower():
                        value_str = f"{current_value:.1f} MB"
                    else:
                        value_str = f"{current_value:.2f}"
                    
                    table.add_row(display_name, value_str, trend_symbol)
        
        return table
    
    def _create_status_panel(self) -> Panel:
        """Create status panel for compact mode."""
        status_lines = []
        
        if self.metrics_collector:
            all_summaries = self.metrics_collector.get_all_summaries()
            active_metrics = sum(1 for s in all_summaries.values() if s.count > 0)
            status_lines.append(f"Active Metrics: {active_metrics}")
        
        if self.profiler:
            all_profiles = self.profiler.get_all_profiles()
            profile_types = len(all_profiles)
            status_lines.append(f"Profile Types: {profile_types}")
        
        status_lines.append(f"Updates: {self._update_count}")
        status_lines.append("")
        status_lines.append("Ctrl+C to exit")
        
        status_text = "\n".join(status_lines)
        
        return Panel(
            Text(status_text, style="dim"),
            title="Status",
            border_style="dim"
        )
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend from list of values."""
        if len(values) < 2:
            return 0.0
        
        # Simple linear trend calculation
        n = len(values)
        x_sum = sum(range(n))
        y_sum = sum(values)
        xy_sum = sum(i * values[i] for i in range(n))
        x2_sum = sum(i * i for i in range(n))
        
        if n * x2_sum - x_sum * x_sum == 0:
            return 0.0
        
        slope = (n * xy_sum - x_sum * y_sum) / (n * x2_sum - x_sum * x_sum)
        return slope
    
    def _get_trend_symbol(self, trend: float) -> str:
        """Get trend symbol from trend value."""
        if abs(trend) < 0.001:  # Essentially flat
            return "→"
        elif trend > 0:
            return "↑" if trend > 0.01 else "↗"
        else:
            return "↓" if trend < -0.01 else "↘"


async def run_dashboard(metrics_collector: Optional[MetricsCollector] = None,
                       profiler: Optional[ResourceProfiler] = None,
                       config: Optional[DashboardConfig] = None) -> None:
    """
    Convenience function to run performance dashboard.
    
    Args:
        metrics_collector: Optional metrics collector
        profiler: Optional profiler
        config: Optional dashboard configuration
    """
    dashboard = PerformanceDashboard(metrics_collector, profiler, config)
    
    try:
        await dashboard.start()
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.stop()