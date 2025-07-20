"""
Tests for CLI Progress Observer System

Tests the CLI progress observer functionality including rich progress bars,
fallback options, output modes, and configuration integration.
"""

import asyncio
import json
import os
import tempfile
import time
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from redditdl.cli.observers.progress import (
    CLIProgressObserver, OutputMode, ProgressDisplay, ETACalculator, 
    create_cli_progress_observer
)
from redditdl.core.events.types import (
    PostDiscoveredEvent, DownloadStartedEvent, DownloadProgressEvent,
    DownloadCompletedEvent, PostProcessedEvent, PipelineStageEvent, ErrorEvent
)


class TestCLIProgressObserver:
    """Test suite for CLIProgressObserver functionality."""
    
    def test_observer_initialization_normal_mode(self):
        """Test observer initialization in normal mode."""
        observer = CLIProgressObserver(
            output_mode=OutputMode.NORMAL,
            progress_display=ProgressDisplay.SIMPLE
        )
        
        assert observer.output_mode == OutputMode.NORMAL
        assert observer.progress_display == ProgressDisplay.SIMPLE
        assert observer.enabled is True
        assert observer.statistics['posts_discovered'] == 0
    
    def test_observer_initialization_quiet_mode(self):
        """Test observer initialization in quiet mode."""
        observer = CLIProgressObserver(
            output_mode=OutputMode.QUIET,
            quiet_mode=True
        )
        
        assert observer.output_mode == OutputMode.QUIET
        assert observer.show_individual is False
        assert observer.show_statistics is False
        assert observer.quiet_mode is True
    
    def test_observer_initialization_json_mode(self):
        """Test observer initialization in JSON mode."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            json_file = f.name
        
        try:
            observer = CLIProgressObserver(
                output_mode=OutputMode.JSON,
                json_output=json_file
            )
            
            assert observer.output_mode == OutputMode.JSON
            assert observer.json_file is not None
            
        finally:
            observer.shutdown()
            if os.path.exists(json_file):
                os.unlink(json_file)
    
    def test_observer_auto_detect_progress_display(self):
        """Test automatic detection of progress display type."""
        # Test with rich available
        with patch('cli.observers.progress.RICH_AVAILABLE', True):
            observer = CLIProgressObserver()
            assert observer.progress_display == ProgressDisplay.RICH
        
        # Test with rich unavailable but tqdm available
        with patch('cli.observers.progress.RICH_AVAILABLE', False), \
             patch('cli.observers.progress.TQDM_AVAILABLE', True):
            observer = CLIProgressObserver()
            assert observer.progress_display == ProgressDisplay.TQDM
        
        # Test with both unavailable
        with patch('cli.observers.progress.RICH_AVAILABLE', False), \
             patch('cli.observers.progress.TQDM_AVAILABLE', False):
            observer = CLIProgressObserver()
            assert observer.progress_display == ProgressDisplay.SIMPLE
    
    def test_post_discovered_event_handling(self):
        """Test handling of PostDiscoveredEvent."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.SIMPLE
        )
        
        event = PostDiscoveredEvent(
            source_type="user",
            target="testuser",
            post_count=25
        )
        
        observer.handle_event(event)
        
        assert observer.statistics['posts_discovered'] == 25
        assert observer.total_posts == 25
    
    def test_download_progress_tracking(self):
        """Test download progress tracking."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.SIMPLE
        )
        
        # Start download
        start_event = DownloadStartedEvent(
            post_id="test123",
            url="https://example.com/image.jpg",
            filename="image.jpg",
            expected_size=1024000
        )
        observer.handle_event(start_event)
        
        # Progress update
        progress_event = DownloadProgressEvent(
            post_id="test123",
            filename="image.jpg",
            bytes_downloaded=512000,
            progress_percentage=50.0,
            download_speed=102400
        )
        observer.handle_event(progress_event)
        
        # Complete download
        complete_event = DownloadCompletedEvent(
            post_id="test123",
            filename="image.jpg",
            success=True,
            file_size=1024000,
            duration_seconds=10.0,
            average_speed=102400
        )
        observer.handle_event(complete_event)
        
        assert observer.statistics['downloads_started'] == 1
        assert observer.statistics['downloads_completed'] == 1
        assert observer.statistics['bytes_downloaded'] == 1024000
        assert len(observer.download_speeds) == 1
    
    def test_error_handling(self):
        """Test error event handling."""
        observer = CLIProgressObserver(
            output_mode=OutputMode.VERBOSE,
            progress_display=ProgressDisplay.SIMPLE
        )
        
        error_event = ErrorEvent(
            error_type="NetworkError",
            error_message="Connection timeout",
            error_context="download_stage",
            recoverable=True
        )
        
        observer.handle_event(error_event)
        
        assert observer.statistics['errors_occurred'] == 1
    
    def test_quiet_mode_error_output(self):
        """Test that quiet mode only outputs errors."""
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            observer = CLIProgressObserver(
                output_mode=OutputMode.QUIET,
                quiet_mode=True
            )
            
            # Regular event should not produce output
            post_event = PostDiscoveredEvent(
                source_type="user",
                target="testuser", 
                post_count=10
            )
            observer.handle_event(post_event)
            assert mock_stderr.getvalue() == ""
            
            # Error event should produce output
            error_event = ErrorEvent(
                error_type="TestError",
                error_message="Test error message",
                error_context="test"
            )
            observer.handle_event(error_event)
            assert "Test error message" in mock_stderr.getvalue()
    
    def test_json_output_mode(self):
        """Test JSON output mode functionality."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            json_file = f.name
        
        try:
            observer = CLIProgressObserver(
                output_mode=OutputMode.JSON,
                json_output=json_file
            )
            
            # Send event
            event = PostDiscoveredEvent(
                source_type="user",
                target="testuser",
                post_count=5
            )
            observer.handle_event(event)
            
            # Read and verify JSON output
            with open(json_file, 'r') as f:
                content = f.read().strip()
                json_data = json.loads(content)
                
                assert 'timestamp' in json_data
                assert 'event' in json_data
                assert 'statistics' in json_data
                assert json_data['event']['post_count'] == 5
            
        finally:
            observer.shutdown()
            if os.path.exists(json_file):
                os.unlink(json_file)
    
    def test_statistics_calculation(self):
        """Test statistics calculation and reporting."""
        observer = CLIProgressObserver()
        
        # Add some test data
        observer.statistics['posts_discovered'] = 10
        observer.statistics['downloads_completed'] = 8
        observer.statistics['downloads_failed'] = 2
        observer.statistics['bytes_downloaded'] = 1024000
        observer.download_speeds = [100000, 200000, 150000]
        observer.session_start = time.time() - 60  # 1 minute ago
        
        stats = observer.get_current_statistics()
        
        assert stats['posts_discovered'] == 10
        assert stats['downloads_completed'] == 8
        assert stats['success_rate'] == 80.0  # 8/10 * 100
        assert stats['average_speed'] == 150000  # average of speeds
        assert stats['session_duration'] > 59  # approximately 60 seconds
    
    def test_observer_shutdown(self):
        """Test proper observer shutdown."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.SIMPLE
        )
        
        # Should not raise exception
        observer.shutdown()
        
        # Multiple shutdowns should be safe
        observer.shutdown()


class TestETACalculator:
    """Test suite for ETA calculation functionality."""
    
    def test_eta_calculator_initialization(self):
        """Test ETA calculator initialization."""
        calc = ETACalculator(smoothing_factor=0.2)
        
        assert calc.smoothing_factor == 0.2
        assert calc.start_time is None
        assert calc.smoothed_rate is None
    
    def test_eta_calculation(self):
        """Test ETA calculation with mock data."""
        calc = ETACalculator()
        calc.start(total_expected=100)
        
        # Simulate progress
        start_time = time.time()
        calc.start_time = start_time - 10  # 10 seconds ago
        
        # After 10 seconds, 20% complete
        eta = calc.update(20)
        
        # Should estimate about 40 more seconds (20 items in 10 seconds = 2 items/sec, 80 items remaining)
        assert eta is not None
        assert 35 <= eta <= 45  # Allow some variance
    
    def test_eta_string_formatting(self):
        """Test ETA string formatting."""
        calc = ETACalculator()
        calc.start(total_expected=100)
        calc.start_time = time.time() - 30  # 30 seconds ago
        
        # Test various completion levels
        eta_string = calc.get_eta_string(50)  # 50% complete
        assert "remaining" in eta_string
        
        # Test with no data
        calc.start_time = None
        eta_string = calc.get_eta_string(10)
        assert eta_string == "calculating..."


class TestConfigurationIntegration:
    """Test configuration integration for progress observers."""
    
    def test_create_cli_progress_observer_from_config(self):
        """Test creating CLI progress observer from configuration."""
        config = {
            'name': 'test_progress',
            'output_mode': 'verbose',
            'progress_display': 'rich',
            'show_individual': False,
            'max_individual_bars': 3,
            'show_eta': False,
            'quiet_mode': False
        }
        
        observer = create_cli_progress_observer(config)
        
        assert observer.name == 'test_progress'
        assert observer.output_mode == OutputMode.VERBOSE
        assert observer.progress_display == ProgressDisplay.RICH
        assert observer.show_individual is False
        assert observer.max_individual_bars == 3
        assert observer.show_eta is False
    
    def test_create_cli_progress_observer_defaults(self):
        """Test creating CLI progress observer with default configuration."""
        config = {}
        
        observer = create_cli_progress_observer(config)
        
        assert observer.name == 'cli_progress'
        assert observer.output_mode == OutputMode.NORMAL
        assert observer.show_individual is True
        assert observer.show_eta is True


class TestProgressDisplayIntegration:
    """Test integration with different progress display types."""
    
    @patch('cli.observers.progress.RICH_AVAILABLE', True)
    def test_rich_display_integration(self):
        """Test Rich display integration."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.RICH,
            show_statistics=False  # Disable to avoid layout complexity
        )
        
        # Should initialize Rich components
        assert observer.progress is not None
        assert observer.console is not None
        
        # Test event handling doesn't raise exceptions
        event = PostDiscoveredEvent(
            source_type="user",
            target="testuser",
            post_count=10
        )
        observer.handle_event(event)
        
        observer.shutdown()
    
    @patch('cli.observers.progress.TQDM_AVAILABLE', True)
    def test_tqdm_display_integration(self):
        """Test tqdm display integration."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.TQDM
        )
        
        # Should initialize tqdm components
        assert not hasattr(observer, 'progress') or observer.progress is None
        assert hasattr(observer, 'tqdm_bars')
        
        observer.shutdown()
    
    def test_simple_display_integration(self):
        """Test simple display integration."""
        with patch('builtins.print') as mock_print:
            observer = CLIProgressObserver(
                progress_display=ProgressDisplay.SIMPLE
            )
            
            # Test event handling produces output
            event = PostDiscoveredEvent(
                source_type="user",
                target="testuser",
                post_count=10
            )
            observer.handle_event(event)
            
            # Should have printed something
            assert mock_print.called
    
    def test_no_display_mode(self):
        """Test no display mode."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.NONE
        )
        
        assert observer.progress is None
        assert observer.console is None
        
        # Events should still update statistics
        event = PostDiscoveredEvent(
            source_type="user",
            target="testuser",
            post_count=10
        )
        observer.handle_event(event)
        
        assert observer.statistics['posts_discovered'] == 10


class TestErrorHandling:
    """Test error handling and fallback behavior."""
    
    @patch('cli.observers.progress.RICH_AVAILABLE', False)
    @patch('cli.observers.progress.TQDM_AVAILABLE', False)
    def test_fallback_to_simple_display(self):
        """Test fallback to simple display when libraries unavailable."""
        observer = CLIProgressObserver()
        
        assert observer.progress_display == ProgressDisplay.SIMPLE
    
    def test_observer_continues_on_display_errors(self):
        """Test that observer continues working even if display errors occur."""
        observer = CLIProgressObserver(
            progress_display=ProgressDisplay.SIMPLE
        )
        
        # Mock print to raise exception
        with patch('builtins.print', side_effect=Exception("Print error")):
            # Should not raise exception
            event = PostDiscoveredEvent(
                source_type="user",
                target="testuser",
                post_count=10
            )
            observer.handle_event(event)
            
            # Statistics should still be updated
            assert observer.statistics['posts_discovered'] == 10
    
    def test_json_output_file_error_handling(self):
        """Test handling of JSON output file errors."""
        # Test with invalid file path
        observer = CLIProgressObserver(
            output_mode=OutputMode.JSON,
            json_output="/invalid/path/output.json"
        )
        
        # Should not raise exception on event handling
        event = PostDiscoveredEvent(
            source_type="user",
            target="testuser",
            post_count=10
        )
        observer.handle_event(event)


if __name__ == "__main__":
    pytest.main([__file__])