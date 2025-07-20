#!/usr/bin/env python3
"""
Tests for RedditDL Event Observers

Comprehensive tests for all observer implementations including
console, logging, statistics, JSON, and progress observers.
"""

import io
import json
import logging
import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import sys
# sys.path.insert(0, '.') # This line is no longer needed with src/ layout

from redditdl.core.events.observers import (
    Observer, ConsoleObserver, LoggingObserver, StatisticsObserver,
    JSONObserver, ProgressObserver
)
from redditdl.core.events.types import (
    BaseEvent, PostDiscoveredEvent, DownloadStartedEvent,
    DownloadProgressEvent, DownloadCompletedEvent, PostProcessedEvent,
    FilterAppliedEvent, PipelineStageEvent, ErrorEvent, StatisticsEvent
)


class TestObserverBase:
    """Test suite for Observer base class."""
    
    def test_abstract_observer_cannot_instantiate(self):
        """Test that Observer base class cannot be instantiated directly."""
        with pytest.raises(TypeError):
            Observer("test")
    
    def test_concrete_observer_implementation(self):
        """Test concrete observer implementation."""
        class TestObserver(Observer):
            def __init__(self, name):
                super().__init__(name)
                self.events_handled = []
            
            def handle_event(self, event):
                self.events_handled.append(event)
        
        observer = TestObserver("test")
        event = BaseEvent()
        
        # Test callable interface
        observer(event)
        
        assert len(observer.events_handled) == 1
        assert observer.events_handled[0] is event
        assert observer.statistics['events_received'] == 1
        assert observer.statistics['events_processed'] == 1
    
    def test_observer_enable_disable(self):
        """Test observer enable/disable functionality."""
        class TestObserver(Observer):
            def __init__(self, name):
                super().__init__(name)
                self.events_handled = []
            
            def handle_event(self, event):
                self.events_handled.append(event)
        
        observer = TestObserver("test")
        event = BaseEvent()
        
        # Initially enabled
        assert observer.enabled is True
        observer(event)
        assert len(observer.events_handled) == 1
        
        # Disable and try again
        observer.disable()
        assert observer.enabled is False
        observer(event)
        assert len(observer.events_handled) == 1  # Should not increase
        
        # Re-enable
        observer.enable()
        assert observer.enabled is True
        observer(event)
        assert len(observer.events_handled) == 2
    
    def test_observer_error_handling(self):
        """Test observer error handling."""
        class FailingObserver(Observer):
            def handle_event(self, event):
                raise ValueError("Test error")
        
        observer = FailingObserver("failing")
        event = BaseEvent()
        
        # Should not raise exception
        observer(event)
        
        # Error should be counted
        assert observer.statistics['events_received'] == 1
        assert observer.statistics['events_processed'] == 0
        assert observer.statistics['events_errored'] == 1


class TestConsoleObserver:
    """Test suite for ConsoleObserver."""
    
    def test_console_observer_initialization(self):
        """Test ConsoleObserver initialization."""
        observer = ConsoleObserver()
        
        assert observer.name == "console"
        assert observer.verbose is True
        assert observer.show_timestamps is True
    
    def test_console_observer_custom_settings(self):
        """Test ConsoleObserver with custom settings."""
        observer = ConsoleObserver(
            name="custom_console",
            verbose=False,
            use_rich=False,
            show_timestamps=False
        )
        
        assert observer.name == "custom_console"
        assert observer.verbose is False
        assert observer.use_rich is False
        assert observer.show_timestamps is False
    
    @patch('builtins.print')
    def test_console_observer_handles_events(self, mock_print):
        """Test that ConsoleObserver handles various event types."""
        observer = ConsoleObserver(use_rich=False, show_timestamps=False)
        
        # Test different event types
        events = [
            PostDiscoveredEvent(post_count=10, target="testuser", source_type="user"),
            DownloadStartedEvent(filename="test.jpg", url="https://example.com/test.jpg"),
            DownloadCompletedEvent(filename="test.jpg", success=True, file_size=1024),
            PostProcessedEvent(post_title="Test Post", success=True),
            FilterAppliedEvent(filter_type="keyword", posts_before=100, posts_after=75, posts_filtered=25),
            PipelineStageEvent(stage_name="processing", stage_status="completed"),
            ErrorEvent(error_type="NetworkError", error_message="Connection failed"),
        ]
        
        for event in events:
            observer.handle_event(event)
        
        # Should have printed for each event
        assert mock_print.call_count == len(events)
    
    @patch('builtins.print')
    def test_console_observer_timestamps(self, mock_print):
        """Test console observer timestamp functionality."""
        observer = ConsoleObserver(use_rich=False, show_timestamps=True)
        event = BaseEvent()
        
        observer.handle_event(event)
        
        # Check that timestamp was added
        printed_text = mock_print.call_args[0][0]
        assert "[" in printed_text and "]" in printed_text
    
    @patch('builtins.print')
    def test_console_observer_verbose_mode(self, mock_print):
        """Test console observer verbose mode differences."""
        verbose_observer = ConsoleObserver(verbose=True, use_rich=False, show_timestamps=False)
        quiet_observer = ConsoleObserver(verbose=False, use_rich=False, show_timestamps=False)
        
        # Download progress event - should only appear in verbose mode
        progress_event = DownloadProgressEvent(filename="test.jpg", bytes_downloaded=512)
        
        verbose_observer.handle_event(progress_event)
        quiet_observer.handle_event(progress_event)
        
        # Verbose should print, quiet should not
        assert mock_print.call_count == 1


class TestLoggingObserver:
    """Test suite for LoggingObserver."""
    
    def test_logging_observer_initialization(self):
        """Test LoggingObserver initialization."""
        observer = LoggingObserver()
        
        assert observer.name == "logging"
        assert observer.log_level == logging.INFO
        assert observer.log_file is None
    
    def test_logging_observer_with_file(self):
        """Test LoggingObserver with log file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            log_file = tmp_file.name
        
        try:
            observer = LoggingObserver(log_file=log_file)
            assert observer.log_file == Path(log_file)
            
            # Test logging an event
            event = PostDiscoveredEvent(post_count=5, target="testuser")
            observer.handle_event(event)
            
            # Check log file was written
            with open(log_file, 'r') as f:
                log_content = f.read()
                assert "PostDiscoveredEvent" in log_content
                assert "testuser" in log_content
        
        finally:
            Path(log_file).unlink(missing_ok=True)
    
    def test_logging_observer_log_levels(self):
        """Test LoggingObserver respects log levels."""
        with patch.object(logging.Logger, 'log') as mock_log:
            observer = LoggingObserver(log_level=logging.WARNING)
            
            # INFO level event - should not be logged
            info_event = PostDiscoveredEvent()
            observer.handle_event(info_event)
            
            # ERROR level event - should be logged
            error_event = ErrorEvent(error_message="Test error")
            observer.handle_event(error_event)
            
            # Should only have logged the error
            assert mock_log.call_count == 1
            assert mock_log.call_args[0][0] == logging.ERROR
    
    def test_logging_observer_event_formatting(self):
        """Test LoggingObserver formats events correctly."""
        observer = LoggingObserver()
        
        # Test different event types produce different messages
        events_and_expected = [
            (PostDiscoveredEvent(post_count=10, target="user"), "Discovered 10 posts"),
            (DownloadStartedEvent(filename="test.jpg"), "Starting download: test.jpg"),
            (DownloadCompletedEvent(filename="test.jpg", success=True), "Download SUCCESS: test.jpg"),
            (ErrorEvent(error_message="Test error"), "ERROR: Test error"),
        ]
        
        for event, expected_text in events_and_expected:
            message = observer._format_event_message(event)
            assert expected_text in message


class TestStatisticsObserver:
    """Test suite for StatisticsObserver."""
    
    def test_statistics_observer_initialization(self):
        """Test StatisticsObserver initialization."""
        observer = StatisticsObserver()
        
        assert observer.name == "statistics"
        assert 'session_start' in observer.stats
        assert observer.stats['posts_discovered'] == 0
        assert observer.stats['downloads_completed'] == 0
    
    def test_statistics_observer_tracks_posts_discovered(self):
        """Test statistics tracking for post discovery."""
        observer = StatisticsObserver()
        
        event = PostDiscoveredEvent(post_count=25)
        observer.handle_event(event)
        
        stats = observer.get_current_statistics()
        assert stats['posts_discovered'] == 25
    
    def test_statistics_observer_tracks_downloads(self):
        """Test statistics tracking for downloads."""
        observer = StatisticsObserver()
        
        # Start download
        start_event = DownloadStartedEvent(filename="test.jpg")
        observer.handle_event(start_event)
        
        # Complete download successfully
        complete_event = DownloadCompletedEvent(
            filename="test.jpg",
            success=True,
            file_size=1024,
            duration_seconds=2.0,
            average_speed=512.0
        )
        observer.handle_event(complete_event)
        
        stats = observer.get_current_statistics()
        assert stats['downloads_started'] == 1
        assert stats['downloads_completed'] == 1
        assert stats['downloads_failed'] == 0
        assert stats['total_bytes_downloaded'] == 1024
        assert stats['total_download_time'] == 2.0
        assert len(observer.download_speeds) == 1
        assert observer.download_speeds[0] == 512.0
    
    def test_statistics_observer_tracks_failed_downloads(self):
        """Test statistics tracking for failed downloads."""
        observer = StatisticsObserver()
        
        # Start and fail download
        start_event = DownloadStartedEvent(filename="test.jpg")
        complete_event = DownloadCompletedEvent(filename="test.jpg", success=False)
        
        observer.handle_event(start_event)
        observer.handle_event(complete_event)
        
        stats = observer.get_current_statistics()
        assert stats['downloads_started'] == 1
        assert stats['downloads_completed'] == 0
        assert stats['downloads_failed'] == 1
    
    def test_statistics_observer_tracks_processing(self):
        """Test statistics tracking for post processing."""
        observer = StatisticsObserver()
        
        # Successful processing
        success_event = PostProcessedEvent(
            post_title="Test Post",
            success=True,
            processing_time=1.5
        )
        observer.handle_event(success_event)
        
        # Failed processing
        fail_event = PostProcessedEvent(
            post_title="Failed Post",
            success=False
        )
        observer.handle_event(fail_event)
        
        stats = observer.get_current_statistics()
        assert stats['posts_processed'] == 2
        assert stats['posts_successful'] == 1
        assert stats['posts_failed'] == 1
        assert len(observer.processing_times) == 1
        assert observer.processing_times[0] == 1.5
    
    def test_statistics_observer_calculates_derived_stats(self):
        """Test calculation of derived statistics."""
        observer = StatisticsObserver()
        
        # Add some successful and failed processing
        for i in range(8):
            observer.handle_event(PostProcessedEvent(success=True))
        for i in range(2):
            observer.handle_event(PostProcessedEvent(success=False))
        
        stats = observer.get_current_statistics()
        assert stats['success_rate'] == 80.0  # 8/10 * 100
        assert stats['session_duration'] > 0


class TestJSONObserver:
    """Test suite for JSONObserver."""
    
    def test_json_observer_initialization(self):
        """Test JSONObserver initialization."""
        observer = JSONObserver()
        
        assert observer.name == "json"
        assert observer.pretty_print is False
        assert observer.output_file is not None
    
    def test_json_observer_with_string_io(self):
        """Test JSONObserver with StringIO output."""
        output = io.StringIO()
        observer = JSONObserver(output_file=output)
        
        event = PostDiscoveredEvent(post_count=5, target="testuser")
        observer.handle_event(event)
        
        # Check JSON was written
        json_output = output.getvalue()
        assert json_output.strip()  # Should have content
        
        # Parse and verify JSON
        event_data = json.loads(json_output.strip())
        assert event_data['event_type'] == 'PostDiscoveredEvent'
        assert event_data['post_count'] == 5
        assert event_data['target'] == 'testuser'
    
    def test_json_observer_pretty_print(self):
        """Test JSONObserver with pretty printing."""
        output = io.StringIO()
        observer = JSONObserver(output_file=output, pretty_print=True)
        
        event = BaseEvent()
        observer.handle_event(event)
        
        # Check JSON was formatted with indentation
        json_output = output.getvalue()
        assert "  " in json_output  # Should have indentation
    
    def test_json_observer_with_file(self):
        """Test JSONObserver with file output."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            json_file = tmp_file.name
        
        try:
            observer = JSONObserver(output_file=json_file)
            event = PostDiscoveredEvent(post_count=10)
            observer.handle_event(event)
            
            # Read and verify file content
            with open(json_file, 'r') as f:
                json_content = f.read().strip()
                event_data = json.loads(json_content)
                assert event_data['event_type'] == 'PostDiscoveredEvent'
                assert event_data['post_count'] == 10
        
        finally:
            Path(json_file).unlink(missing_ok=True)


@pytest.mark.skipif(not hasattr(sys.modules.get('redditdl.core.events.observers', None), 'RICH_AVAILABLE') or 
                   not getattr(sys.modules.get('redditdl.core.events.observers', None), 'RICH_AVAILABLE', False),
                   reason="Rich library not available")
class TestProgressObserver:
    """Test suite for ProgressObserver (requires Rich library)."""
    
    def test_progress_observer_initialization(self):
        """Test ProgressObserver initialization."""
        try:
            observer = ProgressObserver()
            assert observer.name == "progress"
            assert observer.show_overall is True
            assert observer.show_individual is True
            assert observer.max_individual_bars == 5
            observer.shutdown()  # Clean up
        except ImportError:
            pytest.skip("Rich library not available")
    
    def test_progress_observer_tracks_posts_discovered(self):
        """Test progress observer tracks post discovery."""
        try:
            observer = ProgressObserver()
            
            event = PostDiscoveredEvent(post_count=50)
            observer.handle_event(event)
            
            assert observer.total_posts == 50
            observer.shutdown()
        except ImportError:
            pytest.skip("Rich library not available")
    
    def test_progress_observer_tracks_downloads(self):
        """Test progress observer tracks download progress."""
        try:
            observer = ProgressObserver()
            
            # Start download
            start_event = DownloadStartedEvent(
                post_id="test123",
                filename="test.jpg",
                expected_size=1024
            )
            observer.handle_event(start_event)
            
            # Progress update
            progress_event = DownloadProgressEvent(
                post_id="test123",
                filename="test.jpg",
                bytes_downloaded=512,
                total_bytes=1024,
                download_speed=256.0
            )
            observer.handle_event(progress_event)
            
            # Complete download
            complete_event = DownloadCompletedEvent(
                post_id="test123",
                filename="test.jpg",
                success=True,
                file_size=1024
            )
            observer.handle_event(complete_event)
            
            # Should handle events without errors
            assert "test123" not in observer.download_tasks  # Should be removed after completion
            observer.shutdown()
        except ImportError:
            pytest.skip("Rich library not available")


class TestObserverIntegration:
    """Test suite for observer integration scenarios."""
    
    def test_multiple_observers_same_event(self):
        """Test multiple observers can handle the same event."""
        console_observer = ConsoleObserver(use_rich=False, show_timestamps=False)
        stats_observer = StatisticsObserver()
        json_output = io.StringIO()
        json_observer = JSONObserver(output_file=json_output)
        
        observers = [console_observer, stats_observer, json_observer]
        
        # Create event and send to all observers
        event = PostDiscoveredEvent(post_count=15, target="testuser")
        
        with patch('builtins.print'):  # Suppress console output
            for observer in observers:
                observer.handle_event(event)
        
        # Check each observer processed the event
        assert stats_observer.stats['posts_discovered'] == 15
        
        json_content = json_output.getvalue().strip()
        event_data = json.loads(json_content)
        assert event_data['post_count'] == 15
    
    def test_observer_statistics_tracking(self):
        """Test that observer statistics are tracked correctly."""
        observer = ConsoleObserver(use_rich=False)
        
        # Handle several events
        events = [
            BaseEvent(),
            PostDiscoveredEvent(),
            DownloadStartedEvent(),
        ]
        
        with patch('builtins.print'):  # Suppress output
            for event in events:
                observer.handle_event(event)
        
        stats = observer.get_statistics()
        assert stats['events_received'] == 3
        assert stats['events_processed'] == 3
        assert stats['events_errored'] == 0
        assert stats['last_event_time'] is not None


if __name__ == "__main__":
    pytest.main([__file__])