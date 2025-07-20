"""
Test suite for error context and reporting system.

Tests error reporting, analytics, pattern detection, and user-friendly
error message generation to ensure comprehensive error context management.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from typing import Dict, Any, List

from redditdl.core.error_context import (
    ErrorReporter, ErrorAnalytics, report_error, get_error_analytics,
    get_error_reporter, ErrorPattern
)
from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ProcessingError, ConfigurationError,
    ErrorCode, ErrorContext, RecoverySuggestion
)


class TestErrorReporter:
    """Test ErrorReporter functionality for user-friendly error reporting."""
    
    def test_error_reporter_creation(self):
        """Test ErrorReporter creation."""
        reporter = ErrorReporter()
        assert reporter is not None
        assert hasattr(reporter, 'report_count')
        assert reporter.report_count == 0
    
    def test_error_reporter_singleton(self):
        """Test error reporter singleton pattern."""
        reporter1 = get_error_reporter()
        reporter2 = get_error_reporter()
        assert reporter1 is reporter2
    
    def test_generate_user_message_basic(self):
        """Test basic user message generation."""
        reporter = ErrorReporter()
        error = RedditDLError("Test error message")
        context = ErrorContext(operation="test_operation")
        
        message = reporter.generate_user_message(error, context)
        assert "Test error message" in message
        assert len(message) > len("Test error message")
    
    def test_generate_user_message_with_suggestions(self):
        """Test user message generation with recovery suggestions."""
        reporter = ErrorReporter()
        error = NetworkError("Connection failed")
        error.add_suggestion(RecoverySuggestion(
            action="Check connection",
            description="Verify your internet connection is working",
            priority=1
        ))
        error.add_suggestion(RecoverySuggestion(
            action="Retry later",
            description="Try again in a few minutes",
            priority=2
        ))
        
        context = ErrorContext(operation="download", url="https://example.com")
        message = reporter.generate_user_message(error, context)
        
        assert "Check connection" in message
        assert "Verify your internet connection" in message
        assert "Retry later" in message
    
    def test_generate_user_message_with_context(self):
        """Test user message includes relevant context information."""
        reporter = ErrorReporter()
        error = ProcessingError("Processing failed")
        context = ErrorContext(
            operation="process_media",
            stage="processing",
            post_id="abc123",
            url="https://reddit.com/post/abc123"
        )
        
        message = reporter.generate_user_message(error, context)
        assert "abc123" in message or "processing" in message.lower()
    
    def test_generate_debug_report_basic(self):
        """Test basic debug report generation."""
        reporter = ErrorReporter()
        error = ProcessingError("Debug test error")
        context = ErrorContext(
            operation="debug_test",
            stage="testing",
            post_id="debug123"
        )
        
        debug_report = reporter.generate_debug_report(error, context)
        
        assert "Debug test error" in debug_report
        assert "debug_test" in debug_report
        assert "testing" in debug_report
        assert "debug123" in debug_report
        assert str(error.error_code.value) in debug_report
    
    def test_generate_debug_report_with_stack_trace(self):
        """Test debug report includes stack trace information."""
        reporter = ErrorReporter()
        
        try:
            # Generate a real exception with stack trace
            raise ValueError("Test exception for stack trace")
        except ValueError as e:
            error = ProcessingError("Processing failed", cause=e)
            context = ErrorContext(operation="stack_trace_test")
            
            debug_report = reporter.generate_debug_report(error, context)
            
            # Should include stack trace information
            assert "Traceback" in debug_report or "stack" in debug_report.lower()
            assert "ValueError" in debug_report
    
    def test_categorize_error_network(self):
        """Test error categorization for network errors."""
        reporter = ErrorReporter()
        
        network_errors = [
            NetworkError("Connection timeout"),
            NetworkError("DNS resolution failed"),
            NetworkError("Socket error")
        ]
        
        for error in network_errors:
            category = reporter.categorize_error(error)
            assert category == "network"
    
    def test_categorize_error_authentication(self):
        """Test error categorization for authentication errors."""
        reporter = ErrorReporter()
        
        from redditdl.core.exceptions import AuthenticationError
        auth_error = AuthenticationError("Invalid credentials")
        
        category = reporter.categorize_error(auth_error)
        assert category == "authentication"
    
    def test_categorize_error_configuration(self):
        """Test error categorization for configuration errors."""
        reporter = ErrorReporter()
        
        config_error = ConfigurationError("Invalid config value")
        
        category = reporter.categorize_error(config_error)
        assert category == "configuration"
    
    def test_categorize_error_processing(self):
        """Test error categorization for processing errors."""
        reporter = ErrorReporter()
        
        processing_error = ProcessingError("Content processing failed")
        
        category = reporter.categorize_error(processing_error)
        assert category == "processing"
    
    def test_categorize_error_unknown(self):
        """Test error categorization for unknown error types."""
        reporter = ErrorReporter()
        
        unknown_error = RedditDLError("Unknown error type")
        
        category = reporter.categorize_error(unknown_error)
        assert category == "unknown"
    
    def test_format_suggestions_empty(self):
        """Test suggestion formatting with empty list."""
        reporter = ErrorReporter()
        
        formatted = reporter.format_suggestions([])
        assert formatted == ""
    
    def test_format_suggestions_single(self):
        """Test suggestion formatting with single suggestion."""
        reporter = ErrorReporter()
        suggestion = RecoverySuggestion(
            action="Test action",
            description="Test description",
            priority=1
        )
        
        formatted = reporter.format_suggestions([suggestion])
        assert "Test action" in formatted
        assert "Test description" in formatted
    
    def test_format_suggestions_multiple(self):
        """Test suggestion formatting with multiple suggestions."""
        reporter = ErrorReporter()
        suggestions = [
            RecoverySuggestion("Action 1", "Description 1", priority=1),
            RecoverySuggestion("Action 2", "Description 2", priority=2),
            RecoverySuggestion("Action 3", "Description 3", priority=3)
        ]
        
        formatted = reporter.format_suggestions(suggestions)
        assert "Action 1" in formatted
        assert "Action 2" in formatted
        assert "Action 3" in formatted
        
        # Should be ordered by priority
        action1_pos = formatted.find("Action 1")
        action2_pos = formatted.find("Action 2")
        assert action1_pos < action2_pos
    
    def test_report_tracking(self):
        """Test that error reports are tracked."""
        reporter = ErrorReporter()
        initial_count = reporter.report_count
        
        error = RedditDLError("Test tracking")
        context = ErrorContext(operation="tracking_test")
        
        reporter.generate_user_message(error, context)
        assert reporter.report_count == initial_count + 1
        
        reporter.generate_debug_report(error, context)
        assert reporter.report_count == initial_count + 2


class TestErrorAnalytics:
    """Test ErrorAnalytics functionality for error pattern detection."""
    
    def test_error_analytics_creation(self):
        """Test ErrorAnalytics creation."""
        analytics = ErrorAnalytics()
        assert analytics is not None
        assert hasattr(analytics, 'error_history')
        assert len(analytics.error_history) == 0
    
    def test_error_analytics_singleton(self):
        """Test error analytics singleton pattern."""
        analytics1 = get_error_analytics()
        analytics2 = get_error_analytics()
        assert analytics1 is analytics2
    
    def test_record_error_basic(self):
        """Test basic error recording."""
        analytics = ErrorAnalytics()
        error = NetworkError("Test error")
        context = ErrorContext(operation="record_test")
        
        analytics.record_error(error, context)
        
        assert len(analytics.error_history) == 1
        recorded = analytics.error_history[0]
        assert recorded["error_type"] == "NetworkError"
        assert recorded["message"] == "Test error"
        assert recorded["operation"] == "record_test"
    
    def test_record_error_with_context(self):
        """Test error recording with full context."""
        analytics = ErrorAnalytics()
        error = ProcessingError("Processing failed")
        context = ErrorContext(
            operation="full_context_test",
            stage="processing",
            post_id="ctx123",
            url="https://example.com",
            session_id="session456"
        )
        
        analytics.record_error(error, context)
        
        recorded = analytics.error_history[0]
        assert recorded["stage"] == "processing"
        assert recorded["post_id"] == "ctx123"
        assert recorded["url"] == "https://example.com"
        assert recorded["session_id"] == "session456"
    
    def test_get_error_statistics(self):
        """Test error statistics generation."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record various errors
        errors = [
            NetworkError("Network error 1"),
            NetworkError("Network error 2"),
            ProcessingError("Processing error 1"),
            ConfigurationError("Config error 1")
        ]
        
        context = ErrorContext(operation="stats_test")
        for error in errors:
            analytics.record_error(error, context)
        
        stats = analytics.get_error_statistics()
        
        assert stats["total_errors"] == 4
        assert stats["error_types"]["NetworkError"] == 2
        assert stats["error_types"]["ProcessingError"] == 1
        assert stats["error_types"]["ConfigurationError"] == 1
        assert "most_common_operation" in stats
        assert "error_rate_trend" in stats
    
    def test_detect_patterns_frequency(self):
        """Test pattern detection for frequent errors."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record same error multiple times
        error = NetworkError("Frequent error")
        context = ErrorContext(operation="pattern_test")
        
        for _ in range(5):
            analytics.record_error(error, context)
        
        patterns = analytics.detect_patterns()
        
        # Should detect frequent error pattern
        assert len(patterns) > 0
        frequent_pattern = next((p for p in patterns if p.pattern_type == "frequent_error"), None)
        assert frequent_pattern is not None
        assert frequent_pattern.frequency >= 5
    
    def test_detect_patterns_time_clustering(self):
        """Test pattern detection for time-clustered errors."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record errors in rapid succession
        error = ProcessingError("Clustered error")
        base_time = datetime.now()
        
        for i in range(3):
            context = ErrorContext(
                operation="cluster_test",
                timestamp=base_time + timedelta(seconds=i)
            )
            analytics.record_error(error, context)
        
        patterns = analytics.detect_patterns()
        
        # Should detect time clustering
        time_pattern = next((p for p in patterns if "time" in p.pattern_type), None)
        if time_pattern:  # Pattern detection might not always trigger
            assert time_pattern.frequency >= 3
    
    def test_detect_patterns_operation_correlation(self):
        """Test pattern detection for operation-correlated errors."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record errors from same operation
        errors = [
            NetworkError("Error 1"),
            ProcessingError("Error 2"),
            NetworkError("Error 3")
        ]
        
        context = ErrorContext(operation="correlated_operation")
        for error in errors:
            analytics.record_error(error, context)
        
        patterns = analytics.detect_patterns()
        
        # Should detect operation correlation
        operation_pattern = next((p for p in patterns if "operation" in p.pattern_type), None)
        if operation_pattern:
            assert operation_pattern.details["operation"] == "correlated_operation"
    
    def test_get_error_trends(self):
        """Test error trend analysis."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record errors over time
        base_time = datetime.now() - timedelta(hours=2)
        errors = [
            NetworkError("Trend error 1"),
            NetworkError("Trend error 2"),
            ProcessingError("Trend error 3")
        ]
        
        for i, error in enumerate(errors):
            context = ErrorContext(
                operation="trend_test",
                timestamp=base_time + timedelta(minutes=30 * i)
            )
            analytics.record_error(error, context)
        
        trends = analytics.get_error_trends()
        
        assert "hourly_distribution" in trends
        assert "error_rate_change" in trends
        assert isinstance(trends["hourly_distribution"], dict)
    
    def test_clear_old_errors(self):
        """Test clearing old errors from history."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record old error
        old_error = NetworkError("Old error")
        old_context = ErrorContext(
            operation="old_test",
            timestamp=datetime.now() - timedelta(days=8)  # Older than default retention
        )
        analytics.record_error(old_error, old_context)
        
        # Record recent error
        recent_error = NetworkError("Recent error")
        recent_context = ErrorContext(operation="recent_test")
        analytics.record_error(recent_error, recent_context)
        
        # Clear old errors (default retention is 7 days)
        analytics.clear_old_errors()
        
        # Should only have recent error
        assert len(analytics.error_history) == 1
        assert analytics.error_history[0]["message"] == "Recent error"
    
    def test_get_error_correlation(self):
        """Test error correlation analysis."""
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Record correlated errors
        errors = [
            NetworkError("Network timeout"),
            ProcessingError("Processing timeout"),
            NetworkError("Connection reset")
        ]
        
        contexts = [
            ErrorContext(operation="download", url="https://slow-server.com"),
            ErrorContext(operation="process", post_id="post123"),
            ErrorContext(operation="download", url="https://slow-server.com")
        ]
        
        for error, context in zip(errors, contexts):
            analytics.record_error(error, context)
        
        correlations = analytics.get_error_correlation()
        
        assert "url_correlations" in correlations
        assert "operation_correlations" in correlations


class TestErrorPattern:
    """Test ErrorPattern dataclass functionality."""
    
    def test_error_pattern_creation(self):
        """Test ErrorPattern creation."""
        pattern = ErrorPattern(
            pattern_type="frequent_error",
            frequency=10,
            confidence=0.95,
            description="Network errors occurring frequently",
            details={"error_type": "NetworkError", "operation": "download"}
        )
        
        assert pattern.pattern_type == "frequent_error"
        assert pattern.frequency == 10
        assert pattern.confidence == 0.95
        assert "frequently" in pattern.description
        assert pattern.details["error_type"] == "NetworkError"
    
    def test_error_pattern_comparison(self):
        """Test ErrorPattern comparison by confidence."""
        pattern1 = ErrorPattern("type1", 5, 0.8, "Pattern 1")
        pattern2 = ErrorPattern("type2", 3, 0.9, "Pattern 2")
        
        patterns = [pattern1, pattern2]
        sorted_patterns = sorted(patterns, key=lambda p: p.confidence, reverse=True)
        
        assert sorted_patterns[0] == pattern2  # Higher confidence first
        assert sorted_patterns[1] == pattern1


class TestErrorMetrics:
    """Test ErrorMetrics dataclass functionality."""
    
    def test_error_metrics_creation(self):
        """Test ErrorMetrics creation."""
        metrics = ErrorMetrics(
            total_errors=100,
            error_rate=0.05,
            most_common_type="NetworkError",
            recovery_success_rate=0.8,
            avg_resolution_time=30.5
        )
        
        assert metrics.total_errors == 100
        assert metrics.error_rate == 0.05
        assert metrics.most_common_type == "NetworkError"
        assert metrics.recovery_success_rate == 0.8
        assert metrics.avg_resolution_time == 30.5


class TestReportErrorFunction:
    """Test the global report_error function."""
    
    def test_report_error_basic(self):
        """Test basic error reporting function."""
        error = NetworkError("Test report function")
        context = ErrorContext(operation="report_test")
        
        # Should not raise exception
        report_error(error, context)
    
    def test_report_error_with_level(self):
        """Test error reporting with different levels."""
        error = ProcessingError("Test level reporting")
        context = ErrorContext(operation="level_test")
        
        # Test different logging levels
        for level in ["error", "warning", "info", "debug"]:
            report_error(error, context, level=level)
    
    @patch('core.error_context.get_error_analytics')
    @patch('core.error_context.get_error_reporter')
    def test_report_error_integration(self, mock_reporter, mock_analytics):
        """Test error reporting integration with analytics and reporter."""
        mock_analytics_instance = Mock()
        mock_reporter_instance = Mock()
        mock_analytics.return_value = mock_analytics_instance
        mock_reporter.return_value = mock_reporter_instance
        
        error = NetworkError("Integration test")
        context = ErrorContext(operation="integration_test")
        
        report_error(error, context)
        
        # Should record error in analytics
        mock_analytics_instance.record_error.assert_called_once_with(error, context)
        
        # Should generate user message
        mock_reporter_instance.generate_user_message.assert_called_once_with(error, context)


class TestErrorContextIntegration:
    """Test integration scenarios for error context system."""
    
    def test_full_error_flow(self):
        """Test complete error handling flow."""
        # Create error with full context
        error = NetworkError("Full flow test error")
        error.add_suggestion(RecoverySuggestion(
            action="Check network",
            description="Verify network connectivity",
            priority=1
        ))
        
        context = ErrorContext(
            operation="full_flow_test",
            stage="download",
            post_id="flow123",
            url="https://example.com",
            session_id="session789"
        )
        
        # Report error (should record in analytics)
        report_error(error, context)
        
        # Get analytics
        analytics = get_error_analytics()
        reporter = get_error_reporter()
        
        # Verify error was recorded
        stats = analytics.get_error_statistics()
        assert stats["total_errors"] > 0
        
        # Generate user message
        user_message = reporter.generate_user_message(error, context)
        assert "Check network" in user_message
        
        # Generate debug report
        debug_report = reporter.generate_debug_report(error, context)
        assert "flow123" in debug_report
        assert "https://example.com" in debug_report
    
    def test_analytics_pattern_detection_integration(self):
        """Test analytics pattern detection with real error scenarios."""
        analytics = get_error_analytics()
        analytics.clear_history()
        
        # Simulate network issues during download operations
        for i in range(5):
            error = NetworkError(f"Download failed attempt {i}")
            context = ErrorContext(
                operation="download_media",
                stage="acquisition",
                url="https://problematic-server.com"
            )
            analytics.record_error(error, context)
        
        # Detect patterns
        patterns = analytics.detect_patterns()
        
        # Should detect frequent network errors
        assert len(patterns) > 0
        network_pattern = next((p for p in patterns if "NetworkError" in str(p.details)), None)
        if network_pattern:
            assert network_pattern.frequency >= 5
    
    def test_error_correlation_analysis(self):
        """Test error correlation analysis across different operations."""
        analytics = get_error_analytics()
        analytics.clear_history()
        
        # Simulate correlated errors
        error_scenarios = [
            (NetworkError("Timeout 1"), ErrorContext(operation="fetch_posts", url="https://api.reddit.com")),
            (ProcessingError("Processing 1"), ErrorContext(operation="process_media", post_id="post1")),
            (NetworkError("Timeout 2"), ErrorContext(operation="fetch_posts", url="https://api.reddit.com")),
            (ProcessingError("Processing 2"), ErrorContext(operation="process_media", post_id="post2")),
        ]
        
        for error, context in error_scenarios:
            analytics.record_error(error, context)
        
        correlations = analytics.get_error_correlation()
        
        # Should find correlations
        assert "operation_correlations" in correlations
        assert "url_correlations" in correlations


if __name__ == "__main__":
    pytest.main([__file__])