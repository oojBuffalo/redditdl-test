"""
Error Context and Reporting System

Provides comprehensive error information capture, correlation tracking,
user-friendly message generation, and error analytics.
"""

import json
import logging
import time
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable, Set
from dataclasses import dataclass, field, asdict

from redditdl.core.exceptions import RedditDLError, ErrorCode, ErrorContext, RecoverySuggestion


logger = logging.getLogger(__name__)


@dataclass
class ErrorPattern:
    """Represents a pattern of recurring errors."""
    
    error_type: str
    error_code: Optional[str] = None
    context_pattern: Dict[str, Any] = field(default_factory=dict)
    frequency: int = 1
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    affected_operations: Set[str] = field(default_factory=set)
    
    def matches(self, error: Exception, context: ErrorContext) -> bool:
        """Check if an error matches this pattern."""
        # Check error type
        if self.error_type != type(error).__name__:
            return False
        
        # Check error code if available
        if isinstance(error, RedditDLError) and self.error_code:
            if error.error_code.value != self.error_code:
                return False
        
        # Check context patterns
        for key, expected_value in self.context_pattern.items():
            if not hasattr(context, key):
                continue
            actual_value = getattr(context, key)
            if actual_value != expected_value:
                return False
        
        return True
    
    def update(self, context: ErrorContext) -> None:
        """Update pattern with new occurrence."""
        self.frequency += 1
        self.last_seen = time.time()
        if context.operation:
            self.affected_operations.add(context.operation)


@dataclass
class ErrorCorrelation:
    """Tracks correlated errors across operations."""
    
    correlation_id: str
    primary_error: Dict[str, Any]
    related_errors: List[Dict[str, Any]] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    operations: Set[str] = field(default_factory=set)
    
    def add_related_error(self, error: Exception, context: ErrorContext) -> None:
        """Add a related error to this correlation."""
        error_data = {
            'error_type': type(error).__name__,
            'message': str(error),
            'timestamp': time.time(),
            'context': asdict(context)
        }
        
        if isinstance(error, RedditDLError):
            error_data['error_code'] = error.error_code.value
            error_data['recoverable'] = error.recoverable
        
        self.related_errors.append(error_data)
        self.last_update = time.time()
        
        if context.operation:
            self.operations.add(context.operation)
    
    @property
    def duration(self) -> float:
        """Get the duration of this error correlation."""
        return self.last_update - self.start_time
    
    @property
    def error_count(self) -> int:
        """Get total number of errors in this correlation."""
        return 1 + len(self.related_errors)  # Primary + related


class ErrorReporter:
    """
    Handles error reporting, message generation, and user communication.
    """
    
    def __init__(self):
        self.message_templates = self._load_message_templates()
        self.user_message_cache: Dict[str, str] = {}
        self.report_handlers: List[Callable] = []
    
    def register_report_handler(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register a handler for error reports."""
        self.report_handlers.append(handler)
    
    def generate_user_message(self, error: Exception, context: ErrorContext) -> str:
        """
        Generate a user-friendly error message with actionable suggestions.
        
        Args:
            error: The exception that occurred
            context: Error context information
            
        Returns:
            User-friendly error message
        """
        # Check cache first
        cache_key = f"{type(error).__name__}:{hash(str(error))}"
        if cache_key in self.user_message_cache:
            return self.user_message_cache[cache_key]
        
        # Build message components
        message_parts = []
        
        # Main error description
        if isinstance(error, RedditDLError):
            message_parts.append(f"❌ {error.message}")
            
            # Add context-specific information
            if context.operation:
                message_parts.append(f"   Operation: {context.operation}")
            if context.stage:
                message_parts.append(f"   Stage: {context.stage}")
            if context.target:
                message_parts.append(f"   Target: {context.target}")
            if context.url:
                message_parts.append(f"   URL: {context.url}")
            
            # Add error code and correlation ID
            if error.error_code != ErrorCode.UNKNOWN_ERROR:
                message_parts.append(f"   Error Code: {error.error_code.value}")
            if context.correlation_id:
                message_parts.append(f"   Correlation ID: {context.correlation_id}")
            
            # Add recovery suggestions
            if error.suggestions:
                message_parts.append("\n💡 Suggested solutions:")
                for i, suggestion in enumerate(error.suggestions[:3], 1):
                    message_parts.append(f"   {i}. {suggestion.action}")
                    message_parts.append(f"      {suggestion.description}")
                    if suggestion.command:
                        message_parts.append(f"      💻 Try: {suggestion.command}")
                    if suggestion.url:
                        message_parts.append(f"      🔗 More info: {suggestion.url}")
        else:
            # Generic error handling
            message_parts.append(f"❌ An unexpected error occurred: {error}")
            if context.correlation_id:
                message_parts.append(f"   Correlation ID: {context.correlation_id}")
            
            # Add generic suggestions
            message_parts.append("\n💡 Suggested solutions:")
            message_parts.append("   1. Try running the command again")
            message_parts.append("   2. Check your internet connection")
            message_parts.append("   3. Verify your configuration settings")
        
        # Add help information
        message_parts.append("\n📖 For more help:")
        message_parts.append("   • Run 'redditdl --help' for command options")
        message_parts.append("   • Check the documentation for troubleshooting guides")
        message_parts.append("   • Report issues with the correlation ID if the problem persists")
        
        user_message = "\n".join(message_parts)
        
        # Cache the message
        self.user_message_cache[cache_key] = user_message
        
        return user_message
    
    def generate_debug_report(self, error: Exception, context: ErrorContext) -> Dict[str, Any]:
        """
        Generate comprehensive debug information for error analysis.
        
        Args:
            error: The exception that occurred
            context: Error context information
            
        Returns:
            Debug report dictionary
        """
        report = {
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'error': {
                'type': type(error).__name__,
                'message': str(error),
                'module': getattr(error, '__module__', None)
            },
            'context': asdict(context),
            'system': {
                'platform': context.system_info.get('platform'),
                'python_version': context.system_info.get('python_version'),
                'working_directory': context.system_info.get('cwd')
            }
        }
        
        # Add RedditDL-specific information
        if isinstance(error, RedditDLError):
            report['redditdl'] = {
                'error_code': error.error_code.value,
                'recoverable': error.recoverable,
                'suggestions': [asdict(s) for s in error.suggestions],
                'debug_info': error.get_debug_info()
            }
        
        # Add stack trace
        if hasattr(error, '__traceback__') and error.__traceback__:
            import traceback
            report['stack_trace'] = traceback.format_exception(
                type(error), error, error.__traceback__
            )
        
        return report
    
    def report_error(self, error: Exception, context: ErrorContext, level: str = "error") -> None:
        """
        Report an error through all registered handlers.
        
        Args:
            error: The exception that occurred
            context: Error context information
            level: Log level (debug, info, warning, error, critical)
        """
        report = self.generate_debug_report(error, context)
        report['level'] = level
        
        # Send to all registered handlers
        for handler in self.report_handlers:
            try:
                handler(report)
            except Exception as handler_error:
                logger.error(f"Error report handler failed: {handler_error}")
        
        # Default logging
        log_func = getattr(logger, level, logger.error)
        log_func(f"Error reported: {type(error).__name__}: {error}")
    
    def _load_message_templates(self) -> Dict[str, str]:
        """Load error message templates."""
        return {
            'network_connection': "Unable to connect to Reddit. Please check your internet connection.",
            'auth_failed': "Authentication failed. Please verify your Reddit API credentials.",
            'config_invalid': "Configuration error detected. Please check your settings.",
            'target_not_found': "The specified target was not found or is not accessible.",
            'processing_failed': "Content processing failed. The file may be corrupted or unsupported.",
            'rate_limited': "Rate limit exceeded. Please wait before trying again."
        }


class ErrorAnalytics:
    """
    Provides error pattern detection and analytics capabilities.
    """
    
    def __init__(self):
        self.patterns: List[ErrorPattern] = []
        self.correlations: Dict[str, ErrorCorrelation] = {}
        self.error_history: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        
        # Configuration
        self.max_history_size = 1000
        self.pattern_detection_threshold = 3
        self.correlation_timeout = 300  # 5 minutes
    
    def record_error(self, error: Exception, context: ErrorContext) -> None:
        """
        Record an error for pattern detection and analytics.
        
        Args:
            error: The exception that occurred
            context: Error context information
        """
        with self.lock:
            # Record in history
            error_record = {
                'timestamp': time.time(),
                'error_type': type(error).__name__,
                'message': str(error),
                'context': asdict(context)
            }
            
            if isinstance(error, RedditDLError):
                error_record['error_code'] = error.error_code.value
                error_record['recoverable'] = error.recoverable
            
            self.error_history.append(error_record)
            
            # Trim history if needed
            if len(self.error_history) > self.max_history_size:
                self.error_history = self.error_history[-self.max_history_size:]
            
            # Update patterns
            self._update_patterns(error, context)
            
            # Handle correlations
            self._handle_correlations(error, context)
    
    def _update_patterns(self, error: Exception, context: ErrorContext) -> None:
        """Update error patterns with new error."""
        # Find matching pattern
        matching_pattern = None
        for pattern in self.patterns:
            if pattern.matches(error, context):
                matching_pattern = pattern
                break
        
        if matching_pattern:
            matching_pattern.update(context)
        else:
            # Create new pattern
            new_pattern = ErrorPattern(
                error_type=type(error).__name__,
                error_code=error.error_code.value if isinstance(error, RedditDLError) else None,
                context_pattern={
                    'operation': context.operation,
                    'stage': context.stage
                },
                affected_operations={context.operation} if context.operation else set()
            )
            self.patterns.append(new_pattern)
    
    def _handle_correlations(self, error: Exception, context: ErrorContext) -> None:
        """Handle error correlations."""
        correlation_id = context.correlation_id
        
        if not correlation_id:
            # Create new correlation
            correlation_id = str(uuid.uuid4())[:8]
            context.correlation_id = correlation_id
            
            correlation = ErrorCorrelation(
                correlation_id=correlation_id,
                primary_error={
                    'error_type': type(error).__name__,
                    'message': str(error),
                    'timestamp': time.time(),
                    'context': asdict(context)
                }
            )
            
            if context.operation:
                correlation.operations.add(context.operation)
            
            self.correlations[correlation_id] = correlation
        else:
            # Add to existing correlation
            correlation = self.correlations.get(correlation_id)
            if correlation:
                correlation.add_related_error(error, context)
        
        # Clean up old correlations
        self._cleanup_old_correlations()
    
    def _cleanup_old_correlations(self) -> None:
        """Remove old correlations that have timed out."""
        current_time = time.time()
        expired_correlations = []
        
        for correlation_id, correlation in self.correlations.items():
            if current_time - correlation.last_update > self.correlation_timeout:
                expired_correlations.append(correlation_id)
        
        for correlation_id in expired_correlations:
            del self.correlations[correlation_id]
    
    def get_frequent_patterns(self, min_frequency: Optional[int] = None) -> List[ErrorPattern]:
        """Get error patterns that occur frequently."""
        threshold = min_frequency or self.pattern_detection_threshold
        return [p for p in self.patterns if p.frequency >= threshold]
    
    def get_error_statistics(self, time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """Get error statistics for analysis."""
        cutoff_time = None
        if time_window:
            cutoff_time = time.time() - time_window.total_seconds()
        
        # Filter errors by time window
        relevant_errors = self.error_history
        if cutoff_time:
            relevant_errors = [
                e for e in self.error_history 
                if e['timestamp'] >= cutoff_time
            ]
        
        if not relevant_errors:
            return {'total_errors': 0}
        
        # Calculate statistics
        error_types = defaultdict(int)
        error_codes = defaultdict(int)
        operations = defaultdict(int)
        
        for error in relevant_errors:
            error_types[error['error_type']] += 1
            
            if 'error_code' in error:
                error_codes[error['error_code']] += 1
            
            operation = error['context'].get('operation')
            if operation:
                operations[operation] += 1
        
        return {
            'total_errors': len(relevant_errors),
            'error_types': dict(error_types),
            'error_codes': dict(error_codes),
            'operations': dict(operations),
            'patterns_detected': len(self.get_frequent_patterns()),
            'active_correlations': len(self.correlations),
            'time_window': str(time_window) if time_window else 'all_time'
        }
    
    def generate_analytics_report(self) -> Dict[str, Any]:
        """Generate comprehensive analytics report."""
        return {
            'summary': self.get_error_statistics(),
            'frequent_patterns': [
                {
                    'error_type': p.error_type,
                    'frequency': p.frequency,
                    'affected_operations': list(p.affected_operations),
                    'first_seen': p.first_seen,
                    'last_seen': p.last_seen
                }
                for p in self.get_frequent_patterns()
            ],
            'active_correlations': [
                {
                    'correlation_id': c.correlation_id,
                    'error_count': c.error_count,
                    'duration': c.duration,
                    'operations': list(c.operations)
                }
                for c in self.correlations.values()
            ],
            'recent_errors': self.error_history[-10:] if self.error_history else []
        }


# Global instances
_global_error_reporter = ErrorReporter()
_global_error_analytics = ErrorAnalytics()


def get_error_reporter() -> ErrorReporter:
    """Get the global error reporter."""
    return _global_error_reporter


def get_error_analytics() -> ErrorAnalytics:
    """Get the global error analytics."""
    return _global_error_analytics


def report_error(error: Exception, context: Optional[ErrorContext] = None, level: str = "error") -> None:
    """Convenience function to report an error."""
    if context is None:
        context = ErrorContext()
    
    # Record for analytics
    _global_error_analytics.record_error(error, context)
    
    # Report through handlers
    _global_error_reporter.report_error(error, context, level)


def generate_user_message(error: Exception, context: Optional[ErrorContext] = None) -> str:
    """Convenience function to generate user-friendly error message."""
    if context is None:
        context = ErrorContext()
    
    return _global_error_reporter.generate_user_message(error, context)