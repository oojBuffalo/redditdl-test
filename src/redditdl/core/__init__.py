"""
Core RedditDL Package

Contains core infrastructure components including pipeline, events, plugins, 
configuration, state management, and error handling.
"""

from redditdl.core.exceptions import (
    RedditDLError,
    NetworkError,
    ConfigurationError,
    AuthenticationError,
    ProcessingError,
    ValidationError,
    ErrorCode,
    ErrorContext,
    RecoverySuggestion
)

from redditdl.core.error_recovery import (
    ErrorRecoveryManager,
    RecoveryStrategy,
    RecoveryResult,
    OperationState,
    get_recovery_manager,
    error_boundary
)

from redditdl.core.error_context import (
    ErrorReporter,
    ErrorAnalytics,
    get_error_reporter,
    get_error_analytics,
    report_error,
    generate_user_message
)

__version__ = "0.2.0"

__all__ = [
    # Exception classes
    'RedditDLError',
    'NetworkError', 
    'ConfigurationError',
    'AuthenticationError',
    'ProcessingError',
    'ValidationError',
    'ErrorCode',
    'ErrorContext',
    'RecoverySuggestion',
    
    # Error recovery
    'ErrorRecoveryManager',
    'RecoveryStrategy',
    'RecoveryResult',
    'OperationState',
    'get_recovery_manager',
    'error_boundary',
    
    # Error reporting and analytics
    'ErrorReporter',
    'ErrorAnalytics',
    'get_error_reporter',
    'get_error_analytics',
    'report_error',
    'generate_user_message'
]