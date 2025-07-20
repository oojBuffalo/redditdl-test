"""
Core Exception Hierarchy for RedditDL

Provides comprehensive error classification with error codes, recovery suggestions,
and detailed context information for improved debugging and user experience.
"""

import sys
import traceback
import time
from enum import Enum
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field


class ErrorCode(Enum):
    """Standard error codes for different error categories."""
    
    # Network related errors (1000-1999)
    NETWORK_CONNECTION_FAILED = 1001
    NETWORK_TIMEOUT = 1002
    NETWORK_DNS_RESOLUTION = 1003
    NETWORK_SSL_ERROR = 1004
    NETWORK_RATE_LIMITED = 1005
    NETWORK_INVALID_RESPONSE = 1006
    
    # Authentication errors (2000-2999)  
    AUTH_INVALID_CREDENTIALS = 2001
    AUTH_EXPIRED_TOKEN = 2002
    AUTH_INSUFFICIENT_PERMISSIONS = 2003
    AUTH_OAUTH_FAILED = 2004
    AUTH_MISSING_CREDENTIALS = 2005
    AUTH_ACCOUNT_SUSPENDED = 2006
    
    # Configuration errors (3000-3999)
    CONFIG_INVALID_FORMAT = 3001
    CONFIG_MISSING_REQUIRED = 3002
    CONFIG_INVALID_VALUE = 3003
    CONFIG_FILE_NOT_FOUND = 3004
    CONFIG_PERMISSION_DENIED = 3005
    CONFIG_SCHEMA_VALIDATION = 3006
    
    # Processing errors (4000-4999)
    PROCESSING_INVALID_CONTENT = 4001
    PROCESSING_UNSUPPORTED_FORMAT = 4002
    PROCESSING_DEPENDENCY_MISSING = 4003
    PROCESSING_INSUFFICIENT_RESOURCES = 4004
    PROCESSING_CORRUPT_DATA = 4005
    PROCESSING_OPERATION_FAILED = 4006
    
    # Validation errors (5000-5999)
    VALIDATION_INVALID_INPUT = 5001
    VALIDATION_MISSING_FIELD = 5002
    VALIDATION_TYPE_MISMATCH = 5003
    VALIDATION_RANGE_ERROR = 5004
    VALIDATION_FORMAT_ERROR = 5005
    VALIDATION_CONSTRAINT_VIOLATION = 5006
    
    # File system errors (6000-6999)
    FS_FILE_NOT_FOUND = 6001
    FS_PERMISSION_DENIED = 6002
    FS_DISK_FULL = 6003
    FS_INVALID_PATH = 6004
    FS_DIRECTORY_NOT_EMPTY = 6005
    FS_SYMLINK_LOOP = 6006
    
    # Plugin errors (7000-7999)
    PLUGIN_NOT_FOUND = 7001
    PLUGIN_LOAD_FAILED = 7002
    PLUGIN_VALIDATION_FAILED = 7003
    PLUGIN_EXECUTION_FAILED = 7004
    PLUGIN_DEPENDENCY_MISSING = 7005
    PLUGIN_SECURITY_VIOLATION = 7006
    
    # Target/scraping errors (8000-8999)
    TARGET_NOT_FOUND = 8001
    TARGET_ACCESS_DENIED = 8002
    TARGET_INVALID_FORMAT = 8003
    TARGET_CONTENT_UNAVAILABLE = 8004
    TARGET_RATE_LIMITED = 8005
    TARGET_SUSPENDED = 8006
    
    # Generic/unknown errors (9000-9999)
    UNKNOWN_ERROR = 9000
    INTERNAL_ERROR = 9001
    OPERATION_CANCELLED = 9002
    OPERATION_TIMEOUT = 9003


@dataclass
class ErrorContext:
    """Contextual information about an error occurrence."""
    
    operation: str = ""
    stage: str = ""
    post_id: Optional[str] = None
    url: Optional[str] = None
    file_path: Optional[str] = None
    target: Optional[str] = None
    session_id: Optional[str] = None
    correlation_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    system_info: Dict[str, Any] = field(default_factory=dict)
    user_context: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for serialization."""
        return {
            'operation': self.operation,
            'stage': self.stage,
            'post_id': self.post_id,
            'url': self.url,
            'file_path': self.file_path,
            'target': self.target,
            'session_id': self.session_id,
            'correlation_id': self.correlation_id,
            'timestamp': self.timestamp,
            'system_info': self.system_info,
            'user_context': self.user_context
        }


@dataclass
class RecoverySuggestion:
    """Structured recovery suggestion for error resolution."""
    
    action: str  # Brief action description
    description: str  # Detailed explanation
    automatic: bool = False  # Whether this can be attempted automatically
    command: Optional[str] = None  # CLI command to resolve
    url: Optional[str] = None  # Documentation URL
    priority: int = 1  # Priority order (1=highest)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert suggestion to dictionary."""
        return {
            'action': self.action,
            'description': self.description,
            'automatic': self.automatic,
            'command': self.command,
            'url': self.url,
            'priority': self.priority
        }


class RedditDLError(Exception):
    """
    Base exception for all RedditDL errors.
    
    Provides comprehensive error information including error codes,
    recovery suggestions, and detailed context for debugging.
    """
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.UNKNOWN_ERROR,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
        recoverable: bool = True,
        suggestions: Optional[List[RecoverySuggestion]] = None
    ):
        """
        Initialize RedditDL error.
        
        Args:
            message: Human-readable error description
            error_code: Standardized error code
            context: Contextual information about the error
            cause: Original exception that caused this error
            recoverable: Whether the error can potentially be recovered
            suggestions: List of recovery suggestions
        """
        super().__init__(message)
        
        self.message = message
        self.error_code = error_code
        self.context = context or ErrorContext()
        self.cause = cause
        self.recoverable = recoverable
        self.suggestions = suggestions or []
        self.stack_trace = traceback.format_exc()
        
        # Auto-generate correlation ID if not provided
        if not self.context.correlation_id:
            import uuid
            self.context.correlation_id = str(uuid.uuid4())[:8]
        
        # Capture system information
        if not self.context.system_info:
            self.context.system_info = {
                'platform': sys.platform,
                'python_version': sys.version,
                'cwd': sys.path[0] if sys.path else None
            }
    
    def add_suggestion(self, suggestion: RecoverySuggestion) -> None:
        """Add a recovery suggestion to the error."""
        self.suggestions.append(suggestion)
        # Sort by priority
        self.suggestions.sort(key=lambda s: s.priority)
    
    def get_user_message(self) -> str:
        """Get user-friendly error message with suggestions."""
        lines = [f"Error: {self.message}"]
        
        if self.error_code != ErrorCode.UNKNOWN_ERROR:
            lines.append(f"Error Code: {self.error_code.value}")
        
        if self.context.correlation_id:
            lines.append(f"Correlation ID: {self.context.correlation_id}")
        
        if self.suggestions:
            lines.append("\nSuggested solutions:")
            for i, suggestion in enumerate(self.suggestions[:3], 1):
                lines.append(f"  {i}. {suggestion.action}")
                lines.append(f"     {suggestion.description}")
                if suggestion.command:
                    lines.append(f"     Command: {suggestion.command}")
        
        return "\n".join(lines)
    
    def get_debug_info(self) -> Dict[str, Any]:
        """Get comprehensive debug information."""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'error_code': self.error_code.value,
            'recoverable': self.recoverable,
            'context': self.context.to_dict(),
            'cause': {
                'type': type(self.cause).__name__ if self.cause else None,
                'message': str(self.cause) if self.cause else None
            },
            'suggestions': [s.to_dict() for s in self.suggestions],
            'stack_trace': self.stack_trace
        }


class NetworkError(RedditDLError):
    """Exception for network-related errors."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.NETWORK_CONNECTION_FAILED,
        url: Optional[str] = None,
        status_code: Optional[int] = None,
        **kwargs
    ):
        # Set URL in context if provided
        context = kwargs.get('context') or ErrorContext()
        if url:
            context.url = url
            context.user_context['status_code'] = status_code
        
        kwargs['context'] = context
        kwargs['error_code'] = error_code
        
        super().__init__(message, **kwargs)
        
        # Add standard network error suggestions
        if error_code == ErrorCode.NETWORK_CONNECTION_FAILED:
            self.add_suggestion(RecoverySuggestion(
                action="Check internet connection",
                description="Verify your internet connection is working and try again.",
                automatic=False,
                priority=1
            ))
            self.add_suggestion(RecoverySuggestion(
                action="Retry with backoff",
                description="The operation will be retried automatically with exponential backoff.",
                automatic=True,
                priority=2
            ))
        elif error_code == ErrorCode.NETWORK_RATE_LIMITED:
            self.add_suggestion(RecoverySuggestion(
                action="Wait and retry",
                description="Rate limit exceeded. Wait for the rate limit to reset.",
                automatic=True,
                priority=1
            ))


class ConfigurationError(RedditDLError):
    """Exception for configuration-related errors."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.CONFIG_INVALID_FORMAT,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        **kwargs
    ):
        context = kwargs.get('context') or ErrorContext()
        if config_key:
            context.user_context['config_key'] = config_key
            context.user_context['config_value'] = config_value
        
        kwargs['context'] = context
        kwargs['error_code'] = error_code
        
        super().__init__(message, **kwargs)
        
        # Add standard configuration error suggestions
        if error_code == ErrorCode.CONFIG_FILE_NOT_FOUND:
            self.add_suggestion(RecoverySuggestion(
                action="Create configuration file",
                description="Create a configuration file using the default template.",
                command="redditdl config init",
                automatic=False,
                priority=1
            ))
        elif error_code == ErrorCode.CONFIG_INVALID_VALUE:
            self.add_suggestion(RecoverySuggestion(
                action="Check configuration values",
                description="Review the configuration file for invalid values and correct them.",
                command="redditdl config validate",
                automatic=False,
                priority=1
            ))


class AuthenticationError(RedditDLError):
    """Exception for authentication-related errors."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.AUTH_INVALID_CREDENTIALS,
        auth_method: Optional[str] = None,
        **kwargs
    ):
        context = kwargs.get('context') or ErrorContext()
        if auth_method:
            context.user_context['auth_method'] = auth_method
        
        kwargs['context'] = context
        kwargs['error_code'] = error_code
        
        super().__init__(message, **kwargs)
        
        # Add standard authentication error suggestions
        if error_code == ErrorCode.AUTH_INVALID_CREDENTIALS:
            self.add_suggestion(RecoverySuggestion(
                action="Check credentials",
                description="Verify your Reddit API credentials are correct.",
                url="https://www.reddit.com/prefs/apps",
                automatic=False,
                priority=1
            ))
            self.add_suggestion(RecoverySuggestion(
                action="Use public mode",
                description="Try running without API credentials (public mode).",
                command="redditdl scrape --no-api",
                automatic=False,
                priority=2
            ))


class ProcessingError(RedditDLError):
    """Exception for content processing errors."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.PROCESSING_OPERATION_FAILED,
        content_type: Optional[str] = None,
        **kwargs
    ):
        context = kwargs.get('context') or ErrorContext()
        if content_type:
            context.user_context['content_type'] = content_type
        
        kwargs['context'] = context
        kwargs['error_code'] = error_code
        
        super().__init__(message, **kwargs)


class ValidationError(RedditDLError):
    """Exception for input validation errors."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.VALIDATION_INVALID_INPUT,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        **kwargs
    ):
        context = kwargs.get('context') or ErrorContext()
        if field_name:
            context.user_context['field_name'] = field_name
            context.user_context['field_value'] = field_value
        
        kwargs['context'] = context
        kwargs['error_code'] = error_code
        
        super().__init__(message, **kwargs)


# Convenience functions for creating common errors
def network_error(message: str, url: Optional[str] = None, **kwargs) -> NetworkError:
    """Create a network error with standard suggestions."""
    return NetworkError(message, url=url, **kwargs)


def auth_error(message: str, **kwargs) -> AuthenticationError:
    """Create an authentication error with standard suggestions."""
    return AuthenticationError(message, **kwargs)


def config_error(message: str, key: Optional[str] = None, **kwargs) -> ConfigurationError:
    """Create a configuration error with standard suggestions."""
    return ConfigurationError(message, config_key=key, **kwargs)


def processing_error(message: str, content_type: Optional[str] = None, **kwargs) -> ProcessingError:
    """Create a processing error with context."""
    return ProcessingError(message, content_type=content_type, **kwargs)


def validation_error(message: str, field: Optional[str] = None, **kwargs) -> ValidationError:
    """Create a validation error with field context."""
    return ValidationError(message, field_name=field, **kwargs)