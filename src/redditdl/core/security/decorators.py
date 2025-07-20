"""
Security Decorators

Provides decorators for adding security validation and audit logging
to functions and methods across the RedditDL application.
"""

import functools
import inspect
import time
from typing import Any, Callable, Dict, List, Optional, Union
from pathlib import Path

from .validation import InputValidator, SecurityValidationError
from .audit import get_auditor, SecurityEvent, EventType, Severity
from redditdl.core.exceptions import ValidationError, ErrorCode


def validate_args(**validation_rules):
    """
    Decorator to validate function arguments using security validation.
    
    Args:
        **validation_rules: Validation rules for arguments
        
    Example:
        @validate_args(
            url=('url', {}),
            file_path=('path', {'base_path': '/safe/dir'}),
            user_input=('string', {'max_length': 100})
        )
        def my_function(url, file_path, user_input):
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            validator = InputValidator()
            
            # Get function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Validate arguments
            for arg_name, (validation_type, constraints) in validation_rules.items():
                if arg_name in bound_args.arguments:
                    value = bound_args.arguments[arg_name]
                    
                    try:
                        if validation_type == 'url':
                            validator.validate_url(value, **constraints)
                        elif validation_type == 'path':
                            validator.validate_path(value, **constraints)
                        elif validation_type == 'target':
                            validator.validate_target(value)
                        elif validation_type == 'filename':
                            validator.sanitize_filename(value, **constraints)
                        elif validation_type == 'string':
                            validator.validate_config_value(
                                arg_name, value, str, constraints
                            )
                        elif validation_type == 'int':
                            validator.validate_config_value(
                                arg_name, value, int, constraints
                            )
                    except SecurityValidationError as e:
                        auditor = get_auditor()
                        auditor.log_validation_failure(
                            field=arg_name,
                            value=str(value),
                            reason=str(e)
                        )
                        raise
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def audit_operation(operation_type: str, 
                   resource_arg: Optional[str] = None,
                   severity: Severity = Severity.LOW):
    """
    Decorator to audit function operations.
    
    Args:
        operation_type: Type of operation being performed
        resource_arg: Name of argument containing resource identifier
        severity: Severity level for audit event
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            auditor = get_auditor()
            start_time = time.time()
            
            # Get resource value if specified
            resource = None
            if resource_arg:
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                resource = bound_args.arguments.get(resource_arg)
            
            # Get session/user info from kwargs if available
            session_id = kwargs.get('session_id')
            user_id = kwargs.get('user_id')
            
            try:
                result = func(*args, **kwargs)
                
                # Log successful operation
                auditor.log_event(SecurityEvent(
                    event_type=getattr(EventType, operation_type.upper(), EventType.SYSTEM_START),
                    severity=severity,
                    message=f"Operation {operation_type} completed successfully",
                    session_id=session_id,
                    user_id=user_id,
                    resource=str(resource) if resource else None,
                    action=operation_type,
                    result="success",
                    context={
                        'function': func.__name__,
                        'duration': time.time() - start_time
                    }
                ))
                
                return result
                
            except Exception as e:
                # Log failed operation
                auditor.log_event(SecurityEvent(
                    event_type=getattr(EventType, operation_type.upper(), EventType.ERROR_OCCURRED),
                    severity=Severity.HIGH if isinstance(e, SecurityValidationError) else Severity.MEDIUM,
                    message=f"Operation {operation_type} failed: {str(e)}",
                    session_id=session_id,
                    user_id=user_id,
                    resource=str(resource) if resource else None,
                    action=operation_type,
                    result="failure",
                    context={
                        'function': func.__name__,
                        'duration': time.time() - start_time,
                        'error_type': type(e).__name__
                    }
                ))
                raise
        
        return wrapper
    return decorator


def secure_path_access(base_path: Optional[Union[str, Path]] = None):
    """
    Decorator to ensure secure path access for functions dealing with file paths.
    
    Args:
        base_path: Base directory to restrict access to
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            validator = InputValidator()
            auditor = get_auditor()
            
            # Get function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Find path arguments
            for arg_name, value in bound_args.arguments.items():
                if isinstance(value, (str, Path)) and ('path' in arg_name.lower() or 'file' in arg_name.lower()):
                    try:
                        validated_path = validator.validate_path(
                            value,
                            base_path=base_path,
                            allow_create=True
                        )
                        # Update the argument with validated path
                        bound_args.arguments[arg_name] = validated_path
                    except SecurityValidationError as e:
                        auditor.log_security_violation(
                            violation_type="path_access_denied",
                            description=f"Unauthorized path access attempt: {value}",
                            severity=Severity.HIGH,
                            resource=str(value)
                        )
                        raise
            
            # Call function with validated paths
            return func(*bound_args.args, **bound_args.kwargs)
        
        return wrapper
    return decorator


def rate_limit(max_calls: int, time_window: int = 60):
    """
    Decorator to implement rate limiting with security logging.
    
    Args:
        max_calls: Maximum number of calls allowed
        time_window: Time window in seconds
    """
    call_history = []
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            auditor = get_auditor()
            current_time = time.time()
            
            # Clean old entries
            cutoff_time = current_time - time_window
            call_history[:] = [t for t in call_history if t > cutoff_time]
            
            # Check rate limit
            if len(call_history) >= max_calls:
                auditor.log_event(SecurityEvent(
                    event_type=EventType.RATE_LIMIT_HIT,
                    severity=Severity.MEDIUM,
                    message=f"Rate limit exceeded for {func.__name__}",
                    action="rate_limit_check",
                    result="blocked",
                    context={
                        'function': func.__name__,
                        'max_calls': max_calls,
                        'time_window': time_window,
                        'current_calls': len(call_history)
                    }
                ))
                raise SecurityValidationError(
                    f"Rate limit exceeded: {max_calls} calls per {time_window} seconds",
                    security_concern="rate_limit_exceeded",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
            
            # Record call
            call_history.append(current_time)
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def require_auth(permission: Optional[str] = None):
    """
    Decorator to require authentication and authorization.
    
    Args:
        permission: Optional specific permission required
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            auditor = get_auditor()
            
            # Get auth info from kwargs
            session_id = kwargs.get('session_id')
            user_id = kwargs.get('user_id')
            
            # Basic auth check (simplified for this example)
            if not session_id and not user_id:
                auditor.log_event(SecurityEvent(
                    event_type=EventType.ACCESS_DENIED,
                    severity=Severity.HIGH,
                    message=f"Unauthorized access attempt to {func.__name__}",
                    action="authorization_check",
                    result="denied",
                    context={
                        'function': func.__name__,
                        'required_permission': permission
                    }
                ))
                raise SecurityValidationError(
                    "Authentication required",
                    security_concern="unauthorized_access",
                    error_code=ErrorCode.AUTH_MISSING_CREDENTIALS
                )
            
            # Log successful auth
            auditor.log_event(SecurityEvent(
                event_type=EventType.ACCESS_GRANTED,
                severity=Severity.LOW,
                message=f"Access granted to {func.__name__}",
                session_id=session_id,
                user_id=user_id,
                action="authorization_check",
                result="granted",
                context={
                    'function': func.__name__,
                    'required_permission': permission
                }
            ))
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def sanitize_input(**sanitization_rules):
    """
    Decorator to sanitize function inputs.
    
    Args:
        **sanitization_rules: Rules for sanitizing specific arguments
        
    Example:
        @sanitize_input(
            filename='filename',
            text='string'
        )
        def my_function(filename, text):
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            validator = InputValidator()
            
            # Get function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Sanitize arguments
            for arg_name, sanitization_type in sanitization_rules.items():
                if arg_name in bound_args.arguments:
                    value = bound_args.arguments[arg_name]
                    
                    if sanitization_type == 'filename':
                        sanitized = validator.sanitize_filename(value)
                        bound_args.arguments[arg_name] = sanitized
                    elif sanitization_type == 'string':
                        # Basic string sanitization
                        if isinstance(value, str):
                            # Remove control characters except whitespace
                            sanitized = ''.join(
                                c for c in value 
                                if ord(c) >= 32 or c in '\t\n\r'
                            )
                            bound_args.arguments[arg_name] = sanitized
            
            # Call function with sanitized arguments
            return func(*bound_args.args, **bound_args.kwargs)
        
        return wrapper
    return decorator


def log_security_events(func):
    """
    Decorator to automatically log security-relevant events.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        auditor = get_auditor()
        
        # Determine event type based on function name
        func_name = func.__name__.lower()
        if 'auth' in func_name or 'login' in func_name:
            event_type = EventType.AUTH_SUCCESS
        elif 'download' in func_name:
            event_type = EventType.FILE_DOWNLOAD
        elif 'config' in func_name:
            event_type = EventType.CONFIG_LOAD
        elif 'plugin' in func_name:
            event_type = EventType.PLUGIN_EXECUTE
        else:
            event_type = EventType.SYSTEM_START
        
        try:
            result = func(*args, **kwargs)
            
            # Log successful execution
            auditor.log_event(SecurityEvent(
                event_type=event_type,
                severity=Severity.LOW,
                message=f"Function {func.__name__} executed successfully",
                action=func.__name__,
                result="success"
            ))
            
            return result
            
        except Exception as e:
            # Log failed execution
            auditor.log_event(SecurityEvent(
                event_type=EventType.ERROR_OCCURRED,
                severity=Severity.MEDIUM,
                message=f"Function {func.__name__} failed: {str(e)}",
                action=func.__name__,
                result="failure",
                context={'error_type': type(e).__name__}
            ))
            raise
    
    return wrapper


class SecurityContext:
    """
    Context manager for security operations with automatic cleanup and logging.
    """
    
    def __init__(self, operation: str, resource: Optional[str] = None,
                 session_id: Optional[str] = None, user_id: Optional[str] = None):
        self.operation = operation
        self.resource = resource
        self.session_id = session_id
        self.user_id = user_id
        self.auditor = get_auditor()
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        
        # Log operation start
        self.auditor.log_event(SecurityEvent(
            event_type=EventType.SYSTEM_START,
            severity=Severity.LOW,
            message=f"Starting security operation: {self.operation}",
            session_id=self.session_id,
            user_id=self.user_id,
            resource=self.resource,
            action=self.operation,
            context={'operation_start': True}
        ))
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time if self.start_time else 0
        
        if exc_type is None:
            # Success
            self.auditor.log_event(SecurityEvent(
                event_type=EventType.SYSTEM_START,
                severity=Severity.LOW,
                message=f"Security operation completed: {self.operation}",
                session_id=self.session_id,
                user_id=self.user_id,
                resource=self.resource,
                action=self.operation,
                result="success",
                context={
                    'operation_end': True,
                    'duration': duration
                }
            ))
        else:
            # Error occurred
            self.auditor.log_event(SecurityEvent(
                event_type=EventType.ERROR_OCCURRED,
                severity=Severity.HIGH if issubclass(exc_type, SecurityValidationError) else Severity.MEDIUM,
                message=f"Security operation failed: {self.operation} - {str(exc_val)}",
                session_id=self.session_id,
                user_id=self.user_id,
                resource=self.resource,
                action=self.operation,
                result="failure",
                context={
                    'operation_end': True,
                    'duration': duration,
                    'error_type': exc_type.__name__ if exc_type else None
                }
            ))
        
        return False  # Don't suppress exceptions