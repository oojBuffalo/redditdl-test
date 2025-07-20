"""
Security and Validation Module for RedditDL

Provides comprehensive input validation, security hardening, and audit logging
capabilities to protect against common security vulnerabilities.
"""

from .validation import InputValidator, SecurityValidationError
from .audit import SecurityAuditor, SecurityEvent, EventType, Severity
from .file_ops import SecureFileOperations, get_secure_ops
from .plugin_security import PluginSecurityScanner
from .decorators import (
    validate_args, audit_operation, secure_path_access, 
    rate_limit, require_auth, sanitize_input, 
    log_security_events, SecurityContext
)

__all__ = [
    'InputValidator',
    'SecurityValidationError', 
    'SecurityAuditor',
    'SecurityEvent',
    'EventType',
    'Severity',
    'SecureFileOperations',
    'get_secure_ops',
    'PluginSecurityScanner',
    'validate_args',
    'audit_operation',
    'secure_path_access',
    'rate_limit',
    'require_auth',
    'sanitize_input',
    'log_security_events',
    'SecurityContext'
]