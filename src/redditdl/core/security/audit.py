"""
Security Audit Logging System

Provides comprehensive security event logging, audit trail maintenance,
and suspicious activity detection for RedditDL operations.
"""

import json
import time
import threading
import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field, asdict
from collections import defaultdict, deque

from redditdl.core.exceptions import ErrorContext


class EventType(Enum):
    """Security event types for categorization and filtering."""
    
    # Authentication events
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    AUTH_TOKEN_REFRESH = "auth_token_refresh"
    AUTH_LOGOUT = "auth_logout"
    
    # Access control events
    ACCESS_GRANTED = "access_granted"
    ACCESS_DENIED = "access_denied"
    PERMISSION_CHECK = "permission_check"
    
    # File operations
    FILE_READ = "file_read"
    FILE_WRITE = "file_write"
    FILE_DELETE = "file_delete"
    FILE_DOWNLOAD = "file_download"
    FILE_UPLOAD = "file_upload"
    
    # Configuration events
    CONFIG_LOAD = "config_load"
    CONFIG_CHANGE = "config_change"
    CONFIG_VALIDATION = "config_validation"
    
    # Plugin events
    PLUGIN_LOAD = "plugin_load"
    PLUGIN_UNLOAD = "plugin_unload"
    PLUGIN_EXECUTE = "plugin_execute"
    PLUGIN_VALIDATION = "plugin_validation"
    
    # Security events
    SECURITY_VIOLATION = "security_violation"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    RATE_LIMIT_HIT = "rate_limit_hit"
    VALIDATION_FAILURE = "validation_failure"
    
    # System events
    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop"
    ERROR_OCCURRED = "error_occurred"


class Severity(Enum):
    """Event severity levels."""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class SecurityEvent:
    """
    Represents a security-relevant event in the system.
    """
    
    event_type: EventType
    severity: Severity
    message: str
    timestamp: float = field(default_factory=time.time)
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    source_ip: Optional[str] = None
    user_agent: Optional[str] = None
    resource: Optional[str] = None
    action: Optional[str] = None
    result: Optional[str] = None
    error_code: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert event to JSON string."""
        data = self.to_dict()
        # Convert enum values to strings
        data['event_type'] = self.event_type.value
        data['severity'] = self.severity.value
        return json.dumps(data, default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SecurityEvent':
        """Create SecurityEvent from dictionary."""
        # Convert string enum values back to enums
        if isinstance(data.get('event_type'), str):
            data['event_type'] = EventType(data['event_type'])
        if isinstance(data.get('severity'), str):
            data['severity'] = Severity(data['severity'])
        
        return cls(**data)


class SuspiciousActivityDetector:
    """
    Detects patterns that may indicate suspicious or malicious activity.
    """
    
    def __init__(self, window_size: int = 100, time_window: int = 300):
        """
        Initialize detector.
        
        Args:
            window_size: Number of recent events to analyze
            time_window: Time window in seconds for pattern detection
        """
        self.window_size = window_size
        self.time_window = time_window
        self.recent_events = deque(maxlen=window_size)
        self.failure_counts = defaultdict(int)
        self.rate_counters = defaultdict(lambda: deque())
        self._lock = threading.RLock()
    
    def analyze_event(self, event: SecurityEvent) -> List[SecurityEvent]:
        """
        Analyze an event for suspicious patterns.
        
        Args:
            event: Event to analyze
            
        Returns:
            List of additional suspicious activity events detected
        """
        suspicious_events = []
        
        with self._lock:
            self.recent_events.append(event)
            
            # Check for repeated failures
            if event.result == "failure":
                key = f"{event.event_type.value}_{event.user_id or 'unknown'}"
                self.failure_counts[key] += 1
                
                if self.failure_counts[key] >= 5:  # 5 failures threshold
                    suspicious_events.append(SecurityEvent(
                        event_type=EventType.SUSPICIOUS_ACTIVITY,
                        severity=Severity.HIGH,
                        message=f"Multiple {event.event_type.value} failures detected",
                        session_id=event.session_id,
                        user_id=event.user_id,
                        context={
                            "failure_count": self.failure_counts[key],
                            "original_event_type": event.event_type.value,
                            "pattern": "repeated_failures"
                        }
                    ))
            
            # Check for rate limit violations
            rate_key = f"{event.event_type.value}_{event.user_id or 'unknown'}"
            now = time.time()
            
            # Clean old entries
            while (self.rate_counters[rate_key] and 
                   self.rate_counters[rate_key][0] < now - self.time_window):
                self.rate_counters[rate_key].popleft()
            
            self.rate_counters[rate_key].append(now)
            
            # Check rate thresholds
            rate_thresholds = {
                EventType.AUTH_FAILURE: 10,  # 10 auth failures in 5 minutes
                EventType.FILE_DOWNLOAD: 100,  # 100 downloads in 5 minutes
                EventType.VALIDATION_FAILURE: 20,  # 20 validation failures
            }
            
            threshold = rate_thresholds.get(event.event_type, 50)
            if len(self.rate_counters[rate_key]) > threshold:
                suspicious_events.append(SecurityEvent(
                    event_type=EventType.RATE_LIMIT_HIT,
                    severity=Severity.MEDIUM,
                    message=f"Rate limit exceeded for {event.event_type.value}",
                    session_id=event.session_id,
                    user_id=event.user_id,
                    context={
                        "event_count": len(self.rate_counters[rate_key]),
                        "threshold": threshold,
                        "time_window": self.time_window,
                        "original_event_type": event.event_type.value
                    }
                ))
            
            # Check for suspicious patterns in recent events
            if len(self.recent_events) >= 10:
                pattern_events = self._detect_patterns()
                suspicious_events.extend(pattern_events)
        
        return suspicious_events
    
    def _detect_patterns(self) -> List[SecurityEvent]:
        """Detect suspicious patterns in recent events."""
        patterns = []
        recent = list(self.recent_events)[-20:]  # Last 20 events
        
        # Pattern: Rapid privilege escalation attempts
        escalation_events = [
            e for e in recent 
            if e.event_type in [EventType.ACCESS_DENIED, EventType.PERMISSION_CHECK]
        ]
        
        if len(escalation_events) >= 5:
            patterns.append(SecurityEvent(
                event_type=EventType.SUSPICIOUS_ACTIVITY,
                severity=Severity.HIGH,
                message="Potential privilege escalation attempt detected",
                context={
                    "pattern": "privilege_escalation",
                    "event_count": len(escalation_events),
                    "time_span": escalation_events[-1].timestamp - escalation_events[0].timestamp
                }
            ))
        
        # Pattern: Scan-like behavior (many different resources accessed)
        resources = set(e.resource for e in recent if e.resource)
        if len(resources) >= 15:  # Accessing many different resources quickly
            patterns.append(SecurityEvent(
                event_type=EventType.SUSPICIOUS_ACTIVITY,
                severity=Severity.MEDIUM,
                message="Potential scanning behavior detected",
                context={
                    "pattern": "resource_scanning",
                    "unique_resources": len(resources),
                    "total_events": len(recent)
                }
            ))
        
        return patterns


class SecurityAuditor:
    """
    Central security audit logging system.
    
    Handles security event logging, audit trail maintenance, suspicious activity
    detection, and secure log storage with rotation.
    """
    
    def __init__(self, 
                 log_file: Optional[Union[str, Path]] = None,
                 max_file_size: int = 10 * 1024 * 1024,  # 10MB
                 backup_count: int = 5,
                 enable_detection: bool = True):
        """
        Initialize security auditor.
        
        Args:
            log_file: Path to audit log file (None for memory-only)
            max_file_size: Maximum log file size before rotation
            backup_count: Number of backup files to keep
            enable_detection: Whether to enable suspicious activity detection
        """
        self.log_file = Path(log_file) if log_file else None
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.enable_detection = enable_detection
        
        # Initialize components
        self.logger = logging.getLogger(__name__)
        self.detector = SuspiciousActivityDetector() if enable_detection else None
        self._event_buffer = deque(maxlen=1000)  # In-memory buffer
        self._lock = threading.RLock()
        
        # Create log directory if needed
        if self.log_file:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Setup file handler with rotation
        self._setup_file_logging()
    
    def _setup_file_logging(self):
        """Setup file logging with rotation."""
        if not self.log_file:
            return
        
        from logging.handlers import RotatingFileHandler
        
        # Create rotating file handler
        handler = RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count
        )
        
        # Create formatter for structured logging
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S UTC'
        )
        handler.setFormatter(formatter)
        
        # Create dedicated security logger
        self.security_logger = logging.getLogger('redditdl.security')
        self.security_logger.setLevel(logging.INFO)
        self.security_logger.addHandler(handler)
        self.security_logger.propagate = False
    
    def log_event(self, event: SecurityEvent) -> None:
        """
        Log a security event.
        
        Args:
            event: Security event to log
        """
        with self._lock:
            # Add to in-memory buffer
            self._event_buffer.append(event)
            
            # Log to file if configured
            if hasattr(self, 'security_logger'):
                log_level = self._get_log_level(event.severity)
                self.security_logger.log(log_level, event.to_json())
            
            # Check for suspicious activity, but avoid recursion
            if self.detector and event.event_type != EventType.SUSPICIOUS_ACTIVITY:
                suspicious_events = self.detector.analyze_event(event)
                if suspicious_events:
                    # Log these without re-triggering detection
                    for s_event in suspicious_events:
                        self._event_buffer.append(s_event)
                        if hasattr(self, 'security_logger'):
                            s_log_level = self._get_log_level(s_event.severity)
                            self.security_logger.log(s_log_level, s_event.to_json())
    
    def _get_log_level(self, severity: Severity) -> int:
        """Convert severity to logging level."""
        mapping = {
            Severity.LOW: logging.INFO,
            Severity.MEDIUM: logging.WARNING,
            Severity.HIGH: logging.ERROR,
            Severity.CRITICAL: logging.CRITICAL
        }
        return mapping.get(severity, logging.INFO)
    
    def log_authentication(self, success: bool, user_id: Optional[str] = None,
                          session_id: Optional[str] = None,
                          source_ip: Optional[str] = None,
                          error_message: Optional[str] = None) -> None:
        """Log authentication event."""
        event = SecurityEvent(
            event_type=EventType.AUTH_SUCCESS if success else EventType.AUTH_FAILURE,
            severity=Severity.LOW if success else Severity.MEDIUM,
            message=f"Authentication {'successful' if success else 'failed'}",
            session_id=session_id,
            user_id=user_id,
            source_ip=source_ip,
            result="success" if success else "failure",
            context={"error_message": error_message} if error_message else {}
        )
        self.log_event(event)
    
    def log_file_operation(self, operation: str, file_path: str,
                          success: bool, user_id: Optional[str] = None,
                          session_id: Optional[str] = None,
                          error_message: Optional[str] = None) -> None:
        """Log file operation event."""
        event_types = {
            'read': EventType.FILE_READ,
            'write': EventType.FILE_WRITE,
            'delete': EventType.FILE_DELETE,
            'download': EventType.FILE_DOWNLOAD,
            'upload': EventType.FILE_UPLOAD
        }
        
        event = SecurityEvent(
            event_type=event_types.get(operation.lower(), EventType.FILE_READ),
            severity=Severity.LOW if success else Severity.MEDIUM,
            message=f"File {operation} {'successful' if success else 'failed'}",
            session_id=session_id,
            user_id=user_id,
            resource=file_path,
            action=operation,
            result="success" if success else "failure",
            context={"error_message": error_message} if error_message else {}
        )
        self.log_event(event)
    
    def log_config_event(self, action: str, config_key: Optional[str] = None,
                        success: bool = True, user_id: Optional[str] = None,
                        session_id: Optional[str] = None,
                        old_value: Optional[str] = None,
                        new_value: Optional[str] = None) -> None:
        """Log configuration event."""
        event_types = {
            'load': EventType.CONFIG_LOAD,
            'change': EventType.CONFIG_CHANGE,
            'validate': EventType.CONFIG_VALIDATION
        }
        
        context = {}
        if config_key:
            context['config_key'] = config_key
        if old_value is not None:
            context['old_value'] = str(old_value)[:100]  # Truncate for security
        if new_value is not None:
            context['new_value'] = str(new_value)[:100]
        
        event = SecurityEvent(
            event_type=event_types.get(action.lower(), EventType.CONFIG_LOAD),
            severity=Severity.LOW if success else Severity.MEDIUM,
            message=f"Configuration {action} {'successful' if success else 'failed'}",
            session_id=session_id,
            user_id=user_id,
            resource=config_key,
            action=action,
            result="success" if success else "failure",
            context=context
        )
        self.log_event(event)
    
    def log_plugin_event(self, action: str, plugin_name: str,
                        success: bool = True, user_id: Optional[str] = None,
                        session_id: Optional[str] = None,
                        error_message: Optional[str] = None) -> None:
        """Log plugin event."""
        event_types = {
            'load': EventType.PLUGIN_LOAD,
            'unload': EventType.PLUGIN_UNLOAD,
            'execute': EventType.PLUGIN_EXECUTE,
            'validate': EventType.PLUGIN_VALIDATION
        }
        
        # Plugin events are higher severity due to security implications
        severity = Severity.MEDIUM if success else Severity.HIGH
        
        event = SecurityEvent(
            event_type=event_types.get(action.lower(), EventType.PLUGIN_EXECUTE),
            severity=severity,
            message=f"Plugin {action} {'successful' if success else 'failed'}",
            session_id=session_id,
            user_id=user_id,
            resource=plugin_name,
            action=action,
            result="success" if success else "failure",
            context={"error_message": error_message} if error_message else {}
        )
        self.log_event(event)
    
    def log_security_violation(self, violation_type: str, description: str,
                              severity: Severity = Severity.HIGH,
                              user_id: Optional[str] = None,
                              session_id: Optional[str] = None,
                              resource: Optional[str] = None,
                              context: Optional[Dict[str, Any]] = None) -> None:
        """Log security violation."""
        event = SecurityEvent(
            event_type=EventType.SECURITY_VIOLATION,
            severity=severity,
            message=f"Security violation: {description}",
            session_id=session_id,
            user_id=user_id,
            resource=resource,
            action=violation_type,
            result="violation",
            context=context or {}
        )
        self.log_event(event)
    
    def log_validation_failure(self, field: str, value: str, reason: str,
                              user_id: Optional[str] = None,
                              session_id: Optional[str] = None) -> None:
        """Log validation failure."""
        event = SecurityEvent(
            event_type=EventType.VALIDATION_FAILURE,
            severity=Severity.MEDIUM,
            message=f"Validation failed for {field}: {reason}",
            session_id=session_id,
            user_id=user_id,
            resource=field,
            action="validate",
            result="failure",
            context={
                "field": field,
                "value": value[:100] if isinstance(value, str) else str(value)[:100],
                "reason": reason
            }
        )
        self.log_event(event)
    
    def log_from_exception(self, exception: Exception, 
                          context: Optional[ErrorContext] = None) -> None:
        """Log security event from an exception."""
        severity = Severity.HIGH if isinstance(exception, SecurityError) else Severity.MEDIUM
        
        event = SecurityEvent(
            event_type=EventType.ERROR_OCCURRED,
            severity=severity,
            message=f"Exception occurred: {str(exception)}",
            session_id=context.session_id if context else None,
            correlation_id=context.correlation_id if context else None,
            resource=context.file_path if context else None,
            context={
                "exception_type": type(exception).__name__,
                "operation": context.operation if context else None,
                "stage": context.stage if context else None
            }
        )
        self.log_event(event)
    
    def get_recent_events(self, count: int = 100, 
                         event_type: Optional[EventType] = None,
                         severity: Optional[Severity] = None) -> List[SecurityEvent]:
        """
        Get recent security events.
        
        Args:
            count: Maximum number of events to return
            event_type: Optional filter by event type
            severity: Optional filter by severity
            
        Returns:
            List of recent security events
        """
        with self._lock:
            events = list(self._event_buffer)
        
        # Apply filters
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if severity:
            events = [e for e in events if e.severity == severity]
        
        # Sort by timestamp (newest first) and limit
        events.sort(key=lambda e: e.timestamp, reverse=True)
        return events[:count]
    
    def get_security_summary(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get security summary for the specified time period.
        
        Args:
            hours: Number of hours to analyze
            
        Returns:
            Dictionary with security statistics
        """
        cutoff_time = time.time() - (hours * 3600)
        recent_events = [e for e in self._event_buffer if e.timestamp >= cutoff_time]
        
        # Count events by type
        event_counts = defaultdict(int)
        severity_counts = defaultdict(int)
        
        for event in recent_events:
            event_counts[event.event_type.value] += 1
            severity_counts[event.severity.value] += 1
        
        # Calculate failure rates
        total_auth = event_counts.get(EventType.AUTH_SUCCESS.value, 0) + \
                    event_counts.get(EventType.AUTH_FAILURE.value, 0)
        auth_failure_rate = 0.0
        if total_auth > 0:
            auth_failure_rate = event_counts.get(EventType.AUTH_FAILURE.value, 0) / total_auth
        
        return {
            "time_period_hours": hours,
            "total_events": len(recent_events),
            "event_counts": dict(event_counts),
            "severity_counts": dict(severity_counts),
            "auth_failure_rate": auth_failure_rate,
            "security_violations": event_counts.get(EventType.SECURITY_VIOLATION.value, 0),
            "suspicious_activities": event_counts.get(EventType.SUSPICIOUS_ACTIVITY.value, 0),
            "high_severity_events": severity_counts.get(Severity.HIGH.value, 0) + \
                                  severity_counts.get(Severity.CRITICAL.value, 0)
        }


# Convenience class for SecurityError to integrate with existing exception hierarchy
class SecurityError(Exception):
    """Base exception for security-related errors."""
    pass


# Global auditor instance
_auditor: Optional[SecurityAuditor] = None


def get_auditor() -> SecurityAuditor:
    """Get the global security auditor instance."""
    global _auditor
    if _auditor is None:
        # Initialize with default settings
        _auditor = SecurityAuditor(
            log_file="logs/security_audit.log",
            enable_detection=True
        )
    return _auditor


def set_auditor(auditor: SecurityAuditor) -> None:
    """Set the global security auditor instance."""
    global _auditor
    _auditor = auditor