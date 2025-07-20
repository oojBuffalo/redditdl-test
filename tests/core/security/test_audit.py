"""
Tests for Security Audit System

Tests for security event logging, audit trail maintenance,
and suspicious activity detection.
"""

import pytest
import tempfile
import time
from pathlib import Path

from redditdl.core.security.audit import (
    SecurityAuditor, SecurityEvent, EventType, Severity,
    SuspiciousActivityDetector, get_auditor, set_auditor
)


class TestSecurityEvent:
    """Test suite for SecurityEvent class."""
    
    def test_security_event_creation(self):
        """Test SecurityEvent creation with basic fields."""
        event = SecurityEvent(
            event_type=EventType.AUTH_SUCCESS,
            severity=Severity.LOW,
            message="Test authentication success"
        )
        
        assert event.event_type == EventType.AUTH_SUCCESS
        assert event.severity == Severity.LOW
        assert event.message == "Test authentication success"
        assert event.timestamp > 0
    
    def test_security_event_to_dict(self):
        """Test SecurityEvent dictionary conversion."""
        event = SecurityEvent(
            event_type=EventType.FILE_DOWNLOAD,
            severity=Severity.MEDIUM,
            message="File download test",
            session_id="test-session",
            user_id="test-user",
            resource="/path/to/file.txt"
        )
        
        event_dict = event.to_dict()
        
        assert event_dict['event_type'] == EventType.FILE_DOWNLOAD
        assert event_dict['severity'] == Severity.MEDIUM
        assert event_dict['message'] == "File download test"
        assert event_dict['session_id'] == "test-session"
        assert event_dict['user_id'] == "test-user"
        assert event_dict['resource'] == "/path/to/file.txt"
    
    def test_security_event_json_serialization(self):
        """Test SecurityEvent JSON serialization."""
        event = SecurityEvent(
            event_type=EventType.SECURITY_VIOLATION,
            severity=Severity.HIGH,
            message="Security violation detected",
            context={"violation_type": "path_traversal", "count": 3}
        )
        
        json_str = event.to_json()
        assert "security_violation" in json_str
        assert "high" in json_str
        assert "path_traversal" in json_str
    
    def test_security_event_from_dict(self):
        """Test SecurityEvent creation from dictionary."""
        event_data = {
            'event_type': 'auth_failure',
            'severity': 'medium',
            'message': 'Authentication failed',
            'session_id': 'test-session',
            'timestamp': time.time()
        }
        
        event = SecurityEvent.from_dict(event_data)
        
        assert event.event_type == EventType.AUTH_FAILURE
        assert event.severity == Severity.MEDIUM
        assert event.message == "Authentication failed"
        assert event.session_id == "test-session"


class TestSuspiciousActivityDetector:
    """Test suite for SuspiciousActivityDetector."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.detector = SuspiciousActivityDetector(window_size=20, time_window=60)
    
    def test_repeated_failures_detection(self):
        """Test detection of repeated failure patterns."""
        # Create multiple auth failure events
        for i in range(6):
            event = SecurityEvent(
                event_type=EventType.AUTH_FAILURE,
                severity=Severity.MEDIUM,
                message=f"Auth failure {i}",
                user_id="test-user",
                result="failure"
            )
            
            suspicious_events = self.detector.analyze_event(event)
            
            # Should detect suspicious activity after 5 failures
            if i >= 4:
                assert len(suspicious_events) > 0
                assert suspicious_events[0].event_type == EventType.SUSPICIOUS_ACTIVITY
                assert "repeated_failures" in suspicious_events[0].context.get('pattern', '')
    
    def test_rate_limit_detection(self):
        """Test detection of rate limit violations."""
        # Create many events in short time
        for i in range(12):
            event = SecurityEvent(
                event_type=EventType.AUTH_FAILURE,
                severity=Severity.MEDIUM,
                message=f"Auth attempt {i}",
                user_id="test-user"
            )
            
            suspicious_events = self.detector.analyze_event(event)
            
            # Should detect rate limit violation after threshold
            if i >= 9:  # Threshold is 10 for auth failures
                rate_events = [e for e in suspicious_events if e.event_type == EventType.RATE_LIMIT_HIT]
                assert len(rate_events) > 0
    
    def test_privilege_escalation_pattern(self):
        """Test detection of privilege escalation patterns."""
        # Create multiple access denied events (simulating escalation attempts)
        events = []
        for i in range(6):
            event = SecurityEvent(
                event_type=EventType.ACCESS_DENIED,
                severity=Severity.MEDIUM,
                message=f"Access denied {i}",
                user_id="test-user"
            )
            events.append(event)
            self.detector.recent_events.append(event)
        
        # Analyze the last event
        suspicious_events = self.detector.analyze_event(events[-1])
        
        # Should detect privilege escalation pattern
        escalation_events = [
            e for e in suspicious_events 
            if e.event_type == EventType.SUSPICIOUS_ACTIVITY and 
               e.context.get('pattern') == 'privilege_escalation'
        ]
        assert len(escalation_events) > 0
    
    def test_scanning_behavior_detection(self):
        """Test detection of scanning-like behavior."""
        # Create events accessing many different resources
        for i in range(16):
            event = SecurityEvent(
                event_type=EventType.FILE_READ,
                severity=Severity.LOW,
                message=f"File access {i}",
                user_id="test-user",
                resource=f"/path/to/file{i}.txt"
            )
            self.detector.recent_events.append(event)
        
        # Analyze the last event
        suspicious_events = self.detector.analyze_event(event)
        
        # Should detect scanning behavior
        scan_events = [
            e for e in suspicious_events 
            if e.event_type == EventType.SUSPICIOUS_ACTIVITY and 
               e.context.get('pattern') == 'resource_scanning'
        ]
        assert len(scan_events) > 0


class TestSecurityAuditor:
    """Test suite for SecurityAuditor class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = Path(self.temp_dir) / "security.log"
        self.auditor = SecurityAuditor(
            log_file=self.log_file,
            enable_detection=True
        )
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_log_event_basic(self):
        """Test basic event logging."""
        event = SecurityEvent(
            event_type=EventType.FILE_DOWNLOAD,
            severity=Severity.LOW,
            message="Test file download"
        )
        
        self.auditor.log_event(event)
        
        # Check event was added to buffer
        assert len(self.auditor._event_buffer) == 1
        assert self.auditor._event_buffer[0] == event
    
    def test_log_authentication_success(self):
        """Test authentication logging for successful login."""
        self.auditor.log_authentication(
            success=True,
            user_id="test-user",
            session_id="test-session"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.AUTH_SUCCESS
        assert events[0].user_id == "test-user"
        assert events[0].session_id == "test-session"
    
    def test_log_authentication_failure(self):
        """Test authentication logging for failed login."""
        self.auditor.log_authentication(
            success=False,
            user_id="test-user",
            error_message="Invalid credentials"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.AUTH_FAILURE
        assert events[0].severity == Severity.MEDIUM
        assert "Invalid credentials" in events[0].context.get('error_message', '')
    
    def test_log_file_operation(self):
        """Test file operation logging."""
        self.auditor.log_file_operation(
            operation="download",
            file_path="/path/to/file.txt",
            success=True,
            user_id="test-user"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.FILE_DOWNLOAD
        assert events[0].resource == "/path/to/file.txt"
        assert events[0].result == "success"
    
    def test_log_config_event(self):
        """Test configuration event logging."""
        self.auditor.log_config_event(
            action="change",
            config_key="api.rate_limit",
            success=True,
            old_value="10",
            new_value="20"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.CONFIG_CHANGE
        assert events[0].resource == "api.rate_limit"
        assert "old_value" in events[0].context
        assert "new_value" in events[0].context
    
    def test_log_plugin_event(self):
        """Test plugin event logging."""
        self.auditor.log_plugin_event(
            action="load",
            plugin_name="test_plugin",
            success=True,
            user_id="test-user"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.PLUGIN_LOAD
        assert events[0].resource == "test_plugin"
        assert events[0].severity == Severity.MEDIUM  # Plugin events have higher severity
    
    def test_log_security_violation(self):
        """Test security violation logging."""
        self.auditor.log_security_violation(
            violation_type="path_traversal",
            description="Attempted path traversal attack",
            severity=Severity.HIGH,
            user_id="malicious-user",
            resource="../../../etc/passwd"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.SECURITY_VIOLATION
        assert events[0].severity == Severity.HIGH
        assert events[0].action == "path_traversal"
        assert events[0].resource == "../../../etc/passwd"
    
    def test_log_validation_failure(self):
        """Test validation failure logging."""
        self.auditor.log_validation_failure(
            field="username",
            value="invalid@user",
            reason="Contains invalid characters"
        )
        
        events = list(self.auditor._event_buffer)
        assert len(events) == 1
        assert events[0].event_type == EventType.VALIDATION_FAILURE
        assert events[0].resource == "username"
        assert "invalid@user" in events[0].context.get('value', '')
    
    def test_get_recent_events(self):
        """Test retrieving recent events."""
        # Add multiple events
        for i in range(5):
            event = SecurityEvent(
                event_type=EventType.FILE_READ,
                severity=Severity.LOW,
                message=f"File read {i}"
            )
            self.auditor.log_event(event)
        
        # Get recent events
        recent = self.auditor.get_recent_events(count=3)
        assert len(recent) == 3
        
        # Should be in reverse chronological order (newest first)
        assert "File read 4" in recent[0].message
        assert "File read 3" in recent[1].message
        assert "File read 2" in recent[2].message
    
    def test_get_recent_events_with_filters(self):
        """Test retrieving recent events with filters."""
        # Add events of different types
        auth_event = SecurityEvent(
            event_type=EventType.AUTH_SUCCESS,
            severity=Severity.LOW,
            message="Auth success"
        )
        file_event = SecurityEvent(
            event_type=EventType.FILE_DOWNLOAD,
            severity=Severity.MEDIUM,
            message="File download"
        )
        
        self.auditor.log_event(auth_event)
        self.auditor.log_event(file_event)
        
        # Filter by event type
        auth_events = self.auditor.get_recent_events(
            count=10, 
            event_type=EventType.AUTH_SUCCESS
        )
        assert len(auth_events) == 1
        assert auth_events[0].event_type == EventType.AUTH_SUCCESS
        
        # Filter by severity
        medium_events = self.auditor.get_recent_events(
            count=10,
            severity=Severity.MEDIUM
        )
        assert len(medium_events) == 1
        assert medium_events[0].severity == Severity.MEDIUM
    
    def test_get_security_summary(self):
        """Test security summary generation."""
        # Add various events
        events = [
            SecurityEvent(EventType.AUTH_SUCCESS, Severity.LOW, "Auth success"),
            SecurityEvent(EventType.AUTH_FAILURE, Severity.MEDIUM, "Auth failure"),
            SecurityEvent(EventType.SECURITY_VIOLATION, Severity.HIGH, "Security violation"),
            SecurityEvent(EventType.FILE_DOWNLOAD, Severity.LOW, "File download")
        ]
        
        for event in events:
            self.auditor.log_event(event)
        
        summary = self.auditor.get_security_summary(hours=24)
        
        assert summary['total_events'] == 4
        assert summary['event_counts']['auth_success'] == 1
        assert summary['event_counts']['auth_failure'] == 1
        assert summary['security_violations'] == 1
        assert summary['high_severity_events'] == 1
        assert 0 <= summary['auth_failure_rate'] <= 1
    
    def test_suspicious_activity_integration(self):
        """Test integration with suspicious activity detection."""
        # Create multiple failure events that should trigger detection
        for i in range(6):
            event = SecurityEvent(
                event_type=EventType.AUTH_FAILURE,
                severity=Severity.MEDIUM,
                message=f"Auth failure {i}",
                user_id="test-user",
                result="failure"
            )
            self.auditor.log_event(event)
        
        # Should have original events plus suspicious activity events
        events = list(self.auditor._event_buffer)
        suspicious_events = [e for e in events if e.event_type == EventType.SUSPICIOUS_ACTIVITY]
        assert len(suspicious_events) > 0
    
    def test_file_logging(self):
        """Test logging to file."""
        event = SecurityEvent(
            event_type=EventType.FILE_DOWNLOAD,
            severity=Severity.LOW,
            message="Test file logging"
        )
        
        self.auditor.log_event(event)
        
        # Check that log file was created and contains event
        assert self.log_file.exists()
        log_content = self.log_file.read_text()
        assert "file_download" in log_content
        assert "Test file logging" in log_content


class TestGlobalAuditorFunctions:
    """Test global auditor functions."""
    
    def test_get_auditor_singleton(self):
        """Test global auditor singleton pattern."""
        auditor1 = get_auditor()
        auditor2 = get_auditor()
        
        assert auditor1 is auditor2
    
    def test_set_auditor(self):
        """Test setting custom global auditor."""
        original_auditor = get_auditor()
        
        custom_auditor = SecurityAuditor(enable_detection=False)
        set_auditor(custom_auditor)
        
        assert get_auditor() is custom_auditor
        
        # Restore original
        set_auditor(original_auditor)


@pytest.mark.integration
class TestAuditIntegration:
    """Integration tests for audit system."""
    
    def test_end_to_end_audit_flow(self):
        """Test complete audit workflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "audit.log"
            auditor = SecurityAuditor(log_file=log_file, enable_detection=True)
            
            # Simulate a complete user session
            auditor.log_authentication(True, user_id="user123", session_id="session456")
            auditor.log_config_event("load", "app.config", True)
            auditor.log_file_operation("download", "/path/file.txt", True, user_id="user123")
            auditor.log_plugin_event("load", "csv_exporter", True)
            auditor.log_authentication(False, user_id="user123", session_id="session456")
            
            # Verify events were logged
            assert len(auditor._event_buffer) == 5
            
            # Verify file logging
            assert log_file.exists()
            log_content = log_file.read_text()
            assert "auth_success" in log_content
            assert "config_load" in log_content
            assert "file_download" in log_content
            assert "plugin_load" in log_content
            assert "auth_failure" in log_content
            
            # Verify summary
            summary = auditor.get_security_summary()
            assert summary['total_events'] == 5
            assert summary['auth_failure_rate'] == 0.5  # 1 failure out of 2 auth events