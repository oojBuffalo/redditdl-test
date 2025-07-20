"""
Test suite for core exception system.

Tests error hierarchy, error context, recovery suggestions, and error classification
to ensure comprehensive error handling across the RedditDL application.
"""

import pytest
from datetime import datetime
from typing import Dict, Any, List, Optional

from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ConfigurationError, AuthenticationError,
    ProcessingError, ValidationError, ErrorCode, ErrorContext, 
    RecoverySuggestion, processing_error,
    network_error, config_error, auth_error, validation_error
)
from redditdl.core.error_context import generate_user_message


class TestErrorHierarchy:
    """Test the error class hierarchy and inheritance."""
    
    def test_base_error_creation(self):
        """Test RedditDLError base class creation."""
        error = RedditDLError("Test error")
        assert str(error) == "Test error"
        assert error.error_code == ErrorCode.UNKNOWN_ERROR
        assert error.recoverable is True
        assert error.context is None
        assert error.suggestions == []
    
    def test_base_error_with_context(self):
        """Test RedditDLError with error context."""
        context = ErrorContext(
            operation="test_operation",
            stage="test_stage",
            post_id="test_post"
        )
        error = RedditDLError("Test error", context=context)
        assert error.context == context
        assert error.context.operation == "test_operation"
    
    def test_base_error_with_suggestions(self):
        """Test RedditDLError with recovery suggestions."""
        suggestion = RecoverySuggestion(
            action="test_action",
            description="Test description",
            automatic=True,
            priority=1
        )
        error = RedditDLError("Test error", suggestions=[suggestion])
        assert len(error.suggestions) == 1
        assert error.suggestions[0].action == "test_action"
    
    def test_network_error_inheritance(self):
        """Test NetworkError inherits from RedditDLError."""
        error = NetworkError("Network failed")
        assert isinstance(error, RedditDLError)
        assert error.error_code == ErrorCode.NETWORK_CONNECTION_FAILED
        assert "network" in str(error).lower()
    
    def test_configuration_error_inheritance(self):
        """Test ConfigurationError inherits from RedditDLError."""
        error = ConfigurationError("Config invalid")
        assert isinstance(error, RedditDLError)
        assert error.error_code == ErrorCode.CONFIG_INVALID_VALUE
    
    def test_authentication_error_inheritance(self):
        """Test AuthenticationError inherits from RedditDLError."""
        error = AuthenticationError("Auth failed")
        assert isinstance(error, RedditDLError)
        assert error.error_code == ErrorCode.AUTH_INVALID_CREDENTIALS
    
    def test_processing_error_inheritance(self):
        """Test ProcessingError inherits from RedditDLError."""
        error = ProcessingError("Processing failed")
        assert isinstance(error, RedditDLError)
        assert error.error_code == ErrorCode.PROCESSING_OPERATION_FAILED
    
    def test_validation_error_inheritance(self):
        """Test ValidationError inherits from RedditDLError."""
        error = ValidationError("Validation failed", field_name="test_field")
        assert isinstance(error, RedditDLError)
        assert error.error_code == ErrorCode.VALIDATION_FIELD_INVALID
        assert error.field_name == "test_field"


class TestErrorCodes:
    """Test error code classification and ranges."""
    
    def test_error_code_ranges(self):
        """Test error codes fall within expected ranges."""
        # Unknown errors: 1000-1999
        assert 1000 <= ErrorCode.UNKNOWN_ERROR.value < 2000
        
        # Network errors: 2000-2999
        assert 2000 <= ErrorCode.NETWORK_CONNECTION_FAILED.value < 3000
        assert 2000 <= ErrorCode.NETWORK_TIMEOUT.value < 3000
        assert 2000 <= ErrorCode.NETWORK_RATE_LIMITED.value < 3000
        
        # Authentication errors: 3000-3999
        assert 3000 <= ErrorCode.AUTH_INVALID_CREDENTIALS.value < 4000
        assert 3000 <= ErrorCode.AUTH_TOKEN_EXPIRED.value < 4000
        assert 3000 <= ErrorCode.AUTH_INSUFFICIENT_PERMISSIONS.value < 4000
        
        # Configuration errors: 4000-4999
        assert 4000 <= ErrorCode.CONFIG_FILE_NOT_FOUND.value < 5000
        assert 4000 <= ErrorCode.CONFIG_INVALID_VALUE.value < 5000
        assert 4000 <= ErrorCode.CONFIG_MISSING_REQUIRED.value < 5000
        
        # Validation errors: 5000-5999
        assert 5000 <= ErrorCode.VALIDATION_FIELD_INVALID.value < 6000
        assert 5000 <= ErrorCode.VALIDATION_FIELD_MISSING.value < 6000
        assert 5000 <= ErrorCode.VALIDATION_FIELD_TYPE_MISMATCH.value < 6000
        
        # Processing errors: 6000-6999
        assert 6000 <= ErrorCode.PROCESSING_OPERATION_FAILED.value < 7000
        assert 6000 <= ErrorCode.PROCESSING_INVALID_CONTENT.value < 7000
        assert 6000 <= ErrorCode.PROCESSING_DEPENDENCY_MISSING.value < 7000
        
        # Target errors: 7000-7999
        assert 7000 <= ErrorCode.TARGET_NOT_FOUND.value < 8000
        assert 7000 <= ErrorCode.TARGET_ACCESS_DENIED.value < 8000
        assert 7000 <= ErrorCode.TARGET_CONTENT_UNAVAILABLE.value < 8000
    
    def test_error_code_uniqueness(self):
        """Test all error codes are unique."""
        error_codes = [code.value for code in ErrorCode]
        assert len(error_codes) == len(set(error_codes))


class TestErrorContext:
    """Test ErrorContext dataclass functionality."""
    
    def test_error_context_creation(self):
        """Test ErrorContext creation with basic fields."""
        context = ErrorContext(
            operation="test_op",
            stage="test_stage"
        )
        assert context.operation == "test_op"
        assert context.stage == "test_stage"
        assert context.post_id is None
        assert context.url is None
        assert context.session_id is None
        assert isinstance(context.timestamp, datetime)
    
    def test_error_context_full_creation(self):
        """Test ErrorContext creation with all fields."""
        timestamp = datetime.now()
        context = ErrorContext(
            operation="full_test",
            stage="full_stage",
            post_id="post123",
            url="https://example.com",
            session_id="session456",
            timestamp=timestamp,
            additional_data={"key": "value"}
        )
        assert context.operation == "full_test"
        assert context.stage == "full_stage"
        assert context.post_id == "post123"
        assert context.url == "https://example.com"
        assert context.session_id == "session456"
        assert context.timestamp == timestamp
        assert context.additional_data == {"key": "value"}
    
    def test_error_context_serialization(self):
        """Test ErrorContext can be serialized to dict."""
        context = ErrorContext(
            operation="serialize_test",
            stage="serialize_stage",
            post_id="post789"
        )
        context_dict = context.__dict__
        assert "operation" in context_dict
        assert "stage" in context_dict
        assert "post_id" in context_dict
        assert context_dict["operation"] == "serialize_test"


class TestRecoverySuggestions:
    """Test RecoverySuggestion functionality."""
    
    def test_recovery_suggestion_creation(self):
        """Test RecoverySuggestion creation."""
        suggestion = RecoverySuggestion(
            action="retry_operation",
            description="Retry the failed operation",
            automatic=True,
            priority=1
        )
        assert suggestion.action == "retry_operation"
        assert suggestion.description == "Retry the failed operation"
        assert suggestion.automatic is True
        assert suggestion.priority == 1
    
    def test_recovery_suggestion_defaults(self):
        """Test RecoverySuggestion with defaults."""
        suggestion = RecoverySuggestion(
            action="manual_action",
            description="Manual intervention required"
        )
        assert suggestion.automatic is False
        assert suggestion.priority == 5
    
    def test_recovery_suggestion_comparison(self):
        """Test RecoverySuggestion comparison by priority."""
        high_priority = RecoverySuggestion("action1", "desc1", priority=1)
        low_priority = RecoverySuggestion("action2", "desc2", priority=5)
        
        suggestions = [low_priority, high_priority]
        sorted_suggestions = sorted(suggestions, key=lambda s: s.priority)
        
        assert sorted_suggestions[0] == high_priority
        assert sorted_suggestions[1] == low_priority


class TestErrorMethods:
    """Test error methods and functionality."""
    
    def test_add_suggestion_method(self):
        """Test adding suggestions to errors."""
        error = RedditDLError("Test error")
        suggestion = RecoverySuggestion("test_action", "Test description")
        
        error.add_suggestion(suggestion)
        assert len(error.suggestions) == 1
        assert error.suggestions[0] == suggestion
    
    def test_get_user_message_basic(self):
        """Test basic user message generation."""
        error = RedditDLError("Test error message")
        user_message = error.get_user_message()
        assert "Test error message" in user_message
    
    def test_get_user_message_with_suggestions(self):
        """Test user message with recovery suggestions."""
        error = RedditDLError("Test error")
        suggestion = RecoverySuggestion(
            action="Try this",
            description="This might help",
            automatic=False,
            priority=1
        )
        error.add_suggestion(suggestion)
        
        user_message = error.get_user_message()
        assert "Try this" in user_message
        assert "This might help" in user_message
    
    def test_get_debug_info(self):
        """Test debug information generation."""
        context = ErrorContext(
            operation="debug_test",
            stage="debug_stage",
            post_id="debug_post"
        )
        error = RedditDLError("Debug error", context=context)
        
        debug_info = error.get_debug_info()
        assert "debug_test" in debug_info
        assert "debug_stage" in debug_info
        assert "debug_post" in debug_info
        assert str(error.error_code.value) in debug_info
    
    def test_get_correlation_id(self):
        """Test correlation ID generation."""
        context = ErrorContext(session_id="test_session_123")
        error = RedditDLError("Test error", context=context)
        
        correlation_id = error.get_correlation_id()
        assert correlation_id is not None
        assert isinstance(correlation_id, str)
        assert len(correlation_id) > 0


class TestConvenienceFunctions:
    """Test convenience functions for error creation."""
    
    def test_processing_error_function(self):
        """Test processing_error convenience function."""
        context = ErrorContext(operation="test_processing")
        error = processing_error("Processing failed", context=context)
        
        assert isinstance(error, ProcessingError)
        assert error.context == context
        assert "Processing failed" in str(error)
    
    def test_network_error_function(self):
        """Test network_error convenience function."""
        error = network_error("Network failed", url="https://example.com")
        
        assert isinstance(error, NetworkError)
        assert error.url == "https://example.com"
        assert "Network failed" in str(error)
    
    def test_config_error_function(self):
        """Test config_error convenience function."""
        error = config_error("Config invalid", field="test_field")
        
        assert isinstance(error, ConfigurationError)
        assert "Config invalid" in str(error)
    
    def test_auth_error_function(self):
        """Test auth_error convenience function."""
        error = auth_error("Auth failed")
        
        assert isinstance(error, AuthenticationError)
        assert "Auth failed" in str(error)
    
    def test_validation_error_function(self):
        """Test validation_error convenience function."""
        error = validation_error("Invalid value", field="test_field", value="test_value")
        
        assert isinstance(error, ValidationError)
        assert error.field_name == "test_field"
        assert error.field_value == "test_value"


class TestErrorSerialization:
    """Test error serialization and deserialization."""
    
    def test_error_dict_conversion(self):
        """Test converting error to dictionary."""
        context = ErrorContext(operation="serialize_test")
        suggestion = RecoverySuggestion("test_action", "Test description")
        
        error = RedditDLError(
            "Serialization test",
            error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
            context=context,
            suggestions=[suggestion]
        )
        
        error_dict = error.to_dict()
        
        assert error_dict["message"] == "Serialization test"
        assert error_dict["error_code"] == ErrorCode.PROCESSING_OPERATION_FAILED.value
        assert error_dict["recoverable"] is True
        assert "context" in error_dict
        assert "suggestions" in error_dict
        assert len(error_dict["suggestions"]) == 1
    
    def test_error_json_serialization(self):
        """Test error JSON serialization."""
        import json
        
        error = RedditDLError("JSON test")
        error_dict = error.to_dict()
        
        # Should be JSON serializable
        json_str = json.dumps(error_dict)
        assert json_str is not None
        
        # Should be deserializable
        loaded_dict = json.loads(json_str)
        assert loaded_dict["message"] == "JSON test"


class TestSpecializedErrors:
    """Test specialized error types and their specific functionality."""
    
    def test_network_error_with_url(self):
        """Test NetworkError with URL field."""
        error = NetworkError("Connection failed", url="https://reddit.com")
        assert error.url == "https://reddit.com"
        assert "https://reddit.com" in error.get_debug_info()
    
    def test_validation_error_with_field_info(self):
        """Test ValidationError with field information."""
        error = ValidationError(
            "Invalid field value",
            field_name="username",
            field_value="invalid@user",
            expected_type="str"
        )
        assert error.field_name == "username"
        assert error.field_value == "invalid@user"
        assert error.expected_type == "str"
    
    def test_configuration_error_with_file_info(self):
        """Test ConfigurationError with file information."""
        error = ConfigurationError(
            "Config file not found",
            config_file="/path/to/config.yaml",
            config_section="api"
        )
        assert error.config_file == "/path/to/config.yaml"
        assert error.config_section == "api"
    
    def test_authentication_error_with_auth_info(self):
        """Test AuthenticationError with authentication details."""
        error = AuthenticationError(
            "Invalid credentials",
            auth_method="api_key",
            username="test_user"
        )
        assert error.auth_method == "api_key"
        assert error.username == "test_user"


class TestUserMessageGeneration:
    """Test user-friendly message generation."""
    
    def test_generate_user_message_function(self):
        """Test standalone generate_user_message function."""
        context = ErrorContext(operation="user_message_test")
        suggestions = [
            RecoverySuggestion("retry", "Try again", priority=1),
            RecoverySuggestion("check_config", "Check configuration", priority=2)
        ]
        
        message = generate_user_message(
            "Test error occurred",
            ErrorCode.PROCESSING_OPERATION_FAILED,
            suggestions=suggestions,
            context=context
        )
        
        assert "Test error occurred" in message
        assert "retry" in message.lower()
        assert "try again" in message.lower()
    
    def test_user_message_formatting(self):
        """Test user message formatting and structure."""
        error = NetworkError("Connection timeout")
        error.add_suggestion(RecoverySuggestion(
            "check_connection",
            "Check your internet connection",
            priority=1
        ))
        
        message = error.get_user_message()
        
        # Should be user-friendly and actionable
        assert len(message) > 20  # Not just the error message
        assert "check" in message.lower()
        assert "connection" in message.lower()
    
    def test_user_message_without_suggestions(self):
        """Test user message generation without suggestions."""
        error = RedditDLError("Simple error")
        message = error.get_user_message()
        
        assert "Simple error" in message
        assert len(message) >= len("Simple error")


if __name__ == "__main__":
    pytest.main([__file__])