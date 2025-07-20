"""
Test suite for CLI error handling.

Tests basic CLI error handling functionality and ensures commands work properly.
"""

import pytest
from unittest.mock import Mock, patch
from typer.testing import CliRunner
from pathlib import Path

from redditdl.cli.commands.scrape import app as scrape_app
from redditdl.cli.commands.audit import app as audit_app
from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ConfigurationError, AuthenticationError,
    ProcessingError, ValidationError, ErrorCode, ErrorContext,
    RecoverySuggestion
)


@pytest.mark.cli
class TestBasicCLIFunctionality:
    """Test basic CLI functionality and help commands."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_scrape_app_help(self, runner):
        """Test that scrape app help works."""
        result = runner.invoke(scrape_app, ["--help"])
        assert result.exit_code == 0
        assert "scrape" in result.output.lower()
    
    def test_user_command_help(self, runner):
        """Test that user command help works."""
        result = runner.invoke(scrape_app, ["user", "--help"])
        assert result.exit_code == 0
        assert "user" in result.output.lower()
    
    def test_subreddit_command_help(self, runner):
        """Test that subreddit command help works."""
        result = runner.invoke(scrape_app, ["subreddit", "--help"])
        assert result.exit_code == 0
        assert "subreddit" in result.output.lower()
    
    def test_audit_app_help(self, runner):
        """Test that audit app help works."""
        result = runner.invoke(audit_app, ["--help"])
        assert result.exit_code == 0
        assert "audit" in result.output.lower()
    
    def test_invalid_command_handling(self, runner):
        """Test handling of invalid commands."""
        result = runner.invoke(scrape_app, ["nonexistent"])
        assert result.exit_code != 0
        assert "usage" in result.output.lower() or "error" in result.output.lower()


@pytest.mark.cli
class TestCLIValidation:
    """Test CLI argument validation."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_user_command_requires_username(self, runner):
        """Test that user command requires a username."""
        result = runner.invoke(scrape_app, ["user"])
        assert result.exit_code != 0
        assert "usage" in result.output.lower() or "missing" in result.output.lower()
    
    def test_subreddit_command_requires_name(self, runner):
        """Test that subreddit command requires a subreddit name."""
        result = runner.invoke(scrape_app, ["subreddit"])
        assert result.exit_code != 0
        assert "usage" in result.output.lower() or "missing" in result.output.lower()
    
    def test_invalid_limit_values(self, runner):
        """Test that invalid limit values are rejected."""
        # Test negative limit
        result = runner.invoke(scrape_app, ["user", "testuser", "--limit", "-5"])
        # May exit with code 2 (typer validation) or 1 (app validation)
        assert result.exit_code != 0
        assert any(word in result.output.lower() for word in ["error", "invalid", "positive"])


@pytest.mark.cli
class TestErrorExceptionClasses:
    """Test the exception classes themselves."""
    
    def test_network_error_creation(self):
        """Test NetworkError can be created and has proper attributes."""
        error = NetworkError(
            "Connection failed",
            url="https://example.com",
            error_code=ErrorCode.NETWORK_CONNECTION_FAILED
        )
        assert "Connection failed" in str(error)
        assert error.error_code == ErrorCode.NETWORK_CONNECTION_FAILED
        assert "Connection failed" in error.get_user_message()
    
    def test_authentication_error_creation(self):
        """Test AuthenticationError can be created and has proper attributes."""
        error = AuthenticationError(
            "Invalid credentials",
            error_code=ErrorCode.AUTH_INVALID_CREDENTIALS
        )
        assert "Invalid credentials" in str(error)
        assert error.error_code == ErrorCode.AUTH_INVALID_CREDENTIALS
        assert "Invalid credentials" in error.get_user_message()
    
    def test_configuration_error_creation(self):
        """Test ConfigurationError can be created and has proper attributes."""
        error = ConfigurationError(
            "Invalid config",
            error_code=ErrorCode.CONFIG_INVALID_VALUE
        )
        assert "Invalid config" in str(error)
        assert error.error_code == ErrorCode.CONFIG_INVALID_VALUE
        assert "Invalid config" in error.get_user_message()
    
    def test_processing_error_creation(self):
        """Test ProcessingError can be created and has proper attributes."""
        error = ProcessingError(
            "Processing failed",
            error_code=ErrorCode.PROCESSING_OPERATION_FAILED
        )
        assert "Processing failed" in str(error)
        assert error.error_code == ErrorCode.PROCESSING_OPERATION_FAILED
        assert "Processing failed" in error.get_user_message()
    
    def test_validation_error_creation(self):
        """Test ValidationError can be created and has proper attributes."""
        error = ValidationError(
            "Invalid value",
            error_code=ErrorCode.VALIDATION_INVALID_INPUT,
            field_name="test_field",
            field_value="invalid"
        )
        assert "Invalid value" in str(error)
        assert error.error_code == ErrorCode.VALIDATION_INVALID_INPUT
        assert "Invalid value" in error.get_user_message()


@pytest.mark.cli
class TestErrorSuggestions:
    """Test error suggestion functionality."""
    
    def test_recovery_suggestion_creation(self):
        """Test RecoverySuggestion can be created."""
        suggestion = RecoverySuggestion(
            action="Test action",
            description="Test description",
            priority=1
        )
        assert suggestion.action == "Test action"
        assert suggestion.description == "Test description"
        assert suggestion.priority == 1
        assert not suggestion.automatic
    
    def test_error_with_suggestions(self):
        """Test adding suggestions to errors."""
        error = NetworkError("Test error")
        # NetworkError may have default suggestions, so get initial count
        initial_count = len(error.suggestions)
        
        suggestion = RecoverySuggestion(
            action="Custom action",
            description="Custom description",
            priority=1
        )
        error.add_suggestion(suggestion)
        
        # Should have one more suggestion
        assert len(error.suggestions) == initial_count + 1
        
        # Find our custom suggestion
        custom_suggestions = [s for s in error.suggestions if s.action == "Custom action"]
        assert len(custom_suggestions) == 1
        assert custom_suggestions[0].description == "Custom description"


@pytest.mark.cli
class TestErrorAnalyticsImport:
    """Test that error analytics can be imported."""
    
    def test_error_analytics_import(self):
        """Test that error analytics functions can be imported."""
        from redditdl.core.error_context import get_error_analytics, report_error
        
        # Should be able to import these functions
        assert callable(get_error_analytics)
        assert callable(report_error)
    
    def test_error_context_creation(self):
        """Test that ErrorContext can be created."""
        context = ErrorContext(
            operation="test_operation",
            target="test_target"
        )
        assert context.operation == "test_operation"
        assert context.target == "test_target"


@pytest.mark.cli
class TestSimpleErrorIntegration:
    """Test simple error integration without deep mocking."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_authentication_error_in_scrape(self, runner):
        """Test that authentication errors are handled in scrape command."""
        with patch('redditdl.cli.commands.scrape.validate_api_credentials') as mock_validate:
            # Create a simple auth error
            auth_error = AuthenticationError(
                "Test auth error",
                error_code=ErrorCode.AUTH_INVALID_CREDENTIALS
            )
            mock_validate.side_effect = auth_error
            
            result = runner.invoke(scrape_app, ["user", "testuser", "--api"])
            
            # Should exit with non-zero code (may be 1 or 2 depending on where error occurs)
            assert result.exit_code != 0
            # Should show some kind of error message
            assert any(word in result.output.lower() for word in ["error", "auth", "credentials", "invalid"])
    
    def test_configuration_error_in_scrape(self, runner):
        """Test that configuration errors are handled in scrape command."""
        with patch('redditdl.cli.commands.scrape.load_config_from_cli') as mock_config:
            # Create a simple config error
            config_error = ConfigurationError(
                "Test config error",
                error_code=ErrorCode.CONFIG_INVALID_VALUE
            )
            mock_config.side_effect = config_error
            
            result = runner.invoke(scrape_app, ["user", "testuser"])
            
            # Should exit with non-zero code (may be 1 or 2 depending on where error occurs)
            assert result.exit_code != 0
            # Should show some kind of error message
            assert any(word in result.output.lower() for word in ["error", "config", "invalid"])


@pytest.mark.cli
class TestAuditCommands:
    """Test audit command functionality."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_audit_check_help(self, runner):
        """Test audit check command help."""
        result = runner.invoke(audit_app, ["check", "--help"])
        assert result.exit_code == 0
        assert "check" in result.output.lower()
    
    def test_audit_repair_help(self, runner):
        """Test audit repair command help."""
        result = runner.invoke(audit_app, ["repair", "--help"])
        assert result.exit_code == 0
        assert "repair" in result.output.lower()
    
    def test_audit_stats_help(self, runner):
        """Test audit stats command help."""
        result = runner.invoke(audit_app, ["stats", "--help"])
        assert result.exit_code == 0
        assert "stats" in result.output.lower()
    
    def test_audit_check_nonexistent_path(self, runner):
        """Test audit check with non-existent path."""
        # Use a path that definitely doesn't exist
        nonexistent_path = "/this/path/definitely/does/not/exist/anywhere"
        result = runner.invoke(audit_app, ["check", nonexistent_path])
        
        # Should exit with error
        assert result.exit_code != 0
        # Should mention the path or that it doesn't exist
        output_lower = result.output.lower()
        assert any(word in output_lower for word in ["not found", "exist", "error", "path"])


@pytest.mark.cli
class TestInteractiveCommands:
    """Test interactive command functionality."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_interactive_help(self, runner):
        """Test interactive command help."""
        from redditdl.cli.commands.interactive import app as interactive_app
        
        result = runner.invoke(interactive_app, ["--help"])
        assert result.exit_code == 0
        assert "interactive" in result.output.lower()
    
    def test_interactive_start_help(self, runner):
        """Test interactive start command help."""
        from redditdl.cli.commands.interactive import app as interactive_app
        
        result = runner.invoke(interactive_app, ["start", "--help"])
        assert result.exit_code == 0
        assert "start" in result.output.lower()


if __name__ == "__main__":
    pytest.main([__file__])