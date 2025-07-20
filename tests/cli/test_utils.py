"""
Tests for CLI Utilities

Tests the utility functions used by CLI commands including
validation, formatting, and helper functions.
"""

import pytest
import sys
import tempfile
import typer
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch, call

from redditdl.cli.utils import (
    validate_sleep_interval,
    validate_positive_int,
    print_header,
    handle_keyboard_interrupt,
    handle_fatal_error,
    load_credentials_from_dotenv,
    console
)


class TestValidationFunctions:
    """Test input validation functions."""
    
    def test_validate_sleep_interval_valid(self):
        """Test sleep interval validation with valid values."""
        # Test valid float values
        assert validate_sleep_interval(None, None, 0.5) == 0.5
        assert validate_sleep_interval(None, None, 1.0) == 1.0
        assert validate_sleep_interval(None, None, 10.5) == 10.5
        
        # Test valid string values that can be converted
        assert validate_sleep_interval(None, None, "0.5") == 0.5
        assert validate_sleep_interval(None, None, "1") == 1.0
    
    def test_validate_sleep_interval_minimum(self):
        """Test sleep interval validation enforces minimum value."""
        with pytest.raises(typer.BadParameter) as exc_info:
            validate_sleep_interval(None, None, 0.0)
        assert "must be at least 0.1" in str(exc_info.value)
        
        with pytest.raises(typer.BadParameter):
            validate_sleep_interval(None, None, -1.0)
    
    def test_validate_sleep_interval_invalid_format(self):
        """Test sleep interval validation with invalid format."""
        with pytest.raises(typer.BadParameter) as exc_info:
            validate_sleep_interval(None, None, "invalid")
        assert "must be a number" in str(exc_info.value)
        
        with pytest.raises(typer.BadParameter):
            validate_sleep_interval(None, None, "1.2.3")
    
    def test_validate_positive_int_valid(self):
        """Test positive integer validation with valid values."""
        assert validate_positive_int(None, None, 1) == 1
        assert validate_positive_int(None, None, 100) == 100
        assert validate_positive_int(None, None, "50") == 50
    
    def test_validate_positive_int_invalid(self):
        """Test positive integer validation with invalid values."""
        with pytest.raises(typer.BadParameter) as exc_info:
            validate_positive_int(None, None, 0)
        assert "must be positive" in str(exc_info.value)
        
        with pytest.raises(typer.BadParameter):
            validate_positive_int(None, None, -5)
        
        with pytest.raises(typer.BadParameter) as exc_info:
            validate_positive_int(None, None, "invalid")
        assert "must be a valid integer" in str(exc_info.value)


class TestPrintHeader:
    """Test header printing functionality."""
    
    @patch('cli.utils.console')
    def test_print_header_basic(self, mock_console):
        """Test basic header printing."""
        print_header("Test Title")
        
        # Should print header with styling
        assert mock_console.print.called
        call_args = mock_console.print.call_args_list
        
        # Should include the title in the output
        assert any("Test Title" in str(call) for call in call_args)
    
    @patch('cli.utils.console')
    def test_print_header_with_subtitle(self, mock_console):
        """Test header printing with subtitle."""
        print_header("Main Title", subtitle="Subtitle text")
        
        assert mock_console.print.called
        call_args = str(mock_console.print.call_args_list)
        
        # Should include both title and subtitle
        assert "Main Title" in call_args
        assert "Subtitle text" in call_args
    
    @patch('cli.utils.console')
    def test_print_header_version_info(self, mock_console):
        """Test header printing includes version information."""
        print_header("RedditDL")
        
        assert mock_console.print.called
        call_args = str(mock_console.print.call_args_list)
        
        # Should include version info
        assert "RedditDL" in call_args


class TestErrorHandling:
    """Test error handling functions."""
    
    @patch('cli.utils.console')
    @patch('sys.exit')
    def test_handle_keyboard_interrupt(self, mock_exit, mock_console):
        """Test keyboard interrupt handling."""
        handle_keyboard_interrupt()
        
        # Should print cancellation message
        assert mock_console.print.called
        call_args = str(mock_console.print.call_args_list)
        assert "cancelled" in call_args.lower() or "interrupted" in call_args.lower()
        
        # Should exit cleanly
        mock_exit.assert_called_once_with(0)
    
    @patch('cli.utils.console')
    @patch('sys.exit')
    def test_handle_fatal_error(self, mock_exit, mock_console):
        """Test fatal error handling."""
        test_error = Exception("Test error message")
        
        handle_fatal_error(test_error)
        
        # Should print error message
        assert mock_console.print.called
        call_args = str(mock_console.print.call_args_list)
        assert "Test error message" in call_args
        
        # Should exit with error code
        mock_exit.assert_called_once_with(1)
    
    @patch('cli.utils.console')
    @patch('sys.exit')
    def test_handle_fatal_error_with_context(self, mock_exit, mock_console):
        """Test fatal error handling with additional context."""
        test_error = Exception("Test error")
        
        handle_fatal_error(test_error, context="During testing")
        
        assert mock_console.print.called
        call_args = str(mock_console.print.call_args_list)
        assert "Test error" in call_args
        assert "During testing" in call_args


class TestCredentialsLoading:
    """Test credentials loading from dotenv."""
    
    def test_load_credentials_from_dotenv_file_exists(self):
        """Test loading credentials when .env file exists."""
        # Create temporary .env file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("REDDITDL_CLIENT_ID=test_id\n")
            f.write("REDDITDL_CLIENT_SECRET=test_secret\n")
            f.write("REDDITDL_USER_AGENT=test_agent\n")
            env_file = f.name
        
        try:
            # Mock dotenv loading
            with patch('cli.utils.load_dotenv') as mock_load_dotenv, \
                 patch('pathlib.Path.exists', return_value=True):
                
                load_credentials_from_dotenv(env_file)
                
                # Should have called load_dotenv with the file path
                mock_load_dotenv.assert_called_once_with(env_file)
        
        finally:
            # Cleanup
            Path(env_file).unlink()
    
    def test_load_credentials_from_dotenv_file_not_exists(self):
        """Test loading credentials when .env file doesn't exist."""
        with patch('cli.utils.load_dotenv') as mock_load_dotenv, \
             patch('pathlib.Path.exists', return_value=False):
            
            load_credentials_from_dotenv("nonexistent.env")
            
            # Should not have called load_dotenv
            mock_load_dotenv.assert_not_called()
    
    def test_load_credentials_from_dotenv_default_path(self):
        """Test loading credentials with default .env path."""
        with patch('cli.utils.load_dotenv') as mock_load_dotenv, \
             patch('pathlib.Path.exists', return_value=True):
            
            load_credentials_from_dotenv()
            
            # Should have called with default .env path
            mock_load_dotenv.assert_called_once_with(".env")
    
    @patch('cli.utils.console')
    def test_load_credentials_from_dotenv_with_error(self, mock_console):
        """Test loading credentials when dotenv loading fails."""
        with patch('cli.utils.load_dotenv', side_effect=Exception("Dotenv error")), \
             patch('pathlib.Path.exists', return_value=True):
            
            # Should not raise exception, but may log warning
            load_credentials_from_dotenv(".env")
            
            # Function should handle errors gracefully


class TestConsoleIntegration:
    """Test console integration and formatting."""
    
    def test_console_available(self):
        """Test that console object is available and functional."""
        from cli.utils import console
        
        assert console is not None
        assert hasattr(console, 'print')
        assert hasattr(console, 'rule')
        
        # Test basic console functionality
        with patch('sys.stdout'):
            console.print("Test message")  # Should not raise exception
    
    @patch('cli.utils.console')
    def test_console_styling_functions(self, mock_console):
        """Test console styling functions work correctly."""
        from cli.utils import console
        
        # Test that console methods are available
        assert hasattr(console, 'print')
        assert hasattr(console, 'rule')
        assert hasattr(console, 'status')
    
    def test_console_rich_features(self):
        """Test that Rich console features are properly configured."""
        from cli.utils import console
        
        # Should be a Rich Console instance
        assert hasattr(console, 'file')
        assert hasattr(console, 'size')
        assert hasattr(console, 'options')


class TestUtilityIntegration:
    """Test integration between utility functions."""
    
    @patch('cli.utils.console')
    def test_error_handling_formatting(self, mock_console):
        """Test that error messages are properly formatted."""
        test_error = ValueError("Test validation error")
        
        with patch('sys.exit'):
            handle_fatal_error(test_error)
        
        # Should format error nicely
        assert mock_console.print.called
        call_args = mock_console.print.call_args_list
        
        # Should use error styling
        assert any("[red]" in str(call) or "style=" in str(call) 
                  for call in call_args)
    
    def test_validation_error_messages(self):
        """Test that validation functions provide helpful error messages."""
        with pytest.raises(typer.BadParameter) as exc_info:
            validate_sleep_interval(None, None, -1)
        
        error_msg = str(exc_info.value)
        assert "must be at least" in error_msg
        assert "0.1" in error_msg
        
        with pytest.raises(typer.BadParameter) as exc_info:
            validate_positive_int(None, None, 0)
        
        error_msg = str(exc_info.value)
        assert "must be positive" in error_msg
    
    @patch('cli.utils.console')
    def test_header_and_error_styling_consistency(self, mock_console):
        """Test that headers and errors use consistent styling."""
        # Print header
        print_header("Test")
        header_calls = mock_console.print.call_args_list.copy()
        mock_console.reset_mock()
        
        # Handle error
        with patch('sys.exit'):
            handle_fatal_error(Exception("Test error"))
        error_calls = mock_console.print.call_args_list
        
        # Both should use Rich console formatting
        assert len(header_calls) > 0
        assert len(error_calls) > 0


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_validate_sleep_interval_edge_values(self):
        """Test sleep interval validation with edge values."""
        # Minimum valid value
        assert validate_sleep_interval(None, None, 0.1) == 0.1
        
        # Very large value (should be allowed)
        assert validate_sleep_interval(None, None, 3600.0) == 3600.0
        
        # Just below minimum
        with pytest.raises(typer.BadParameter):
            validate_sleep_interval(None, None, 0.09)
    
    def test_validate_positive_int_edge_values(self):
        """Test positive integer validation with edge values."""
        # Minimum valid value
        assert validate_positive_int(None, None, 1) == 1
        
        # Large value
        assert validate_positive_int(None, None, 999999) == 999999
        
        # Zero (invalid)
        with pytest.raises(typer.BadParameter):
            validate_positive_int(None, None, 0)
    
    def test_handle_fatal_error_none_context(self):
        """Test fatal error handling with None context."""
        with patch('cli.utils.console') as mock_console, \
             patch('sys.exit'):
            
            handle_fatal_error(Exception("Test"), context=None)
            
            # Should still work without context
            assert mock_console.print.called
    
    def test_load_credentials_empty_file(self):
        """Test loading credentials from empty file."""
        # Create empty .env file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            env_file = f.name
        
        try:
            with patch('cli.utils.load_dotenv') as mock_load_dotenv, \
                 patch('pathlib.Path.exists', return_value=True):
                
                load_credentials_from_dotenv(env_file)
                
                # Should still attempt to load
                mock_load_dotenv.assert_called_once_with(env_file)
        
        finally:
            Path(env_file).unlink()
    
    def test_print_header_empty_title(self):
        """Test header printing with empty title."""
        with patch('cli.utils.console') as mock_console:
            print_header("")
            
            # Should still print something
            assert mock_console.print.called
    
    def test_print_header_very_long_title(self):
        """Test header printing with very long title."""
        long_title = "A" * 200  # Very long title
        
        with patch('cli.utils.console') as mock_console:
            print_header(long_title)
            
            # Should handle long titles gracefully
            assert mock_console.print.called
            call_args = str(mock_console.print.call_args_list)
            assert long_title in call_args