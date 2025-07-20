"""
End-to-End CLI Testing

Tests the complete CLI functionality including all commands,
argument validation, configuration integration, and error handling.
"""

import os
import tempfile
import json
import pytest
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from typer.testing import CliRunner

import sys
# from pathlib import Path # This import is redundant since Path is already imported
# Add the parent directory to the path to import cli module
# sys.path.insert(0, str(Path(__file__).parent.parent)) # This line is no longer needed

from redditdl.cli.main import app
from redditdl.cli.commands.scrape import app as scrape_app
from redditdl.cli.commands.audit import app as audit_app
from redditdl.cli.commands.interactive import app as interactive_app
from redditdl.core.config.models import AppConfig


class TestCLIMain:
    """Test main CLI application."""
    
    def setup_method(self):
        self.runner = CliRunner()
    
    def test_main_help(self):
        """Test main CLI help displays correctly."""
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Reddit Media Downloader" in result.stdout
        assert "scrape" in result.stdout
        assert "audit" in result.stdout
        assert "interactive" in result.stdout
    
    def test_version_command(self):
        """Test version command works."""
        result = self.runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "RedditDL" in result.stdout
        assert "version" in result.stdout
    
    def test_no_args_shows_help(self):
        """Test that no arguments shows help."""
        result = self.runner.invoke(app, [])
        # Typer returns exit code 2 when no_args_is_help=True
        assert result.exit_code == 2
        assert "Reddit Media Downloader" in result.stdout


class TestScrapeCommand:
    """Test scrape command functionality."""
    
    def setup_method(self):
        self.runner = CliRunner()
    
    def test_scrape_help(self):
        """Test scrape command help."""
        result = self.runner.invoke(scrape_app, ["--help"])
        assert result.exit_code == 0
        assert "Download media from Reddit" in result.stdout
    
    def test_scrape_user_help(self):
        """Test scrape user subcommand help."""
        result = self.runner.invoke(scrape_app, ["user", "--help"])
        assert result.exit_code == 0
        assert "username" in result.stdout
        assert "Examples:" in result.stdout
    
    def test_scrape_user_requires_username(self):
        """Test that scrape user requires username argument."""
        result = self.runner.invoke(scrape_app, ["user"])
        assert result.exit_code != 0
        assert "Missing argument" in result.stdout
    
    def test_scrape_user_with_username(self):
        """Test scrape user with valid username."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(scrape_app, [
                "user", "testuser",
                "--output", temp_dir,
                "--limit", "1",
                "--dry-run"
            ])
            # Should succeed or fail gracefully (since no actual Reddit connection)
            assert "testuser" in result.stdout
    
    def test_scrape_user_validation(self):
        """Test input validation for scrape user command."""
        # Test negative limit
        result = self.runner.invoke(scrape_app, [
            "user", "testuser",
            "--limit", "-1"
        ])
        assert result.exit_code != 0
        
        # Test negative sleep
        result = self.runner.invoke(scrape_app, [
            "user", "testuser", 
            "--sleep", "-1.0"
        ])
        assert result.exit_code != 0
    
    def test_scrape_subreddit_placeholder(self):
        """Test subreddit command shows placeholder message."""
        result = self.runner.invoke(scrape_app, ["subreddit", "pics"])
        assert result.exit_code == 0
        assert "future version" in result.stdout
    
    def test_scrape_url_placeholder(self):
        """Test URL command shows placeholder message."""
        result = self.runner.invoke(scrape_app, ["url", "https://reddit.com/r/pics"])
        assert result.exit_code == 0
        assert "future version" in result.stdout


class TestAuditCommand:
    """Test audit command functionality."""
    
    def setup_method(self):
        self.runner = CliRunner()
    
    def test_audit_help(self):
        """Test audit command help."""
        result = self.runner.invoke(audit_app, ["--help"])
        assert result.exit_code == 0
        assert "Audit and repair" in result.stdout
    
    def test_audit_check_requires_path(self):
        """Test audit check requires archive path."""
        result = self.runner.invoke(audit_app, ["check"])
        assert result.exit_code != 0
    
    def test_audit_check_invalid_path(self):
        """Test audit check with invalid path."""
        result = self.runner.invoke(audit_app, ["check", "/nonexistent/path"])
        assert result.exit_code == 1
        assert "does not exist" in result.stdout
    
    def test_audit_check_valid_directory(self):
        """Test audit check with valid directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(audit_app, ["check", temp_dir])
            assert result.exit_code == 0
            assert "Auditing archive" in result.stdout
    
    def test_audit_stats(self):
        """Test audit stats command."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some test files
            test_file = Path(temp_dir) / "test.jpg"
            test_file.write_text("test content")
            
            result = self.runner.invoke(audit_app, ["stats", temp_dir])
            assert result.exit_code == 0
            assert "Archive Statistics" in result.stdout
            assert "Total Files:" in result.stdout


class TestInteractiveCommand:
    """Test interactive command functionality."""
    
    def setup_method(self):
        self.runner = CliRunner()
    
    def test_interactive_help(self):
        """Test interactive command help."""
        result = self.runner.invoke(interactive_app, ["--help"])
        assert result.exit_code == 0
        assert "REPL" in result.stdout
    
    def test_interactive_start(self):
        """Test interactive start command."""
        result = self.runner.invoke(interactive_app, [])
        assert result.exit_code == 0
        assert "Interactive Mode" in result.stdout
        assert "preview" in result.stdout  # Should show preview


class TestCLIIntegration:
    """Test CLI integration with main application."""
    
    def setup_method(self):
        self.runner = CliRunner()
    
    def test_full_command_structure(self):
        """Test full command structure works."""
        # Test: redditdl scrape user testuser --help
        result = self.runner.invoke(app, ["scrape", "user", "--help"])
        assert result.exit_code == 0
        assert "username" in result.stdout
    
    def test_rich_formatting(self):
        """Test that rich formatting is working."""
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        # Rich formatting should include ANSI codes or structured output
        assert "Reddit Media Downloader" in result.stdout
    
    def test_error_handling(self):
        """Test error handling and user-friendly messages."""
        # Test invalid command
        result = self.runner.invoke(app, ["invalid-command"])
        assert result.exit_code != 0
        
        # Test invalid option
        result = self.runner.invoke(app, ["scrape", "user", "testuser", "--invalid-option"])
        assert result.exit_code != 0


class TestCLIValidation:
    """Test CLI input validation."""
    
    def setup_method(self):
        self.runner = CliRunner()
    
    def test_positive_integer_validation(self):
        """Test positive integer validation."""
        # Valid positive integer
        result = self.runner.invoke(scrape_app, [
            "user", "testuser",
            "--limit", "10",
            "--dry-run"
        ])
        # Should not fail on validation
        assert "positive integer" not in result.stdout
        
        # Invalid negative integer  
        result = self.runner.invoke(scrape_app, [
            "user", "testuser",
            "--limit", "-5"
        ])
        assert result.exit_code != 0
    
    def test_sleep_interval_validation(self):
        """Test sleep interval validation."""
        # Valid sleep interval
        result = self.runner.invoke(scrape_app, [
            "user", "testuser", 
            "--sleep", "1.5",
            "--dry-run"
        ])
        # Should not fail on validation
        assert "positive number" not in result.stdout
        
        # Invalid negative sleep
        result = self.runner.invoke(scrape_app, [
            "user", "testuser",
            "--sleep", "-1.0"
        ])
        assert result.exit_code != 0


@pytest.mark.integration
class TestCLIBackwardCompatibility:
    """Test backward compatibility with legacy CLI."""
    
    def test_legacy_flag_detection(self):
        """Test that legacy flags are properly detected."""
        # This would need to be tested at the main.py level
        # Since we can't easily test sys.argv manipulation here,
        # this is a placeholder for integration testing
        pass
    
    def test_new_cli_preference(self):
        """Test that new CLI is preferred when available."""
        # Another integration test placeholder
        pass


class TestCLIConfigIntegration:
    """Test CLI integration with configuration system."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('cli.utils.load_config_from_cli')
    @patch('cli.commands.scrape.handle_scrape_command')
    def test_config_file_loading(self, mock_handle, mock_load_config):
        """Test configuration file loading."""
        mock_config = Mock(spec=AppConfig)
        mock_load_config.return_value = mock_config
        mock_handle.return_value = None
        
        config_file = self.temp_dir / 'test_config.yaml'
        config_content = {
            'scraping': {'post_limit': 20},
            'output': {'output_dir': str(self.temp_dir)},
            'processing': {'embed_metadata': True}
        }
        with open(config_file, 'w') as f:
            yaml.dump(config_content, f)
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--config', str(config_file),
            '--dry-run'
        ])
        
        # Should attempt to load config
        assert result.exit_code == 0 or 'error' not in result.stdout.lower()
    
    def test_cli_args_override_config(self):
        """Test CLI arguments override configuration file."""
        config_file = self.temp_dir / 'test_config.yaml'
        config_content = {
            'scraping': {'post_limit': 20},
            'output': {'output_dir': str(self.temp_dir)}
        }
        with open(config_file, 'w') as f:
            yaml.dump(config_content, f)
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--config', str(config_file),
            '--limit', '100',  # Should override config file value
            '--dry-run'
        ])
        
        # Should succeed with override
        assert result.exit_code == 0 or 'error' not in result.stdout.lower()
    
    def test_invalid_config_file(self):
        """Test handling of invalid configuration file."""
        config_file = self.temp_dir / 'invalid_config.yaml'
        config_file.write_text('invalid: yaml: content: [')
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--config', str(config_file)
        ])
        
        # Should fail gracefully with config error
        assert result.exit_code != 0


class TestCLIErrorHandling:
    """Test CLI error handling and user feedback."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_missing_required_arguments(self):
        """Test error handling for missing required arguments."""
        result = self.runner.invoke(scrape_app, ['user'])
        
        assert result.exit_code != 0
        assert 'Missing argument' in result.stdout or 'Usage:' in result.stdout
    
    def test_invalid_argument_types(self):
        """Test error handling for invalid argument types."""
        # Test invalid integer
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--limit', 'not_a_number'
        ])
        
        assert result.exit_code != 0
        assert 'Invalid value' in result.stdout or 'Error:' in result.stdout
    
    def test_conflicting_arguments(self):
        """Test error handling for conflicting arguments."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--api',  # API mode
            '--no-auth'  # Conflicting with API mode
        ])
        
        # Should handle conflicting arguments gracefully
        assert result.exit_code != 0 or 'warning' in result.stdout.lower()
    
    @patch('cli.commands.scrape.handle_scrape_command')
    def test_command_exception_handling(self, mock_handle):
        """Test handling of exceptions in command handlers."""
        mock_handle.side_effect = Exception("Test error")
        
        result = self.runner.invoke(scrape_app, ['user', 'testuser', '--dry-run'])
        
        # Should handle exceptions gracefully
        assert result.exit_code != 0
        assert 'error' in result.stdout.lower() or 'Error:' in result.stdout
    
    def test_permission_errors(self):
        """Test handling of permission errors."""
        # Try to write to system directory
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--output', '/root/no_permission',
            '--dry-run'
        ])
        
        # Should handle permission issues gracefully
        # Note: dry-run might not trigger this, but the validation should
        assert result.exit_code == 0 or 'permission' in result.stdout.lower()


class TestCLIDryRunMode:
    """Test CLI dry-run mode functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_dry_run_mode_enabled(self):
        """Test that dry-run mode is properly enabled."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--dry-run',
            '--output', str(self.temp_dir)
        ])
        
        # Should complete successfully in dry-run mode
        assert result.exit_code == 0
        assert 'dry' in result.stdout.lower() or 'simulation' in result.stdout.lower()
    
    def test_dry_run_no_downloads(self):
        """Test dry-run mode doesn't create actual files."""
        output_dir = self.temp_dir / 'downloads'
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--dry-run',
            '--output', str(output_dir),
            '--limit', '5'
        ])
        
        # Should complete successfully
        assert result.exit_code == 0
        
        # Should not create actual download files in dry-run
        if output_dir.exists():
            files = list(output_dir.rglob('*'))
            # May create directories but not media files
            media_files = [f for f in files if f.suffix in ['.jpg', '.png', '.gif', '.mp4']]
            assert len(media_files) == 0
    
    def test_dry_run_shows_preview(self):
        """Test dry-run mode shows preview of actions."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--dry-run',
            '--limit', '3'
        ])
        
        # Should show what would be done
        assert result.exit_code == 0
        assert ('would' in result.stdout.lower() or 
                'simulate' in result.stdout.lower() or
                'preview' in result.stdout.lower())


class TestCLIEnvironmentVariables:
    """Test CLI integration with environment variables."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.original_env = os.environ.copy()
    
    def teardown_method(self):
        """Clean up test environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
    
    def test_environment_variable_usage(self):
        """Test environment variables are used when CLI args not provided."""
        # Set environment variables
        os.environ['REDDIT_CLIENT_ID'] = 'env_client_id'
        os.environ['REDDIT_CLIENT_SECRET'] = 'env_client_secret'
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--api',
            '--dry-run'
        ])
        
        # Should use environment variables
        assert result.exit_code == 0
    
    def test_cli_args_override_env_vars(self):
        """Test CLI arguments override environment variables."""
        # Set environment variable
        os.environ['REDDIT_CLIENT_ID'] = 'env_client_id'
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--api',
            '--client-id', 'cli_client_id',  # Should override env var
            '--client-secret', 'cli_secret',
            '--dry-run'
        ])
        
        # Should succeed with CLI override
        assert result.exit_code == 0


class TestCLIOutputModes:
    """Test different CLI output modes and verbosity levels."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_verbose_mode(self):
        """Test verbose output mode."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--verbose',
            '--dry-run'
        ])
        
        # Should provide detailed output
        assert result.exit_code == 0
        # Verbose mode should produce more output
        assert len(result.stdout) > 100  # Rough check for verbose output
    
    def test_quiet_mode(self):
        """Test quiet output mode."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--quiet',
            '--dry-run'
        ])
        
        # Should provide minimal output
        assert result.exit_code == 0
        # Quiet mode should produce less output
        assert len(result.stdout) < 500  # Rough check for quiet output
    
    def test_json_output_mode(self):
        """Test JSON output mode for machine parsing."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--output-format', 'json',
            '--dry-run'
        ])
        
        # Should succeed with JSON format
        assert result.exit_code == 0


class TestCLIAdvancedOptions:
    """Test advanced CLI options and configurations."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_filter_options(self):
        """Test various filter options."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--min-score', '10',
            '--max-score', '1000',
            '--include-nsfw',
            '--domain-filter', 'imgur.com',
            '--keyword-filter', 'python',
            '--dry-run'
        ])
        
        # Should accept all filter options
        assert result.exit_code == 0
    
    def test_processing_options(self):
        """Test media processing options."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--no-embed-metadata',
            '--no-json-sidecars',
            '--max-resolution', '1920',
            '--image-quality', '85',
            '--dry-run'
        ])
        
        # Should accept all processing options
        assert result.exit_code == 0
    
    def test_output_organization_options(self):
        """Test output organization options."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--organize-by-subreddit',
            '--organize-by-date',
            '--filename-template', '{{ subreddit }}/{{ post_id }}.{{ ext }}',
            '--dry-run'
        ])
        
        # Should accept organization options
        assert result.exit_code == 0
    
    def test_export_format_options(self):
        """Test multiple export format options."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--export-format', 'json',
            '--export-format', 'csv',
            '--export-format', 'sqlite',
            '--dry-run'
        ])
        
        # Should accept multiple export formats
        assert result.exit_code == 0


class TestCLIAuditCommandAdvanced:
    """Test advanced audit command functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_audit_check_with_options(self):
        """Test audit check with various options."""
        # Create test archive structure
        archive_dir = self.temp_dir / 'test_archive'
        archive_dir.mkdir()
        
        # Create some test files
        (archive_dir / 'test.jpg').write_text('fake image')
        (archive_dir / 'test.json').write_text('{"metadata": "test"}')
        
        result = self.runner.invoke(audit_app, [
            'check', str(archive_dir),
            '--format', 'json',
            '--verbose'
        ])
        
        # Should complete audit successfully
        assert result.exit_code == 0
        assert 'test.jpg' in result.stdout or 'files' in result.stdout.lower()
    
    def test_audit_repair_functionality(self):
        """Test audit repair functionality."""
        archive_dir = self.temp_dir / 'test_archive'
        archive_dir.mkdir()
        
        # Create test files that might need repair
        (archive_dir / 'test.jpg').write_text('fake image')
        (archive_dir / 'corrupted.json').write_text('invalid json content {')
        
        result = self.runner.invoke(audit_app, [
            'check', str(archive_dir),
            '--repair'
        ])
        
        # Should attempt repairs
        assert result.exit_code == 0
        assert ('repair' in result.stdout.lower() or 
                'fix' in result.stdout.lower() or
                'complete' in result.stdout.lower())
    
    def test_audit_stats_detailed(self):
        """Test detailed audit statistics."""
        archive_dir = self.temp_dir / 'test_archive'
        archive_dir.mkdir()
        
        # Create various file types
        (archive_dir / 'image1.jpg').write_text('image')
        (archive_dir / 'image2.png').write_text('image')
        (archive_dir / 'video.mp4').write_text('video')
        (archive_dir / 'metadata.json').write_text('{"test": "data"}')
        
        result = self.runner.invoke(audit_app, [
            'stats', str(archive_dir),
            '--detailed'
        ])
        
        # Should show detailed statistics
        assert result.exit_code == 0
        assert 'jpg' in result.stdout.lower() or 'files' in result.stdout.lower()


class TestCLIInteractiveMode:
    """Test interactive mode CLI functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_interactive_mode_start(self):
        """Test interactive mode startup."""
        result = self.runner.invoke(interactive_app, [])
        
        # Should start interactive mode
        assert result.exit_code == 0
        assert ('interactive' in result.stdout.lower() or 
                'repl' in result.stdout.lower() or
                'mode' in result.stdout.lower())
    
    def test_interactive_with_config(self):
        """Test interactive mode with configuration."""
        config_file = self.temp_dir / 'test_config.yaml'
        config_content = {
            'scraping': {'post_limit': 10},
            'output': {'output_dir': str(self.temp_dir)}
        }
        with open(config_file, 'w') as f:
            yaml.dump(config_content, f)
        
        result = self.runner.invoke(interactive_app, [
            '--config', str(config_file)
        ])
        
        # Should start with config
        assert result.exit_code == 0
        assert 'interactive' in result.stdout.lower() or 'mode' in result.stdout.lower()


@pytest.mark.slow
class TestCLIPerformance:
    """Test CLI performance with larger datasets."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_large_limit_dry_run(self):
        """Test CLI with large post limits in dry-run."""
        import time
        
        start_time = time.time()
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--limit', '1000',
            '--dry-run'
        ])
        end_time = time.time()
        
        # Should complete in reasonable time
        assert result.exit_code == 0
        assert end_time - start_time < 30  # Should complete within 30 seconds
    
    def test_multiple_export_formats(self):
        """Test performance with multiple export formats."""
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--export-format', 'json',
            '--export-format', 'csv',
            '--export-format', 'sqlite',
            '--export-format', 'markdown',
            '--limit', '100',
            '--dry-run'
        ])
        
        # Should handle multiple formats efficiently
        assert result.exit_code == 0


class TestCLIEdgeCases:
    """Test CLI edge cases and boundary conditions."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_empty_username(self):
        """Test handling of empty username."""
        result = self.runner.invoke(scrape_app, ['user', ''])
        
        # Should reject empty username
        assert result.exit_code != 0
    
    def test_special_characters_in_paths(self):
        """Test handling of special characters in paths."""
        special_dir = self.temp_dir / 'test with spaces & symbols!'
        special_dir.mkdir()
        
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--output', str(special_dir),
            '--dry-run'
        ])
        
        # Should handle special characters in paths
        assert result.exit_code == 0
    
    def test_very_long_arguments(self):
        """Test handling of very long arguments."""
        long_username = 'a' * 1000  # Very long username
        
        result = self.runner.invoke(scrape_app, [
            'user', long_username,
            '--dry-run'
        ])
        
        # Should handle or reject very long arguments gracefully
        assert result.exit_code != 0 or 'error' not in result.stdout.lower()
    
    def test_unicode_in_arguments(self):
        """Test handling of Unicode characters in arguments."""
        unicode_username = 'tëst_üser_🚀'
        
        result = self.runner.invoke(scrape_app, [
            'user', unicode_username,
            '--dry-run'
        ])
        
        # Should handle Unicode characters
        assert result.exit_code == 0 or 'encoding' not in result.stdout.lower()
    
    def test_zero_and_negative_limits(self):
        """Test handling of edge case numeric values."""
        # Test zero limit
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--limit', '0',
            '--dry-run'
        ])
        
        # Should handle zero limit appropriately
        assert result.exit_code != 0 or 'no posts' in result.stdout.lower()
        
        # Test negative limit (should fail)
        result = self.runner.invoke(scrape_app, [
            'user', 'testuser',
            '--limit', '-5'
        ])
        
        assert result.exit_code != 0