"""
Tests for CLI Commands

Tests the CLI command implementations including scrape, audit, and interactive commands.
"""

import asyncio
import pytest
import tempfile
import typer
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from typer.testing import CliRunner

from redditdl.cli.main import app as main_app


@pytest.mark.cli
class TestScrapeCommand:
    """Test the scrape command functionality."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.runner = CliRunner()
    
    @patch('redditdl.cli.commands.scrape.asyncio.run')
    @patch('redditdl.cli.commands.scrape._scrape_user_pipeline')
    def test_scrape_user_basic(self, mock_scrape_pipeline, mock_asyncio_run):
        """Test basic user scraping command."""
        # Mock async pipeline execution
        mock_scrape_pipeline.return_value = None
        mock_asyncio_run.return_value = None
        
        # Run command
        result = self.runner.invoke(main_app, ['scrape', 'user', 'testuser'])
        
        # Verify result
        assert result.exit_code == 0
        assert "Downloading from user: testuser" in result.stdout
    
    def test_scrape_user_dry_run(self):
        """Test scrape command with dry run option."""
        result = self.runner.invoke(main_app, ['scrape', 'user', 'testuser', '--dry-run'])
        assert result.exit_code == 0
        assert "Dry Run" in result.stdout
    
    def test_scrape_user_api_mode(self):
        """Test scrape command with API mode."""
        result = self.runner.invoke(main_app, ['scrape', 'user', 'testuser', '--api'])
        assert result.exit_code == 0
        assert "API" in result.stdout

    def test_scrape_user_with_options(self):
        """Test scrape command with various options."""
        result = self.runner.invoke(main_app, [
            'scrape', 'user', 'testuser',
            '--limit', '50',
            '--sleep', '1.5',
            '--output', 'test_downloads',
            '--verbose'
        ])
        
        assert result.exit_code == 0
        assert "test_downloads" in result.stdout
        assert "50" in result.stdout
    
    def test_scrape_user_invalid_arguments(self):
        """Test scrape command with invalid arguments."""
        result = self.runner.invoke(main_app, ['scrape', 'user', 'testuser', '--sleep', '-1'])
        assert result.exit_code != 0
        assert "Invalid value" in result.stdout

    def test_scrape_subreddit_placeholder(self):
        """Test subreddit command shows placeholder message."""
        result = self.runner.invoke(main_app, ["scrape", "subreddit", "pics"])
        assert result.exit_code == 0
        assert "Subreddit scraping will be implemented" in result.stdout

    def test_scrape_url_placeholder(self):
        """Test URL command shows placeholder message."""
        result = self.runner.invoke(main_app, ["scrape", "url", "https://reddit.com/r/pics"])
        assert result.exit_code == 0
        assert "URL scraping will be implemented" in result.stdout

class TestAuditCommand:
    """Test the audit command functionality."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.runner = CliRunner()
    
    @patch('redditdl.cli.commands.audit.ArchiveAuditor')
    def test_audit_check_basic(self, mock_auditor_class):
        """Test basic archive audit check."""
        mock_auditor = Mock()
        mock_auditor.audit_archive.return_value = []
        mock_auditor_class.return_value = mock_auditor
        
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(main_app, ['audit', 'check', temp_dir])
            
            assert result.exit_code == 0
            assert "Auditing archive" in result.stdout
            mock_auditor.audit_archive.assert_called_once()
    
    @patch('redditdl.cli.commands.audit.ArchiveAuditor')
    def test_audit_repair_force(self, mock_auditor_class):
        """Test archive repair with force option."""
        mock_auditor = Mock()
        mock_auditor.audit_archive.return_value = [{'type': 'test_issue'}]
        mock_auditor.repair_archive.return_value = [{'operation': 'test', 'success': True, 'count': 1}]
        mock_auditor_class.return_value = mock_auditor
        
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(main_app, ['audit', 'repair', temp_dir, '--force'])
            
            assert result.exit_code == 0
            assert "Repairing" in result.stdout
    
    @patch('redditdl.cli.commands.audit.ArchiveAuditor')
    def test_audit_compress_command(self, mock_auditor_class):
        """Test audit compress CLI command."""
        mock_auditor = mock_auditor_class.return_value
        mock_auditor.compress_archive.return_value = {
            'success': True,
            'output_path': 'test_archive.zip',
            'files_compressed': 10,
            'output_size': 1024,
            'original_size': 2048,
            'compression_ratio': 0.5
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(main_app, ['audit', 'compress', temp_dir])
            
            assert result.exit_code == 0
            assert "Archive Compression" in result.stdout

class TestInteractiveCommand:
    """Test the interactive command functionality."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.runner = CliRunner()
    
    @patch('redditdl.cli.commands.interactive.InteractiveShell')
    def test_interactive_basic(self, mock_shell_class):
        """Test basic interactive mode launch."""
        mock_shell = Mock()
        mock_shell.start_repl = AsyncMock()
        mock_shell_class.return_value = mock_shell
        
        with patch('asyncio.run') as mock_asyncio_run:
            result = self.runner.invoke(main_app, ['interactive'])
            
            assert result.exit_code == 0
            mock_shell_class.assert_called_once()
            mock_asyncio_run.assert_called_once()

class TestMainApp:
    """Test the main CLI application."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.runner = CliRunner()
    
    def test_main_app_help(self):
        """Test main app help output."""
        result = self.runner.invoke(main_app, ['--help'])
        
        assert result.exit_code == 0
        assert "RedditDL - Modern Reddit Media Downloader" in result.stdout
    
    def test_main_app_version(self):
        """Test main app version output."""
        result = self.runner.invoke(main_app, ['--version'])
        
        assert result.exit_code == 0
        assert "RedditDL version" in result.stdout
    
    def test_main_app_invalid_command(self):
        """Test main app with invalid command."""
        result = self.runner.invoke(main_app, ['invalid_command'])
        
        assert result.exit_code != 0
        assert "No such command" in result.stdout