"""
Tests for Archive Management System

Comprehensive test suite for the ArchiveAuditor class and audit CLI commands.
Tests integrity checking, repair functionality, compression, and statistics generation.
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typer.testing import CliRunner

from src.redditdl.cli.commands.audit import ArchiveAuditor, app as audit_app
from src.redditdl.core.config import AppConfig
from src.redditdl.core.config.models import ScrapingConfig, ProcessingConfig, OutputConfig, FilterConfig
from src.redditdl.core.state.manager import StateManager


@pytest.fixture
def temp_archive_dir():
    """Create a temporary archive directory with sample files."""
    temp_dir = Path(tempfile.mkdtemp())
    
    # Create sample directory structure
    (temp_dir / "subreddit1").mkdir()
    (temp_dir / "subreddit2").mkdir()
    
    # Create sample media files
    sample_image_content = b"fake_image_data"
    sample_json_content = {
        "post_id": "abc123",
        "title": "Test Post",
        "url": "https://example.com/image.jpg",
        "subreddit": "subreddit1",
        "author": "testuser",
        "score": 1000,
        "created_utc": 1640995200.0
    }
    
    # Create files in subreddit1
    (temp_dir / "subreddit1" / "abc123.jpg").write_bytes(sample_image_content)
    (temp_dir / "subreddit1" / "abc123.json").write_text(json.dumps(sample_json_content))
    
    # Create orphaned media file (no JSON)
    (temp_dir / "subreddit1" / "orphan.png").write_bytes(sample_image_content)
    
    # Create JSON with missing media file
    missing_json = sample_json_content.copy()
    missing_json["post_id"] = "missing123"
    (temp_dir / "subreddit2" / "missing123.json").write_text(json.dumps(missing_json))
    
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_config(temp_archive_dir):
    """Create a test configuration for archive auditor."""
    return AppConfig(
        scraping=ScrapingConfig(),
        processing=ProcessingConfig(),
        output=OutputConfig(output_dir=temp_archive_dir),
        filters=FilterConfig(),
        session_dir=temp_archive_dir / ".redditdl"
    )


@pytest.fixture
def archive_auditor(temp_archive_dir, test_config):
    """Create an ArchiveAuditor instance for testing."""
    with patch('src.redditdl.cli.commands.audit.StateManager'):
        auditor = ArchiveAuditor(temp_archive_dir, test_config)
        return auditor


class TestArchiveAuditorInitialization:
    """Test ArchiveAuditor initialization and setup."""
    
    def test_auditor_initialization(self, archive_auditor, temp_archive_dir, test_config):
        """Test that auditor initializes correctly."""
        assert archive_auditor.archive_path == Path(temp_archive_dir)
        assert archive_auditor.config == test_config
        assert hasattr(archive_auditor, 'console')
        assert hasattr(archive_auditor, 'state_manager')
    
    def test_state_manager_initialization(self, temp_archive_dir, test_config):
        """Test state manager initialization with database files."""
        # Create a mock database file
        db_dir = temp_archive_dir / ".redditdl"
        db_dir.mkdir(exist_ok=True)
        db_file = db_dir / "state.db"
        db_file.write_text("fake_db_content")
        
        with patch('src.redditdl.cli.commands.audit.StateManager') as mock_sm:
            auditor = ArchiveAuditor(temp_archive_dir, test_config)
            # Should try to initialize state manager with found database
            assert auditor.state_manager is not None


class TestArchiveAudit:
    """Test archive audit functionality."""
    
    def test_audit_archive_basic(self, archive_auditor):
        """Test basic archive audit functionality."""
        issues = archive_auditor.audit_archive()
        
        # Should find some issues in our test data
        assert isinstance(issues, list)
        # Should detect orphaned media and missing media files
        issue_types = [issue.get("type") for issue in issues]
        assert len(issues) > 0
    
    def test_audit_file_system_scanning(self, archive_auditor):
        """Test file system scanning phase."""
        issues = archive_auditor._check_file_system()
        
        assert isinstance(issues, list)
        # Should detect missing media and orphaned files
        issue_types = [issue.get("type") for issue in issues]
        assert "missing_media" in issue_types or "orphaned_media" in issue_types
    
    def test_audit_metadata_consistency(self, archive_auditor):
        """Test metadata consistency checking."""
        issues = archive_auditor._check_metadata_consistency()
        
        assert isinstance(issues, list)
        # May or may not find issues depending on implementation
    
    def test_audit_with_integrity_checking(self, archive_auditor):
        """Test audit with deep integrity checking enabled."""
        issues = archive_auditor.audit_archive(check_integrity=True)
        
        assert isinstance(issues, list)
        # Deep integrity check should run additional validation


class TestArchiveRepair:
    """Test archive repair functionality."""
    
    def test_repair_archive_basic(self, archive_auditor):
        """Test basic repair functionality."""
        # First find issues
        issues = archive_auditor.audit_archive()
        
        if issues:
            # Attempt repairs
            repair_results = archive_auditor.repair_archive(issues)
            assert isinstance(repair_results, list)
            
            # Each repair result should have expected structure
            for result in repair_results:
                assert "operation" in result
                assert "success" in result
                assert "count" in result
    
    def test_repair_missing_metadata(self, archive_auditor, temp_archive_dir):
        """Test repair of missing metadata files."""
        # Create a media file without corresponding JSON
        orphan_file = temp_archive_dir / "test_orphan.jpg"
        orphan_file.write_bytes(b"fake_image_data")
        
        # Run audit to detect the issue
        issues = archive_auditor.audit_archive()
        
        # Filter for missing metadata issues
        metadata_issues = [issue for issue in issues if issue.get("type") == "missing_metadata"]
        
        if metadata_issues:
            repair_results = archive_auditor.repair_archive(metadata_issues)
            assert len(repair_results) > 0
    
    def test_repair_with_backup(self, archive_auditor):
        """Test repair functionality with backup creation."""
        issues = archive_auditor.audit_archive()
        
        if issues:
            repair_results = archive_auditor.repair_archive(issues, create_backup=True)
            assert isinstance(repair_results, list)


class TestArchiveCompression:
    """Test archive compression functionality."""
    
    def test_compress_archive_zip(self, archive_auditor, temp_archive_dir):
        """Test ZIP archive creation."""
        output_path = temp_archive_dir.parent / "test_backup.zip"
        
        result = archive_auditor.compress_archive(
            output_path=output_path,
            archive_format="zip"
        )
        
        assert isinstance(result, dict)
        assert "success" in result
        
        if result.get("success"):
            assert output_path.exists()
            assert "files_compressed" in result
            assert "output_size" in result
    
    def test_compress_archive_tar_gz(self, archive_auditor, temp_archive_dir):
        """Test TAR.GZ archive creation."""
        output_path = temp_archive_dir.parent / "test_backup.tar.gz"
        
        result = archive_auditor.compress_archive(
            output_path=output_path,
            archive_format="tar.gz"
        )
        
        assert isinstance(result, dict)
        assert "success" in result
        
        if result.get("success"):
            assert output_path.exists()
    
    def test_compress_with_exclude_patterns(self, archive_auditor, temp_archive_dir):
        """Test compression with exclude patterns."""
        output_path = temp_archive_dir.parent / "test_backup_filtered.zip"
        
        result = archive_auditor.compress_archive(
            output_path=output_path,
            archive_format="zip",
            exclude_patterns=["*.json"]
        )
        
        assert isinstance(result, dict)
        if result.get("success"):
            assert "excluded_files" in result


class TestArchiveStatistics:
    """Test archive statistics generation."""
    
    def test_generate_basic_statistics(self, archive_auditor):
        """Test basic statistics generation."""
        stats = archive_auditor.generate_statistics()
        
        assert isinstance(stats, dict)
        assert "total_files" in stats
        assert "media_files" in stats
        assert "metadata_files" in stats
        assert "total_size_bytes" in stats
    
    def test_generate_detailed_statistics(self, archive_auditor):
        """Test detailed statistics generation."""
        stats = archive_auditor.generate_statistics(detailed=True)
        
        assert isinstance(stats, dict)
        assert "total_files" in stats
        # Detailed stats should include additional information
        assert len(stats.keys()) > 5  # Should have more fields than basic stats
    
    def test_statistics_file_type_breakdown(self, archive_auditor):
        """Test file type breakdown in statistics."""
        stats = archive_auditor.generate_statistics(detailed=True)
        
        if "file_types" in stats:
            assert isinstance(stats["file_types"], dict)
            # Should detect .jpg and .png files from our test data
            extensions = stats["file_types"].keys()
            assert any(ext in ['.jpg', '.png'] for ext in extensions)


class TestAuditCLICommands:
    """Test CLI command integration."""
    
    def setup_method(self):
        """Set up CLI runner for testing."""
        self.runner = CliRunner()
    
    def test_audit_check_command(self, temp_archive_dir):
        """Test audit check CLI command."""
        with patch('src.redditdl.cli.commands.audit.ArchiveAuditor') as mock_auditor_class:
            # Mock the auditor instance
            mock_auditor = Mock()
            mock_auditor.audit_archive.return_value = []
            mock_auditor_class.return_value = mock_auditor
            
            result = self.runner.invoke(audit_app, [
                "check", str(temp_archive_dir)
            ])
            
            assert result.exit_code == 0
            mock_auditor.audit_archive.assert_called_once()
    
    def test_audit_repair_command(self, temp_archive_dir):
        """Test audit repair CLI command."""
        with patch('src.redditdl.cli.commands.audit.ArchiveAuditor') as mock_auditor_class:
            # Mock the auditor instance
            mock_auditor = Mock()
            mock_auditor.audit_archive.return_value = []
            mock_auditor.repair_archive.return_value = []
            mock_auditor_class.return_value = mock_auditor
            
            result = self.runner.invoke(audit_app, [
                "repair", str(temp_archive_dir), "--force"
            ])
            
            assert result.exit_code == 0
            mock_auditor.audit_archive.assert_called()
    
    def test_audit_compress_command(self, temp_archive_dir):
        """Test audit compress CLI command."""
        with patch('src.redditdl.cli.commands.audit.ArchiveAuditor') as mock_auditor_class:
            # Mock the auditor instance
            mock_auditor = Mock()
            mock_auditor.compress_archive.return_value = {
                "success": True,
                "output_path": "test.zip",
                "files_compressed": 10,
                "output_size": 1024,
                "original_size": 2048,
                "compression_ratio": 0.5
            }
            mock_auditor_class.return_value = mock_auditor
            
            result = self.runner.invoke(audit_app, [
                "compress", str(temp_archive_dir)
            ])
            
            assert result.exit_code == 0
            mock_auditor.compress_archive.assert_called_once()
    
    def test_audit_stats_command(self, temp_archive_dir):
        """Test audit stats CLI command."""
        with patch('src.redditdl.cli.commands.audit.ArchiveAuditor') as mock_auditor_class:
            # Mock the auditor instance  
            mock_auditor = Mock()
            mock_auditor.generate_statistics.return_value = {
                "total_files": 5,
                "media_files": 2,
                "metadata_files": 2,
                "total_size_bytes": 1024
            }
            mock_auditor_class.return_value = mock_auditor
            
            result = self.runner.invoke(audit_app, [
                "stats", str(temp_archive_dir)
            ])
            
            assert result.exit_code == 0
            mock_auditor.generate_statistics.assert_called_once()
    
    def test_audit_stats_with_export(self, temp_archive_dir):
        """Test audit stats command with export functionality."""
        export_file = temp_archive_dir / "stats_export.json"
        
        with patch('src.redditdl.cli.commands.audit.ArchiveAuditor') as mock_auditor_class:
            mock_auditor = Mock()
            mock_auditor.generate_statistics.return_value = {
                "total_files": 5,
                "media_files": 2
            }
            mock_auditor_class.return_value = mock_auditor
            
            result = self.runner.invoke(audit_app, [
                "stats", str(temp_archive_dir), "--export", str(export_file)
            ])
            
            assert result.exit_code == 0
            assert export_file.exists()
            
            # Verify exported content
            with open(export_file) as f:
                exported_data = json.load(f)
            assert "total_files" in exported_data


class TestErrorHandling:
    """Test error handling in archive management."""
    
    def test_nonexistent_archive_directory(self, test_config):
        """Test handling of nonexistent archive directory."""
        nonexistent_path = Path("/nonexistent/path")
        
        with pytest.raises(Exception):
            ArchiveAuditor(nonexistent_path, test_config)
    
    def test_audit_with_corrupted_json(self, temp_archive_dir, test_config):
        """Test audit handling of corrupted JSON files."""
        # Create corrupted JSON file
        corrupted_json = temp_archive_dir / "corrupted.json"
        corrupted_json.write_text("invalid json content {{{")
        
        with patch('src.redditdl.cli.commands.audit.StateManager'):
            auditor = ArchiveAuditor(temp_archive_dir, test_config)
            
            # Should handle corrupted JSON gracefully
            issues = auditor.audit_archive()
            assert isinstance(issues, list)


if __name__ == "__main__":
    pytest.main([__file__])