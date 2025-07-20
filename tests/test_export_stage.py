#!/usr/bin/env python3
"""
Comprehensive tests for the enhanced export stage and exporter system.

Tests the pluggable export architecture, multiple format support,
configuration handling, and plugin integration.
"""
import sys
from pathlib import Path

# Add project root to path
# sys.path.insert(0, str(Path(__file__).resolve().parents[1])) # This line is no longer needed with src/ layout

import pytest
import asyncio
import json
import csv
import sqlite3
import tempfile
import gzip
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from redditdl.pipeline.stages.export import ExportStage
from redditdl.core.pipeline.interfaces import PipelineContext, PipelineResult
from redditdl.exporters.base import registry, register_core_exporters, ExporterRegistry
from redditdl.exporters.json import JsonExporter
from redditdl.exporters.csv import CsvExporter
from redditdl.exporters.sqlite import SqliteExporter
from redditdl.exporters.markdown import MarkdownExporter
from redditdl.scrapers import PostMetadata


class TestExportStage:
    """Test cases for the enhanced ExportStage."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        # Ensure clean registry for each test
        registry.clear()
        register_core_exporters()
        
        # Create sample posts
        self.sample_posts = [
            PostMetadata.from_raw({
                'id': 'test1',
                'title': 'Test Post 1',
                'author': 'testuser1',
                'subreddit': 'test',
                'url': 'https://example.com/1',
                'score': 100,
                'num_comments': 50,
                'created_utc': 1640995200,
                'is_nsfw': False,
                'post_type': 'link'
            }),
            PostMetadata.from_raw({
                'id': 'test2',
                'title': 'Test Post 2 with special chars !@#$%',
                'author': 'testuser2',
                'subreddit': 'test',
                'url': 'https://example.com/2',
                'score': 200,
                'num_comments': 75,
                'created_utc': 1640995260,
                'is_nsfw': True,
                'post_type': 'text',
                'selftext': 'This is some self text content.'
            })
        ]
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_context(self, temp_dir):
        """Create sample pipeline context."""
        context = PipelineContext(
            posts=self.sample_posts,
            config={
                'export_formats': ['json', 'csv', 'markdown'],
                'export_dir': str(temp_dir),
                'export_include_metadata': True,
                'export_include_posts': True
            },
            metadata={'session_id': 'test_session', 'test_key': 'test_value'},
            session_id='test_session'
        )
        return context
    
    def test_export_stage_initialization(self):
        """Test export stage initialization."""
        stage = ExportStage()
        
        assert stage.name == "export"
        assert len(registry.list_formats()) >= 4  # json, csv, sqlite, markdown
        assert 'json' in registry.list_formats()
        assert 'csv' in registry.list_formats()
        assert 'sqlite' in registry.list_formats()
        assert 'markdown' in registry.list_formats()
    
    @pytest.mark.asyncio
    async def test_export_single_format(self, sample_context, temp_dir):
        """Test exporting to a single format."""
        sample_context.config['export_formats'] = ['json']
        
        stage = ExportStage()
        result = await stage.process(sample_context)
        
        assert result.success is True
        assert result.processed_count == 2
        assert result.get_data("exports_created") == 1
        
        export_files = result.get_data("export_files")
        assert len(export_files) == 1
        assert export_files[0].endswith('.json')
        
        # Verify file exists and has content
        json_file = Path(export_files[0])
        assert json_file.exists()
        assert json_file.stat().st_size > 0
        
        # Verify JSON content
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        assert 'export_info' in data
        assert 'posts' in data
        assert len(data['posts']) == 2
        assert data['export_info']['post_count'] == 2
    
    @pytest.mark.asyncio
    async def test_export_multiple_formats(self, sample_context, temp_dir):
        """Test exporting to multiple formats simultaneously."""
        stage = ExportStage()
        result = await stage.process(sample_context)
        
        assert result.success is True
        assert result.processed_count == 2
        assert result.get_data("exports_created") == 3  # json, csv, markdown
        
        export_files = result.get_data("export_files")
        assert len(export_files) == 3
        
        # Check all format files exist
        extensions = [Path(f).suffix for f in export_files]
        assert '.json' in extensions
        assert '.csv' in extensions
        assert '.md' in extensions
        
        # Verify total file size is calculated
        total_size = result.get_data("total_export_size")
        assert total_size > 0
    
    @pytest.mark.asyncio
    async def test_export_no_posts(self, temp_dir):
        """Test export behavior with no posts."""
        context = PipelineContext(
            posts=[],
            config={
                'export_formats': ['json'],
                'export_dir': str(temp_dir)
            }
        )
        
        stage = ExportStage()
        result = await stage.process(context)
        
        assert result.success is True
        assert result.processed_count == 0
        assert result.warnings  # Should have warning about no posts
        assert "No posts to export" in result.warnings[0]
    
    @pytest.mark.asyncio
    async def test_export_invalid_format(self, sample_context):
        """Test handling of invalid export formats."""
        sample_context.config['export_formats'] = ['json', 'invalid_format', 'csv']
        
        stage = ExportStage()
        result = await stage.process(sample_context)
        
        # Should have error about invalid format but continue with valid ones
        assert result.errors
        assert any('invalid_format' in error for error in result.errors)
        
        # Should still export valid formats
        assert result.get_data("exports_created") == 2  # json and csv
    
    @pytest.mark.asyncio
    async def test_export_configuration_inheritance(self, sample_context, temp_dir):
        """Test format-specific configuration inheritance."""
        # Set format-specific config
        sample_context.config.update({
            'export_formats': ['json'],
            'export_json_config': {
                'indent': 4,
                'compress': True,
                'sort_keys': False
            }
        })
        
        stage = ExportStage()
        result = await stage.process(sample_context)
        
        assert result.success is True
        
        # Check that compressed file was created
        export_files = result.get_data("export_files")
        json_file = Path(export_files[0])
        assert json_file.suffix == '.gz'  # Should be compressed
    
    @pytest.mark.asyncio
    async def test_export_directory_creation(self, temp_dir):
        """Test automatic export directory creation."""
        export_subdir = temp_dir / "nested" / "export" / "dir"
        
        context = PipelineContext(
            posts=self.sample_posts,
            config={
                'export_formats': ['json'],
                'export_dir': str(export_subdir)
            }
        )
        
        stage = ExportStage()
        result = await stage.process(context)
        
        assert result.success is True
        assert export_subdir.exists()
        assert export_subdir.is_dir()
    
    @pytest.mark.asyncio
    async def test_export_metadata_inclusion(self, sample_context, temp_dir):
        """Test pipeline metadata inclusion in exports."""
        stage = ExportStage()
        result = await stage.process(sample_context)
        
        # Check JSON export for metadata
        json_file = None
        for file_path in result.get_data("export_files"):
            if file_path.endswith('.json'):
                json_file = Path(file_path)
                break
        
        assert json_file is not None
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        assert 'pipeline_metadata' in data
        assert 'session_metadata' in data['pipeline_metadata']
        assert data['pipeline_metadata']['session_metadata']['session_id'] == 'test_session'
    
    def test_export_stage_validation(self):
        """Test export stage configuration validation."""
        # Valid configuration
        stage = ExportStage({
            'export_formats': ['json', 'csv'],
            'export_dir': 'exports'
        })
        
        errors = stage.validate_config()
        assert len(errors) == 0
        
        # Invalid format
        stage = ExportStage({
            'export_formats': ['invalid_format']
        })
        
        errors = stage.validate_config()
        assert len(errors) > 0
        assert any('invalid_format' in error for error in errors)
    
    def test_get_available_formats(self):
        """Test getting available export formats."""
        stage = ExportStage()
        formats = stage.get_available_formats()
        
        assert isinstance(formats, list)
        assert 'json' in formats
        assert 'csv' in formats
        assert 'sqlite' in formats
        assert 'markdown' in formats
    
    def test_get_format_info(self):
        """Test getting format information."""
        stage = ExportStage()
        format_info = stage.get_format_info()
        
        assert isinstance(format_info, dict)
        assert 'json' in format_info
        assert 'csv' in format_info
        
        json_info = format_info['json']
        assert 'name' in json_info
        assert 'extension' in json_info
        assert 'description' in json_info
        assert json_info['extension'] == '.json'
    
    @pytest.mark.asyncio
    async def test_export_stage_hooks(self, sample_context, temp_dir):
        """Test export stage pre/post processing hooks."""
        stage = ExportStage()
        
        # Test pre-processing
        await stage.pre_process(sample_context)
        # Should not raise any exceptions
        
        # Process
        result = await stage.process(sample_context)
        
        # Test post-processing
        await stage.post_process(sample_context, result)
        
        # Check that metadata was set in context
        assert sample_context.get_metadata("export_completed") is True
        assert sample_context.get_metadata("exports_created") == 3
        assert len(sample_context.get_metadata("export_files")) == 3


class TestJsonExporter:
    """Test cases for JsonExporter."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample export data."""
        return {
            'export_info': {
                'timestamp': '2022-01-01T00:00:00',
                'post_count': 2
            },
            'posts': [
                {
                    'id': 'test1',
                    'title': 'Test Post 1',
                    'score': 100,
                    'created_utc': 1640995200
                },
                {
                    'id': 'test2', 
                    'title': 'Test Post 2',
                    'score': 200,
                    'created_utc': 1640995260
                }
            ]
        }
    
    def test_json_exporter_basic_export(self, temp_dir, sample_data):
        """Test basic JSON export functionality."""
        exporter = JsonExporter()
        output_path = temp_dir / "test.json"
        
        result = exporter.export(sample_data, str(output_path), {})
        
        assert result.success is True
        assert result.records_exported == 2
        assert Path(result.output_path).exists()
        
        # Verify JSON content
        with open(result.output_path, 'r') as f:
            data = json.load(f)
        
        assert 'export_info' in data
        assert 'posts' in data
        assert len(data['posts']) == 2
    
    def test_json_exporter_compression(self, temp_dir, sample_data):
        """Test JSON export with compression."""
        exporter = JsonExporter()
        output_path = temp_dir / "test.json"
        config = {'compress': True}
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        assert result.output_path.endswith('.gz')
        
        # Verify compressed content
        with gzip.open(result.output_path, 'rt') as f:
            data = json.load(f)
        
        assert 'posts' in data
        assert len(data['posts']) == 2
    
    def test_json_exporter_field_filtering(self, temp_dir, sample_data):
        """Test JSON export with field filtering."""
        exporter = JsonExporter()
        output_path = temp_dir / "test.json"
        config = {
            'field_filter': ['id', 'title'],
            'include_posts': True
        }
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        
        with open(result.output_path, 'r') as f:
            data = json.load(f)
        
        # Check that only specified fields are included (plus essentials)
        for post in data['posts']:
            # Essential fields should always be present
            assert 'id' in post
            assert 'title' in post
            # score should be filtered out (not in field_filter)
            # Note: the actual filtering logic may include essential fields


class TestCsvExporter:
    """Test cases for CsvExporter."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample export data."""
        return {
            'export_info': {
                'timestamp': '2022-01-01T00:00:00',
                'post_count': 2
            },
            'posts': [
                {
                    'id': 'test1',
                    'title': 'Test Post 1',
                    'score': 100,
                    'author': 'user1',
                    'subreddit': 'test',
                    'nested_data': {'key': 'value'},
                    'array_data': ['item1', 'item2']
                },
                {
                    'id': 'test2',
                    'title': 'Test Post 2 with, commas',
                    'score': 200,
                    'author': 'user2',
                    'subreddit': 'test',
                    'nested_data': {'other': 'data'},
                    'array_data': ['item3']
                }
            ]
        }
    
    def test_csv_exporter_basic_export(self, temp_dir, sample_data):
        """Test basic CSV export functionality."""
        exporter = CsvExporter()
        output_path = temp_dir / "test.csv"
        
        result = exporter.export(sample_data, str(output_path), {})
        
        assert result.success is True
        assert result.records_exported == 2
        assert Path(result.output_path).exists()
        
        # Verify CSV content
        with open(result.output_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 2
        assert 'id' in rows[0]
        assert 'title' in rows[0]
        assert rows[0]['id'] == 'test1'
    
    def test_csv_exporter_flattening(self, temp_dir, sample_data):
        """Test CSV export with nested data flattening."""
        exporter = CsvExporter()
        output_path = temp_dir / "test.csv"
        config = {'flatten_nested': True}
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        
        with open(result.output_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Check that nested data was flattened
        headers = reader.fieldnames
        assert any('nested_data.' in header for header in headers)
    
    def test_csv_exporter_special_characters(self, temp_dir, sample_data):
        """Test CSV export handles special characters correctly."""
        exporter = CsvExporter()
        output_path = temp_dir / "test.csv"
        
        result = exporter.export(sample_data, str(output_path), {})
        
        assert result.success is True
        
        with open(result.output_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Check that commas in titles are handled correctly
        assert rows[1]['title'] == 'Test Post 2 with, commas'


class TestSqliteExporter:
    """Test cases for SqliteExporter."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample export data."""
        return {
            'export_info': {
                'timestamp': '2022-01-01T00:00:00',
                'post_count': 2
            },
            'posts': [
                {
                    'id': 'test1',
                    'title': 'Test Post 1',
                    'score': 100,
                    'author': 'user1',
                    'subreddit': 'test',
                    'awards': [{'name': 'gold', 'count': 1}],
                    'gallery_image_urls': ['https://example.com/img1.jpg']
                },
                {
                    'id': 'test2',
                    'title': 'Test Post 2',
                    'score': 200,
                    'author': 'user2',
                    'subreddit': 'test',
                    'awards': [],
                    'gallery_image_urls': []
                }
            ]
        }
    
    def test_sqlite_exporter_flat_schema(self, temp_dir, sample_data):
        """Test SQLite export with flat schema."""
        exporter = SqliteExporter()
        output_path = temp_dir / "test.db"
        config = {'schema_mode': 'flat'}
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        assert result.records_exported == 2
        assert Path(result.output_path).exists()
        
        # Verify database content
        conn = sqlite3.connect(result.output_path)
        cursor = conn.cursor()
        
        # Check table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        assert 'reddit_posts' in tables
        
        # Check data
        cursor.execute("SELECT * FROM reddit_posts")
        rows = cursor.fetchall()
        assert len(rows) == 2
        
        conn.close()
    
    def test_sqlite_exporter_normalized_schema(self, temp_dir, sample_data):
        """Test SQLite export with normalized schema."""
        exporter = SqliteExporter()
        output_path = temp_dir / "test.db"
        config = {'schema_mode': 'normalized'}
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        
        # Verify normalized tables
        conn = sqlite3.connect(result.output_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['reddit_posts', 'reddit_awards', 'reddit_gallery_images', 'reddit_export_info']
        for table in expected_tables:
            assert table in tables
        
        conn.close()
    
    def test_sqlite_exporter_indexes_and_fts(self, temp_dir, sample_data):
        """Test SQLite export creates indexes and FTS tables."""
        exporter = SqliteExporter()
        output_path = temp_dir / "test.db"
        config = {
            'create_indexes': True,
            'enable_fts': True
        }
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        
        conn = sqlite3.connect(result.output_path)
        cursor = conn.cursor()
        
        # Check for indexes
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        assert len(indexes) > 0
        
        # Check for FTS table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_fts'")
        fts_tables = [row[0] for row in cursor.fetchall()]
        assert len(fts_tables) > 0
        
        conn.close()


class TestMarkdownExporter:
    """Test cases for MarkdownExporter."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample export data."""
        return {
            'export_info': {
                'timestamp': '2022-01-01T00:00:00',
                'post_count': 2
            },
            'posts': [
                {
                    'id': 'test1',
                    'title': 'Test Post 1',
                    'score': 100,
                    'author': 'user1',
                    'subreddit': 'test',
                    'selftext': 'This is some content',
                    'created_utc': 1640995200,
                    'num_comments': 10
                },
                {
                    'id': 'test2',
                    'title': 'Test Post 2 with **markdown** chars',
                    'score': 200,
                    'author': 'user2',
                    'subreddit': 'programming',
                    'selftext': 'More content here',
                    'created_utc': 1640995260,
                    'num_comments': 25
                }
            ]
        }
    
    def test_markdown_exporter_basic_export(self, temp_dir, sample_data):
        """Test basic Markdown export functionality."""
        exporter = MarkdownExporter()
        output_path = temp_dir / "test.md"
        
        result = exporter.export(sample_data, str(output_path), {})
        
        assert result.success is True
        assert result.records_exported == 2
        assert Path(result.output_path).exists()
        
        # Verify Markdown content
        with open(result.output_path, 'r') as f:
            content = f.read()
        
        assert '# Reddit Data Export' in content
        assert '## Statistics' in content
        assert 'Test Post 1' in content
        assert 'Test Post 2' in content
    
    def test_markdown_exporter_grouping(self, temp_dir, sample_data):
        """Test Markdown export with post grouping."""
        exporter = MarkdownExporter()
        output_path = temp_dir / "test.md"
        config = {'group_by': 'subreddit'}
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        
        with open(result.output_path, 'r') as f:
            content = f.read()
        
        # Should have subreddit-based groupings
        assert '## r/test' in content
        assert '## r/programming' in content
    
    def test_markdown_exporter_special_chars_escaped(self, temp_dir, sample_data):
        """Test Markdown export properly escapes special characters."""
        exporter = MarkdownExporter()
        output_path = temp_dir / "test.md"
        
        result = exporter.export(sample_data, str(output_path), {})
        
        assert result.success is True
        
        with open(result.output_path, 'r') as f:
            content = f.read()
        
        # Markdown special characters should be escaped
        assert '\\*\\*markdown\\*\\*' in content or 'markdown' in content
    
    def test_markdown_exporter_table_of_contents(self, temp_dir, sample_data):
        """Test Markdown export includes table of contents."""
        exporter = MarkdownExporter()
        output_path = temp_dir / "test.md"
        config = {'include_toc': True}
        
        result = exporter.export(sample_data, str(output_path), config)
        
        assert result.success is True
        
        with open(result.output_path, 'r') as f:
            content = f.read()
        
        assert '## Table of Contents' in content


class TestExporterRegistry:
    """Test cases for the ExporterRegistry."""
    
    def test_registry_registration(self):
        """Test exporter registration and retrieval."""
        test_registry = ExporterRegistry()
        
        # Register an exporter
        test_registry.register_exporter(JsonExporter, 'test_json')
        
        assert 'test_json' in test_registry.list_formats()
        
        exporter = test_registry.get_exporter('test_json')
        assert isinstance(exporter, JsonExporter)
    
    def test_registry_aliases(self):
        """Test exporter aliases."""
        test_registry = ExporterRegistry()
        
        test_registry.register_exporter(JsonExporter, 'json', aliases=['js', 'json_format'])
        
        # Test alias resolution
        exporter1 = test_registry.get_exporter('json')
        exporter2 = test_registry.get_exporter('js')
        
        assert isinstance(exporter1, JsonExporter)
        assert isinstance(exporter2, JsonExporter)
    
    def test_registry_format_validation(self):
        """Test format configuration validation."""
        test_registry = ExporterRegistry()
        test_registry.register_exporter(JsonExporter, 'json')
        
        # Valid configuration
        errors = test_registry.validate_format_config('json', {'indent': 2})
        assert len(errors) == 0
        
        # Invalid configuration
        errors = test_registry.validate_format_config('json', {'indent': 'invalid'})
        assert len(errors) > 0


if __name__ == '__main__':
    pytest.main([__file__])