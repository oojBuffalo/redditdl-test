"""
Tests for CSV Exporter

Tests the CSV export functionality including data formatting,
field selection, and file output handling.
"""

import csv
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from redditdl.exporters.csv import CsvExporter
from redditdl.scrapers import PostMetadata


class TestCsvExporter:
    """Test suite for CsvExporter."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.exporter = CsvExporter()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_exporter_initialization(self):
        """Test exporter initialization."""
        assert isinstance(self.exporter, CsvExporter)
        assert hasattr(self.exporter, 'export')
        assert hasattr(self.exporter, 'get_supported_formats')
        assert hasattr(self.exporter, 'validate_config')
    
    def test_get_supported_formats(self):
        """Test supported format detection."""
        formats = self.exporter.get_supported_formats()
        
        assert isinstance(formats, list)
        assert 'csv' in formats
    
    def test_export_single_post(self):
        """Test exporting a single post to CSV."""
        # Create test post
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='testuser',
            subreddit='testsubreddit',
            score=42,
            num_comments=10,
            created_utc=1640995200,
            is_nsfw=False,
            is_self=False
        )
        
        posts = [post]
        output_file = self.temp_dir / 'test_export.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ','
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 1
            row = rows[0]
            
            assert row['id'] == 'abc123'
            assert row['title'] == 'Test Post'
            assert row['url'] == 'https://example.com/image.jpg'
            assert row['author'] == 'testuser'
            assert row['subreddit'] == 'testsubreddit'
            assert row['score'] == '42'
            assert row['num_comments'] == '10'
    
    def test_export_multiple_posts(self):
        """Test exporting multiple posts to CSV."""
        # Create test posts
        posts = []
        for i in range(5):
            post = PostMetadata(
                id=f'post_{i}',
                title=f'Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                author=f'user_{i}',
                subreddit='testsubreddit',
                score=i * 10,
                num_comments=i * 2,
                created_utc=1640995200 + i * 3600,
                is_nsfw=(i % 2 == 0),
                is_self=False
            )
            posts.append(post)
        
        output_file = self.temp_dir / 'multiple_posts.csv'
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ','
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 5
            
            for i, row in enumerate(rows):
                assert row['id'] == f'post_{i}'
                assert row['title'] == f'Test Post {i}'
                assert row['score'] == str(i * 10)
    
    def test_export_with_custom_delimiter(self):
        """Test exporting with custom delimiter."""
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='testuser'
        )
        
        posts = [post]
        output_file = self.temp_dir / 'custom_delimiter.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ';'  # Semicolon delimiter
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify delimiter used
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert ';' in content
            assert content.count(';') > content.count(',')  # More semicolons than commas
    
    def test_export_without_headers(self):
        """Test exporting without headers."""
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='testuser'
        )
        
        posts = [post]
        output_file = self.temp_dir / 'no_headers.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': False,
            'delimiter': ','
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify no headers
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            # First line should be data, not headers
            assert 'abc123' in lines[0]
            assert 'id' not in lines[0]  # Header word shouldn't be there
    
    def test_export_with_field_selection(self):
        """Test exporting with specific field selection."""
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='testuser',
            subreddit='testsubreddit',
            score=42,
            num_comments=10,
            created_utc=1640995200
        )
        
        posts = [post]
        output_file = self.temp_dir / 'selected_fields.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ',',
            'selected_fields': ['id', 'title', 'author', 'score']  # Only specific fields
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify only selected fields
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 1
            row = rows[0]
            
            # Should have selected fields
            assert 'id' in row
            assert 'title' in row
            assert 'author' in row
            assert 'score' in row
            
            # Should not have unselected fields
            assert 'url' not in row
            assert 'domain' not in row
            assert 'subreddit' not in row
    
    def test_export_with_field_mapping(self):
        """Test exporting with custom field names."""
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            author='testuser',
            subreddit='testsubreddit'
        )
        
        posts = [post]
        output_file = self.temp_dir / 'mapped_fields.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ',',
            'field_mapping': {
                'id': 'Post ID',
                'title': 'Post Title',
                'author': 'Username',
                'subreddit': 'Community'
            }
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify custom field names
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            assert 'Post ID' in headers
            assert 'Post Title' in headers
            assert 'Username' in headers
            assert 'Community' in headers
            
            # Original field names should not be present
            assert 'id' not in headers
            assert 'title' not in headers
            assert 'author' not in headers
            assert 'subreddit' not in headers
    
    def test_export_with_unicode_content(self):
        """Test exporting posts with Unicode content."""
        post = PostMetadata(
            id='unicode123',
            title='Test with émojis 🎉 and ñoño characters',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='usér_with_àccents',
            subreddit='tëst_subreddit'
        )
        
        posts = [post]
        output_file = self.temp_dir / 'unicode_content.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ',',
            'encoding': 'utf-8'
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify Unicode content preserved
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 1
            row = rows[0]
            
            assert 'émojis 🎉' in row['title']
            assert 'ñoño' in row['title']
            assert 'usér_with_àccents' in row['author']
            assert 'tëst_subreddit' in row['subreddit']
    
    def test_export_with_special_characters_in_content(self):
        """Test exporting posts with CSV special characters."""
        post = PostMetadata(
            id='special123',
            title='Title with "quotes", commas, and\nnewlines',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='user,with,commas',
            selftext='Text with "quoted text" and,\nmore special chars'
        )
        
        posts = [post]
        output_file = self.temp_dir / 'special_chars.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ','
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify special characters handled correctly
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 1
            row = rows[0]
            
            # CSV should properly escape/quote these values
            assert '"quotes"' in row['title']
            assert 'commas' in row['title']
            assert 'newlines' in row['title']
            assert 'user,with,commas' in row['author']
    
    def test_export_with_null_values(self):
        """Test exporting posts with null/None values."""
        post = PostMetadata(
            id='null123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            author=None,  # None value
            subreddit='',  # Empty string
            score=None,
            selftext=None
        )
        
        posts = [post]
        output_file = self.temp_dir / 'null_values.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ',',
            'null_value': 'NULL'  # Custom null representation
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify null handling
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 1
            row = rows[0]
            
            assert row['author'] == 'NULL'  # None converted to custom null value
            assert row['subreddit'] == ''  # Empty string preserved
            assert row['score'] == 'NULL'
    
    def test_export_empty_post_list(self):
        """Test exporting empty post list."""
        posts = []
        output_file = self.temp_dir / 'empty_export.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ','
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        assert output_file.exists()
        
        # Should create file with headers only
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            if config['include_headers']:
                assert len(lines) == 1  # Only header line
                assert 'id' in lines[0]  # Should have header
            else:
                assert len(lines) == 0  # Completely empty
    
    def test_export_with_file_write_error(self):
        """Test export handling of file write errors."""
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        posts = [post]
        
        # Try to write to invalid path
        config = {
            'output_file': '/invalid/path/export.csv',
            'include_headers': True,
            'delimiter': ','
        }
        
        # Export should fail gracefully
        result = self.exporter.export(posts, config)
        
        assert result['success'] is False
        assert 'error' in result
        assert 'permission' in result['error'].lower() or 'not found' in result['error'].lower()
    
    def test_validate_config_valid(self):
        """Test configuration validation with valid config."""
        config = {
            'output_file': str(self.temp_dir / 'test.csv'),
            'include_headers': True,
            'delimiter': ',',
            'encoding': 'utf-8'
        }
        
        validation_result = self.exporter.validate_config(config)
        
        assert validation_result['valid'] is True
        assert 'errors' not in validation_result or len(validation_result['errors']) == 0
    
    def test_validate_config_missing_output_file(self):
        """Test configuration validation with missing output file."""
        config = {
            'include_headers': True,
            'delimiter': ','
        }
        
        validation_result = self.exporter.validate_config(config)
        
        assert validation_result['valid'] is False
        assert 'errors' in validation_result
        assert any('output_file' in error for error in validation_result['errors'])
    
    def test_validate_config_invalid_delimiter(self):
        """Test configuration validation with invalid delimiter."""
        config = {
            'output_file': str(self.temp_dir / 'test.csv'),
            'delimiter': ''  # Empty delimiter
        }
        
        validation_result = self.exporter.validate_config(config)
        
        assert validation_result['valid'] is False
        assert 'errors' in validation_result
        assert any('delimiter' in error for error in validation_result['errors'])
    
    def test_get_default_config(self):
        """Test getting default configuration."""
        default_config = self.exporter.get_default_config()
        
        assert isinstance(default_config, dict)
        assert 'include_headers' in default_config
        assert 'delimiter' in default_config
        assert 'encoding' in default_config
        
        # Verify sensible defaults
        assert default_config['include_headers'] is True
        assert default_config['delimiter'] == ','
        assert default_config['encoding'] == 'utf-8'
    
    def test_field_data_type_conversion(self):
        """Test proper data type conversion for CSV export."""
        post = PostMetadata(
            id='type_test',
            title='Type Test',
            url='https://example.com/image.jpg',
            domain='example.com',
            score=42,  # Integer
            created_utc=1640995200.5,  # Float
            is_nsfw=True,  # Boolean
            gallery_image_urls=['url1', 'url2'],  # List
            media={'type': 'image'}  # Dict
        )
        
        posts = [post]
        output_file = self.temp_dir / 'type_conversion.csv'
        
        config = {
            'output_file': str(output_file),
            'include_headers': True,
            'delimiter': ','
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify data type handling
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 1
            row = rows[0]
            
            # All values should be strings in CSV
            assert row['score'] == '42'
            assert row['created_utc'] == '1640995200.5'
            assert row['is_nsfw'] == 'True'
            # Complex types should be serialized appropriately
            assert 'url1' in row['gallery_image_urls']
            assert 'image' in row['media']