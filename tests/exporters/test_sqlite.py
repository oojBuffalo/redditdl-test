"""
Tests for SQLite Exporter

Tests the SQLite export functionality including database creation,
schema generation, and data insertion.
"""

import pytest
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from redditdl.exporters.sqlite import SqliteExporter
from redditdl.scrapers import PostMetadata


class TestSqliteExporter:
    """Test suite for SqliteExporter."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.exporter = SqliteExporter()
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_exporter_initialization(self):
        """Test exporter initialization."""
        assert isinstance(self.exporter, SqliteExporter)
        assert hasattr(self.exporter, 'export')
        assert hasattr(self.exporter, 'get_supported_formats')
        assert hasattr(self.exporter, 'validate_config')
    
    def test_get_supported_formats(self):
        """Test supported format detection."""
        formats = self.exporter.get_supported_formats()
        
        assert isinstance(formats, list)
        assert 'sqlite' in formats
        assert 'db' in formats
    
    def test_export_single_post(self):
        """Test exporting a single post to SQLite."""
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
        db_file = self.temp_dir / 'test_export.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        assert db_file.exists()
        
        # Verify database content
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM posts")
        rows = cursor.fetchall()
        
        assert len(rows) == 1
        row = rows[0]
        
        assert row['id'] == 'abc123'
        assert row['title'] == 'Test Post'
        assert row['url'] == 'https://example.com/image.jpg'
        assert row['author'] == 'testuser'
        assert row['subreddit'] == 'testsubreddit'
        assert row['score'] == 42
        assert row['num_comments'] == 10
        
        conn.close()
    
    def test_export_multiple_posts(self):
        """Test exporting multiple posts to SQLite."""
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
        
        db_file = self.temp_dir / 'multiple_posts.db'
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        assert db_file.exists()
        
        # Verify database content
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM posts ORDER BY id")
        rows = cursor.fetchall()
        
        assert len(rows) == 5
        
        for i, row in enumerate(rows):
            assert row['id'] == f'post_{i}'
            assert row['title'] == f'Test Post {i}'
            assert row['score'] == i * 10
        
        conn.close()
    
    def test_schema_creation(self):
        """Test database schema creation."""
        posts = []
        db_file = self.temp_dir / 'schema_test.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export empty list to test schema creation
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        assert db_file.exists()
        
        # Verify schema
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Check table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
        tables = cursor.fetchall()
        assert len(tables) == 1
        
        # Check table structure
        cursor.execute("PRAGMA table_info(posts)")
        columns = cursor.fetchall()
        
        column_names = [col[1] for col in columns]
        
        # Verify essential columns exist
        assert 'id' in column_names
        assert 'title' in column_names
        assert 'url' in column_names
        assert 'author' in column_names
        assert 'subreddit' in column_names
        assert 'score' in column_names
        assert 'created_utc' in column_names
        assert 'is_nsfw' in column_names
        
        conn.close()
    
    def test_custom_table_name(self):
        """Test exporting with custom table name."""
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        posts = [post]
        db_file = self.temp_dir / 'custom_table.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'reddit_posts',  # Custom table name
            'create_indexes': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify custom table name
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reddit_posts'")
        tables = cursor.fetchall()
        assert len(tables) == 1
        
        cursor.execute("SELECT COUNT(*) FROM reddit_posts")
        count = cursor.fetchone()[0]
        assert count == 1
        
        conn.close()
    
    def test_index_creation(self):
        """Test database index creation."""
        posts = []
        db_file = self.temp_dir / 'index_test.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify indexes created
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = cursor.fetchall()
        
        index_names = [idx[0] for idx in indexes if not idx[0].startswith('sqlite_')]
        
        # Should have indexes on common query fields
        assert any('id' in name.lower() for name in index_names)
        assert any('author' in name.lower() for name in index_names)
        assert any('subreddit' in name.lower() for name in index_names)
        
        conn.close()
    
    def test_existing_database_append(self):
        """Test appending to existing database."""
        # Create initial post
        post1 = PostMetadata(
            id='post1',
            title='First Post',
            url='https://example.com/image1.jpg',
            domain='example.com'
        )
        
        db_file = self.temp_dir / 'append_test.db'
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True,
            'append_mode': True
        }
        
        # First export
        result1 = self.exporter.export([post1], config)
        assert result1['success'] is True
        
        # Create second post
        post2 = PostMetadata(
            id='post2',
            title='Second Post',
            url='https://example.com/image2.jpg',
            domain='example.com'
        )
        
        # Second export (append)
        result2 = self.exporter.export([post2], config)
        assert result2['success'] is True
        
        # Verify both posts exist
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM posts")
        count = cursor.fetchone()[0]
        assert count == 2
        
        cursor.execute("SELECT id FROM posts ORDER BY id")
        ids = [row[0] for row in cursor.fetchall()]
        assert 'post1' in ids
        assert 'post2' in ids
        
        conn.close()
    
    def test_existing_database_replace(self):
        """Test replacing existing database content."""
        # Create initial post
        post1 = PostMetadata(
            id='post1',
            title='First Post',
            url='https://example.com/image1.jpg',
            domain='example.com'
        )
        
        db_file = self.temp_dir / 'replace_test.db'
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True,
            'append_mode': False  # Replace mode
        }
        
        # First export
        result1 = self.exporter.export([post1], config)
        assert result1['success'] is True
        
        # Create second post
        post2 = PostMetadata(
            id='post2',
            title='Second Post',
            url='https://example.com/image2.jpg',
            domain='example.com'
        )
        
        # Second export (replace)
        result2 = self.exporter.export([post2], config)
        assert result2['success'] is True
        
        # Verify only second post exists
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM posts")
        count = cursor.fetchone()[0]
        assert count == 1
        
        cursor.execute("SELECT id FROM posts")
        post_id = cursor.fetchone()[0]
        assert post_id == 'post2'
        
        conn.close()
    
    def test_unicode_content_handling(self):
        """Test handling of Unicode content in SQLite."""
        post = PostMetadata(
            id='unicode123',
            title='Test with émojis 🎉 and ñoño characters',
            url='https://example.com/image.jpg',
            domain='example.com',
            author='usér_with_àccents',
            subreddit='tëst_subreddit'
        )
        
        posts = [post]
        db_file = self.temp_dir / 'unicode_test.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify Unicode content preserved
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM posts")
        row = cursor.fetchone()
        
        assert 'émojis 🎉' in row['title']
        assert 'ñoño' in row['title']
        assert 'usér_with_àccents' in row['author']
        assert 'tëst_subreddit' in row['subreddit']
        
        conn.close()
    
    def test_null_value_handling(self):
        """Test handling of None/null values."""
        post = PostMetadata(
            id='null_test',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            author=None,  # None value
            subreddit='',  # Empty string
            score=None,
            selftext=None
        )
        
        posts = [post]
        db_file = self.temp_dir / 'null_test.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify null handling
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM posts")
        row = cursor.fetchone()
        
        assert row['author'] is None  # None should be stored as NULL
        assert row['subreddit'] == ''  # Empty string preserved
        assert row['score'] is None
        assert row['selftext'] is None
        
        conn.close()
    
    def test_complex_data_types_serialization(self):
        """Test serialization of complex data types (lists, dicts)."""
        post = PostMetadata(
            id='complex_data',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com',
            gallery_image_urls=['url1', 'url2', 'url3'],  # List
            poll_data={'question': 'Test?', 'options': ['A', 'B']},  # Dict
            awards=[{'name': 'Gold', 'count': 1}],  # List of dicts
            media={'type': 'image', 'width': 1920, 'height': 1080}  # Dict
        )
        
        posts = [post]
        db_file = self.temp_dir / 'complex_data.db'
        
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True,
            'serialize_complex_types': True
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify complex data serialization
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM posts")
        row = cursor.fetchone()
        
        # Complex types should be serialized as JSON strings
        import json
        
        gallery_urls = json.loads(row['gallery_image_urls'])
        assert gallery_urls == ['url1', 'url2', 'url3']
        
        poll_data = json.loads(row['poll_data'])
        assert poll_data['question'] == 'Test?'
        assert poll_data['options'] == ['A', 'B']
        
        conn.close()
    
    def test_transaction_handling(self):
        """Test transaction handling for bulk inserts."""
        # Create many posts
        posts = []
        for i in range(100):
            post = PostMetadata(
                id=f'bulk_{i}',
                title=f'Bulk Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                domain='example.com'
            )
            posts.append(post)
        
        db_file = self.temp_dir / 'transaction_test.db'
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True,
            'batch_size': 50  # Process in batches
        }
        
        # Export
        result = self.exporter.export(posts, config)
        
        assert result['success'] is True
        
        # Verify all posts inserted
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM posts")
        count = cursor.fetchone()[0]
        assert count == 100
        
        conn.close()
    
    def test_export_with_database_error(self):
        """Test export handling of database errors."""
        post = PostMetadata(
            id='error_test',
            title='Test Post',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        posts = [post]
        
        # Try to create database in read-only directory
        config = {
            'database_file': '/root/readonly.db',  # Should fail on most systems
            'table_name': 'posts',
            'create_indexes': True
        }
        
        # Export should fail gracefully
        result = self.exporter.export(posts, config)
        
        assert result['success'] is False
        assert 'error' in result
    
    def test_validate_config_valid(self):
        """Test configuration validation with valid config."""
        config = {
            'database_file': str(self.temp_dir / 'test.db'),
            'table_name': 'posts',
            'create_indexes': True,
            'append_mode': False
        }
        
        validation_result = self.exporter.validate_config(config)
        
        assert validation_result['valid'] is True
        assert 'errors' not in validation_result or len(validation_result['errors']) == 0
    
    def test_validate_config_missing_database_file(self):
        """Test configuration validation with missing database file."""
        config = {
            'table_name': 'posts',
            'create_indexes': True
        }
        
        validation_result = self.exporter.validate_config(config)
        
        assert validation_result['valid'] is False
        assert 'errors' in validation_result
        assert any('database_file' in error for error in validation_result['errors'])
    
    def test_validate_config_invalid_table_name(self):
        """Test configuration validation with invalid table name."""
        config = {
            'database_file': str(self.temp_dir / 'test.db'),
            'table_name': '123invalid',  # Invalid SQL identifier
            'create_indexes': True
        }
        
        validation_result = self.exporter.validate_config(config)
        
        assert validation_result['valid'] is False
        assert 'errors' in validation_result
        assert any('table_name' in error for error in validation_result['errors'])
    
    def test_get_default_config(self):
        """Test getting default configuration."""
        default_config = self.exporter.get_default_config()
        
        assert isinstance(default_config, dict)
        assert 'table_name' in default_config
        assert 'create_indexes' in default_config
        assert 'append_mode' in default_config
        assert 'serialize_complex_types' in default_config
        
        # Verify sensible defaults
        assert default_config['table_name'] == 'posts'
        assert default_config['create_indexes'] is True
        assert default_config['append_mode'] is False
    
    def test_performance_with_large_dataset(self):
        """Test exporter performance with large dataset."""
        # Create a reasonably large dataset
        posts = []
        for i in range(1000):
            post = PostMetadata(
                id=f'perf_{i:04d}',
                title=f'Performance Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                author=f'user_{i % 50}',  # 50 different users
                subreddit=f'sub_{i % 10}',  # 10 different subreddits
                score=i * 5,
                created_utc=1640995200 + i * 60
            )
            posts.append(post)
        
        db_file = self.temp_dir / 'performance_test.db'
        config = {
            'database_file': str(db_file),
            'table_name': 'posts',
            'create_indexes': True,
            'batch_size': 100
        }
        
        # Export and measure
        import time
        start_time = time.time()
        
        result = self.exporter.export(posts, config)
        
        end_time = time.time()
        export_time = end_time - start_time
        
        assert result['success'] is True
        
        # Verify all data exported
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM posts")
        count = cursor.fetchone()[0]
        assert count == 1000
        
        # Performance should be reasonable (less than 10 seconds for 1000 records)
        assert export_time < 10.0
        
        conn.close()