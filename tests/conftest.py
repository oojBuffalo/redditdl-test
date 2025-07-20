"""
Comprehensive Test Configuration and Fixtures

This module provides shared fixtures and configuration for the entire test suite,
including mocks for external services, sample data generation, and test environment setup.
"""

import asyncio
import json
import os
import pytest
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import Mock, MagicMock, patch

import praw
import prawcore
import requests

# Test data and utilities
from rich.console import Console

from redditdl.scrapers import PostMetadata
from redditdl.core.config.models import AppConfig
from redditdl.core.config.manager import ConfigManager
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.state.manager import StateManager
from redditdl.core.security.validation import InputValidator
from redditdl.core.plugins.manager import PluginManager


# Test Data Fixtures
@pytest.fixture
def sample_reddit_post_data():
    """Generate sample Reddit post data for various post types."""
    return {
        'standard_post': {
            'id': 'abc123',
            'title': 'Sample Post Title',
            'selftext': 'This is the post content',
            'created_utc': 1640995200,  # 2022-01-01 00:00:00 UTC
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/abc123/sample_post/',
            'url': 'https://example.com/image.jpg',
            'author': 'testuser',
            'score': 42,
            'num_comments': 10,
            'is_video': False,
            'is_nsfw': False,
            'is_self': False,
            'domain': 'example.com',
            'media_url': 'https://example.com/image.jpg',
            'post_type': 'link',
            'awards': [],
            'media': None,
            'crosspost_parent_id': None,
            'gallery_image_urls': [],
            'poll_data': None,
            'edited': False,
            'locked': False,
            'archived': False,
            'spoiler': False,
            'stickied': False
        },
        'gallery_post': {
            'id': 'def456',
            'title': 'Gallery Post Title',
            'selftext': '',
            'created_utc': 1640995260,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/def456/gallery_post/',
            'url': 'https://www.reddit.com/gallery/def456',
            'author': 'galleryuser',
            'score': 123,
            'num_comments': 25,
            'is_video': False,
            'is_nsfw': False,
            'is_self': False,
            'domain': 'reddit.com',
            'media_url': '',
            'post_type': 'gallery',
            'awards': [{'name': 'Helpful', 'count': 1}],
            'media': {
                'type': 'gallery',
                'items': [
                    {'url': 'https://i.redd.it/image1.jpg'},
                    {'url': 'https://i.redd.it/image2.png'}
                ]
            },
            'crosspost_parent_id': None,
            'gallery_image_urls': [
                'https://i.redd.it/image1.jpg',
                'https://i.redd.it/image2.png'
            ],
            'poll_data': None,
            'edited': False,
            'locked': False,
            'archived': False,
            'spoiler': False,
            'stickied': False
        },
        'video_post': {
            'id': 'ghi789',
            'title': 'Video Post Title',
            'selftext': '',
            'created_utc': 1640995320,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/ghi789/video_post/',
            'url': 'https://v.redd.it/abcd1234',
            'author': 'videouser',
            'score': 89,
            'num_comments': 15,
            'is_video': True,
            'is_nsfw': False,
            'is_self': False,
            'domain': 'v.redd.it',
            'media_url': 'https://v.redd.it/abcd1234/DASH_720.mp4',
            'post_type': 'video',
            'awards': [],
            'media': {
                'type': 'video',
                'reddit_video': {
                    'fallback_url': 'https://v.redd.it/abcd1234/DASH_720.mp4',
                    'height': 720,
                    'width': 1280,
                    'duration': 30
                }
            },
            'crosspost_parent_id': None,
            'gallery_image_urls': [],
            'poll_data': None,
            'edited': False,
            'locked': False,
            'archived': False,
            'spoiler': False,
            'stickied': False
        },
        'text_post': {
            'id': 'jkl012',
            'title': 'Text Post Title',
            'selftext': 'This is a long text post with **markdown** formatting.',
            'created_utc': 1640995380,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/jkl012/text_post/',
            'url': 'https://www.reddit.com/r/testsubreddit/comments/jkl012/text_post/',
            'author': 'textuser',
            'score': 56,
            'num_comments': 8,
            'is_video': False,
            'is_nsfw': False,
            'is_self': True,
            'domain': 'self.testsubreddit',
            'media_url': '',
            'post_type': 'text',
            'awards': [],
            'media': None,
            'crosspost_parent_id': None,
            'gallery_image_urls': [],
            'poll_data': None,
            'edited': False,
            'locked': False,
            'archived': False,
            'spoiler': False,
            'stickied': False
        },
        'poll_post': {
            'id': 'mno345',
            'title': 'Poll Post Title',
            'selftext': 'What do you think?',
            'created_utc': 1640995440,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/mno345/poll_post/',
            'url': 'https://www.reddit.com/r/testsubreddit/comments/mno345/poll_post/',
            'author': 'polluser',
            'score': 78,
            'num_comments': 20,
            'is_video': False,
            'is_nsfw': False,
            'is_self': True,
            'domain': 'self.testsubreddit',
            'media_url': '',
            'post_type': 'poll',
            'awards': [],
            'media': None,
            'crosspost_parent_id': None,
            'gallery_image_urls': [],
            'poll_data': {
                'question': 'What is your favorite color?',
                'options': [
                    {'text': 'Red', 'votes': 10},
                    {'text': 'Blue', 'votes': 15},
                    {'text': 'Green', 'votes': 8}
                ],
                'total_votes': 33,
                'voting_end_timestamp': 1641081840
            },
            'edited': False,
            'locked': False,
            'archived': False,
            'spoiler': False,
            'stickied': False
        },
        'crosspost': {
            'id': 'pqr678',
            'title': 'Crosspost Title',
            'selftext': '',
            'created_utc': 1640995500,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/pqr678/crosspost/',
            'url': 'https://www.reddit.com/r/originalsubreddit/comments/abc123/original_post/',
            'author': 'crosspostuser',
            'score': 34,
            'num_comments': 5,
            'is_video': False,
            'is_nsfw': False,
            'is_self': False,
            'domain': 'reddit.com',
            'media_url': '',
            'post_type': 'crosspost',
            'awards': [],
            'media': None,
            'crosspost_parent_id': 'abc123',
            'gallery_image_urls': [],
            'poll_data': None,
            'edited': False,
            'locked': False,
            'archived': False,
            'spoiler': False,
            'stickied': False
        }
    }


@pytest.fixture
def sample_posts(sample_reddit_post_data):
    """Generate PostMetadata objects from sample data."""
    posts = {}
    for post_type, data in sample_reddit_post_data.items():
        posts[post_type] = PostMetadata.from_raw(data)
    return posts


# Mock Reddit API Fixtures
@pytest.fixture
def mock_reddit_api():
    """Mock the Reddit API (PRAW) for testing."""
    with patch('praw.Reddit') as mock_reddit_class:
        mock_reddit = Mock()
        mock_reddit_class.return_value = mock_reddit
        
        # Mock user
        mock_user = Mock()
        mock_user.name = 'testuser'
        mock_reddit.user.return_value = mock_user
        
        # Mock subreddit
        mock_subreddit = Mock()
        mock_subreddit.display_name = 'testsubreddit'
        mock_reddit.subreddit.return_value = mock_subreddit
        
        # Mock submissions
        mock_submission = Mock()
        mock_submission.id = 'abc123'
        mock_submission.title = 'Test Post'
        mock_submission.selftext = 'Test content'
        mock_submission.created_utc = 1640995200
        mock_submission.subreddit.display_name = 'testsubreddit'
        mock_submission.permalink = '/r/testsubreddit/comments/abc123/test_post/'
        mock_submission.url = 'https://example.com/image.jpg'
        mock_submission.author.name = 'testuser'
        mock_submission.score = 42
        mock_submission.num_comments = 10
        mock_submission.is_video = False
        mock_submission.over_18 = False
        mock_submission.is_self = False
        mock_submission.domain = 'example.com'
        
        # Configure iterators for user submissions
        mock_user.submissions.new.return_value = iter([mock_submission])
        mock_user.submissions.hot.return_value = iter([mock_submission])
        mock_user.submissions.top.return_value = iter([mock_submission])
        
        # Configure iterators for subreddit submissions  
        mock_subreddit.new.return_value = iter([mock_submission])
        mock_subreddit.hot.return_value = iter([mock_submission])
        mock_subreddit.top.return_value = iter([mock_submission])
        
        yield mock_reddit


@pytest.fixture
def mock_yars_api():
    """Mock the YARS API for testing."""
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'children': [
                    {
                        'data': {
                            'id': 'abc123',
                            'title': 'Test Post',
                            'selftext': 'Test content',
                            'created_utc': 1640995200,
                            'subreddit': 'testsubreddit',
                            'permalink': '/r/testsubreddit/comments/abc123/test_post/',
                            'url': 'https://example.com/image.jpg',
                            'author': 'testuser',
                            'score': 42,
                            'num_comments': 10,
                            'is_video': False,
                            'over_18': False,
                            'is_self': False,
                            'domain': 'example.com'
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_response
        yield mock_get


# Configuration Fixtures
@pytest.fixture
def temp_config():
    """Create a temporary configuration for testing."""
    config_data = {
        'dry_run': False,
        'verbose': False,
        'use_pipeline': True,
        'scraping': {
            'api_mode': False,
            'client_id': '',
            'client_secret': '',
            'user_agent': 'test-agent',
            'username': '',
            'password': '',
            'post_limit': 10,
            'sleep_interval_api': 0.7,
            'sleep_interval_public': 6.1,
            'timeout': 30,
            'max_retries': 3
        },
        'output': {
            'output_dir': 'test_downloads',
            'export_formats': ['json'],
            'organize_by_date': False,
            'organize_by_author': False,
            'organize_by_subreddit': True,
            'filename_template': '{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}'
        },
        'processing': {
            'embed_metadata': True,
            'create_json_sidecars': True,
            'concurrent_downloads': 3,
            'enable_processing': False,
            'processing_profile': 'default',
            'image_format': 'original',
            'image_quality': 95,
            'max_image_resolution': None,
            'video_format': 'original',
            'video_quality': 23,
            'max_video_resolution': None,
            'generate_thumbnails': False,
            'thumbnail_size': 256,
            'thumbnail_timestamp': '00:00:01'
        },
        'filters': {
            'min_score': None,
            'max_score': None,
            'include_nsfw': False,
            'nsfw_only': False,
            'nsfw_mode': 'exclude',
            'include_keywords': [],
            'exclude_keywords': [],
            'keyword_case_sensitive': False,
            'keyword_regex': False,
            'keyword_whole_words': False,
            'after_date': None,
            'before_date': None,
            'allowed_domains': [],
            'blocked_domains': [],
            'media_types': [],
            'exclude_media_types': [],
            'file_extensions': [],
            'exclude_file_extensions': [],
            'filter_composition': 'and'
        },
        'ui_config': {
            'output_mode': 'normal',
            'progress_display': None,
            'show_individual_progress': True,
            'max_individual_bars': 5,
            'show_eta': True,
            'show_speed': True,
            'show_statistics': True,
            'quiet_mode': False,
            'no_progress': False,
            'json_output': None
        },
        'observer_config': {
            'enabled_observers': ['console', 'cli_progress'],
            'console_observer': {
                'use_rich': True,
                'show_timestamps': True,
                'color_output': True
            },
            'logging_observer': {
                'log_file': 'redditdl.log',
                'log_level': 'INFO',
                'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        }
    }
    
    return AppConfig(**config_data)


@pytest.fixture
def temp_config_file():
    """Create a temporary configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        config_content = """
dry_run: false
verbose: false
use_pipeline: true

scraping:
  api_mode: false
  post_limit: 10
  sleep_interval_api: 0.7
  sleep_interval_public: 6.1
  timeout: 30
  max_retries: 3

output:
  output_dir: "test_downloads"
  export_formats: ["json"]
  organize_by_subreddit: true
  filename_template: "{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}"

processing:
  embed_metadata: true
  create_json_sidecars: true
  concurrent_downloads: 3

filters:
  include_nsfw: false
  filter_composition: "and"
"""
        f.write(config_content)
        f.flush()
        yield f.name
    
    # Cleanup
    os.unlink(f.name)


# Database Fixtures
@pytest.fixture
def temp_database():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    # Create database with schema
    conn = sqlite3.connect(db_path)
    
    # Read schema from file
    schema_path = Path(__file__).parent.parent / "core" / "state" / "schema.sql"
    with open(schema_path, 'r') as schema_file:
        schema = schema_file.read()
    
    conn.executescript(schema)
    conn.close()
    
    yield db_path
    
    # Cleanup
    os.unlink(db_path)


@pytest.fixture
def state_manager(temp_database):
    """Create a StateManager instance with temporary database."""
    return StateManager(db_path=temp_database)


# File System Fixtures
@pytest.fixture
def temp_download_dir():
    """Create a temporary download directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_file_system():
    """Mock file system operations for testing."""
    with patch('pathlib.Path.mkdir') as mock_mkdir, \
         patch('pathlib.Path.exists') as mock_exists, \
         patch('pathlib.Path.is_file') as mock_is_file, \
         patch('pathlib.Path.is_dir') as mock_is_dir, \
         patch('builtins.open', create=True) as mock_open:
        
        mock_exists.return_value = True
        mock_is_file.return_value = True
        mock_is_dir.return_value = True
        
        yield {
            'mkdir': mock_mkdir,
            'exists': mock_exists,
            'is_file': mock_is_file,
            'is_dir': mock_is_dir,
            'open': mock_open
        }


# Event System Fixtures
@pytest.fixture
def event_emitter():
    """Create an EventEmitter instance for testing."""
    return EventEmitter()


@pytest.fixture
def mock_event_observer():
    """Create a mock event observer for testing."""
    observer = Mock()
    observer.name = 'test_observer'
    observer.handle_event = Mock()
    return observer


# Plugin System Fixtures
@pytest.fixture
def plugin_manager():
    """Create a PluginManager instance for testing."""
    return PluginManager()


@pytest.fixture
def mock_plugin():
    """Create a mock plugin for testing."""
    plugin = Mock()
    plugin.name = 'test_plugin'
    plugin.version = '1.0.0'
    plugin.can_handle = Mock(return_value=True)
    plugin.process = Mock(return_value={'success': True})
    return plugin


# Network Fixtures
@pytest.fixture
def mock_http_requests():
    """Mock HTTP requests for testing."""
    with patch('requests.get') as mock_get, \
         patch('requests.head') as mock_head, \
         patch('requests.Session') as mock_session:
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024', 'content-type': 'image/jpeg'}
        mock_response.content = b'fake image data'
        mock_response.iter_content = Mock(return_value=[b'fake', b' image', b' data'])
        
        mock_get.return_value = mock_response
        mock_head.return_value = mock_response
        
        # Mock session
        mock_session_instance = Mock()
        mock_session_instance.get.return_value = mock_response
        mock_session_instance.head.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        yield {
            'get': mock_get,
            'head': mock_head,
            'session': mock_session,
            'response': mock_response
        }


# Utility Fixtures
@pytest.fixture
def sample_media_urls():
    """Sample media URLs for testing."""
    return {
        'image_jpg': 'https://example.com/image.jpg',
        'image_png': 'https://example.com/image.png',
        'image_gif': 'https://example.com/animation.gif',
        'video_mp4': 'https://example.com/video.mp4',
        'video_webm': 'https://example.com/video.webm',
        'reddit_video': 'https://v.redd.it/abcd1234',
        'reddit_gallery': 'https://www.reddit.com/gallery/abc123',
        'external_link': 'https://youtube.com/watch?v=abcd1234'
    }


@pytest.fixture
def sample_file_data():
    """Sample file data for testing."""
    return {
        'image_data': b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82',
        'json_data': '{"test": "data", "number": 42}',
        'text_data': 'This is sample text content for testing.',
        'html_data': '<html><body><h1>Test Page</h1></body></html>'
    }


# Async Testing Fixtures
@pytest.fixture
def event_loop():
    """Create an event loop for async testing."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def async_test_setup():
    """Setup for async tests."""
    # Any async setup needed
    yield
    # Any async cleanup needed


# Performance Testing Fixtures
@pytest.fixture
def performance_monitor():
    """Monitor performance metrics during testing."""
    import psutil
    process = psutil.Process()
    
    start_time = time.time()
    start_memory = process.memory_info().rss
    
    yield {
        'start_time': start_time,
        'start_memory': start_memory,
        'process': process
    }
    
    end_time = time.time()
    end_memory = process.memory_info().rss
    
    # Could log performance metrics here if needed
    execution_time = end_time - start_time
    memory_delta = end_memory - start_memory
    
    # Optionally assert performance requirements
    assert execution_time < 60, f"Test took too long: {execution_time:.2f}s"
    assert memory_delta < 100 * 1024 * 1024, f"Memory usage too high: {memory_delta / 1024 / 1024:.2f}MB"


# Pytest Configuration
def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Mark slow tests
        if "slow" in item.nodeid or "performance" in item.nodeid:
            item.add_marker(pytest.mark.slow)
        
        # Mark integration tests
        if "integration" in item.nodeid or "test_integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        
        # Mark end-to-end tests
        if "e2e" in item.nodeid or "end_to_end" in item.nodeid:
            item.add_marker(pytest.mark.e2e)