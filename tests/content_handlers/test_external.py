"""
Tests for External Content Handler

Tests the external link content handler functionality including
URL validation, metadata extraction, and link processing.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path

from redditdl.content_handlers.external import ExternalLinksHandler
from redditdl.scrapers import PostMetadata


class TestExternalContentHandler:
    """Test suite for ExternalContentHandler."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.handler = ExternalLinksHandler()
        self.temp_dir = Path('test_downloads')
    
    def test_handler_initialization(self):
        """Test handler initialization."""
        assert isinstance(self.handler, ExternalLinksHandler)
        assert hasattr(self.handler, 'priority')
        assert hasattr(self.handler, 'can_handle')
        assert hasattr(self.handler, 'process')
    
    def test_can_handle_external_urls(self):
        """Test handler can identify external URLs."""
        # YouTube URL
        post_data = {
            'url': 'https://youtube.com/watch?v=abcd1234',
            'domain': 'youtube.com',
            'is_self': False
        }
        assert self.handler.can_handle('external', post_data) is True
        
        # Twitter URL
        post_data = {
            'url': 'https://twitter.com/user/status/123456',
            'domain': 'twitter.com',
            'is_self': False
        }
        assert self.handler.can_handle('external', post_data) is True
        
        # General external URL
        post_data = {
            'url': 'https://example.com/article',
            'domain': 'example.com',
            'is_self': False
        }
        assert self.handler.can_handle('external', post_data) is True
    
    def test_can_handle_non_external_content(self):
        """Test handler rejects non-external content."""
        # Reddit self post
        post_data = {
            'url': 'https://www.reddit.com/r/test/comments/abc123/',
            'domain': 'self.test',
            'is_self': True
        }
        assert self.handler.can_handle('text', post_data) is False
        
        # Reddit media
        post_data = {
            'url': 'https://i.redd.it/image.jpg',
            'domain': 'i.redd.it',
            'is_self': False
        }
        assert self.handler.can_handle('image', post_data) is False
        
        # Reddit video
        post_data = {
            'url': 'https://v.redd.it/video123',
            'domain': 'v.redd.it',
            'is_self': False
        }
        assert self.handler.can_handle('video', post_data) is False
    
    def test_get_supported_types(self):
        """Test handler returns correct supported types."""
        supported_types = self.handler.get_supported_types()
        
        assert isinstance(supported_types, list)
        assert 'external' in supported_types
        assert len(supported_types) >= 1
    
    @pytest.mark.asyncio
    async def test_process_youtube_url(self):
        """Test processing YouTube URL."""
        post_data = {
            'id': 'abc123',
            'title': 'Test Video',
            'url': 'https://youtube.com/watch?v=dQw4w9WgXcQ',
            'domain': 'youtube.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'create_json_sidecars': True,
            'save_external_links': True
        }
        
        with patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True) as mock_open:
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert result['content_type'] == 'external'
            assert result['url'] == post_data['url']
            assert 'youtube' in result['platform'].lower()
            
            # Should have saved link metadata
            mock_open.assert_called()
    
    @pytest.mark.asyncio
    async def test_process_twitter_url(self):
        """Test processing Twitter URL."""
        post_data = {
            'id': 'def456',
            'title': 'Tweet Link',
            'url': 'https://twitter.com/user/status/123456789',
            'domain': 'twitter.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'create_json_sidecars': True,
            'save_external_links': True
        }
        
        with patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert result['content_type'] == 'external'
            assert 'twitter' in result['platform'].lower()
    
    @pytest.mark.asyncio
    async def test_process_generic_external_url(self):
        """Test processing generic external URL."""
        post_data = {
            'id': 'ghi789',
            'title': 'External Article',
            'url': 'https://example.com/interesting-article',
            'domain': 'example.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'create_json_sidecars': True,
            'save_external_links': True
        }
        
        with patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert result['content_type'] == 'external'
            assert result['platform'] == 'external'
    
    @pytest.mark.asyncio
    async def test_process_with_metadata_extraction(self):
        """Test processing external URL with metadata extraction."""
        post_data = {
            'id': 'jkl012',
            'title': 'Article Link',
            'url': 'https://news.example.com/article/123',
            'domain': 'news.example.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'create_json_sidecars': True,
            'save_external_links': True,
            'extract_metadata': True
        }
        
        # Mock HTTP request for metadata extraction
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '''
        <html>
        <head>
            <title>Test Article</title>
            <meta property="og:title" content="Open Graph Title">
            <meta property="og:description" content="Article description">
            <meta property="og:image" content="https://example.com/image.jpg">
        </head>
        <body>Article content</body>
        </html>
        '''
        
        with patch('requests.get', return_value=mock_response), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert 'metadata' in result
            assert result['metadata']['title'] == 'Open Graph Title'
            assert result['metadata']['description'] == 'Article description'
    
    @pytest.mark.asyncio
    async def test_process_save_disabled(self):
        """Test processing when external link saving is disabled."""
        post_data = {
            'id': 'mno345',
            'title': 'External Link',
            'url': 'https://example.com/page',
            'domain': 'example.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'save_external_links': False
        }
        
        result = await self.handler.process(post_data, config)
        
        assert result['success'] is True
        assert result['action'] == 'skipped'
        assert 'External link saving disabled' in result['message']
    
    @pytest.mark.asyncio
    async def test_process_network_error(self):
        """Test processing with network errors during metadata extraction."""
        post_data = {
            'id': 'pqr678',
            'title': 'Failing Link',
            'url': 'https://unreachable.example.com/page',
            'domain': 'unreachable.example.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'create_json_sidecars': True,
            'save_external_links': True,
            'extract_metadata': True
        }
        
        # Mock network error
        with patch('requests.get', side_effect=ConnectionError("Network error")), \
             patch('pathlib.Path.mkdir'), \
             patch('builtins.open', create=True):
            
            result = await self.handler.process(post_data, config)
            
            # Should handle error gracefully
            assert result['success'] is True
            assert 'error' in result
            assert 'Network error' in result['error']
    
    @pytest.mark.asyncio
    async def test_process_file_write_error(self):
        """Test processing with file write errors."""
        post_data = {
            'id': 'stu901',
            'title': 'Link with Write Error',
            'url': 'https://example.com/page',
            'domain': 'example.com',
            'author': 'testuser',
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'create_json_sidecars': True,
            'save_external_links': True
        }
        
        with patch('pathlib.Path.mkdir'), \
             patch('builtins.open', side_effect=IOError("Permission denied")):
            
            result = await self.handler.process(post_data, config)
            
            # Should handle file error gracefully
            assert result['success'] is False
            assert 'error' in result
            assert 'Permission denied' in result['error']
    
    def test_platform_detection(self):
        """Test platform detection for various URLs."""
        # Test YouTube detection
        assert self.handler._detect_platform('https://youtube.com/watch?v=123') == 'youtube'
        assert self.handler._detect_platform('https://youtu.be/123') == 'youtube'
        
        # Test Twitter detection
        assert self.handler._detect_platform('https://twitter.com/user/status/123') == 'twitter'
        assert self.handler._detect_platform('https://x.com/user/status/123') == 'twitter'
        
        # Test Reddit detection (should not be handled as external)
        assert self.handler._detect_platform('https://reddit.com/r/test/') == 'reddit'
        
        # Test generic platform
        assert self.handler._detect_platform('https://unknown.com/page') == 'external'
    
    def test_url_validation(self):
        """Test URL validation functionality."""
        # Valid URLs
        assert self.handler._is_valid_url('https://example.com') is True
        assert self.handler._is_valid_url('http://example.com') is True
        assert self.handler._is_valid_url('https://sub.example.com/path') is True
        
        # Invalid URLs
        assert self.handler._is_valid_url('not-a-url') is False
        assert self.handler._is_valid_url('') is False
        assert self.handler._is_valid_url(None) is False
        assert self.handler._is_valid_url('ftp://example.com') is False
    
    def test_metadata_extraction_from_html(self):
        """Test metadata extraction from HTML content."""
        html_content = '''
        <html>
        <head>
            <title>Page Title</title>
            <meta property="og:title" content="OG Title">
            <meta property="og:description" content="Page description">
            <meta property="og:image" content="https://example.com/image.jpg">
            <meta property="og:url" content="https://example.com/page">
            <meta name="description" content="Meta description">
            <meta name="author" content="Page Author">
        </head>
        <body>Content</body>
        </html>
        '''
        
        metadata = self.handler._extract_metadata_from_html(html_content)
        
        assert metadata['title'] == 'OG Title'  # Should prefer Open Graph
        assert metadata['description'] == 'Page description'
        assert metadata['image'] == 'https://example.com/image.jpg'
        assert metadata['url'] == 'https://example.com/page'
        assert metadata['author'] == 'Page Author'
    
    def test_metadata_extraction_fallback(self):
        """Test metadata extraction with fallback to regular meta tags."""
        html_content = '''
        <html>
        <head>
            <title>Basic Title</title>
            <meta name="description" content="Basic description">
            <meta name="author" content="Basic Author">
        </head>
        <body>Content</body>
        </html>
        '''
        
        metadata = self.handler._extract_metadata_from_html(html_content)
        
        assert metadata['title'] == 'Basic Title'
        assert metadata['description'] == 'Basic description'
        assert metadata['author'] == 'Basic Author'
    
    def test_filename_generation(self):
        """Test filename generation for external links."""
        post_data = {
            'id': 'abc123',
            'title': 'Test Article Title',
            'url': 'https://example.com/article',
            'domain': 'example.com'
        }
        
        filename = self.handler._generate_filename(post_data)
        
        assert filename.endswith('.json')
        assert 'abc123' in filename
        assert 'external' in filename
    
    def test_sanitize_filename(self):
        """Test filename sanitization."""
        unsafe_name = 'title with/invalid\\chars<>:"|?*'
        safe_name = self.handler._sanitize_filename(unsafe_name)
        
        # Should remove or replace unsafe characters
        assert '/' not in safe_name
        assert '\\' not in safe_name
        assert '<' not in safe_name
        assert '>' not in safe_name
        assert ':' not in safe_name
        assert '"' not in safe_name
        assert '|' not in safe_name
        assert '?' not in safe_name
        assert '*' not in safe_name
    
    def test_priority_setting(self):
        """Test handler priority is set correctly."""
        assert hasattr(self.handler, 'priority')
        assert isinstance(self.handler.priority, int)
        # External handler should have lower priority than media handlers
        assert self.handler.priority > 0