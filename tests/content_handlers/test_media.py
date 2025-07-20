"""
Tests for Media Content Handler

Tests the media content handler functionality including
image and video processing, download management, and metadata handling.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from pathlib import Path
import tempfile

from redditdl.content_handlers.media import MediaContentHandler
from redditdl.scrapers import PostMetadata


class TestMediaContentHandler:
    """Test suite for MediaContentHandler."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.handler = MediaContentHandler()
        self.temp_dir = Path('test_downloads')
    
    def test_handler_initialization(self):
        """Test handler initialization."""
        assert isinstance(self.handler, MediaContentHandler)
        assert hasattr(self.handler, 'priority')
        assert hasattr(self.handler, 'can_handle')
        assert hasattr(self.handler, 'process')
    
    def test_can_handle_image_content(self):
        """Test handler can identify image content."""
        # Direct image URL
        post_data = {
            'url': 'https://example.com/image.jpg',
            'domain': 'example.com',
            'is_video': False,
            'media_url': 'https://example.com/image.jpg'
        }
        assert self.handler.can_handle('image', post_data) is True
        
        # Reddit image
        post_data = {
            'url': 'https://i.redd.it/abc123.png',
            'domain': 'i.redd.it',
            'is_video': False,
            'media_url': 'https://i.redd.it/abc123.png'
        }
        assert self.handler.can_handle('image', post_data) is True
        
        # Imgur image
        post_data = {
            'url': 'https://i.imgur.com/abc123.gif',
            'domain': 'i.imgur.com',
            'is_video': False,
            'media_url': 'https://i.imgur.com/abc123.gif'
        }
        assert self.handler.can_handle('image', post_data) is True
    
    def test_can_handle_video_content(self):
        """Test handler can identify video content."""
        # Reddit video
        post_data = {
            'url': 'https://v.redd.it/abc123',
            'domain': 'v.redd.it',
            'is_video': True,
            'media_url': 'https://v.redd.it/abc123/DASH_720.mp4'
        }
        assert self.handler.can_handle('video', post_data) is True
        
        # Direct video URL
        post_data = {
            'url': 'https://example.com/video.mp4',
            'domain': 'example.com',
            'is_video': True,
            'media_url': 'https://example.com/video.mp4'
        }
        assert self.handler.can_handle('video', post_data) is True
    
    def test_can_handle_non_media_content(self):
        """Test handler rejects non-media content."""
        # Text post
        post_data = {
            'url': 'https://www.reddit.com/r/test/comments/abc123/',
            'domain': 'self.test',
            'is_self': True,
            'is_video': False,
            'media_url': ''
        }
        assert self.handler.can_handle('text', post_data) is False
        
        # External link
        post_data = {
            'url': 'https://youtube.com/watch?v=abc123',
            'domain': 'youtube.com',
            'is_video': False,
            'media_url': ''
        }
        assert self.handler.can_handle('external', post_data) is False
    
    def test_get_supported_types(self):
        """Test handler returns correct supported types."""
        supported_types = self.handler.get_supported_types()
        
        assert isinstance(supported_types, list)
        assert 'image' in supported_types
        assert 'video' in supported_types
        assert len(supported_types) >= 2
    
    @pytest.mark.asyncio
    async def test_process_image_download(self):
        """Test processing image download."""
        post_data = {
            'id': 'abc123',
            'title': 'Test Image',
            'url': 'https://example.com/image.jpg',
            'media_url': 'https://example.com/image.jpg',
            'domain': 'example.com',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'is_video': False,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'embed_metadata': True,
            'create_json_sidecars': True,
            'concurrent_downloads': 3
        }
        
        # Mock successful download
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': True,
            'local_path': str(self.temp_dir / 'testsubreddit' / 'abc123-test-image.jpg'),
            'file_size': 1024000,
            'download_time': 2.5
        })
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader), \
             patch('content_handlers.media.MetadataEmbedder') as mock_embedder_class:
            
            mock_embedder = Mock()
            mock_embedder_class.return_value = mock_embedder
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert result['content_type'] == 'image'
            assert result['media_type'] == 'image'
            assert 'local_path' in result
            assert 'file_size' in result
            assert 'download_time' in result
            
            # Should have attempted download
            mock_downloader.download_media.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_video_download(self):
        """Test processing video download."""
        post_data = {
            'id': 'def456',
            'title': 'Test Video',
            'url': 'https://v.redd.it/abc123',
            'media_url': 'https://v.redd.it/abc123/DASH_720.mp4',
            'domain': 'v.redd.it',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'is_video': True,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'embed_metadata': True,
            'create_json_sidecars': True,
            'concurrent_downloads': 3
        }
        
        # Mock successful download
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': True,
            'local_path': str(self.temp_dir / 'testsubreddit' / 'def456-test-video.mp4'),
            'file_size': 5120000,
            'download_time': 10.2
        })
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert result['content_type'] == 'video'
            assert result['media_type'] == 'video'
            assert 'local_path' in result
            assert result['file_size'] == 5120000
            assert result['download_time'] == 10.2
    
    @pytest.mark.asyncio
    async def test_process_with_metadata_embedding(self):
        """Test processing with metadata embedding."""
        post_data = {
            'id': 'ghi789',
            'title': 'Test Image with Metadata',
            'url': 'https://example.com/image.jpg',
            'media_url': 'https://example.com/image.jpg',
            'domain': 'example.com',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'score': 42,
            'num_comments': 10,
            'is_video': False,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'embed_metadata': True,
            'create_json_sidecars': True
        }
        
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': True,
            'local_path': str(self.temp_dir / 'testsubreddit' / 'ghi789-test-image.jpg'),
            'file_size': 1024000,
            'download_time': 2.5
        })
        
        mock_embedder = Mock()
        mock_embedder.embed_exif_metadata = Mock()
        mock_embedder.create_json_sidecar = Mock()
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader), \
             patch('content_handlers.media.MetadataEmbedder', return_value=mock_embedder):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            
            # Should have embedded metadata
            mock_embedder.embed_exif_metadata.assert_called_once()
            mock_embedder.create_json_sidecar.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_download_failure(self):
        """Test processing when download fails."""
        post_data = {
            'id': 'jkl012',
            'title': 'Failing Download',
            'url': 'https://example.com/nonexistent.jpg',
            'media_url': 'https://example.com/nonexistent.jpg',
            'domain': 'example.com',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'is_video': False,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'embed_metadata': True,
            'create_json_sidecars': True
        }
        
        # Mock failed download
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': False,
            'error': '404 Not Found',
            'retry_count': 3
        })
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is False
            assert 'error' in result
            assert '404 Not Found' in result['error']
            assert result['retry_count'] == 3
    
    @pytest.mark.asyncio
    async def test_process_metadata_embedding_failure(self):
        """Test processing when metadata embedding fails."""
        post_data = {
            'id': 'mno345',
            'title': 'Metadata Failure',
            'url': 'https://example.com/image.jpg',
            'media_url': 'https://example.com/image.jpg',
            'domain': 'example.com',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'is_video': False,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'embed_metadata': True,
            'create_json_sidecars': True
        }
        
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': True,
            'local_path': str(self.temp_dir / 'testsubreddit' / 'mno345-image.jpg'),
            'file_size': 1024000,
            'download_time': 2.5
        })
        
        mock_embedder = Mock()
        mock_embedder.embed_exif_metadata.side_effect = Exception("EXIF error")
        mock_embedder.create_json_sidecar = Mock()
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader), \
             patch('content_handlers.media.MetadataEmbedder', return_value=mock_embedder):
            
            result = await self.handler.process(post_data, config)
            
            # Should still succeed even if metadata embedding fails
            assert result['success'] is True
            assert 'metadata_warnings' in result
            assert 'EXIF error' in result['metadata_warnings'][0]
    
    @pytest.mark.asyncio
    async def test_process_concurrent_downloads(self):
        """Test processing with concurrent download limits."""
        post_data = {
            'id': 'pqr678',
            'title': 'Concurrent Test',
            'url': 'https://example.com/image.jpg',
            'media_url': 'https://example.com/image.jpg',
            'domain': 'example.com',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'is_video': False,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'concurrent_downloads': 1,  # Limit concurrency
            'embed_metadata': False,
            'create_json_sidecars': False
        }
        
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': True,
            'local_path': str(self.temp_dir / 'testsubreddit' / 'pqr678-image.jpg'),
            'file_size': 1024000,
            'download_time': 2.5
        })
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            # Should respect concurrent download settings
            mock_downloader.download_media.assert_called_once()
    
    def test_media_type_detection(self):
        """Test media type detection from URLs and metadata."""
        # Image extensions
        assert self.handler._detect_media_type('https://example.com/image.jpg') == 'image'
        assert self.handler._detect_media_type('https://example.com/image.png') == 'image'
        assert self.handler._detect_media_type('https://example.com/image.gif') == 'image'
        assert self.handler._detect_media_type('https://example.com/image.webp') == 'image'
        
        # Video extensions
        assert self.handler._detect_media_type('https://example.com/video.mp4') == 'video'
        assert self.handler._detect_media_type('https://example.com/video.webm') == 'video'
        assert self.handler._detect_media_type('https://example.com/video.mov') == 'video'
        
        # Reddit-specific URLs
        assert self.handler._detect_media_type('https://i.redd.it/abc123.jpg') == 'image'
        assert self.handler._detect_media_type('https://v.redd.it/abc123') == 'video'
        
        # Unknown/unsupported
        assert self.handler._detect_media_type('https://example.com/document.pdf') == 'unknown'
        assert self.handler._detect_media_type('https://example.com/no-extension') == 'unknown'
    
    def test_url_validation(self):
        """Test URL validation for media content."""
        # Valid media URLs
        assert self.handler._is_valid_media_url('https://example.com/image.jpg') is True
        assert self.handler._is_valid_media_url('https://i.redd.it/abc123.png') is True
        assert self.handler._is_valid_media_url('https://v.redd.it/abc123') is True
        
        # Invalid URLs
        assert self.handler._is_valid_media_url('') is False
        assert self.handler._is_valid_media_url(None) is False
        assert self.handler._is_valid_media_url('not-a-url') is False
        assert self.handler._is_valid_media_url('ftp://example.com/file.jpg') is False
    
    def test_filename_generation(self):
        """Test filename generation for media files."""
        post_data = {
            'id': 'abc123',
            'title': 'Test Image Title',
            'subreddit': 'testsubreddit',
            'url': 'https://example.com/image.jpg'
        }
        
        filename = self.handler._generate_filename(post_data, 'image')
        
        assert filename.endswith('.jpg')
        assert 'abc123' in filename
        assert 'test-image-title' in filename.lower()
    
    def test_filename_sanitization(self):
        """Test filename sanitization for media files."""
        post_data = {
            'id': 'abc123',
            'title': 'Title with/invalid\\chars<>:"|?*',
            'subreddit': 'testsubreddit',
            'url': 'https://example.com/image.jpg'
        }
        
        filename = self.handler._generate_filename(post_data, 'image')
        
        # Should sanitize unsafe characters
        assert '/' not in filename
        assert '\\' not in filename
        assert '<' not in filename
        assert '>' not in filename
        assert ':' not in filename
        assert '"' not in filename
        assert '|' not in filename
        assert '?' not in filename
        assert '*' not in filename
    
    def test_file_extension_detection(self):
        """Test file extension detection from URLs."""
        assert self.handler._get_file_extension('https://example.com/image.jpg') == '.jpg'
        assert self.handler._get_file_extension('https://example.com/image.PNG') == '.png'
        assert self.handler._get_file_extension('https://example.com/video.mp4?param=value') == '.mp4'
        assert self.handler._get_file_extension('https://example.com/no-extension') == ''
        assert self.handler._get_file_extension('https://v.redd.it/abc123') == '.mp4'  # Default for Reddit videos
    
    def test_priority_setting(self):
        """Test handler priority is set correctly."""
        assert hasattr(self.handler, 'priority')
        assert isinstance(self.handler.priority, int)
        # Media handler should have high priority
        assert self.handler.priority < 100
    
    @pytest.mark.asyncio
    async def test_process_with_processing_pipeline(self):
        """Test processing with media processing pipeline integration."""
        post_data = {
            'id': 'stu901',
            'title': 'Processing Test',
            'url': 'https://example.com/image.jpg',
            'media_url': 'https://example.com/image.jpg',
            'domain': 'example.com',
            'author': 'testuser',
            'subreddit': 'testsubreddit',
            'is_video': False,
            'created_utc': 1640995200
        }
        
        config = {
            'output_dir': self.temp_dir,
            'enable_processing': True,
            'processing_profile': 'optimize',
            'image_quality': 80,
            'max_image_resolution': 1920
        }
        
        mock_downloader = Mock()
        mock_downloader.download_media = AsyncMock(return_value={
            'success': True,
            'local_path': str(self.temp_dir / 'testsubreddit' / 'stu901-image.jpg'),
            'file_size': 1024000,
            'download_time': 2.5
        })
        
        mock_processor = Mock()
        mock_processor.process_image = AsyncMock(return_value={
            'success': True,
            'processed_path': str(self.temp_dir / 'testsubreddit' / 'stu901-image-processed.jpg'),
            'original_size': 1024000,
            'processed_size': 512000,
            'operations': ['resize', 'quality_adjust']
        })
        
        with patch('content_handlers.media.MediaDownloader', return_value=mock_downloader), \
             patch('content_handlers.media.ImageProcessor', return_value=mock_processor):
            
            result = await self.handler.process(post_data, config)
            
            assert result['success'] is True
            assert 'processing_result' in result
            assert result['processing_result']['operations'] == ['resize', 'quality_adjust']
    
    def test_content_type_classification(self):
        """Test content type classification based on post data."""
        # Image post
        image_post = {
            'url': 'https://i.redd.it/abc123.jpg',
            'is_video': False,
            'media': None
        }
        assert self.handler._classify_content_type(image_post) == 'image'
        
        # Video post
        video_post = {
            'url': 'https://v.redd.it/abc123',
            'is_video': True,
            'media': {'type': 'video'}
        }
        assert self.handler._classify_content_type(video_post) == 'video'
        
        # Gallery post (should be handled by gallery handler)
        gallery_post = {
            'url': 'https://www.reddit.com/gallery/abc123',
            'is_video': False,
            'media': {'type': 'gallery'}
        }
        assert self.handler._classify_content_type(gallery_post) == 'gallery'