"""
Integration tests for processing pipeline.

Tests the complete integration between processing components and MediaContentHandler,
ensuring that the media processing workflow works end-to-end.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
from PIL import Image

from redditdl.content_handlers.media import MediaContentHandler
from redditdl.scrapers import PostMetadata
from redditdl.processing import PROCESSING_AVAILABLE


class TestProcessingIntegration:
    """Integration test cases for processing pipeline."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def sample_image(self, temp_dir):
        """Create a sample test image file."""
        img = Image.new('RGB', (800, 600), color='blue')
        img_path = temp_dir / "sample.jpg"
        img.save(img_path, 'JPEG', quality=95)
        return img_path
    
    @pytest.fixture
    def sample_video(self, temp_dir):
        """Create a fake video file for testing."""
        video_path = temp_dir / "sample.mp4"
        video_path.write_bytes(b"fake video content for testing")
        return video_path
    
    @pytest.fixture
    def sample_post_metadata(self):
        """Create sample PostMetadata for testing."""
        return PostMetadata(
            id="test123",
            title="Test Post",
            url="https://example.com/media.jpg",
            media_url="https://example.com/media.jpg",
            author="testuser",
            subreddit="testsubreddit",
            date_iso="2024-01-01",
            is_video=False
        )
    
    @pytest.fixture
    def handler(self):
        """Create MediaContentHandler instance."""
        return MediaContentHandler()
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    async def test_image_processing_disabled(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test media handler with processing disabled."""
        config = {
            'sleep_interval': 0.1,
            'processing': {'enabled': False}
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            assert result.success is True
            assert len(result.files_created) == 1  # Only original file
            assert 'processing' not in result.operations_performed
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    async def test_image_format_conversion_processing(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test image format conversion through processing pipeline."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'image_format_conversion': True,
                'target_image_format': 'png',
                'image_quality': 85,
                'preserve_original_metadata': True
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            assert result.success is True
            # Should have original file plus converted file
            assert len(result.files_created) >= 1
            assert 'image_format_conversion' in result.operations_performed
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    async def test_image_thumbnail_generation_processing(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test image thumbnail generation through processing pipeline."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'generate_thumbnails': True,
                'thumbnail_size': 256
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            assert result.success is True
            assert 'thumbnail_generation' in result.operations_performed
            
            # Check that thumbnail file was created
            thumbnail_files = [f for f in result.files_created if 'thumb' in f.name]
            assert len(thumbnail_files) > 0
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    async def test_image_quality_adjustment_processing(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test image quality adjustment through processing pipeline."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'image_quality_adjustment': True,
                'image_quality': 60
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            assert result.success is True
            assert 'image_quality_adjustment' in result.operations_performed
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    async def test_image_resolution_limiting_processing(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test image resolution limiting through processing pipeline."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'max_image_resolution': 400
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            assert result.success is True
            assert 'image_resize' in result.operations_performed
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @patch('processing.video_processor.VideoProcessor._check_ffmpeg_available', return_value=True)
    @patch('processing.video_processor.ffmpeg')
    async def test_video_processing_format_conversion(self, mock_ffmpeg, mock_ffmpeg_check, 
                                               handler, sample_video, temp_dir):
        """Test video format conversion through processing pipeline."""
        # Create video post metadata
        video_post = PostMetadata(
            id="video123",
            title="Test Video",
            url="https://example.com/video.mp4",
            media_url="https://example.com/video.mp4",
            author="testuser",
            subreddit="testsubreddit",
            date_iso="2024-01-01",
            is_video=True
        )
        
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'video_format_conversion': True,
                'target_video_format': 'avi',
                'video_quality_crf': 23
            }
        }
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_output = Mock()
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_video
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            # Create fake converted file
            converted_path = sample_video.with_suffix('.avi')
            converted_path.write_bytes(b"fake converted video")
            
            result = await handler.process(video_post, temp_dir, config)
            
            assert result.success is True
            assert 'video_format_conversion' in result.operations_performed
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @patch('processing.video_processor.VideoProcessor._check_ffmpeg_available', return_value=True)
    @patch('processing.video_processor.ffmpeg')
    async def test_video_thumbnail_extraction(self, mock_ffmpeg, mock_ffmpeg_check,
                                       handler, sample_video, temp_dir):
        """Test video thumbnail extraction through processing pipeline."""
        # Create video post metadata
        video_post = PostMetadata(
            id="video456",
            title="Test Video Thumbnail",
            url="https://example.com/video.mp4",
            media_url="https://example.com/video.mp4",
            author="testuser",
            subreddit="testsubreddit",
            date_iso="2024-01-01",
            is_video=True
        )
        
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'generate_thumbnails': True,
                'thumbnail_timestamp': '00:00:03'
            }
        }
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_output = Mock()
        mock_filter = Mock()
        mock_input.filter.return_value = mock_filter
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_video
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            # Create fake thumbnail file
            thumbnail_path = sample_video.with_name(f"{sample_video.stem}_thumb.jpg")
            thumbnail_path.write_bytes(b"fake thumbnail image")
            
            result = await handler.process(video_post, temp_dir, config)
            
            assert result.success is True
            assert 'video_thumbnail_extraction' in result.operations_performed
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    async def test_processing_error_handling(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test processing error handling doesn't break main workflow."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'image_format_conversion': True,
                'target_image_format': 'invalid_format'  # This should cause an error
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            # Processing should fail but not break the main download
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            # Main download should still succeed despite processing failure
            assert result.success is True
            assert len(result.files_created) == 1  # Only original file
    
    @pytest.mark.asyncio
    async def test_processing_not_available_graceful_fallback(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test graceful fallback when processing is not available."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'image_format_conversion': True
            }
        }
        
        # Mock processing as unavailable
        with patch('content_handlers.media.PROCESSING_AVAILABLE', False):
            with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
                mock_downloader = Mock()
                mock_downloader.download.return_value = sample_image
                mock_downloader.embedder = None
                mock_get_downloader.return_value = mock_downloader
                
                result = await handler.process(sample_post_metadata, temp_dir, config)
                
                assert result.success is True
                # Should work normally without processing
                assert len(result.files_created) == 1
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_unknown_content_type_skip_processing(self, handler, temp_dir):
        """Test that unknown content types skip processing gracefully."""
        # Create file with unknown extension
        unknown_file = temp_dir / "unknown.xyz"
        unknown_file.write_bytes(b"unknown content")
        
        unknown_post = PostMetadata(
            id="unknown123",
            title="Unknown File",
            url="https://example.com/unknown.xyz",
            media_url="https://example.com/unknown.xyz",
            author="testuser",
            subreddit="testsubreddit",
            date_iso="2024-01-01",
            is_video=False
        )
        
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'image_format_conversion': True
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = unknown_file
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(unknown_post, temp_dir, config)
            
            assert result.success is True
            # Should skip processing for unknown type
            assert len(result.files_created) == 1  # Only original file
    
    @pytest.mark.skipif(not PROCESSING_AVAILABLE, reason="Processing not available")
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_multiple_processing_operations(self, handler, sample_image, sample_post_metadata, temp_dir):
        """Test multiple processing operations applied together."""
        config = {
            'sleep_interval': 0.1,
            'processing': {
                'enabled': True,
                'generate_thumbnails': True,
                'thumbnail_size': 128,
                'max_image_resolution': 500,
                'preserve_original_metadata': True
            }
        }
        
        # Mock the downloader to simulate successful download
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.return_value = sample_image
            mock_downloader.embedder = None
            mock_get_downloader.return_value = mock_downloader
            
            result = await handler.process(sample_post_metadata, temp_dir, config)
            
            assert result.success is True
            # Should have multiple operations
            assert 'thumbnail_generation' in result.operations_performed
            assert 'image_resize' in result.operations_performed
            # Should have multiple processed files
            assert len(result.files_created) >= 2