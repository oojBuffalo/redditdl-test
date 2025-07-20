"""
Tests for ProcessorFactory class.

Comprehensive test suite for processor factory functionality including
automatic processor selection, content type detection, and configuration validation.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

from redditdl.processing.factory import ProcessorFactory
from redditdl.processing.image_processor import ImageProcessor
from redditdl.processing.video_processor import VideoProcessor
from redditdl.processing.exceptions import ProcessingError, UnsupportedFormatError


class TestProcessorFactory:
    """Test cases for ProcessorFactory functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def factory(self):
        """Create ProcessorFactory instance for testing."""
        return ProcessorFactory()
    
    @pytest.fixture
    def factory_with_config(self):
        """Create ProcessorFactory with custom configuration."""
        config = {
            'image_quality': 70,
            'video_quality_crf': 28,
            'preserve_original_metadata': False
        }
        return ProcessorFactory(config)
    
    def test_init_default_config(self):
        """Test ProcessorFactory initialization with default config."""
        factory = ProcessorFactory()
        assert factory.config == {}
    
    def test_init_custom_config(self):
        """Test ProcessorFactory initialization with custom config."""
        config = {'test_key': 'test_value'}
        factory = ProcessorFactory(config)
        assert factory.config == config
    
    def test_detect_content_type_image_extensions(self, factory):
        """Test content type detection for image file extensions."""
        assert factory.detect_content_type("image.jpg") == 'image'
        assert factory.detect_content_type("photo.jpeg") == 'image'
        assert factory.detect_content_type("picture.png") == 'image'
        assert factory.detect_content_type("graphic.gif") == 'image'
        assert factory.detect_content_type("bitmap.bmp") == 'image'
        assert factory.detect_content_type("document.tiff") == 'image'
        assert factory.detect_content_type("web.webp") == 'image'
    
    def test_detect_content_type_video_extensions(self, factory):
        """Test content type detection for video file extensions."""
        assert factory.detect_content_type("movie.mp4") == 'video'
        assert factory.detect_content_type("clip.avi") == 'video'
        assert factory.detect_content_type("film.mkv") == 'video'
        assert factory.detect_content_type("stream.webm") == 'video'
        assert factory.detect_content_type("video.mov") == 'video'
        assert factory.detect_content_type("media.flv") == 'video'
    
    def test_detect_content_type_audio_extensions(self, factory):
        """Test content type detection for audio file extensions."""
        assert factory.detect_content_type("song.mp3") == 'audio'
        assert factory.detect_content_type("track.wav") == 'audio'
        assert factory.detect_content_type("music.flac") == 'audio'
        assert factory.detect_content_type("audio.aac") == 'audio'
        assert factory.detect_content_type("sound.ogg") == 'audio'
    
    def test_detect_content_type_urls(self, factory):
        """Test content type detection for URLs."""
        assert factory.detect_content_type("https://i.redd.it/abc123.jpg") == 'image'
        assert factory.detect_content_type("https://v.redd.it/def456/DASH_720.mp4") == 'video'
        assert factory.detect_content_type("https://imgur.com/gallery/xyz789.png") == 'image'
        assert factory.detect_content_type("https://gfycat.com/video123") == 'video'
    
    def test_detect_content_type_unknown(self, factory):
        """Test content type detection for unknown formats."""
        assert factory.detect_content_type("document.txt") == 'unknown'
        assert factory.detect_content_type("archive.zip") == 'unknown'
        assert factory.detect_content_type("data.json") == 'unknown'
    
    def test_detect_content_type_path_object(self, factory, temp_dir):
        """Test content type detection with Path objects."""
        image_path = temp_dir / "test.jpg"
        video_path = temp_dir / "test.mp4"
        
        assert factory.detect_content_type(image_path) == 'image'
        assert factory.detect_content_type(video_path) == 'video'
    
    def test_create_processor_image(self, factory):
        """Test creating image processor."""
        processor = factory.create_processor('image')
        assert isinstance(processor, ImageProcessor)
    
    @patch('processing.factory.VideoProcessor')
    def test_create_processor_video(self, mock_video_processor, factory):
        """Test creating video processor."""
        mock_instance = Mock()
        mock_video_processor.return_value = mock_instance
        
        processor = factory.create_processor('video')
        assert processor == mock_instance
        mock_video_processor.assert_called_once()
    
    @patch('processing.factory.VideoProcessor')
    def test_create_processor_audio(self, mock_video_processor, factory):
        """Test creating processor for audio (uses video processor)."""
        mock_instance = Mock()
        mock_video_processor.return_value = mock_instance
        
        processor = factory.create_processor('audio')
        assert processor == mock_instance
        mock_video_processor.assert_called_once()
    
    def test_create_processor_unsupported(self, factory):
        """Test creating processor for unsupported content type."""
        with pytest.raises(ProcessingError):
            factory.create_processor('unsupported')
    
    def test_create_processor_with_config(self, factory_with_config):
        """Test creating processor with custom configuration."""
        processor = factory_with_config.create_processor('image')
        assert isinstance(processor, ImageProcessor)
        assert processor.default_quality == 70
        assert processor.preserve_exif is False
    
    def test_create_processor_for_file_image(self, factory, temp_dir):
        """Test creating processor based on file detection."""
        image_file = temp_dir / "test.jpg"
        image_file.write_bytes(b"fake image")
        
        processor = factory.create_processor_for_file(image_file)
        assert isinstance(processor, ImageProcessor)
    
    @patch('processing.factory.VideoProcessor')
    def test_create_processor_for_file_video(self, mock_video_processor, factory, temp_dir):
        """Test creating processor for video file."""
        mock_instance = Mock()
        mock_video_processor.return_value = mock_instance
        
        video_file = temp_dir / "test.mp4"
        video_file.write_bytes(b"fake video")
        
        processor = factory.create_processor_for_file(video_file)
        assert processor == mock_instance
    
    def test_create_processor_for_file_unknown(self, factory, temp_dir):
        """Test creating processor for unknown file type."""
        unknown_file = temp_dir / "test.xyz"
        unknown_file.write_bytes(b"unknown content")
        
        with pytest.raises(UnsupportedFormatError):
            factory.create_processor_for_file(unknown_file)
    
    def test_get_supported_formats_image(self, factory):
        """Test getting supported formats for images."""
        formats = factory.get_supported_formats('image')
        assert 'jpg' in formats
        assert 'png' in formats
        assert 'gif' in formats
        assert 'webp' in formats
    
    def test_get_supported_formats_video(self, factory):
        """Test getting supported formats for videos."""
        formats = factory.get_supported_formats('video')
        assert 'mp4' in formats
        assert 'avi' in formats
        assert 'mkv' in formats
        assert 'webm' in formats
    
    def test_get_supported_formats_unknown(self, factory):
        """Test getting supported formats for unknown content type."""
        formats = factory.get_supported_formats('unknown')
        assert formats == set()
    
    def test_is_format_supported_image(self, factory):
        """Test format support checking for images."""
        assert factory.is_format_supported("test.jpg", "image") is True
        assert factory.is_format_supported("test.png", "image") is True
        assert factory.is_format_supported("test.xyz", "image") is False
    
    def test_is_format_supported_video(self, factory):
        """Test format support checking for videos."""
        assert factory.is_format_supported("test.mp4", "video") is True
        assert factory.is_format_supported("test.avi", "video") is True
        assert factory.is_format_supported("test.xyz", "video") is False
    
    def test_is_format_supported_type_mismatch(self, factory):
        """Test format support with content type mismatch."""
        # Image file but expecting video
        assert factory.is_format_supported("test.jpg", "video") is False
        # Video file but expecting image
        assert factory.is_format_supported("test.mp4", "image") is False
    
    def test_get_processor_capabilities_image(self, factory):
        """Test getting image processor capabilities."""
        caps = factory.get_processor_capabilities('image')
        
        assert 'formats' in caps
        assert 'operations' in caps
        assert 'convert_format' in caps['operations']
        assert 'adjust_quality' in caps['operations']
        assert 'resize_image' in caps['operations']
        assert 'generate_thumbnail' in caps['operations']
        assert caps['metadata_support'] is True
    
    def test_get_processor_capabilities_video(self, factory):
        """Test getting video processor capabilities."""
        caps = factory.get_processor_capabilities('video')
        
        assert 'formats' in caps
        assert 'operations' in caps
        assert 'convert_format' in caps['operations']
        assert 'adjust_quality' in caps['operations']
        assert 'limit_resolution' in caps['operations']
        assert 'extract_thumbnail' in caps['operations']
        assert caps['metadata_support'] is True
    
    def test_get_processor_capabilities_unknown(self, factory):
        """Test getting capabilities for unknown content type."""
        caps = factory.get_processor_capabilities('unknown')
        
        assert caps['formats'] == []
        assert caps['operations'] == []
        assert caps['metadata_support'] is False
    
    def test_validate_processing_config_image_valid(self, factory):
        """Test validating valid image processing configuration."""
        config = {
            'image_quality': 85,
            'target_image_format': 'jpeg',
            'preserve_original_metadata': True
        }
        
        errors = factory.validate_processing_config('image', config)
        assert errors == []
    
    def test_validate_processing_config_image_invalid_quality(self, factory):
        """Test validating invalid image quality."""
        config = {'image_quality': 150}  # Out of range
        
        errors = factory.validate_processing_config('image', config)
        assert len(errors) > 0
        assert any('image_quality' in error for error in errors)
    
    def test_validate_processing_config_image_invalid_format(self, factory):
        """Test validating invalid image format."""
        config = {'target_image_format': 'unsupported'}
        
        errors = factory.validate_processing_config('image', config)
        assert len(errors) > 0
        assert any('Unsupported image format' in error for error in errors)
    
    def test_validate_processing_config_video_valid(self, factory):
        """Test validating valid video processing configuration."""
        config = {
            'video_quality_crf': 23,
            'target_video_format': 'mp4',
            'preserve_original_metadata': True
        }
        
        errors = factory.validate_processing_config('video', config)
        assert errors == []
    
    def test_validate_processing_config_video_invalid_crf(self, factory):
        """Test validating invalid video CRF."""
        config = {'video_quality_crf': 60}  # Out of range
        
        errors = factory.validate_processing_config('video', config)
        assert len(errors) > 0
        assert any('video_quality_crf' in error for error in errors)
    
    def test_validate_processing_config_video_invalid_format(self, factory):
        """Test validating invalid video format."""
        config = {'target_video_format': 'unsupported'}
        
        errors = factory.validate_processing_config('video', config)
        assert len(errors) > 0
        assert any('Unsupported video format' in error for error in errors)
    
    def test_validate_processing_config_metadata_invalid(self, factory):
        """Test validating invalid metadata preservation setting."""
        config = {'preserve_original_metadata': 'not_a_boolean'}
        
        errors = factory.validate_processing_config('image', config)
        assert len(errors) > 0
        assert any('preserve_original_metadata' in error for error in errors)
    
    def test_clear_cache(self, factory):
        """Test clearing processor cache."""
        # Create some processors to cache
        factory.create_processor('image')
        
        # Clear cache
        factory.clear_cache()
        
        # Verify cache is cleared (no direct way to check, but method should not error)
        assert True  # Method completed successfully
    
    def test_extension_constants(self):
        """Test that file extension constants are properly defined."""
        factory = ProcessorFactory()
        
        # Check image extensions
        assert '.jpg' in factory.IMAGE_EXTENSIONS
        assert '.png' in factory.IMAGE_EXTENSIONS
        assert '.gif' in factory.IMAGE_EXTENSIONS
        
        # Check video extensions
        assert '.mp4' in factory.VIDEO_EXTENSIONS
        assert '.avi' in factory.VIDEO_EXTENSIONS
        assert '.mkv' in factory.VIDEO_EXTENSIONS
        
        # Check audio extensions
        assert '.mp3' in factory.AUDIO_EXTENSIONS
        assert '.wav' in factory.AUDIO_EXTENSIONS
        assert '.flac' in factory.AUDIO_EXTENSIONS
    
    def test_config_merging(self, factory):
        """Test configuration merging between factory and processor configs."""
        factory_config = {'image_quality': 70}
        factory = ProcessorFactory(factory_config)
        
        processor_config = {'preserve_original_metadata': False}
        
        processor = factory.create_processor('image', config=processor_config)
        
        # Both configs should be applied
        assert processor.default_quality == 70  # From factory config
        assert processor.preserve_exif is False  # From processor config