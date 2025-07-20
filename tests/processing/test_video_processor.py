"""
Tests for VideoProcessor class.

Comprehensive test suite for video processing functionality including format conversion,
quality adjustment, resolution limiting, and thumbnail extraction.

Note: These tests mock FFmpeg operations to avoid requiring actual FFmpeg installation
during testing, while still validating the processor logic.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock

from redditdl.processing.video_processor import VideoProcessor
from redditdl.processing.exceptions import VideoProcessingError, FFmpegNotFoundError, UnsupportedFormatError


class TestVideoProcessor:
    """Test cases for VideoProcessor functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def sample_video_path(self, temp_dir):
        """Create a fake video file for testing."""
        video_path = temp_dir / "test_video.mp4"
        video_path.write_bytes(b"fake video content")
        return video_path
    
    @pytest.fixture
    def mock_ffmpeg_available(self):
        """Mock FFmpeg availability check."""
        with patch('redditdl.processing.video_processor.VideoProcessor._check_ffmpeg_available', return_value=True):
            yield
    
    @pytest.fixture
    def processor(self, mock_ffmpeg_available):
        """Create VideoProcessor instance for testing."""
        return VideoProcessor()
    
    def test_init_default_config(self, mock_ffmpeg_available):
        """Test VideoProcessor initialization with default config."""
        processor = VideoProcessor()
        assert processor.default_crf == 23
        assert processor.preserve_metadata is True
    
    def test_init_custom_config(self, mock_ffmpeg_available):
        """Test VideoProcessor initialization with custom config."""
        config = {
            'video_quality_crf': 18,
            'preserve_original_metadata': False
        }
        processor = VideoProcessor(config)
        assert processor.default_crf == 18
        assert processor.preserve_metadata is False
    
    def test_init_ffmpeg_not_available(self):
        """Test VideoProcessor initialization when FFmpeg is not available."""
        with patch('redditdl.processing.video_processor.VideoProcessor._check_ffmpeg_available', return_value=False):
            with pytest.raises(FFmpegNotFoundError):
                VideoProcessor()
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_convert_format_mp4_to_avi(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test converting MP4 to AVI format."""
        output_path = temp_dir / "converted.avi"
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_output = Mock()
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Create fake output file
        output_path.write_bytes(b"fake converted video")
        
        result = processor.convert_format(
            sample_video_path, output_path, 'avi'
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify FFmpeg was called correctly
        mock_ffmpeg.input.assert_called_once_with(str(sample_video_path))
        mock_ffmpeg.output.assert_called_once()
        mock_ffmpeg.run.assert_called_once()
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_convert_format_with_quality(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test format conversion with specific CRF quality setting."""
        output_path = temp_dir / "converted_quality.mp4"
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_output = Mock()
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Create fake output file
        output_path.write_bytes(b"fake converted video")
        
        result = processor.convert_format(
            sample_video_path, output_path, 'mp4', quality_crf=18
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify FFmpeg was called with quality settings
        mock_ffmpeg.output.assert_called_once()
        call_args = mock_ffmpeg.output.call_args
        assert 'crf' in call_args[1]
        assert call_args[1]['crf'] == 18
    
    def test_convert_format_unsupported(self, processor, sample_video_path, temp_dir):
        """Test error handling for unsupported format."""
        output_path = temp_dir / "converted.xyz"
        
        with pytest.raises(UnsupportedFormatError):
            processor.convert_format(sample_video_path, output_path, 'xyz')
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_adjust_quality(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test quality adjustment functionality."""
        output_path = temp_dir / "quality_adjusted.mp4"
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_output = Mock()
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Create fake output file
        output_path.write_bytes(b"fake quality adjusted video")
        
        result = processor.adjust_quality(
            sample_video_path, output_path, quality_crf=28
        )
        
        assert result == output_path
        assert output_path.exists()
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_limit_resolution(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test video resolution limiting functionality."""
        output_path = temp_dir / "resized.mp4"
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_video = Mock()
        mock_audio = Mock()
        mock_filter = Mock()
        mock_output = Mock()
        
        mock_input.video = mock_video
        mock_input.audio = mock_audio
        mock_video.filter.return_value = mock_filter
        
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Create fake output file
        output_path.write_bytes(b"fake resized video")
        
        result = processor.limit_resolution(
            sample_video_path, output_path, "1280x720"
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify scale filter was applied
        mock_video.filter.assert_called_once()
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_extract_thumbnail(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test video thumbnail extraction."""
        output_path = temp_dir / "thumbnail.jpg"
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_filter = Mock()
        mock_output = Mock()
        
        mock_input.filter.return_value = mock_filter
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Create fake output file
        output_path.write_bytes(b"fake thumbnail image")
        
        result = processor.extract_thumbnail(
            sample_video_path, output_path, timestamp="00:00:05"
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify FFmpeg was called with correct timestamp
        mock_ffmpeg.input.assert_called_once_with(str(sample_video_path), ss="00:00:05")
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_extract_thumbnail_with_size(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test video thumbnail extraction with specific size."""
        output_path = temp_dir / "thumbnail_sized.jpg"
        
        # Mock FFmpeg operations
        mock_input = Mock()
        mock_filter = Mock()
        mock_output = Mock()
        
        mock_input.filter.return_value = mock_filter
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Create fake output file
        output_path.write_bytes(b"fake sized thumbnail")
        
        result = processor.extract_thumbnail(
            sample_video_path, output_path, 
            timestamp="00:00:02", thumbnail_size="256x256"
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify filter was applied for sizing
        mock_input.filter.assert_called_once_with('scale', 256, 256)
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_get_video_info(self, mock_ffmpeg, processor, sample_video_path):
        """Test getting video information."""
        # Mock ffprobe response
        mock_probe_data = {
            'format': {
                'format_name': 'mov,mp4,m4a,3gp,3g2,mj2',
                'duration': '10.5',
                'size': '1024000',
                'bit_rate': '500000'
            },
            'streams': [
                {
                    'codec_type': 'video',
                    'codec_name': 'h264',
                    'width': 1920,
                    'height': 1080,
                    'r_frame_rate': '30/1',
                    'bit_rate': '400000'
                },
                {
                    'codec_type': 'audio',
                    'codec_name': 'aac',
                    'sample_rate': '44100',
                    'channels': 2,
                    'bit_rate': '100000'
                }
            ]
        }
        
        mock_ffmpeg.probe.return_value = mock_probe_data
        
        info = processor.get_video_info(sample_video_path)
        
        assert info['format'] == 'mov,mp4,m4a,3gp,3g2,mj2'
        assert info['duration'] == 10.5
        assert info['width'] == 1920
        assert info['height'] == 1080
        assert info['fps'] == 30.0
        assert info['video_codec'] == 'h264'
        assert info['audio_codec'] == 'aac'
        assert info['filename'] == 'test_video.mp4'
    
    def test_parse_resolution_common_formats(self, processor):
        """Test parsing common resolution formats."""
        assert processor._parse_resolution('1080p') == (1920, 1080)
        assert processor._parse_resolution('720p') == (1280, 720)
        assert processor._parse_resolution('480p') == (854, 480)
        assert processor._parse_resolution('4k') == (3840, 2160)
    
    def test_parse_resolution_custom_format(self, processor):
        """Test parsing custom resolution format."""
        assert processor._parse_resolution('1600x900') == (1600, 900)
        assert processor._parse_resolution('800x600') == (800, 600)
    
    def test_parse_resolution_invalid(self, processor):
        """Test parsing invalid resolution format."""
        with pytest.raises(ValueError):
            processor._parse_resolution('invalid')
        
        with pytest.raises(ValueError):
            processor._parse_resolution('1920xabc')
    
    def test_build_output_kwargs_mp4(self, processor):
        """Test building FFmpeg output kwargs for MP4."""
        kwargs = processor._build_output_kwargs('mp4', 23, True)
        
        assert kwargs['vcodec'] == 'libx264'
        assert kwargs['crf'] == 23
        assert kwargs['map_metadata'] == 0
        assert kwargs['loglevel'] == 'error'
    
    def test_build_output_kwargs_webm(self, processor):
        """Test building FFmpeg output kwargs for WebM."""
        kwargs = processor._build_output_kwargs('webm', 28, False)
        
        assert kwargs['vcodec'] == 'libvpx-vp9'
        assert kwargs['crf'] == 28
        assert 'map_metadata' not in kwargs
    
    def test_build_output_kwargs_quality_bounds(self, processor):
        """Test CRF quality bounds checking."""
        # Test below minimum
        kwargs = processor._build_output_kwargs('mp4', -5, True)
        assert kwargs['crf'] == 0
        
        # Test above maximum
        kwargs = processor._build_output_kwargs('mp4', 60, True)
        assert kwargs['crf'] == 51
    
    @patch('redditdl.processing.video_processor.subprocess.run')
    def test_check_ffmpeg_available_success(self, mock_run):
        """Test successful FFmpeg availability check."""
        mock_run.return_value.returncode = 0
        
        processor = VideoProcessor.__new__(VideoProcessor)  # Skip __init__
        result = processor._check_ffmpeg_available()
        
        assert result is True
        mock_run.assert_called_once_with(
            ['ffmpeg', '-version'], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
    
    @patch('redditdl.processing.video_processor.subprocess.run')
    def test_check_ffmpeg_available_failure(self, mock_run):
        """Test failed FFmpeg availability check."""
        mock_run.side_effect = FileNotFoundError()
        
        processor = VideoProcessor.__new__(VideoProcessor)  # Skip __init__
        result = processor._check_ffmpeg_available()
        
        assert result is False
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_error_handling_ffmpeg_error(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test error handling for FFmpeg execution errors."""
        output_path = temp_dir / "output.mp4"
        
        # Mock FFmpeg operations to raise error
        mock_input = Mock()
        mock_output = Mock()
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        
        # Create FFmpeg error
        error = Mock()
        error.stderr = b"FFmpeg error message"
        mock_ffmpeg.Error = Exception
        mock_ffmpeg.run.side_effect = error
        
        with pytest.raises(VideoProcessingError):
            processor.convert_format(sample_video_path, output_path, 'mp4')
    
    @patch('redditdl.processing.video_processor.ffmpeg')
    def test_error_handling_no_output_file(self, mock_ffmpeg, processor, sample_video_path, temp_dir):
        """Test error handling when no output file is created."""
        output_path = temp_dir / "output.mp4"
        
        # Mock FFmpeg operations to succeed but not create file
        mock_input = Mock()
        mock_output = Mock()
        mock_ffmpeg.input.return_value = mock_input
        mock_ffmpeg.output.return_value = mock_output
        mock_ffmpeg.run.return_value = None
        
        # Don't create output file
        
        with pytest.raises(VideoProcessingError):
            processor.convert_format(sample_video_path, output_path, 'mp4')
    
    def test_output_directory_creation(self, processor, sample_video_path, temp_dir):
        """Test automatic output directory creation."""
        with patch('redditdl.processing.video_processor.ffmpeg') as mock_ffmpeg:
            nested_dir = temp_dir / "nested" / "directory"
            output_path = nested_dir / "output.mp4"
            
            # Mock FFmpeg operations
            mock_input = Mock()
            mock_output = Mock()
            mock_ffmpeg.input.return_value = mock_input
            mock_ffmpeg.output.return_value = mock_output
            mock_ffmpeg.run.return_value = None
            
            # Create fake output file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"fake video")
            
            result = processor.convert_format(
                sample_video_path, output_path, 'mp4'
            )
            
            assert result == output_path
            assert output_path.exists()
            assert nested_dir.exists()
    
    def test_supported_formats_constant(self):
        """Test that supported formats constant is properly defined."""
        assert 'mp4' in VideoProcessor.SUPPORTED_FORMATS
        assert 'avi' in VideoProcessor.SUPPORTED_FORMATS
        assert 'mkv' in VideoProcessor.SUPPORTED_FORMATS
        assert 'webm' in VideoProcessor.SUPPORTED_FORMATS
        assert 'mov' in VideoProcessor.SUPPORTED_FORMATS
    
    def test_quality_formats_constant(self):
        """Test that quality formats constant is properly defined."""
        assert 'mp4' in VideoProcessor.QUALITY_FORMATS
        assert 'mkv' in VideoProcessor.QUALITY_FORMATS
        assert 'webm' in VideoProcessor.QUALITY_FORMATS
        assert 'mov' in VideoProcessor.QUALITY_FORMATS
    
    def test_codec_map_constant(self):
        """Test that codec map constant is properly defined."""
        assert VideoProcessor.CODEC_MAP['mp4'] == 'libx264'
        assert VideoProcessor.CODEC_MAP['webm'] == 'libvpx-vp9'
        assert VideoProcessor.CODEC_MAP['mkv'] == 'libx264'