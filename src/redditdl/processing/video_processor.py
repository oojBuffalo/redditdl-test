"""
Video Processing Module

FFmpeg-based video processing for format conversion, quality adjustment,
resolution limiting, and thumbnail extraction with metadata preservation.
"""

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

try:
    import ffmpeg
    FFMPEG_AVAILABLE = True
except ImportError:
    ffmpeg = None
    FFMPEG_AVAILABLE = False

from redditdl.processing.exceptions import VideoProcessingError, FFmpegNotFoundError, UnsupportedFormatError


class VideoProcessor:
    """
    Comprehensive video processing using FFmpeg.
    
    Provides video format conversion, quality adjustment, resolution limiting,
    and thumbnail extraction while preserving video metadata when possible.
    """
    
    # Supported video formats
    SUPPORTED_FORMATS = {
        'mp4', 'avi', 'mkv', 'webm', 'mov', 'flv', 'wmv', 'ogv'
    }
    
    # Formats that support quality (CRF) settings
    QUALITY_FORMATS = {'mp4', 'mkv', 'webm', 'mov'}
    
    # Video codec mapping
    CODEC_MAP = {
        'mp4': 'libx264',
        'webm': 'libvpx-vp9',
        'mkv': 'libx264',
        'avi': 'libx264',
        'mov': 'libx264'
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the video processor.
        
        Args:
            config: Processing configuration options
            
        Raises:
            FFmpegNotFoundError: If FFmpeg is not available
        """
        self.config = config or {}
        self.logger = logging.getLogger("redditdl.processing.video")
        
        # Check FFmpeg availability
        if not self._check_ffmpeg_available():
            raise FFmpegNotFoundError()
        
        # Set default values from config
        self.default_crf = self.config.get('video_quality_crf', 23)
        self.preserve_metadata = self.config.get('preserve_original_metadata', True)
        
    def convert_format(
        self,
        input_path: Path,
        output_path: Path,
        target_format: str,
        quality_crf: Optional[int] = None,
        preserve_metadata: bool = True
    ) -> Path:
        """
        Convert video to a different format.
        
        Args:
            input_path: Path to input video file
            output_path: Path for output video file
            target_format: Target format (mp4, avi, mkv, webm, mov)
            quality_crf: CRF quality setting (0-51, lower = better quality)
            preserve_metadata: Whether to preserve video metadata
            
        Returns:
            Path to the converted video file
            
        Raises:
            VideoProcessingError: If conversion fails
            UnsupportedFormatError: If format is not supported
        """
        target_format = target_format.lower()
        if target_format not in self.SUPPORTED_FORMATS:
            raise UnsupportedFormatError(target_format, self.SUPPORTED_FORMATS)
        
        if quality_crf is None:
            quality_crf = self.default_crf
            
        self.logger.info(f"Converting {input_path} to {target_format} format")
        
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build FFmpeg command
            input_stream = ffmpeg.input(str(input_path))
            output_kwargs = self._build_output_kwargs(target_format, quality_crf, preserve_metadata)
            
            # Create output stream
            output_stream = ffmpeg.output(input_stream, str(output_path), **output_kwargs)
            
            # Run FFmpeg conversion
            self._run_ffmpeg(output_stream, input_path, output_path)
            
            self.logger.info(f"Successfully converted video to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Video conversion failed: {e}")
            raise VideoProcessingError(f"Failed to convert video: {e}") from e
    
    def adjust_quality(
        self,
        input_path: Path,
        output_path: Path,
        quality_crf: int,
        preserve_metadata: bool = True
    ) -> Path:
        """
        Adjust video quality (re-encode with different CRF).
        
        Args:
            input_path: Path to input video file
            output_path: Path for output video file
            quality_crf: CRF quality setting (0-51, lower = better quality)
            preserve_metadata: Whether to preserve video metadata
            
        Returns:
            Path to the quality-adjusted video file
            
        Raises:
            VideoProcessingError: If quality adjustment fails
        """
        self.logger.info(f"Adjusting quality of {input_path} to CRF {quality_crf}")
        
        try:
            # Determine output format from file extension or preserve original
            output_format = output_path.suffix.lower()[1:] or 'mp4'
            if output_format not in self.SUPPORTED_FORMATS:
                output_format = 'mp4'  # Default to MP4
            
            return self.convert_format(
                input_path, output_path, output_format,
                quality_crf, preserve_metadata
            )
            
        except Exception as e:
            self.logger.error(f"Quality adjustment failed: {e}")
            raise VideoProcessingError(f"Failed to adjust video quality: {e}") from e
    
    def limit_resolution(
        self,
        input_path: Path,
        output_path: Path,
        max_resolution: str,
        preserve_aspect_ratio: bool = True,
        preserve_metadata: bool = True
    ) -> Path:
        """
        Limit video resolution while preserving aspect ratio.
        
        Args:
            input_path: Path to input video file
            output_path: Path for output video file
            max_resolution: Maximum resolution (e.g., '1920x1080', '720p', '480p')
            preserve_aspect_ratio: Whether to preserve aspect ratio
            preserve_metadata: Whether to preserve video metadata
            
        Returns:
            Path to the resized video file
            
        Raises:
            VideoProcessingError: If resolution limiting fails
        """
        self.logger.info(f"Limiting resolution of {input_path} to {max_resolution}")
        
        try:
            # Parse resolution
            width, height = self._parse_resolution(max_resolution)
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build scale filter
            if preserve_aspect_ratio:
                # Scale to fit within bounds while preserving aspect ratio
                scale_filter = f"scale='min({width},iw)':'min({height},ih)':force_original_aspect_ratio=decrease"
            else:
                # Scale to exact dimensions
                scale_filter = f"scale={width}:{height}"
            
            # Build FFmpeg command
            input_stream = ffmpeg.input(str(input_path))
            
            # Determine output format
            output_format = output_path.suffix.lower()[1:] or 'mp4'
            output_kwargs = self._build_output_kwargs(output_format, self.default_crf, preserve_metadata)
            
            # Apply scale filter and create output
            output_stream = ffmpeg.output(
                input_stream.video.filter('scale', **self._parse_scale_args(scale_filter)),
                input_stream.audio,
                str(output_path),
                **output_kwargs
            )
            
            # Run FFmpeg
            self._run_ffmpeg(output_stream, input_path, output_path)
            
            self.logger.info(f"Successfully limited resolution to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Resolution limiting failed: {e}")
            raise VideoProcessingError(f"Failed to limit video resolution: {e}") from e
    
    def extract_thumbnail(
        self,
        input_path: Path,
        output_path: Path,
        timestamp: str = "00:00:01",
        thumbnail_size: Optional[str] = None
    ) -> Path:
        """
        Extract thumbnail image from video.
        
        Args:
            input_path: Path to input video file
            output_path: Path for thumbnail image file
            timestamp: Timestamp to extract thumbnail from (e.g., '00:00:01')
            thumbnail_size: Thumbnail size (e.g., '256x256')
            
        Returns:
            Path to the thumbnail file
            
        Raises:
            VideoProcessingError: If thumbnail extraction fails
        """
        self.logger.info(f"Extracting thumbnail from {input_path} at {timestamp}")
        
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build FFmpeg command for thumbnail extraction
            input_stream = ffmpeg.input(str(input_path), ss=timestamp)
            output_kwargs = {
                'vframes': 1,  # Extract only one frame
                'format': 'image2',
                'loglevel': 'error'
            }
            
            # Add thumbnail size if specified
            if thumbnail_size:
                width, height = self._parse_resolution(thumbnail_size)
                input_stream = input_stream.filter('scale', width, height)
            
            # Create output stream
            output_stream = ffmpeg.output(input_stream, str(output_path), **output_kwargs)
            
            # Run FFmpeg
            self._run_ffmpeg(output_stream, input_path, output_path)
            
            self.logger.info(f"Successfully extracted thumbnail: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Thumbnail extraction failed: {e}")
            raise VideoProcessingError(f"Failed to extract video thumbnail: {e}") from e
    
    def get_video_info(self, input_path: Path) -> Dict[str, Any]:
        """
        Get detailed information about a video file.
        
        Args:
            input_path: Path to video file
            
        Returns:
            Dictionary with video information
            
        Raises:
            VideoProcessingError: If video cannot be read
        """
        try:
            # Use ffprobe to get video information
            probe = ffmpeg.probe(str(input_path))
            
            # Extract video stream information
            video_stream = next(
                (stream for stream in probe['streams'] if stream['codec_type'] == 'video'),
                None
            )
            
            # Extract audio stream information
            audio_stream = next(
                (stream for stream in probe['streams'] if stream['codec_type'] == 'audio'),
                None
            )
            
            info = {
                'format': probe['format']['format_name'],
                'duration': float(probe['format'].get('duration', 0)),
                'size': int(probe['format'].get('size', 0)),
                'bit_rate': int(probe['format'].get('bit_rate', 0)),
                'filename': input_path.name,
                'file_size': input_path.stat().st_size if input_path.exists() else 0
            }
            
            # Add video stream info
            if video_stream:
                info.update({
                    'video_codec': video_stream.get('codec_name'),
                    'width': int(video_stream.get('width', 0)),
                    'height': int(video_stream.get('height', 0)),
                    'fps': eval(video_stream.get('r_frame_rate', '0/1')),
                    'video_bit_rate': int(video_stream.get('bit_rate', 0))
                })
            
            # Add audio stream info
            if audio_stream:
                info.update({
                    'audio_codec': audio_stream.get('codec_name'),
                    'sample_rate': int(audio_stream.get('sample_rate', 0)),
                    'channels': int(audio_stream.get('channels', 0)),
                    'audio_bit_rate': int(audio_stream.get('bit_rate', 0))
                })
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get video info: {e}")
            raise VideoProcessingError(f"Failed to read video information: {e}") from e
    
    def _check_ffmpeg_available(self) -> bool:
        """
        Check if FFmpeg is available.
        
        Returns:
            True if FFmpeg is available, False otherwise
        """
        # Check if ffmpeg-python is installed
        if not FFMPEG_AVAILABLE:
            return False
        
        # Check if FFmpeg executable is available
        try:
            result = subprocess.run(
                ['ffmpeg', '-version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False
    
    def _build_output_kwargs(self, target_format: str, quality_crf: int, preserve_metadata: bool) -> Dict[str, Any]:
        """
        Build FFmpeg output keyword arguments.
        
        Args:
            target_format: Target video format
            quality_crf: CRF quality setting
            preserve_metadata: Whether to preserve metadata
            
        Returns:
            Dictionary of FFmpeg output arguments
        """
        kwargs = {
            'loglevel': 'error'
        }
        
        # Add codec if supported
        if target_format in self.CODEC_MAP:
            kwargs['vcodec'] = self.CODEC_MAP[target_format]
        
        # Add quality settings for supported formats
        if target_format in self.QUALITY_FORMATS:
            kwargs['crf'] = max(0, min(51, quality_crf))
        
        # Preserve metadata if requested
        if preserve_metadata and self.preserve_metadata:
            kwargs['map_metadata'] = 0
        
        return kwargs
    
    def _parse_resolution(self, resolution_str: str) -> Tuple[int, int]:
        """
        Parse resolution string into width and height.
        
        Args:
            resolution_str: Resolution string (e.g., '1920x1080', '720p')
            
        Returns:
            Tuple of (width, height)
        """
        resolution_str = resolution_str.lower()
        
        # Handle common resolution names
        resolution_map = {
            '4k': (3840, 2160),
            '1080p': (1920, 1080),
            '720p': (1280, 720),
            '480p': (854, 480),
            '360p': (640, 360),
            '240p': (426, 240)
        }
        
        if resolution_str in resolution_map:
            return resolution_map[resolution_str]
        
        # Parse WIDTHxHEIGHT format
        if 'x' in resolution_str:
            try:
                width, height = resolution_str.split('x')
                return int(width), int(height)
            except ValueError:
                pass
        
        # Default fallback
        raise ValueError(f"Invalid resolution format: {resolution_str}")
    
    def _parse_scale_args(self, scale_filter: str) -> Dict[str, Any]:
        """
        Parse scale filter into FFmpeg arguments.
        
        Args:
            scale_filter: Scale filter string
            
        Returns:
            Dictionary of scale arguments
        """
        # Extract width and height from scale filter
        # This is a simplified parser for basic scale operations
        if "scale=" in scale_filter:
            scale_part = scale_filter.split("scale=")[1].split(":")[0:2]
            if len(scale_part) == 2:
                return {'width': scale_part[0], 'height': scale_part[1]}
        
        # Fallback to default scale args
        return {'width': -1, 'height': -1}
    
    def _run_ffmpeg(self, output_stream, input_path: Path, output_path: Path) -> None:
        """
        Run FFmpeg command with proper error handling.
        
        Args:
            output_stream: FFmpeg output stream
            input_path: Input file path for error reporting
            output_path: Output file path for cleanup on error
        """
        try:
            # Run the FFmpeg command
            ffmpeg.run(output_stream, overwrite_output=True, quiet=True)
            
            # Verify output file was created
            if not output_path.exists() or output_path.stat().st_size == 0:
                raise VideoProcessingError(f"FFmpeg did not produce valid output file: {output_path}")
            
        except ffmpeg.Error as e:
            # Clean up failed output file
            if output_path.exists():
                try:
                    output_path.unlink()
                except OSError:
                    pass
            
            # Extract error message from FFmpeg stderr
            error_msg = "Unknown FFmpeg error"
            if hasattr(e, 'stderr') and e.stderr:
                error_msg = e.stderr.decode('utf-8')
            
            raise VideoProcessingError(f"FFmpeg error: {error_msg}") from e