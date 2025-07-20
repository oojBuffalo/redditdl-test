"""
Media Processing Pipeline

Comprehensive media processing capabilities for RedditDL including image and video
conversion, quality adjustment, thumbnail generation, and metadata preservation.

This module provides:
- ImageProcessor: PIL/Pillow-based image processing
- VideoProcessor: FFmpeg-based video processing  
- ProcessorFactory: Automatic processor selection based on content type
- Processing exceptions and error handling

Requirements:
- PIL/Pillow for image processing (installed)
- FFmpeg system dependency for video processing (requires separate installation)
- ffmpeg-python for Python FFmpeg integration

Example usage:
    from processing import ProcessorFactory
    
    # Create processor for an image file
    processor = ProcessorFactory.create_processor('image', config)
    
    # Process the file
    result = await processor.convert_format(
        input_path, output_path, 'jpeg', quality=85
    )
"""

from redditdl.processing.exceptions import (
    ProcessingError,
    ImageProcessingError, 
    VideoProcessingError,
    FFmpegNotFoundError,
    UnsupportedFormatError
)

from redditdl.processing.image_processor import ImageProcessor

try:
    from redditdl.processing.video_processor import VideoProcessor
    VIDEO_PROCESSING_AVAILABLE = True
except ImportError:
    VideoProcessor = None
    VIDEO_PROCESSING_AVAILABLE = False

from redditdl.processing.factory import ProcessorFactory

# General processing availability flag
PROCESSING_AVAILABLE = True

__all__ = [
    'ProcessingError',
    'ImageProcessingError',
    'VideoProcessingError', 
    'FFmpegNotFoundError',
    'UnsupportedFormatError',
    'ImageProcessor',
    'VideoProcessor',
    'ProcessorFactory',
    'VIDEO_PROCESSING_AVAILABLE',
    'PROCESSING_AVAILABLE'
]