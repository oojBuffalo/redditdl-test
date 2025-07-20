"""
Processor Factory

Factory class for automatic processor selection based on content type and format.
Provides centralized processor creation and configuration management.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union, Set, List
from urllib.parse import urlparse

from redditdl.processing.image_processor import ImageProcessor
from redditdl.processing.video_processor import VideoProcessor
from redditdl.processing.exceptions import ProcessingError, UnsupportedFormatError


class ProcessorFactory:
    """
    Factory for creating appropriate processors based on content type and format.
    
    Automatically selects ImageProcessor or VideoProcessor based on file format
    and content type detection. Handles processor configuration and initialization.
    """
    
    # File extension to content type mapping
    IMAGE_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.ico'
    }
    
    VIDEO_EXTENSIONS = {
        '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.ogv'
    }
    
    AUDIO_EXTENSIONS = {
        '.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a'
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the processor factory.
        
        Args:
            config: Global processing configuration
        """
        self.config = config or {}
        self.logger = logging.getLogger("redditdl.processing.factory")
        
        # Cache for processor instances
        self._image_processor: Optional[ImageProcessor] = None
        self._video_processor: Optional[VideoProcessor] = None
    
    def create_processor(
        self, 
        content_type: str, 
        file_path: Optional[Union[str, Path]] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> Union[ImageProcessor, VideoProcessor]:
        """
        Create appropriate processor based on content type and file format.
        
        Args:
            content_type: Content type ('image', 'video', 'audio')
            file_path: Optional file path for format detection
            config: Optional processor-specific configuration
            
        Returns:
            Appropriate processor instance
            
        Raises:
            ProcessingError: If no suitable processor available
            UnsupportedFormatError: If format is not supported
        """
        # Merge configurations
        processor_config = {**self.config, **(config or {})}
        
        # Normalize content type
        content_type = content_type.lower().strip()
        
        # Auto-detect content type from file path if provided
        if file_path and content_type in ['unknown', 'auto']:
            content_type = self.detect_content_type(file_path)
        
        self.logger.debug(f"Creating processor for content type: {content_type}")
        
        if content_type == 'image':
            return self._get_image_processor(processor_config)
        elif content_type in ['video', 'audio']:
            return self._get_video_processor(processor_config)
        else:
            raise ProcessingError(f"Unsupported content type: {content_type}")
    
    def detect_content_type(self, file_path: Union[str, Path]) -> str:
        """
        Detect content type from file path or URL.
        
        Args:
            file_path: File path or URL to analyze
            
        Returns:
            Detected content type ('image', 'video', 'audio', 'unknown')
        """
        original_str = str(file_path).lower()
        
        if isinstance(file_path, str):
            # Handle URLs
            if file_path.startswith(('http://', 'https://')):
                parsed_url = urlparse(file_path)
                file_path = Path(parsed_url.path)
            else:
                file_path = Path(file_path)
        
        # Get file extension
        extension = file_path.suffix.lower()
        
        # Check against known extensions
        if extension in self.IMAGE_EXTENSIONS:
            return 'image'
        elif extension in self.VIDEO_EXTENSIONS:
            return 'video'
        elif extension in self.AUDIO_EXTENSIONS:
            return 'audio'
        
        # Try to detect from URL patterns for common hosts (use original string)
        if any(host in original_str for host in ['i.redd.it', 'imgur.com', 'i.imgur.com']):
            return 'image'
        elif any(host in original_str for host in ['v.redd.it', 'gfycat.com', 'redgifs.com']):
            return 'video'
        
        return 'unknown'
    
    def get_supported_formats(self, content_type: str) -> Set[str]:
        """
        Get supported formats for a given content type.
        
        Args:
            content_type: Content type to check
            
        Returns:
            Set of supported file extensions (without leading dot)
        """
        content_type = content_type.lower()
        
        if content_type == 'image':
            return {ext[1:] for ext in self.IMAGE_EXTENSIONS}
        elif content_type == 'video':
            return {ext[1:] for ext in self.VIDEO_EXTENSIONS}
        elif content_type == 'audio':
            return {ext[1:] for ext in self.AUDIO_EXTENSIONS}
        else:
            return set()
    
    def is_format_supported(self, file_path: Union[str, Path], content_type: str) -> bool:
        """
        Check if a file format is supported for processing.
        
        Args:
            file_path: File path or URL to check
            content_type: Expected content type
            
        Returns:
            True if format is supported, False otherwise
        """
        detected_type = self.detect_content_type(file_path)
        
        # Check if detected type matches expected type
        if detected_type != content_type.lower():
            return False
        
        # Check if format is in supported list
        if isinstance(file_path, str):
            file_path = Path(file_path)
        
        extension = file_path.suffix.lower()
        
        if content_type == 'image':
            return extension in self.IMAGE_EXTENSIONS
        elif content_type in ['video', 'audio']:
            return extension in self.VIDEO_EXTENSIONS or extension in self.AUDIO_EXTENSIONS
        
        return False
    
    def create_processor_for_file(
        self, 
        file_path: Union[str, Path],
        config: Optional[Dict[str, Any]] = None
    ) -> Union[ImageProcessor, VideoProcessor]:
        """
        Create processor based on file format detection.
        
        Args:
            file_path: File path or URL to process
            config: Optional processor-specific configuration
            
        Returns:
            Appropriate processor instance
            
        Raises:
            ProcessingError: If no suitable processor available
            UnsupportedFormatError: If format is not supported
        """
        content_type = self.detect_content_type(file_path)
        
        if content_type == 'unknown':
            raise UnsupportedFormatError(
                str(file_path), 
                list(self.IMAGE_EXTENSIONS | self.VIDEO_EXTENSIONS | self.AUDIO_EXTENSIONS)
            )
        
        return self.create_processor(content_type, file_path, config)
    
    def get_processor_capabilities(self, content_type: str) -> Dict[str, Any]:
        """
        Get capabilities of processor for given content type.
        
        Args:
            content_type: Content type to check
            
        Returns:
            Dictionary describing processor capabilities
        """
        content_type = content_type.lower()
        
        if content_type == 'image':
            return {
                'formats': list(ImageProcessor.SUPPORTED_FORMATS),
                'quality_formats': list(ImageProcessor.QUALITY_FORMATS),
                'exif_formats': list(ImageProcessor.EXIF_FORMATS),
                'operations': [
                    'convert_format',
                    'adjust_quality', 
                    'resize_image',
                    'generate_thumbnail'
                ],
                'metadata_support': True
            }
        elif content_type in ['video', 'audio']:
            return {
                'formats': list(VideoProcessor.SUPPORTED_FORMATS),
                'quality_formats': list(VideoProcessor.QUALITY_FORMATS),
                'codec_map': dict(VideoProcessor.CODEC_MAP),
                'operations': [
                    'convert_format',
                    'adjust_quality',
                    'limit_resolution', 
                    'extract_thumbnail'
                ],
                'metadata_support': True
            }
        else:
            return {
                'formats': [],
                'quality_formats': [],
                'operations': [],
                'metadata_support': False
            }
    
    def validate_processing_config(self, content_type: str, config: Dict[str, Any]) -> List[str]:
        """
        Validate processing configuration for given content type.
        
        Args:
            content_type: Content type to validate for
            config: Configuration to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        content_type = content_type.lower()
        
        # Common validations
        if 'preserve_original_metadata' in config:
            if not isinstance(config['preserve_original_metadata'], bool):
                errors.append("preserve_original_metadata must be a boolean")
        
        # Content-type specific validations
        if content_type == 'image':
            # Image quality validation
            if 'image_quality' in config:
                quality = config['image_quality']
                if not isinstance(quality, int) or not (1 <= quality <= 100):
                    errors.append("image_quality must be an integer between 1 and 100")
            
            # Image format validation
            if 'target_image_format' in config:
                fmt = config['target_image_format'].lower()
                if fmt not in ImageProcessor.SUPPORTED_FORMATS:
                    errors.append(f"Unsupported image format: {fmt}")
        
        elif content_type in ['video', 'audio']:
            # Video quality (CRF) validation
            if 'video_quality_crf' in config:
                crf = config['video_quality_crf']
                if not isinstance(crf, int) or not (0 <= crf <= 51):
                    errors.append("video_quality_crf must be an integer between 0 and 51")
            
            # Video format validation
            if 'target_video_format' in config:
                fmt = config['target_video_format'].lower()
                if fmt not in VideoProcessor.SUPPORTED_FORMATS:
                    errors.append(f"Unsupported video format: {fmt}")
        
        return errors
    
    def _get_image_processor(self, config: Dict[str, Any]) -> ImageProcessor:
        """
        Get or create ImageProcessor instance.
        
        Args:
            config: Processor configuration
            
        Returns:
            ImageProcessor instance
        """
        # Create new processor with current config each time to allow
        # for configuration changes between calls
        return ImageProcessor(config)
    
    def _get_video_processor(self, config: Dict[str, Any]) -> VideoProcessor:
        """
        Get or create VideoProcessor instance.
        
        Args:
            config: Processor configuration
            
        Returns:
            VideoProcessor instance
            
        Raises:
            ProcessingError: If VideoProcessor cannot be created
        """
        try:
            # Create new processor with current config each time
            return VideoProcessor(config)
        except Exception as e:
            raise ProcessingError(f"Failed to create video processor: {e}") from e
    
    def clear_cache(self) -> None:
        """Clear cached processor instances."""
        self._image_processor = None
        self._video_processor = None
        self.logger.debug("Cleared processor cache")