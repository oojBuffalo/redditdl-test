#!/usr/bin/env python3
"""
RedditDL Image Format Converter Plugin

An example plugin that demonstrates advanced image processing capabilities
by converting downloaded images between different formats (JPEG, PNG, WEBP)
with quality and optimization settings.

This plugin showcases:
- Advanced content handler implementation
- Configuration-driven processing
- Image format detection and conversion
- Quality optimization
- Error handling and fallback mechanisms
- Integration with existing MediaDownloader

Usage:
    Add to your redditdl configuration:
    
    plugins:
      enabled:
        - image_converter
      
    processing:
      image_converter:
        target_format: "webp"  # jpeg, png, webp
        quality: 85
        optimize: true
        preserve_original: false
        max_width: 1920
        max_height: 1080

Author: RedditDL Plugin Development Kit
License: MIT
Version: 1.0.0
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

try:
    from PIL import Image, ImageOps
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

import pluggy

from content_handlers.base import BaseContentHandler
from core.plugins.hooks import ContentHandlerHooks

logger = logging.getLogger(__name__)

@dataclass
class ImageConverterConfig:
    """Configuration for image converter plugin."""
    target_format: str = "webp"
    quality: int = 85
    optimize: bool = True
    preserve_original: bool = False
    max_width: Optional[int] = 1920
    max_height: Optional[int] = 1080
    progressive: bool = True
    strip_metadata: bool = False

class ImageConverterPlugin(BaseContentHandler):
    """
    Advanced image format converter plugin.
    
    Converts downloaded images to optimized formats with quality control.
    Supports JPEG, PNG, and WEBP formats with various optimization options.
    """
    
    name = "image_converter"
    description = "Converts images to optimized formats with quality control"
    version = "1.0.0"
    priority = 50  # Run after basic media download but before organization
    
    SUPPORTED_INPUT_FORMATS = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tiff', '.gif'}
    SUPPORTED_OUTPUT_FORMATS = {'jpeg', 'png', 'webp'}
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the image converter plugin."""
        super().__init__()
        
        if not PIL_AVAILABLE:
            raise ImportError(
                "PIL/Pillow is required for image conversion. "
                "Install with: pip install Pillow"
            )
        
        # Load configuration
        converter_config = config.get('processing', {}).get('image_converter', {})
        self.config = ImageConverterConfig(**converter_config)
        
        # Validate configuration
        self._validate_config()
        
        logger.info(f"ImageConverter plugin initialized with format: {self.config.target_format}")
    
    def _validate_config(self) -> None:
        """Validate plugin configuration."""
        if self.config.target_format not in self.SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"Unsupported target format: {self.config.target_format}. "
                f"Supported formats: {', '.join(self.SUPPORTED_OUTPUT_FORMATS)}"
            )
        
        if not (1 <= self.config.quality <= 100):
            raise ValueError(f"Quality must be between 1-100, got: {self.config.quality}")
        
        if self.config.max_width and self.config.max_width < 1:
            raise ValueError(f"max_width must be positive, got: {self.config.max_width}")
        
        if self.config.max_height and self.config.max_height < 1:
            raise ValueError(f"max_height must be positive, got: {self.config.max_height}")
    
    def can_handle(self, post_data: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """
        Check if this plugin can handle the given content.
        
        Args:
            post_data: Post metadata dictionary
            context: Processing context
            
        Returns:
            bool: True if plugin can handle this content
        """
        # Check if there are downloaded image files to convert
        downloads = context.get('downloads', [])
        
        for download_info in downloads:
            file_path = download_info.get('local_path')
            if file_path and self._is_supported_image(file_path):
                return True
        
        return False
    
    def _is_supported_image(self, file_path: str) -> bool:
        """Check if file is a supported image format."""
        try:
            path = Path(file_path)
            return path.suffix.lower() in self.SUPPORTED_INPUT_FORMATS
        except Exception:
            return False
    
    async def process(self, post_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and convert downloaded images.
        
        Args:
            post_data: Post metadata
            context: Processing context with download information
            
        Returns:
            Dict containing processing results
        """
        results = {
            'processed_files': [],
            'converted_files': [],
            'errors': [],
            'statistics': {
                'total_processed': 0,
                'successful_conversions': 0,
                'size_saved': 0,
                'processing_time': 0
            }
        }
        
        downloads = context.get('downloads', [])
        
        for download_info in downloads:
            file_path = download_info.get('local_path')
            
            if not file_path or not self._is_supported_image(file_path):
                continue
            
            try:
                # Convert the image
                conversion_result = await self._convert_image(file_path, post_data)
                
                if conversion_result['success']:
                    results['converted_files'].append(conversion_result)
                    results['statistics']['successful_conversions'] += 1
                    results['statistics']['size_saved'] += conversion_result.get('size_saved', 0)
                else:
                    results['errors'].append({
                        'file': file_path,
                        'error': conversion_result.get('error', 'Unknown error')
                    })
                
                results['processed_files'].append(file_path)
                results['statistics']['total_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing image {file_path}: {e}")
                results['errors'].append({
                    'file': file_path,
                    'error': str(e)
                })
        
        # Log processing summary
        stats = results['statistics']
        logger.info(
            f"Image conversion complete: {stats['successful_conversions']}/{stats['total_processed']} "
            f"files converted, {stats['size_saved']} bytes saved"
        )
        
        return results
    
    async def _convert_image(self, input_path: str, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a single image file.
        
        Args:
            input_path: Path to input image
            post_data: Post metadata for context
            
        Returns:
            Dict containing conversion results
        """
        try:
            input_path = Path(input_path)
            original_size = input_path.stat().st_size
            
            # Generate output path
            output_path = self._generate_output_path(input_path, post_data)
            
            # Load and process image
            with Image.open(input_path) as img:
                # Convert to RGB if necessary (for JPEG output)
                if self.config.target_format == 'jpeg' and img.mode in ('RGBA', 'LA', 'P'):
                    # Create white background for transparency
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = background
                
                # Resize if configured
                if self.config.max_width or self.config.max_height:
                    img = self._resize_image(img)
                
                # Strip metadata if configured
                if self.config.strip_metadata:
                    img = self._strip_metadata(img)
                
                # Save with format-specific options
                save_options = self._get_save_options()
                img.save(output_path, format=self.config.target_format.upper(), **save_options)
            
            # Calculate size savings
            new_size = output_path.stat().st_size
            size_saved = original_size - new_size
            
            # Handle original file
            if not self.config.preserve_original and output_path != input_path:
                input_path.unlink()  # Delete original
                logger.debug(f"Removed original file: {input_path}")
            
            return {
                'success': True,
                'input_path': str(input_path),
                'output_path': str(output_path),
                'original_size': original_size,
                'new_size': new_size,
                'size_saved': size_saved,
                'compression_ratio': (size_saved / original_size) * 100 if original_size > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to convert image {input_path}: {e}")
            return {
                'success': False,
                'input_path': str(input_path),
                'error': str(e)
            }
    
    def _generate_output_path(self, input_path: Path, post_data: Dict[str, Any]) -> Path:
        """Generate output path for converted image."""
        # Use same directory as input
        output_dir = input_path.parent
        
        # Generate new filename with target extension
        base_name = input_path.stem
        new_extension = f".{self.config.target_format}"
        
        # Add quality suffix if different from original format
        if input_path.suffix.lower() != new_extension.lower():
            if self.config.quality != 85:  # Only add if non-default quality
                base_name += f"_q{self.config.quality}"
        
        output_path = output_dir / f"{base_name}{new_extension}"
        
        # Handle filename conflicts
        counter = 1
        while output_path.exists() and output_path != input_path:
            output_path = output_dir / f"{base_name}_{counter}{new_extension}"
            counter += 1
        
        return output_path
    
    def _resize_image(self, img: Image.Image) -> Image.Image:
        """Resize image if it exceeds maximum dimensions."""
        original_size = img.size
        max_size = (
            self.config.max_width or original_size[0],
            self.config.max_height or original_size[1]
        )
        
        if original_size[0] <= max_size[0] and original_size[1] <= max_size[1]:
            return img  # No resize needed
        
        # Calculate new size maintaining aspect ratio
        img.thumbnail(max_size, Image.Resampling.LANCZOS)
        logger.debug(f"Resized image from {original_size} to {img.size}")
        
        return img
    
    def _strip_metadata(self, img: Image.Image) -> Image.Image:
        """Remove EXIF and other metadata from image."""
        # Create new image without metadata
        data = list(img.getdata())
        img_without_exif = Image.new(img.mode, img.size)
        img_without_exif.putdata(data)
        return img_without_exif
    
    def _get_save_options(self) -> Dict[str, Any]:
        """Get format-specific save options."""
        options = {}
        
        if self.config.target_format == 'jpeg':
            options.update({
                'quality': self.config.quality,
                'optimize': self.config.optimize,
                'progressive': self.config.progressive,
            })
        elif self.config.target_format == 'png':
            options.update({
                'optimize': self.config.optimize,
                'compress_level': 6,  # Good balance of compression vs speed
            })
        elif self.config.target_format == 'webp':
            options.update({
                'quality': self.config.quality,
                'optimize': self.config.optimize,
                'method': 6,  # Highest quality encoding method
            })
        
        return options
    
    def get_supported_extensions(self) -> set:
        """Return set of supported file extensions."""
        return self.SUPPORTED_INPUT_FORMATS

# Plugin registration hook
@pluggy.hookimpl
def get_content_handlers():
    """Plugin hook to register the image converter handler."""
    return [ImageConverterPlugin]

# Plugin metadata for discovery
PLUGIN_METADATA = {
    'name': 'image_converter',
    'version': '1.0.0',
    'description': 'Advanced image format converter with optimization',
    'author': 'RedditDL Plugin Development Kit',
    'license': 'MIT',
    'dependencies': ['pillow>=9.0.0'],
    'hooks': ['get_content_handlers'],
    'config_schema': {
        'type': 'object',
        'properties': {
            'processing': {
                'type': 'object',
                'properties': {
                    'image_converter': {
                        'type': 'object',
                        'properties': {
                            'target_format': {
                                'type': 'string',
                                'enum': ['jpeg', 'png', 'webp'],
                                'default': 'webp'
                            },
                            'quality': {
                                'type': 'integer',
                                'minimum': 1,
                                'maximum': 100,
                                'default': 85
                            },
                            'optimize': {
                                'type': 'boolean',
                                'default': True
                            },
                            'preserve_original': {
                                'type': 'boolean',
                                'default': False
                            },
                            'max_width': {
                                'type': ['integer', 'null'],
                                'minimum': 1,
                                'default': 1920
                            },
                            'max_height': {
                                'type': ['integer', 'null'],
                                'minimum': 1,
                                'default': 1080
                            }
                        }
                    }
                }
            }
        }
    }
}

if __name__ == "__main__":
    # Example usage and testing
    import asyncio
    import tempfile
    import json
    
    async def test_plugin():
        """Test the image converter plugin."""
        print("Testing ImageConverter Plugin...")
        
        # Mock configuration
        config = {
            'processing': {
                'image_converter': {
                    'target_format': 'webp',
                    'quality': 80,
                    'max_width': 800,
                    'preserve_original': True
                }
            }
        }
        
        # Initialize plugin
        try:
            plugin = ImageConverterPlugin(config)
            print(f"✓ Plugin initialized successfully")
            print(f"  Target format: {plugin.config.target_format}")
            print(f"  Quality: {plugin.config.quality}")
            print(f"  Max dimensions: {plugin.config.max_width}x{plugin.config.max_height}")
            
        except Exception as e:
            print(f"✗ Plugin initialization failed: {e}")
            return
        
        # Test configuration validation
        print("✓ Configuration validation passed")
        
        # Test supported extensions
        print(f"✓ Supported extensions: {plugin.get_supported_extensions()}")
        
        print("ImageConverter plugin test completed successfully!")
    
    # Run test if script is executed directly
    asyncio.run(test_plugin())