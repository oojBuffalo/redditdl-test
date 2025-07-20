"""
Image Processing Module

PIL/Pillow-based image processing for format conversion, quality adjustment,
resizing, and thumbnail generation with EXIF metadata preservation.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from PIL import Image, ImageOps
from PIL.ExifTags import TAGS
import piexif

from .exceptions import ImageProcessingError, UnsupportedFormatError


class ImageProcessor:
    """
    Comprehensive image processing using PIL/Pillow.
    
    Provides image format conversion, quality adjustment, resizing, and thumbnail
    generation while preserving EXIF metadata when possible.
    """
    
    # Supported image formats
    SUPPORTED_FORMATS = {
        'jpeg', 'jpg', 'png', 'webp', 'bmp', 'tiff', 'gif'
    }
    
    # Formats that support quality settings
    QUALITY_FORMATS = {'jpeg', 'jpg', 'webp'}
    
    # Formats that support EXIF data
    EXIF_FORMATS = {'jpeg', 'jpg', 'tiff'}
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the image processor.
        
        Args:
            config: Processing configuration options
        """
        self.config = config or {}
        self.logger = logging.getLogger("redditdl.processing.image")
        
        # Set default values from config
        self.default_quality = self.config.get('image_quality', 85)
        self.preserve_exif = self.config.get('preserve_original_metadata', True)
        
    def convert_format(
        self, 
        input_path: Path, 
        output_path: Path, 
        target_format: str,
        quality: Optional[int] = None,
        preserve_metadata: bool = True
    ) -> Path:
        """
        Convert image to a different format.
        
        Args:
            input_path: Path to input image file
            output_path: Path for output image file
            target_format: Target format (jpeg, png, webp, etc.)
            quality: Quality setting for lossy formats (1-100)
            preserve_metadata: Whether to preserve EXIF metadata
            
        Returns:
            Path to the converted image file
            
        Raises:
            ImageProcessingError: If conversion fails
            UnsupportedFormatError: If format is not supported
        """
        target_format = target_format.lower()
        if target_format not in self.SUPPORTED_FORMATS:
            raise UnsupportedFormatError(target_format, self.SUPPORTED_FORMATS)
        
        if quality is None:
            quality = self.default_quality
            
        self.logger.info(f"Converting {input_path} to {target_format} format")
        
        try:
            # Open and process the image
            with Image.open(input_path) as img:
                # Handle different color modes
                processed_img = self._prepare_image_for_format(img, target_format)
                
                # Prepare save parameters
                save_kwargs = {}
                
                # Add quality for supported formats
                if target_format in self.QUALITY_FORMATS:
                    save_kwargs['quality'] = max(1, min(100, quality))
                    save_kwargs['optimize'] = True
                
                # Handle EXIF metadata preservation
                if preserve_metadata and self.preserve_exif:
                    exif_data = self._extract_exif(img)
                    if exif_data and target_format in self.EXIF_FORMATS:
                        save_kwargs['exif'] = exif_data
                
                # Ensure output directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Save the converted image
                processed_img.save(output_path, format=target_format.upper(), **save_kwargs)
                
                self.logger.info(f"Successfully converted image to {output_path}")
                return output_path
                
        except Exception as e:
            self.logger.error(f"Image conversion failed: {e}")
            raise ImageProcessingError(f"Failed to convert image: {e}") from e
    
    def adjust_quality(
        self, 
        input_path: Path, 
        output_path: Path, 
        quality: int,
        preserve_metadata: bool = True
    ) -> Path:
        """
        Adjust image quality (recompress).
        
        Args:
            input_path: Path to input image file
            output_path: Path for output image file  
            quality: Quality setting (1-100)
            preserve_metadata: Whether to preserve EXIF metadata
            
        Returns:
            Path to the quality-adjusted image file
            
        Raises:
            ImageProcessingError: If quality adjustment fails
        """
        self.logger.info(f"Adjusting quality of {input_path} to {quality}")
        
        try:
            with Image.open(input_path) as img:
                # Determine output format from file extension or preserve original
                output_format = output_path.suffix.lower()[1:] or 'jpeg'
                # Normalize jpg to jpeg for PIL compatibility
                if output_format == 'jpg':
                    output_format = 'jpeg'
                if output_format not in self.QUALITY_FORMATS:
                    output_format = 'jpeg'  # Default to JPEG for quality adjustment
                
                return self.convert_format(
                    input_path, output_path, output_format, 
                    quality, preserve_metadata
                )
                
        except Exception as e:
            self.logger.error(f"Quality adjustment failed: {e}")
            raise ImageProcessingError(f"Failed to adjust image quality: {e}") from e
    
    def resize_image(
        self, 
        input_path: Path, 
        output_path: Path, 
        max_dimension: int,
        preserve_aspect_ratio: bool = True,
        preserve_metadata: bool = True
    ) -> Path:
        """
        Resize image while preserving aspect ratio.
        
        Args:
            input_path: Path to input image file
            output_path: Path for output image file
            max_dimension: Maximum pixels on longest side
            preserve_aspect_ratio: Whether to preserve aspect ratio
            preserve_metadata: Whether to preserve EXIF metadata
            
        Returns:
            Path to the resized image file
            
        Raises:
            ImageProcessingError: If resizing fails
        """
        self.logger.info(f"Resizing {input_path} to max {max_dimension}px")
        
        try:
            with Image.open(input_path) as img:
                original_size = img.size
                
                # Calculate new size
                if preserve_aspect_ratio:
                    img.thumbnail((max_dimension, max_dimension), Image.Resampling.LANCZOS)
                    new_size = img.size
                else:
                    new_size = (max_dimension, max_dimension)
                    img = img.resize(new_size, Image.Resampling.LANCZOS)
                
                # Determine output format
                output_format = output_path.suffix.lower()[1:] or 'jpeg'
                # Normalize jpg to jpeg for PIL compatibility
                if output_format == 'jpg':
                    output_format = 'jpeg'
                
                # Prepare save parameters
                save_kwargs = {}
                if output_format in self.QUALITY_FORMATS:
                    save_kwargs['quality'] = self.default_quality
                    save_kwargs['optimize'] = True
                
                # Handle EXIF metadata
                if preserve_metadata and self.preserve_exif:
                    exif_data = self._extract_exif(img)
                    if exif_data and output_format in self.EXIF_FORMATS:
                        save_kwargs['exif'] = exif_data
                
                # Ensure output directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Save resized image
                img.save(output_path, format=output_format.upper(), **save_kwargs)
                
                self.logger.info(
                    f"Successfully resized image from {original_size} to {new_size}, "
                    f"saved as {output_path}"
                )
                return output_path
                
        except Exception as e:
            self.logger.error(f"Image resizing failed: {e}")
            raise ImageProcessingError(f"Failed to resize image: {e}") from e
    
    def generate_thumbnail(
        self, 
        input_path: Path, 
        output_path: Path, 
        thumbnail_size: int = 256,
        preserve_metadata: bool = False
    ) -> Path:
        """
        Generate thumbnail image.
        
        Args:
            input_path: Path to input image file
            output_path: Path for thumbnail file
            thumbnail_size: Thumbnail size in pixels (square)
            preserve_metadata: Whether to preserve EXIF metadata
            
        Returns:
            Path to the thumbnail file
            
        Raises:
            ImageProcessingError: If thumbnail generation fails
        """
        self.logger.info(f"Generating {thumbnail_size}px thumbnail for {input_path}")
        
        try:
            with Image.open(input_path) as img:
                # Create thumbnail
                img.thumbnail((thumbnail_size, thumbnail_size), Image.Resampling.LANCZOS)
                
                # Apply auto-orientation
                img = ImageOps.exif_transpose(img)
                
                # Determine output format (prefer JPEG for thumbnails)
                output_format = output_path.suffix.lower()[1:] or 'jpeg'
                # Normalize jpg to jpeg for PIL compatibility
                if output_format == 'jpg':
                    output_format = 'jpeg'
                if output_format not in self.SUPPORTED_FORMATS:
                    output_format = 'jpeg'
                
                # Prepare save parameters
                save_kwargs = {}
                if output_format in self.QUALITY_FORMATS:
                    save_kwargs['quality'] = 85  # Good quality for thumbnails
                    save_kwargs['optimize'] = True
                
                # Handle EXIF metadata (usually not needed for thumbnails)
                if preserve_metadata and self.preserve_exif:
                    exif_data = self._extract_exif(img)
                    if exif_data and output_format in self.EXIF_FORMATS:
                        save_kwargs['exif'] = exif_data
                
                # Ensure output directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Save thumbnail
                img.save(output_path, format=output_format.upper(), **save_kwargs)
                
                self.logger.info(f"Successfully generated thumbnail: {output_path}")
                return output_path
                
        except Exception as e:
            self.logger.error(f"Thumbnail generation failed: {e}")
            raise ImageProcessingError(f"Failed to generate thumbnail: {e}") from e
    
    def _prepare_image_for_format(self, img: Image.Image, target_format: str) -> Image.Image:
        """
        Prepare image for specific output format (handle color modes).
        
        Args:
            img: PIL Image object
            target_format: Target format
            
        Returns:
            Processed PIL Image object
        """
        # Handle transparency and color modes
        if target_format in ['jpeg', 'jpg']:
            # JPEG doesn't support transparency
            if img.mode in ['RGBA', 'LA', 'P']:
                # Create white background
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode in ['RGBA', 'LA'] else None)
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')
        
        elif target_format == 'png':
            # PNG supports transparency
            if img.mode not in ['RGB', 'RGBA', 'L', 'LA']:
                if 'transparency' in img.info:
                    img = img.convert('RGBA')
                else:
                    img = img.convert('RGB')
        
        elif target_format == 'webp':
            # WebP supports both RGB and RGBA
            if img.mode not in ['RGB', 'RGBA']:
                if 'transparency' in img.info or img.mode in ['RGBA', 'LA']:
                    img = img.convert('RGBA')
                else:
                    img = img.convert('RGB')
        
        else:
            # For other formats, convert to RGB as safe default
            if img.mode not in ['RGB', 'RGBA', 'L']:
                img = img.convert('RGB')
        
        return img
    
    def _extract_exif(self, img: Image.Image) -> Optional[bytes]:
        """
        Extract EXIF data from image.
        
        Args:
            img: PIL Image object
            
        Returns:
            EXIF data as bytes or None if not available
        """
        try:
            if hasattr(img, '_getexif') and img._getexif() is not None:
                return img.info.get('exif')
            return img.info.get('exif')
        except Exception as e:
            self.logger.debug(f"Could not extract EXIF data: {e}")
            return None
    
    def get_image_info(self, input_path: Path) -> Dict[str, Any]:
        """
        Get detailed information about an image file.
        
        Args:
            input_path: Path to image file
            
        Returns:
            Dictionary with image information
            
        Raises:
            ImageProcessingError: If image cannot be read
        """
        try:
            with Image.open(input_path) as img:
                info = {
                    'format': img.format,
                    'mode': img.mode,
                    'size': img.size,
                    'width': img.width,
                    'height': img.height,
                    'filename': input_path.name,
                    'file_size': input_path.stat().st_size if input_path.exists() else 0
                }
                
                # Add EXIF info if available
                if hasattr(img, '_getexif'):
                    exif = img._getexif()
                    if exif:
                        info['has_exif'] = True
                        info['exif_tags'] = len(exif)
                    else:
                        info['has_exif'] = False
                        info['exif_tags'] = 0
                
                return info
                
        except Exception as e:
            self.logger.error(f"Failed to get image info: {e}")
            raise ImageProcessingError(f"Failed to read image information: {e}") from e