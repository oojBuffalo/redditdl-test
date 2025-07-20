#!/usr/bin/env python3
"""
Metadata embedding and sidecar generation for Reddit media downloads.

This module handles embedding metadata into image files via EXIF tags and
creating JSON sidecar files for all media types to preserve Reddit post information.
"""

import json
from pathlib import Path
from typing import Dict, Any

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    import piexif
except ImportError as e:
    raise ImportError(f"Required packages not installed: {e}. Install with: uv add Pillow piexif")

from redditdl.utils import sanitize_filename, api_retry


class MetadataEmbedder:
    """
    Handles embedding metadata into images and creating JSON sidecar files.
    
    This class provides functionality to embed Reddit post metadata into image
    EXIF tags and create JSON sidecar files for all media types, ensuring
    metadata preservation across different file formats and use cases.
    """
    
    def __init__(self):
        """
        Initialize the MetadataEmbedder.
        
        Sets up any necessary configuration for metadata processing.
        """
        # Supported image formats for EXIF embedding
        self.supported_image_formats = {'.jpg', '.jpeg', '.tiff', '.tif'}
        
        # EXIF UserComment tag code (where we'll store JSON metadata)
        self.user_comment_tag = piexif.ExifIFD.UserComment
    
    def embed_into_image(self, image_path: Path, metadata: Dict[str, Any]) -> None:
        """
        Embed metadata into image EXIF UserComment tag as JSON.
        
        Uses Pillow and piexif to embed JSON-serialized metadata into the EXIF
        'UserComment' tag of JPEG and TIFF images. For other formats, this method
        will raise an exception to allow fallback to sidecar files.
        
        Args:
            image_path: Path to the image file
            metadata: Dictionary containing metadata to embed
            
        Raises:
            ValueError: If the image format doesn't support EXIF
            FileNotFoundError: If the image file doesn't exist
            PermissionError: If unable to write to the image file
            PIL.UnidentifiedImageError: If the file is not a valid image
            Exception: For other PIL/piexif related errors (propagated for fallback)
        """
        if not image_path.exists():
            raise FileNotFoundError(f"Image file not found: {image_path}")
        
        # Check if format supports EXIF
        if image_path.suffix.lower() not in self.supported_image_formats:
            raise ValueError(f"Image format {image_path.suffix} does not support EXIF metadata")
        
        try:
            # Open image and get existing EXIF data
            with Image.open(image_path) as img:
                # Get existing EXIF data or create new dict
                if "exif" in img.info:
                    exif_dict = piexif.load(img.info["exif"])
                else:
                    exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
                
                # Serialize metadata to JSON and encode for EXIF
                metadata_json = json.dumps(metadata, ensure_ascii=False, separators=(',', ':'))
                metadata_bytes = metadata_json.encode('utf-8')
                
                # Add charset encoding prefix for proper Unicode handling
                # Format: [encoding][metadata] where encoding is 8 bytes
                charset_prefix = b'UNICODE\x00'
                exif_comment = charset_prefix + metadata_bytes
                
                # Store in UserComment tag
                exif_dict["Exif"][self.user_comment_tag] = exif_comment
                
                # Convert back to binary EXIF data
                exif_bytes = piexif.dump(exif_dict)
                
                # Save image with updated EXIF data
                img.save(image_path, exif=exif_bytes, quality=95, optimize=True)
                
        except (OSError, ValueError, KeyError) as e:
            # Re-raise with more context for calling code to handle
            raise Exception(f"Failed to embed metadata into {image_path}: {e}") from e
    
    def write_sidecar(self, media_path: Path, metadata: Dict[str, Any]) -> None:
        """
        Write metadata to a JSON sidecar file.
        
        Creates a .json file alongside the media file containing pretty-printed
        metadata. This method works for all media types and serves as a fallback
        when EXIF embedding is not possible.
        
        Args:
            media_path: Path to the media file
            metadata: Dictionary containing metadata to write
            
        Raises:
            PermissionError: If unable to write to the sidecar file location
            OSError: For other filesystem-related errors
        """
        # Create sidecar file path (original_file.extension.json)
        sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
        
        try:
            # Write pretty-printed JSON to sidecar file
            with open(sidecar_path, 'w', encoding='utf-8') as f:
                json.dump(
                    metadata,
                    f,
                    ensure_ascii=False,
                    indent=2,
                    separators=(',', ': '),
                    sort_keys=True
                )
                # Add trailing newline for better file handling
                f.write('\n')
                
        except (OSError, PermissionError) as e:
            raise OSError(f"Failed to write sidecar file {sidecar_path}: {e}") from e
    
    def process_media(self, media_path: Path, metadata: Dict[str, Any], 
                     prefer_sidecar: bool = False) -> Dict[str, bool]:
        """
        Process metadata for a media file using the best available method.
        
        Attempts EXIF embedding for supported image formats, with automatic
        fallback to sidecar files. For non-image media or when prefer_sidecar
        is True, writes only sidecar files.
        
        Args:
            media_path: Path to the media file
            metadata: Dictionary containing metadata to embed/write
            prefer_sidecar: If True, skip EXIF embedding and use only sidecar
            
        Returns:
            Dictionary with 'exif_embedded' and 'sidecar_written' boolean flags
            
        Raises:
            FileNotFoundError: If the media file doesn't exist
            OSError: If unable to write sidecar file
        """
        result = {"exif_embedded": False, "sidecar_written": False}
        
        if not media_path.exists():
            raise FileNotFoundError(f"Media file not found: {media_path}")
        
        # Try EXIF embedding for supported images (unless sidecar preferred)
        if not prefer_sidecar and media_path.suffix.lower() in self.supported_image_formats:
            try:
                self.embed_into_image(media_path, metadata)
                result["exif_embedded"] = True
            except Exception:
                # EXIF embedding failed, fall back to sidecar
                pass
        
        # Always write sidecar file as backup/universal format
        try:
            self.write_sidecar(media_path, metadata)
            result["sidecar_written"] = True
        except OSError:
            # If sidecar also fails and no EXIF was written, re-raise
            if not result["exif_embedded"]:
                raise
        
        return result 