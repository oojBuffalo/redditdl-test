#!/usr/bin/env python3
"""
Media downloader for Reddit posts with metadata embedding.

This module provides the MediaDownloader class for downloading media files
from URLs, saving them to disk with proper filenames, and integrating with
the MetadataEmbedder for preserving Reddit post metadata.
"""

import os
import time
import mimetypes
from pathlib import Path
from typing import Dict, Any, Optional
import requests
from urllib.parse import urlparse

from redditdl.metadata import MetadataEmbedder
from redditdl.utils import sanitize_filename, api_retry


class MediaDownloader:
    """
    Downloads media files and handles metadata embedding.
    
    This class provides functionality to download media files from URLs,
    save them to a specified directory, and embed or attach metadata using
    the MetadataEmbedder. Includes proper error handling, rate limiting,
    and support for various media types.
    """
    
    def __init__(
        self, 
        outdir: Path, 
        sleep_interval: float = 1.0,
        embedder: Optional[MetadataEmbedder] = None
    ):
        """
        Initialize MediaDownloader with output directory and configuration.
        
        Args:
            outdir: Path to the output directory for downloaded files
            sleep_interval: Time to sleep after each download (default 1.0s)
            embedder: Optional MetadataEmbedder for metadata processing
            
        Raises:
            OSError: If unable to create the output directory
        """
        self.outdir = Path(outdir)
        self.sleep_interval = sleep_interval
        self.embedder = embedder
        
        # Create output directory if it doesn't exist
        try:
            os.makedirs(self.outdir, exist_ok=True)
            print(f"[INFO] Output directory ready: {self.outdir}")
        except OSError as e:
            print(f"[ERROR] Failed to create output directory {self.outdir}: {e}")
            raise OSError(f"Failed to create output directory {self.outdir}: {e}") from e
    
    @api_retry(max_retries=3, initial_delay=0.7)
    def download(self, media_url: str, filename: str, metadata: Dict[str, Any]) -> Path:
        """
        Download a media file and embed metadata.
        
        Downloads the media file from the given URL, saves it with the specified
        filename in the output directory, and processes metadata using the embedder.
        
        Args:
            media_url: URL of the media file to download
            filename: Desired filename for the downloaded file
            metadata: Metadata dictionary to embed/attach to the file
            
        Returns:
            Path to the downloaded file
            
        Raises:
            requests.RequestException: If download fails after retries
            ValueError: If URL or filename is invalid
            OSError: If unable to write the file after retries
        """
        if not media_url or not media_url.strip():
            raise ValueError("Media URL cannot be empty")
        
        if not filename or not filename.strip():
            raise ValueError("Filename cannot be empty")
        
        # Sanitize the filename for filesystem safety
        safe_filename = sanitize_filename(filename.strip())
        output_path = self.outdir / safe_filename
        
        # Set up request headers with User-Agent
        headers = {
            'User-Agent': 'RedditDL/1.0 (Media Downloader Bot)'
        }
        
        try:
            # Download the file with streaming
            response = requests.get(media_url, stream=True, headers=headers, timeout=30)
            
            # Handle different HTTP status codes
            if response.status_code == 404:
                print(f"[WARN] Media not found (404): {media_url}")
                return output_path  # Return path even if download failed
            elif response.status_code == 503:
                print(f"[WARN] Service unavailable (503): {media_url}")
                return output_path  # Return path even if download failed
            elif not response.ok:
                print(f"[WARN] HTTP {response.status_code} for {media_url}")
                return output_path  # Return path even if download failed
            
            # Write file content in chunks with OSError handling
            self._write_file_safely(output_path, response)
            
            # Determine file type and handle metadata embedding
            self._process_metadata(output_path, metadata)
            
            print(f"[INFO] Successfully downloaded: {output_path.name}")
            return output_path
            
        except requests.RequestException as e:
            print(f"[ERROR] Network error downloading {media_url}: {e}")
            # Re-raise to let the retry decorator handle it
            raise
        except OSError as e:
            print(f"[ERROR] File system error writing {output_path}: {e}")
            # Re-raise to let the retry decorator handle it if applicable
            raise
        finally:
            # Apply rate limiting in all cases
            time.sleep(self.sleep_interval)
    
    def _write_file_safely(self, output_path: Path, response: requests.Response) -> None:
        """
        Safely write the response content to file with proper error handling.
        
        Args:
            output_path: Path where the file should be written
            response: HTTP response object containing the file data
            
        Raises:
            OSError: If file writing fails
        """
        try:
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)
        except OSError as e:
            # Add detailed context to the error
            print(f"[ERROR] Failed to write file {output_path}: {e}")
            print(f"[ERROR] Check permissions and disk space for directory: {output_path.parent}")
            raise OSError(f"Failed to write file {output_path}: {e}") from e
        except Exception as e:
            # Catch any other file-related errors
            print(f"[ERROR] Unexpected error writing file {output_path}: {e}")
            raise OSError(f"Unexpected error writing file {output_path}: {e}") from e
    
    def _process_metadata(self, file_path: Path, metadata: Dict[str, Any]) -> None:
        """
        Process metadata for the downloaded file.
        
        Determines the file type and calls the appropriate metadata embedding
        method. For images (JPEG, PNG, WebP), attempts EXIF embedding with
        fallback to sidecar. For videos and other types, creates sidecar files.
        
        Args:
            file_path: Path to the downloaded file
            metadata: Metadata dictionary to process
        """
        if not self.embedder:
            # No embedder configured, skip metadata processing
            print(f"[INFO] No metadata embedder configured, skipping metadata for {file_path.name}")
            return
        
        # Get file extension and determine media type
        file_extension = file_path.suffix.lower()
        
        # Define supported formats
        image_formats = {'.jpg', '.jpeg', '.png', '.webp', '.tiff', '.tif'}
        video_formats = {'.mp4', '.mov', '.webm', '.avi', '.mkv', '.flv'}
        
        try:
            if file_extension in image_formats:
                # For images, try EXIF embedding with fallback to sidecar
                try:
                    self.embedder.embed_into_image(file_path, metadata)
                    print(f"[INFO] Successfully embedded metadata into image: {file_path.name}")
                except Exception as e:
                    # EXIF embedding failed, fall back to sidecar
                    print(f"[INFO] EXIF embedding failed for {file_path.name}, falling back to JSON sidecar: {e}")
                    self.embedder.write_sidecar(file_path, metadata)
                    print(f"[INFO] Created JSON metadata sidecar for: {file_path.name}")
            else:
                # For videos and other formats, use sidecar files
                self.embedder.write_sidecar(file_path, metadata)
                print(f"[INFO] Created JSON metadata sidecar for: {file_path.name}")
                
        except OSError as e:
            # File system errors during metadata processing
            print(f"[ERROR] File system error processing metadata for {file_path}: {e}")
            print(f"[WARN] Continuing without metadata for file: {file_path.name}")
        except Exception as e:
            # Any other metadata processing error
            print(f"[WARN] Failed to process metadata for {file_path}: {e}")
            print(f"[WARN] Continuing without metadata for file: {file_path.name}")
    
    def _determine_file_extension(self, url: str, response: requests.Response) -> str:
        """
        Determine the appropriate file extension from URL or response headers.
        
        Args:
            url: The original URL
            response: The HTTP response object
            
        Returns:
            File extension string (with leading dot)
        """
        # First try to get extension from URL
        parsed_url = urlparse(url)
        url_path = parsed_url.path
        if url_path and '.' in url_path:
            extension = Path(url_path).suffix.lower()
            if extension:
                return extension
        
        # Try to determine from Content-Type header
        content_type = response.headers.get('content-type', '').lower()
        if content_type:
            # Remove charset and other parameters
            content_type = content_type.split(';')[0].strip()
            
            # Map common content types to extensions
            content_type_map = {
                'image/jpeg': '.jpg',
                'image/jpg': '.jpg',
                'image/png': '.png',
                'image/gif': '.gif',
                'image/webp': '.webp',
                'video/mp4': '.mp4',
                'video/webm': '.webm',
                'video/mpeg': '.mpg',
            }
            
            if content_type in content_type_map:
                return content_type_map[content_type]
            
            # Use mimetypes to guess extension
            extension = mimetypes.guess_extension(content_type)
            if extension:
                return extension
        
        # Default fallback
        return '.bin'