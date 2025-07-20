"""
Gallery Content Handler

Handles Reddit gallery posts by downloading all images in the gallery
with proper naming and ordering. Preserves gallery metadata and handles
multi-image downloads efficiently.
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, Set, List

from .base import BaseContentHandler, HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.downloader import MediaDownloader
from redditdl.metadata import MetadataEmbedder
from redditdl.utils import sanitize_filename


class GalleryContentHandler(BaseContentHandler):
    """
    Content handler for Reddit gallery posts.
    
    Downloads all images in a gallery with sequential numbering and
    proper organization. Creates gallery metadata files and handles
    image ordering preservation.
    """
    
    def __init__(self, priority: int = 40):
        super().__init__("gallery", priority)
        self._downloader: MediaDownloader = None
        self._embedder: MetadataEmbedder = None
    
    @property
    def supported_content_types(self) -> Set[str]:
        """Gallery handler supports gallery content type."""
        return {'gallery'}
    
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this is a gallery post with image URLs
        """
        if content_type != 'gallery':
            return False
        
        # Must have gallery image URLs
        gallery_urls = getattr(post, 'gallery_image_urls', [])
        return bool(gallery_urls)
    
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Download all images in the gallery.
        
        Args:
            post: PostMetadata object to process
            output_dir: Directory to save content to
            config: Handler configuration options
            
        Returns:
            HandlerResult with download details
            
        Raises:
            HandlerError: If processing fails
        """
        start_time = time.time()
        result = HandlerResult(
            handler_name=self.name,
            content_type='gallery'
        )
        
        try:
            # Get gallery URLs
            gallery_urls = getattr(post, 'gallery_image_urls', [])
            if not gallery_urls:
                raise HandlerError("No gallery URLs found in post")
            
            # Create gallery subdirectory
            gallery_dir = self._create_gallery_directory(post, output_dir, config)
            
            # Initialize downloader
            downloader = self._get_or_create_downloader(gallery_dir, config)
            
            self.logger.info(f"Processing gallery with {len(gallery_urls)} images")
            
            successful_downloads = 0
            failed_downloads = 0
            
            # Download each image in the gallery
            for i, image_url in enumerate(gallery_urls, 1):
                try:
                    # Generate filename with sequence number
                    filename = self._construct_image_filename(post, image_url, i, len(gallery_urls), config)
                    
                    self.logger.debug(f"Downloading gallery image {i}/{len(gallery_urls)}: {image_url}")
                    
                    # Download the image
                    output_path = downloader.download(image_url, filename, post.to_dict())
                    
                    if output_path and output_path.exists():
                        result.add_file(output_path)
                        successful_downloads += 1
                        self.logger.debug(f"✓ Downloaded: {output_path.name}")
                    else:
                        failed_downloads += 1
                        self.logger.warning(f"✗ Failed to download gallery image {i}")
                        
                except Exception as e:
                    failed_downloads += 1
                    self.logger.error(f"Error downloading gallery image {i}: {e}")
            
            # Create gallery metadata file
            metadata_path = self._create_gallery_metadata(post, gallery_urls, gallery_dir, config)
            if metadata_path:
                result.add_file(metadata_path)
                result.add_operation("gallery_metadata")
            
            # Update result
            result.add_operation("gallery_download")
            
            if successful_downloads > 0:
                result.success = True
                if downloader.embedder:
                    result.metadata_embedded = True
                    result.add_operation("metadata_embed")
                
                self.logger.info(f"Gallery download completed: {successful_downloads} successful, {failed_downloads} failed")
            else:
                result.success = False
                result.error_message = f"No images downloaded from gallery ({failed_downloads} failed)"
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Error processing gallery post {post.id}: {e}")
            raise HandlerError(f"Gallery processing failed: {e}") from e
        
        result.processing_time = time.time() - start_time
        return result
    
    def _create_gallery_directory(self, post: PostMetadata, output_dir: Path, config: Dict[str, Any]) -> Path:
        """
        Create a subdirectory for the gallery.
        
        Args:
            post: PostMetadata object
            output_dir: Base output directory
            config: Configuration options
            
        Returns:
            Path to the gallery directory
        """
        # Create gallery directory name  
        title_part = post.title[:30] if post.title else "gallery"
        # Replace colons in date for safer directory names
        safe_date = post.date_iso.replace(':', '_')
        dir_name = f"{safe_date}_{post.id}_{title_part}"
        safe_dir_name = sanitize_filename(dir_name)
        
        gallery_dir = output_dir / safe_dir_name
        gallery_dir.mkdir(parents=True, exist_ok=True)
        
        return gallery_dir
    
    def _construct_image_filename(
        self, 
        post: PostMetadata, 
        image_url: str, 
        index: int, 
        total: int, 
        config: Dict[str, Any]
    ) -> str:
        """
        Construct filename for a gallery image.
        
        Args:
            post: PostMetadata object
            image_url: URL of the image
            index: Image index (1-based)
            total: Total number of images
            config: Configuration options
            
        Returns:
            Safe filename with sequential numbering
        """
        # Get file extension from URL
        extension = Path(image_url).suffix.lower()
        if not extension:
            extension = '.jpg'  # Default extension
        
        # Create base filename with padding for proper sorting
        padding = len(str(total))
        base_name = f"{index:0{padding}d}_image{extension}"
        
        return sanitize_filename(base_name)
    
    def _create_gallery_metadata(
        self, 
        post: PostMetadata, 
        gallery_urls: List[str], 
        gallery_dir: Path, 
        config: Dict[str, Any]
    ) -> Path:
        """
        Create a metadata file for the gallery.
        
        Args:
            post: PostMetadata object
            gallery_urls: List of gallery image URLs
            gallery_dir: Gallery directory
            config: Configuration options
            
        Returns:
            Path to the metadata file
        """
        metadata_path = gallery_dir / "gallery_metadata.json"
        
        try:
            metadata = {
                'post_data': post.to_dict(),
                'gallery_info': {
                    'total_images': len(gallery_urls),
                    'image_urls': gallery_urls,
                    'created_at': time.time(),
                    'handler': self.name
                }
            }
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            return metadata_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create gallery metadata: {e}")
            return None
    
    def _get_or_create_downloader(self, output_dir: Path, config: Dict[str, Any]) -> MediaDownloader:
        """
        Get or create MediaDownloader instance.
        
        Args:
            output_dir: Output directory for downloads
            config: Configuration options
            
        Returns:
            MediaDownloader instance
        """
        sleep_interval = config.get('sleep_interval', 1.0)
        embed_metadata = config.get('embed_metadata', True)
        
        # Create embedder if needed and metadata embedding is enabled
        if embed_metadata and not self._embedder:
            self._embedder = MetadataEmbedder()
        
        # Create downloader if needed
        if not self._downloader:
            self._downloader = MediaDownloader(
                outdir=output_dir,
                sleep_interval=sleep_interval,
                embedder=self._embedder if embed_metadata else None
            )
        
        return self._downloader
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate gallery handler configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate sleep interval
        sleep_interval = config.get('sleep_interval')
        if sleep_interval is not None and sleep_interval < 0:
            errors.append("sleep_interval must be non-negative")
        
        # Validate embed_metadata flag
        embed_metadata = config.get('embed_metadata')
        if embed_metadata is not None and not isinstance(embed_metadata, bool):
            errors.append("embed_metadata must be a boolean")
        
        return errors