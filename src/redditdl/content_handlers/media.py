"""
Media Content Handler

Handles image, video, and audio downloads by wrapping the existing MediaDownloader
functionality in the new content handler interface. Preserves all existing retry
logic, error handling, and metadata embedding capabilities.
"""

import time
from pathlib import Path
from typing import Dict, Any, Set, List, Optional
from urllib.parse import urlparse

from .base import BaseContentHandler, HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.metadata import MetadataEmbedder
from redditdl.downloader import MediaDownloader
from redditdl.utils import sanitize_filename
from redditdl.core.templates import FilenameTemplateEngine

# Import enhanced error handling
from redditdl.core.exceptions import (
    RedditDLError, ProcessingError, NetworkError, ErrorCode, 
    ErrorContext, RecoverySuggestion, processing_error
)
from redditdl.core.error_recovery import get_recovery_manager
from redditdl.core.error_context import report_error

try:
    from redditdl.processing import ProcessorFactory, ProcessingError, VIDEO_PROCESSING_AVAILABLE
    PROCESSING_AVAILABLE = True
except ImportError:
    ProcessorFactory = None
    ProcessingError = None
    VIDEO_PROCESSING_AVAILABLE = False
    PROCESSING_AVAILABLE = False


class MediaContentHandler(BaseContentHandler):
    """
    Content handler for downloadable media (images, videos, audio).
    
    Wraps the existing MediaDownloader functionality while implementing
    the content handler interface. Handles all types of media files
    including images, videos, and audio from various sources.
    """
    
    def __init__(self, priority: int = 50):
        super().__init__("media", priority)
        self._downloader: MediaDownloader = None
        self._embedder: MetadataEmbedder = None
        self._template_engine: FilenameTemplateEngine = None
        self._processor_factory: ProcessorFactory = None
    
    @property
    def supported_content_types(self) -> Set[str]:
        """Media handler supports image, video, and audio content."""
        return {'image', 'video', 'audio'}
    
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this is a media post with a downloadable URL
        """
        # Check if content type is supported
        if content_type not in self.supported_content_types:
            return False
        
        # Must have a URL to download
        media_url = post.media_url or post.url
        if not media_url:
            return False
        
        # Check if URL looks like media
        return self._is_media_url(media_url)
    
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Download media content from the post.
        
        Args:
            post: PostMetadata object to process
            output_dir: Directory to save content to
            config: Handler configuration options
            
        Returns:
            HandlerResult with download details
            
        Raises:
            HandlerError: If download fails
        """
        start_time = time.time()
        result = HandlerResult(
            handler_name=self.name,
            content_type=config.get('content_type', 'unknown')
        )
        
        recovery_manager = get_recovery_manager()
        post_id = getattr(post, 'id', 'unknown')
        
        # Create error context for this operation
        error_context = ErrorContext(
            operation="media_handler_process",
            stage="processing",
            post_id=post_id,
            url=post.media_url or post.url
        )
        
        try:
            # Get media URL with validation
            media_url = post.media_url or post.url
            if not media_url:
                validation_error = ProcessingError(
                    message=f"No media URL found in post {post_id}",
                    error_code=ErrorCode.PROCESSING_INVALID_CONTENT,
                    context=error_context
                )
                
                validation_error.add_suggestion(RecoverySuggestion(
                    action="Check post content",
                    description="Verify the post contains valid media URL or media_url field",
                    automatic=False,
                    priority=1
                ))
                
                report_error(validation_error, error_context)
                raise HandlerError(validation_error.get_user_message())
            
            error_context.url = media_url
            
            # Initialize downloader if needed with error handling
            try:
                downloader = self._get_or_create_downloader(output_dir, config)
            except Exception as e:
                config_error = ProcessingError(
                    message=f"Failed to initialize media downloader for post {post_id}",
                    error_code=ErrorCode.PROCESSING_DEPENDENCY_MISSING,
                    context=error_context,
                    cause=e
                )
                
                config_error.add_suggestion(RecoverySuggestion(
                    action="Check downloader configuration",
                    description="Verify output directory exists and MediaDownloader dependencies are available",
                    automatic=False,
                    priority=1
                ))
                
                report_error(config_error, error_context)
                raise HandlerError(f"Downloader initialization failed: {str(e)}")
            
            # Generate filename with error handling
            try:
                filename = self._construct_filename(post, media_url, config)
            except Exception as e:
                filename_error = ProcessingError(
                    message=f"Failed to generate filename for post {post_id}",
                    error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
                    context=error_context,
                    cause=e
                )
                
                # Use fallback filename
                fallback_filename = f"{post_id}_{int(time.time())}"
                filename_error.add_suggestion(RecoverySuggestion(
                    action="Use fallback filename",
                    description=f"Using fallback filename: {fallback_filename}",
                    automatic=True,
                    priority=1
                ))
                
                report_error(filename_error, error_context, level="warning")
                self.logger.warning(f"Using fallback filename for post {post_id}: {e}")
                filename = fallback_filename
            
            self.logger.info(f"Downloading media: {media_url}")
            
            # Download the file with enhanced error recovery
            try:
                output_path = downloader.download(media_url, filename, post.to_dict())
            except Exception as download_error:
                # Create structured error for download failure
                if "network" in str(download_error).lower() or "connection" in str(download_error).lower():
                    enhanced_error = NetworkError(
                        message=f"Network error downloading media for post {post_id}",
                        error_code=ErrorCode.NETWORK_CONNECTION_FAILED,
                        url=media_url,
                        context=error_context,
                        cause=download_error
                    )
                else:
                    enhanced_error = ProcessingError(
                        message=f"Media download failed for post {post_id}",
                        error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
                        context=error_context,
                        cause=download_error
                    )
                
                enhanced_error.add_suggestion(RecoverySuggestion(
                    action="Retry download",
                    description="Download will be retried automatically with exponential backoff",
                    automatic=True,
                    priority=1
                ))
                
                # Attempt recovery
                recovery_result = await recovery_manager.recover_from_error(enhanced_error, error_context)
                
                if recovery_result.success:
                    # Retry the download
                    try:
                        output_path = downloader.download(media_url, filename, post.to_dict())
                        self.logger.info(f"Download recovered successfully for post {post_id}")
                    except Exception as retry_error:
                        report_error(enhanced_error, error_context)
                        raise HandlerError(f"Download failed after recovery: {str(retry_error)}")
                else:
                    report_error(enhanced_error, error_context)
                    raise HandlerError(enhanced_error.get_user_message())
            
            if output_path and output_path.exists():
                result.success = True
                result.add_file(output_path)
                result.add_operation("download")
                
                # Check if metadata was embedded
                if downloader.embedder:
                    result.metadata_embedded = True
                    result.add_operation("metadata_embed")
                
                # Check for sidecar file
                sidecar_path = output_path.with_suffix(output_path.suffix + '.json')
                if sidecar_path.exists():
                    result.sidecar_created = True
                    result.add_file(sidecar_path)
                    result.add_operation("sidecar_creation")
                
                # Apply post-download processing if enabled
                processing_result = self._apply_processing(output_path, config)
                if processing_result:
                    for file in processing_result.get('processed_files', []):
                        result.add_file(file)
                    for operation in processing_result.get('operations', []):
                        result.add_operation(operation)
                
                self.logger.info(f"Successfully downloaded: {output_path.name}")
            else:
                result.success = False
                result.error_message = f"Download failed for {media_url}"
                self.logger.error(result.error_message)
        
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Error processing media post {post.id}: {e}")
            if isinstance(e, HandlerError):
                raise
            else:
                raise HandlerError(f"Media download failed: {e}") from e
        
        result.processing_time = time.time() - start_time
        return result
    
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
        
        # Create downloader if needed or if configuration changed
        if not self._downloader:
            self._downloader = MediaDownloader(
                outdir=output_dir,
                sleep_interval=sleep_interval,
                embedder=self._embedder if embed_metadata else None
            )
        
        return self._downloader
    
    def _construct_filename(self, post: PostMetadata, media_url: str, config: Dict[str, Any]) -> str:
        """
        Construct a filename for the downloaded media.
        
        Args:
            post: PostMetadata object containing post information
            media_url: URL of the media to download
            config: Configuration options
            
        Returns:
            Safe filename string
        """
        # Check if custom filename template is provided
        filename_template = config.get('filename_template')
        if filename_template:
            return self._apply_template(filename_template, post, media_url, config)
        
        # Use default filename construction
        return self._construct_default_filename(post, media_url)
    
    def _apply_template(self, template: str, post: PostMetadata, media_url: str, config: Dict[str, Any]) -> str:
        """
        Apply Jinja2 template rendering for filename generation.
        
        Args:
            template: Jinja2 template string
            post: PostMetadata object
            media_url: Media URL
            config: Configuration options
            
        Returns:
            Rendered filename string
        """
        # Initialize template engine if needed
        if self._template_engine is None:
            self._template_engine = FilenameTemplateEngine()
        
        # Prepare template variables
        template_vars = self._prepare_template_variables(post, media_url, config)
        
        try:
            # Get max filename length from config
            max_length = config.get('max_filename_length', 200)
            
            # Render the template
            filename = self._template_engine.render(template, template_vars, max_length)
            
            return filename
            
        except Exception as e:
            self.logger.warning(f"Template rendering failed: {e}, falling back to default")
            return self._construct_default_filename(post, media_url)
    
    def _prepare_template_variables(self, post: PostMetadata, media_url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare template variables for filename rendering.
        
        Args:
            post: PostMetadata object
            media_url: Media URL
            config: Configuration options
            
        Returns:
            Dictionary of template variables
        """
        # Parse URL to get extension
        parsed_url = urlparse(media_url)
        extension = Path(parsed_url.path).suffix.lower()
        if not extension:
            extension = self._guess_extension(media_url)
        
        # Remove leading dot from extension
        ext = extension[1:] if extension.startswith('.') else extension
        
        # Determine content type
        content_type = 'image'
        if post.is_video or ext in ['mp4', 'webm', 'mov', 'avi', 'mkv']:
            content_type = 'video'
        elif ext in ['mp3', 'wav', 'ogg', 'flac']:
            content_type = 'audio'
        
        # Build template variables from post metadata
        template_vars = {
            'subreddit': post.subreddit,
            'post_id': post.id,
            'title': post.title,
            'author': post.author,
            'date': post.date_iso,
            'ext': ext,
            'content_type': content_type,
            'url': post.url,
            'media_url': media_url,
            'is_video': post.is_video,
        }
        
        # Add any additional variables from post data
        post_dict = post.to_dict()
        for key, value in post_dict.items():
            if key not in template_vars:
                template_vars[key] = value
        
        return template_vars
    
    def _construct_default_filename(self, post: PostMetadata, media_url: str) -> str:
        """
        Construct default filename using date, post ID, and title.
        
        Args:
            post: PostMetadata object
            media_url: Media URL
            
        Returns:
            Default filename
        """
        # Parse URL to get potential file extension
        parsed_url = urlparse(media_url)
        extension = Path(parsed_url.path).suffix.lower()
        
        # If no extension, try to guess from common patterns
        if not extension:
            extension = self._guess_extension(media_url)
        
        # Create base filename using date, post ID, and sanitized title
        title_part = post.title[:50] if post.title else "untitled"
        base_filename = f"{post.date_iso}_{post.id}_{title_part}"
        
        # Sanitize and add extension
        safe_base = sanitize_filename(base_filename)
        return f"{safe_base}{extension}"
    
    def _guess_extension(self, media_url: str) -> str:
        """
        Guess file extension from URL patterns.
        
        Args:
            media_url: URL to analyze
            
        Returns:
            File extension with leading dot
        """
        url_lower = media_url.lower()
        
        # Common image hosts
        if 'i.redd.it' in url_lower or 'imgur.com' in url_lower:
            return '.jpg'
        elif 'v.redd.it' in url_lower:
            return '.mp4'
        elif 'gfycat.com' in url_lower or 'redgifs.com' in url_lower:
            return '.mp4'
        
        return '.jpg'  # Default fallback
    
    def _is_media_url(self, url: str) -> bool:
        """
        Determine if a URL points to downloadable media.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL appears to be media, False otherwise
        """
        if not url:
            return False
        
        url_lower = url.lower()
        
        # Known media hosts
        media_hosts = [
            'i.redd.it',
            'v.redd.it', 
            'imgur.com',
            'i.imgur.com',
            'gfycat.com',
            'redgifs.com'
        ]
        
        # Check if URL is from a known media host
        for host in media_hosts:
            if host in url_lower:
                return True
        
        # Check for common media file extensions
        media_extensions = [
            '.jpg', '.jpeg', '.png', '.gif', '.webp',
            '.mp4', '.webm', '.mov', '.avi', '.mkv',
            '.mp3', '.wav', '.ogg', '.flac'
        ]
        
        for ext in media_extensions:
            if url_lower.endswith(ext):
                return True
        
        return False
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate media handler configuration.
        
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
    
    def _apply_processing(self, file_path: Path, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Apply post-download processing to media file.
        
        Args:
            file_path: Path to downloaded file
            config: Handler configuration
            
        Returns:
            Dictionary with processing results or None if no processing applied
        """
        # Check if processing is available and enabled
        if not PROCESSING_AVAILABLE:
            return None
        
        processing_config = config.get('processing', {})
        if not processing_config.get('enabled', False):
            return None
        
        try:
            # Initialize processor factory if needed
            if self._processor_factory is None:
                self._processor_factory = ProcessorFactory(processing_config)
            
            # Detect content type and create processor
            content_type = self._processor_factory.detect_content_type(file_path)
            if content_type == 'unknown':
                self.logger.debug(f"Unknown content type for {file_path}, skipping processing")
                return None
            
            processor = self._processor_factory.create_processor(content_type, file_path, processing_config)
            
            # Apply processing operations based on configuration
            results = {
                'processed_files': [],
                'operations': []
            }
            
            # Image processing
            if content_type == 'image':
                results.update(self._process_image(processor, file_path, processing_config))
            
            # Video processing
            elif content_type == 'video' and VIDEO_PROCESSING_AVAILABLE:
                results.update(self._process_video(processor, file_path, processing_config))
            
            return results if results['processed_files'] or results['operations'] else None
            
        except Exception as e:
            self.logger.warning(f"Processing failed for {file_path}: {e}")
            return None
    
    def _process_image(self, processor, file_path: Path, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply image processing operations.
        
        Args:
            processor: ImageProcessor instance
            file_path: Path to image file
            config: Processing configuration
            
        Returns:
            Dictionary with processing results
        """
        results = {'processed_files': [], 'operations': []}
        
        try:
            # Format conversion
            if config.get('image_format_conversion', False):
                target_format = config.get('target_image_format', 'jpeg')
                if target_format != file_path.suffix[1:].lower():
                    converted_path = file_path.with_suffix(f'.{target_format}')
                    processor.convert_format(
                        file_path, 
                        converted_path, 
                        target_format,
                        quality=config.get('image_quality', 85),
                        preserve_metadata=config.get('preserve_original_metadata', True)
                    )
                    results['processed_files'].append(converted_path)
                    results['operations'].append('image_format_conversion')
            
            # Quality adjustment
            elif config.get('image_quality_adjustment', False):
                quality = config.get('image_quality', 85)
                if quality != 100:  # Only process if quality is reduced
                    quality_path = file_path.with_name(f"{file_path.stem}_q{quality}{file_path.suffix}")
                    processor.adjust_quality(
                        file_path,
                        quality_path,
                        quality,
                        preserve_metadata=config.get('preserve_original_metadata', True)
                    )
                    results['processed_files'].append(quality_path)
                    results['operations'].append('image_quality_adjustment')
            
            # Resolution limiting
            max_resolution = config.get('max_image_resolution')
            if max_resolution:
                resized_path = file_path.with_name(f"{file_path.stem}_resized{file_path.suffix}")
                processor.resize_image(
                    file_path,
                    resized_path,
                    max_resolution,
                    preserve_aspect_ratio=True,
                    preserve_metadata=config.get('preserve_original_metadata', True)
                )
                results['processed_files'].append(resized_path)
                results['operations'].append('image_resize')
            
            # Thumbnail generation
            if config.get('generate_thumbnails', False):
                thumbnail_size = config.get('thumbnail_size', 256)
                thumbnail_path = file_path.with_name(f"{file_path.stem}_thumb{file_path.suffix}")
                processor.generate_thumbnail(
                    file_path,
                    thumbnail_path,
                    thumbnail_size,
                    preserve_metadata=False  # Thumbnails typically don't need metadata
                )
                results['processed_files'].append(thumbnail_path)
                results['operations'].append('thumbnail_generation')
            
        except Exception as e:
            self.logger.warning(f"Image processing failed: {e}")
        
        return results
    
    def _process_video(self, processor, file_path: Path, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply video processing operations.
        
        Args:
            processor: VideoProcessor instance
            file_path: Path to video file
            config: Processing configuration
            
        Returns:
            Dictionary with processing results
        """
        results = {'processed_files': [], 'operations': []}
        
        try:
            # Format conversion
            if config.get('video_format_conversion', False):
                target_format = config.get('target_video_format', 'mp4')
                if target_format != file_path.suffix[1:].lower():
                    converted_path = file_path.with_suffix(f'.{target_format}')
                    processor.convert_format(
                        file_path,
                        converted_path,
                        target_format,
                        quality_crf=config.get('video_quality_crf', 23),
                        preserve_metadata=config.get('preserve_original_metadata', True)
                    )
                    results['processed_files'].append(converted_path)
                    results['operations'].append('video_format_conversion')
            
            # Quality adjustment
            elif config.get('video_quality_adjustment', False):
                crf = config.get('video_quality_crf', 23)
                if crf != 23:  # Only process if quality is different from default
                    quality_path = file_path.with_name(f"{file_path.stem}_crf{crf}{file_path.suffix}")
                    processor.adjust_quality(
                        file_path,
                        quality_path,
                        crf,
                        preserve_metadata=config.get('preserve_original_metadata', True)
                    )
                    results['processed_files'].append(quality_path)
                    results['operations'].append('video_quality_adjustment')
            
            # Resolution limiting
            max_resolution = config.get('max_video_resolution')
            if max_resolution:
                resized_path = file_path.with_name(f"{file_path.stem}_resized{file_path.suffix}")
                processor.limit_resolution(
                    file_path,
                    resized_path,
                    max_resolution,
                    preserve_aspect_ratio=True,
                    preserve_metadata=config.get('preserve_original_metadata', True)
                )
                results['processed_files'].append(resized_path)
                results['operations'].append('video_resize')
            
            # Thumbnail extraction
            if config.get('generate_thumbnails', False):
                thumbnail_path = file_path.with_suffix('.jpg')
                thumbnail_path = thumbnail_path.with_name(f"{file_path.stem}_thumb.jpg")
                processor.extract_thumbnail(
                    file_path,
                    thumbnail_path,
                    timestamp=config.get('thumbnail_timestamp', '00:00:01'),
                    thumbnail_size=config.get('thumbnail_size')
                )
                results['processed_files'].append(thumbnail_path)
                results['operations'].append('video_thumbnail_extraction')
            
        except Exception as e:
            self.logger.warning(f"Video processing failed: {e}")
        
        return results