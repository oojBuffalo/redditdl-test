"""
Base Content Handler Architecture

Provides the foundation for all content handlers including abstract base classes,
registration system, content type detection, and plugin integration.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Type, Union
from urllib.parse import urlparse

from redditdl.scrapers import PostMetadata


@dataclass
class HandlerResult:
    """
    Result of content handler processing.
    
    Contains information about the processing operation including
    success status, files created, operations performed, and timing.
    """
    success: bool = False
    error_message: str = ""
    files_created: List[Path] = field(default_factory=list)
    operations_performed: List[str] = field(default_factory=list)
    metadata_embedded: bool = False
    sidecar_created: bool = False
    processing_time: float = 0.0
    handler_name: str = ""
    content_type: str = ""
    
    def add_file(self, file_path: Path) -> None:
        """Add a created file to the result."""
        if file_path not in self.files_created:
            self.files_created.append(file_path)
    
    def add_operation(self, operation: str) -> None:
        """Add a performed operation to the result."""
        if operation not in self.operations_performed:
            self.operations_performed.append(operation)


class HandlerError(Exception):
    """Exception raised by content handlers during processing."""
    pass


class BaseContentHandler(ABC):
    """
    Abstract base class for all content handlers.
    
    Content handlers are responsible for processing specific types of Reddit content
    such as images, videos, text posts, galleries, polls, etc. Each handler implements
    the standard interface defined here and can be registered with the handler registry.
    """
    
    def __init__(self, name: str, priority: int = 100):
        """
        Initialize the content handler.
        
        Args:
            name: Human-readable name for this handler
            priority: Handler priority (lower = higher priority)
        """
        self.name = name
        self.priority = priority
        self.logger = logging.getLogger(f"redditdl.handlers.{name}")
    
    @abstractmethod
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the given post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this handler can process the post
        """
        pass
    
    @abstractmethod
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Process the post content.
        
        Args:
            post: PostMetadata object to process
            output_dir: Directory to save content to
            config: Handler configuration options
            
        Returns:
            HandlerResult with processing details
            
        Raises:
            HandlerError: If processing fails
        """
        pass
    
    @property
    @abstractmethod
    def supported_content_types(self) -> Set[str]:
        """
        Get the content types this handler supports.
        
        Returns:
            Set of content type strings
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate handler configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation error messages
        """
        return []
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', priority={self.priority})"


class ContentTypeDetector:
    """
    Utility class for detecting content types from Reddit posts.
    
    Analyzes post metadata to determine the type of content and
    provide routing information for content handlers with enhanced
    priority handling and fallback detection.
    """
    
    # Known media file extensions
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff', '.svg'}
    VIDEO_EXTENSIONS = {'.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv', '.wmv', '.m4v'}
    AUDIO_EXTENSIONS = {'.mp3', '.wav', '.ogg', '.flac', '.aac', '.m4a', '.wma'}
    
    # Known media domains
    IMAGE_DOMAINS = {
        'i.redd.it', 'i.imgur.com', 'imgur.com', 'preview.redd.it',
        'imgur.com', 'flickr.com', 'photobucket.com', 'tinypic.com'
    }
    VIDEO_DOMAINS = {
        'v.redd.it', 'gfycat.com', 'redgifs.com', 'streamable.com',
        'youtube.com', 'youtu.be', 'vimeo.com', 'dailymotion.com'
    }
    
    # Content type priority (lower = higher priority)
    CONTENT_TYPE_PRIORITY = {
        'crosspost': 10,    # Highest priority - check first
        'gallery': 20,      # High priority for multi-content
        'poll': 30,         # High priority for interactive content
        'video': 40,        # Media content priority
        'image': 50,        # Image content
        'audio': 60,        # Audio content
        'text': 70,         # Text content
        'external': 80      # Lowest priority - fallback
    }
    
    @classmethod
    def detect_content_type(cls, post: PostMetadata) -> str:
        """
        Detect the content type of a Reddit post with priority handling.
        
        Args:
            post: PostMetadata object to analyze
            
        Returns:
            Content type string ('image', 'video', 'gallery', 'poll', 'text', 'external', 'crosspost')
        """
        detected_types = []
        
        # Run all detection methods and collect results with priorities
        detected_types.extend(cls._detect_special_types(post))
        detected_types.extend(cls._detect_media_types(post))
        detected_types.extend(cls._detect_fallback_types(post))
        
        # Sort by priority and return the highest priority type
        if detected_types:
            detected_types.sort(key=lambda x: cls.CONTENT_TYPE_PRIORITY.get(x, 999))
            return detected_types[0]
        
        # Ultimate fallback
        return 'external'
    
    @classmethod
    def _detect_special_types(cls, post: PostMetadata) -> List[str]:
        """
        Detect special content types (crosspost, gallery, poll).
        
        Args:
            post: PostMetadata object to analyze
            
        Returns:
            List of detected special content types
        """
        types = []
        
        # Check for crosspost first (highest priority)
        if hasattr(post, 'crosspost_parent_id') and post.crosspost_parent_id:
            types.append('crosspost')
        
        # Check for gallery posts
        if hasattr(post, 'gallery_image_urls') and post.gallery_image_urls:
            types.append('gallery')
        elif hasattr(post, 'post_type') and post.post_type == 'gallery':
            types.append('gallery')
        
        # Check for poll posts
        if hasattr(post, 'poll_data') and post.poll_data:
            types.append('poll')
        elif hasattr(post, 'post_type') and post.post_type == 'poll':
            types.append('poll')
        
        return types
    
    @classmethod
    def _detect_media_types(cls, post: PostMetadata) -> List[str]:
        """
        Detect media content types (image, video, audio).
        
        Args:
            post: PostMetadata object to analyze
            
        Returns:
            List of detected media content types
        """
        types = []
        
        # Check explicit video flag
        if getattr(post, 'is_video', False):
            types.append('video')
        
        # Analyze URLs for media content
        urls_to_check = []
        if post.media_url:
            urls_to_check.append(post.media_url)
        if post.url and post.url != post.media_url:
            urls_to_check.append(post.url)
        
        for url in urls_to_check:
            if not url:
                continue
                
            # Parse URL for analysis
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            path = parsed_url.path.lower()
            
            # Check domain-based detection
            if any(img_domain in domain for img_domain in cls.IMAGE_DOMAINS):
                if 'image' not in types:
                    types.append('image')
            
            if any(vid_domain in domain for vid_domain in cls.VIDEO_DOMAINS):
                if 'video' not in types:
                    types.append('video')
            
            # Check file extension
            if any(path.endswith(ext) for ext in cls.IMAGE_EXTENSIONS):
                if 'image' not in types:
                    types.append('image')
            
            if any(path.endswith(ext) for ext in cls.VIDEO_EXTENSIONS):
                if 'video' not in types:
                    types.append('video')
            
            if any(path.endswith(ext) for ext in cls.AUDIO_EXTENSIONS):
                if 'audio' not in types:
                    types.append('audio')
        
        # Check post_type field if available
        if hasattr(post, 'post_type'):
            post_type = post.post_type.lower()
            if post_type in ['image', 'video', 'audio'] and post_type not in types:
                types.append(post_type)
        
        return types
    
    @classmethod
    def _detect_fallback_types(cls, post: PostMetadata) -> List[str]:
        """
        Detect fallback content types (text, external).
        
        Args:
            post: PostMetadata object to analyze
            
        Returns:
            List of detected fallback content types
        """
        types = []
        
        # Check for self posts (text posts)
        is_self = getattr(post, 'is_self', False)
        has_selftext = post.selftext and post.selftext.strip()
        
        if is_self and has_selftext:
            types.append('text')
        elif is_self and not has_selftext:
            # Self post without content - could be text or external
            types.extend(['text', 'external'])
        elif not is_self:
            # Link post - external content
            types.append('external')
        
        return types
    
    @classmethod
    def detect_content_types_with_confidence(cls, post: PostMetadata) -> Dict[str, float]:
        """
        Detect content types with confidence scores.
        
        Args:
            post: PostMetadata object to analyze
            
        Returns:
            Dictionary mapping content types to confidence scores (0.0-1.0)
        """
        confidence_scores = {}
        
        # Special types get high confidence when detected
        special_types = cls._detect_special_types(post)
        for content_type in special_types:
            if content_type == 'crosspost':
                confidence_scores[content_type] = 0.95
            elif content_type == 'gallery':
                confidence_scores[content_type] = 0.90
            elif content_type == 'poll':
                confidence_scores[content_type] = 0.90
        
        # Media types confidence based on detection method
        media_types = cls._detect_media_types(post)
        for content_type in media_types:
            confidence = 0.5  # Base confidence
            
            # Boost confidence for explicit flags
            if content_type == 'video' and getattr(post, 'is_video', False):
                confidence = 0.85
            
            # Boost confidence for known domains
            urls = [post.media_url, post.url]
            for url in urls:
                if url:
                    domain = urlparse(url).netloc.lower()
                    if content_type == 'image' and any(d in domain for d in cls.IMAGE_DOMAINS):
                        confidence = max(confidence, 0.80)
                    elif content_type == 'video' and any(d in domain for d in cls.VIDEO_DOMAINS):
                        confidence = max(confidence, 0.80)
            
            confidence_scores[content_type] = confidence
        
        # Fallback types get lower confidence
        fallback_types = cls._detect_fallback_types(post)
        for content_type in fallback_types:
            if content_type == 'text':
                is_self = getattr(post, 'is_self', False)
                has_selftext = post.selftext and post.selftext.strip()
                if is_self and has_selftext:
                    confidence_scores[content_type] = 0.75
                else:
                    confidence_scores[content_type] = 0.40
            elif content_type == 'external':
                confidence_scores[content_type] = 0.30
        
        return confidence_scores
    
    @classmethod
    def get_content_type_with_fallbacks(cls, post: PostMetadata) -> List[str]:
        """
        Get ordered list of content types for a post (primary + fallbacks).
        
        Args:
            post: PostMetadata object to analyze
            
        Returns:
            List of content types in priority order
        """
        confidence_scores = cls.detect_content_types_with_confidence(post)
        
        # Sort by confidence score (descending)
        sorted_types = sorted(
            confidence_scores.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        return [content_type for content_type, _ in sorted_types]
    
    @classmethod
    def is_media_content(cls, post: PostMetadata) -> bool:
        """
        Check if a post contains downloadable media.
        
        Args:
            post: PostMetadata object to check
            
        Returns:
            True if post contains media that can be downloaded
        """
        content_type = cls.detect_content_type(post)
        return content_type in {'image', 'video', 'audio', 'gallery'}


class ContentHandlerRegistry:
    """
    Registry for managing content handlers.
    
    Provides registration, discovery, and selection of content handlers
    with support for plugin-based extensions and priority ordering.
    """
    
    def __init__(self):
        self._handlers: List[BaseContentHandler] = []
        self._handlers_by_type: Dict[str, List[BaseContentHandler]] = {}
        self.logger = logging.getLogger("redditdl.registry")
    
    def register_handler(self, handler: BaseContentHandler) -> None:
        """
        Register a content handler.
        
        Args:
            handler: Content handler to register
        """
        if handler in self._handlers:
            self.logger.warning(f"Handler {handler.name} already registered")
            return
        
        self._handlers.append(handler)
        
        # Index by supported content types
        for content_type in handler.supported_content_types:
            if content_type not in self._handlers_by_type:
                self._handlers_by_type[content_type] = []
            self._handlers_by_type[content_type].append(handler)
        
        # Sort handlers by priority (lower = higher priority)
        self._handlers.sort(key=lambda h: h.priority)
        for handlers_list in self._handlers_by_type.values():
            handlers_list.sort(key=lambda h: h.priority)
        
        self.logger.debug(f"Registered handler: {handler.name} (priority: {handler.priority})")
    
    def unregister_handler(self, handler: BaseContentHandler) -> None:
        """
        Unregister a content handler.
        
        Args:
            handler: Content handler to unregister
        """
        if handler in self._handlers:
            self._handlers.remove(handler)
            
            # Remove from content type indexes
            for content_type in handler.supported_content_types:
                if content_type in self._handlers_by_type:
                    if handler in self._handlers_by_type[content_type]:
                        self._handlers_by_type[content_type].remove(handler)
            
            self.logger.debug(f"Unregistered handler: {handler.name}")
    
    def get_handler_for_post(
        self, 
        post: PostMetadata, 
        content_type: Optional[str] = None
    ) -> Optional[BaseContentHandler]:
        """
        Get the best handler for a specific post.
        
        Args:
            post: PostMetadata object to find handler for
            content_type: Optional pre-detected content type
            
        Returns:
            Best matching handler or None if no handler found
        """
        if content_type is None:
            content_type = ContentTypeDetector.detect_content_type(post)
        
        # First try handlers registered for this specific content type
        if content_type in self._handlers_by_type:
            for handler in self._handlers_by_type[content_type]:
                if handler.can_handle(post, content_type):
                    return handler
        
        # Fall back to checking all handlers in priority order
        for handler in self._handlers:
            if handler.can_handle(post, content_type):
                return handler
        
        return None
    
    def get_handlers_for_content_type(self, content_type: str) -> List[BaseContentHandler]:
        """
        Get all handlers that support a specific content type.
        
        Args:
            content_type: Content type to find handlers for
            
        Returns:
            List of handlers supporting the content type
        """
        return self._handlers_by_type.get(content_type, []).copy()
    
    def list_all_handlers(self) -> List[BaseContentHandler]:
        """
        Get all registered handlers.
        
        Returns:
            List of all registered handlers
        """
        return self._handlers.copy()
    
    def get_handler_stats(self) -> Dict[str, Any]:
        """
        Get statistics about registered handlers.
        
        Returns:
            Dictionary with handler statistics
        """
        stats = {
            'total_handlers': len(self._handlers),
            'handlers_by_type': {
                content_type: len(handlers) 
                for content_type, handlers in self._handlers_by_type.items()
            },
            'handler_list': [
                {
                    'name': h.name,
                    'priority': h.priority,
                    'content_types': list(h.supported_content_types)
                }
                for h in self._handlers
            ]
        }
        return stats


# Global registry instance
handler_registry = ContentHandlerRegistry()


def register_content_handler(handler_class: Type[BaseContentHandler], **kwargs) -> None:
    """
    Convenience function to register a content handler class.
    
    Args:
        handler_class: Handler class to instantiate and register
        **kwargs: Additional arguments to pass to handler constructor
    """
    handler = handler_class(**kwargs)
    handler_registry.register_handler(handler)


def get_content_handler_for_post(
    post: PostMetadata, 
    content_type: Optional[str] = None
) -> Optional[BaseContentHandler]:
    """
    Convenience function to get a handler for a post.
    
    Args:
        post: PostMetadata object to find handler for
        content_type: Optional pre-detected content type
        
    Returns:
        Best matching handler or None
    """
    return handler_registry.get_handler_for_post(post, content_type)