"""
Media type filtering for Reddit posts.

Filters posts based on their content type (image, video, text, link, etc.)
and specific media formats. Supports flexible content type detection.
"""

import time
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Set
from redditdl.filters.base import Filter, FilterResult
from redditdl.scrapers import PostMetadata


class MediaTypeFilter(Filter):
    """
    Filter posts based on their media/content type.
    
    Configuration options:
    - media_types: List of allowed media types
    - file_extensions: List of allowed file extensions
    - exclude_media_types: List of excluded media types
    - exclude_file_extensions: List of excluded file extensions
    - include_self_posts: Whether to include text/self posts (default: True)
    - include_link_posts: Whether to include external link posts (default: True)
    - strict_mode: Whether to be strict about unknown types (default: False)
    
    Supported media types:
    - "image": Image files (jpg, png, gif, webp, etc.)
    - "video": Video files (mp4, webm, mov, etc.)  
    - "audio": Audio files (mp3, wav, etc.)
    - "text": Self posts with text content
    - "link": External links
    - "gallery": Reddit galleries
    - "poll": Reddit polls
    - "crosspost": Crossposts
    """
    
    # Predefined media type mappings
    IMAGE_EXTENSIONS = {
        'jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'tiff', 'tif', 'svg',
        'ico', 'heic', 'heif', 'avif', 'jfif'
    }
    
    VIDEO_EXTENSIONS = {
        'mp4', 'webm', 'mov', 'avi', 'mkv', 'flv', 'wmv', 'm4v', 'mpg',
        'mpeg', '3gp', 'ogv', 'gifv'
    }
    
    AUDIO_EXTENSIONS = {
        'mp3', 'wav', 'flac', 'aac', 'ogg', 'wma', 'm4a', 'opus'
    }
    
    IMAGE_DOMAINS = {
        'i.redd.it', 'i.imgur.com', 'imgur.com', 'i.postimg.cc', 'preview.redd.it'
    }
    
    VIDEO_DOMAINS = {
        'v.redd.it', 'gfycat.com', 'redgifs.com', 'streamable.com'
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the media type filter.
        
        Args:
            config: Configuration dictionary with media type criteria
        """
        super().__init__(config)
        
        # Media type lists
        self.media_types = set(self.config.get('media_types', []))
        self.file_extensions = set(ext.lower().lstrip('.') for ext in self.config.get('file_extensions', []))
        self.exclude_media_types = set(self.config.get('exclude_media_types', []))
        self.exclude_file_extensions = set(ext.lower().lstrip('.') for ext in self.config.get('exclude_file_extensions', []))
        
        # Content inclusion options
        self.include_self_posts = self.config.get('include_self_posts', True)
        self.include_link_posts = self.config.get('include_link_posts', True)
        self.strict_mode = self.config.get('strict_mode', False)
    
    @property
    def name(self) -> str:
        """Human-readable name of the filter."""
        return "Media Type Filter"
    
    @property
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        criteria = []
        
        if self.media_types:
            criteria.append(f"allowed types: {', '.join(sorted(self.media_types))}")
        
        if self.file_extensions:
            criteria.append(f"allowed extensions: {', '.join(sorted(self.file_extensions))}")
        
        if self.exclude_media_types:
            criteria.append(f"excluded types: {', '.join(sorted(self.exclude_media_types))}")
        
        if self.exclude_file_extensions:
            criteria.append(f"excluded extensions: {', '.join(sorted(self.exclude_file_extensions))}")
        
        if criteria:
            return f"Posts with {', '.join(criteria)}"
        else:
            return "No media type filtering (all posts pass)"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the media type filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        start_time = time.time()
        
        try:
            # If no filtering criteria specified, pass all posts
            if not any([self.media_types, self.file_extensions, 
                       self.exclude_media_types, self.exclude_file_extensions]):
                return FilterResult(
                    passed=True,
                    reason="No media type filter configured",
                    metadata={
                        "detected_type": self._detect_content_type(post),
                        "detected_extension": self._extract_file_extension(post)
                    },
                    execution_time=time.time() - start_time
                )
            
            # Detect content type and file extension
            detected_type = self._detect_content_type(post)
            detected_extension = self._extract_file_extension(post)
            
            # Apply exclusion filters first
            if self.exclude_media_types and detected_type in self.exclude_media_types:
                return FilterResult(
                    passed=False,
                    reason=f"Media type '{detected_type}' is excluded",
                    metadata={
                        "detected_type": detected_type,
                        "detected_extension": detected_extension,
                        "exclude_media_types": list(self.exclude_media_types),
                        "failed_criteria": "excluded_media_type"
                    },
                    execution_time=time.time() - start_time
                )
            
            if self.exclude_file_extensions and detected_extension in self.exclude_file_extensions:
                return FilterResult(
                    passed=False,
                    reason=f"File extension '{detected_extension}' is excluded",
                    metadata={
                        "detected_type": detected_type,
                        "detected_extension": detected_extension,
                        "exclude_file_extensions": list(self.exclude_file_extensions),
                        "failed_criteria": "excluded_file_extension"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply inclusion filters
            type_allowed = True
            extension_allowed = True
            
            if self.media_types:
                type_allowed = detected_type in self.media_types
            
            if self.file_extensions:
                extension_allowed = detected_extension in self.file_extensions
            
            # Handle special cases for self posts and links
            if detected_type == 'text' and not self.include_self_posts:
                return FilterResult(
                    passed=False,
                    reason="Self posts are disabled",
                    metadata={
                        "detected_type": detected_type,
                        "include_self_posts": self.include_self_posts,
                        "failed_criteria": "self_posts_disabled"
                    },
                    execution_time=time.time() - start_time
                )
            
            if detected_type == 'link' and not self.include_link_posts:
                return FilterResult(
                    passed=False,
                    reason="Link posts are disabled",
                    metadata={
                        "detected_type": detected_type,
                        "include_link_posts": self.include_link_posts,
                        "failed_criteria": "link_posts_disabled"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Check if type and extension criteria are met
            if not type_allowed:
                return FilterResult(
                    passed=False,
                    reason=f"Media type '{detected_type}' not in allowed types",
                    metadata={
                        "detected_type": detected_type,
                        "detected_extension": detected_extension,
                        "media_types": list(self.media_types),
                        "failed_criteria": "media_type_not_allowed"
                    },
                    execution_time=time.time() - start_time
                )
            
            if not extension_allowed:
                return FilterResult(
                    passed=False,
                    reason=f"File extension '{detected_extension}' not in allowed extensions",
                    metadata={
                        "detected_type": detected_type,
                        "detected_extension": detected_extension,
                        "file_extensions": list(self.file_extensions),
                        "failed_criteria": "file_extension_not_allowed"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Handle strict mode for unknown types
            if self.strict_mode and detected_type == 'unknown':
                return FilterResult(
                    passed=False,
                    reason="Unknown content type in strict mode",
                    metadata={
                        "detected_type": detected_type,
                        "detected_extension": detected_extension,
                        "strict_mode": self.strict_mode,
                        "failed_criteria": "unknown_type_strict_mode"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Post passed all media type criteria
            return FilterResult(
                passed=True,
                reason=f"Media type '{detected_type}' matches criteria",
                metadata={
                    "detected_type": detected_type,
                    "detected_extension": detected_extension,
                    "media_types": list(self.media_types),
                    "file_extensions": list(self.file_extensions)
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Error applying media type filter to post {getattr(post, 'id', 'unknown')}: {e}")
            return FilterResult(
                passed=False,
                reason=f"Filter error: {e}",
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def _detect_content_type(self, post: PostMetadata) -> str:
        """
        Detect the content type of a post.
        
        Args:
            post: Reddit post metadata
            
        Returns:
            Content type string
        """
        # Check if it's a self post
        if getattr(post, 'is_self', False) or not getattr(post, 'url', ''):
            return 'text'
        
        # Check for gallery posts
        if hasattr(post, 'gallery_image_urls') and getattr(post, 'gallery_image_urls'):
            return 'gallery'
        
        # Check for poll posts
        if hasattr(post, 'poll_data') and getattr(post, 'poll_data'):
            return 'poll'
        
        # Check for crosspost
        if hasattr(post, 'crosspost_parent_id') and getattr(post, 'crosspost_parent_id'):
            return 'crosspost'
        
        url = getattr(post, 'url', '') or ''
        
        # Check by domain
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            if domain in self.IMAGE_DOMAINS:
                return 'image'
            elif domain in self.VIDEO_DOMAINS:
                return 'video'
            elif 'reddit.com' in domain:
                return 'text'  # Self post
            
        except Exception:
            pass
        
        # Check by file extension
        extension = self._extract_file_extension(post)
        if extension:
            if extension in self.IMAGE_EXTENSIONS:
                return 'image'
            elif extension in self.VIDEO_EXTENSIONS:
                return 'video'
            elif extension in self.AUDIO_EXTENSIONS:
                return 'audio'
        
        # Check for video indicators in URL
        if any(indicator in url.lower() for indicator in ['v.redd.it', 'youtube.com', 'youtu.be', 'vimeo.com']):
            return 'video'
        
        # Default to link for external URLs
        if url and not url.startswith('/'):
            return 'link'
        
        return 'unknown'
    
    def _extract_file_extension(self, post: PostMetadata) -> Optional[str]:
        """
        Extract file extension from post URL.
        
        Args:
            post: Reddit post metadata
            
        Returns:
            File extension (without dot) or None
        """
        url = getattr(post, 'url', '') or ''
        
        if not url:
            return None
        
        try:
            # Parse URL and get path
            parsed = urlparse(url)
            path = parsed.path
            
            # Remove query parameters and fragments
            if '?' in path:
                path = path.split('?')[0]
            if '#' in path:
                path = path.split('#')[0]
            
            # Extract extension
            if '.' in path:
                extension = path.split('.')[-1].lower()
                # Validate extension (basic check)
                if len(extension) <= 5 and extension.isalnum():
                    return extension
            
        except Exception:
            pass
        
        return None
    
    def validate_config(self) -> List[str]:
        """
        Validate the media type filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate media type lists
        valid_types = {'image', 'video', 'audio', 'text', 'link', 'gallery', 'poll', 'crosspost', 'unknown'}
        
        if not isinstance(self.config.get('media_types', []), list):
            errors.append("media_types must be a list")
        else:
            for media_type in self.media_types:
                if media_type not in valid_types:
                    errors.append(f"Invalid media type '{media_type}'. Valid types: {', '.join(sorted(valid_types))}")
        
        if not isinstance(self.config.get('exclude_media_types', []), list):
            errors.append("exclude_media_types must be a list")
        else:
            for media_type in self.exclude_media_types:
                if media_type not in valid_types:
                    errors.append(f"Invalid excluded media type '{media_type}'. Valid types: {', '.join(sorted(valid_types))}")
        
        # Validate file extension lists
        if not isinstance(self.config.get('file_extensions', []), list):
            errors.append("file_extensions must be a list")
        
        if not isinstance(self.config.get('exclude_file_extensions', []), list):
            errors.append("exclude_file_extensions must be a list")
        
        # Validate boolean options
        for option_name in ['include_self_posts', 'include_link_posts', 'strict_mode']:
            option_value = self.config.get(option_name)
            if option_value is not None and not isinstance(option_value, bool):
                errors.append(f"{option_name} must be a boolean")
        
        # Check for conflicts
        if self.media_types and self.exclude_media_types:
            conflicts = self.media_types.intersection(self.exclude_media_types)
            if conflicts:
                errors.append(f"Media types cannot be in both include and exclude lists: {', '.join(conflicts)}")
        
        if self.file_extensions and self.exclude_file_extensions:
            conflicts = self.file_extensions.intersection(self.exclude_file_extensions)
            if conflicts:
                errors.append(f"File extensions cannot be in both include and exclude lists: {', '.join(conflicts)}")
        
        return errors
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for the media type filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {
                "media_types": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["image", "video", "audio", "text", "link", "gallery", "poll", "crosspost"]
                    },
                    "description": "List of allowed media types",
                    "examples": [["image", "video"], ["text"]]
                },
                "file_extensions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of allowed file extensions",
                    "examples": [["jpg", "png", "gif"], ["mp4", "webm"]]
                },
                "exclude_media_types": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["image", "video", "audio", "text", "link", "gallery", "poll", "crosspost"]
                    },
                    "description": "List of excluded media types",
                    "examples": [["text"], ["link"]]
                },
                "exclude_file_extensions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of excluded file extensions",
                    "examples": [["gif"], ["exe", "zip"]]
                },
                "include_self_posts": {
                    "type": "boolean",
                    "default": True,
                    "description": "Whether to include text/self posts"
                },
                "include_link_posts": {
                    "type": "boolean",
                    "default": True,
                    "description": "Whether to include external link posts"
                },
                "strict_mode": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether to be strict about unknown types"
                }
            },
            "additionalProperties": False,
            "examples": [
                {"media_types": ["image", "video"]},
                {"exclude_media_types": ["text"]},
                {
                    "media_types": ["image"],
                    "file_extensions": ["jpg", "png"],
                    "strict_mode": True
                }
            ]
        }