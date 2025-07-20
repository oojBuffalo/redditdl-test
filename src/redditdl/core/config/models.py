"""
Configuration Models

Pydantic models for type-safe configuration management with validation,
defaults, and comprehensive field documentation.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
import re


class ScrapingConfig(BaseModel):
    """Configuration for Reddit scraping operations."""
    
    # API Mode Settings
    api_mode: bool = Field(
        default=False,
        description="Use Reddit API mode (PRAW) instead of public scraping"
    )
    client_id: Optional[str] = Field(
        default=None,
        description="Reddit API client ID (required for API mode)"
    )
    client_secret: Optional[str] = Field(
        default=None,
        description="Reddit API client secret (required for API mode)"
    )
    user_agent: str = Field(
        default="RedditDL/2.0 by u/redditdl",
        description="User agent string for API requests"
    )
    
    # Authentication Settings
    username: Optional[str] = Field(
        default=None,
        description="Reddit username for authenticated access"
    )
    password: Optional[str] = Field(
        default=None,
        description="Reddit password for authenticated access"
    )
    
    # Rate Limiting
    sleep_interval: float = Field(
        default=1.0,
        ge=0.1,
        le=60.0,
        description="Sleep interval between requests (seconds)"
    )
    api_rate_limit: float = Field(
        default=0.7,
        ge=0.1,
        le=10.0,
        description="Rate limit for API requests (seconds)"
    )
    public_rate_limit: float = Field(
        default=6.1,
        ge=1.0,
        le=30.0,
        description="Rate limit for public scraping (seconds)"
    )
    
    # Request Settings
    timeout: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Request timeout in seconds"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts for failed requests"
    )
    retry_backoff: float = Field(
        default=2.0,
        ge=1.0,
        le=10.0,
        description="Backoff multiplier for retry attempts"
    )
    
    # Content Limits
    post_limit: int = Field(
        default=20,
        ge=1,
        le=10000,
        description="Maximum number of posts to process"
    )
    
    @model_validator(mode='after')
    def validate_api_credentials(self):
        """Validate API credentials when API mode is enabled."""
        if self.api_mode and (not self.client_id or not self.client_secret):
            raise ValueError("API credentials required when api_mode is True")
        return self
    
    @model_validator(mode='after')
    def validate_sleep_interval(self):
        """Adjust sleep interval based on rate limits when in specific modes."""
        # Only adjust sleep interval if it's below the rate limit for the active mode
        if self.api_mode:
            # In API mode, enforce API rate limit
            if self.sleep_interval < self.api_rate_limit:
                self.sleep_interval = self.api_rate_limit
        # Note: Public mode rate limiting is handled in get_effective_sleep_interval()
        # to allow for more flexible configuration
        return self
    


class ProcessingConfig(BaseModel):
    """Configuration for content processing and media handling."""
    
    # Processing Control
    enabled: bool = Field(
        default=False,
        description="Enable post-download processing"
    )
    profile: Optional[str] = Field(
        default=None,
        description="Processing profile preset (lossless, high, medium, low, custom)"
    )
    
    # Download Settings
    chunk_size: int = Field(
        default=8192,
        ge=1024,
        le=1048576,
        description="Download chunk size in bytes"
    )
    concurrent_downloads: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Maximum concurrent downloads"
    )
    
    # Image Processing
    image_format_conversion: bool = Field(
        default=False,
        description="Enable image format conversion"
    )
    target_image_format: str = Field(
        default="jpeg",
        description="Target format for image conversion (jpeg, png, webp)"
    )
    image_quality_adjustment: bool = Field(
        default=False,
        description="Enable image quality adjustment"
    )
    image_quality: int = Field(
        default=85,
        ge=1,
        le=100,
        description="Image quality for lossy compression (1-100)"
    )
    max_image_resolution: Optional[int] = Field(
        default=None,
        ge=100,
        description="Maximum image resolution (pixels on longest side)"
    )
    generate_thumbnails: bool = Field(
        default=False,
        description="Generate thumbnail images"
    )
    thumbnail_size: int = Field(
        default=256,
        ge=64,
        le=1024,
        description="Thumbnail size in pixels"
    )
    
    # Video Processing
    video_format_conversion: bool = Field(
        default=False,
        description="Enable video format conversion (requires FFmpeg)"
    )
    target_video_format: str = Field(
        default="mp4",
        description="Target format for video conversion"
    )
    video_quality_adjustment: bool = Field(
        default=False,
        description="Enable video quality adjustment"
    )
    video_quality_crf: int = Field(
        default=23,
        ge=0,
        le=51,
        description="Video quality CRF value (lower = better quality)"
    )
    max_video_resolution: Optional[str] = Field(
        default=None,
        description="Maximum video resolution (e.g., '1920x1080', '720p')"
    )
    
    # Thumbnail Processing  
    thumbnail_timestamp: str = Field(
        default="00:00:01",
        description="Timestamp for video thumbnail extraction (HH:MM:SS)"
    )
    
    # Metadata Handling
    embed_metadata: bool = Field(
        default=True,
        description="Embed metadata in media files (EXIF for images)"
    )
    create_json_sidecars: bool = Field(
        default=True,
        description="Create JSON sidecar files with metadata"
    )
    preserve_original_metadata: bool = Field(
        default=True,
        description="Preserve original file metadata when processing"
    )
    
    @field_validator('target_image_format')
    @classmethod
    def validate_image_format(cls, v):
        """Validate image format is supported."""
        supported_formats = {'jpeg', 'jpg', 'png', 'webp', 'bmp', 'tiff'}
        if v.lower() not in supported_formats:
            raise ValueError(f"Unsupported image format: {v}")
        return v.lower()
    
    @field_validator('target_video_format')
    @classmethod
    def validate_video_format(cls, v):
        """Validate video format is supported."""
        supported_formats = {'mp4', 'avi', 'mkv', 'webm', 'mov'}
        if v.lower() not in supported_formats:
            raise ValueError(f"Unsupported video format: {v}")
        return v.lower()
    
    @field_validator('profile')
    @classmethod
    def validate_profile(cls, v):
        """Validate processing profile is supported."""
        if v is not None:
            supported_profiles = {'lossless', 'high', 'medium', 'low', 'custom'}
            if v.lower() not in supported_profiles:
                raise ValueError(f"Unsupported processing profile: {v}. Supported: {', '.join(supported_profiles)}")
            return v.lower()
        return v
    
    @field_validator('thumbnail_timestamp')
    @classmethod
    def validate_thumbnail_timestamp(cls, v):
        """Validate thumbnail timestamp format."""
        import re
        pattern = r'^(\d{2}):(\d{2}):(\d{2})$'
        if not re.match(pattern, v):
            raise ValueError("Thumbnail timestamp must be in HH:MM:SS format")
        return v
    
    @model_validator(mode='after')
    def apply_profile_settings(self):
        """Apply profile settings if profile is specified."""
        if self.profile and self.profile != 'custom':
            self._apply_profile_preset(self.profile)
        return self
    
    def _apply_profile_preset(self, profile: str) -> None:
        """Apply settings for a specific processing profile."""
        if profile == 'lossless':
            # Lossless profile - no compression, original quality
            self.enabled = True
            self.image_format_conversion = False
            self.image_quality_adjustment = False
            self.video_format_conversion = False
            self.video_quality_adjustment = False
            self.generate_thumbnails = True
            self.thumbnail_size = 512
            self.preserve_original_metadata = True
            
        elif profile == 'high':
            # High quality profile - minimal compression
            self.enabled = True
            self.image_format_conversion = True
            self.target_image_format = 'png'
            self.image_quality_adjustment = True
            self.image_quality = 95
            self.video_format_conversion = True
            self.target_video_format = 'mp4'
            self.video_quality_adjustment = True
            self.video_quality_crf = 18
            self.generate_thumbnails = True
            self.thumbnail_size = 512
            self.preserve_original_metadata = True
            
        elif profile == 'medium':
            # Medium quality profile - balanced compression
            self.enabled = True
            self.image_format_conversion = True
            self.target_image_format = 'jpeg'
            self.image_quality_adjustment = True
            self.image_quality = 85
            self.video_format_conversion = True
            self.target_video_format = 'mp4'
            self.video_quality_adjustment = True
            self.video_quality_crf = 23
            self.generate_thumbnails = True
            self.thumbnail_size = 256
            self.preserve_original_metadata = True
            
        elif profile == 'low':
            # Low quality profile - high compression for storage
            self.enabled = True
            self.image_format_conversion = True
            self.target_image_format = 'jpeg'
            self.image_quality_adjustment = True
            self.image_quality = 65
            self.max_image_resolution = 1920
            self.video_format_conversion = True
            self.target_video_format = 'mp4'
            self.video_quality_adjustment = True
            self.video_quality_crf = 28
            self.max_video_resolution = '1080p'
            self.generate_thumbnails = True
            self.thumbnail_size = 128
            self.preserve_original_metadata = False


class OutputConfig(BaseModel):
    """Configuration for output organization and file naming."""
    
    # Directory Settings
    output_dir: Path = Field(
        default=Path("downloads"),
        description="Base output directory for downloaded files"
    )
    create_subdirs: bool = Field(
        default=True,
        description="Create subdirectories for organization"
    )
    
    # File Naming
    filename_template: str = Field(
        default="{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}",
        description="Jinja2 template for file naming"
    )
    max_filename_length: int = Field(
        default=200,
        ge=50,
        le=500,
        description="Maximum filename length (including path)"
    )
    
    # Organization Patterns
    organize_by_subreddit: bool = Field(
        default=True,
        description="Organize files by subreddit"
    )
    organize_by_date: bool = Field(
        default=False,
        description="Organize files by date (YYYY/MM/DD structure)"
    )
    organize_by_author: bool = Field(
        default=False,
        description="Organize files by post author"
    )
    organize_by_content_type: bool = Field(
        default=False,
        description="Organize files by content type (images, videos, etc.)"
    )
    
    # Export Settings
    export_formats: List[str] = Field(
        default=["json"],
        description="Export formats for metadata (json, csv, sqlite, markdown)"
    )
    export_dir: Optional[Path] = Field(
        default=None,
        description="Directory for exported metadata (defaults to output_dir/exports)"
    )
    export_include_metadata: bool = Field(
        default=True,
        description="Include pipeline metadata in exports"
    )
    export_include_posts: bool = Field(
        default=True,
        description="Include post data in exports"
    )
    export_overwrite: bool = Field(
        default=True,
        description="Overwrite existing export files"
    )
    
    # Format-specific Export Configuration
    export_json_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "indent": 2,
            "sort_keys": True,
            "ensure_ascii": False,
            "compress": False,
            "validate_output": True
        },
        description="JSON export configuration options"
    )
    export_csv_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "delimiter": ",",
            "include_header": True,
            "flatten_nested": True,
            "max_text_length": 1000,
            "encoding": "utf-8"
        },
        description="CSV export configuration options"
    )
    export_sqlite_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "schema_mode": "auto",
            "create_indexes": True,
            "enable_fts": True,
            "journal_mode": "WAL",
            "foreign_keys": True
        },
        description="SQLite export configuration options"
    )
    export_markdown_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "template": "report",
            "include_toc": True,
            "include_statistics": True,
            "group_by": "subreddit",
            "max_posts": 0
        },
        description="Markdown export configuration options"
    )
    
    @field_validator('filename_template')
    @classmethod
    def validate_filename_template(cls, v):
        """Validate Jinja2 template syntax and safety."""
        # Import here to avoid circular imports
        from redditdl.core.templates import FilenameTemplateEngine
        
        # Create template engine for validation
        engine = FilenameTemplateEngine()
        
        # Validate template syntax and safety
        validation_errors = engine.validate_template(v)
        if validation_errors:
            error_msg = "; ".join(validation_errors)
            raise ValueError(f"Template validation failed: {error_msg}")
        
        return v
    
    @field_validator('export_formats')
    @classmethod
    def validate_export_formats(cls, v):
        """Validate export formats are supported."""
        supported_formats = {'json', 'csv', 'sqlite', 'markdown', 'xml'}
        for fmt in v:
            if fmt.lower() not in supported_formats:
                raise ValueError(f"Unsupported export format: {fmt}")
        return [fmt.lower() for fmt in v]


class FilterConfig(BaseModel):
    """Configuration for comprehensive content filtering with all PRD-specified options."""
    
    # Score Filters
    min_score: Optional[int] = Field(
        default=None,
        description="Minimum post score (upvotes - downvotes)"
    )
    max_score: Optional[int] = Field(
        default=None,
        description="Maximum post score"
    )
    
    # Date Filters
    date_from: Optional[Union[str, datetime]] = Field(
        default=None,
        alias="after_date",
        description="Include posts after this date (ISO format, datetime, or relative like '7 days ago')"
    )
    date_to: Optional[Union[str, datetime]] = Field(
        default=None,
        alias="before_date", 
        description="Include posts before this date (ISO format, datetime, or relative)"
    )
    date_after: Optional[Union[str, datetime]] = Field(
        default=None,
        description="Alternative name for date_from"
    )
    date_before: Optional[Union[str, datetime]] = Field(
        default=None,
        description="Alternative name for date_to"
    )
    
    # Keyword Filters
    keywords_include: List[str] = Field(
        default=[],
        alias="include_keywords",
        description="Include posts containing these keywords in title/text"
    )
    keywords_exclude: List[str] = Field(
        default=[],
        alias="exclude_keywords", 
        description="Exclude posts containing these keywords"
    )
    keyword_case_sensitive: bool = Field(
        default=False,
        alias="case_sensitive_keywords",
        description="Use case-sensitive keyword matching"
    )
    keyword_regex_mode: bool = Field(
        default=False,
        description="Treat keywords as regular expressions"
    )
    keyword_whole_words: bool = Field(
        default=False,
        description="Match whole words only (not substrings)"
    )
    
    # Domain Filters
    domains_allow: List[str] = Field(
        default=[],
        alias="allowed_domains",
        description="Allow only these domains (empty = all domains)"
    )
    domains_block: List[str] = Field(
        default=[],
        alias="blocked_domains",
        description="Block these domains"
    )
    domain_case_sensitive: bool = Field(
        default=False,
        description="Use case-sensitive domain matching"
    )
    
    # Media Type Filters
    media_types: List[str] = Field(
        default=[],
        description="Include only these media types (image, video, gif, gallery, text, link, poll, crosspost)"
    )
    exclude_media_types: List[str] = Field(
        default=[],
        description="Exclude these media types"
    )
    file_extensions: List[str] = Field(
        default=[],
        description="Include only these file extensions (e.g., ['jpg', 'png', 'mp4'])"
    )
    exclude_file_extensions: List[str] = Field(
        default=[],
        description="Exclude these file extensions"
    )
    
    # Legacy Media Type Options (for backward compatibility)
    include_images: bool = Field(default=True, description="Include image posts")
    include_videos: bool = Field(default=True, description="Include video posts") 
    include_gifs: bool = Field(default=True, description="Include GIF posts")
    include_galleries: bool = Field(default=True, description="Include gallery posts")
    include_external_links: bool = Field(default=False, description="Include external links")
    include_self_posts: bool = Field(default=True, description="Include text/self posts")
    include_link_posts: bool = Field(default=True, description="Include link posts")
    
    # NSFW Filters
    nsfw_mode: Optional[str] = Field(
        default=None,
        description="NSFW filtering mode: 'include' (default), 'exclude', or 'only'"
    )
    # Legacy NSFW options (for backward compatibility)
    include_nsfw: bool = Field(
        default=True,
        description="Include NSFW content (legacy option)"
    )
    nsfw_only: bool = Field(
        default=False,
        description="Include only NSFW content (legacy option)"
    )
    nsfw_filter: Optional[str] = Field(
        default=None,
        description="Alternative name for nsfw_mode"
    )
    
    # Filter Composition
    filter_composition: str = Field(
        default="and",
        description="How to combine filters: 'and' (all must pass) or 'or' (any must pass)"
    )
    
    @field_validator('min_score', 'max_score')
    @classmethod
    def validate_score_range(cls, v):
        """Validate score values are reasonable."""
        if v is not None and v < -10000:
            raise ValueError('Score must be greater than -10000')
        return v
    
    @field_validator('nsfw_mode', 'nsfw_filter')
    @classmethod
    def validate_nsfw_mode(cls, v):
        """Validate NSFW mode is a valid option."""
        if v is not None:
            valid_modes = {'include', 'exclude', 'only'}
            if v.lower() not in valid_modes:
                raise ValueError(f"NSFW mode must be one of: {', '.join(valid_modes)}")
            return v.lower()
        return v
    
    @field_validator('filter_composition')
    @classmethod
    def validate_filter_composition(cls, v):
        """Validate filter composition is valid."""
        valid_compositions = {'and', 'or'}
        if v.lower() not in valid_compositions:
            raise ValueError(f"Filter composition must be one of: {', '.join(valid_compositions)}")
        return v.lower()
    
    @field_validator('media_types', 'exclude_media_types')
    @classmethod
    def validate_media_types(cls, v):
        """Validate media types are supported."""
        valid_types = {'image', 'video', 'gif', 'gallery', 'text', 'link', 'poll', 'crosspost', 'external'}
        for media_type in v:
            if media_type.lower() not in valid_types:
                raise ValueError(f"Unsupported media type: {media_type}. Valid types: {', '.join(valid_types)}")
        return [t.lower() for t in v]
    
    @field_validator('file_extensions', 'exclude_file_extensions')
    @classmethod
    def validate_file_extensions(cls, v):
        """Validate and normalize file extensions."""
        normalized = []
        for ext in v:
            # Remove leading dot if present and normalize case
            clean_ext = ext.lstrip('.').lower()
            if clean_ext:
                normalized.append(clean_ext)
        return normalized
    
    @model_validator(mode='after')
    def validate_filter_consistency(self):
        """Validate filter settings are consistent and resolve aliases."""
        # Resolve date aliases
        if self.date_after is not None and self.date_from is None:
            self.date_from = self.date_after
        if self.date_before is not None and self.date_to is None:
            self.date_to = self.date_before
            
        # Resolve NSFW mode from legacy options
        if self.nsfw_mode is None and self.nsfw_filter is not None:
            self.nsfw_mode = self.nsfw_filter
        
        if self.nsfw_mode is None:
            # Convert legacy NSFW options to new mode
            if self.nsfw_only:
                self.nsfw_mode = "only"
            elif not self.include_nsfw:
                self.nsfw_mode = "exclude"
            else:
                self.nsfw_mode = "include"
        
        # Validate NSFW settings consistency 
        if self.nsfw_only and not self.include_nsfw:
            raise ValueError("nsfw_only requires include_nsfw to be True")
        
        # Validate score range consistency
        if self.min_score is not None and self.max_score is not None:
            # DEBUG: Print values and types to find the issue
            import traceback
            print(f"DEBUG: About to compare min_score={self.min_score} (type: {type(self.min_score)}) with max_score={self.max_score} (type: {type(self.max_score)})")
            print("DEBUG: Current stack trace:")
            traceback.print_stack()
            
            # Additional defensive check to ensure values are still not None
            try:
                if (self.min_score is not None and self.max_score is not None and 
                    self.max_score <= self.min_score):
                    raise ValueError('max_score must be greater than min_score')
            except TypeError as e:
                print(f"DEBUG: TypeError in score comparison: {e}")
                print(f"DEBUG: min_score={self.min_score}, max_score={self.max_score}")
                traceback.print_exc()
                raise
        
        # Validate date range consistency
        if self.date_from is not None and self.date_to is not None:
            # If both are strings, skip validation (will be handled by filter implementation)
            if isinstance(self.date_from, datetime) and isinstance(self.date_to, datetime):
                if self.date_to <= self.date_from:
                    raise ValueError('date_to must be after date_from')
        
        # Validate conflicting media type filters
        if self.media_types and self.exclude_media_types:
            overlap = set(self.media_types) & set(self.exclude_media_types)
            if overlap:
                raise ValueError(f"Media types cannot be both included and excluded: {', '.join(overlap)}")
        
        if self.file_extensions and self.exclude_file_extensions:
            overlap = set(self.file_extensions) & set(self.exclude_file_extensions)
            if overlap:
                raise ValueError(f"File extensions cannot be both included and excluded: {', '.join(overlap)}")
        
        return self


class AppConfig(BaseModel):
    """Root application configuration model."""
    
    # Metadata
    version: str = Field(default="0.2.0", description="Configuration version")
    created: datetime = Field(default_factory=datetime.now, description="Configuration creation time")
    
    # Core Configuration Sections
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig, description="Scraping configuration")
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig, description="Processing configuration")
    output: OutputConfig = Field(default_factory=OutputConfig, description="Output configuration")
    filters: FilterConfig = Field(default_factory=FilterConfig, description="Filter configuration")
    
    # General Settings
    dry_run: bool = Field(
        default=False,
        description="Execute without downloading files"
    )
    verbose: bool = Field(
        default=False,
        description="Enable verbose logging output"
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode with detailed logging"
    )
    
    # Pipeline Settings
    use_pipeline: bool = Field(
        default=True,
        description="Use modern pipeline architecture"
    )
    max_workers: int = Field(
        default=4,
        ge=1,
        le=20,
        description="Maximum worker threads for pipeline processing"
    )
    
    # Plugin Settings
    enable_plugins: bool = Field(
        default=True,
        description="Enable plugin system"
    )
    plugin_dirs: List[Path] = Field(
        default=[Path("plugins")],
        description="Directories to search for plugins"
    )
    disabled_plugins: List[str] = Field(
        default=[],
        description="List of disabled plugin names"
    )
    
    # UI & Progress Configuration
    ui_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "output_mode": "normal",  # normal, quiet, verbose, json
            "progress_display": None,  # auto-detect: rich, tqdm, simple, none
            "show_individual_progress": True,
            "max_individual_bars": 5,
            "show_eta": True,
            "show_speed": True,
            "show_statistics": True,
            "quiet_mode": False,
            "json_output": None,
            "progress_bar_style": "default",
            "console_width": None,
            "force_color": None,
            "disable_rich": False
        },
        description="UI and progress display configuration"
    )
    
    # Observer Configuration
    observer_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "enabled_observers": ["console", "logging", "statistics"],
            "console_observer": {
                "verbose": True,
                "use_rich": None,  # auto-detect
                "show_timestamps": True
            },
            "logging_observer": {
                "log_file": "redditdl.log",
                "log_level": "INFO",
                "format_string": None
            },
            "statistics_observer": {
                "enable_performance_tracking": True,
                "track_stage_times": True,
                "track_download_speeds": True
            },
            "progress_observer": {
                "enabled": True,
                "fallback_to_tqdm": True,
                "fallback_to_simple": True
            }
        },
        description="Event observer configuration"
    )
    
    # Session Management
    session_dir: Path = Field(
        default=Path(".redditdl"),
        description="Directory for session state and cache"
    )
    auto_resume: bool = Field(
        default=True,
        description="Automatically resume interrupted sessions"
    )
    
    model_config = ConfigDict(
        extra="forbid",  # Forbid extra fields
        validate_assignment=True,  # Validate on assignment
        use_enum_values=True,  # Use enum values in serialization
    )
    
    @field_validator('plugin_dirs')
    @classmethod
    def validate_plugin_dirs(cls, v):
        """Ensure plugin directories exist or can be created."""
        validated_dirs = []
        for dir_path in v:
            path = Path(dir_path)
            if not path.exists():
                try:
                    path.mkdir(parents=True, exist_ok=True)
                except (OSError, PermissionError):
                    # Skip directories that can't be created
                    continue
            validated_dirs.append(path)
        return validated_dirs
    
    def get_effective_sleep_interval(self) -> float:
        """Get the effective sleep interval based on scraping mode."""
        if self.scraping.api_mode:
            return max(self.scraping.sleep_interval, self.scraping.api_rate_limit)
        else:
            return max(self.scraping.sleep_interval, self.scraping.public_rate_limit)
    
    def get_export_dir(self) -> Path:
        """Get the effective export directory."""
        if self.output.export_dir:
            return self.output.export_dir
        return self.output.output_dir / "exports"