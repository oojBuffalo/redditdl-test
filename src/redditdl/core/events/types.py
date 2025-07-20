"""
Event Types for RedditDL Pipeline

Defines the event hierarchy for all pipeline operations, enabling
rich progress tracking and decoupled observer notifications.
"""

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union


@dataclass
class BaseEvent:
    """
    Base class for all events in the RedditDL pipeline.
    
    Provides common fields for event identification, timing, and session tracking.
    All specific event types should inherit from this class.
    """
    timestamp: float = field(default_factory=time.time)
    session_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    
    @property
    def datetime(self) -> datetime:
        """Get event timestamp as datetime object."""
        return datetime.fromtimestamp(self.timestamp)
    
    @property
    def event_type(self) -> str:
        """Get the event type name."""
        return self.__class__.__name__
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return {
            'event_type': self.event_type,
            'timestamp': self.timestamp,
            'session_id': self.session_id,
            'event_id': self.event_id,
            'datetime': self.datetime.isoformat(),
            **{k: v for k, v in self.__dict__.items() 
               if k not in ['timestamp', 'session_id', 'event_id']}
        }


@dataclass
class PostDiscoveredEvent(BaseEvent):
    """
    Emitted when posts are discovered during the acquisition stage.
    
    Provides information about the discovered posts and their source.
    """
    post_count: int = 0
    source: str = ""
    target: str = ""
    source_type: str = ""  # 'user', 'subreddit', 'saved', 'upvoted'
    posts_preview: List[Dict[str, Any]] = field(default_factory=list)  # First few posts for preview


@dataclass 
class DownloadStartedEvent(BaseEvent):
    """
    Emitted when a media download begins.
    
    Contains download details and expected size information if available.
    """
    post_id: str = ""
    url: str = ""
    filename: str = ""
    expected_size: Optional[int] = None
    content_type: str = ""
    media_type: str = ""  # 'image', 'video', 'audio', 'other'


@dataclass
class DownloadProgressEvent(BaseEvent):
    """
    Emitted during media download to track progress.
    
    Provides real-time progress metrics for download monitoring.
    """
    post_id: str = ""
    url: str = ""
    filename: str = ""
    bytes_downloaded: int = 0
    total_bytes: Optional[int] = None
    download_speed: float = 0.0  # bytes per second
    eta_seconds: Optional[float] = None
    
    @property
    def progress_percentage(self) -> Optional[float]:
        """Calculate download progress as percentage."""
        if self.total_bytes and self.total_bytes > 0:
            return (self.bytes_downloaded / self.total_bytes) * 100
        return None
    
    @property
    def progress_ratio(self) -> Optional[float]:
        """Calculate download progress as ratio (0.0 to 1.0)."""
        if self.total_bytes and self.total_bytes > 0:
            return self.bytes_downloaded / self.total_bytes
        return None


@dataclass
class DownloadCompletedEvent(BaseEvent):
    """
    Emitted when a media download completes (successfully or with failure).
    
    Contains the final result status and any error information.
    """
    post_id: str = ""
    url: str = ""
    filename: str = ""
    success: bool = False
    file_size: int = 0
    duration_seconds: float = 0.0
    average_speed: float = 0.0  # bytes per second
    error_message: str = ""
    local_path: str = ""


@dataclass
class PostProcessedEvent(BaseEvent):
    """
    Emitted when post processing completes.
    
    Includes processing results and any metadata operations performed.
    """
    post_id: str = ""
    post_title: str = ""
    processing_stage: str = ""
    success: bool = False
    operations_performed: List[str] = field(default_factory=list)
    metadata_embedded: bool = False
    sidecar_created: bool = False
    processing_time: float = 0.0
    error_message: str = ""
    file_paths: List[str] = field(default_factory=list)


@dataclass
class FilterAppliedEvent(BaseEvent):
    """
    Emitted when filters are applied to posts.
    
    Provides information about filter results and which posts were affected.
    """
    filter_type: str = ""
    filter_config: Dict[str, Any] = field(default_factory=dict)
    posts_before: int = 0
    posts_after: int = 0
    posts_filtered: int = 0
    filter_criteria: List[str] = field(default_factory=list)
    processing_time: float = 0.0
    
    @property
    def filter_percentage(self) -> float:
        """Calculate percentage of posts filtered out."""
        if self.posts_before > 0:
            return (self.posts_filtered / self.posts_before) * 100
        return 0.0


@dataclass
class PipelineStageEvent(BaseEvent):
    """
    Emitted for pipeline stage lifecycle events.
    
    Tracks stage execution, timing, and results.
    """
    stage_name: str = ""
    stage_status: str = ""  # 'started', 'completed', 'failed', 'skipped'
    execution_time: float = 0.0
    posts_processed: int = 0
    posts_successful: int = 0
    posts_failed: int = 0
    stage_config: Dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    stage_data: Dict[str, Any] = field(default_factory=dict)  # Stage-specific data


@dataclass
class ErrorEvent(BaseEvent):
    """
    Emitted when errors occur during pipeline execution.
    
    Provides comprehensive error information for debugging and recovery.
    """
    error_type: str = ""
    error_message: str = ""
    error_context: str = ""  # Where the error occurred
    stage_name: str = ""
    post_id: str = ""
    url: str = ""
    recoverable: bool = True
    retry_count: int = 0
    stack_trace: str = ""
    additional_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StatisticsEvent(BaseEvent):
    """
    Emitted for periodic statistics updates.
    
    Provides aggregated statistics about pipeline execution.
    """
    total_posts: int = 0
    posts_processed: int = 0
    posts_successful: int = 0
    posts_failed: int = 0
    posts_skipped: int = 0
    downloads_active: int = 0
    downloads_completed: int = 0
    downloads_failed: int = 0
    total_bytes_downloaded: int = 0
    current_download_speed: float = 0.0
    average_download_speed: float = 0.0
    elapsed_time: float = 0.0
    estimated_time_remaining: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.posts_processed > 0:
            return (self.posts_successful / self.posts_processed) * 100
        return 0.0
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_posts > 0:
            return (self.posts_processed / self.total_posts) * 100
        return 0.0


# Type alias for any event type
EventType = Union[
    BaseEvent,
    PostDiscoveredEvent,
    DownloadStartedEvent,
    DownloadProgressEvent,
    DownloadCompletedEvent,
    PostProcessedEvent,
    FilterAppliedEvent,
    PipelineStageEvent,
    ErrorEvent,
    StatisticsEvent
]


# Event type registry for dynamic event handling
EVENT_TYPES = {
    'BaseEvent': BaseEvent,
    'PostDiscoveredEvent': PostDiscoveredEvent,
    'DownloadStartedEvent': DownloadStartedEvent,
    'DownloadProgressEvent': DownloadProgressEvent,
    'DownloadCompletedEvent': DownloadCompletedEvent,
    'PostProcessedEvent': PostProcessedEvent,
    'FilterAppliedEvent': FilterAppliedEvent,
    'PipelineStageEvent': PipelineStageEvent,
    'ErrorEvent': ErrorEvent,
    'StatisticsEvent': StatisticsEvent
}


def create_event_from_dict(event_data: Dict[str, Any]) -> EventType:
    """
    Create an event instance from a dictionary.
    
    Useful for deserializing events from storage or network.
    
    Args:
        event_data: Dictionary containing event data with 'event_type' key
        
    Returns:
        Event instance of the appropriate type
        
    Raises:
        ValueError: If event_type is unknown or data is invalid
    """
    event_type = event_data.get('event_type')
    if not event_type:
        raise ValueError("Event data must contain 'event_type' field")
    
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Unknown event type: {event_type}")
    
    event_class = EVENT_TYPES[event_type]
    
    # Remove metadata fields that are handled by BaseEvent
    event_kwargs = {k: v for k, v in event_data.items() 
                   if k not in ['event_type', 'datetime']}
    
    try:
        return event_class(**event_kwargs)
    except TypeError as e:
        raise ValueError(f"Invalid event data for {event_type}: {e}")