"""
Event System for RedditDL Pipeline

This module provides the Observer pattern implementation for decoupled
UI/reporting and rich progress tracking across the RedditDL pipeline.

The event system enables:
- Real-time progress tracking with detailed metrics
- Multiple concurrent observers without blocking pipeline execution
- Thread-safe event delivery with async support
- Event history and replay capabilities
- Extensible observer registration for plugins

Core components:
- Event types hierarchy for all pipeline operations
- EventEmitter for thread-safe event broadcasting
- Standard observers for console, logging, statistics, and JSON output
- Observer lifecycle management and error isolation
"""

from redditdl.core.events.types import (
    BaseEvent,
    PostDiscoveredEvent,
    DownloadStartedEvent,
    DownloadProgressEvent,
    DownloadCompletedEvent,
    PostProcessedEvent,
    FilterAppliedEvent,
    PipelineStageEvent,
    ErrorEvent
)

from redditdl.core.events.emitter import EventEmitter

from redditdl.core.events.observers import (
    Observer,
    ConsoleObserver,
    LoggingObserver,
    StatisticsObserver,
    JSONObserver,
    ProgressObserver
)

__all__ = [
    # Event types
    'BaseEvent',
    'PostDiscoveredEvent', 
    'DownloadStartedEvent',
    'DownloadProgressEvent',
    'DownloadCompletedEvent',
    'PostProcessedEvent',
    'FilterAppliedEvent',
    'PipelineStageEvent',
    'ErrorEvent',
    
    # Event system
    'EventEmitter',
    
    # Observers
    'Observer',
    'ConsoleObserver',
    'LoggingObserver', 
    'StatisticsObserver',
    'JSONObserver',
    'ProgressObserver'
]