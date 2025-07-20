"""
Pipeline Architecture Interfaces

Abstract base classes and data structures for the Pipeline & Filter pattern.
Defines the contracts that all pipeline stages must implement for consistent
processing and data flow.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union, TYPE_CHECKING
from datetime import datetime
import asyncio
import logging

# Import existing types - will be enhanced in later tasks
from redditdl.scrapers import PostMetadata

# Import event system - TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from redditdl.core.events.emitter import EventEmitter


@dataclass
class PipelineContext:
    """
    Shared context passed between pipeline stages.
    
    Contains all the data and configuration needed for processing Reddit content
    through the pipeline stages. This context flows through each stage, allowing
    stages to access and modify the shared state.
    
    Attributes:
        posts: List of PostMetadata objects being processed
        config: Application configuration (will be enhanced in Task 5)
        session_state: Session state tracking (will be enhanced in Task 6)
        events: Event emitter for notifications (integrated in Task 2)
        metadata: Additional metadata storage for inter-stage communication
        start_time: Pipeline execution start timestamp
        stage_results: Results from previous stages
        state_manager: SQLite state manager for session persistence
        session_id: Current session identifier
    """
    posts: List[PostMetadata] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    session_state: Dict[str, Any] = field(default_factory=dict)
    events: Optional['EventEmitter'] = None  # EventEmitter instance for event system
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    stage_results: Dict[str, Any] = field(default_factory=dict)
    state_manager: Optional[Any] = None  # StateManager instance for persistence
    session_id: Optional[str] = None  # Current session identifier
    targets: List[str] = field(default_factory=list)  # Multi-target support
    targets_file: Optional[str] = None  # Path to file containing target list
    
    def add_posts(self, new_posts: List[PostMetadata]) -> None:
        """Add new posts to the context."""
        self.posts.extend(new_posts)
    
    def filter_posts(self, predicate) -> None:
        """Filter posts based on a predicate function."""
        self.posts = [post for post in self.posts if predicate(post)]
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value with default fallback."""
        return self.config.get(key, default)
    
    def set_config(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self.config[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value with default fallback."""
        return self.metadata.get(key, default)
    
    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value."""
        self.metadata[key] = value
    
    def emit_event(self, event) -> bool:
        """
        Emit an event through the event system if available.
        
        Args:
            event: Event instance to emit
            
        Returns:
            True if event was emitted, False if no event emitter available
        """
        if self.events:
            return self.events.emit(event)
        return False
    
    async def emit_event_async(self, event) -> bool:
        """
        Emit an event asynchronously through the event system if available.
        
        Args:
            event: Event instance to emit
            
        Returns:
            True if event was emitted, False if no event emitter available
        """
        if self.events:
            return await self.events.emit_async(event)
        return False


@dataclass
class PipelineResult:
    """
    Result object returned by each pipeline stage.
    
    Contains information about the stage execution including success status,
    any errors encountered, performance metrics, and stage-specific data.
    
    Attributes:
        success: Whether the stage completed successfully
        stage_name: Name of the stage that produced this result
        processed_count: Number of items processed
        error_count: Number of errors encountered
        errors: List of error messages or exceptions
        execution_time: Time taken to execute the stage in seconds
        data: Stage-specific result data
        warnings: Non-fatal warnings from stage execution
    """
    success: bool = True
    stage_name: str = ""
    processed_count: int = 0
    error_count: int = 0
    errors: List[Union[str, Exception]] = field(default_factory=list)
    execution_time: float = 0.0
    data: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, error: Union[str, Exception]) -> None:
        """Add an error to the result."""
        self.errors.append(error)
        self.error_count += 1
        self.success = False
    
    def add_warning(self, warning: str) -> None:
        """Add a warning to the result."""
        self.warnings.append(warning)
    
    def set_data(self, key: str, value: Any) -> None:
        """Set result data value."""
        self.data[key] = value
    
    def get_data(self, key: str, default: Any = None) -> Any:
        """Get result data value with default fallback."""
        return self.data.get(key, default)


class PipelineStage(ABC):
    """
    Abstract base class for all pipeline stages.
    
    Each stage in the pipeline must inherit from this class and implement
    the process method. Stages should be stateless and reusable, with all
    state passed through the PipelineContext.
    
    The pipeline follows these principles:
    - Each stage is independent and testable
    - Stages communicate through the shared context
    - Stages can be added, removed, or reordered dynamically
    - Error handling is centralized in the executor
    """
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipeline stage.
        
        Args:
            name: Human-readable name for this stage
            config: Stage-specific configuration options
        """
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(f"pipeline.{name}")
    
    @abstractmethod
    async def process(self, context: PipelineContext) -> PipelineResult:
        """
        Process the pipeline context and return results.
        
        This is the main method that each stage must implement. It should:
        1. Read data from the context (posts, config, metadata)
        2. Perform the stage-specific processing
        3. Update the context with results
        4. Return a PipelineResult with execution details
        
        Args:
            context: The shared pipeline context
            
        Returns:
            PipelineResult: Execution results and status
            
        Raises:
            Exception: Any errors during processing (will be caught by executor)
        """
        pass
    
    async def pre_process(self, context: PipelineContext) -> None:
        """
        Optional pre-processing hook called before main processing.
        
        Can be overridden by stages that need setup or validation before
        the main process method.
        
        Args:
            context: The shared pipeline context
        """
        pass
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """
        Optional post-processing hook called after main processing.
        
        Can be overridden by stages that need cleanup or additional
        processing after the main process method.
        
        Args:
            context: The shared pipeline context
            result: The result from the main process method
        """
        pass
    
    def validate_config(self) -> List[str]:
        """
        Validate the stage configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        return []
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value with default fallback."""
        return self.config.get(key, default)
    
    def set_config(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self.config[key] = value
    
    def __str__(self) -> str:
        return f"PipelineStage({self.name})"
    
    def __repr__(self) -> str:
        return f"PipelineStage(name='{self.name}', config={self.config})"