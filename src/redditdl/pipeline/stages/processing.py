"""
Processing Pipeline Stage

Handles the processing of Reddit post content through a sophisticated content
handler system. Routes posts to appropriate specialized handlers based on content
type detection, supports plugin-based extensions, and emits processing events.
"""

import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.core.events.types import PostProcessedEvent
from redditdl.core.plugins.manager import PluginManager
from redditdl.scrapers import PostMetadata

# Import enhanced error handling
from redditdl.core.exceptions import (
    RedditDLError, ProcessingError, ConfigurationError, ValidationError,
    ErrorCode, ErrorContext, RecoverySuggestion, processing_error
)
from redditdl.core.error_recovery import get_recovery_manager
from redditdl.core.error_context import report_error

# Import content handler system
from redditdl.content_handlers.base import (
    ContentHandlerRegistry, 
    ContentTypeDetector, 
    handler_registry,
    HandlerResult
)
from redditdl.content_handlers.media import MediaContentHandler
from redditdl.content_handlers.text import TextContentHandler
from redditdl.content_handlers.gallery import GalleryContentHandler
from redditdl.content_handlers.poll import PollContentHandler
from redditdl.content_handlers.crosspost import CrosspostContentHandler
from redditdl.content_handlers.external import ExternalLinksHandler


class ProcessingStage(PipelineStage):
    """
    Pipeline stage for processing Reddit content through specialized handlers.
    
    This stage implements a sophisticated content handler system that:
    - Detects content types automatically
    - Routes posts to appropriate specialized handlers
    - Supports plugin-based handler extensions
    - Provides fallback handling for unknown content types
    - Emits detailed processing events
    - Tracks processing statistics and performance
    
    Configuration options:
    - output_dir: Directory to save processed content
    - sleep_interval: Time to sleep between operations
    - embed_metadata: Whether to embed metadata in files
    - create_sidecars: Whether to create JSON sidecar files
    - filename_template: Template for generating filenames
    - handler_config: Configuration specific to handlers
    - enable_plugins: Whether to load plugin handlers
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("processing", config)
        self._registry: ContentHandlerRegistry = handler_registry
        self._detector: ContentTypeDetector = ContentTypeDetector()
        self._plugin_manager: Optional[PluginManager] = None
        self._handlers_initialized = False
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """
        Process Reddit posts through specialized content handlers.
        
        Args:
            context: Pipeline context containing posts to process
            
        Returns:
            PipelineResult: Results of the processing operation
        """
        result = PipelineResult(stage_name=self.name)
        start_time = time.time()
        recovery_manager = get_recovery_manager()
        
        # Create error context for this operation
        error_context = ErrorContext(
            operation="processing_stage_process",
            stage="processing",
            session_id=context.session_id
        )
        
        try:
            posts_to_process = context.posts
            initial_count = len(posts_to_process)
            
            self.logger.info(f"Processing {initial_count} posts through content handlers")
            
            if initial_count == 0:
                result.add_warning("No posts to process")
                return result
            
            # Initialize handlers if not already done with error handling
            try:
                await self._ensure_handlers_initialized(context)
            except Exception as e:
                config_error = ConfigurationError(
                    message="Failed to initialize content handlers",
                    error_code=ErrorCode.CONFIG_INVALID_VALUE,
                    context=error_context,
                    cause=e
                )
                
                config_error.add_suggestion(RecoverySuggestion(
                    action="Check handler configuration",
                    description="Verify content handler settings and plugin availability",
                    automatic=False,
                    priority=1
                ))
                
                report_error(config_error, error_context)
                result.add_error(config_error.get_user_message())
                return result
            
            # Get output directory with error handling
            try:
                output_dir = self._get_output_directory(context)
            except Exception as e:
                config_error = ConfigurationError(
                    message="Failed to get output directory for processing",
                    error_code=ErrorCode.CONFIG_INVALID_VALUE,
                    context=error_context,
                    cause=e
                )
                
                config_error.add_suggestion(RecoverySuggestion(
                    action="Check output directory configuration",
                    description="Verify output directory path exists and is writable",
                    automatic=False,
                    priority=1
                ))
                
                report_error(config_error, error_context)
                result.add_error(config_error.get_user_message())
                return result
            
            # Processing statistics
            successful_processing = 0
            failed_processing = 0
            skipped_processing = 0
            handler_stats = {}
            processing_errors = 0
            
            for i, post in enumerate(posts_to_process, 1):
                post_id = getattr(post, 'id', f'post_{i}')
                post_title = getattr(post, 'title', 'Unknown title')[:50]
                
                post_error_context = ErrorContext(
                    operation="process_post",
                    stage="processing",
                    post_id=post_id,
                    session_id=context.session_id
                )
                
                try:
                    self.logger.info(f"[{i}/{initial_count}] Processing post {post_id}: {post_title}...")
                    
                    # Detect content type with error handling
                    try:
                        content_type = self._detector.detect_content_type(post)
                        self.logger.debug(f"Detected content type for {post_id}: {content_type}")
                    except Exception as e:
                        processing_error_obj = ProcessingError(
                            message=f"Content type detection failed for post {post_id}",
                            error_code=ErrorCode.PROCESSING_INVALID_CONTENT,
                            context=post_error_context,
                            cause=e
                        )
                        
                        report_error(processing_error_obj, post_error_context, level="warning")
                        self.logger.warning(f"Content type detection failed for {post_id}, using default: {e}")
                        content_type = "unknown"
                    
                    # Find appropriate handler
                    handler = self._registry.get_handler_for_post(post, content_type)
                    
                    if not handler:
                        validation_error = ValidationError(
                            message=f"No handler found for post {post_id} (content type: {content_type})",
                            error_code=ErrorCode.PROCESSING_UNSUPPORTED_FORMAT,
                            field_name="content_type",
                            field_value=content_type,
                            context=post_error_context
                        )
                        
                        validation_error.add_suggestion(RecoverySuggestion(
                            action="Check content handlers",
                            description="Ensure appropriate content handlers are available for this content type",
                            automatic=False,
                            priority=1
                        ))
                        
                        report_error(validation_error, post_error_context, level="warning")
                        self.logger.warning(f"No handler found for post {post_id} (type: {content_type})")
                        skipped_processing += 1
                        continue
                    
                    # Track handler usage
                    handler_name = handler.name
                    if handler_name not in handler_stats:
                        handler_stats[handler_name] = {'count': 0, 'success': 0, 'failed': 0, 'errors': 0}
                    handler_stats[handler_name]['count'] += 1
                    
                    # Process the post with error recovery
                    try:
                        handler_config = self._build_handler_config(context, content_type)
                        handler_result = await handler.process(post, output_dir, handler_config)
                        
                        # Emit PostProcessedEvent
                        await self._emit_post_processed_event(context, post, handler_result, content_type)
                        
                        if handler_result.success:
                            successful_processing += 1
                            handler_stats[handler_name]['success'] += 1
                            self.logger.info(f"✓ Processed by {handler_name}: {len(handler_result.files_created)} files created")
                        else:
                            # Handler reported failure
                            handler_error = ProcessingError(
                                message=f"Handler {handler_name} failed for post {post_id}: {handler_result.error_message}",
                                error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
                                context=post_error_context
                            )
                            
                            # Attempt recovery
                            recovery_result = await recovery_manager.recover_from_error(handler_error, post_error_context)
                            
                            if recovery_result.success:
                                # Retry might be handled by the recovery system
                                self.logger.info(f"Processing recovered for post {post_id}")
                                successful_processing += 1
                                handler_stats[handler_name]['success'] += 1
                            else:
                                failed_processing += 1
                                handler_stats[handler_name]['failed'] += 1
                                report_error(handler_error, post_error_context, level="warning")
                                self.logger.error(f"✗ Processing failed ({handler_name}): {handler_result.error_message}")
                                result.add_error(f"Post {post_id}: {handler_result.error_message}")
                        
                    except Exception as handler_exception:
                        processing_errors += 1
                        handler_stats[handler_name]['errors'] += 1
                        
                        # Create structured error for handler exceptions
                        handler_error = ProcessingError(
                            message=f"Handler {handler_name} raised exception for post {post_id}",
                            error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
                            context=post_error_context,
                            cause=handler_exception
                        )
                        
                        handler_error.add_suggestion(RecoverySuggestion(
                            action="Check handler compatibility",
                            description="Verify the handler can process this content type",
                            automatic=False,
                            priority=1
                        ))
                        
                        # Attempt recovery
                        recovery_result = await recovery_manager.recover_from_error(handler_error, post_error_context)
                        
                        if recovery_result.success:
                            self.logger.warning(f"Handler error recovered for post {post_id}")
                            successful_processing += 1
                            handler_stats[handler_name]['success'] += 1
                        else:
                            failed_processing += 1
                            handler_stats[handler_name]['failed'] += 1
                            
                            report_error(handler_error, post_error_context)
                            self.logger.error(f"✗ Handler error for post {post_id}: {handler_exception}")
                            result.add_error(f"Handler error for post {post_id}: {str(handler_exception)}")
                    
                except Exception as e:
                    processing_errors += 1
                    
                    # Create processing error for unexpected failures
                    processing_error_obj = processing_error(
                        f"Unexpected error processing post {post_id}: {str(e)}",
                        context=post_error_context, cause=e
                    )
                    
                    # Attempt recovery
                    recovery_result = await recovery_manager.recover_from_error(processing_error_obj, post_error_context)
                    
                    if recovery_result.success:
                        self.logger.warning(f"Processing error recovered for post {post_id}")
                        successful_processing += 1
                    else:
                        failed_processing += 1
                        report_error(processing_error_obj, post_error_context)
                        self.logger.error(f"✗ Error processing post {post_id}: {e}")
                        result.add_error(f"Error processing post {post_id}: {str(e)}")
            
            # Update result with processing statistics
            result.processed_count = initial_count
            result.set_data("successful_processing", successful_processing)
            result.set_data("failed_processing", failed_processing) 
            result.set_data("skipped_processing", skipped_processing)
            result.set_data("processing_errors", processing_errors)
            result.set_data("handler_statistics", handler_stats)
            result.set_data("total_processed", initial_count)
            
            # Add warnings if there were errors
            if processing_errors > 0:
                result.add_warning(f"{processing_errors} posts had processing errors")
            
            # Log final statistics
            self.logger.info(
                f"Processing completed: {successful_processing} successful, "
                f"{failed_processing} failed, {skipped_processing} skipped"
                f"{f', {processing_errors} errors' if processing_errors > 0 else ''}"
            )
            
            # Log handler usage statistics
            for handler_name, stats in handler_stats.items():
                self.logger.debug(
                    f"Handler {handler_name}: {stats['count']} total, "
                    f"{stats['success']} success, {stats['failed']} failed, {stats.get('errors', 0)} errors"
                )
            
            # Determine overall success
            result.success = successful_processing > 0 or (failed_processing == 0 and skipped_processing > 0)
            if not result.success and failed_processing > 0:
                result.add_error("No posts processed successfully")
            
        except Exception as e:
            # Create comprehensive error for unexpected stage failures
            enhanced_error = processing_error(
                f"Processing stage failed unexpectedly: {str(e)}",
                context=error_context, cause=e
            )
            
            enhanced_error.add_suggestion(RecoverySuggestion(
                action="Check processing configuration",
                description="Verify content handlers, output directory, and processing settings",
                automatic=False,
                priority=1
            ))
            
            report_error(enhanced_error, error_context)
            result.add_error(enhanced_error.get_user_message())
            self.logger.error(f"Processing stage error: {enhanced_error.get_debug_info()}")
        
        result.execution_time = time.time() - start_time
        return result
    
    async def _ensure_handlers_initialized(self, context: PipelineContext) -> None:
        """
        Ensure all content handlers are initialized and registered.
        
        Args:
            context: Pipeline context with configuration
        """
        if self._handlers_initialized:
            return
        
        # Register built-in handlers
        self._registry.register_handler(MediaContentHandler())
        self._registry.register_handler(TextContentHandler())
        self._registry.register_handler(GalleryContentHandler())
        self._registry.register_handler(PollContentHandler())
        self._registry.register_handler(CrosspostContentHandler())
        self._registry.register_handler(ExternalLinksHandler())
        
        # Load plugin handlers if enabled
        if context.get_config("enable_plugins", self.get_config("enable_plugins", True)):
            await self._load_plugin_handlers(context)
        
        self._handlers_initialized = True
        
        # Log registered handlers
        handlers = self._registry.list_all_handlers()
        self.logger.debug(f"Registered {len(handlers)} content handlers:")
        for handler in handlers:
            self.logger.debug(f"  {handler.name} (priority: {handler.priority}) - {list(handler.supported_content_types)}")
    
    async def _load_plugin_handlers(self, context: PipelineContext) -> None:
        """
        Load content handlers from plugins.
        
        Args:
            context: Pipeline context with configuration
        """
        try:
            if not self._plugin_manager:
                self._plugin_manager = PluginManager()
            
            # Get plugin handlers
            plugin_handlers = self._plugin_manager.get_content_handlers()
            
            for handler_info in plugin_handlers:
                handler_class = handler_info.get('handler_class')
                if handler_class:
                    handler = handler_class()
                    self._registry.register_handler(handler)
                    self.logger.debug(f"Loaded plugin handler: {handler.name}")
        
        except Exception as e:
            self.logger.warning(f"Failed to load plugin handlers: {e}")
    
    def _get_output_directory(self, context: PipelineContext) -> Path:
        """
        Get the output directory for processed content.
        
        Args:
            context: Pipeline context with configuration
            
        Returns:
            Path to output directory
        """
        output_dir = context.get_config("output_dir", self.get_config("output_dir", "downloads"))
        return Path(output_dir)
    
    def _build_handler_config(self, context: PipelineContext, content_type: str) -> Dict[str, Any]:
        """
        Build configuration for content handlers.
        
        Args:
            context: Pipeline context with configuration
            content_type: Type of content being processed
            
        Returns:
            Configuration dictionary for handlers
        """
        # Base configuration from context and stage config
        handler_config = {
            'sleep_interval': context.get_config("sleep_interval", self.get_config("sleep_interval", 1.0)),
            'embed_metadata': context.get_config("embed_metadata", self.get_config("embed_metadata", True)),
            'create_sidecars': context.get_config("create_sidecars", self.get_config("create_sidecars", False)),
            'filename_template': context.get_config("filename_template", self.get_config("filename_template")),
            'content_type': content_type
        }
        
        # Add handler-specific configuration
        handler_specific = context.get_config("handler_config", self.get_config("handler_config", {}))
        if content_type in handler_specific:
            handler_config.update(handler_specific[content_type])
        
        return handler_config
    
    async def _emit_post_processed_event(
        self, 
        context: PipelineContext, 
        post: PostMetadata, 
        handler_result: HandlerResult,
        content_type: str
    ) -> None:
        """
        Emit a PostProcessedEvent for the processed post.
        
        Args:
            context: Pipeline context with event emitter
            post: Post that was processed
            handler_result: Result from the content handler
            content_type: Detected content type
        """
        if not context.events:
            return
        
        event = PostProcessedEvent(
            post_id=post.id,
            post_title=post.title,
            processing_stage=self.name,
            success=handler_result.success,
            operations_performed=handler_result.operations_performed,
            metadata_embedded=handler_result.metadata_embedded,
            sidecar_created=handler_result.sidecar_created,
            processing_time=handler_result.processing_time,
            error_message=handler_result.error_message,
            file_paths=[str(path) for path in handler_result.files_created],
            session_id=context.session_id or ""
        )
        
        await context.emit_event_async(event)
    
    def get_handler_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about registered handlers.
        
        Returns:
            Dictionary with handler statistics
        """
        return self._registry.get_handler_stats()
    
    def validate_config(self) -> List[str]:
        """
        Validate the processing stage configuration.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate output directory
        output_dir = self.get_config("output_dir")
        if output_dir:
            try:
                output_path = Path(output_dir)
                # Check if parent directory exists (don't create it here)
                if not output_path.parent.exists():
                    errors.append(f"Parent directory does not exist: {output_path.parent}")
            except Exception as e:
                errors.append(f"Invalid output directory path: {e}")
        
        # Validate sleep interval
        sleep_interval = self.get_config("sleep_interval")
        if sleep_interval is not None and sleep_interval < 0:
            errors.append("sleep_interval must be non-negative")
        
        # Validate embed_metadata flag
        embed_metadata = self.get_config("embed_metadata")
        if embed_metadata is not None and not isinstance(embed_metadata, bool):
            errors.append("embed_metadata must be a boolean")
        
        # Validate create_sidecars flag
        create_sidecars = self.get_config("create_sidecars")
        if create_sidecars is not None and not isinstance(create_sidecars, bool):
            errors.append("create_sidecars must be a boolean")
        
        # Validate enable_plugins flag
        enable_plugins = self.get_config("enable_plugins")
        if enable_plugins is not None and not isinstance(enable_plugins, bool):
            errors.append("enable_plugins must be a boolean")
        
        # Validate handler_config structure
        handler_config = self.get_config("handler_config")
        if handler_config is not None and not isinstance(handler_config, dict):
            errors.append("handler_config must be a dictionary")
        
        return errors
    
    async def pre_process(self, context: PipelineContext) -> None:
        """Pre-processing setup for processing stage."""
        self.logger.debug("Processing stage pre-processing")
        
        # Ensure output directory exists
        output_dir = self._get_output_directory(context)
        
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Output directory ready: {output_dir}")
        except Exception as e:
            self.logger.error(f"Failed to create output directory {output_dir}: {e}")
            raise
        
        # Initialize content handlers
        await self._ensure_handlers_initialized(context)
        
        # Log configuration for debugging
        sleep_interval = context.get_config("sleep_interval", self.get_config("sleep_interval", 1.0))
        embed_metadata = context.get_config("embed_metadata", self.get_config("embed_metadata", True))
        enable_plugins = context.get_config("enable_plugins", self.get_config("enable_plugins", True))
        
        self.logger.debug(
            f"Processing configuration - Output: {output_dir}, "
            f"Sleep: {sleep_interval}s, Embed metadata: {embed_metadata}, "
            f"Plugins: {enable_plugins}"
        )
        
        # Log handler statistics
        stats = self.get_handler_statistics()
        self.logger.debug(f"Content handler system ready: {stats['total_handlers']} handlers registered")
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """Post-processing cleanup for processing stage."""
        self.logger.debug("Processing stage post-processing")
        
        # Store processing metadata in context for other stages
        if result.success:
            context.set_metadata("processing_completed", True)
            context.set_metadata("successful_processing", result.get_data("successful_processing", 0))
            context.set_metadata("failed_processing", result.get_data("failed_processing", 0))
            context.set_metadata("skipped_processing", result.get_data("skipped_processing", 0))
            context.set_metadata("handler_statistics", result.get_data("handler_statistics", {}))
        else:
            context.set_metadata("processing_completed", False)
            
        # Log final statistics
        if result.success:
            successful = result.get_data("successful_processing", 0)
            failed = result.get_data("failed_processing", 0)
            skipped = result.get_data("skipped_processing", 0)
            handler_stats = result.get_data("handler_statistics", {})
            
            self.logger.info(
                f"Processing stage completed - Success: {successful}, "
                f"Failed: {failed}, Skipped: {skipped}"
            )
            
            # Log handler usage summary
            if handler_stats:
                self.logger.info("Handler usage summary:")
                for handler_name, stats in handler_stats.items():
                    self.logger.info(f"  {handler_name}: {stats['success']}/{stats['count']} successful")
        
        # Clean up handler registry if needed
        # Note: We don't clear the registry as it may be reused