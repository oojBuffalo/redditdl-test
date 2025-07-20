"""
Enhanced Acquisition Pipeline Stage

Handles the acquisition of Reddit posts from multiple target sources with
advanced batch processing, concurrent handling, and specialized target support.
Integrates the new target handlers system for improved multi-target processing.
"""

import time
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.core.events.types import PostDiscoveredEvent

# Import enhanced error handling
from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ConfigurationError, ValidationError,
    ErrorCode, ErrorContext, RecoverySuggestion, processing_error, auth_error
)
from redditdl.core.error_recovery import get_recovery_manager
from redditdl.core.error_context import report_error

# Import enhanced target system
from redditdl.targets.resolver import TargetResolver, TargetInfo, TargetType
from redditdl.targets.base_scraper import ScrapingConfig
from redditdl.targets.scrapers import ScraperFactory, ScrapingError, AuthenticationError, TargetNotFoundError
from redditdl.targets.handlers import (
    BatchTargetProcessor, BatchProcessingConfig, TargetProcessingResult,
    TargetHandlerRegistry, ListingType, TimePeriod
)

# Import existing PostMetadata
from redditdl.scrapers import PostMetadata


class AcquisitionStage(PipelineStage):
    """
    Enhanced pipeline stage for acquiring Reddit posts from multiple target types.
    
    This stage provides:
    - Advanced target resolution with support for all Reddit target types
    - Concurrent batch processing of multiple targets with error isolation
    - Specialized target handlers for users, subreddits, saved/upvoted posts
    - Subreddit listing support (hot, new, top, controversial, rising)
    - File-based target loading and mixed target format parsing
    - Event emission for post discovery notifications with detailed progress
    - Rate limiting and authentication handling
    
    Configuration options:
    - targets: List of targets to scrape (users, subreddits, URLs, file paths)
    - target_user: Single username to scrape (legacy compatibility)
    - targets_file: Path to file containing list of targets
    - concurrent_targets: Maximum concurrent target processing (default: 3)
    - listing_type: Default subreddit listing type (hot, new, top, controversial)
    - time_period: Time period for top/controversial listings (hour, day, week, month, year, all)
    - client_id: Reddit API client ID (for PRAW)
    - client_secret: Reddit API client secret (for PRAW)
    - user_agent: User agent string for requests
    - username: Reddit username for authenticated requests
    - password: Reddit password for authenticated requests
    - sleep_interval: Time to sleep between requests
    - post_limit: Maximum number of posts to fetch per target
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("acquisition", config)
        self.target_resolver = TargetResolver()
        self._batch_processor: Optional[BatchTargetProcessor] = None
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """
        Acquire Reddit posts from resolved targets using enhanced batch processing.
        
        Args:
            context: Pipeline context to populate with posts
            
        Returns:
            PipelineResult: Results of the acquisition process
        """
        result = PipelineResult(stage_name=self.name)
        start_time = time.time()
        recovery_manager = get_recovery_manager()
        
        # Create error context for this operation
        error_context = ErrorContext(
            operation="acquisition_stage_process",
            stage="acquisition",
            session_id=context.session_id
        )
        
        try:
            # Build configurations with error handling
            try:
                scraping_config = self._build_scraping_config(context)
                batch_config = self._build_batch_config(context)
            except Exception as e:
                config_error = ConfigurationError(
                    message="Failed to build acquisition stage configuration",
                    error_code=ErrorCode.CONFIG_INVALID_VALUE,
                    context=error_context,
                    cause=e
                )
                report_error(config_error, error_context)
                result.add_error(config_error.get_user_message())
                return result
            
            # Initialize batch processor if needed
            if not self._batch_processor:
                self._batch_processor = BatchTargetProcessor(batch_config, scraping_config)
            
            # Get target strings to process
            target_strings = self._get_targets(context)
            if not target_strings:
                validation_error = ValidationError(
                    message="No targets specified for acquisition",
                    error_code=ErrorCode.VALIDATION_MISSING_FIELD,
                    field_name="targets",
                    context=error_context
                )
                report_error(validation_error, error_context)
                result.add_error(validation_error.get_user_message())
                return result
            
            # Resolve all targets with enhanced metadata
            target_infos = self._resolve_targets_with_metadata(target_strings, context)
            if not target_infos:
                validation_error = ValidationError(
                    message="No valid targets could be resolved",
                    error_code=ErrorCode.VALIDATION_INVALID_INPUT,
                    field_name="targets",
                    context=error_context
                )
                report_error(validation_error, error_context)
                result.add_error(validation_error.get_user_message())
                return result
            
            self.logger.info(f"Processing {len(target_infos)} resolved target(s) with batch processor")
            
            # Process targets using batch processor with error recovery
            try:
                processing_results = await self._batch_processor.process_targets(target_infos)
            except Exception as e:
                # Attempt recovery for batch processing errors
                error_context.operation = "batch_target_processing"
                recovery_result = await recovery_manager.recover_from_error(e, error_context)
                
                if recovery_result.success:
                    # Retry the operation
                    processing_results = await self._batch_processor.process_targets(target_infos)
                else:
                    # Recovery failed, create appropriate error
                    if isinstance(e, (AuthenticationError, TargetNotFoundError)):
                        enhanced_error = auth_error(
                            f"Target processing failed: {str(e)}",
                            context=error_context, cause=e
                        )
                    else:
                        enhanced_error = processing_error(
                            f"Batch target processing failed: {str(e)}",
                            context=error_context, cause=e
                        )
                    
                    report_error(enhanced_error, error_context)
                    result.add_error(enhanced_error.get_user_message())
                    return result
            
            # Process results and collect data
            total_posts = 0
            successful_targets = 0
            processed_targets = []
            
            for processing_result in processing_results:
                if processing_result.success:
                    # Add posts to context
                    if processing_result.posts:
                        context.add_posts(processing_result.posts)
                        total_posts += len(processing_result.posts)
                        
                        # Emit post discovery event
                        await self._emit_post_discovered_event(context, processing_result.target_info, processing_result.posts)
                    
                    successful_targets += 1
                    self.logger.info(f"Successfully processed {processing_result.target_info.target_value}: "
                                   f"{len(processing_result.posts)} posts in {processing_result.processing_time:.2f}s")
                else:
                    # Create structured error for failed target processing
                    target_error_context = ErrorContext(
                        operation="target_processing",
                        stage="acquisition",
                        target=processing_result.target_info.target_value,
                        session_id=context.session_id
                    )
                    
                    target_error = processing_error(
                        f"Target processing failed: {processing_result.error_message}",
                        context=target_error_context
                    )
                    
                    report_error(target_error, target_error_context, level="warning")
                    result.add_error(f"Target {processing_result.target_info.target_value}: {processing_result.error_message}")
                
                # Collect processing metadata
                processed_targets.append({
                    'target': processing_result.target_info.target_value,
                    'type': processing_result.target_info.target_type.value,
                    'posts_count': len(processing_result.posts),
                    'success': processing_result.success,
                    'processing_time': processing_result.processing_time,
                    'error_message': processing_result.error_message,
                    'metadata': processing_result.metadata
                })
            
            # Set result data
            result.processed_count = total_posts
            result.set_data("total_posts_acquired", total_posts)
            result.set_data("targets_total", len(target_infos))
            result.set_data("targets_successful", successful_targets)
            result.set_data("targets_failed", len(target_infos) - successful_targets)
            result.set_data("processed_targets", processed_targets)
            result.set_data("batch_processing_enabled", True)
            
            # Log summary
            if total_posts > 0:
                self.logger.info(f"Batch acquisition completed: {total_posts} posts from "
                               f"{successful_targets}/{len(target_infos)} targets")
            else:
                result.add_warning("No posts were acquired from any targets")
            
        except Exception as e:
            # Create comprehensive error for unexpected failures
            enhanced_error = processing_error(
                f"Acquisition stage failed unexpectedly: {str(e)}",
                context=error_context, cause=e
            )
            
            report_error(enhanced_error, error_context)
            result.add_error(enhanced_error.get_user_message())
            self.logger.error(f"Acquisition stage error: {enhanced_error.get_debug_info()}")
        
        result.execution_time = time.time() - start_time
        return result
    
    def _build_scraping_config(self, context: PipelineContext) -> ScrapingConfig:
        """
        Build scraping configuration from context and stage config.
        
        Args:
            context: Pipeline context with configuration
            
        Returns:
            ScrapingConfig object for scraper initialization
        """
        return ScrapingConfig(
            post_limit=context.get_config("post_limit", self.get_config("post_limit", 20)),
            sleep_interval=context.get_config("sleep_interval", self.get_config("sleep_interval", 1.0)),
            user_agent=context.get_config("user_agent", self.get_config("user_agent", "RedditDL/2.0")),
            timeout=context.get_config("timeout", self.get_config("timeout", 30.0)),
            retries=context.get_config("retries", self.get_config("retries", 3)),
            client_id=context.get_config("client_id", self.get_config("client_id")),
            client_secret=context.get_config("client_secret", self.get_config("client_secret")),
            username=context.get_config("username", self.get_config("username")),
            password=context.get_config("password", self.get_config("password"))
        )
    
    def _build_batch_config(self, context: PipelineContext) -> BatchProcessingConfig:
        """
        Build batch processing configuration from context and stage config.
        
        Args:
            context: Pipeline context with configuration
            
        Returns:
            BatchProcessingConfig object for batch processor initialization
        """
        return BatchProcessingConfig(
            max_concurrent=context.get_config("concurrent_targets", self.get_config("concurrent_targets", 3)),
            rate_limit_delay=context.get_config("rate_limit_delay", self.get_config("rate_limit_delay", 1.0)),
            retry_attempts=context.get_config("retry_attempts", self.get_config("retry_attempts", 3)),
            retry_delay=context.get_config("retry_delay", self.get_config("retry_delay", 2.0)),
            timeout_per_target=context.get_config("timeout_per_target", self.get_config("timeout_per_target", 300.0)),
            fail_fast=context.get_config("fail_fast", self.get_config("fail_fast", False))
        )
    
    def _get_targets(self, context: PipelineContext) -> List[str]:
        """
        Get list of targets to process from context, stage config, and files.
        
        Args:
            context: Pipeline context with configuration
            
        Returns:
            List of target strings to process
        """
        targets = []
        
        # Check for targets file first
        targets_file = context.get_config("targets_file", self.get_config("targets_file"))
        if targets_file:
            targets.extend(self._load_targets_from_file(targets_file))
        
        # Check for multiple targets
        config_targets = context.get_config("targets", self.get_config("targets"))
        if config_targets:
            if isinstance(config_targets, str):
                # Single target as string
                targets.append(config_targets)
            elif isinstance(config_targets, list):
                # Multiple targets as list
                targets.extend(config_targets)
        
        # Fall back to legacy single target_user for backward compatibility
        target_user = context.get_config("target_user", self.get_config("target_user"))
        if target_user:
            targets.append(target_user)
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(targets))
    
    def _load_targets_from_file(self, file_path: str) -> List[str]:
        """
        Load targets from a file.
        
        Args:
            file_path: Path to file containing targets (one per line)
            
        Returns:
            List of target strings loaded from file
        """
        targets = []
        
        error_context = ErrorContext(
            operation="load_targets_from_file",
            stage="acquisition",
            file_path=file_path
        )
        
        try:
            path = Path(file_path)
            if not path.exists():
                config_error = ConfigurationError(
                    message=f"Targets file not found: {file_path}",
                    error_code=ErrorCode.CONFIG_FILE_NOT_FOUND,
                    context=error_context
                )
                report_error(config_error, error_context, level="warning")
                return targets
            
            with open(path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith('#'):
                        targets.append(line)
                        
            self.logger.info(f"Loaded {len(targets)} targets from file: {file_path}")
            
        except PermissionError as e:
            config_error = ConfigurationError(
                message=f"Permission denied reading targets file: {file_path}",
                error_code=ErrorCode.CONFIG_PERMISSION_DENIED,
                context=error_context,
                cause=e
            )
            report_error(config_error, error_context)
        except (UnicodeDecodeError, OSError) as e:
            config_error = ConfigurationError(
                message=f"Error reading targets file: {file_path}",
                error_code=ErrorCode.CONFIG_INVALID_FORMAT,
                context=error_context,
                cause=e
            )
            report_error(config_error, error_context)
        except Exception as e:
            config_error = ConfigurationError(
                message=f"Unexpected error loading targets from file {file_path}: {str(e)}",
                error_code=ErrorCode.CONFIG_INVALID_FORMAT,
                context=error_context,
                cause=e
            )
            report_error(config_error, error_context)
        
        return targets
    
    def _resolve_targets_with_metadata(self, target_strings: List[str], context: PipelineContext) -> List[TargetInfo]:
        """
        Resolve target strings to TargetInfo objects with enhanced metadata.
        
        Args:
            target_strings: List of target strings to resolve
            context: Pipeline context for additional configuration
            
        Returns:
            List of TargetInfo objects with enhanced metadata
        """
        target_infos = []
        
        # Get default listing configuration
        default_listing = context.get_config("listing_type", self.get_config("listing_type", "new"))
        default_time_period = context.get_config("time_period", self.get_config("time_period"))
        
        for target_string in target_strings:
            error_context = ErrorContext(
                operation="resolve_target",
                stage="acquisition",
                target=target_string,
                session_id=context.session_id
            )
            
            try:
                # Resolve basic target info
                target_info = self.target_resolver.resolve_target(target_string)
                
                # Add enhanced metadata for subreddit targets
                if target_info.target_type == TargetType.SUBREDDIT:
                    target_info.metadata.update({
                        'listing_type': default_listing,
                        'time_period': default_time_period
                    })
                
                # Validate target accessibility
                has_api_auth = bool(context.get_config("client_id") and context.get_config("client_secret"))
                validation = self.target_resolver.validate_target_accessibility(target_info, has_api_auth)
                
                if validation['accessible']:
                    target_infos.append(target_info)
                    self.logger.debug(f"Resolved and validated target: {target_info.target_value} ({target_info.target_type.value})")
                else:
                    # Create validation error for inaccessible target
                    validation_error = ValidationError(
                        message=f"Target is not accessible: {target_string}",
                        error_code=ErrorCode.TARGET_ACCESS_DENIED,
                        field_name="target",
                        field_value=target_string,
                        context=error_context
                    )
                    
                    # Add recommendations as recovery suggestions
                    for recommendation in validation['recommendations']:
                        validation_error.add_suggestion(RecoverySuggestion(
                            action="Check target accessibility",
                            description=recommendation,
                            automatic=False,
                            priority=1
                        ))
                    
                    report_error(validation_error, error_context, level="warning")
                    self.logger.warning(f"Target '{target_string}' is not accessible: {'; '.join(validation['recommendations'])}")
                
            except ValueError as e:
                # Create validation error for invalid target format
                validation_error = ValidationError(
                    message=f"Invalid target format: {target_string}",
                    error_code=ErrorCode.TARGET_INVALID_FORMAT,
                    field_name="target",
                    field_value=target_string,
                    context=error_context,
                    cause=e
                )
                
                # Add recovery suggestions
                validation_error.add_suggestion(RecoverySuggestion(
                    action="Check target format",
                    description="Ensure target follows valid format: 'user:username', 'r/subreddit', or 'https://reddit.com/...'",
                    automatic=False,
                    priority=1
                ))
                
                report_error(validation_error, error_context, level="warning")
                self.logger.warning(f"Failed to resolve target '{target_string}': {e}")
            
            except Exception as e:
                # Create processing error for unexpected failures
                processing_error_obj = processing_error(
                    f"Unexpected error resolving target '{target_string}': {str(e)}",
                    context=error_context, cause=e
                )
                
                report_error(processing_error_obj, error_context, level="warning")
                self.logger.warning(f"Unexpected error resolving target '{target_string}': {e}")
        
        return target_infos
    
    async def _emit_post_discovered_event(self, context: PipelineContext, target_info: TargetInfo, posts: List[PostMetadata]) -> None:
        """
        Emit a PostDiscoveredEvent for the acquired posts.
        
        Args:
            context: Pipeline context with event emitter
            target_info: Information about the target that was scraped
            posts: List of posts that were discovered
        """
        if not context.events:
            return
        
        # Create preview of first few posts
        posts_preview = []
        for post in posts[:3]:  # Preview first 3 posts
            posts_preview.append({
                'id': post.id,
                'title': post.title[:100] + ('...' if len(post.title) > 100 else ''),
                'subreddit': post.subreddit,
                'author': post.author
            })
        
        event = PostDiscoveredEvent(
            post_count=len(posts),
            source=target_info.target_value,
            target=target_info.original_input,
            source_type=target_info.target_type.value,
            posts_preview=posts_preview,
            session_id=context.session_id or ""
        )
        
        await context.emit_event_async(event)
    
    def validate_config(self) -> List[str]:
        """
        Validate the enhanced acquisition stage configuration.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate targets
        targets = self.get_config("targets")
        target_user = self.get_config("target_user")
        targets_file = self.get_config("targets_file")
        
        if not targets and not target_user and not targets_file:
            errors.append("At least one target must be specified (targets, target_user, or targets_file)")
        
        # Validate targets file if provided
        if targets_file:
            try:
                path = Path(targets_file)
                if not path.exists():
                    errors.append(f"Targets file does not exist: {targets_file}")
                elif not path.is_file():
                    errors.append(f"Targets file path is not a file: {targets_file}")
            except Exception as e:
                errors.append(f"Invalid targets file path '{targets_file}': {e}")
        
        # Validate batch processing configuration
        concurrent_targets = self.get_config("concurrent_targets")
        if concurrent_targets is not None and (concurrent_targets < 1 or concurrent_targets > 20):
            errors.append("concurrent_targets must be between 1 and 20")
        
        # Validate listing type
        listing_type = self.get_config("listing_type")
        if listing_type is not None:
            try:
                ListingType(listing_type.lower())
            except ValueError:
                valid_types = [lt.value for lt in ListingType]
                errors.append(f"Invalid listing_type '{listing_type}'. Valid options: {valid_types}")
        
        # Validate time period
        time_period = self.get_config("time_period")
        if time_period is not None:
            try:
                TimePeriod(time_period.lower())
            except ValueError:
                valid_periods = [tp.value for tp in TimePeriod]
                errors.append(f"Invalid time_period '{time_period}'. Valid options: {valid_periods}")
        
        # Validate numeric configuration values
        numeric_configs = {
            'sleep_interval': (0, None),
            'post_limit': (1, None),
            'timeout': (1, None),
            'retries': (0, None),
            'rate_limit_delay': (0, None),
            'retry_delay': (0, None),
            'timeout_per_target': (1, None)
        }
        
        for config_name, (min_val, max_val) in numeric_configs.items():
            value = self.get_config(config_name)
            if value is not None:
                if value < min_val:
                    errors.append(f"{config_name} must be >= {min_val}")
                if max_val is not None and value > max_val:
                    errors.append(f"{config_name} must be <= {max_val}")
        
        # Validate target formats if provided
        if targets:
            target_list = targets if isinstance(targets, list) else [targets]
            resolver = TargetResolver()
            
            for target in target_list:
                try:
                    target_info = resolver.resolve_target(target)
                    if target_info.target_type == TargetType.UNKNOWN:
                        errors.append(f"Unknown target format: {target}")
                except ValueError as e:
                    errors.append(f"Invalid target '{target}': {e}")
        
        return errors
    
    async def pre_process(self, context: PipelineContext) -> None:
        """Enhanced pre-processing setup for acquisition stage."""
        self.logger.debug("Enhanced acquisition stage pre-processing")
        
        # Log configuration for debugging
        targets = self._get_targets(context)
        post_limit = context.get_config("post_limit", self.get_config("post_limit", 20))
        concurrent_targets = context.get_config("concurrent_targets", self.get_config("concurrent_targets", 3))
        listing_type = context.get_config("listing_type", self.get_config("listing_type", "new"))
        has_api_auth = bool(context.get_config("client_id") and context.get_config("client_secret"))
        
        self.logger.debug(f"Enhanced Configuration - Targets: {len(targets)}, Limit: {post_limit}, "
                         f"Concurrent: {concurrent_targets}, Listing: {listing_type}, API Auth: {has_api_auth}")
        
        # Early target validation and resolution
        if targets:
            valid_targets = 0
            for target in targets:
                try:
                    target_info = self.target_resolver.resolve_target(target)
                    if target_info.target_type != TargetType.UNKNOWN:
                        valid_targets += 1
                        self.logger.debug(f"Pre-validated target: {target} -> {target_info.target_type.value}")
                except ValueError as e:
                    self.logger.warning(f"Target validation warning for '{target}': {e}")
            
            self.logger.info(f"Pre-processing completed: {valid_targets}/{len(targets)} targets valid")
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """Enhanced post-processing cleanup for acquisition stage."""
        self.logger.debug("Enhanced acquisition stage post-processing")
        
        # Store enhanced acquisition metadata in context for other stages
        if result.success:
            context.set_metadata("acquisition_completed", True)
            context.set_metadata("total_posts_acquired", result.get_data("total_posts_acquired", 0))
            context.set_metadata("targets_total", result.get_data("targets_total", 0))
            context.set_metadata("targets_successful", result.get_data("targets_successful", 0))
            context.set_metadata("targets_failed", result.get_data("targets_failed", 0))
            context.set_metadata("processed_targets", result.get_data("processed_targets", []))
            context.set_metadata("batch_processing_enabled", result.get_data("batch_processing_enabled", False))
        else:
            context.set_metadata("acquisition_completed", False)
        
        # Log final summary
        total_posts = result.get_data("total_posts_acquired", 0)
        successful_targets = result.get_data("targets_successful", 0)
        total_targets = result.get_data("targets_total", 0)
        
        if total_targets > 0:
            self.logger.info(f"Post-processing completed: {total_posts} posts from "
                           f"{successful_targets}/{total_targets} targets")
        
        # Clean up batch processor if needed
        if self._batch_processor:
            # Batch processor cleanup is automatic via garbage collection
            pass