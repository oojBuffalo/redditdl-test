"""
Filter Pipeline Stage

Handles content filtering based on various criteria such as score thresholds,
date ranges, keywords, domains, media types, and NSFW status. This stage
implements the comprehensive filtering logic from the PRD requirements.
"""

import time
from typing import Dict, Any, List, Optional
from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.scrapers import PostMetadata
from redditdl.filters import FilterFactory, FilterChain, FilterComposition

# Import enhanced error handling
from redditdl.core.exceptions import (
    RedditDLError, ProcessingError, ConfigurationError, ValidationError,
    ErrorCode, ErrorContext, RecoverySuggestion, processing_error
)
from redditdl.core.error_recovery import get_recovery_manager
from redditdl.core.error_context import report_error


class FilterStage(PipelineStage):
    """
    Pipeline stage for filtering Reddit posts based on various criteria.
    
    This stage implements comprehensive filtering using the new filter chain system
    with support for all PRD-specified filters including score thresholds, date ranges,
    keywords, domains, media types, and NSFW filtering with AND/OR logic composition.
    
    Configuration options:
    - min_score: Minimum post score threshold
    - max_score: Maximum post score threshold  
    - date_from/date_after: Start date for date range filtering
    - date_to/date_before: End date for date range filtering
    - keywords_include: Keywords that must be present
    - keywords_exclude: Keywords that must not be present
    - domains_allow: Allowed domains list
    - domains_block: Blocked domains list
    - media_types: Allowed media types
    - exclude_media_types: Excluded media types
    - file_extensions: Allowed file extensions
    - exclude_file_extensions: Excluded file extensions
    - nsfw_filter/nsfw_mode: NSFW filtering mode ("include", "exclude", "only")
    - filter_composition: How to combine filters ("and" or "or", default: "and")
    
    Plus all advanced options for each filter type (case sensitivity, regex mode, etc.)
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("filter", config)
        self._filter_chain: Optional[FilterChain] = None
        self._filter_factory = FilterFactory()
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """
        Filter posts in the pipeline context based on configured criteria.
        
        Args:
            context: Pipeline context containing posts to filter
            
        Returns:
            PipelineResult: Results of the filtering process
        """
        result = PipelineResult(stage_name=self.name)
        start_time = time.time()
        recovery_manager = get_recovery_manager()
        
        # Create error context for this operation
        error_context = ErrorContext(
            operation="filter_stage_process",
            stage="filter",
            session_id=context.session_id
        )
        
        try:
            initial_count = len(context.posts)
            self.logger.info(f"Filtering {initial_count} posts")
            
            if initial_count == 0:
                result.add_warning("No posts to filter")
                return result
            
            # Build filter chain based on configuration with error handling
            try:
                filter_chain = self._build_filter_chain(context)
            except Exception as e:
                config_error = ConfigurationError(
                    message="Failed to build filter chain from configuration",
                    error_code=ErrorCode.CONFIG_INVALID_VALUE,
                    context=error_context,
                    cause=e
                )
                
                config_error.add_suggestion(RecoverySuggestion(
                    action="Check filter configuration",
                    description="Verify all filter parameters are valid and properly formatted",
                    automatic=False,
                    priority=1
                ))
                
                report_error(config_error, error_context)
                result.add_error(config_error.get_user_message())
                return result
            
            if not filter_chain:
                self.logger.info("No filters configured, passing all posts through")
                result.processed_count = initial_count
                result.set_data("posts_before_filter", initial_count)
                result.set_data("posts_after_filter", initial_count)
                result.set_data("posts_filtered_out", 0)
                result.set_data("filter_results", [])
                return result
            
            # Apply filter chain to posts with enhanced error handling
            filtered_posts = []
            filtered_out_count = 0
            filter_results = []
            filter_errors = 0
            
            for post in context.posts:
                post_id = getattr(post, 'id', 'unknown')
                post_error_context = ErrorContext(
                    operation="apply_filter_chain",
                    stage="filter",
                    post_id=post_id,
                    session_id=context.session_id
                )
                
                try:
                    # Apply filter chain to post
                    chain_result = filter_chain.apply(post)
                    filter_results.append({
                        "post_id": post_id,
                        "passed": chain_result.passed,
                        "reason": chain_result.reason,
                        "execution_time": chain_result.execution_time
                    })
                    
                    if chain_result.passed:
                        filtered_posts.append(post)
                    else:
                        filtered_out_count += 1
                        self.logger.debug(f"Post {post_id} filtered out: {chain_result.reason}")
                        
                except Exception as e:
                    filter_errors += 1
                    
                    # Create structured error for filter processing failure
                    filter_error = ProcessingError(
                        message=f"Filter processing failed for post {post_id}",
                        error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
                        context=post_error_context,
                        cause=e
                    )
                    
                    filter_error.add_suggestion(RecoverySuggestion(
                        action="Include post by default",
                        description="Post will be included to avoid data loss due to filter error",
                        automatic=True,
                        priority=1
                    ))
                    
                    # Attempt recovery - include post by default to be safe
                    recovery_result = await recovery_manager.recover_from_error(filter_error, post_error_context)
                    
                    if recovery_result.success or recovery_result.strategy_used.value in ['skip', 'ignore']:
                        # Include the post to be safe
                        filtered_posts.append(post)
                        filter_results.append({
                            "post_id": post_id,
                            "passed": True,
                            "reason": f"Filter error - included by default: {str(e)}",
                            "execution_time": 0
                        })
                        
                        report_error(filter_error, post_error_context, level="warning")
                        self.logger.warning(f"Filter error for post {post_id}, including by default: {e}")
                    else:
                        # Recovery failed, exclude post
                        filtered_out_count += 1
                        filter_results.append({
                            "post_id": post_id,
                            "passed": False,
                            "reason": f"Filter error and recovery failed: {str(e)}",
                            "execution_time": 0
                        })
                        
                        report_error(filter_error, post_error_context)
                        self.logger.error(f"Filter error for post {post_id}, excluding: {e}")
            
            # Update context with filtered posts
            context.posts = filtered_posts
            final_count = len(filtered_posts)
            
            # Calculate filter performance metrics
            total_filter_time = sum(r["execution_time"] for r in filter_results)
            avg_filter_time = total_filter_time / len(filter_results) if filter_results else 0
            
            result.processed_count = initial_count
            result.set_data("posts_before_filter", initial_count)
            result.set_data("posts_after_filter", final_count)
            result.set_data("posts_filtered_out", filtered_out_count)
            result.set_data("filter_errors", filter_errors)
            result.set_data("filters_applied", len(filter_chain.filters))
            result.set_data("filter_composition", filter_chain.composition.value)
            result.set_data("filter_results", filter_results)
            result.set_data("total_filter_time", total_filter_time)
            result.set_data("avg_filter_time", avg_filter_time)
            
            # Add warnings if there were errors
            if filter_errors > 0:
                result.add_warning(f"{filter_errors} posts had filter processing errors")
            
            self.logger.info(
                f"Filtering completed: {initial_count} -> {final_count} posts "
                f"({filtered_out_count} filtered out) using {len(filter_chain.filters)} filters "
                f"with {filter_chain.composition.value.upper()} composition"
                f"{f', {filter_errors} errors' if filter_errors > 0 else ''}"
            )
            
        except Exception as e:
            # Create comprehensive error for unexpected failures
            enhanced_error = processing_error(
                f"Filter stage failed unexpectedly: {str(e)}",
                context=error_context, cause=e
            )
            
            enhanced_error.add_suggestion(RecoverySuggestion(
                action="Check filter configuration",
                description="Verify filter settings and try with simpler filter criteria",
                automatic=False,
                priority=1
            ))
            
            report_error(enhanced_error, error_context)
            result.add_error(enhanced_error.get_user_message())
            self.logger.error(f"Filter stage error: {enhanced_error.get_debug_info()}")
        
        result.execution_time = time.time() - start_time
        return result
    
    def _build_filter_chain(self, context: PipelineContext) -> Optional[FilterChain]:
        """
        Build a filter chain based on configuration from context and stage config.
        
        Args:
            context: Pipeline context with configuration
            
        Returns:
            FilterChain instance or None if no filters configured
        """
        try:
            # Merge context config with stage config, with context taking precedence
            merged_config = dict(self.config or {})
            
            # Add context configuration values
            for key in ['min_score', 'max_score', 'date_from', 'date_to', 'date_after', 'date_before',
                       'keywords_include', 'keywords_exclude', 'domains_allow', 'domains_block',
                       'media_types', 'exclude_media_types', 'file_extensions', 'exclude_file_extensions',
                       'nsfw_filter', 'nsfw_mode', 'filter_composition']:
                context_value = context.get_config(key)
                if context_value is not None:
                    merged_config[key] = context_value
            
            # Create filter chain from merged configuration
            filter_chain = self._filter_factory.create_from_cli_args(merged_config)
            
            if filter_chain:
                self.logger.debug(f"Created filter chain with {len(filter_chain.filters)} filters using {filter_chain.composition.value.upper()} composition")
                for i, filter_obj in enumerate(filter_chain.filters):
                    self.logger.debug(f"  Filter {i+1}: {filter_obj.name} - {filter_obj.description}")
                    
            return filter_chain
            
        except Exception as e:
            self.logger.error(f"Error building filter chain: {e}")
            return None
    
    def validate_config(self) -> List[str]:
        """
        Validate the filter stage configuration using the filter factory validation.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        try:
            # Try to create a filter chain from the configuration to validate it
            filter_chain = self._filter_factory.create_from_cli_args(self.config or {})
            
            # If filter chain was created, validate each filter in the chain
            if filter_chain:
                for i, filter_obj in enumerate(filter_chain.filters):
                    filter_errors = filter_obj.validate_config()
                    for error in filter_errors:
                        errors.append(f"Filter {i+1} ({filter_obj.name}): {error}")
            
        except Exception as e:
            errors.append(f"Error validating filter configuration: {e}")
        
        # Additional validation for composition
        composition = self.get_config("filter_composition", "and")
        if composition not in ["and", "or"]:
            errors.append("filter_composition must be 'and' or 'or'")
        
        return errors
    
    async def pre_process(self, context: PipelineContext) -> None:
        """Pre-processing setup for filter stage."""
        self.logger.debug("Filter stage pre-processing")
        
        # Build filter chain early to log configuration
        filter_chain = self._build_filter_chain(context)
        self._filter_chain = filter_chain
        
        if filter_chain:
            self.logger.debug(f"Configured {len(filter_chain.filters)} filters with {filter_chain.composition.value.upper()} composition:")
            for i, filter_obj in enumerate(filter_chain.filters):
                self.logger.debug(f"  {i+1}. {filter_obj.name}: {filter_obj.description}")
        else:
            self.logger.debug("No filters configured")
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """Post-processing cleanup for filter stage."""
        self.logger.debug("Filter stage post-processing")
        
        # Store comprehensive filtering metadata in context for other stages
        if result.success:
            context.set_metadata("filtering_completed", True)
            context.set_metadata("posts_before_filter", result.get_data("posts_before_filter", 0))
            context.set_metadata("posts_after_filter", result.get_data("posts_after_filter", 0))
            context.set_metadata("posts_filtered_out", result.get_data("posts_filtered_out", 0))
            context.set_metadata("filters_applied", result.get_data("filters_applied", 0))
            context.set_metadata("filter_composition", result.get_data("filter_composition", "and"))
            context.set_metadata("total_filter_time", result.get_data("total_filter_time", 0))
            context.set_metadata("avg_filter_time", result.get_data("avg_filter_time", 0))
            
            # Store detailed filter results for debugging/reporting
            filter_results = result.get_data("filter_results", [])
            context.set_metadata("filter_results_summary", {
                "total_posts": len(filter_results),
                "passed_posts": sum(1 for r in filter_results if r["passed"]),
                "failed_posts": sum(1 for r in filter_results if not r["passed"]),
                "avg_execution_time": result.get_data("avg_filter_time", 0)
            })
        else:
            context.set_metadata("filtering_completed", False)
        
        # Clean up filter chain reference
        self._filter_chain = None