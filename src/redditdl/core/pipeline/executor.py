"""
Pipeline Executor

Orchestrates the execution of pipeline stages in sequence, managing context flow,
error handling, and stage lifecycle. Provides the main execution engine for the
Pipeline & Filter architectural pattern.
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime

from .interfaces import PipelineStage, PipelineContext, PipelineResult

# Import event types for pipeline event emission
try:
    from core.events.types import PipelineStageEvent, ErrorEvent, StatisticsEvent
    EVENTS_AVAILABLE = True
except ImportError:
    EVENTS_AVAILABLE = False

# Import enhanced error handling
try:
    from core.exceptions import (
        RedditDLError, ProcessingError, ErrorContext, ErrorCode, RecoverySuggestion
    )
    from core.error_recovery import (
        get_recovery_manager, RecoveryStrategy, error_boundary
    )
    from core.error_context import report_error, generate_user_message
    ERROR_HANDLING_AVAILABLE = True
except ImportError:
    ERROR_HANDLING_AVAILABLE = False


@dataclass
class ExecutionMetrics:
    """
    Metrics for pipeline execution tracking.
    
    Attributes:
        total_stages: Total number of stages executed
        successful_stages: Number of stages that completed successfully
        failed_stages: Number of stages that failed
        total_execution_time: Total time for entire pipeline execution
        stage_times: Execution time for each stage
        total_posts_processed: Total number of posts processed
        start_time: Pipeline execution start time
        end_time: Pipeline execution end time
    """
    total_stages: int = 0
    successful_stages: int = 0
    failed_stages: int = 0
    total_execution_time: float = 0.0
    stage_times: Dict[str, float] = field(default_factory=dict)
    total_posts_processed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def add_stage_result(self, stage_name: str, result: PipelineResult) -> None:
        """Add results from a stage execution."""
        self.total_stages += 1
        self.stage_times[stage_name] = result.execution_time
        self.total_posts_processed += result.processed_count
        
        if result.success:
            self.successful_stages += 1
        else:
            self.failed_stages += 1


class PipelineExecutor:
    """
    Orchestrates the execution of pipeline stages.
    
    The executor manages the flow of data through pipeline stages, handles errors,
    and provides monitoring and control capabilities. It implements the core
    execution logic for the Pipeline & Filter pattern.
    
    Key features:
    - Sequential stage execution with context flow
    - Error handling and recovery mechanisms
    - Conditional stage execution based on previous results
    - Performance monitoring and metrics collection
    - Dynamic stage management (add/remove/reorder)
    """
    
    def __init__(self, stages: Optional[List[PipelineStage]] = None, 
                 error_handling: str = "continue",
                 max_concurrent_stages: int = 1):
        """
        Initialize the pipeline executor.
        
        Args:
            stages: Initial list of pipeline stages
            error_handling: Error handling strategy ("halt", "continue", "skip")
            max_concurrent_stages: Maximum number of stages to run concurrently
        """
        self.stages: List[PipelineStage] = stages or []
        self.error_handling = error_handling
        self.max_concurrent_stages = max_concurrent_stages
        self.logger = logging.getLogger("pipeline.executor")
        
        # Execution state
        self._is_running = False
        self._execution_metrics = ExecutionMetrics()
        self._stage_results: List[PipelineResult] = []
        
        # Hooks for extensibility
        self._pre_execution_hooks: List[Callable] = []
        self._post_execution_hooks: List[Callable] = []
        self._stage_hooks: Dict[str, List[Callable]] = {}
        
        # Event emission helpers
        self._emit_events = EVENTS_AVAILABLE
    
    def add_stage(self, stage: PipelineStage, position: int = -1) -> None:
        """
        Add a stage to the pipeline.
        
        Args:
            stage: Pipeline stage to add
            position: Position to insert the stage (-1 for end)
        """
        if position == -1:
            self.stages.append(stage)
        else:
            self.stages.insert(position, stage)
        
        self.logger.info(f"Added stage '{stage.name}' at position {position}")
    
    def remove_stage(self, stage_name: str) -> bool:
        """
        Remove a stage from the pipeline by name.
        
        Args:
            stage_name: Name of the stage to remove
            
        Returns:
            True if stage was removed, False if not found
        """
        for i, stage in enumerate(self.stages):
            if stage.name == stage_name:
                removed_stage = self.stages.pop(i)
                self.logger.info(f"Removed stage '{removed_stage.name}' from position {i}")
                return True
        
        self.logger.warning(f"Stage '{stage_name}' not found for removal")
        return False
    
    def get_stage(self, stage_name: str) -> Optional[PipelineStage]:
        """
        Get a stage by name.
        
        Args:
            stage_name: Name of the stage to find
            
        Returns:
            PipelineStage if found, None otherwise
        """
        for stage in self.stages:
            if stage.name == stage_name:
                return stage
        return None
    
    def reorder_stages(self, stage_names: List[str]) -> bool:
        """
        Reorder stages according to the provided list of names.
        
        Args:
            stage_names: List of stage names in desired order
            
        Returns:
            True if reordering was successful, False otherwise
        """
        if len(stage_names) != len(self.stages):
            self.logger.error("Stage count mismatch in reorder operation")
            return False
        
        # Create mapping of name to stage
        stage_map = {stage.name: stage for stage in self.stages}
        
        # Check all stages exist
        for name in stage_names:
            if name not in stage_map:
                self.logger.error(f"Stage '{name}' not found for reordering")
                return False
        
        # Reorder stages
        self.stages = [stage_map[name] for name in stage_names]
        self.logger.info(f"Reordered stages: {stage_names}")
        return True
    
    async def execute(self, context: PipelineContext) -> ExecutionMetrics:
        """
        Execute the pipeline with the given context.
        
        Args:
            context: Pipeline context to process
            
        Returns:
            ExecutionMetrics: Execution results and performance data
            
        Raises:
            RuntimeError: If pipeline is already running or has critical errors
        """
        if self._is_running:
            raise RuntimeError("Pipeline is already running")
        
        self._is_running = True
        self._execution_metrics = ExecutionMetrics()
        self._stage_results = []
        
        try:
            # Initialize metrics
            self._execution_metrics.start_time = datetime.now()
            pipeline_start_time = time.time()
            
            self.logger.info(f"Starting pipeline execution with {len(self.stages)} stages")
            self.logger.info(f"Initial context: {len(context.posts)} posts")
            
            # Run pre-execution hooks
            await self._run_pre_execution_hooks(context)
            
            # Validate all stages before execution
            validation_errors = self._validate_stages()
            if validation_errors:
                self.logger.error(f"Stage validation failed: {validation_errors}")
                raise RuntimeError(f"Pipeline validation failed: {validation_errors}")
            
            # Execute each stage sequentially
            for i, stage in enumerate(self.stages):
                stage_start_time = time.time()
                
                try:
                    self.logger.info(f"Executing stage {i+1}/{len(self.stages)}: {stage.name}")
                    
                    # Emit stage started event
                    if EVENTS_AVAILABLE:
                        stage_started_event = PipelineStageEvent(
                            stage_name=stage.name,
                            stage_status="started",
                            stage_config=stage.config
                        )
                        await self._emit_pipeline_event_async(context, stage_started_event)
                    
                    # Run stage hooks
                    await self._run_stage_hooks(stage.name, "pre", context)
                    
                    # Execute stage pre-processing
                    await stage.pre_process(context)
                    
                    # Execute main stage processing
                    result = await stage.process(context)
                    result.stage_name = stage.name
                    result.execution_time = time.time() - stage_start_time
                    
                    # Execute stage post-processing
                    await stage.post_process(context, result)
                    
                    # Store stage results in context for other stages
                    context.stage_results[stage.name] = result
                    self._stage_results.append(result)
                    
                    # Update metrics
                    self._execution_metrics.add_stage_result(stage.name, result)
                    
                    # Run stage hooks
                    await self._run_stage_hooks(stage.name, "post", context, result)
                    
                    # Log stage completion
                    self.logger.info(
                        f"Stage '{stage.name}' completed: "
                        f"success={result.success}, "
                        f"processed={result.processed_count}, "
                        f"errors={result.error_count}, "
                        f"time={result.execution_time:.2f}s"
                    )
                    
                    # Emit stage completed event
                    if EVENTS_AVAILABLE:
                        stage_completed_event = PipelineStageEvent(
                            stage_name=stage.name,
                            stage_status="completed" if result.success else "failed",
                            execution_time=result.execution_time,
                            posts_processed=result.processed_count,
                            posts_successful=result.processed_count - result.error_count,
                            posts_failed=result.error_count,
                            stage_config=stage.config,
                            error_message=str(result.errors[0]) if result.errors else "",
                            stage_data=result.data
                        )
                        await self._emit_pipeline_event_async(context, stage_completed_event)
                    
                    # Handle stage errors based on error handling strategy
                    if not result.success:
                        await self._handle_stage_error(stage, result, context)
                    
                except Exception as e:
                    # Handle unexpected stage exceptions with enhanced error handling
                    stage_time = time.time() - stage_start_time
                    
                    # Create error context
                    error_context = ErrorContext(
                        operation=f"pipeline_stage_{stage.name}",
                        stage=stage.name,
                        session_id=getattr(context, 'session_id', None)
                    )
                    
                    # Use enhanced error handling if available
                    if ERROR_HANDLING_AVAILABLE:
                        # Convert generic exception to ProcessingError if not already a RedditDLError
                        if not isinstance(e, RedditDLError):
                            enhanced_error = ProcessingError(
                                message=f"Stage '{stage.name}' execution failed: {str(e)}",
                                error_code=ErrorCode.PROCESSING_OPERATION_FAILED,
                                context=error_context,
                                cause=e
                            )
                            # Add recovery suggestions
                            enhanced_error.add_suggestion(RecoverySuggestion(
                                action="Retry stage execution",
                                description="The stage may have failed due to temporary issues. Try running the pipeline again.",
                                automatic=False,
                                priority=1
                            ))
                            enhanced_error.add_suggestion(RecoverySuggestion(
                                action="Skip this stage",
                                description="Continue pipeline execution without this stage.",
                                automatic=False,
                                priority=2
                            ))
                        else:
                            enhanced_error = e
                        
                        # Report error for analytics and logging
                        report_error(enhanced_error, error_context, level="error")
                        
                        # Attempt error recovery
                        recovery_manager = get_recovery_manager()
                        recovery_result = await recovery_manager.recover_from_error(
                            enhanced_error, error_context
                        )
                        
                        if recovery_result.success and recovery_result.strategy_used == RecoveryStrategy.RETRY:
                            # Retry the stage if recovery suggests it
                            self.logger.info(f"Attempting to retry stage '{stage.name}' after recovery")
                            try:
                                # Execute stage again
                                result = await stage.process(context)
                                result.stage_name = stage.name
                                result.execution_time = time.time() - stage_start_time
                                
                                # If retry succeeds, continue normally
                                if result.success:
                                    self.logger.info(f"Stage '{stage.name}' succeeded on retry")
                                    context.stage_results[stage.name] = result
                                    self._stage_results.append(result)
                                    self._execution_metrics.add_stage_result(stage.name, result)
                                    continue
                            except Exception as retry_error:
                                self.logger.error(f"Stage '{stage.name}' retry failed: {retry_error}")
                                # Fall through to create error result
                        
                        error_to_use = enhanced_error
                    else:
                        error_to_use = e
                    
                    # Create error result
                    error_result = PipelineResult(
                        success=False,
                        stage_name=stage.name,
                        execution_time=stage_time
                    )
                    error_result.add_error(error_to_use)
                    
                    self._stage_results.append(error_result)
                    self._execution_metrics.add_stage_result(stage.name, error_result)
                    
                    self.logger.error(f"Stage '{stage.name}' failed with exception: {error_to_use}")
                    
                    # Emit enhanced error event
                    if EVENTS_AVAILABLE:
                        error_event = ErrorEvent(
                            error_type=type(error_to_use).__name__,
                            error_message=str(error_to_use),
                            error_context=f"Stage: {stage.name}",
                            stage_name=stage.name,
                            recoverable=getattr(error_to_use, 'recoverable', True),
                            stack_trace=getattr(error_to_use, 'stack_trace', str(error_to_use)),
                            additional_info={
                                'stage_config': stage.config,
                                'execution_time': stage_time,
                                'error_code': getattr(error_to_use, 'error_code', None)
                            }
                        )
                        await self._emit_pipeline_event_async(context, error_event)
                        
                        # Also emit a failed stage event
                        stage_failed_event = PipelineStageEvent(
                            stage_name=stage.name,
                            stage_status="failed",
                            execution_time=stage_time,
                            error_message=str(error_to_use),
                            stage_config=stage.config
                        )
                        await self._emit_pipeline_event_async(context, stage_failed_event)
                    
                    await self._handle_stage_error(stage, error_result, context)
            
            # Calculate final metrics
            self._execution_metrics.total_execution_time = time.time() - pipeline_start_time
            self._execution_metrics.end_time = datetime.now()
            
            # Run post-execution hooks
            await self._run_post_execution_hooks(context, self._execution_metrics)
            
            self.logger.info(
                f"Pipeline execution completed: "
                f"stages={self._execution_metrics.successful_stages}/{self._execution_metrics.total_stages}, "
                f"posts={len(context.posts)}, "
                f"time={self._execution_metrics.total_execution_time:.2f}s"
            )
            
            return self._execution_metrics
            
        finally:
            self._is_running = False
    
    async def _handle_stage_error(self, stage: PipelineStage, result: PipelineResult, 
                                 context: PipelineContext) -> None:
        """
        Handle stage execution errors based on the error handling strategy.
        
        Args:
            stage: The stage that encountered an error
            result: The error result from the stage
            context: The pipeline context
            
        Raises:
            RuntimeError: If error handling strategy is "halt"
        """
        if self.error_handling == "halt":
            self.logger.error(f"Halting pipeline due to error in stage '{stage.name}'")
            raise RuntimeError(f"Pipeline halted due to error in stage '{stage.name}': {result.errors}")
        
        elif self.error_handling == "continue":
            self.logger.warning(f"Continuing pipeline despite error in stage '{stage.name}'")
            # Continue to next stage
        
        elif self.error_handling == "skip":
            self.logger.warning(f"Skipping remaining processing due to error in stage '{stage.name}'")
            # Skip remaining stages but don't halt
            return
        
        else:
            self.logger.error(f"Unknown error handling strategy: {self.error_handling}")
            raise RuntimeError(f"Unknown error handling strategy: {self.error_handling}")
    
    def _validate_stages(self) -> List[str]:
        """
        Validate all stages in the pipeline.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        if not self.stages:
            errors.append("No stages configured in pipeline")
        
        stage_names = set()
        for stage in self.stages:
            # Check for duplicate stage names
            if stage.name in stage_names:
                errors.append(f"Duplicate stage name: {stage.name}")
            stage_names.add(stage.name)
            
            # Validate individual stage configuration
            stage_errors = stage.validate_config()
            if stage_errors:
                errors.extend([f"Stage '{stage.name}': {error}" for error in stage_errors])
        
        return errors
    
    def add_pre_execution_hook(self, hook: Callable) -> None:
        """Add a hook to run before pipeline execution."""
        self._pre_execution_hooks.append(hook)
    
    def add_post_execution_hook(self, hook: Callable) -> None:
        """Add a hook to run after pipeline execution."""
        self._post_execution_hooks.append(hook)
    
    def add_stage_hook(self, stage_name: str, when: str, hook: Callable) -> None:
        """
        Add a hook to run before or after a specific stage.
        
        Args:
            stage_name: Name of the stage
            when: "pre" or "post"
            hook: Callable hook function
        """
        key = f"{stage_name}_{when}"
        if key not in self._stage_hooks:
            self._stage_hooks[key] = []
        self._stage_hooks[key].append(hook)
    
    async def _run_pre_execution_hooks(self, context: PipelineContext) -> None:
        """Run all pre-execution hooks."""
        for hook in self._pre_execution_hooks:
            try:
                if asyncio.iscoroutinefunction(hook):
                    await hook(context)
                else:
                    hook(context)
            except Exception as e:
                self.logger.warning(f"Pre-execution hook failed: {e}")
    
    async def _run_post_execution_hooks(self, context: PipelineContext, 
                                       metrics: ExecutionMetrics) -> None:
        """Run all post-execution hooks."""
        for hook in self._post_execution_hooks:
            try:
                if asyncio.iscoroutinefunction(hook):
                    await hook(context, metrics)
                else:
                    hook(context, metrics)
            except Exception as e:
                self.logger.warning(f"Post-execution hook failed: {e}")
    
    def _emit_pipeline_event(self, context: PipelineContext, event) -> None:
        """
        Emit a pipeline event if event system is available and configured.
        
        Args:
            context: Pipeline context containing event emitter
            event: Event instance to emit
        """
        if self._emit_events and EVENTS_AVAILABLE and context.events:
            try:
                context.emit_event(event)
            except Exception as e:
                self.logger.warning(f"Failed to emit pipeline event: {e}")
    
    async def _emit_pipeline_event_async(self, context: PipelineContext, event) -> None:
        """
        Emit a pipeline event asynchronously if event system is available and configured.
        
        Args:
            context: Pipeline context containing event emitter
            event: Event instance to emit
        """
        if self._emit_events and EVENTS_AVAILABLE and context.events:
            try:
                await context.emit_event_async(event)
            except Exception as e:
                self.logger.warning(f"Failed to emit pipeline event: {e}")
    
    async def _run_stage_hooks(self, stage_name: str, when: str, context: PipelineContext,
                              result: Optional[PipelineResult] = None) -> None:
        """Run hooks for a specific stage."""
        key = f"{stage_name}_{when}"
        hooks = self._stage_hooks.get(key, [])
        
        for hook in hooks:
            try:
                if asyncio.iscoroutinefunction(hook):
                    if when == "pre":
                        await hook(context)
                    else:
                        await hook(context, result)
                else:
                    if when == "pre":
                        hook(context)
                    else:
                        hook(context, result)
            except Exception as e:
                self.logger.warning(f"Stage hook failed for {stage_name}_{when}: {e}")
    
    def get_execution_metrics(self) -> ExecutionMetrics:
        """Get the current execution metrics."""
        return self._execution_metrics
    
    def get_stage_results(self) -> List[PipelineResult]:
        """Get results from all executed stages."""
        return self._stage_results
    
    def is_running(self) -> bool:
        """Check if the pipeline is currently running."""
        return self._is_running
    
    def get_stage_names(self) -> List[str]:
        """Get the names of all stages in order."""
        return [stage.name for stage in self.stages]
    
    def clear_stages(self) -> None:
        """Remove all stages from the pipeline."""
        self.stages.clear()
        self.logger.info("Cleared all stages from pipeline")
    
    def __len__(self) -> int:
        """Get the number of stages in the pipeline."""
        return len(self.stages)
    
    def __str__(self) -> str:
        stage_names = [stage.name for stage in self.stages]
        return f"PipelineExecutor({len(self.stages)} stages: {stage_names})"
    
    def __repr__(self) -> str:
        return (f"PipelineExecutor(stages={len(self.stages)}, "
                f"error_handling='{self.error_handling}', "
                f"max_concurrent_stages={self.max_concurrent_stages})")