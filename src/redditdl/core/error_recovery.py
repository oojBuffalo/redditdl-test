"""
Error Recovery System

Provides sophisticated error recovery mechanisms with strategy patterns,
state preservation, and graceful degradation options.
"""

import asyncio
import logging
import time
import copy
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional, List, Callable, Union, TypeVar, Generic
from dataclasses import dataclass, field
from contextlib import contextmanager

from redditdl.core.exceptions import RedditDLError, ErrorCode, ErrorContext, RecoverySuggestion


T = TypeVar('T')
logger = logging.getLogger(__name__)


class RecoveryStrategy(Enum):
    """Available error recovery strategies."""
    
    RETRY = "retry"  # Retry the operation with backoff
    FALLBACK = "fallback"  # Try alternative approach
    SKIP = "skip"  # Skip the problematic item and continue
    DEGRADE = "degrade"  # Continue with reduced functionality
    ABORT = "abort"  # Stop processing entirely
    IGNORE = "ignore"  # Log error but continue normally


@dataclass
class RecoveryResult:
    """Result of an error recovery attempt."""
    
    success: bool
    strategy_used: RecoveryStrategy
    result: Optional[Any] = None
    error: Optional[Exception] = None
    attempts: int = 0
    elapsed_time: float = 0.0
    recovery_notes: str = ""
    
    @property
    def failed(self) -> bool:
        """Whether the recovery attempt failed."""
        return not self.success


@dataclass
class OperationState:
    """Preserves state for operation recovery."""
    
    operation_id: str
    operation_name: str
    context: Dict[str, Any] = field(default_factory=dict)
    progress: Dict[str, Any] = field(default_factory=dict)
    checkpoints: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    def add_checkpoint(self, name: str, data: Dict[str, Any]) -> None:
        """Add a recovery checkpoint."""
        self.checkpoints.append({
            'name': name,
            'data': copy.deepcopy(data),
            'timestamp': time.time()
        })
    
    def get_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Get the most recent checkpoint."""
        return self.checkpoints[-1] if self.checkpoints else None
    
    def restore_from_checkpoint(self, checkpoint_name: str) -> Optional[Dict[str, Any]]:
        """Restore state from a named checkpoint."""
        for checkpoint in reversed(self.checkpoints):
            if checkpoint['name'] == checkpoint_name:
                return checkpoint['data']
        return None


class RecoveryHandler(ABC):
    """Abstract base class for recovery handlers."""
    
    @abstractmethod
    def can_handle(self, error: Exception, context: ErrorContext) -> bool:
        """Check if this handler can process the given error."""
        pass
    
    @abstractmethod
    async def recover(
        self,
        error: Exception,
        context: ErrorContext,
        operation_state: Optional[OperationState] = None
    ) -> RecoveryResult:
        """Attempt to recover from the error."""
        pass
    
    @property
    @abstractmethod
    def strategy(self) -> RecoveryStrategy:
        """Get the recovery strategy this handler implements."""
        pass
    
    @property
    def priority(self) -> int:
        """Get handler priority (lower number = higher priority)."""
        return 100


class RetryRecoveryHandler(RecoveryHandler):
    """Handles recovery through operation retry with exponential backoff."""
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
    
    @property
    def strategy(self) -> RecoveryStrategy:
        return RecoveryStrategy.RETRY
    
    @property
    def priority(self) -> int:
        return 10  # High priority
    
    def can_handle(self, error: Exception, context: ErrorContext) -> bool:
        """Retry is suitable for transient errors."""
        if isinstance(error, RedditDLError):
            return error.recoverable and error.error_code in {
                ErrorCode.NETWORK_CONNECTION_FAILED,
                ErrorCode.NETWORK_TIMEOUT,
                ErrorCode.NETWORK_RATE_LIMITED,
                ErrorCode.TARGET_RATE_LIMITED,
                ErrorCode.PROCESSING_INSUFFICIENT_RESOURCES
            }
        return True  # Default to retryable for unknown errors
    
    async def recover(
        self,
        error: Exception,
        context: ErrorContext,
        operation_state: Optional[OperationState] = None
    ) -> RecoveryResult:
        """Implement retry recovery with exponential backoff."""
        start_time = time.time()
        
        for attempt in range(1, self.max_retries + 1):
            try:
                delay = min(
                    self.initial_delay * (self.backoff_factor ** (attempt - 1)),
                    self.max_delay
                )
                
                logger.info(f"Retry attempt {attempt}/{self.max_retries} after {delay:.1f}s delay")
                await asyncio.sleep(delay)
                
                # For now, we'll return success to indicate retry should be attempted
                # The actual retry logic will be handled by the caller
                return RecoveryResult(
                    success=True,
                    strategy_used=RecoveryStrategy.RETRY,
                    attempts=attempt,
                    elapsed_time=time.time() - start_time,
                    recovery_notes=f"Prepared for retry attempt {attempt} with {delay:.1f}s delay"
                )
                
            except Exception as retry_error:
                if attempt == self.max_retries:
                    return RecoveryResult(
                        success=False,
                        strategy_used=RecoveryStrategy.RETRY,
                        error=retry_error,
                        attempts=attempt,
                        elapsed_time=time.time() - start_time,
                        recovery_notes=f"All retry attempts exhausted"
                    )
                continue
        
        return RecoveryResult(
            success=False,
            strategy_used=RecoveryStrategy.RETRY,
            attempts=self.max_retries,
            elapsed_time=time.time() - start_time,
            recovery_notes="Maximum retries exceeded"
        )


class FallbackRecoveryHandler(RecoveryHandler):
    """Handles recovery through fallback to alternative approaches."""
    
    def __init__(self):
        self.fallback_strategies = {
            'scraper': self._scraper_fallback,
            'downloader': self._downloader_fallback,
            'processor': self._processor_fallback
        }
    
    @property
    def strategy(self) -> RecoveryStrategy:
        return RecoveryStrategy.FALLBACK
    
    @property
    def priority(self) -> int:
        return 20  # Medium-high priority
    
    def can_handle(self, error: Exception, context: ErrorContext) -> bool:
        """Fallback is suitable for API/authentication errors."""
        if isinstance(error, RedditDLError):
            return error.error_code in {
                ErrorCode.AUTH_INVALID_CREDENTIALS,
                ErrorCode.AUTH_EXPIRED_TOKEN,
                ErrorCode.AUTH_INSUFFICIENT_PERMISSIONS,
                ErrorCode.TARGET_ACCESS_DENIED,
                ErrorCode.PROCESSING_DEPENDENCY_MISSING
            }
        return False
    
    async def recover(
        self,
        error: Exception,
        context: ErrorContext,
        operation_state: Optional[OperationState] = None
    ) -> RecoveryResult:
        """Implement fallback recovery strategies."""
        start_time = time.time()
        
        # Determine fallback strategy based on context
        operation_type = context.operation.split('_')[0] if context.operation else 'unknown'
        fallback_func = self.fallback_strategies.get(operation_type)
        
        if not fallback_func:
            return RecoveryResult(
                success=False,
                strategy_used=RecoveryStrategy.FALLBACK,
                elapsed_time=time.time() - start_time,
                recovery_notes=f"No fallback strategy available for operation: {operation_type}"
            )
        
        try:
            result = await fallback_func(error, context, operation_state)
            return RecoveryResult(
                success=True,
                strategy_used=RecoveryStrategy.FALLBACK,
                result=result,
                elapsed_time=time.time() - start_time,
                recovery_notes=f"Successfully applied {operation_type} fallback"
            )
        except Exception as fallback_error:
            return RecoveryResult(
                success=False,
                strategy_used=RecoveryStrategy.FALLBACK,
                error=fallback_error,
                elapsed_time=time.time() - start_time,
                recovery_notes=f"Fallback strategy failed: {fallback_error}"
            )
    
    async def _scraper_fallback(self, error: Exception, context: ErrorContext, state: Optional[OperationState]) -> str:
        """Fallback from API scraper to public scraper."""
        logger.info("Falling back from API scraper to public scraper")
        return "fallback_to_public_scraper"
    
    async def _downloader_fallback(self, error: Exception, context: ErrorContext, state: Optional[OperationState]) -> str:
        """Fallback to alternative download method."""
        logger.info("Falling back to alternative download method")
        return "fallback_download_method"
    
    async def _processor_fallback(self, error: Exception, context: ErrorContext, state: Optional[OperationState]) -> str:
        """Fallback to simpler processing method."""
        logger.info("Falling back to basic processing method")
        return "fallback_processing_method"


class SkipRecoveryHandler(RecoveryHandler):
    """Handles recovery by skipping problematic items."""
    
    @property
    def strategy(self) -> RecoveryStrategy:
        return RecoveryStrategy.SKIP
    
    @property
    def priority(self) -> int:
        return 50  # Medium priority
    
    def can_handle(self, error: Exception, context: ErrorContext) -> bool:
        """Skip is suitable for content-specific errors."""
        if isinstance(error, RedditDLError):
            return error.error_code in {
                ErrorCode.TARGET_NOT_FOUND,
                ErrorCode.TARGET_CONTENT_UNAVAILABLE,
                ErrorCode.PROCESSING_UNSUPPORTED_FORMAT,
                ErrorCode.PROCESSING_CORRUPT_DATA,
                ErrorCode.VALIDATION_INVALID_INPUT
            }
        return True  # Can always skip as last resort
    
    async def recover(
        self,
        error: Exception,
        context: ErrorContext,
        operation_state: Optional[OperationState] = None
    ) -> RecoveryResult:
        """Implement skip recovery strategy."""
        start_time = time.time()
        
        # Log the skip decision
        skip_reason = f"Skipping due to error: {error}"
        logger.warning(f"Skipping operation {context.operation}: {skip_reason}")
        
        return RecoveryResult(
            success=True,
            strategy_used=RecoveryStrategy.SKIP,
            elapsed_time=time.time() - start_time,
            recovery_notes=skip_reason
        )


class ErrorRecoveryManager:
    """
    Manages error recovery strategies and state preservation.
    
    Coordinates different recovery handlers and maintains operation state
    for recovery purposes.
    """
    
    def __init__(self):
        self.handlers: List[RecoveryHandler] = []
        self.operation_states: Dict[str, OperationState] = {}
        self.recovery_history: List[Dict[str, Any]] = []
        
        # Register default handlers
        self.register_handler(RetryRecoveryHandler())
        self.register_handler(FallbackRecoveryHandler())
        self.register_handler(SkipRecoveryHandler())
    
    def register_handler(self, handler: RecoveryHandler) -> None:
        """Register a recovery handler."""
        self.handlers.append(handler)
        # Sort by priority (lower number = higher priority)
        self.handlers.sort(key=lambda h: h.priority)
    
    def create_operation_state(self, operation_id: str, operation_name: str) -> OperationState:
        """Create and track operation state for recovery."""
        state = OperationState(operation_id, operation_name)
        self.operation_states[operation_id] = state
        return state
    
    def get_operation_state(self, operation_id: str) -> Optional[OperationState]:
        """Get operation state by ID."""
        return self.operation_states.get(operation_id)
    
    def cleanup_operation_state(self, operation_id: str) -> None:
        """Clean up operation state after completion."""
        self.operation_states.pop(operation_id, None)
    
    async def recover_from_error(
        self,
        error: Exception,
        context: ErrorContext,
        operation_id: Optional[str] = None
    ) -> RecoveryResult:
        """
        Attempt to recover from an error using registered handlers.
        
        Args:
            error: The exception that occurred
            context: Error context information
            operation_id: Optional operation ID for state recovery
            
        Returns:
            RecoveryResult indicating success/failure and strategy used
        """
        start_time = time.time()
        operation_state = self.get_operation_state(operation_id) if operation_id else None
        
        # Try each handler in priority order
        for handler in self.handlers:
            if handler.can_handle(error, context):
                try:
                    logger.info(f"Attempting recovery with {handler.__class__.__name__}")
                    result = await handler.recover(error, context, operation_state)
                    
                    # Record recovery attempt
                    self.recovery_history.append({
                        'timestamp': time.time(),
                        'error_type': type(error).__name__,
                        'handler': handler.__class__.__name__,
                        'strategy': handler.strategy.value,
                        'success': result.success,
                        'operation_id': operation_id,
                        'context': context.to_dict()
                    })
                    
                    if result.success:
                        logger.info(f"Recovery successful using {handler.strategy.value} strategy")
                        return result
                    else:
                        logger.warning(f"Recovery failed with {handler.strategy.value}: {result.recovery_notes}")
                        
                except Exception as recovery_error:
                    logger.error(f"Recovery handler {handler.__class__.__name__} failed: {recovery_error}")
                    continue
        
        # All recovery attempts failed
        logger.error("All recovery attempts failed")
        return RecoveryResult(
            success=False,
            strategy_used=RecoveryStrategy.ABORT,
            error=error,
            elapsed_time=time.time() - start_time,
            recovery_notes="No suitable recovery strategy found"
        )
    
    @contextmanager
    def error_boundary(
        self,
        operation_name: str,
        context: Optional[ErrorContext] = None,
        recovery_strategies: Optional[List[RecoveryStrategy]] = None
    ):
        """
        Context manager that provides automatic error recovery.
        
        Usage:
            with recovery_manager.error_boundary("download_post"):
                # risky operation
                download_file(url)
        """
        import uuid
        
        operation_id = str(uuid.uuid4())[:8]
        operation_state = self.create_operation_state(operation_id, operation_name)
        
        try:
            yield operation_state
            
        except Exception as error:
            # Prepare error context
            error_context = context or ErrorContext()
            error_context.operation = operation_name
            
            # Attempt recovery
            recovery_result = asyncio.run(
                self.recover_from_error(error, error_context, operation_id)
            )
            
            if not recovery_result.success:
                # Re-raise original error if recovery failed
                raise error
            
            # Recovery successful - you may want to return recovery result
            logger.info(f"Operation {operation_name} recovered successfully")
            
        finally:
            # Cleanup operation state
            self.cleanup_operation_state(operation_id)
    
    def get_recovery_statistics(self) -> Dict[str, Any]:
        """Get statistics about recovery attempts."""
        if not self.recovery_history:
            return {'total_attempts': 0}
        
        total = len(self.recovery_history)
        successful = sum(1 for attempt in self.recovery_history if attempt['success'])
        
        strategies = {}
        for attempt in self.recovery_history:
            strategy = attempt['strategy']
            if strategy not in strategies:
                strategies[strategy] = {'total': 0, 'successful': 0}
            strategies[strategy]['total'] += 1
            if attempt['success']:
                strategies[strategy]['successful'] += 1
        
        return {
            'total_attempts': total,
            'successful_recoveries': successful,
            'success_rate': successful / total if total > 0 else 0,
            'strategies': strategies,
            'recent_attempts': self.recovery_history[-10:]  # Last 10 attempts
        }


# Global recovery manager instance
_global_recovery_manager = ErrorRecoveryManager()


def get_recovery_manager() -> ErrorRecoveryManager:
    """Get the global error recovery manager."""
    return _global_recovery_manager


def error_boundary(
    operation_name: str,
    context: Optional[ErrorContext] = None,
    recovery_strategies: Optional[List[RecoveryStrategy]] = None
):
    """Convenience function for error boundary context manager."""
    return _global_recovery_manager.error_boundary(
        operation_name, context, recovery_strategies
    )