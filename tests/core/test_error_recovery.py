"""
Test suite for error recovery system.

Tests recovery manager, recovery strategies, operation state preservation,
and recovery result handling to ensure robust error recovery capabilities.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable, Union, TypeVar, Generic
from contextlib import contextmanager

from redditdl.core.error_recovery import (
    ErrorRecoveryManager, RecoveryStrategy, RecoveryResult, OperationState,
    RetryRecoveryHandler, FallbackRecoveryHandler, SkipRecoveryHandler,
    get_recovery_manager, error_boundary
)
from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ProcessingError, ErrorCode, ErrorContext,
    RecoverySuggestion
)


class TestRecoveryResult:
    """Test RecoveryResult dataclass functionality."""
    
    def test_recovery_result_creation(self):
        """Test RecoveryResult creation with basic fields."""
        result = RecoveryResult(
            success=True,
            strategy=RecoveryStrategy.RETRY,
            message="Recovery successful"
        )
        assert result.success is True
        assert result.strategy == RecoveryStrategy.RETRY
        assert result.message == "Recovery successful"
        assert result.retry_count == 0
        assert result.fallback_used is None
        assert result.metadata == {}
    
    def test_recovery_result_full_creation(self):
        """Test RecoveryResult creation with all fields."""
        metadata = {"attempt": 3, "fallback": "alternative_method"}
        result = RecoveryResult(
            success=False,
            strategy=RecoveryStrategy.FALLBACK,
            message="Fallback attempted",
            retry_count=2,
            fallback_used="backup_service",
            metadata=metadata
        )
        assert result.success is False
        assert result.strategy == RecoveryStrategy.FALLBACK
        assert result.retry_count == 2
        assert result.fallback_used == "backup_service"
        assert result.metadata == metadata


class TestOperationState:
    """Test OperationState functionality for state preservation."""
    
    def test_operation_state_creation(self):
        """Test OperationState creation."""
        state = OperationState(
            operation_id="op123",
            operation_type="download",
            data={"url": "https://example.com", "filename": "test.jpg"}
        )
        assert state.operation_id == "op123"
        assert state.operation_type == "download"
        assert state.data["url"] == "https://example.com"
        assert isinstance(state.created_at, datetime)
    
    def test_operation_state_serialization(self):
        """Test OperationState serialization."""
        state = OperationState(
            operation_id="serialize_test",
            operation_type="process",
            data={"key": "value"}
        )
        
        state_dict = state.to_dict()
        assert state_dict["operation_id"] == "serialize_test"
        assert state_dict["operation_type"] == "process"
        assert state_dict["data"]["key"] == "value"
        assert "created_at" in state_dict
    
    def test_operation_state_from_dict(self):
        """Test OperationState deserialization."""
        timestamp = datetime.now()
        state_dict = {
            "operation_id": "deserialize_test",
            "operation_type": "upload",
            "data": {"test": "data"},
            "created_at": timestamp.isoformat()
        }
        
        state = OperationState.from_dict(state_dict)
        assert state.operation_id == "deserialize_test"
        assert state.operation_type == "upload"
        assert state.data["test"] == "data"


class TestRetryRecoveryHandler:
    """Test retry recovery handler functionality."""
    
    def test_retry_handler_creation(self):
        """Test RetryRecoveryHandler creation."""
        handler = RetryRecoveryHandler(max_retries=5, base_delay=2.0)
        assert handler.max_retries == 5
        assert handler.base_delay == 2.0
        assert handler.backoff_factor == 2.0
        assert handler.max_delay == 60.0
    
    def test_retry_handler_can_handle(self):
        """Test retry handler can handle recoverable errors."""
        handler = RetryRecoveryHandler()
        
        # Recoverable network error
        network_error = NetworkError("Connection failed", recoverable=True)
        assert handler.can_handle(network_error) is True
        
        # Non-recoverable error
        fatal_error = RedditDLError("Fatal error", recoverable=False)
        assert handler.can_handle(fatal_error) is False
    
    @pytest.mark.asyncio
    async def test_retry_handler_success(self):
        """Test successful retry recovery."""
        handler = RetryRecoveryHandler(max_retries=3)
        error = NetworkError("Temporary failure")
        context = ErrorContext(operation="test_retry")
        
        # Mock successful operation on second try
        operation_mock = AsyncMock()
        operation_mock.side_effect = [error, "success"]
        
        with patch.object(handler, '_execute_with_retry', return_value=RecoveryResult(
            success=True,
            strategy=RecoveryStrategy.RETRY,
            message="Retry successful",
            retry_count=1
        )):
            result = await handler.recover(error, context)
            assert result.success is True
            assert result.strategy == RecoveryStrategy.RETRY
            assert result.retry_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_handler_exhaustion(self):
        """Test retry handler when retries are exhausted."""
        handler = RetryRecoveryHandler(max_retries=2)
        error = NetworkError("Persistent failure")
        context = ErrorContext(operation="test_exhaustion")
        
        result = await handler.recover(error, context)
        assert result.success is False
        assert result.retry_count <= handler.max_retries
    
    def test_retry_delay_calculation(self):
        """Test exponential backoff delay calculation."""
        handler = RetryRecoveryHandler(base_delay=1.0, backoff_factor=2.0)
        
        # Test delay progression
        assert handler._calculate_delay(0) == 1.0
        assert handler._calculate_delay(1) == 2.0
        assert handler._calculate_delay(2) == 4.0
        assert handler._calculate_delay(3) == 8.0
    
    def test_retry_delay_max_limit(self):
        """Test retry delay respects maximum limit."""
        handler = RetryRecoveryHandler(base_delay=10.0, max_delay=20.0)
        
        # Should cap at max_delay
        assert handler._calculate_delay(10) <= handler.max_delay


class TestFallbackRecoveryHandler:
    """Test fallback recovery handler functionality."""
    
    def test_fallback_handler_creation(self):
        """Test FallbackRecoveryHandler creation."""
        fallbacks = ["backup_service", "alternative_method"]
        handler = FallbackRecoveryHandler(fallback_options=fallbacks)
        assert handler.fallback_options == fallbacks
    
    def test_fallback_handler_can_handle(self):
        """Test fallback handler determines when to handle errors."""
        handler = FallbackRecoveryHandler(fallback_options=["backup"])
        
        # Should handle processing errors
        processing_error = ProcessingError("Processing failed")
        assert handler.can_handle(processing_error) is True
        
        # Should handle network errors
        network_error = NetworkError("Service unavailable")
        assert handler.can_handle(network_error) is True
    
    @pytest.mark.asyncio
    async def test_fallback_handler_success(self):
        """Test successful fallback recovery."""
        handler = FallbackRecoveryHandler(fallback_options=["backup_service"])
        error = ProcessingError("Primary service failed")
        context = ErrorContext(operation="test_fallback")
        
        result = await handler.recover(error, context)
        assert result.success is True
        assert result.strategy == RecoveryStrategy.FALLBACK
        assert result.fallback_used == "backup_service"
    
    @pytest.mark.asyncio
    async def test_fallback_handler_no_options(self):
        """Test fallback handler with no options available."""
        handler = FallbackRecoveryHandler(fallback_options=[])
        error = ProcessingError("Service failed")
        context = ErrorContext(operation="test_no_fallback")
        
        result = await handler.recover(error, context)
        assert result.success is False
        assert "No fallback options" in result.message


class TestSkipRecoveryHandler:
    """Test skip recovery handler functionality."""
    
    def test_skip_handler_creation(self):
        """Test SkipRecoveryHandler creation."""
        handler = SkipRecoveryHandler()
        assert handler is not None
    
    def test_skip_handler_can_handle(self):
        """Test skip handler determines when to skip operations."""
        handler = SkipRecoveryHandler()
        
        # Should handle validation errors
        validation_error = ProcessingError("Invalid content")
        assert handler.can_handle(validation_error) is True
    
    @pytest.mark.asyncio
    async def test_skip_handler_recovery(self):
        """Test skip recovery functionality."""
        handler = SkipRecoveryHandler()
        error = ProcessingError("Content not processable")
        context = ErrorContext(operation="test_skip", post_id="post123")
        
        result = await handler.recover(error, context)
        assert result.success is True
        assert result.strategy == RecoveryStrategy.SKIP
        assert "Skipping" in result.recovery_notes


class TestErrorRecoveryManager:
    """Test ErrorRecoveryManager main functionality."""
    
    def test_recovery_manager_creation(self):
        """Test ErrorRecoveryManager creation."""
        manager = ErrorRecoveryManager()
        assert manager is not None
        assert len(manager.handlers) > 0
    
    def test_recovery_manager_singleton(self):
        """Test recovery manager singleton pattern."""
        manager1 = get_recovery_manager()
        manager2 = get_recovery_manager()
        assert manager1 is manager2
    
    def test_add_recovery_handler(self):
        """Test adding custom recovery handlers."""
        manager = ErrorRecoveryManager()
        initial_count = len(manager.handlers)
        
        custom_handler = SkipRecoveryHandler()
        manager.add_handler(custom_handler)
        
        assert len(manager.handlers) == initial_count + 1
        assert custom_handler in manager.handlers
    
    def test_remove_recovery_handler(self):
        """Test removing recovery handlers."""
        manager = ErrorRecoveryManager()
        handler = SkipRecoveryHandler()
        
        manager.add_handler(handler)
        initial_count = len(manager.handlers)
        
        manager.remove_handler(handler)
        assert len(manager.handlers) == initial_count - 1
        assert handler not in manager.handlers
    
    @pytest.mark.asyncio
    async def test_recovery_manager_recovery(self):
        """Test ErrorRecoveryManager recovery process."""
        manager = ErrorRecoveryManager()
        error = NetworkError("Test recovery")
        context = ErrorContext(operation="test_recovery")
        
        result = await manager.recover_from_error(error, context)
        assert isinstance(result, RecoveryResult)
        assert result.strategy in RecoveryStrategy
    
    @pytest.mark.asyncio
    async def test_recovery_manager_with_operation_state(self):
        """Test recovery with operation state preservation."""
        manager = ErrorRecoveryManager()
        error = ProcessingError("State test error")
        context = ErrorContext(operation="test_state")
        
        # Create operation state
        state = OperationState(
            operation_id="state123",
            operation_type="download",
            data={"url": "https://example.com"}
        )
        
        result = await manager.recover_from_error(error, context, operation_id="state123")
        assert isinstance(result, RecoveryResult)
    
    def test_get_recovery_statistics(self):
        """Test recovery statistics collection."""
        manager = ErrorRecoveryManager()
        stats = manager.get_recovery_statistics()
        
        assert "total_recoveries" in stats
        assert "successful_recoveries" in stats
        assert "failed_recoveries" in stats
        assert "recovery_strategies" in stats
        assert isinstance(stats["total_recoveries"], int)
    
    def test_clear_recovery_statistics(self):
        """Test clearing recovery statistics."""
        manager = ErrorRecoveryManager()
        
        # Add some statistics
        manager._record_recovery_attempt(RecoveryStrategy.RETRY, True)
        stats_before = manager.get_recovery_statistics()
        
        manager.clear_statistics()
        stats_after = manager.get_recovery_statistics()
        
        assert stats_after["total_recoveries"] == 0
        assert stats_after["successful_recoveries"] == 0
    
    @pytest.mark.asyncio
    async def test_recovery_handler_selection(self):
        """Test recovery handler selection logic."""
        manager = ErrorRecoveryManager()
        
        # Network error should select retry handler
        network_error = NetworkError("Connection failed")
        context = ErrorContext(operation="test_selection")
        
        # Mock the handlers to verify selection
        with patch.object(manager, 'handlers') as mock_handlers:
            mock_retry_handler = Mock()
            mock_retry_handler.can_handle.return_value = True
            mock_retry_handler.recover = AsyncMock(return_value=RecoveryResult(
                success=True, strategy=RecoveryStrategy.RETRY, message="Success"
            ))
            
            mock_skip_handler = Mock()
            mock_skip_handler.can_handle.return_value = False
            
            mock_handlers.__iter__.return_value = [mock_retry_handler, mock_skip_handler]
            
            result = await manager.recover_from_error(network_error, context)
            
            mock_retry_handler.can_handle.assert_called_once_with(network_error)
            mock_retry_handler.recover.assert_called_once()
            mock_skip_handler.can_handle.assert_called_once_with(network_error)


class TestErrorBoundary:
    """Test error boundary context manager functionality."""
    
    @pytest.mark.asyncio
    async def test_error_boundary_success(self):
        """Test error boundary with successful operation."""
        async def successful_operation():
            return "success"
        
        async with error_boundary("test_operation") as boundary:
            result = await successful_operation()
            assert result == "success"
            assert boundary.error is None
            assert boundary.recovery_result is None
    
    @pytest.mark.asyncio
    async def test_error_boundary_with_recovery(self):
        """Test error boundary with error and recovery."""
        async def failing_operation():
            raise NetworkError("Test failure")
        
        context = ErrorContext(operation="boundary_test")
        
        async with error_boundary("test_operation", context=context) as boundary:
            try:
                await failing_operation()
            except NetworkError:
                pass  # Error should be caught by boundary
        
        assert boundary.error is not None
        assert isinstance(boundary.error, NetworkError)
        assert boundary.recovery_result is not None
    
    @pytest.mark.asyncio
    async def test_error_boundary_without_recovery(self):
        """Test error boundary that disables recovery."""
        async def failing_operation():
            raise RedditDLError("Test failure", recoverable=False)
        
        async with error_boundary("test_operation", enable_recovery=False) as boundary:
            with pytest.raises(RedditDLError):
                await failing_operation()
        
        assert boundary.error is not None
        assert boundary.recovery_result is None
    
    @pytest.mark.asyncio
    async def test_error_boundary_custom_recovery_manager(self):
        """Test error boundary with custom recovery manager."""
        custom_manager = ErrorRecoveryManager()
        
        async def failing_operation():
            raise ProcessingError("Custom recovery test")
        
        async with error_boundary(
            "test_operation", 
            recovery_manager=custom_manager
        ) as boundary:
            try:
                await failing_operation()
            except ProcessingError:
                pass
        
        assert boundary.error is not None
        assert boundary.recovery_result is not None


class TestRecoveryIntegration:
    """Test recovery system integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_multiple_recovery_attempts(self):
        """Test multiple recovery attempts with different strategies."""
        manager = ErrorRecoveryManager()
        
        # Simulate an error that requires multiple recovery attempts
        persistent_error = NetworkError("Persistent network issue")
        context = ErrorContext(operation="multi_recovery_test")
        
        # First attempt - retry
        result1 = await manager.recover_from_error(persistent_error, context)
        
        # Second attempt - fallback
        if not result1.success:
            fallback_error = ProcessingError("Retry failed, trying fallback")
            result2 = await manager.recover_from_error(fallback_error, context)
            assert isinstance(result2, RecoveryResult)
    
    @pytest.mark.asyncio
    async def test_recovery_with_state_restoration(self):
        """Test recovery with operation state restoration."""
        manager = ErrorRecoveryManager()
        
        # Create operation state
        state = OperationState(
            operation_id="restore_test",
            operation_type="download",
            data={
                "url": "https://example.com/image.jpg",
                "filename": "image.jpg",
                "progress": 50
            }
        )
        
        # Save state
        manager.save_operation_state(state)
        
        # Simulate error and recovery
        error = NetworkError("Download interrupted")
        context = ErrorContext(operation="download", post_id="post123")
        
        result = await manager.recover_from_error(error, context, operation_id="restore_test")
        
        # Verify state can be restored
        restored_state = manager.get_operation_state("restore_test")
        assert restored_state is not None
        assert restored_state.operation_id == "restore_test"
        assert restored_state.data["progress"] == 50
    
    def test_recovery_strategy_prioritization(self):
        """Test recovery strategy prioritization."""
        manager = ErrorRecoveryManager()
        
        # Test that handlers are tried in appropriate order
        handlers = manager.handlers
        
        # Retry should be attempted before fallback for recoverable errors
        retry_index = None
        fallback_index = None
        
        for i, handler in enumerate(handlers):
            if isinstance(handler, RetryRecoveryHandler):
                retry_index = i
            elif isinstance(handler, FallbackRecoveryHandler):
                fallback_index = i
        
        if retry_index is not None and fallback_index is not None:
            assert retry_index < fallback_index
    
    @pytest.mark.asyncio
    async def test_recovery_statistics_tracking(self):
        """Test that recovery attempts are properly tracked."""
        manager = ErrorRecoveryManager()
        manager.clear_statistics()
        
        # Perform some recovery attempts
        errors = [
            NetworkError("Network error 1"),
            ProcessingError("Processing error 1"),
            NetworkError("Network error 2")
        ]
        
        context = ErrorContext(operation="stats_test")
        
        for error in errors:
            await manager.recover_from_error(error, context)
        
        stats = manager.get_recovery_statistics()
        assert stats["total_recoveries"] >= len(errors)
        assert "recovery_strategies" in stats
        assert len(stats["recovery_strategies"]) > 0


if __name__ == "__main__":
    pytest.main([__file__])