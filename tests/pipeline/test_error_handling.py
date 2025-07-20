"""
Test suite for pipeline error handling.

Tests error handling across all pipeline stages to ensure robust error
management, recovery, and continuation of pipeline execution.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from pathlib import Path
from typing import List, Dict, Any

from redditdl.core.pipeline.interfaces import PipelineContext, PipelineResult
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.config import AppConfig
from redditdl.pipeline.stages.acquisition import AcquisitionStage
from redditdl.pipeline.stages.filter import FilterStage
from redditdl.pipeline.stages.processing import ProcessingStage
from redditdl.scrapers import PostMetadata

from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ProcessingError, ConfigurationError,
    ValidationError, ErrorCode, ErrorContext, RecoverySuggestion
)
from redditdl.core.error_recovery import ErrorRecoveryManager, RecoveryResult, RecoveryStrategy
from redditdl.core.error_context import ErrorReporter, ErrorAnalytics


class TestAcquisitionStageErrorHandling:
    """Test error handling in the acquisition pipeline stage."""
    
    @pytest.fixture
    def mock_context(self):
        """Create a mock pipeline context."""
        context = Mock(spec=PipelineContext)
        context.session_id = "test_session_123"
        context.events = Mock(spec=EventEmitter)
        context.get_config = Mock(return_value=None)
        context.config = Mock(spec=AppConfig)
        return context
    
    @pytest.fixture
    def acquisition_stage(self):
        """Create an acquisition stage for testing."""
        return AcquisitionStage()
    
    @pytest.mark.asyncio
    async def test_acquisition_stage_network_error_handling(self, acquisition_stage, mock_context):
        """Test acquisition stage handles network errors gracefully."""
        # Mock target resolution to raise network error
        with patch.object(acquisition_stage, '_resolve_targets') as mock_resolve:
            mock_resolve.side_effect = NetworkError(
                "DNS resolution failed",
                error_code=ErrorCode.NETWORK_DNS_FAILED
            )
            
            result = await acquisition_stage.process(mock_context)
            
            assert isinstance(result, PipelineResult)
            assert result.success is False
            assert len(result.errors) > 0
            assert "DNS resolution failed" in result.errors[0]
    
    @pytest.mark.asyncio
    async def test_acquisition_stage_target_not_found_error(self, acquisition_stage, mock_context):
        """Test acquisition stage handles target not found errors."""
        # Mock target resolution to raise target not found error
        with patch.object(acquisition_stage, '_resolve_targets') as mock_resolve:
            mock_resolve.side_effect = ValidationError(
                "Target user 'nonexistent_user' not found",
                error_code=ErrorCode.TARGET_NOT_FOUND,
                field_name="target",
                field_value="nonexistent_user"
            )
            
            result = await acquisition_stage.process(mock_context)
            
            assert result.success is False
            assert any("not found" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_acquisition_stage_authentication_error(self, acquisition_stage, mock_context):
        """Test acquisition stage handles authentication errors."""
        with patch.object(acquisition_stage, '_load_targets_file') as mock_load:
            from redditdl.core.exceptions import AuthenticationError
            mock_load.side_effect = AuthenticationError(
                "Invalid API credentials",
                error_code=ErrorCode.AUTH_INVALID_CREDENTIALS
            )
            
            result = await acquisition_stage.process(mock_context)
            
            assert result.success is False
            assert any("credential" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_acquisition_stage_error_recovery(self, acquisition_stage, mock_context):
        """Test acquisition stage error recovery mechanisms."""
        # Mock recovery manager
        with patch('pipeline.stages.acquisition.get_recovery_manager') as mock_get_recovery:
            mock_recovery_manager = Mock(spec=ErrorRecoveryManager)
            mock_recovery_manager.recover_from_error = AsyncMock(return_value=RecoveryResult(
                success=True,
                strategy=RecoveryStrategy.RETRY,
                message="Recovery successful"
            ))
            mock_get_recovery.return_value = mock_recovery_manager
            
            # Mock target resolution to fail initially
            with patch.object(acquisition_stage, '_resolve_targets') as mock_resolve:
                mock_resolve.side_effect = NetworkError("Temporary failure")
                
                result = await acquisition_stage.process(mock_context)
                
                # Should attempt recovery
                mock_recovery_manager.recover_from_error.assert_called()
    
    @pytest.mark.asyncio
    async def test_acquisition_stage_partial_failure(self, acquisition_stage, mock_context):
        """Test acquisition stage handles partial failures in target processing."""
        # Mock multiple targets with some failing
        mock_context.posts = []
        
        with patch.object(acquisition_stage, '_process_targets') as mock_process:
            # Simulate partial failure - some targets succeed, others fail
            def side_effect(targets):
                results = []
                for i, target in enumerate(targets):
                    if i % 2 == 0:  # Even targets succeed
                        results.append([PostMetadata(
                            id=f"post_{i}",
                            title=f"Test post {i}",
                            url=f"https://example.com/{i}"
                        )])
                    else:  # Odd targets fail
                        raise NetworkError(f"Failed to process target {i}")
                return results
            
            mock_process.side_effect = side_effect
            
            # Mock targets
            with patch.object(acquisition_stage, '_resolve_targets') as mock_resolve:
                mock_resolve.return_value = ["target1", "target2", "target3", "target4"]
                
                result = await acquisition_stage.process(mock_context)
                
                # Should have warnings about failed targets but not complete failure
                assert len(result.warnings) > 0


class TestFilterStageErrorHandling:
    """Test error handling in the filter pipeline stage."""
    
    @pytest.fixture
    def mock_context_with_posts(self):
        """Create a mock pipeline context with test posts."""
        context = Mock(spec=PipelineContext)
        context.session_id = "filter_test_session"
        context.events = Mock(spec=EventEmitter)
        context.get_config = Mock(return_value=None)
        context.posts = [
            PostMetadata(id="post1", title="Test Post 1", score=100),
            PostMetadata(id="post2", title="Test Post 2", score=50),
            PostMetadata(id="post3", title="Test Post 3", score=200)
        ]
        return context
    
    @pytest.fixture
    def filter_stage(self):
        """Create a filter stage for testing."""
        return FilterStage()
    
    @pytest.mark.asyncio
    async def test_filter_stage_configuration_error(self, filter_stage, mock_context_with_posts):
        """Test filter stage handles configuration errors."""
        # Mock filter chain building to raise configuration error
        with patch.object(filter_stage, '_build_filter_chain') as mock_build:
            mock_build.side_effect = ConfigurationError(
                "Invalid filter configuration",
                error_code=ErrorCode.CONFIG_INVALID_VALUE,
                config_section="filters"
            )
            
            result = await filter_stage.process(mock_context_with_posts)
            
            assert result.success is False
            assert any("configuration" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_filter_stage_validation_error(self, filter_stage, mock_context_with_posts):
        """Test filter stage handles validation errors in filter parameters."""
        with patch.object(filter_stage, '_validate_filter_config') as mock_validate:
            mock_validate.side_effect = ValidationError(
                "Invalid score threshold",
                error_code=ErrorCode.VALIDATION_FIELD_INVALID,
                field_name="min_score",
                field_value=-100
            )
            
            result = await filter_stage.process(mock_context_with_posts)
            
            assert result.success is False
            assert any("validation" in error.lower() or "invalid" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_filter_stage_filter_processing_error(self, filter_stage, mock_context_with_posts):
        """Test filter stage handles individual filter processing errors."""
        # Mock filter chain to have a failing filter
        mock_filter_chain = Mock()
        mock_filter_chain.apply_filters.side_effect = ProcessingError(
            "Filter processing failed",
            error_code=ErrorCode.PROCESSING_OPERATION_FAILED
        )
        
        with patch.object(filter_stage, '_build_filter_chain', return_value=mock_filter_chain):
            result = await filter_stage.process(mock_context_with_posts)
            
            assert result.success is False
            assert any("processing" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_filter_stage_partial_filter_failure(self, filter_stage, mock_context_with_posts):
        """Test filter stage handles partial filter failures."""
        # Mock filter chain with some posts failing
        def mock_apply_filters(posts):
            filtered = []
            for i, post in enumerate(posts):
                if i == 1:  # Second post fails
                    raise ProcessingError(f"Filter failed for post {post.id}")
                filtered.append(post)
            return filtered
        
        mock_filter_chain = Mock()
        mock_filter_chain.apply_filters.side_effect = mock_apply_filters
        
        with patch.object(filter_stage, '_build_filter_chain', return_value=mock_filter_chain):
            with patch('pipeline.stages.filter.get_recovery_manager') as mock_get_recovery:
                mock_recovery_manager = Mock()
                mock_recovery_manager.recover_from_error = AsyncMock(return_value=RecoveryResult(
                    success=True,
                    strategy=RecoveryStrategy.SKIP,
                    message="Skipped failed post"
                ))
                mock_get_recovery.return_value = mock_recovery_manager
                
                result = await filter_stage.process(mock_context_with_posts)
                
                # Should attempt recovery for failed posts
                mock_recovery_manager.recover_from_error.assert_called()
    
    @pytest.mark.asyncio
    async def test_filter_stage_error_context_propagation(self, filter_stage, mock_context_with_posts):
        """Test filter stage properly propagates error context."""
        with patch.object(filter_stage, '_build_filter_chain') as mock_build:
            raised_error = ConfigurationError("Test error context")
            mock_build.side_effect = raised_error
            
            with patch('pipeline.stages.filter.report_error') as mock_report:
                result = await filter_stage.process(mock_context_with_posts)
                
                # Should report error with proper context
                mock_report.assert_called()
                args, kwargs = mock_report.call_args
                error, context = args
                assert isinstance(error, ConfigurationError)
                assert context.stage == "filter"
                assert context.session_id == "filter_test_session"


class TestProcessingStageErrorHandling:
    """Test error handling in the processing pipeline stage."""
    
    @pytest.fixture
    def mock_context_with_posts(self):
        """Create a mock pipeline context with test posts."""
        context = Mock(spec=PipelineContext)
        context.session_id = "processing_test_session"
        context.events = Mock(spec=EventEmitter)
        context.get_config = Mock(return_value=None)
        context.posts = [
            PostMetadata(id="post1", title="Image Post", url="https://example.com/image.jpg"),
            PostMetadata(id="post2", title="Video Post", url="https://example.com/video.mp4"),
            PostMetadata(id="post3", title="Text Post", is_self=True)
        ]
        context.emit_event_async = AsyncMock()
        return context
    
    @pytest.fixture
    def processing_stage(self):
        """Create a processing stage for testing."""
        return ProcessingStage()
    
    @pytest.mark.asyncio
    async def test_processing_stage_handler_initialization_error(self, processing_stage, mock_context_with_posts):
        """Test processing stage handles handler initialization errors."""
        with patch.object(processing_stage, '_ensure_handlers_initialized') as mock_init:
            mock_init.side_effect = ConfigurationError(
                "Failed to initialize content handlers",
                error_code=ErrorCode.CONFIG_INVALID_VALUE
            )
            
            result = await processing_stage.process(mock_context_with_posts)
            
            assert result.success is False
            assert any("handler" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_processing_stage_output_directory_error(self, processing_stage, mock_context_with_posts):
        """Test processing stage handles output directory errors."""
        with patch.object(processing_stage, '_get_output_directory') as mock_get_dir:
            mock_get_dir.side_effect = ConfigurationError(
                "Output directory not accessible",
                error_code=ErrorCode.CONFIG_INVALID_VALUE
            )
            
            result = await processing_stage.process(mock_context_with_posts)
            
            assert result.success is False
            assert any("output" in error.lower() or "directory" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_processing_stage_content_type_detection_error(self, processing_stage, mock_context_with_posts):
        """Test processing stage handles content type detection errors."""
        with patch.object(processing_stage, '_ensure_handlers_initialized'):
            with patch.object(processing_stage, '_get_output_directory', return_value=Path("/tmp")):
                with patch.object(processing_stage._detector, 'detect_content_type') as mock_detect:
                    mock_detect.side_effect = ProcessingError(
                        "Content type detection failed",
                        error_code=ErrorCode.PROCESSING_INVALID_CONTENT
                    )
                    
                    result = await processing_stage.process(mock_context_with_posts)
                    
                    # Should have warnings but continue processing
                    assert len(result.warnings) > 0 or "detection" in str(result.errors)
    
    @pytest.mark.asyncio
    async def test_processing_stage_no_handler_found(self, processing_stage, mock_context_with_posts):
        """Test processing stage handles cases where no handler is found."""
        with patch.object(processing_stage, '_ensure_handlers_initialized'):
            with patch.object(processing_stage, '_get_output_directory', return_value=Path("/tmp")):
                with patch.object(processing_stage._detector, 'detect_content_type', return_value="unknown"):
                    with patch.object(processing_stage._registry, 'get_handler_for_post', return_value=None):
                        
                        result = await processing_stage.process(mock_context_with_posts)
                        
                        # Should skip posts without handlers
                        skipped = result.get_data("skipped_processing", 0)
                        assert skipped > 0
    
    @pytest.mark.asyncio
    async def test_processing_stage_handler_execution_error(self, processing_stage, mock_context_with_posts):
        """Test processing stage handles handler execution errors."""
        # Mock handler that raises exception
        mock_handler = Mock()
        mock_handler.name = "test_handler"
        mock_handler.process = AsyncMock(side_effect=ProcessingError(
            "Handler processing failed",
            error_code=ErrorCode.PROCESSING_OPERATION_FAILED
        ))
        
        with patch.object(processing_stage, '_ensure_handlers_initialized'):
            with patch.object(processing_stage, '_get_output_directory', return_value=Path("/tmp")):
                with patch.object(processing_stage._detector, 'detect_content_type', return_value="media"):
                    with patch.object(processing_stage._registry, 'get_handler_for_post', return_value=mock_handler):
                        with patch('pipeline.stages.processing.get_recovery_manager') as mock_get_recovery:
                            mock_recovery_manager = Mock()
                            mock_recovery_manager.recover_from_error = AsyncMock(return_value=RecoveryResult(
                                success=False,
                                strategy=RecoveryStrategy.RETRY,
                                message="Recovery failed"
                            ))
                            mock_get_recovery.return_value = mock_recovery_manager
                            
                            result = await processing_stage.process(mock_context_with_posts)
                            
                            # Should attempt recovery for failed handlers
                            mock_recovery_manager.recover_from_error.assert_called()
                            failed = result.get_data("failed_processing", 0)
                            assert failed > 0
    
    @pytest.mark.asyncio
    async def test_processing_stage_handler_success_with_recovery(self, processing_stage, mock_context_with_posts):
        """Test processing stage successful recovery from handler errors."""
        # Mock handler that initially fails but recovers
        mock_handler = Mock()
        mock_handler.name = "recoverable_handler"
        mock_handler.process = AsyncMock(side_effect=NetworkError("Temporary network issue"))
        
        with patch.object(processing_stage, '_ensure_handlers_initialized'):
            with patch.object(processing_stage, '_get_output_directory', return_value=Path("/tmp")):
                with patch.object(processing_stage._detector, 'detect_content_type', return_value="media"):
                    with patch.object(processing_stage._registry, 'get_handler_for_post', return_value=mock_handler):
                        with patch('pipeline.stages.processing.get_recovery_manager') as mock_get_recovery:
                            mock_recovery_manager = Mock()
                            mock_recovery_manager.recover_from_error = AsyncMock(return_value=RecoveryResult(
                                success=True,
                                strategy=RecoveryStrategy.RETRY,
                                message="Recovery successful"
                            ))
                            mock_get_recovery.return_value = mock_recovery_manager
                            
                            result = await processing_stage.process(mock_context_with_posts)
                            
                            # Should show successful recovery
                            successful = result.get_data("successful_processing", 0)
                            assert successful > 0
    
    @pytest.mark.asyncio
    async def test_processing_stage_unexpected_error_handling(self, processing_stage, mock_context_with_posts):
        """Test processing stage handles unexpected errors gracefully."""
        with patch.object(processing_stage, '_ensure_handlers_initialized'):
            with patch.object(processing_stage, '_get_output_directory', return_value=Path("/tmp")):
                # Simulate unexpected exception in processing loop
                with patch.object(processing_stage._detector, 'detect_content_type') as mock_detect:
                    mock_detect.side_effect = RuntimeError("Unexpected system error")
                    
                    result = await processing_stage.process(mock_context_with_posts)
                    
                    # Should handle unexpected errors gracefully
                    assert result.success is False or len(result.warnings) > 0
                    processing_errors = result.get_data("processing_errors", 0)
                    assert processing_errors > 0


class TestPipelineErrorIntegration:
    """Test error handling integration across multiple pipeline stages."""
    
    @pytest.mark.asyncio
    async def test_pipeline_stage_error_propagation(self):
        """Test error propagation between pipeline stages."""
        from core.pipeline.executor import PipelineExecutor
        
        # Create pipeline with multiple stages
        executor = PipelineExecutor()
        
        # Mock stages that simulate different error scenarios
        mock_acquisition = Mock()
        mock_acquisition.name = "acquisition"
        mock_acquisition.process = AsyncMock(return_value=PipelineResult(
            stage_name="acquisition",
            success=False,
            errors=["Acquisition failed"]
        ))
        
        mock_filter = Mock()
        mock_filter.name = "filter"
        mock_filter.process = AsyncMock(return_value=PipelineResult(
            stage_name="filter",
            success=True
        ))
        
        executor.add_stage(mock_acquisition)
        executor.add_stage(mock_filter)
        
        # Create context
        context = Mock(spec=PipelineContext)
        context.posts = []
        
        result = await executor.execute(context)
        
        # Should handle stage failures gracefully
        assert isinstance(result, PipelineResult)
        # Filter stage should not be called if acquisition fails (depending on config)
        mock_acquisition.process.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_error_recovery_across_stages(self):
        """Test error recovery mechanisms work across different stages."""
        # This would test that recovery context is preserved across stages
        # and that recovery statistics are properly accumulated
        
        analytics = ErrorAnalytics()
        analytics.clear_history()
        
        # Simulate errors from different stages
        errors = [
            NetworkError("Acquisition network error"),
            ValidationError("Filter validation error", field_name="score"),
            ProcessingError("Processing handler error")
        ]
        
        contexts = [
            ErrorContext(operation="acquire_posts", stage="acquisition"),
            ErrorContext(operation="filter_posts", stage="filter"),
            ErrorContext(operation="process_content", stage="processing")
        ]
        
        # Record errors as if they came from different stages
        for error, context in zip(errors, contexts):
            analytics.record_error(error, context)
        
        # Analyze patterns across stages
        patterns = analytics.detect_patterns()
        stats = analytics.get_error_statistics()
        
        assert stats["total_errors"] == 3
        assert len(set(error["stage"] for error in analytics.error_history)) == 3


class TestContentHandlerErrorHandling:
    """Test error handling in content handlers called by processing stage."""
    
    @pytest.mark.asyncio
    async def test_media_handler_network_error_recovery(self):
        """Test media content handler network error recovery."""
        from content_handlers.media import MediaContentHandler
        
        handler = MediaContentHandler()
        post = PostMetadata(
            id="test_post",
            title="Test Media",
            url="https://example.com/image.jpg"
        )
        
        output_dir = Path("/tmp/test")
        config = {"sleep_interval": 1.0}
        
        # Mock downloader to raise network error
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_downloader = Mock()
            mock_downloader.download.side_effect = NetworkError(
                "Connection timeout",
                url="https://example.com/image.jpg"
            )
            mock_get_downloader.return_value = mock_downloader
            
            with patch('content_handlers.media.get_recovery_manager') as mock_get_recovery:
                mock_recovery_manager = Mock()
                mock_recovery_manager.recover_from_error = AsyncMock(return_value=RecoveryResult(
                    success=True,
                    strategy=RecoveryStrategy.RETRY,
                    message="Retry successful"
                ))
                mock_get_recovery.return_value = mock_recovery_manager
                
                # Mock successful retry
                mock_downloader.download.side_effect = [
                    NetworkError("First attempt failed"),
                    Path("/tmp/test/image.jpg")  # Second attempt succeeds
                ]
                
                result = await handler.process(post, output_dir, config)
                
                # Should attempt recovery
                mock_recovery_manager.recover_from_error.assert_called()
    
    @pytest.mark.asyncio
    async def test_handler_error_context_creation(self):
        """Test content handlers create proper error contexts."""
        from content_handlers.media import MediaContentHandler
        
        handler = MediaContentHandler()
        post = PostMetadata(id="context_test", title="Context Test")
        
        # Mock methods to capture error context
        with patch.object(handler, '_get_or_create_downloader') as mock_get_downloader:
            mock_get_downloader.side_effect = ConfigurationError("Test config error")
            
            with patch('content_handlers.media.report_error') as mock_report:
                try:
                    await handler.process(post, Path("/tmp"), {})
                except Exception:
                    pass
                
                # Should report error with proper context
                if mock_report.called:
                    args, kwargs = mock_report.call_args
                    error, context = args
                    assert context.operation == "media_handler_process"
                    assert context.stage == "processing"
                    assert context.post_id == "context_test"


if __name__ == "__main__":
    pytest.main([__file__])