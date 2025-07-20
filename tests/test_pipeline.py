#!/usr/bin/env python3
"""
Tests for the Pipeline & Filter architecture implementation.

Comprehensive test suite covering pipeline interfaces, executor, and stage
implementations to ensure the new pipeline architecture works correctly.
"""

import pytest
import asyncio
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import Mock, patch, MagicMock

# Add current directory to path for imports
sys.path.insert(0, '.')

# Import pipeline components
from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.core.pipeline.executor import PipelineExecutor, ExecutionMetrics
from redditdl.pipeline.stages.acquisition import AcquisitionStage
from redditdl.pipeline.stages.filter import FilterStage
from redditdl.pipeline.stages.processing import ProcessingStage
from redditdl.pipeline.stages.organization import OrganizationStage
from redditdl.pipeline.stages.export import ExportStage

# Import other components for testing
from redditdl.scrapers import PostMetadata


class MockPipelineStage(PipelineStage):
    """Mock pipeline stage for testing."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None, should_fail: bool = False):
        super().__init__(name, config)
        self.should_fail = should_fail
        self.execution_count = 0
        self.pre_process_called = False
        self.post_process_called = False
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """Mock processing that can succeed or fail."""
        self.execution_count += 1
        result = PipelineResult(stage_name=self.name)
        
        if self.should_fail:
            result.add_error("Mock stage failure")
            return result
        
        # Mock some processing
        result.processed_count = len(context.posts)
        result.set_data("mock_data", f"processed_by_{self.name}")
        
        return result
    
    async def pre_process(self, context: PipelineContext) -> None:
        """Mock pre-processing."""
        self.pre_process_called = True
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """Mock post-processing."""
        self.post_process_called = True


def create_test_post(post_id: str, title: str = "Test Post") -> PostMetadata:
    """Create a test PostMetadata object."""
    return PostMetadata({
        'id': post_id,
        'title': title,
        'selftext': '',
        'subreddit': 'test',
        'permalink': f'/r/test/comments/{post_id}/',
        'url': f'https://example.com/{post_id}',
        'author': 'testuser',
        'is_video': False,
        'media': None,
        'created_utc': time.time()
    })


class TestPipelineContext:
    """Test suite for PipelineContext."""
    
    def test_context_initialization(self):
        """Test PipelineContext initialization."""
        context = PipelineContext()
        
        assert context.posts == []
        assert context.config == {}
        assert context.session_state == {}
        assert context.metadata == {}
        assert context.stage_results == {}
        assert isinstance(context.start_time, datetime)
    
    def test_context_post_management(self):
        """Test post management methods."""
        context = PipelineContext()
        
        # Test adding posts
        posts = [create_test_post("1"), create_test_post("2")]
        context.add_posts(posts)
        
        assert len(context.posts) == 2
        assert context.posts[0].id == "1"
        assert context.posts[1].id == "2"
        
        # Test filtering posts
        context.filter_posts(lambda post: post.id == "1")
        assert len(context.posts) == 1
        assert context.posts[0].id == "1"
    
    def test_context_config_management(self):
        """Test configuration management methods."""
        context = PipelineContext()
        
        # Test setting and getting config
        context.set_config("test_key", "test_value")
        assert context.get_config("test_key") == "test_value"
        assert context.get_config("nonexistent", "default") == "default"
    
    def test_context_metadata_management(self):
        """Test metadata management methods."""
        context = PipelineContext()
        
        # Test setting and getting metadata
        context.set_metadata("test_meta", {"nested": "value"})
        assert context.get_metadata("test_meta") == {"nested": "value"}
        assert context.get_metadata("nonexistent", None) is None


class TestPipelineResult:
    """Test suite for PipelineResult."""
    
    def test_result_initialization(self):
        """Test PipelineResult initialization."""
        result = PipelineResult()
        
        assert result.success is True
        assert result.stage_name == ""
        assert result.processed_count == 0
        assert result.error_count == 0
        assert result.errors == []
        assert result.warnings == []
        assert result.data == {}
    
    def test_result_error_handling(self):
        """Test error handling methods."""
        result = PipelineResult()
        
        # Test adding errors
        result.add_error("Test error")
        assert result.error_count == 1
        assert result.success is False
        assert "Test error" in result.errors
        
        # Test adding warnings
        result.add_warning("Test warning")
        assert len(result.warnings) == 1
        assert "Test warning" in result.warnings
    
    def test_result_data_management(self):
        """Test data management methods."""
        result = PipelineResult()
        
        # Test setting and getting data
        result.set_data("test_key", "test_value")
        assert result.get_data("test_key") == "test_value"
        assert result.get_data("nonexistent", "default") == "default"


class TestPipelineStage:
    """Test suite for PipelineStage abstract base class."""
    
    def test_stage_initialization(self):
        """Test stage initialization."""
        config = {"test": "value"}
        stage = MockPipelineStage("test_stage", config)
        
        assert stage.name == "test_stage"
        assert stage.config == config
        assert stage.get_config("test") == "value"
        assert stage.get_config("nonexistent", "default") == "default"
    
    def test_stage_config_management(self):
        """Test stage configuration management."""
        stage = MockPipelineStage("test_stage")
        
        # Test setting and getting config
        stage.set_config("new_key", "new_value")
        assert stage.get_config("new_key") == "new_value"
    
    def test_stage_validation(self):
        """Test stage validation."""
        stage = MockPipelineStage("test_stage")
        
        # Default validation should return empty list
        errors = stage.validate_config()
        assert errors == []
    
    @pytest.mark.asyncio
    async def test_stage_lifecycle_hooks(self):
        """Test stage lifecycle hooks."""
        stage = MockPipelineStage("test_stage")
        context = PipelineContext()
        
        # Test pre-processing hook
        await stage.pre_process(context)
        assert stage.pre_process_called is True
        
        # Test main processing
        result = await stage.process(context)
        assert isinstance(result, PipelineResult)
        assert stage.execution_count == 1
        
        # Test post-processing hook
        await stage.post_process(context, result)
        assert stage.post_process_called is True


class TestPipelineExecutor:
    """Test suite for PipelineExecutor."""
    
    def test_executor_initialization(self):
        """Test executor initialization."""
        stages = [MockPipelineStage("stage1"), MockPipelineStage("stage2")]
        executor = PipelineExecutor(stages, error_handling="continue")
        
        assert len(executor) == 2
        assert executor.get_stage_names() == ["stage1", "stage2"]
        assert executor.error_handling == "continue"
        assert not executor.is_running()
    
    def test_executor_stage_management(self):
        """Test stage management methods."""
        executor = PipelineExecutor()
        
        # Test adding stages
        stage1 = MockPipelineStage("stage1")
        stage2 = MockPipelineStage("stage2")
        
        executor.add_stage(stage1)
        executor.add_stage(stage2, position=0)  # Insert at beginning
        
        assert len(executor) == 2
        assert executor.get_stage_names() == ["stage2", "stage1"]
        
        # Test getting stage
        retrieved_stage = executor.get_stage("stage1")
        assert retrieved_stage is stage1
        
        # Test removing stage
        removed = executor.remove_stage("stage2")
        assert removed is True
        assert len(executor) == 1
        assert executor.get_stage_names() == ["stage1"]
        
        # Test removing non-existent stage
        removed = executor.remove_stage("nonexistent")
        assert removed is False
    
    def test_executor_stage_reordering(self):
        """Test stage reordering."""
        stage1 = MockPipelineStage("stage1")
        stage2 = MockPipelineStage("stage2")
        stage3 = MockPipelineStage("stage3")
        
        executor = PipelineExecutor([stage1, stage2, stage3])
        
        # Test successful reordering
        success = executor.reorder_stages(["stage3", "stage1", "stage2"])
        assert success is True
        assert executor.get_stage_names() == ["stage3", "stage1", "stage2"]
        
        # Test failed reordering (wrong count)
        success = executor.reorder_stages(["stage1", "stage2"])
        assert success is False
        
        # Test failed reordering (non-existent stage)
        success = executor.reorder_stages(["stage1", "stage2", "nonexistent"])
        assert success is False
    
    @pytest.mark.asyncio
    async def test_executor_successful_execution(self):
        """Test successful pipeline execution."""
        stages = [
            MockPipelineStage("stage1"),
            MockPipelineStage("stage2"),
            MockPipelineStage("stage3")
        ]
        executor = PipelineExecutor(stages)
        
        context = PipelineContext()
        context.add_posts([create_test_post("1"), create_test_post("2")])
        
        metrics = await executor.execute(context)
        
        assert isinstance(metrics, ExecutionMetrics)
        assert metrics.total_stages == 3
        assert metrics.successful_stages == 3
        assert metrics.failed_stages == 0
        assert metrics.total_execution_time > 0
        
        # Check that all stages were executed
        for stage in stages:
            assert stage.execution_count == 1
            assert stage.pre_process_called is True
            assert stage.post_process_called is True
    
    @pytest.mark.asyncio
    async def test_executor_error_handling_continue(self):
        """Test executor error handling with 'continue' strategy."""
        stages = [
            MockPipelineStage("stage1"),
            MockPipelineStage("stage2", should_fail=True),  # This stage will fail
            MockPipelineStage("stage3")
        ]
        executor = PipelineExecutor(stages, error_handling="continue")
        
        context = PipelineContext()
        context.add_posts([create_test_post("1")])
        
        metrics = await executor.execute(context)
        
        assert metrics.total_stages == 3
        assert metrics.successful_stages == 2
        assert metrics.failed_stages == 1
        
        # All stages should have been attempted
        assert stages[0].execution_count == 1
        assert stages[1].execution_count == 1
        assert stages[2].execution_count == 1
    
    @pytest.mark.asyncio
    async def test_executor_error_handling_halt(self):
        """Test executor error handling with 'halt' strategy."""
        stages = [
            MockPipelineStage("stage1"),
            MockPipelineStage("stage2", should_fail=True),  # This stage will fail
            MockPipelineStage("stage3")
        ]
        executor = PipelineExecutor(stages, error_handling="halt")
        
        context = PipelineContext()
        context.add_posts([create_test_post("1")])
        
        with pytest.raises(RuntimeError, match="Pipeline halted"):
            await executor.execute(context)
        
        # Only first two stages should have been attempted
        assert stages[0].execution_count == 1
        assert stages[1].execution_count == 1
        assert stages[2].execution_count == 0
    
    @pytest.mark.asyncio
    async def test_executor_validation(self):
        """Test executor validation."""
        # Test with duplicate stage names
        stages = [
            MockPipelineStage("duplicate"),
            MockPipelineStage("duplicate")
        ]
        executor = PipelineExecutor(stages)
        
        context = PipelineContext()
        
        with pytest.raises(RuntimeError, match="Pipeline validation failed"):
            await executor.execute(context)
    
    @pytest.mark.asyncio
    async def test_executor_concurrent_execution_prevention(self):
        """Test that concurrent execution is prevented."""
        executor = PipelineExecutor([MockPipelineStage("stage1")])
        context = PipelineContext()
        
        # Start first execution
        task1 = asyncio.create_task(executor.execute(context))
        
        # Try to start second execution
        with pytest.raises(RuntimeError, match="Pipeline is already running"):
            await executor.execute(context)
        
        # Wait for first execution to complete
        await task1


class TestAcquisitionStage:
    """Test suite for AcquisitionStage."""
    
    @pytest.mark.asyncio
    async def test_acquisition_stage_validation(self):
        """Test acquisition stage configuration validation."""
        # Test valid API mode configuration
        config = {
            "api_mode": True,
            "client_id": "test_id",
            "client_secret": "test_secret"
        }
        stage = AcquisitionStage(config)
        errors = stage.validate_config()
        assert errors == []
        
        # Test invalid API mode configuration (missing credentials)
        config = {"api_mode": True}
        stage = AcquisitionStage(config)
        errors = stage.validate_config()
        assert len(errors) > 0
        assert any("client_id" in error for error in errors)
    
    @pytest.mark.asyncio
    @patch('redditdl.pipeline.stages.acquisition.YarsScraper')
    async def test_acquisition_stage_non_api_mode(self, mock_yars):
        """Test acquisition stage in non-API mode."""
        # Mock YARS scraper
        mock_scraper = Mock()
        mock_posts = [create_test_post("1"), create_test_post("2")]
        mock_scraper.fetch_user_posts.return_value = mock_posts
        mock_yars.return_value = mock_scraper
        
        stage = AcquisitionStage({"api_mode": False, "target_user": "testuser"})
        context = PipelineContext()
        
        result = await stage.process(context)
        
        assert result.success is True
        assert result.processed_count == 2
        assert result.get_data("total_posts_acquired") == 2
        assert result.get_data("targets_processed") == 1
        assert len(context.posts) == 2
    
    def test_filter_stage_validation(self):
        """Test filter stage configuration validation."""
        # Test invalid NSFW filter
        config = {"nsfw_filter": "invalid"}
        stage = FilterStage(config)
        errors = stage.validate_config()
        assert len(errors) > 0
        assert any("nsfw_filter" in error for error in errors)
        
        # Test invalid keyword types
        config = {"keywords_include": "not_a_list"}
        stage = FilterStage(config)
        errors = stage.validate_config()
        assert len(errors) > 0


class TestProcessingStage:
    """Test suite for ProcessingStage."""
    
    @pytest.mark.asyncio
    @patch('redditdl.pipeline.stages.processing.MediaDownloader')
    @patch('redditdl.pipeline.stages.processing.MetadataEmbedder')
    async def test_processing_stage_success(self, mock_embedder, mock_downloader):
        """Test successful processing stage execution."""
        # Mock downloader
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = Path("test_file.jpg")
        mock_downloader.return_value = mock_downloader_instance
        
        # Mock embedder
        mock_embedder_instance = Mock()
        mock_embedder.return_value = mock_embedder_instance
        
        stage = ProcessingStage({"output_dir": "test_output"})
        context = PipelineContext()
        
        # Create test post with media URL
        post = create_test_post("1", "Test Post")
        post.media_url = "https://i.redd.it/test.jpg"
        context.add_posts([post])
        
        result = await stage.process(context)
        
        assert result.success is True
        assert result.get_data("successful_downloads") == 1
        assert result.get_data("failed_downloads") == 0
    
    def test_processing_stage_validation(self):
        """Test processing stage configuration validation."""
        # Test invalid sleep interval
        config = {"sleep_interval": -1}
        stage = ProcessingStage(config)
        errors = stage.validate_config()
        assert len(errors) > 0
        assert any("sleep_interval" in error for error in errors)


class TestOrganizationStage:
    """Test suite for OrganizationStage."""
    
    @pytest.mark.asyncio
    async def test_organization_stage_disabled(self):
        """Test organization stage when disabled."""
        stage = OrganizationStage({"organize_by": "none"})
        context = PipelineContext()
        context.add_posts([create_test_post("1")])
        
        result = await stage.process(context)
        
        assert result.success is True
        assert result.get_data("organization_scheme") == "none"
        assert result.get_data("files_organized") == 0
    
    def test_organization_stage_validation(self):
        """Test organization stage configuration validation."""
        # Test invalid organization scheme
        config = {"organize_by": "invalid_scheme"}
        stage = OrganizationStage(config)
        errors = stage.validate_config()
        assert len(errors) > 0


class TestExportStage:
    """Test suite for ExportStage."""
    
    @pytest.mark.asyncio
    async def test_export_stage_json_export(self, tmp_path):
        """Test JSON export functionality."""
        config = {
            "export_formats": ["json"],
            "export_dir": str(tmp_path),
            "include_metadata": True,
            "include_posts": True
        }
        stage = ExportStage(config)
        context = PipelineContext()
        context.add_posts([create_test_post("1"), create_test_post("2")])
        
        result = await stage.process(context)
        
        assert result.success is True
        assert result.get_data("exports_created") == 1
        
        # Check that JSON file was created
        export_files = list(tmp_path.glob("*.json"))
        assert len(export_files) == 1
    
    def test_export_stage_validation(self):
        """Test export stage configuration validation."""
        # Test invalid export format
        config = {"export_formats": ["invalid_format"]}
        stage = ExportStage(config)
        errors = stage.validate_config()
        assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__])