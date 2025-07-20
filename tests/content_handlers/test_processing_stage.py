"""
Tests for enhanced ProcessingStage with content handler system
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from redditdl.pipeline.stages.processing import ProcessingStage
from redditdl.core.pipeline.interfaces import PipelineContext, PipelineResult
from redditdl.core.events.types import PostProcessedEvent
from redditdl.scrapers import PostMetadata
from redditdl.content_handlers.base import HandlerResult


class TestProcessingStage:
    """Test ProcessingStage functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.stage = ProcessingStage()
        self.context = Mock(spec=PipelineContext)
        self.context.posts = []
        self.context.session_id = "test_session"
        self.context.events = Mock()
        self.context.get_config = Mock(return_value=None)
        self.context.set_metadata = Mock()
        self.context.emit_event_async = AsyncMock()
    
    def create_test_post(self, post_id="test_post", content_type="image"):
        """Create a test post."""
        post = PostMetadata(
            id=post_id,
            title="Test Post",
            author="test_user",
            subreddit="test",
            url="https://i.redd.it/example.jpg" if content_type == "image" else "https://example.com",
            media_url="https://i.redd.it/example.jpg" if content_type == "image" else None,
            date_iso="2023-01-01"
        )
        
        if content_type == "text":
            post.is_self = True
            post.selftext = "This is a text post"
        
        return post
    
    @pytest.mark.asyncio
    async def test_process_empty_posts(self):
        """Test processing with no posts."""
        self.context.posts = []
        
        result = await self.stage.process(self.context)
        
        assert result.stage_name == "processing"
        assert "No posts to process" in result.warnings
    
    @pytest.mark.asyncio
    async def test_process_single_image_post(self):
        """Test processing a single image post."""
        post = self.create_test_post("img_post", "image")
        self.context.posts = [post]
        self.context.get_config.side_effect = lambda key, default=None: {
            "output_dir": "/tmp/test",
            "sleep_interval": 1.0,
            "embed_metadata": True,
            "enable_plugins": False
        }.get(key, default)
        
        with patch('content_handlers.media.MediaDownloader') as mock_downloader_class:
            # Mock the downloader
            mock_downloader = Mock()
            mock_downloader.download.return_value = Path("/tmp/test/downloaded_file.jpg")
            mock_downloader_class.return_value = mock_downloader
            
            # Mock the handler result
            with patch.object(self.stage, '_emit_post_processed_event', new_callable=AsyncMock):
                result = await self.stage.process(self.context)
        
        assert result.stage_name == "processing"
        assert result.processed_count == 1
        # Note: Actual success depends on handler implementation
    
    @pytest.mark.asyncio
    async def test_handler_initialization(self):
        """Test that handlers are initialized properly."""
        with patch.object(self.stage, '_registry') as mock_registry:
            mock_registry.register_handler = Mock()
            
            await self.stage._ensure_handlers_initialized(self.context)
            
            # Should register all built-in handlers
            assert mock_registry.register_handler.call_count == 6  # 6 built-in handlers
    
    def test_get_output_directory(self):
        """Test output directory resolution."""
        self.context.get_config.side_effect = lambda key, default=None: {
            "output_dir": "/custom/output"
        }.get(key, default)
        
        output_dir = self.stage._get_output_directory(self.context)
        assert output_dir == Path("/custom/output")
    
    def test_build_handler_config(self):
        """Test building handler configuration."""
        self.context.get_config.side_effect = lambda key, default=None: {
            "sleep_interval": 2.0,
            "embed_metadata": False,
            "create_sidecars": True,
            "handler_config": {
                "image": {"quality": "high"},
                "text": {"format": "markdown"}
            }
        }.get(key, default)
        
        # Test image handler config
        image_config = self.stage._build_handler_config(self.context, "image")
        assert image_config["sleep_interval"] == 2.0
        assert image_config["embed_metadata"] == False
        assert image_config["create_sidecars"] == True
        assert image_config["content_type"] == "image"
        assert image_config["quality"] == "high"
        
        # Test text handler config
        text_config = self.stage._build_handler_config(self.context, "text")
        assert text_config["content_type"] == "text"
        assert text_config["format"] == "markdown"
    
    @pytest.mark.asyncio
    async def test_emit_post_processed_event(self):
        """Test PostProcessedEvent emission."""
        post = self.create_test_post()
        
        handler_result = HandlerResult(
            success=True,
            handler_name="test_handler",
            content_type="image",
            processing_time=1.5
        )
        handler_result.add_operation("download")
        handler_result.add_file(Path("/tmp/test.jpg"))
        handler_result.metadata_embedded = True
        
        await self.stage._emit_post_processed_event(
            self.context, post, handler_result, "image"
        )
        
        # Verify event was emitted
        self.context.emit_event_async.assert_called_once()
        
        # Check event details
        event_call = self.context.emit_event_async.call_args[0][0]
        assert isinstance(event_call, PostProcessedEvent)
        assert event_call.post_id == "test_post"
        assert event_call.post_title == "Test Post"
        assert event_call.success == True
        assert event_call.processing_stage == "processing"
        assert "download" in event_call.operations_performed
        assert event_call.metadata_embedded == True
        assert event_call.processing_time == 1.5
        assert "/tmp/test.jpg" in event_call.file_paths
    
    def test_validate_config(self):
        """Test configuration validation."""
        # Test valid config
        valid_config = {
            "output_dir": "/tmp/test",
            "sleep_interval": 1.0,
            "embed_metadata": True,
            "create_sidecars": False,
            "enable_plugins": True,
            "handler_config": {"image": {"quality": "high"}}
        }
        
        stage = ProcessingStage(valid_config)
        errors = stage.validate_config()
        assert len(errors) == 0
        
        # Test invalid config
        invalid_config = {
            "sleep_interval": -1.0,  # Invalid: negative
            "embed_metadata": "yes",  # Invalid: not boolean
            "create_sidecars": 1,  # Invalid: not boolean
            "enable_plugins": "true",  # Invalid: not boolean
            "handler_config": "not_a_dict"  # Invalid: not dict
        }
        
        stage = ProcessingStage(invalid_config)
        errors = stage.validate_config()
        assert len(errors) == 5  # Should have 5 validation errors
        assert any("sleep_interval must be non-negative" in error for error in errors)
        assert any("embed_metadata must be a boolean" in error for error in errors)
        assert any("create_sidecars must be a boolean" in error for error in errors)
        assert any("enable_plugins must be a boolean" in error for error in errors)
        assert any("handler_config must be a dictionary" in error for error in errors)
    
    @pytest.mark.asyncio
    async def test_pre_process(self):
        """Test pre-processing setup."""
        self.context.get_config.side_effect = lambda key, default=None: {
            "output_dir": "/tmp/test_output",
            "sleep_interval": 1.5,
            "embed_metadata": True,
            "enable_plugins": False
        }.get(key, default)
        
        with patch('pathlib.Path.mkdir') as mock_mkdir:
            with patch.object(self.stage, '_ensure_handlers_initialized', new_callable=AsyncMock) as mock_init:
                with patch.object(self.stage, 'get_handler_statistics', return_value={'total_handlers': 6}):
                    await self.stage.pre_process(self.context)
        
        # Verify directory creation
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        # Verify handler initialization
        mock_init.assert_called_once_with(self.context)
    
    @pytest.mark.asyncio
    async def test_post_process_success(self):
        """Test post-processing with successful result."""
        result = PipelineResult(stage_name="processing")
        result.success = True
        result.set_data("successful_processing", 5)
        result.set_data("failed_processing", 1)
        result.set_data("skipped_processing", 2)
        result.set_data("handler_statistics", {
            "media": {"count": 3, "success": 3, "failed": 0},
            "text": {"count": 2, "success": 2, "failed": 0}
        })
        
        await self.stage.post_process(self.context, result)
        
        # Verify metadata was set
        expected_calls = [
            ("processing_completed", True),
            ("successful_processing", 5),
            ("failed_processing", 1),
            ("skipped_processing", 2),
            ("handler_statistics", {
                "media": {"count": 3, "success": 3, "failed": 0},
                "text": {"count": 2, "success": 2, "failed": 0}
            })
        ]
        
        for key, value in expected_calls:
            self.context.set_metadata.assert_any_call(key, value)
    
    @pytest.mark.asyncio
    async def test_post_process_failure(self):
        """Test post-processing with failed result."""
        result = PipelineResult(stage_name="processing")
        result.success = False
        
        await self.stage.post_process(self.context, result)
        
        # Verify failure metadata was set
        self.context.set_metadata.assert_called_with("processing_completed", False)
    
    def test_get_handler_statistics(self):
        """Test getting handler statistics."""
        with patch.object(self.stage._registry, 'get_handler_stats') as mock_stats:
            mock_stats.return_value = {"total_handlers": 6, "handlers_by_type": {}}
            
            stats = self.stage.get_handler_statistics()
            
            mock_stats.assert_called_once()
            assert stats["total_handlers"] == 6
    
    @pytest.mark.asyncio
    async def test_load_plugin_handlers_disabled(self):
        """Test plugin handler loading when plugins are disabled."""
        self.context.get_config.side_effect = lambda key, default=None: {
            "enable_plugins": False
        }.get(key, default)
        
        with patch.object(self.stage, '_load_plugin_handlers', new_callable=AsyncMock) as mock_load:
            await self.stage._ensure_handlers_initialized(self.context)
            
            # Should not attempt to load plugins
            mock_load.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_load_plugin_handlers_error(self):
        """Test plugin handler loading with error."""
        with patch('core.plugins.manager.PluginManager') as mock_manager_class:
            mock_manager = Mock()
            mock_manager.get_content_handlers.side_effect = Exception("Plugin error")
            mock_manager_class.return_value = mock_manager
            
            # Should not raise exception, just log warning
            await self.stage._load_plugin_handlers(self.context)
    
    def test_stage_name(self):
        """Test stage name."""
        assert self.stage.name == "processing"
    
    def test_stage_config(self):
        """Test stage configuration handling."""
        config = {
            "output_dir": "/custom/dir",
            "sleep_interval": 2.0
        }
        
        stage = ProcessingStage(config)
        assert stage.get_config("output_dir") == "/custom/dir"
        assert stage.get_config("sleep_interval") == 2.0
        assert stage.get_config("nonexistent", "default") == "default"