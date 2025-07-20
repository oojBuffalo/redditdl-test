#!/usr/bin/env python3
"""
Integration Tests

Tests the integration between different core systems:
- Pipeline + Plugin integration
- Event system + Plugin integration  
- End-to-end processing flow
"""

import pytest
import asyncio
import tempfile
import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch, MagicMock

# Add current directory to path for imports
sys.path.insert(0, '.')

from redditdl.core.plugins.manager import PluginManager
from redditdl.core.plugins.hooks import BaseContentHandler, BaseFilter
from redditdl.core.pipeline.executor import PipelineExecutor
from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.events.types import PostDiscoveredEvent, DownloadCompletedEvent


class MockContentHandler(BaseContentHandler):
    """Mock content handler for integration testing."""
    
    priority = 100
    
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        return content_type == "test_content"
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "success": True,
            "handler": "MockContentHandler",
            "processed_title": post_data.get("title", ""),
            "processed_url": post_data.get("url", "")
        }
    
    def get_supported_types(self) -> List[str]:
        return ["test_content"]


class MockFilter(BaseFilter):
    """Mock filter for integration testing."""
    
    priority = 100
    
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Filter posts based on score
        min_score = config.get('min_score', 0)
        return [post for post in posts if post.get('score', 0) >= min_score]
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "min_score": {
                "type": "integer",
                "default": 0,
                "description": "Minimum score threshold"
            }
        }


class PluginProcessingStage(PipelineStage):
    """Pipeline stage that uses plugins for content processing."""
    
    def __init__(self, plugin_manager: PluginManager):
        super().__init__("plugin_processing")
        self.plugin_manager = plugin_manager
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """Process posts using registered plugins."""
        processed_posts = []
        handlers_used = 0
        
        # Convert posts to dict format for processing
        posts_as_dicts = [post.__dict__ if hasattr(post, '__dict__') else post for post in context.posts]
        
        for post in posts_as_dicts:
            # Find appropriate content handler
            content_type = post.get('content_type', 'unknown')
            
            handlers = self.plugin_manager.get_content_handlers()
            suitable_handlers = [
                h for h in handlers 
                if h['instance'].can_handle(content_type, post)
            ]
            
            if suitable_handlers:
                # Use highest priority handler
                handler = sorted(suitable_handlers, key=lambda x: x['priority'])[0]
                result = await handler['instance'].process(post, context.config)
                
                # Merge processing results with original post
                processed_post = post.copy()
                processed_post.update(result)
                processed_posts.append(processed_post)
                if result.get('success'):
                    handlers_used += 1
            else:
                # No handler available, pass through unchanged
                processed_posts.append(post)
        
        # Update context with processed posts
        context.posts = processed_posts
        
        result = PipelineResult(
            success=True,
            stage_name=self.name,
            processed_count=len(processed_posts)
        )
        result.set_data("handlers_used", handlers_used)
        return result


class PluginFilterStage(PipelineStage):
    """Pipeline stage that uses plugins for filtering."""
    
    def __init__(self, plugin_manager: PluginManager):
        super().__init__("plugin_filtering")
        self.plugin_manager = plugin_manager
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """Filter posts using registered plugins."""
        # Convert posts to dict format for processing
        posts_as_dicts = [post.__dict__ if hasattr(post, '__dict__') else post for post in context.posts]
        original_count = len(posts_as_dicts)
        
        # Apply all registered filters
        filters = self.plugin_manager.get_filters()
        filters_applied = 0
        
        for filter_info in sorted(filters, key=lambda x: x['priority']):
            filter_config = context.config.get('filters', {}).get(
                filter_info['class'].__name__, {}
            )
            
            before_count = len(posts_as_dicts)
            posts_as_dicts = filter_info['instance'].apply(posts_as_dicts, filter_config)
            after_count = len(posts_as_dicts)
            
            if before_count != after_count:
                filters_applied += 1
        
        # Update context with filtered posts
        context.posts = posts_as_dicts
        filtered_count = original_count - len(posts_as_dicts)
        
        result = PipelineResult(
            success=True,
            stage_name=self.name,
            processed_count=len(posts_as_dicts)
        )
        result.set_data("original_count", original_count)
        result.set_data("filtered_count", filtered_count)
        result.set_data("filters_applied", filters_applied)
        return result


class TestPluginPipelineIntegration:
    """Test integration between plugins and pipeline system."""
    
    @pytest.fixture
    def plugin_manager(self):
        """Create a plugin manager with test plugins."""
        manager = PluginManager()
        
        # Register test plugins manually
        manager._register_content_handler("test_plugin", MockContentHandler)
        manager._register_filter("test_plugin", MockFilter)
        
        return manager
    
    @pytest.fixture
    def sample_posts(self):
        """Sample posts for testing."""
        return [
            {
                "id": "post1",
                "title": "Test Post 1",
                "url": "https://example.com/1",
                "content_type": "test_content",
                "score": 100
            },
            {
                "id": "post2", 
                "title": "Test Post 2",
                "url": "https://example.com/2",
                "content_type": "test_content",
                "score": 50
            },
            {
                "id": "post3",
                "title": "Test Post 3", 
                "url": "https://example.com/3",
                "content_type": "other_content",
                "score": 200
            }
        ]
    
    @pytest.mark.asyncio
    async def test_plugin_content_processing_stage(self, plugin_manager, sample_posts):
        """Test that pipeline stage can use plugins for content processing."""
        # Create processing stage with plugin manager
        stage = PluginProcessingStage(plugin_manager)
        
        # Create pipeline context
        context = PipelineContext(
            posts=sample_posts,
            config={},
            session_state={},
            events=None
        )
        
        # Process through stage
        result = await stage.process(context)
        
        # Verify results
        assert result.success is True
        assert result.processed_count == 3
        assert result.get_data("handlers_used") == 2  # Only 2 posts have test_content type
        
        processed_posts = context.posts
        
        # Check that test_content posts were processed by our handler
        test_content_posts = [p for p in processed_posts if p.get("content_type") == "test_content"]
        assert len(test_content_posts) == 2
        
        for post in test_content_posts:
            assert post["success"] is True
            assert post["handler"] == "MockContentHandler"
            assert "processed_title" in post
            assert "processed_url" in post
    
    @pytest.mark.asyncio
    async def test_plugin_filter_stage(self, plugin_manager, sample_posts):
        """Test that pipeline stage can use plugins for filtering."""
        # Create filter stage with plugin manager
        stage = PluginFilterStage(plugin_manager)
        
        # Create pipeline context with filter config
        context = PipelineContext(
            posts=sample_posts,
            config={
                "filters": {
                    "MockFilter": {
                        "min_score": 75  # Should filter out post2 (score=50)
                    }
                }
            },
            session_state={},
            events=None
        )
        
        # Process through stage
        result = await stage.process(context)
        
        # Verify results
        assert result.success is True
        assert result.get_data("original_count") == 3
        assert result.get_data("filtered_count") == 1  # post2 filtered out
        assert result.get_data("filters_applied") == 1
        
        filtered_posts = context.posts
        assert len(filtered_posts) == 2
        
        # Verify remaining posts have score >= 75
        for post in filtered_posts:
            assert post["score"] >= 75
    
    @pytest.mark.asyncio
    async def test_full_pipeline_with_plugins(self, plugin_manager, sample_posts):
        """Test complete pipeline execution with plugin stages."""
        # Create pipeline executor
        executor = PipelineExecutor()
        
        # Add plugin stages
        processing_stage = PluginProcessingStage(plugin_manager)
        filter_stage = PluginFilterStage(plugin_manager)
        
        executor.add_stage(processing_stage)
        executor.add_stage(filter_stage)
        
        # Create pipeline context
        context = PipelineContext(
            posts=sample_posts,
            config={
                "filters": {
                    "MockFilter": {
                        "min_score": 75
                    }
                }
            },
            session_state={"pipeline_id": "test_run"},
            events=None
        )
        
        # Execute pipeline
        metrics = await executor.execute(context)
        
        # Verify pipeline execution
        assert metrics.successful_stages == 2
        assert metrics.failed_stages == 0
        assert metrics.total_stages == 2
        
        # Check processing stage results
        processing_result = context.stage_results["plugin_processing"]
        assert processing_result.processed_count == 3
        assert processing_result.get_data("handlers_used") == 2
        
        # Check filter stage results  
        filter_result = context.stage_results["plugin_filtering"]
        assert filter_result.get_data("original_count") == 3
        assert filter_result.get_data("filtered_count") == 1
        
        # Final posts should be processed and filtered
        final_posts = context.posts
        assert len(final_posts) == 2
        
        for post in final_posts:
            assert post["score"] >= 75
            if post.get("content_type") == "test_content":
                assert post["success"] is True
                assert post["handler"] == "MockContentHandler"


class TestPluginEventIntegration:
    """Test integration between plugins and event system."""
    
    @pytest.fixture
    def event_emitter(self):
        """Create an event emitter for testing."""
        return EventEmitter()
    
    @pytest.fixture
    def plugin_manager(self):
        """Create a plugin manager for testing."""
        return PluginManager()
    
    @pytest.mark.asyncio
    async def test_plugin_event_observation(self, event_emitter, plugin_manager):
        """Test that plugins can observe events."""
        events_received = []
        
        def event_handler(event):
            events_received.append(event)
        
        # Subscribe to events
        event_emitter.subscribe(PostDiscoveredEvent, event_handler)
        
        # Emit some events
        event1 = PostDiscoveredEvent(
            post_count=1,
            source="reddit",
            target="test1",
            posts_preview=[{"title": "Test Post"}]
        )
        event2 = DownloadCompletedEvent(
            post_id="test1", 
            success=True,
            local_path="/tmp/test.jpg"
        )
        
        await event_emitter.emit_async(event1)
        await event_emitter.emit_async(event2)
        
        # Verify events were received
        assert len(events_received) == 1  # Only PostDiscoveredEvent subscribed
        assert events_received[0].target == "test1"
        assert events_received[0].source == "reddit"
    
    @pytest.mark.asyncio
    async def test_plugin_pipeline_event_integration(self, plugin_manager, event_emitter):
        """Test that pipeline stages can emit events when using plugins."""
        events_received = []
        
        def collect_events(event):
            events_received.append(event)
        
        # Subscribe to all events
        event_emitter.subscribe(PostDiscoveredEvent, collect_events)
        event_emitter.subscribe(DownloadCompletedEvent, collect_events)
        
        # Create pipeline stage that emits events
        class EventEmittingStage(PipelineStage):
            def __init__(self, emitter):
                super().__init__("event_emitting")
                self.emitter = emitter
            
            async def process(self, context: PipelineContext) -> PipelineResult:
                posts_as_dicts = [post.__dict__ if hasattr(post, '__dict__') else post for post in context.posts]
                
                for post in posts_as_dicts:
                    # Emit discovery event
                    discovery_event = PostDiscoveredEvent(
                        post_count=1,
                        source="test",
                        target=post["id"],
                        posts_preview=[post]
                    )
                    await self.emitter.emit_async(discovery_event)
                    
                    # Emit completion event
                    completion_event = DownloadCompletedEvent(
                        post_id=post["id"],
                        success=True,
                        local_path=f"/tmp/{post['id']}.jpg"
                    )
                    await self.emitter.emit_async(completion_event)
                
                return PipelineResult(
                    success=True,
                    stage_name=self.name,
                    processed_count=len(posts_as_dicts)
                )
        
        # Create and execute pipeline
        executor = PipelineExecutor()
        stage = EventEmittingStage(event_emitter)
        executor.add_stage(stage)
        
        sample_posts = [
            {"id": "post1", "title": "Test 1"},
            {"id": "post2", "title": "Test 2"}
        ]
        
        context = PipelineContext(
            posts=sample_posts,
            config={},
            session_state={},
            events=None
        )
        
        metrics = await executor.execute(context)
        
        # Verify events were emitted
        assert metrics.successful_stages == 1
        assert len(events_received) == 4  # 2 discovery + 2 completion events
        
        discovery_events = [e for e in events_received if isinstance(e, PostDiscoveredEvent)]
        completion_events = [e for e in events_received if isinstance(e, DownloadCompletedEvent)]
        
        assert len(discovery_events) == 2
        assert len(completion_events) == 2


class TestPluginDiscoveryIntegration:
    """Test plugin discovery and loading integration."""
    
    @pytest.mark.asyncio
    async def test_directory_plugin_discovery_and_loading(self):
        """Test discovering and loading plugins from directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a test plugin directory
            plugin_dir = temp_path / "test_integration_plugin"
            plugin_dir.mkdir()
            
            # Create plugin manifest
            manifest = {
                "name": "test_integration_plugin",
                "version": "1.0.0",
                "description": "Integration test plugin",
                "author": "Test Suite"
            }
            
            manifest_file = plugin_dir / "plugin.json"
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f)
            
            # Create plugin implementation
            plugin_code = '''
from core.plugins.hooks import BaseContentHandler
from typing import Any, Dict, List

class IntegrationContentHandler(BaseContentHandler):
    priority = 100
    
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        return content_type == "integration_test"
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "success": True,
            "integration_test": True,
            "processed_by": "IntegrationContentHandler"
        }
    
    def get_supported_types(self) -> List[str]:
        return ["integration_test"]

def initialize_plugin():
    pass

def cleanup_plugin():
    pass
'''
            
            init_file = plugin_dir / "__init__.py"
            with open(init_file, 'w') as f:
                f.write(plugin_code)
            
            # Test plugin discovery and loading
            manager = PluginManager(plugin_dirs=[str(temp_dir)])
            
            # Discover plugins
            discovered = manager.discover_plugins()
            assert len(discovered) == 1
            assert discovered[0]["name"] == "test_integration_plugin"
            
            # Load plugin (mock the sandbox to avoid import issues)
            with patch.object(manager, '_apply_sandbox'):
                success = manager.load_plugin(discovered[0])
                assert success
            
            # Verify plugin is loaded and registered
            assert "test_integration_plugin" in manager._loaded_plugins
            handlers = manager.get_content_handlers()
            assert len(handlers) == 1
            assert handlers[0]["class"].__name__ == "IntegrationContentHandler"
            
            # Test the handler works
            handler = handlers[0]["instance"]
            assert handler.can_handle("integration_test", {})
            
            result = await handler.process(
                {"id": "test", "title": "Test Post"}, 
                {}
            )
            assert result["success"] is True
            assert result["integration_test"] is True
            assert result["processed_by"] == "IntegrationContentHandler"


if __name__ == "__main__":
    pytest.main([__file__])