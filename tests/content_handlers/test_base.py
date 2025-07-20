"""
Tests for content handler base architecture
"""

import pytest
from pathlib import Path
from redditdl.content_handlers.base import (
    ContentHandlerRegistry,
    ContentTypeDetector,
    HandlerResult,
    BaseContentHandler,
    HandlerError
)
from redditdl.scrapers import PostMetadata


class MockContentHandler(BaseContentHandler):
    """Mock content handler for testing."""
    
    def __init__(self, name="mock", priority=100, content_types=None):
        super().__init__(name, priority)
        self._content_types = content_types or {"test"}
    
    @property
    def supported_content_types(self):
        return self._content_types
    
    def can_handle(self, post, content_type):
        return content_type in self._content_types
    
    async def process(self, post, output_dir, config):
        result = HandlerResult(
            success=True,
            handler_name=self.name,
            content_type=config.get('content_type', 'test')
        )
        result.add_operation("mock_processing")
        return result


class TestHandlerResult:
    """Test HandlerResult functionality."""
    
    def test_handler_result_creation(self):
        """Test creating a handler result."""
        result = HandlerResult()
        assert not result.success
        assert result.error_message == ""
        assert len(result.files_created) == 0
        assert len(result.operations_performed) == 0
    
    def test_add_file(self):
        """Test adding files to result."""
        result = HandlerResult()
        file_path = Path("/test/file.txt")
        
        result.add_file(file_path)
        assert file_path in result.files_created
        
        # Adding same file twice should not duplicate
        result.add_file(file_path)
        assert len(result.files_created) == 1
    
    def test_add_operation(self):
        """Test adding operations to result."""
        result = HandlerResult()
        
        result.add_operation("download")
        assert "download" in result.operations_performed
        
        # Adding same operation twice should not duplicate
        result.add_operation("download")
        assert len(result.operations_performed) == 1


class TestContentTypeDetector:
    """Test content type detection."""
    
    def test_detect_image_content(self):
        """Test image content detection."""
        post = PostMetadata(
            id="test1",
            title="Test Image",
            author="test_user",
            subreddit="test",
            url="https://i.redd.it/example.jpg",
            media_url="https://i.redd.it/example.jpg",
            date_iso="2023-01-01"
        )
        
        content_type = ContentTypeDetector.detect_content_type(post)
        assert content_type == "image"
    
    def test_detect_video_content(self):
        """Test video content detection."""
        post = PostMetadata(
            id="test2",
            title="Test Video",
            author="test_user",
            subreddit="test",
            url="https://v.redd.it/example",
            media_url="https://v.redd.it/example",
            date_iso="2023-01-01"
        )
        
        content_type = ContentTypeDetector.detect_content_type(post)
        assert content_type == "video"
    
    def test_detect_text_content(self):
        """Test text content detection."""
        post = PostMetadata(
            id="test3",
            title="Test Self Post",
            author="test_user",
            subreddit="test",
            url="https://reddit.com/r/test/comments/test3",
            selftext="This is a self post",
            date_iso="2023-01-01"
        )
        post.is_self = True
        
        content_type = ContentTypeDetector.detect_content_type(post)
        assert content_type == "text"
    
    def test_detect_external_content(self):
        """Test external link detection."""
        post = PostMetadata(
            id="test4",
            title="External Link",
            author="test_user",
            subreddit="test",
            url="https://example.com/article",
            date_iso="2023-01-01"
        )
        
        content_type = ContentTypeDetector.detect_content_type(post)
        assert content_type == "external"
    
    def test_is_media_content(self):
        """Test media content detection."""
        image_post = PostMetadata(
            id="test5",
            title="Image Post",
            author="test_user",
            subreddit="test",
            url="https://i.imgur.com/example.jpg",
            date_iso="2023-01-01"
        )
        
        assert ContentTypeDetector.is_media_content(image_post)
        
        text_post = PostMetadata(
            id="test6",
            title="Text Post",
            author="test_user",
            subreddit="test",
            url="https://reddit.com/r/test/comments/test6",
            selftext="Text content",
            date_iso="2023-01-01"
        )
        text_post.is_self = True
        
        assert not ContentTypeDetector.is_media_content(text_post)


class TestContentHandlerRegistry:
    """Test content handler registry functionality."""
    
    def test_register_handler(self):
        """Test registering a content handler."""
        registry = ContentHandlerRegistry()
        handler = MockContentHandler("test_handler", 50, {"image", "video"})
        
        registry.register_handler(handler)
        
        handlers = registry.list_all_handlers()
        assert len(handlers) == 1
        assert handlers[0] == handler
    
    def test_register_duplicate_handler(self):
        """Test registering the same handler twice."""
        registry = ContentHandlerRegistry()
        handler = MockContentHandler("test_handler")
        
        registry.register_handler(handler)
        registry.register_handler(handler)  # Should not duplicate
        
        handlers = registry.list_all_handlers()
        assert len(handlers) == 1
    
    def test_handler_priority_ordering(self):
        """Test that handlers are ordered by priority."""
        registry = ContentHandlerRegistry()
        
        high_priority = MockContentHandler("high", 10)
        low_priority = MockContentHandler("low", 90)
        medium_priority = MockContentHandler("medium", 50)
        
        registry.register_handler(low_priority)
        registry.register_handler(high_priority)
        registry.register_handler(medium_priority)
        
        handlers = registry.list_all_handlers()
        assert len(handlers) == 3
        assert handlers[0] == high_priority
        assert handlers[1] == medium_priority
        assert handlers[2] == low_priority
    
    def test_get_handler_for_post(self):
        """Test finding appropriate handler for a post."""
        registry = ContentHandlerRegistry()
        
        image_handler = MockContentHandler("image_handler", 50, {"image"})
        text_handler = MockContentHandler("text_handler", 60, {"text"})
        
        registry.register_handler(image_handler)
        registry.register_handler(text_handler)
        
        # Test image post
        image_post = PostMetadata(
            id="test_img",
            title="Image",
            author="user",
            subreddit="test",
            url="https://i.redd.it/example.jpg",
            date_iso="2023-01-01"
        )
        
        handler = registry.get_handler_for_post(image_post, "image")
        assert handler == image_handler
        
        # Test text post
        text_post = PostMetadata(
            id="test_txt",
            title="Text",
            author="user",
            subreddit="test",
            url="https://reddit.com/r/test/comments/test_txt",
            selftext="Text content",
            date_iso="2023-01-01"
        )
        
        handler = registry.get_handler_for_post(text_post, "text")
        assert handler == text_handler
        
        # Test unsupported content type
        handler = registry.get_handler_for_post(image_post, "unsupported")
        assert handler is None
    
    def test_get_handlers_for_content_type(self):
        """Test getting handlers for specific content type."""
        registry = ContentHandlerRegistry()
        
        handler1 = MockContentHandler("handler1", 50, {"image", "video"})
        handler2 = MockContentHandler("handler2", 60, {"image"})
        handler3 = MockContentHandler("handler3", 70, {"text"})
        
        registry.register_handler(handler1)
        registry.register_handler(handler2)
        registry.register_handler(handler3)
        
        image_handlers = registry.get_handlers_for_content_type("image")
        assert len(image_handlers) == 2
        assert handler1 in image_handlers
        assert handler2 in image_handlers
        
        text_handlers = registry.get_handlers_for_content_type("text")
        assert len(text_handlers) == 1
        assert handler3 in text_handlers
        
        unknown_handlers = registry.get_handlers_for_content_type("unknown")
        assert len(unknown_handlers) == 0
    
    def test_unregister_handler(self):
        """Test unregistering a handler."""
        registry = ContentHandlerRegistry()
        handler = MockContentHandler("test_handler")
        
        registry.register_handler(handler)
        assert len(registry.list_all_handlers()) == 1
        
        registry.unregister_handler(handler)
        assert len(registry.list_all_handlers()) == 0
    
    def test_get_handler_stats(self):
        """Test getting handler statistics."""
        registry = ContentHandlerRegistry()
        
        handler1 = MockContentHandler("handler1", 50, {"image", "video"})
        handler2 = MockContentHandler("handler2", 60, {"text"})
        
        registry.register_handler(handler1)
        registry.register_handler(handler2)
        
        stats = registry.get_handler_stats()
        
        assert stats['total_handlers'] == 2
        assert 'image' in stats['handlers_by_type']
        assert 'video' in stats['handlers_by_type']
        assert 'text' in stats['handlers_by_type']
        assert stats['handlers_by_type']['image'] == 1
        assert stats['handlers_by_type']['video'] == 1
        assert stats['handlers_by_type']['text'] == 1
        assert len(stats['handler_list']) == 2


class TestMockContentHandler:
    """Test the mock content handler."""
    
    @pytest.mark.asyncio
    async def test_mock_handler_processing(self):
        """Test mock handler processing."""
        handler = MockContentHandler("test", 50, {"test"})
        
        post = PostMetadata(
            id="test_post",
            title="Test Post",
            author="user",
            subreddit="test",
            url="https://example.com",
            date_iso="2023-01-01"
        )
        
        output_dir = Path("/tmp/test")
        config = {"content_type": "test"}
        
        result = await handler.process(post, output_dir, config)
        
        assert result.success
        assert result.handler_name == "test"
        assert result.content_type == "test"
        assert "mock_processing" in result.operations_performed
    
    def test_mock_handler_can_handle(self):
        """Test mock handler can_handle method."""
        handler = MockContentHandler("test", 50, {"image", "video"})
        
        post = PostMetadata(
            id="test_post",
            title="Test",
            author="user", 
            subreddit="test",
            url="https://example.com",
            date_iso="2023-01-01"
        )
        
        assert handler.can_handle(post, "image")
        assert handler.can_handle(post, "video")
        assert not handler.can_handle(post, "text")
    
    def test_mock_handler_properties(self):
        """Test mock handler properties."""
        handler = MockContentHandler("test_handler", 75, {"custom_type"})
        
        assert handler.name == "test_handler"
        assert handler.priority == 75
        assert handler.supported_content_types == {"custom_type"}
    
    def test_mock_handler_validation(self):
        """Test mock handler config validation."""
        handler = MockContentHandler()
        
        # Base handler should return empty errors for empty config
        errors = handler.validate_config({})
        assert errors == []