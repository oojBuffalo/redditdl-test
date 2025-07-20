"""
Tests for Target Handlers System

Tests specialized target handlers including UserTargetHandler, SubredditTargetHandler,
SavedPostsHandler, UpvotedPostsHandler, and the BatchTargetProcessor.
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import List, Dict, Any

from redditdl.targets.handlers import (
    UserTargetHandler,
    SubredditTargetHandler, 
    SavedPostsHandler,
    UpvotedPostsHandler,
    TargetHandlerRegistry,
    BatchTargetProcessor,
    TargetProcessingResult,
    BatchProcessingConfig,
    ListingType,
    TimePeriod
)
from redditdl.targets.resolver import TargetInfo, TargetType
from redditdl.targets.base_scraper import ScrapingConfig
from redditdl.scrapers import PostMetadata
from redditdl.targets.scrapers import AuthenticationError, TargetNotFoundError, ScrapingError


class TestUserTargetHandler:
    """Test UserTargetHandler functionality."""
    
    @pytest.fixture
    def scraping_config(self):
        """Create a test scraping configuration."""
        return ScrapingConfig(
            post_limit=10,
            sleep_interval=0.7,
            timeout=30.0,
            retries=3,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    @pytest.fixture
    def handler(self, scraping_config):
        """Create a UserTargetHandler instance."""
        return UserTargetHandler(scraping_config)
    
    @pytest.fixture
    def user_target_info(self):
        """Create a user target info."""
        return TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="u/testuser",
            metadata={}
        )
    
    def test_supported_target_types(self, handler):
        """Test that handler supports USER target type."""
        assert TargetType.USER in handler.supported_target_types
        assert len(handler.supported_target_types) == 1
    
    def test_can_handle_target_user(self, handler, user_target_info):
        """Test that handler can handle user targets."""
        assert handler.can_handle_target(user_target_info)
    
    def test_can_handle_target_other(self, handler):
        """Test that handler rejects non-user targets."""
        subreddit_target = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="python",
            original_input="r/python",
            metadata={}
        )
        assert not handler.can_handle_target(subreddit_target)
    
    @pytest.mark.asyncio
    async def test_process_target_success(self, handler, user_target_info):
        """Test successful user target processing."""
        # Mock scraper
        mock_scraper = Mock()
        mock_scraper.scraper_type = "test_scraper"
        mock_scraper.fetch_posts.return_value = [
            PostMetadata(
                id="test1",
                title="Test Post 1",
                url="https://example.com/1",
                subreddit="testsubreddit",
                author="testuser"
            ),
            PostMetadata(
                id="test2", 
                title="Test Post 2",
                url="https://example.com/2",
                subreddit="testsubreddit",
                author="testuser"
            )
        ]
        
        # Mock get_scraper method
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        # Mock _gather_user_metadata
        handler._gather_user_metadata = AsyncMock(return_value={
            'username': 'testuser',
            'account_created': 1234567890
        })
        
        result = await handler.process_target(user_target_info)
        
        assert result.success is True
        assert len(result.posts) == 2
        assert result.target_info == user_target_info
        assert result.error_message is None
        assert result.processing_time > 0
        assert result.metadata['target_type'] == 'user'
        assert result.metadata['username'] == 'testuser'
        assert result.metadata['post_count'] == 2
        assert result.metadata['scraper_type'] == 'test_scraper'
    
    @pytest.mark.asyncio
    async def test_process_target_authentication_error(self, handler, user_target_info):
        """Test user target processing with authentication error."""
        # Mock scraper that raises AuthenticationError
        mock_scraper = Mock()
        mock_scraper.fetch_posts.side_effect = AuthenticationError("Authentication failed")
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(user_target_info)
        
        assert result.success is False
        assert len(result.posts) == 0
        assert result.error_message == "Authentication failed"
        assert result.processing_time > 0
    
    @pytest.mark.asyncio
    async def test_process_target_not_found_error(self, handler, user_target_info):
        """Test user target processing with target not found error."""
        # Mock scraper that raises TargetNotFoundError
        mock_scraper = Mock()
        mock_scraper.fetch_posts.side_effect = TargetNotFoundError("User not found")
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(user_target_info)
        
        assert result.success is False
        assert len(result.posts) == 0
        assert result.error_message == "User not found"
    
    @pytest.mark.asyncio
    async def test_gather_user_metadata_with_praw(self, handler, user_target_info):
        """Test gathering user metadata with PRAW scraper."""
        # Mock PRAW scraper
        mock_reddit = Mock()
        mock_user = Mock()
        mock_user.created_utc = 1234567890
        mock_user.comment_karma = 1000
        mock_user.link_karma = 500
        mock_user.verified = True
        mock_reddit.redditor.return_value = mock_user
        
        mock_scraper = Mock()
        mock_scraper.reddit = mock_reddit
        
        metadata = await handler._gather_user_metadata(user_target_info, mock_scraper)
        
        assert metadata['username'] == 'testuser'
        assert metadata['account_created'] == 1234567890
        assert metadata['comment_karma'] == 1000
        assert metadata['link_karma'] == 500
        assert metadata['is_verified'] is True


class TestSubredditTargetHandler:
    """Test SubredditTargetHandler functionality."""
    
    @pytest.fixture
    def scraping_config(self):
        """Create a test scraping configuration."""
        return ScrapingConfig(
            post_limit=25,
            sleep_interval=0.7,
            timeout=30.0,
            retries=3,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    @pytest.fixture
    def handler(self, scraping_config):
        """Create a SubredditTargetHandler instance."""
        return SubredditTargetHandler(scraping_config)
    
    @pytest.fixture
    def subreddit_target_info(self):
        """Create a subreddit target info."""
        return TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="python",
            original_input="r/python",
            metadata={'listing_type': 'hot', 'time_period': 'week'}
        )
    
    def test_supported_target_types(self, handler):
        """Test that handler supports SUBREDDIT target type."""
        assert TargetType.SUBREDDIT in handler.supported_target_types
        assert len(handler.supported_target_types) == 1
    
    def test_can_handle_target_subreddit(self, handler, subreddit_target_info):
        """Test that handler can handle subreddit targets."""
        assert handler.can_handle_target(subreddit_target_info)
    
    def test_get_listing_type_valid(self, handler, subreddit_target_info):
        """Test parsing valid listing type from metadata."""
        listing_type = handler._get_listing_type(subreddit_target_info)
        assert listing_type == ListingType.HOT
    
    def test_get_listing_type_invalid(self, handler):
        """Test handling invalid listing type in metadata."""
        target_info = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="python",
            original_input="r/python",
            metadata={'listing_type': 'invalid'}
        )
        listing_type = handler._get_listing_type(target_info)
        assert listing_type == ListingType.NEW  # Default fallback
    
    def test_get_time_period_valid(self, handler, subreddit_target_info):
        """Test parsing valid time period from metadata."""
        time_period = handler._get_time_period(subreddit_target_info)
        assert time_period == TimePeriod.WEEK
    
    def test_get_time_period_none(self, handler):
        """Test handling missing time period in metadata."""
        target_info = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="python",
            original_input="r/python",
            metadata={}
        )
        time_period = handler._get_time_period(target_info)
        assert time_period is None
    
    @pytest.mark.asyncio
    async def test_process_target_success(self, handler, subreddit_target_info):
        """Test successful subreddit target processing."""
        # Mock scraper
        mock_scraper = Mock()
        mock_scraper.scraper_type = "test_scraper"
        
        # Mock _fetch_subreddit_posts method
        handler._fetch_subreddit_posts = AsyncMock(return_value=[
            PostMetadata(
                id="post1",
                title="Python Post 1",
                url="https://example.com/1",
                subreddit="python",
                author="user1"
            ),
            PostMetadata(
                id="post2",
                title="Python Post 2", 
                url="https://example.com/2",
                subreddit="python",
                author="user2"
            )
        ])
        
        # Mock _gather_subreddit_metadata
        handler._gather_subreddit_metadata = AsyncMock(return_value={
            'subreddit': 'python',
            'subscribers': 50000
        })
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(subreddit_target_info)
        
        assert result.success is True
        assert len(result.posts) == 2
        assert result.metadata['target_type'] == 'subreddit'
        assert result.metadata['subreddit'] == 'python'
        assert result.metadata['listing_type'] == 'hot'
        assert result.metadata['time_period'] == 'week'
        assert result.metadata['post_count'] == 2
    
    @pytest.mark.asyncio
    async def test_fetch_with_praw_listings_hot(self, handler, subreddit_target_info):
        """Test fetching posts using PRAW with HOT listing."""
        # Mock PRAW structures
        mock_submission = Mock()
        mock_submission.id = "test_id"
        mock_submission.title = "Test Title"
        mock_submission.selftext = "Test content"
        mock_submission.subreddit = "python"
        mock_submission.permalink = "/r/python/comments/test_id/"
        mock_submission.url = "https://reddit.com/test"
        mock_submission.author = Mock()
        mock_submission.author.__str__ = Mock(return_value="testuser")
        mock_submission.is_video = False
        mock_submission.created_utc = 1234567890
        mock_submission.url_overridden_by_dest = "https://example.com"
        mock_submission.score = 100
        mock_submission.num_comments = 50
        mock_submission.over_18 = False
        mock_submission.is_self = False
        
        mock_subreddit = Mock()
        mock_subreddit.hot.return_value = [mock_submission]
        
        mock_reddit = Mock()
        mock_reddit.subreddit.return_value = mock_subreddit
        
        mock_scraper = Mock()
        mock_scraper.reddit = mock_reddit
        mock_scraper.get_rate_limit_interval.return_value = 0.01
        
        with patch('redditdl.scrapers.PostMetadata.from_raw') as mock_from_raw:
            mock_from_raw.return_value = PostMetadata(
                id="test_id",
                title="Test Title",
                url="https://example.com",
                subreddit="python",
                author="testuser"
            )
            
            posts = await handler._fetch_with_praw_listings(
                mock_scraper, subreddit_target_info, ListingType.HOT, TimePeriod.WEEK
            )
        
        assert len(posts) == 1
        assert posts[0].id == "test_id"
        mock_subreddit.hot.assert_called_once_with(limit=25)


class TestSavedPostsHandler:
    """Test SavedPostsHandler functionality."""
    
    @pytest.fixture
    def scraping_config(self):
        """Create a test scraping configuration."""
        return ScrapingConfig(
            post_limit=50,
            sleep_interval=0.7,
            timeout=30.0,
            retries=3,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    @pytest.fixture
    def handler(self, scraping_config):
        """Create a SavedPostsHandler instance."""
        return SavedPostsHandler(scraping_config)
    
    @pytest.fixture
    def saved_target_info(self):
        """Create a saved posts target info."""
        return TargetInfo(
            target_type=TargetType.SAVED,
            target_value="saved",
            original_input="saved",
            metadata={}
        )
    
    def test_supported_target_types(self, handler):
        """Test that handler supports SAVED target type."""
        assert TargetType.SAVED in handler.supported_target_types
        assert len(handler.supported_target_types) == 1
    
    def test_can_handle_target_saved(self, handler, saved_target_info):
        """Test that handler can handle saved targets."""
        assert handler.can_handle_target(saved_target_info)
    
    @pytest.mark.asyncio
    async def test_process_target_authentication_required(self, handler, saved_target_info):
        """Test that saved posts require authentication."""
        # Mock scraper without authentication
        mock_scraper = Mock()
        mock_scraper.requires_authentication = False
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(saved_target_info)
        
        assert result.success is False
        assert "authentication" in result.error_message.lower()
    
    @pytest.mark.asyncio
    async def test_process_target_success(self, handler, saved_target_info):
        """Test successful saved posts processing."""
        # Mock authenticated scraper
        mock_scraper = Mock()
        mock_scraper.requires_authentication = True
        mock_scraper.scraper_type = "praw_scraper"
        
        # Mock _fetch_saved_posts_paginated
        handler._fetch_saved_posts_paginated = AsyncMock(return_value=[
            PostMetadata(
                id="saved1",
                title="Saved Post 1",
                url="https://example.com/saved1",
                subreddit="python",
                author="user1"
            )
        ])
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(saved_target_info)
        
        assert result.success is True
        assert len(result.posts) == 1
        assert result.metadata['target_type'] == 'saved'
        assert result.metadata['requires_auth'] is True
        assert result.metadata['post_count'] == 1


class TestUpvotedPostsHandler:
    """Test UpvotedPostsHandler functionality."""
    
    @pytest.fixture
    def scraping_config(self):
        """Create a test scraping configuration."""
        return ScrapingConfig(
            post_limit=50,
            sleep_interval=0.7,
            timeout=30.0,
            retries=3,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    @pytest.fixture
    def handler(self, scraping_config):
        """Create an UpvotedPostsHandler instance."""
        return UpvotedPostsHandler(scraping_config)
    
    @pytest.fixture
    def upvoted_target_info(self):
        """Create an upvoted posts target info."""
        return TargetInfo(
            target_type=TargetType.UPVOTED,
            target_value="upvoted",
            original_input="upvoted",
            metadata={}
        )
    
    def test_supported_target_types(self, handler):
        """Test that handler supports UPVOTED target type."""
        assert TargetType.UPVOTED in handler.supported_target_types
        assert len(handler.supported_target_types) == 1
    
    def test_can_handle_target_upvoted(self, handler, upvoted_target_info):
        """Test that handler can handle upvoted targets."""
        assert handler.can_handle_target(upvoted_target_info)
    
    @pytest.mark.asyncio
    async def test_process_target_authentication_required(self, handler, upvoted_target_info):
        """Test that upvoted posts require authentication."""
        # Mock scraper without authentication
        mock_scraper = Mock()
        mock_scraper.requires_authentication = False
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(upvoted_target_info)
        
        assert result.success is False
        assert "authentication" in result.error_message.lower()
    
    @pytest.mark.asyncio
    async def test_process_target_success(self, handler, upvoted_target_info):
        """Test successful upvoted posts processing."""
        # Mock authenticated scraper
        mock_scraper = Mock()
        mock_scraper.requires_authentication = True
        mock_scraper.scraper_type = "praw_scraper"
        
        # Mock _fetch_upvoted_posts_paginated
        handler._fetch_upvoted_posts_paginated = AsyncMock(return_value=[
            PostMetadata(
                id="upvoted1",
                title="Upvoted Post 1",
                url="https://example.com/upvoted1",
                subreddit="aww",
                author="user1"
            )
        ])
        
        handler.get_scraper = Mock(return_value=mock_scraper)
        
        result = await handler.process_target(upvoted_target_info)
        
        assert result.success is True
        assert len(result.posts) == 1
        assert result.metadata['target_type'] == 'upvoted'
        assert result.metadata['requires_auth'] is True
        assert result.metadata['post_count'] == 1


class TestTargetHandlerRegistry:
    """Test TargetHandlerRegistry functionality."""
    
    @pytest.fixture
    def scraping_config(self):
        """Create a test scraping configuration."""
        return ScrapingConfig(
            post_limit=10,
            sleep_interval=0.7,
            timeout=30.0,
            retries=3,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    @pytest.fixture
    def registry(self):
        """Create a TargetHandlerRegistry instance."""
        return TargetHandlerRegistry()
    
    def test_register_handler(self, registry, scraping_config):
        """Test registering a handler with the registry."""
        initial_count = len(registry.handlers)
        registry.register_handler(UserTargetHandler, scraping_config)
        assert len(registry.handlers) == initial_count + 1
        assert isinstance(registry.handlers[-1], UserTargetHandler)
    
    def test_register_default_handlers(self, registry, scraping_config):
        """Test registering all default handlers."""
        registry.register_default_handlers(scraping_config)
        assert len(registry.handlers) == 4  # User, Subreddit, Saved, Upvoted
        
        # Check that all handler types are present
        handler_types = [type(handler) for handler in registry.handlers]
        assert UserTargetHandler in handler_types
        assert SubredditTargetHandler in handler_types
        assert SavedPostsHandler in handler_types
        assert UpvotedPostsHandler in handler_types
    
    def test_get_handler_user(self, registry, scraping_config):
        """Test getting handler for user target."""
        registry.register_default_handlers(scraping_config)
        
        user_target = TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="u/testuser",
            metadata={}
        )
        
        handler = registry.get_handler(user_target)
        assert isinstance(handler, UserTargetHandler)
    
    def test_get_handler_subreddit(self, registry, scraping_config):
        """Test getting handler for subreddit target."""
        registry.register_default_handlers(scraping_config)
        
        subreddit_target = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="python",
            original_input="r/python",
            metadata={}
        )
        
        handler = registry.get_handler(subreddit_target)
        assert isinstance(handler, SubredditTargetHandler)
    
    def test_get_handler_not_found(self, registry, scraping_config):
        """Test getting handler for unsupported target type."""
        # Register only user handler
        registry.register_handler(UserTargetHandler, scraping_config)
        
        subreddit_target = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="python",
            original_input="r/python",
            metadata={}
        )
        
        handler = registry.get_handler(subreddit_target)
        assert handler is None
    
    def test_get_supported_target_types(self, registry, scraping_config):
        """Test getting all supported target types."""
        registry.register_default_handlers(scraping_config)
        
        supported_types = registry.get_supported_target_types()
        expected_types = [TargetType.USER, TargetType.SUBREDDIT, TargetType.SAVED, TargetType.UPVOTED]
        
        for target_type in expected_types:
            assert target_type in supported_types


class TestBatchTargetProcessor:
    """Test BatchTargetProcessor functionality."""
    
    @pytest.fixture
    def batch_config(self):
        """Create a batch processing configuration."""
        return BatchProcessingConfig(
            max_concurrent=2,
            rate_limit_delay=0.1,  # Short delay for tests
            retry_attempts=1,
            retry_delay=0.1,
            timeout_per_target=5.0,
            fail_fast=False
        )
    
    @pytest.fixture
    def scraping_config(self):
        """Create a test scraping configuration."""
        return ScrapingConfig(
            post_limit=5,
            sleep_interval=0.7,
            timeout=30.0,
            retries=3,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    @pytest.fixture
    def processor(self, batch_config, scraping_config):
        """Create a BatchTargetProcessor instance."""
        return BatchTargetProcessor(batch_config, scraping_config)
    
    @pytest.fixture
    def target_infos(self):
        """Create a list of target infos for testing."""
        return [
            TargetInfo(
                target_type=TargetType.USER,
                target_value="user1",
                original_input="u/user1",
                metadata={}
            ),
            TargetInfo(
                target_type=TargetType.USER,
                target_value="user2",
                original_input="u/user2",
                metadata={}
            ),
            TargetInfo(
                target_type=TargetType.SUBREDDIT,
                target_value="python",
                original_input="r/python",
                metadata={}
            )
        ]
    
    def test_initialization(self, processor):
        """Test processor initialization."""
        assert isinstance(processor.registry, TargetHandlerRegistry)
        assert len(processor.registry.handlers) == 4  # Default handlers registered
    
    @pytest.mark.asyncio
    async def test_process_targets_empty_list(self, processor):
        """Test processing empty target list."""
        results = await processor.process_targets([])
        assert results == []
    
    @pytest.mark.asyncio
    async def test_process_targets_success(self, processor, target_infos):
        """Test successful processing of multiple targets."""
        # Mock all handlers to return successful results
        for handler in processor.registry.handlers:
            handler.process_target = AsyncMock(return_value=TargetProcessingResult(
                target_info=target_infos[0],  # Will be overridden by actual target_info
                posts=[
                    PostMetadata(
                        id="test_post",
                        title="Test Post",
                        url="https://example.com",
                        subreddit="test",
                        author="testuser"
                    )
                ],
                success=True,
                processing_time=0.5
            ))
        
        results = await processor.process_targets(target_infos)
        
        assert len(results) == 3
        assert all(result.success for result in results)
        assert all(len(result.posts) == 1 for result in results)
    
    @pytest.mark.asyncio
    async def test_process_targets_mixed_results(self, processor, target_infos):
        """Test processing with mixed success/failure results."""
        # Mock handlers to return mixed results
        async def mock_process_target(target_info):
            if target_info.target_value == "user1":
                return TargetProcessingResult(
                    target_info=target_info,
                    posts=[PostMetadata(
                        id="test1",
                        title="Test 1",
                        url="https://example.com/1",
                        subreddit="test",
                        author="user1"
                    )],
                    success=True,
                    processing_time=0.3
                )
            else:
                return TargetProcessingResult(
                    target_info=target_info,
                    posts=[],
                    success=False,
                    error_message="Processing failed",
                    processing_time=0.2
                )
        
        for handler in processor.registry.handlers:
            handler.process_target = mock_process_target
        
        results = await processor.process_targets(target_infos)
        
        assert len(results) == 3
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        assert len(successful_results) == 1
        assert len(failed_results) == 2
        assert successful_results[0].target_info.target_value == "user1"
    
    @pytest.mark.asyncio
    async def test_process_targets_concurrent_execution(self, processor, target_infos):
        """Test that targets are processed concurrently within limits."""
        # Track processing times to verify concurrency
        process_times = []
        
        async def mock_process_target(target_info):
            start_time = time.time()
            await asyncio.sleep(0.2)  # Simulate processing time
            end_time = time.time()
            process_times.append((start_time, end_time))
            
            return TargetProcessingResult(
                target_info=target_info,
                posts=[],
                success=True,
                processing_time=end_time - start_time
            )
        
        for handler in processor.registry.handlers:
            handler.process_target = mock_process_target
        
        start_total = time.time()
        results = await processor.process_targets(target_infos)
        end_total = time.time()
        
        assert len(results) == 3
        assert all(r.success for r in results)
        
        # Verify that processing was concurrent (total time should be less than sequential)
        total_time = end_total - start_total
        sequential_time = len(target_infos) * 0.2
        # Allow some overhead for concurrency setup but should be faster than fully sequential
        assert total_time < sequential_time * 1.2  # Allow 20% overhead for concurrency
    
    @pytest.mark.asyncio
    async def test_process_targets_error_isolation(self, processor, target_infos):
        """Test that error in one target doesn't halt others."""
        call_count = 0
        
        async def mock_process_target(target_info):
            nonlocal call_count
            call_count += 1
            
            if target_info.target_value == "user2":
                # Simulate an exception
                raise Exception("Simulated processing error")
            
            return TargetProcessingResult(
                target_info=target_info,
                posts=[PostMetadata(
                    id=f"post_{call_count}",
                    title=f"Post {call_count}",
                    url=f"https://example.com/{call_count}",
                    subreddit="test",
                    author=target_info.target_value
                )],
                success=True,
                processing_time=0.1
            )
        
        for handler in processor.registry.handlers:
            handler.process_target = mock_process_target
        
        results = await processor.process_targets(target_infos)
        
        assert len(results) == 3
        
        # Check that successful targets still processed
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        assert len(successful_results) == 2  # user1 and python should succeed
        assert len(failed_results) == 1     # user2 should fail
        
        # Verify the failed result has proper error message
        failed_result = failed_results[0]
        assert failed_result.target_info.target_value == "user2"
        assert "error" in failed_result.error_message.lower()
    
    @pytest.mark.asyncio
    async def test_process_targets_no_handler_available(self, processor):
        """Test processing target with no available handler."""
        # Create a target type that has no handler
        unsupported_target = TargetInfo(
            target_type=TargetType.URL,  # Assuming URL type exists but no handler
            target_value="https://example.com",
            original_input="https://example.com",
            metadata={}
        )
        
        results = await processor.process_targets([unsupported_target])
        
        assert len(results) == 1
        assert not results[0].success
        assert "No handler available" in results[0].error_message
    
    @pytest.mark.asyncio 
    async def test_process_targets_timeout(self, processor, target_infos):
        """Test target processing timeout handling."""
        # Set very short timeout
        processor.config.timeout_per_target = 0.1
        
        # Mock handler that takes too long
        async def slow_process_target(target_info):
            await asyncio.sleep(0.5)  # Longer than timeout
            return TargetProcessingResult(
                target_info=target_info,
                posts=[],
                success=True,
                processing_time=0.5
            )
        
        for handler in processor.registry.handlers:
            handler.process_target = slow_process_target
        
        results = await processor.process_targets([target_infos[0]])  # Test with one target
        
        assert len(results) == 1
        assert not results[0].success
        assert "timed out" in results[0].error_message.lower()


class TestIntegrationMultiTarget:
    """Integration tests for multi-target functionality."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_multi_target_processing(self):
        """Test complete end-to-end multi-target processing workflow."""
        # Create realistic configurations
        batch_config = BatchProcessingConfig(
            max_concurrent=2,
            rate_limit_delay=0.05,  # Minimal delay for tests
            retry_attempts=1,
            timeout_per_target=10.0
        )
        
        scraping_config = ScrapingConfig(
            post_limit=3,
            sleep_interval=0.1,
            timeout=30.0,
            retries=2
        )
        
        # Create processor
        processor = BatchTargetProcessor(batch_config, scraping_config)
        
        # Create mixed target list
        targets = [
            TargetInfo(
                target_type=TargetType.USER,
                target_value="testuser1",
                original_input="u/testuser1",
                metadata={}
            ),
            TargetInfo(
                target_type=TargetType.SUBREDDIT,
                target_value="programming",
                original_input="r/programming",
                metadata={'listing_type': 'hot'}
            )
        ]
        
        # Mock scrapers to return predictable results
        mock_posts_user = [
            PostMetadata(
                id="user_post_1",
                title="User Post 1",
                url="https://reddit.com/user_post_1",
                subreddit="programming",
                author="testuser1"
            )
        ]
        
        mock_posts_subreddit = [
            PostMetadata(
                id="sub_post_1",
                title="Subreddit Post 1", 
                url="https://reddit.com/sub_post_1",
                subreddit="programming",
                author="random_user"
            ),
            PostMetadata(
                id="sub_post_2",
                title="Subreddit Post 2",
                url="https://reddit.com/sub_post_2", 
                subreddit="programming",
                author="another_user"
            )
        ]
        
        # Mock the handlers
        for handler in processor.registry.handlers:
            if isinstance(handler, UserTargetHandler):
                handler.process_target = AsyncMock(return_value=TargetProcessingResult(
                    target_info=targets[0],
                    posts=mock_posts_user,
                    success=True,
                    processing_time=0.5,
                    metadata={
                        'target_type': 'user',
                        'username': 'testuser1',
                        'post_count': 1,
                        'scraper_type': 'mock_scraper'
                    }
                ))
            elif isinstance(handler, SubredditTargetHandler):
                handler.process_target = AsyncMock(return_value=TargetProcessingResult(
                    target_info=targets[1],
                    posts=mock_posts_subreddit,
                    success=True,
                    processing_time=0.7,
                    metadata={
                        'target_type': 'subreddit',
                        'subreddit': 'programming',
                        'listing_type': 'hot',
                        'post_count': 2,
                        'scraper_type': 'mock_scraper'
                    }
                ))
        
        # Process targets
        results = await processor.process_targets(targets)
        
        # Verify results
        assert len(results) == 2
        assert all(result.success for result in results)
        
        # Check user target result
        user_result = next(r for r in results if r.metadata.get('target_type') == 'user')
        assert len(user_result.posts) == 1
        assert user_result.posts[0].author == "testuser1"
        assert user_result.metadata['username'] == 'testuser1'
        
        # Check subreddit target result
        subreddit_result = next(r for r in results if r.metadata.get('target_type') == 'subreddit')
        assert len(subreddit_result.posts) == 2
        assert subreddit_result.metadata['subreddit'] == 'programming'
        assert subreddit_result.metadata['listing_type'] == 'hot'
        
        # Verify all posts collected
        total_posts = sum(len(result.posts) for result in results)
        assert total_posts == 3
    
    @pytest.mark.asyncio
    async def test_progress_tracking_per_target(self):
        """Test that progress tracking works correctly per target."""
        batch_config = BatchProcessingConfig(max_concurrent=1, rate_limit_delay=0.05)
        scraping_config = ScrapingConfig(post_limit=5, sleep_interval=0.1)
        
        processor = BatchTargetProcessor(batch_config, scraping_config)
        
        targets = [
            TargetInfo(
                target_type=TargetType.USER,
                target_value=f"user{i}",
                original_input=f"u/user{i}",
                metadata={}
            ) for i in range(1, 4)
        ]
        
        # Track processing order and timing
        processing_log = []
        
        async def tracked_process_target(target_info):
            start_time = time.time()
            processing_log.append(f"Started: {target_info.target_value}")
            
            # Simulate variable processing times
            processing_time = 0.1 * int(target_info.target_value[-1])  # user1=0.1s, user2=0.2s, etc.
            await asyncio.sleep(processing_time)
            
            end_time = time.time()
            processing_log.append(f"Finished: {target_info.target_value}")
            
            return TargetProcessingResult(
                target_info=target_info,
                posts=[PostMetadata(
                    id=f"post_{target_info.target_value}",
                    title=f"Post by {target_info.target_value}",
                    url=f"https://reddit.com/{target_info.target_value}",
                    subreddit="test",
                    author=target_info.target_value
                )],
                success=True,
                processing_time=end_time - start_time
            )
        
        # Mock handlers
        for handler in processor.registry.handlers:
            if isinstance(handler, UserTargetHandler):
                handler.process_target = tracked_process_target
        
        results = await processor.process_targets(targets)
        
        # Verify all targets processed successfully
        assert len(results) == 3
        assert all(result.success for result in results)
        
        # Verify processing times are tracked per target
        for result in results:
            assert result.processing_time > 0
            assert result.posts[0].author == result.target_info.target_value
        
        # Verify processing log shows correct sequence
        assert len(processing_log) == 6  # 3 starts + 3 finishes
        assert processing_log[0].startswith("Started:")
        assert processing_log[-1].startswith("Finished:")


# Performance and edge case tests
class TestMultiTargetPerformance:
    """Performance and edge case tests for multi-target processing."""
    
    @pytest.mark.asyncio
    async def test_large_target_list_performance(self):
        """Test processing a large number of targets efficiently."""
        batch_config = BatchProcessingConfig(
            max_concurrent=5,
            rate_limit_delay=0.01,
            timeout_per_target=1.0
        )
        scraping_config = ScrapingConfig(post_limit=1, sleep_interval=0.01)
        
        processor = BatchTargetProcessor(batch_config, scraping_config)
        
        # Create 20 targets
        targets = [
            TargetInfo(
                target_type=TargetType.USER,
                target_value=f"user{i:02d}",
                original_input=f"u/user{i:02d}",
                metadata={}
            ) for i in range(1, 21)
        ]
        
        # Mock fast processing
        async def fast_process_target(target_info):
            await asyncio.sleep(0.05)  # 50ms processing time
            return TargetProcessingResult(
                target_info=target_info,
                posts=[PostMetadata(
                    id=f"post_{target_info.target_value}",
                    title=f"Post by {target_info.target_value}",
                    url=f"https://reddit.com/{target_info.target_value}",
                    subreddit="test",
                    author=target_info.target_value
                )],
                success=True,
                processing_time=0.05
            )
        
        for handler in processor.registry.handlers:
            if isinstance(handler, UserTargetHandler):
                handler.process_target = fast_process_target
        
        start_time = time.time()
        results = await processor.process_targets(targets)
        end_time = time.time()
        
        # Verify results
        assert len(results) == 20
        assert all(result.success for result in results)
        
        # Verify concurrent processing efficiency
        total_time = end_time - start_time
        # With 5 concurrent workers, 20 targets should complete in ~4 batches
        # Each batch takes ~0.05s + overhead, so should be well under 1 second
        assert total_time < 1.0
        
        # Verify each result has correct data
        for i, result in enumerate(results):
            expected_username = f"user{i+1:02d}"
            assert result.target_info.target_value == expected_username
            assert result.posts[0].author == expected_username