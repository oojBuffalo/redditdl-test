"""
Tests for enhanced acquisition pipeline stage.

Tests the AcquisitionStage implementation with target resolution,
multi-target support, event emission, and scraper integration.
"""

import pytest
import asyncio
import sys
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock, MagicMock

# Add the project root to the Python path
# project_root = Path(__file__).parent.parent.parent
# sys.path.insert(0, str(project_root))

from redditdl.pipeline.stages.acquisition import AcquisitionStage
from redditdl.core.pipeline.interfaces import PipelineContext, PipelineResult
from redditdl.core.events.types import PostDiscoveredEvent
from redditdl.targets.resolver import TargetInfo, TargetType
from redditdl.targets.base_scraper import ScrapingConfig, ScrapingError, AuthenticationError, TargetNotFoundError
from redditdl.scrapers import PostMetadata


class TestAcquisitionStage:
    """Test AcquisitionStage implementation."""
    
    @pytest.fixture
    def acquisition_stage(self):
        """Create AcquisitionStage instance for testing."""
        return AcquisitionStage()
    
    @pytest.fixture
    def context(self):
        """Create PipelineContext for testing."""
        context = PipelineContext()
        context.events = Mock()
        context.events.emit_async = AsyncMock()
        context.session_id = "test_session_123"
        return context
    
    @pytest.fixture
    def sample_posts(self):
        """Create sample PostMetadata objects."""
        return [
            PostMetadata({
                'id': 'post_1',
                'title': 'Test Post 1',
                'author': 'testuser',
                'subreddit': 'test',
                'url': 'https://example.com/1',
                'created_utc': 1640995200
            }),
            PostMetadata({
                'id': 'post_2',
                'title': 'Test Post 2',
                'author': 'testuser',
                'subreddit': 'test',
                'url': 'https://example.com/2',
                'created_utc': 1640995300
            })
        ]
    
    def test_stage_initialization(self, acquisition_stage):
        """Test acquisition stage initialization."""
        assert acquisition_stage.name == "acquisition"
        assert acquisition_stage.target_resolver is not None
        assert acquisition_stage._scrapers == {}
    
    @pytest.mark.asyncio
    async def test_process_no_targets(self, acquisition_stage, context):
        """Test processing with no targets specified."""
        result = await acquisition_stage.process(context)
        
        assert result.success is False
        assert len(result.errors) > 0
        assert "No targets specified" in result.errors[0]
    
    @pytest.mark.asyncio
    async def test_process_single_target_user(self, acquisition_stage, context, sample_posts):
        """Test processing single user target."""
        # Setup context with target
        context.set_config("target_user", "test_user")
        context.set_config("client_id", "test_id")
        context.set_config("client_secret", "test_secret")
        
        # Mock scraper factory and scraper
        mock_scraper = Mock()
        mock_scraper.scraper_type = "praw"
        mock_scraper.fetch_posts.return_value = sample_posts
        
        with patch('pipeline.stages.acquisition.ScraperFactory.create_scraper', return_value=mock_scraper):
            result = await acquisition_stage.process(context)
        
        assert result.success is True
        assert result.processed_count == 2
        assert result.get_data("total_posts_acquired") == 2
        assert result.get_data("targets_processed") == 1
        assert len(context.posts) == 2
    
    @pytest.mark.asyncio
    async def test_process_multiple_targets(self, acquisition_stage, context, sample_posts):
        """Test processing multiple targets."""
        # Setup context with multiple targets
        context.set_config("targets", ["test_user", "r/testsubreddit"])
        context.set_config("client_id", "test_id")
        context.set_config("client_secret", "test_secret")
        
        # Mock scraper factory and scraper
        mock_scraper = Mock()
        mock_scraper.scraper_type = "praw"
        mock_scraper.fetch_posts.return_value = sample_posts
        
        with patch('pipeline.stages.acquisition.ScraperFactory.create_scraper', return_value=mock_scraper):
            result = await acquisition_stage.process(context)
        
        assert result.success is True
        assert result.processed_count == 4  # 2 posts per target
        assert result.get_data("total_posts_acquired") == 4
        assert result.get_data("targets_processed") == 2
        assert len(context.posts) == 4
    
    @pytest.mark.asyncio
    async def test_process_target_not_found(self, acquisition_stage, context):
        """Test processing with target not found error."""
        context.set_config("target_user", "nonexistent_user")
        
        mock_scraper = Mock()
        mock_scraper.fetch_posts.side_effect = TargetNotFoundError("User not found")
        
        with patch('pipeline.stages.acquisition.ScraperFactory.create_scraper', return_value=mock_scraper):
            result = await acquisition_stage.process(context)
        
        assert result.success is False
        assert len(result.errors) > 0
        assert "Target not found" in result.errors[0]
    
    @pytest.mark.asyncio
    async def test_process_authentication_error(self, acquisition_stage, context):
        """Test processing with authentication error."""
        context.set_config("targets", ["saved"])  # Requires auth
        # No API credentials provided
        
        result = await acquisition_stage.process(context)
        
        assert result.success is False
        assert len(result.errors) > 0
        assert "Reddit API authentication" in result.errors[0]
    
    @pytest.mark.asyncio
    async def test_process_scraping_error(self, acquisition_stage, context):
        """Test processing with general scraping error."""
        context.set_config("target_user", "test_user")
        
        mock_scraper = Mock()
        mock_scraper.fetch_posts.side_effect = ScrapingError("Network error")
        
        with patch('pipeline.stages.acquisition.ScraperFactory.create_scraper', return_value=mock_scraper):
            result = await acquisition_stage.process(context)
        
        assert result.success is False
        assert len(result.errors) > 0
        assert "Scraping error" in result.errors[0]
    
    @pytest.mark.asyncio
    async def test_event_emission(self, acquisition_stage, context, sample_posts):
        """Test that PostDiscoveredEvent is emitted."""
        context.set_config("target_user", "test_user")  # Use underscore to ensure user resolution
        
        mock_scraper = Mock()
        mock_scraper.scraper_type = "yars"
        mock_scraper.fetch_posts.return_value = sample_posts
        
        with patch('pipeline.stages.acquisition.ScraperFactory.create_scraper', return_value=mock_scraper):
            result = await acquisition_stage.process(context)
        
        assert result.success is True
        
        # Verify event was emitted
        context.events.emit_async.assert_called_once()
        emitted_event = context.events.emit_async.call_args[0][0]
        assert isinstance(emitted_event, PostDiscoveredEvent)
        assert emitted_event.post_count == 2
        assert emitted_event.source == "test_user"
        assert emitted_event.source_type == "user"
        assert len(emitted_event.posts_preview) == 2
    
    @pytest.mark.asyncio
    async def test_no_event_emission_without_emitter(self, acquisition_stage, sample_posts):
        """Test that no error occurs when no event emitter is available."""
        context = PipelineContext()  # No events emitter
        context.set_config("target_user", "test_user")
        
        mock_scraper = Mock()
        mock_scraper.fetch_posts.return_value = sample_posts
        
        with patch('pipeline.stages.acquisition.ScraperFactory.create_scraper', return_value=mock_scraper):
            result = await acquisition_stage.process(context)
        
        assert result.success is True
        # Should not raise any exceptions
    
    def test_build_scraping_config(self, acquisition_stage, context):
        """Test building scraping configuration from context."""
        context.set_config("post_limit", 50)
        context.set_config("sleep_interval", 2.0)
        context.set_config("client_id", "test_id")
        context.set_config("client_secret", "test_secret")
        context.set_config("user_agent", "TestAgent/1.0")
        
        config = acquisition_stage._build_scraping_config(context)
        
        assert isinstance(config, ScrapingConfig)
        assert config.post_limit == 50
        assert config.sleep_interval == 2.0
        assert config.client_id == "test_id"
        assert config.client_secret == "test_secret"
        assert config.user_agent == "TestAgent/1.0"
    
    def test_get_targets_from_targets_list(self, acquisition_stage, context):
        """Test getting targets from targets list."""
        context.set_config("targets", ["user1", "r/sub1", "saved"])
        
        targets = acquisition_stage._get_targets(context)
        
        assert targets == ["user1", "r/sub1", "saved"]
    
    def test_get_targets_from_single_string(self, acquisition_stage, context):
        """Test getting targets from single string target."""
        context.set_config("targets", "test_user")
        
        targets = acquisition_stage._get_targets(context)
        
        assert targets == ["test_user"]
    
    def test_get_targets_from_legacy_target_user(self, acquisition_stage, context):
        """Test getting targets from legacy target_user config."""
        context.set_config("target_user", "test_user")
        
        targets = acquisition_stage._get_targets(context)
        
        assert targets == ["test_user"]
    
    def test_get_targets_priority(self, acquisition_stage, context):
        """Test that targets config takes priority over target_user."""
        context.set_config("targets", ["user1", "user2"])
        context.set_config("target_user", "legacy_user")
        
        targets = acquisition_stage._get_targets(context)
        
        assert targets == ["user1", "user2"]
    
    def test_validate_config_no_targets(self, acquisition_stage):
        """Test validation with no targets specified."""
        errors = acquisition_stage.validate_config()
        
        assert len(errors) > 0
        assert "At least one target must be specified" in errors[0]
    
    def test_validate_config_valid_targets(self, acquisition_stage):
        """Test validation with valid targets."""
        acquisition_stage.set_config("targets", ["test_user", "r/testsubreddit"])
        
        errors = acquisition_stage.validate_config()
        
        # Should only have basic validation errors if any
        target_errors = [e for e in errors if "target" in e.lower()]
        assert len(target_errors) == 0
    
    def test_validate_config_invalid_targets(self, acquisition_stage):
        """Test validation with invalid target formats."""
        acquisition_stage.set_config("targets", ["valid_user", "invalid_$%^"])
        
        errors = acquisition_stage.validate_config()
        
        invalid_target_errors = [e for e in errors if "invalid_$%^" in e]
        assert len(invalid_target_errors) > 0
    
    def test_validate_config_negative_values(self, acquisition_stage):
        """Test validation with negative configuration values."""
        acquisition_stage.set_config("target_user", "test_user")
        acquisition_stage.set_config("sleep_interval", -1.0)
        acquisition_stage.set_config("post_limit", -5)
        acquisition_stage.set_config("timeout", -10)
        acquisition_stage.set_config("retries", -2)
        
        errors = acquisition_stage.validate_config()
        
        assert "sleep_interval must be non-negative" in errors
        assert "post_limit must be positive" in errors
        assert "timeout must be positive" in errors
        assert "retries must be non-negative" in errors
    
    @pytest.mark.asyncio
    async def test_pre_process_logging(self, acquisition_stage, context):
        """Test pre-processing logs configuration properly."""
        context.set_config("targets", ["test_user"])
        context.set_config("post_limit", 10)
        context.set_config("client_id", "test_id")
        
        # Should not raise any exceptions
        await acquisition_stage.pre_process(context)
    
    @pytest.mark.asyncio
    async def test_post_process_success_metadata(self, acquisition_stage, context):
        """Test post-processing stores success metadata."""
        result = PipelineResult(stage_name="acquisition", success=True)
        result.set_data("total_posts_acquired", 5)
        result.set_data("targets_processed", 2)
        result.set_data("processed_targets", [{"target": "user1", "posts_count": 3}])
        
        await acquisition_stage.post_process(context, result)
        
        assert context.get_metadata("acquisition_completed") is True
        assert context.get_metadata("total_posts_acquired") == 5
        assert context.get_metadata("targets_processed") == 2
        assert len(context.get_metadata("processed_targets")) == 1
    
    @pytest.mark.asyncio
    async def test_post_process_failure_metadata(self, acquisition_stage, context):
        """Test post-processing stores failure metadata."""
        result = PipelineResult(stage_name="acquisition", success=False)
        
        await acquisition_stage.post_process(context, result)
        
        assert context.get_metadata("acquisition_completed") is False
    
    @pytest.mark.asyncio
    async def test_emit_post_discovered_event_preview(self, acquisition_stage, context, sample_posts):
        """Test event emission creates proper post preview."""
        target_info = TargetInfo(
            target_type=TargetType.USER,
            target_value="test_user",
            original_input="test_user"
        )
        
        await acquisition_stage._emit_post_discovered_event(context, target_info, sample_posts)
        
        context.events.emit_async.assert_called_once()
        event = context.events.emit_async.call_args[0][0]
        
        assert len(event.posts_preview) == 2
        assert event.posts_preview[0]['id'] == 'post_1'
        assert event.posts_preview[0]['title'] == 'Test Post 1'
        assert event.posts_preview[1]['id'] == 'post_2'
    
    @pytest.mark.asyncio
    async def test_emit_post_discovered_event_long_title_truncation(self, acquisition_stage, context):
        """Test event emission truncates long post titles."""
        long_title_post = PostMetadata({
            'id': 'long_post',
            'title': 'A' * 150,  # Very long title
            'author': 'testuser',
            'subreddit': 'test',
            'url': 'https://example.com',
            'created_utc': 1640995200
        })
        
        target_info = TargetInfo(
            target_type=TargetType.USER,
            target_value="test_user",
            original_input="test_user"
        )
        
        await acquisition_stage._emit_post_discovered_event(context, target_info, [long_title_post])
        
        event = context.events.emit_async.call_args[0][0]
        preview_title = event.posts_preview[0]['title']
        
        assert len(preview_title) <= 103  # 100 chars + "..."
        assert preview_title.endswith('...')
    
    def test_acquisition_stage_backward_compatibility(self, acquisition_stage, context):
        """Test that stage maintains backward compatibility with old config."""
        # Old style configuration
        context.set_config("target_user", "test_user")
        context.set_config("api_mode", True)
        context.set_config("client_id", "test_id")
        
        targets = acquisition_stage._get_targets(context)
        config = acquisition_stage._build_scraping_config(context)
        
        assert "test_user" in targets
        assert config.client_id == "test_id"