"""
Integration tests for the filtering system.

This module tests the complete filtering pipeline including FilterStage integration.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock
from redditdl.pipeline.stages.filter import FilterStage
from redditdl.core.pipeline.interfaces import PipelineContext, PipelineResult
from redditdl.core.events.emitter import EventEmitter
from redditdl.scrapers import PostMetadata


class TestFilterIntegration:
    """Test complete filter system integration."""
    
    def create_sample_posts(self):
        """Create sample posts for testing."""
        base_time = datetime.now().timestamp()
        
        return [
            PostMetadata(
                id="post1",
                title="Python Programming Tutorial",
                url="https://github.com/python/cpython",
                author="pythondev",
                subreddit="programming",
                created_utc=base_time - 86400,  # 1 day ago
                score=25,
                is_nsfw=False,
                selftext="Learn Python programming with this tutorial"
            ),
            PostMetadata(
                id="post2", 
                title="JavaScript Basics",
                url="https://imgur.com/gallery/js-tutorial",
                author="jsdev",
                subreddit="webdev",
                created_utc=base_time - 172800,  # 2 days ago
                score=5,
                is_nsfw=False,
                selftext="Introduction to JavaScript programming"
            ),
            PostMetadata(
                id="post3",
                title="NSFW Adult Content",
                url="https://example.com/adult",
                author="adultuser",
                subreddit="nsfw",
                created_utc=base_time - 259200,  # 3 days ago
                score=50,
                is_nsfw=True,
                selftext="Adult content warning"
            ),
            PostMetadata(
                id="post4",
                title="Spam Advertisement Buy Now!",
                url="https://spam.com/buy-now",
                author="spammer",
                subreddit="deals",
                created_utc=base_time - 345600,  # 4 days ago
                score=1,
                is_nsfw=False,
                selftext="Buy our spam product now! Advertisement special offer!"
            ),
            PostMetadata(
                id="post5",
                title="High Quality Programming Video",
                url="https://youtube.com/watch?v=abc123",
                author="educator",
                subreddit="programming",
                created_utc=base_time - 86400,  # 1 day ago
                score=150,
                is_nsfw=False,
                selftext="Comprehensive programming tutorial video"
            )
        ]
    
    def create_mock_context(self, config: dict, posts: list) -> PipelineContext:
        """Create mock pipeline context."""
        context = Mock(spec=PipelineContext)
        context.posts = posts
        context.config = config
        context.session = Mock()
        context.event_emitter = Mock(spec=EventEmitter)
        
        # Mock config access methods
        context.get_config = Mock(side_effect=lambda key, default=None: config.get(key, default))
        context.set_metadata = Mock()
        context.get_metadata = Mock(return_value=None)
        
        return context
    
    @pytest.mark.asyncio
    async def test_filter_stage_basic_filtering(self):
        """Test FilterStage with basic score filtering."""
        posts = self.create_sample_posts()
        config = {
            'min_score': 10  # Should filter out posts with score < 10
        }
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.get_data("posts_before_filter") == 5
        assert result.get_data("posts_after_filter") == 3  # posts 1, 3, 5 pass
        assert result.get_data("posts_filtered_out") == 2
        assert len(context.posts) == 3
        
        # Verify correct posts passed
        passing_ids = [post.id for post in context.posts]
        assert "post1" in passing_ids  # score 25
        assert "post3" in passing_ids  # score 50
        assert "post5" in passing_ids  # score 150
        assert "post2" not in passing_ids  # score 5
        assert "post4" not in passing_ids  # score 1
    
    @pytest.mark.asyncio
    async def test_filter_stage_keyword_filtering(self):
        """Test FilterStage with keyword filtering."""
        posts = self.create_sample_posts()
        config = {
            'keywords_include': ['programming', 'tutorial'],
            'keywords_exclude': ['spam', 'advertisement']
        }
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.get_data("posts_after_filter") == 2  # posts 1, 5 should pass
        
        passing_ids = [post.id for post in context.posts]
        assert "post1" in passing_ids  # Has "programming" and "tutorial"
        assert "post5" in passing_ids  # Has "programming" and "tutorial"
        assert "post2" not in passing_ids  # Missing required keywords
        assert "post3" not in passing_ids  # Missing required keywords
        assert "post4" not in passing_ids  # Contains excluded keywords
    
    @pytest.mark.asyncio
    async def test_filter_stage_nsfw_filtering(self):
        """Test FilterStage with NSFW filtering."""
        posts = self.create_sample_posts()
        config = {
            'nsfw_mode': 'exclude'
        }
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.get_data("posts_after_filter") == 4  # All except post3
        
        passing_ids = [post.id for post in context.posts]
        assert "post3" not in passing_ids  # NSFW post excluded
        assert len(passing_ids) == 4
    
    @pytest.mark.asyncio
    async def test_filter_stage_combined_filters_and(self):
        """Test FilterStage with multiple filters using AND composition."""
        posts = self.create_sample_posts()
        config = {
            'min_score': 10,
            'keywords_include': ['programming'],
            'nsfw_mode': 'exclude',
            'filter_composition': 'and'
        }
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.get_data("filter_composition") == "and"
        
        # Only posts that pass ALL filters
        passing_ids = [post.id for post in context.posts]
        assert "post1" in passing_ids  # score 25, has "programming", not NSFW
        assert "post5" in passing_ids  # score 150, has "programming", not NSFW
        assert len(passing_ids) == 2
    
    @pytest.mark.asyncio
    async def test_filter_stage_combined_filters_or(self):
        """Test FilterStage with multiple filters using OR composition."""
        posts = self.create_sample_posts()
        config = {
            'min_score': 100,  # High threshold - only post5
            'keywords_include': ['adult'],  # Only post3
            'filter_composition': 'or'
        }
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.get_data("filter_composition") == "or"
        
        # Posts that pass ANY filter
        passing_ids = [post.id for post in context.posts]
        assert "post3" in passing_ids  # Has "adult" keyword
        assert "post5" in passing_ids  # Score 150 >= 100
        assert len(passing_ids) == 2
    
    @pytest.mark.asyncio
    async def test_filter_stage_no_filters(self):
        """Test FilterStage with no filters configured."""
        posts = self.create_sample_posts()
        config = {}
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.get_data("posts_before_filter") == 5
        assert result.get_data("posts_after_filter") == 5
        assert result.get_data("posts_filtered_out") == 0
        assert len(context.posts) == 5  # All posts pass through
    
    @pytest.mark.asyncio
    async def test_filter_stage_empty_posts(self):
        """Test FilterStage with empty post list."""
        posts = []
        config = {'min_score': 10}
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert len(result.warnings) == 1
        assert "No posts to filter" in result.warnings[0]
    
    @pytest.mark.asyncio
    async def test_filter_stage_performance_metrics(self):
        """Test FilterStage performance metrics collection."""
        posts = self.create_sample_posts()
        config = {'min_score': 10}
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        assert result.success is True
        assert result.execution_time > 0
        assert result.get_data("total_filter_time") >= 0
        assert result.get_data("avg_filter_time") >= 0
        assert result.get_data("filters_applied") == 1
        
        # Check detailed filter results
        filter_results = result.get_data("filter_results", [])
        assert len(filter_results) == 5  # One result per post
        for filter_result in filter_results:
            assert "post_id" in filter_result
            assert "passed" in filter_result
            assert "reason" in filter_result
            assert "execution_time" in filter_result
    
    @pytest.mark.asyncio
    async def test_filter_stage_error_handling(self):
        """Test FilterStage error handling for problematic posts."""
        posts = self.create_sample_posts()
        
        # Add a problematic post that might cause errors
        problematic_post = PostMetadata(
            id="problematic",
            title=None,  # None title might cause issues
            url="invalid-url",
            author="",
            subreddit="",
            created_utc=None,  # None timestamp
            score=None,  # None score
            is_nsfw=None,
            selftext=None
        )
        posts.append(problematic_post)
        
        config = {'min_score': 10}
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        result = await filter_stage.process(context)
        
        # Should handle errors gracefully
        assert result.success is True
        assert len(context.posts) >= 3  # At least the good posts should pass
    
    @pytest.mark.asyncio
    async def test_filter_stage_context_metadata(self):
        """Test FilterStage metadata storage in context."""
        posts = self.create_sample_posts()
        config = {'min_score': 10}
        context = self.create_mock_context(config, posts)
        
        filter_stage = FilterStage()
        
        # Test pre-processing
        await filter_stage.pre_process(context)
        
        # Process
        result = await filter_stage.process(context)
        
        # Test post-processing
        await filter_stage.post_process(context, result)
        
        # Verify metadata was stored in context
        context.set_metadata.assert_called()
        
        # Check that filtering metadata was set
        metadata_calls = [call.args for call in context.set_metadata.call_args_list]
        metadata_keys = [call[0] for call in metadata_calls]
        
        expected_keys = [
            "filtering_completed",
            "posts_before_filter", 
            "posts_after_filter",
            "posts_filtered_out",
            "filters_applied",
            "filter_composition",
            "total_filter_time",
            "avg_filter_time",
            "filter_results_summary"
        ]
        
        for key in expected_keys:
            assert key in metadata_keys
    
    @pytest.mark.asyncio
    async def test_filter_stage_validation(self):
        """Test FilterStage configuration validation."""
        # Valid configuration
        config = {
            'min_score': 10,
            'max_score': 100,
            'filter_composition': 'and'
        }
        filter_stage = FilterStage(config)
        errors = filter_stage.validate_config()
        assert errors == []
        
        # Invalid configuration
        config = {
            'min_score': 100,
            'max_score': 10,  # max < min
            'filter_composition': 'invalid'
        }
        filter_stage = FilterStage(config)
        errors = filter_stage.validate_config()
        assert len(errors) >= 1  # Should have validation errors


if __name__ == "__main__":
    pytest.main([__file__])