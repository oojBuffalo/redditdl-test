"""
Tests for ScoreFilter functionality.

This module tests score-based filtering with minimum and maximum thresholds.
"""

import pytest
from datetime import datetime
from redditdl.filters.score import ScoreFilter
from redditdl.scrapers import PostMetadata


class TestScoreFilter:
    """Test ScoreFilter functionality."""
    
    def create_post_with_score(self, score: int) -> PostMetadata:
        """Create a test post with specified score."""
        return PostMetadata(
            id=f"test_{score}",
            title="Test Post",
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            created_utc=datetime.now().timestamp(),
            score=score,
            is_nsfw=False,
            selftext="This is a test post"
        )
    
    def test_min_score_filter_pass(self):
        """Test minimum score filter with passing post."""
        filter_obj = ScoreFilter(min_score=5)
        post = self.create_post_with_score(10)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "score 10 >= minimum 5" in result.reason
        assert result.metadata["post_score"] == 10
        assert result.metadata["min_score"] == 5
    
    def test_min_score_filter_fail(self):
        """Test minimum score filter with failing post."""
        filter_obj = ScoreFilter(min_score=5)
        post = self.create_post_with_score(3)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "score 3 < minimum 5" in result.reason
        assert result.metadata["post_score"] == 3
        assert result.metadata["min_score"] == 5
    
    def test_max_score_filter_pass(self):
        """Test maximum score filter with passing post."""
        filter_obj = ScoreFilter(max_score=100)
        post = self.create_post_with_score(50)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "score 50 <= maximum 100" in result.reason
        assert result.metadata["post_score"] == 50
        assert result.metadata["max_score"] == 100
    
    def test_max_score_filter_fail(self):
        """Test maximum score filter with failing post."""
        filter_obj = ScoreFilter(max_score=100)
        post = self.create_post_with_score(150)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "score 150 > maximum 100" in result.reason
        assert result.metadata["post_score"] == 150
        assert result.metadata["max_score"] == 100
    
    def test_score_range_filter_pass(self):
        """Test score range filter with passing post."""
        filter_obj = ScoreFilter(min_score=10, max_score=100)
        post = self.create_post_with_score(50)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "score 50 within range [10, 100]" in result.reason
        assert result.metadata["post_score"] == 50
        assert result.metadata["min_score"] == 10
        assert result.metadata["max_score"] == 100
    
    def test_score_range_filter_fail_low(self):
        """Test score range filter with post below minimum."""
        filter_obj = ScoreFilter(min_score=10, max_score=100)
        post = self.create_post_with_score(5)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "score 5 < minimum 10" in result.reason
    
    def test_score_range_filter_fail_high(self):
        """Test score range filter with post above maximum."""
        filter_obj = ScoreFilter(min_score=10, max_score=100)
        post = self.create_post_with_score(150)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "score 150 > maximum 100" in result.reason
    
    def test_no_score_filter(self):
        """Test filter with no score constraints (should always pass)."""
        filter_obj = ScoreFilter()
        post = self.create_post_with_score(42)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "No score constraints" in result.reason
    
    def test_missing_score_attribute(self):
        """Test filter with post missing score attribute."""
        filter_obj = ScoreFilter(min_score=5)
        
        # Create post without score attribute
        post = PostMetadata(
            id="test_no_score",
            title="Test Post",
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            created_utc=datetime.now().timestamp(),
            is_nsfw=False,
            selftext="This is a test post"
        )
        # Remove score attribute if it exists
        if hasattr(post, 'score'):
            delattr(post, 'score')
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "score 0 < minimum 5" in result.reason
        assert result.metadata["post_score"] == 0  # Default fallback
    
    def test_edge_cases(self):
        """Test edge cases for score filtering."""
        # Test with score exactly at minimum
        filter_obj = ScoreFilter(min_score=10)
        post = self.create_post_with_score(10)
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Test with score exactly at maximum
        filter_obj = ScoreFilter(max_score=100)
        post = self.create_post_with_score(100)
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Test with negative scores
        filter_obj = ScoreFilter(min_score=-5)
        post = self.create_post_with_score(-3)
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Test with zero score
        filter_obj = ScoreFilter(min_score=0)
        post = self.create_post_with_score(0)
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_filter_properties(self):
        """Test filter properties and metadata."""
        filter_obj = ScoreFilter(min_score=5, max_score=100)
        
        assert filter_obj.name == "score"
        assert "Score Filter" in filter_obj.description
        assert "min_score=5" in filter_obj.description
        assert "max_score=100" in filter_obj.description
    
    def test_filter_validation(self):
        """Test filter configuration validation."""
        # Valid configuration
        filter_obj = ScoreFilter(min_score=5, max_score=100)
        errors = filter_obj.validate_config()
        assert errors == []
        
        # Invalid configuration - min > max
        filter_obj = ScoreFilter(min_score=100, max_score=5)
        errors = filter_obj.validate_config()
        assert len(errors) == 1
        assert "min_score cannot be greater than max_score" in errors[0]
        
        # Valid single constraint configurations
        filter_obj = ScoreFilter(min_score=5)
        assert filter_obj.validate_config() == []
        
        filter_obj = ScoreFilter(max_score=100)
        assert filter_obj.validate_config() == []
        
        # No constraints (valid)
        filter_obj = ScoreFilter()
        assert filter_obj.validate_config() == []


if __name__ == "__main__":
    pytest.main([__file__])