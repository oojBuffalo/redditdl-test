"""
Tests for DateFilter functionality.

This module tests date-based filtering with flexible date parsing.
"""

import pytest
from datetime import datetime, timezone, timedelta
from redditdl.filters.date import DateFilter
from redditdl.scrapers import PostMetadata


class TestDateFilter:
    """Test DateFilter functionality."""
    
    def create_post_with_date(self, created_utc: float) -> PostMetadata:
        """Create a test post with specified creation date."""
        return PostMetadata(
            id=f"test_{int(created_utc)}",
            title="Test Post",
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            created_utc=created_utc,
            score=10,
            is_nsfw=False,
            selftext="This is a test post"
        )
    
    def test_after_date_filter_pass(self):
        """Test after date filter with passing post."""
        # Create a post from yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        post = self.create_post_with_date(yesterday.timestamp())
        
        # Filter for posts after 2 days ago
        two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
        filter_obj = DateFilter(date_after=two_days_ago)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "after" in result.reason.lower()
        assert result.metadata["post_date"] is not None
        assert result.metadata["date_after"] is not None
    
    def test_after_date_filter_fail(self):
        """Test after date filter with failing post."""
        # Create a post from 3 days ago
        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        post = self.create_post_with_date(three_days_ago.timestamp())
        
        # Filter for posts after yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        filter_obj = DateFilter(date_after=yesterday)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "before required after date" in result.reason
    
    def test_before_date_filter_pass(self):
        """Test before date filter with passing post."""
        # Create a post from 2 days ago
        two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
        post = self.create_post_with_date(two_days_ago.timestamp())
        
        # Filter for posts before yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        filter_obj = DateFilter(date_before=yesterday)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "before" in result.reason.lower()
        assert result.metadata["post_date"] is not None
        assert result.metadata["date_before"] is not None
    
    def test_before_date_filter_fail(self):
        """Test before date filter with failing post."""
        # Create a post from yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        post = self.create_post_with_date(yesterday.timestamp())
        
        # Filter for posts before 2 days ago
        two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
        filter_obj = DateFilter(date_before=two_days_ago)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "after required before date" in result.reason
    
    def test_date_range_filter_pass(self):
        """Test date range filter with passing post."""
        # Create a post from yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        post = self.create_post_with_date(yesterday.timestamp())
        
        # Filter for posts in range [2 days ago, today]
        two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
        today = datetime.now(timezone.utc)
        filter_obj = DateFilter(date_after=two_days_ago, date_before=today)
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "within date range" in result.reason
    
    def test_date_range_filter_fail_early(self):
        """Test date range filter with post before range."""
        # Create a post from 5 days ago
        five_days_ago = datetime.now(timezone.utc) - timedelta(days=5)
        post = self.create_post_with_date(five_days_ago.timestamp())
        
        # Filter for posts in range [2 days ago, today]
        two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
        today = datetime.now(timezone.utc)
        filter_obj = DateFilter(date_after=two_days_ago, date_before=today)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "before required after date" in result.reason
    
    def test_date_range_filter_fail_late(self):
        """Test date range filter with post after range."""
        # Create a post from the future (1 day ahead)
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
        post = self.create_post_with_date(tomorrow.timestamp())
        
        # Filter for posts in range [2 days ago, yesterday]
        two_days_ago = datetime.now(timezone.utc) - timedelta(days=2)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        filter_obj = DateFilter(date_after=two_days_ago, date_before=yesterday)
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "after required before date" in result.reason
    
    def test_string_date_parsing(self):
        """Test parsing dates from string formats."""
        # Create a post from yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        post = self.create_post_with_date(yesterday.timestamp())
        
        # Test ISO format string
        two_days_ago_str = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        filter_obj = DateFilter(date_after=two_days_ago_str)
        
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_relative_date_parsing(self):
        """Test parsing relative date strings."""
        # Create a post from yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        post = self.create_post_with_date(yesterday.timestamp())
        
        # Test relative date string
        filter_obj = DateFilter(date_after="2 days ago")
        
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_no_date_constraints(self):
        """Test filter with no date constraints."""
        post = self.create_post_with_date(datetime.now().timestamp())
        filter_obj = DateFilter()
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "No date constraints" in result.reason
    
    def test_missing_created_utc(self):
        """Test filter with post missing created_utc."""
        # Create post without created_utc
        post = PostMetadata(
            id="test_no_date",
            title="Test Post",
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            score=10,
            is_nsfw=False,
            selftext="This is a test post"
        )
        
        filter_obj = DateFilter(date_after=datetime.now(timezone.utc) - timedelta(days=1))
        result = filter_obj.apply(post)
        
        # Should fail because missing date defaults to epoch (very old)
        assert result.passed is False
        assert "before required after date" in result.reason
    
    def test_invalid_date_string(self):
        """Test filter with invalid date string."""
        post = self.create_post_with_date(datetime.now().timestamp())
        
        # This should raise an exception during filter creation
        with pytest.raises(Exception):
            DateFilter(date_after="not a valid date")
    
    def test_edge_cases(self):
        """Test edge cases for date filtering."""
        now = datetime.now(timezone.utc)
        
        # Test with post exactly at boundary
        post = self.create_post_with_date(now.timestamp())
        filter_obj = DateFilter(date_after=now)
        result = filter_obj.apply(post)
        assert result.passed is True  # Equal should pass
        
        filter_obj = DateFilter(date_before=now)
        result = filter_obj.apply(post)
        assert result.passed is True  # Equal should pass
        
        # Test with timezone-naive datetime
        naive_date = datetime.now()  # No timezone
        filter_obj = DateFilter(date_after=naive_date)
        result = filter_obj.apply(post)
        # Should work - filter should handle timezone conversion
        assert result.passed is not None
    
    def test_filter_properties(self):
        """Test filter properties and metadata."""
        after_date = datetime.now(timezone.utc) - timedelta(days=1)
        before_date = datetime.now(timezone.utc) + timedelta(days=1)
        filter_obj = DateFilter(date_after=after_date, date_before=before_date)
        
        assert filter_obj.name == "date"
        assert "Date Filter" in filter_obj.description
        assert "after" in filter_obj.description.lower()
        assert "before" in filter_obj.description.lower()
    
    def test_filter_validation(self):
        """Test filter configuration validation."""
        now = datetime.now(timezone.utc)
        
        # Valid configuration
        filter_obj = DateFilter(
            date_after=now - timedelta(days=1),
            date_before=now + timedelta(days=1)
        )
        errors = filter_obj.validate_config()
        assert errors == []
        
        # Invalid configuration - after > before
        filter_obj = DateFilter(
            date_after=now + timedelta(days=1),
            date_before=now - timedelta(days=1)
        )
        errors = filter_obj.validate_config()
        assert len(errors) == 1
        assert "date_after cannot be after date_before" in errors[0]
        
        # Valid single constraint configurations
        filter_obj = DateFilter(date_after=now)
        assert filter_obj.validate_config() == []
        
        filter_obj = DateFilter(date_before=now)
        assert filter_obj.validate_config() == []
        
        # No constraints (valid)
        filter_obj = DateFilter()
        assert filter_obj.validate_config() == []


if __name__ == "__main__":
    pytest.main([__file__])