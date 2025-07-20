"""
Tests for base filter functionality.

This module tests the abstract Filter base class, FilterResult, FilterChain,
and FilterComposition functionality.
"""

import pytest
from datetime import datetime
from redditdl.filters.base import Filter, FilterResult, FilterChain, FilterComposition
from redditdl.scrapers import PostMetadata


class TestFilterResult:
    """Test FilterResult dataclass functionality."""
    
    def test_filter_result_creation(self):
        """Test basic FilterResult creation."""
        result = FilterResult(
            passed=True,
            reason="Post meets criteria",
            execution_time=0.001,
            metadata={"filter": "test"}
        )
        
        assert result.passed is True
        assert result.reason == "Post meets criteria"
        assert result.execution_time == 0.001
        assert result.metadata == {"filter": "test"}
    
    def test_filter_result_defaults(self):
        """Test FilterResult with default values."""
        result = FilterResult(passed=False, reason="Failed test")
        
        assert result.passed is False
        assert result.reason == "Failed test"
        assert result.execution_time == 0.0
        assert result.metadata == {}


class MockFilter(Filter):
    """Mock filter for testing purposes."""
    
    def __init__(self, name: str, should_pass: bool = True, execution_time: float = 0.001):
        self.name = name
        self.should_pass = should_pass
        self.execution_time = execution_time
        self.description = f"Mock filter {name}"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """Mock filter implementation."""
        import time
        time.sleep(self.execution_time)  # Simulate processing time
        
        return FilterResult(
            passed=self.should_pass,
            reason=f"Mock filter {self.name} result",
            execution_time=self.execution_time,
            metadata={"filter_name": self.name}
        )
    
    def validate_config(self) -> list:
        """Mock validation."""
        return []


class TestFilterChain:
    """Test FilterChain functionality."""
    
    def create_sample_post(self) -> PostMetadata:
        """Create a sample post for testing."""
        return PostMetadata(
            id="test123",
            title="Test Post",
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            created_utc=datetime.now().timestamp(),
            score=10,
            is_nsfw=False,
            selftext="This is a test post"
        )
    
    def test_filter_chain_and_composition(self):
        """Test FilterChain with AND composition."""
        # Create filters - all pass
        filter1 = MockFilter("filter1", should_pass=True)
        filter2 = MockFilter("filter2", should_pass=True)
        filter3 = MockFilter("filter3", should_pass=True)
        
        chain = FilterChain([filter1, filter2, filter3], FilterComposition.AND)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is True
        assert "All filters passed" in result.reason
        assert result.execution_time > 0
        assert len(result.metadata.get("filter_results", [])) == 3
    
    def test_filter_chain_and_composition_fail(self):
        """Test FilterChain with AND composition where one filter fails."""
        # Create filters - one fails
        filter1 = MockFilter("filter1", should_pass=True)
        filter2 = MockFilter("filter2", should_pass=False)  # This one fails
        filter3 = MockFilter("filter3", should_pass=True)
        
        chain = FilterChain([filter1, filter2, filter3], FilterComposition.AND)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is False
        assert "Failed filter: filter2" in result.reason
        assert len(result.metadata.get("filter_results", [])) == 2  # Early termination
    
    def test_filter_chain_or_composition(self):
        """Test FilterChain with OR composition."""
        # Create filters - one passes
        filter1 = MockFilter("filter1", should_pass=False)
        filter2 = MockFilter("filter2", should_pass=True)  # This one passes
        filter3 = MockFilter("filter3", should_pass=False)
        
        chain = FilterChain([filter1, filter2, filter3], FilterComposition.OR)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is True
        assert "Passed filter: filter2" in result.reason
        assert len(result.metadata.get("filter_results", [])) == 2  # Early termination on success
    
    def test_filter_chain_or_composition_all_fail(self):
        """Test FilterChain with OR composition where all filters fail."""
        # Create filters - all fail
        filter1 = MockFilter("filter1", should_pass=False)
        filter2 = MockFilter("filter2", should_pass=False)
        filter3 = MockFilter("filter3", should_pass=False)
        
        chain = FilterChain([filter1, filter2, filter3], FilterComposition.OR)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is False
        assert "All filters failed" in result.reason
        assert len(result.metadata.get("filter_results", [])) == 3  # All executed
    
    def test_empty_filter_chain(self):
        """Test empty FilterChain."""
        chain = FilterChain([], FilterComposition.AND)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is True
        assert result.reason == "No filters to apply"
        assert result.metadata.get("filter_results") == []
    
    def test_single_filter_chain(self):
        """Test FilterChain with single filter."""
        filter1 = MockFilter("single", should_pass=True)
        chain = FilterChain([filter1], FilterComposition.AND)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is True
        assert "All filters passed" in result.reason
        assert len(result.metadata.get("filter_results", [])) == 1
    
    def test_filter_chain_execution_time_tracking(self):
        """Test that FilterChain properly tracks execution time."""
        # Create filters with known execution times
        filter1 = MockFilter("filter1", should_pass=True, execution_time=0.001)
        filter2 = MockFilter("filter2", should_pass=True, execution_time=0.002)
        
        chain = FilterChain([filter1, filter2], FilterComposition.AND)
        post = self.create_sample_post()
        
        result = chain.apply(post)
        
        assert result.passed is True
        # Total execution time should be at least the sum of individual times
        assert result.execution_time >= 0.003
        
        # Check individual filter results
        filter_results = result.metadata.get("filter_results", [])
        assert len(filter_results) == 2
        assert filter_results[0]["execution_time"] >= 0.001
        assert filter_results[1]["execution_time"] >= 0.002


class TestFilterComposition:
    """Test FilterComposition enum."""
    
    def test_filter_composition_values(self):
        """Test FilterComposition enum values."""
        assert FilterComposition.AND.value == "and"
        assert FilterComposition.OR.value == "or"
    
    def test_filter_composition_from_string(self):
        """Test creating FilterComposition from string."""
        assert FilterComposition("and") == FilterComposition.AND
        assert FilterComposition("or") == FilterComposition.OR
        
        # Test case insensitivity
        assert FilterComposition("AND") == FilterComposition.AND
        assert FilterComposition("OR") == FilterComposition.OR


class TestFilterValidation:
    """Test filter validation methods."""
    
    def test_mock_filter_validation(self):
        """Test that MockFilter validation works."""
        filter1 = MockFilter("test")
        errors = filter1.validate_config()
        assert errors == []  # MockFilter has no validation errors


if __name__ == "__main__":
    pytest.main([__file__])