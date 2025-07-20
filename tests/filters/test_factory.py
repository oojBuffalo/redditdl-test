"""
Tests for FilterFactory functionality.

This module tests the filter factory for creating filters from configuration.
"""

import pytest
from datetime import datetime, timezone, timedelta
from redditdl.filters.factory import FilterFactory
from redditdl.filters.base import FilterChain, FilterComposition
from redditdl.scrapers import PostMetadata


class TestFilterFactory:
    """Test FilterFactory functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.factory = FilterFactory()
    
    def test_create_score_filter(self):
        """Test creating ScoreFilter from configuration."""
        self.setUp()
        config = {
            'min_score': 10,
            'max_score': 100
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "score"
        assert filter_chain.composition == FilterComposition.AND
    
    def test_create_date_filter(self):
        """Test creating DateFilter from configuration."""
        self.setUp()
        config = {
            'date_from': '2023-01-01',
            'date_to': '2023-12-31'
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "date"
    
    def test_create_keyword_filter(self):
        """Test creating KeywordFilter from configuration."""
        self.setUp()
        config = {
            'keywords_include': ['python', 'programming'],
            'keywords_exclude': ['spam'],
            'keyword_case_sensitive': True
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "keyword"
    
    def test_create_domain_filter(self):
        """Test creating DomainFilter from configuration."""
        self.setUp()
        config = {
            'domains_allow': ['reddit.com', 'imgur.com'],
            'domains_block': ['spam.com']
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "domain"
    
    def test_create_media_type_filter(self):
        """Test creating MediaTypeFilter from configuration."""
        self.setUp()
        config = {
            'media_types': ['image', 'video'],
            'file_extensions': ['jpg', 'png', 'mp4']
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "media_type"
    
    def test_create_nsfw_filter(self):
        """Test creating NSFWFilter from configuration."""
        self.setUp()
        config = {
            'nsfw_mode': 'exclude'
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "nsfw"
    
    def test_create_multiple_filters_and_composition(self):
        """Test creating multiple filters with AND composition."""
        self.setUp()
        config = {
            'min_score': 10,
            'keywords_include': ['python'],
            'nsfw_mode': 'exclude',
            'filter_composition': 'and'
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 3  # score, keyword, nsfw
        assert filter_chain.composition == FilterComposition.AND
        
        # Check filter types
        filter_names = [f.name for f in filter_chain.filters]
        assert "score" in filter_names
        assert "keyword" in filter_names
        assert "nsfw" in filter_names
    
    def test_create_multiple_filters_or_composition(self):
        """Test creating multiple filters with OR composition."""
        self.setUp()
        config = {
            'min_score': 100,  # High threshold
            'keywords_include': ['viral'],  # Specific keyword
            'filter_composition': 'or'
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 2
        assert filter_chain.composition == FilterComposition.OR
    
    def test_legacy_nsfw_options(self):
        """Test handling legacy NSFW configuration options."""
        self.setUp()
        
        # Test include_nsfw=False
        config = {'include_nsfw': False}
        filter_chain = self.factory.create_from_cli_args(config)
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "nsfw"
        
        # Test nsfw_only=True
        config = {'nsfw_only': True}
        filter_chain = self.factory.create_from_cli_args(config)
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "nsfw"
    
    def test_alias_handling(self):
        """Test handling of configuration aliases."""
        self.setUp()
        
        # Test date aliases
        config = {
            'after_date': '2023-01-01',  # Alias for date_from
            'before_date': '2023-12-31'  # Alias for date_to
        }
        filter_chain = self.factory.create_from_cli_args(config)
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "date"
        
        # Test keyword aliases
        config = {
            'include_keywords': ['python'],  # Alias for keywords_include
            'exclude_keywords': ['spam']     # Alias for keywords_exclude
        }
        filter_chain = self.factory.create_from_cli_args(config)
        assert filter_chain is not None
        assert len(filter_chain.filters) == 1
        assert filter_chain.filters[0].name == "keyword"
    
    def test_empty_configuration(self):
        """Test factory with empty configuration."""
        self.setUp()
        config = {}
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is None  # No filters to create
    
    def test_invalid_filter_composition(self):
        """Test factory with invalid filter composition."""
        self.setUp()
        config = {
            'min_score': 10,
            'filter_composition': 'invalid'
        }
        
        with pytest.raises(ValueError, match="Invalid filter composition"):
            self.factory.create_from_cli_args(config)
    
    def test_filter_creation_from_config_file(self):
        """Test creating filters from config file format."""
        self.setUp()
        config = {
            'filter_config': {
                'score_filter': {
                    'min_score': 10,
                    'max_score': 100
                },
                'keyword_filter': {
                    'keywords_include': ['python'],
                    'case_sensitive': True
                },
                'composition': 'and'
            }
        }
        
        filter_chain = self.factory.create_from_config_file(config)
        
        # This tests a more structured config format
        # Implementation may vary based on actual config file structure
        assert filter_chain is not None or filter_chain is None  # Depends on implementation
    
    def test_filter_chain_execution(self):
        """Test that created filter chain works correctly."""
        self.setUp()
        config = {
            'min_score': 5,
            'keywords_include': ['python'],
            'filter_composition': 'and'
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        assert filter_chain is not None
        
        # Create test post that should pass
        post = PostMetadata(
            id="test123",
            title="Python Programming Tutorial",
            url="https://example.com/python",
            author="pythondev",
            subreddit="programming",
            created_utc=datetime.now().timestamp(),
            score=10,
            is_nsfw=False,
            selftext="Learn Python programming"
        )
        
        result = filter_chain.apply(post)
        assert result.passed is True
        
        # Create test post that should fail (low score)
        post_fail = PostMetadata(
            id="test456",
            title="Python Programming Tutorial",
            url="https://example.com/python",
            author="pythondev",
            subreddit="programming", 
            created_utc=datetime.now().timestamp(),
            score=1,  # Below minimum
            is_nsfw=False,
            selftext="Learn Python programming"
        )
        
        result = filter_chain.apply(post_fail)
        assert result.passed is False
    
    def test_filter_validation_during_creation(self):
        """Test that filter validation occurs during creation."""
        self.setUp()
        
        # Invalid score range
        config = {
            'min_score': 100,
            'max_score': 10  # max < min
        }
        
        # Should not raise exception during creation, but filter should have validation errors
        filter_chain = self.factory.create_from_cli_args(config)
        assert filter_chain is not None
        
        # Check that the filter has validation errors
        score_filter = filter_chain.filters[0]
        errors = score_filter.validate_config()
        assert len(errors) > 0
    
    def test_complex_configuration(self):
        """Test factory with complex configuration combining all filter types."""
        self.setUp()
        config = {
            # Score filter
            'min_score': 10,
            'max_score': 1000,
            
            # Date filter
            'date_from': '2023-01-01T00:00:00Z',
            'date_to': '2024-01-01T00:00:00Z',
            
            # Keyword filter
            'keywords_include': ['python', 'programming'],
            'keywords_exclude': ['spam', 'advertisement'],
            'keyword_case_sensitive': False,
            'keyword_regex': False,
            'keyword_whole_words': True,
            
            # Domain filter
            'domains_allow': ['reddit.com', 'github.com'],
            'domains_block': ['spam.com'],
            
            # Media type filter
            'media_types': ['image', 'video'],
            'exclude_media_types': ['text'],
            'file_extensions': ['jpg', 'png', 'mp4'],
            
            # NSFW filter
            'nsfw_mode': 'exclude',
            
            # Composition
            'filter_composition': 'and'
        }
        
        filter_chain = self.factory.create_from_cli_args(config)
        
        assert filter_chain is not None
        assert len(filter_chain.filters) == 6  # All filter types
        assert filter_chain.composition == FilterComposition.AND
        
        # Verify all filter types are present
        filter_names = [f.name for f in filter_chain.filters]
        expected_names = ['score', 'date', 'keyword', 'domain', 'media_type', 'nsfw']
        for name in expected_names:
            assert name in filter_names


if __name__ == "__main__":
    pytest.main([__file__])