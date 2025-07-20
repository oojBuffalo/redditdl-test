"""
Tests for NSFW Filter

Tests the NSFW (Not Safe For Work) filter functionality including
content filtering modes and age restriction handling.
"""

import pytest
from unittest.mock import Mock

from redditdl.filters.nsfw import NSFWFilter
from redditdl.scrapers import PostMetadata


class TestNSFWFilter:
    """Test suite for NSFWFilter."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.filter = NSFWFilter()
    
    def test_filter_initialization_default(self):
        """Test filter initialization with default settings."""
        assert isinstance(self.filter, NSFWFilter)
        assert self.filter.mode == 'exclude'  # Default mode
        assert self.filter.include_nsfw is False
        assert self.filter.nsfw_only is False
    
    def test_filter_initialization_include_mode(self):
        """Test filter initialization in include mode."""
        filter_instance = NSFWFilter(mode='include')
        
        assert filter_instance.mode == 'include'
        assert filter_instance.include_nsfw is True
        assert filter_instance.nsfw_only is False
    
    def test_filter_initialization_only_mode(self):
        """Test filter initialization in NSFW-only mode."""
        filter_instance = NSFWFilter(mode='only')
        
        assert filter_instance.mode == 'only'
        assert filter_instance.include_nsfw is True
        assert filter_instance.nsfw_only is True
    
    def test_filter_initialization_exclude_mode(self):
        """Test filter initialization in exclude mode."""
        filter_instance = NSFWFilter(mode='exclude')
        
        assert filter_instance.mode == 'exclude'
        assert filter_instance.include_nsfw is False
        assert filter_instance.nsfw_only is False
    
    def test_filter_initialization_legacy_parameters(self):
        """Test filter initialization with legacy boolean parameters."""
        # Legacy include_nsfw parameter
        filter_instance = NSFWFilter(include_nsfw=True)
        assert filter_instance.include_nsfw is True
        assert filter_instance.mode == 'include'
        
        # Legacy nsfw_only parameter
        filter_instance = NSFWFilter(nsfw_only=True)
        assert filter_instance.nsfw_only is True
        assert filter_instance.mode == 'only'
    
    def test_filter_exclude_mode_blocks_nsfw(self):
        """Test filter in exclude mode blocks NSFW content."""
        filter_instance = NSFWFilter(mode='exclude')
        
        # NSFW post
        nsfw_post = PostMetadata(
            id='abc123',
            title='NSFW Content',
            url='https://example.com/nsfw.jpg',
            domain='example.com',
            is_nsfw=True
        )
        
        # Safe post
        safe_post = PostMetadata(
            id='def456',
            title='Safe Content',
            url='https://example.com/safe.jpg',
            domain='example.com',
            is_nsfw=False
        )
        
        assert filter_instance.apply(nsfw_post) is False
        assert filter_instance.apply(safe_post) is True
    
    def test_filter_include_mode_allows_all(self):
        """Test filter in include mode allows all content."""
        filter_instance = NSFWFilter(mode='include')
        
        # NSFW post
        nsfw_post = PostMetadata(
            id='abc123',
            title='NSFW Content',
            url='https://example.com/nsfw.jpg',
            domain='example.com',
            is_nsfw=True
        )
        
        # Safe post
        safe_post = PostMetadata(
            id='def456',
            title='Safe Content',
            url='https://example.com/safe.jpg',
            domain='example.com',
            is_nsfw=False
        )
        
        assert filter_instance.apply(nsfw_post) is True
        assert filter_instance.apply(safe_post) is True
    
    def test_filter_only_mode_blocks_safe(self):
        """Test filter in NSFW-only mode blocks safe content."""
        filter_instance = NSFWFilter(mode='only')
        
        # NSFW post
        nsfw_post = PostMetadata(
            id='abc123',
            title='NSFW Content',
            url='https://example.com/nsfw.jpg',
            domain='example.com',
            is_nsfw=True
        )
        
        # Safe post
        safe_post = PostMetadata(
            id='def456',
            title='Safe Content',
            url='https://example.com/safe.jpg',
            domain='example.com',
            is_nsfw=False
        )
        
        assert filter_instance.apply(nsfw_post) is True
        assert filter_instance.apply(safe_post) is False
    
    def test_filter_nsfw_detection_from_various_fields(self):
        """Test NSFW detection from various post metadata fields."""
        filter_instance = NSFWFilter(mode='exclude')
        
        # Post with is_nsfw=True
        nsfw_direct = PostMetadata(
            id='abc123',
            title='Direct NSFW',
            url='https://example.com/content.jpg',
            domain='example.com',
            is_nsfw=True
        )
        
        # Post with over_18=True (Reddit field)
        nsfw_over18 = PostMetadata(
            id='def456',
            title='Over 18 Content',
            url='https://example.com/content.jpg',
            domain='example.com',
            is_nsfw=False  # But over_18 might be checked
        )
        # Simulate over_18 field if available
        setattr(nsfw_over18, 'over_18', True)
        
        assert filter_instance.apply(nsfw_direct) is False
        # Would also check over_18 if the field exists
        if hasattr(nsfw_over18, 'over_18') and nsfw_over18.over_18:
            assert filter_instance._is_nsfw_content(nsfw_over18) is True
    
    def test_filter_nsfw_detection_from_title_keywords(self):
        """Test NSFW detection from title keywords."""
        filter_instance = NSFWFilter(mode='exclude', detect_from_title=True)
        
        # Post with NSFW keywords in title
        nsfw_title = PostMetadata(
            id='abc123',
            title='[NSFW] Adult Content Here',
            url='https://example.com/content.jpg',
            domain='example.com',
            is_nsfw=False  # Not marked but has NSFW in title
        )
        
        # Post with explicit language
        explicit_title = PostMetadata(
            id='def456',
            title='Sexy hot content',
            url='https://example.com/content.jpg',
            domain='example.com',
            is_nsfw=False
        )
        
        # Check if title-based detection works
        assert filter_instance._detect_nsfw_from_title(nsfw_title.title) is True
        
        # Safe title
        safe_title = PostMetadata(
            id='ghi789',
            title='Cute cat pictures',
            url='https://example.com/cats.jpg',
            domain='example.com',
            is_nsfw=False
        )
        
        assert filter_instance._detect_nsfw_from_title(safe_title.title) is False
    
    def test_filter_nsfw_detection_from_subreddit(self):
        """Test NSFW detection from subreddit context."""
        filter_instance = NSFWFilter(mode='exclude', detect_from_subreddit=True)
        
        # Post from known NSFW subreddit
        nsfw_subreddit_post = PostMetadata(
            id='abc123',
            title='Regular Title',
            url='https://example.com/content.jpg',
            domain='example.com',
            subreddit='gonewild',  # Known NSFW subreddit
            is_nsfw=False  # Not explicitly marked
        )
        
        # Post from safe subreddit
        safe_subreddit_post = PostMetadata(
            id='def456',
            title='Regular Title',
            url='https://example.com/content.jpg',
            domain='example.com',
            subreddit='aww',  # Safe subreddit
            is_nsfw=False
        )
        
        # Check subreddit-based detection
        assert filter_instance._is_nsfw_subreddit('gonewild') is True
        assert filter_instance._is_nsfw_subreddit('aww') is False
    
    def test_filter_nsfw_detection_from_domain(self):
        """Test NSFW detection from domain patterns."""
        filter_instance = NSFWFilter(mode='exclude', detect_from_domain=True)
        
        # Post from adult domain
        adult_domain_post = PostMetadata(
            id='abc123',
            title='Content',
            url='https://adult-site.xxx/content.jpg',
            domain='adult-site.xxx',
            is_nsfw=False
        )
        
        # Post from safe domain
        safe_domain_post = PostMetadata(
            id='def456',
            title='Content',
            url='https://imgur.com/image.jpg',
            domain='imgur.com',
            is_nsfw=False
        )
        
        # Check domain-based detection
        assert filter_instance._is_nsfw_domain('adult-site.xxx') is True
        assert filter_instance._is_nsfw_domain('imgur.com') is False
    
    def test_filter_confidence_scoring(self):
        """Test NSFW confidence scoring system."""
        filter_instance = NSFWFilter(mode='exclude', confidence_threshold=0.7)
        
        # High confidence NSFW (marked + title + subreddit)
        high_confidence = PostMetadata(
            id='abc123',
            title='[NSFW] Adult content',
            url='https://example.com/content.jpg',
            domain='example.com',
            subreddit='gonewild',
            is_nsfw=True
        )
        
        # Low confidence (only title suggestion)
        low_confidence = PostMetadata(
            id='def456',
            title='Hot weather today',  # 'hot' might trigger but low confidence
            url='https://example.com/weather.jpg',
            domain='example.com',
            subreddit='weather',
            is_nsfw=False
        )
        
        # Check confidence calculation
        high_score = filter_instance._calculate_nsfw_confidence(high_confidence)
        low_score = filter_instance._calculate_nsfw_confidence(low_confidence)
        
        assert high_score > 0.8
        assert low_score < 0.5
    
    def test_filter_age_verification_mode(self):
        """Test filter with age verification requirements."""
        filter_instance = NSFWFilter(mode='include', require_age_verification=True)
        
        # User hasn't verified age
        filter_instance.age_verified = False
        
        nsfw_post = PostMetadata(
            id='abc123',
            title='NSFW Content',
            url='https://example.com/nsfw.jpg',
            domain='example.com',
            is_nsfw=True
        )
        
        # Should block even in include mode if age not verified
        assert filter_instance.apply(nsfw_post) is False
        
        # After age verification
        filter_instance.age_verified = True
        assert filter_instance.apply(nsfw_post) is True
    
    def test_filter_custom_nsfw_keywords(self):
        """Test filter with custom NSFW keywords."""
        custom_keywords = ['adult', 'mature', 'explicit']
        filter_instance = NSFWFilter(
            mode='exclude',
            custom_nsfw_keywords=custom_keywords,
            detect_from_title=True
        )
        
        # Post with custom keyword
        custom_nsfw = PostMetadata(
            id='abc123',
            title='Explicit content warning',
            url='https://example.com/content.jpg',
            domain='example.com',
            is_nsfw=False
        )
        
        # Should detect based on custom keywords
        assert filter_instance._detect_nsfw_from_title(custom_nsfw.title) is True
    
    def test_filter_whitelist_domains(self):
        """Test filter with whitelisted domains."""
        # Whitelist educational domains even if they might have adult content keywords
        whitelist_domains = ['education.com', 'medical.edu']
        filter_instance = NSFWFilter(
            mode='exclude',
            whitelist_domains=whitelist_domains,
            detect_from_domain=True
        )
        
        # Educational content with adult keywords
        educational_post = PostMetadata(
            id='abc123',
            title='Human anatomy study',
            url='https://medical.edu/anatomy.jpg',
            domain='medical.edu',
            is_nsfw=False
        )
        
        # Should pass despite potentially triggering keywords
        assert filter_instance.apply(educational_post) is True
    
    def test_filter_configuration_from_dict(self):
        """Test filter configuration from dictionary."""
        config = {
            'mode': 'only',
            'detect_from_title': True,
            'detect_from_subreddit': True,
            'detect_from_domain': True,
            'confidence_threshold': 0.8,
            'custom_nsfw_keywords': ['adult', 'mature'],
            'whitelist_domains': ['education.com']
        }
        
        filter_instance = NSFWFilter.from_config(config)
        
        assert filter_instance.mode == 'only'
        assert filter_instance.detect_from_title is True
        assert filter_instance.detect_from_subreddit is True
        assert filter_instance.detect_from_domain is True
        assert filter_instance.confidence_threshold == 0.8
        assert filter_instance.custom_nsfw_keywords == ['adult', 'mature']
        assert filter_instance.whitelist_domains == ['education.com']
    
    def test_filter_get_description(self):
        """Test filter description generation."""
        # Exclude mode
        exclude_filter = NSFWFilter(mode='exclude')
        description = exclude_filter.get_description()
        assert 'exclude' in description.lower()
        assert 'nsfw' in description.lower()
        
        # Include mode
        include_filter = NSFWFilter(mode='include')
        description = include_filter.get_description()
        assert 'include' in description.lower()
        assert 'nsfw' in description.lower()
        
        # Only mode
        only_filter = NSFWFilter(mode='only')
        description = only_filter.get_description()
        assert 'only' in description.lower()
        assert 'nsfw' in description.lower()
    
    def test_filter_invalid_mode_handling(self):
        """Test filter handling of invalid modes."""
        # Should default to exclude for invalid mode
        filter_instance = NSFWFilter(mode='invalid_mode')
        assert filter_instance.mode == 'exclude'
        assert filter_instance.include_nsfw is False
    
    def test_filter_mode_aliases(self):
        """Test filter mode aliases and variations."""
        # Test various aliases for exclude
        for exclude_alias in ['exclude', 'block', 'filter', 'no']:
            filter_instance = NSFWFilter(mode=exclude_alias)
            assert filter_instance.include_nsfw is False
        
        # Test various aliases for include
        for include_alias in ['include', 'allow', 'yes']:
            filter_instance = NSFWFilter(mode=include_alias)
            assert filter_instance.include_nsfw is True
            assert filter_instance.nsfw_only is False
        
        # Test various aliases for only
        for only_alias in ['only', 'exclusive', 'just']:
            filter_instance = NSFWFilter(mode=only_alias)
            assert filter_instance.nsfw_only is True
    
    def test_filter_edge_cases(self):
        """Test filter handling of edge cases."""
        filter_instance = NSFWFilter(mode='exclude')
        
        # Post with None NSFW field
        none_nsfw = PostMetadata(
            id='abc123',
            title='Content',
            url='https://example.com/content.jpg',
            domain='example.com',
            is_nsfw=None  # None instead of boolean
        )
        
        # Should treat None as False (not NSFW)
        assert filter_instance.apply(none_nsfw) is True
        
        # Post with missing NSFW field entirely
        missing_nsfw = PostMetadata(
            id='def456',
            title='Content',
            url='https://example.com/content.jpg',
            domain='example.com'
            # is_nsfw field completely missing
        )
        
        # Should handle gracefully
        assert filter_instance.apply(missing_nsfw) is True
    
    def test_filter_performance_with_large_datasets(self):
        """Test filter performance considerations."""
        filter_instance = NSFWFilter(mode='exclude')
        
        # Generate multiple posts for performance testing
        posts = []
        for i in range(100):
            post = PostMetadata(
                id=f'post_{i}',
                title=f'Post {i}',
                url=f'https://example.com/post_{i}.jpg',
                domain='example.com',
                is_nsfw=(i % 3 == 0)  # Every 3rd post is NSFW
            )
            posts.append(post)
        
        # Filter should handle large datasets efficiently
        filtered_posts = [post for post in posts if filter_instance.apply(post)]
        
        # Should filter out NSFW posts (every 3rd post)
        expected_count = len([p for p in posts if not p.is_nsfw])
        assert len(filtered_posts) == expected_count