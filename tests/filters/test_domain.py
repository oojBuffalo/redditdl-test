"""
Tests for Domain Filter

Tests the domain filter functionality including allowlist/blocklist
filtering and domain pattern matching.
"""

import pytest
from unittest.mock import Mock

from redditdl.filters.domain import DomainFilter
from redditdl.scrapers import PostMetadata


class TestDomainFilter:
    """Test suite for DomainFilter."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.filter = DomainFilter()
    
    def test_filter_initialization_default(self):
        """Test filter initialization with default settings."""
        assert isinstance(self.filter, DomainFilter)
        assert self.filter.allowed_domains == []
        assert self.filter.blocked_domains == []
        assert self.filter.strict_mode is False
    
    def test_filter_initialization_with_allowed_domains(self):
        """Test filter initialization with allowed domains."""
        allowed_domains = ['imgur.com', 'i.redd.it', 'example.com']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        assert filter_instance.allowed_domains == allowed_domains
        assert filter_instance.blocked_domains == []
    
    def test_filter_initialization_with_blocked_domains(self):
        """Test filter initialization with blocked domains."""
        blocked_domains = ['spam.com', 'malicious.example', 'ads.domain']
        filter_instance = DomainFilter(blocked_domains=blocked_domains)
        
        assert filter_instance.allowed_domains == []
        assert filter_instance.blocked_domains == blocked_domains
    
    def test_filter_initialization_with_both_lists(self):
        """Test filter initialization with both allowed and blocked domains."""
        allowed_domains = ['imgur.com', 'i.redd.it']
        blocked_domains = ['spam.com', 'ads.domain']
        filter_instance = DomainFilter(
            allowed_domains=allowed_domains,
            blocked_domains=blocked_domains
        )
        
        assert filter_instance.allowed_domains == allowed_domains
        assert filter_instance.blocked_domains == blocked_domains
    
    def test_filter_allowed_domains_passes(self):
        """Test filter passes posts from allowed domains."""
        allowed_domains = ['imgur.com', 'i.redd.it', 'example.com']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        # Create post from allowed domain
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://imgur.com/image.jpg',
            domain='imgur.com'
        )
        
        assert filter_instance.apply(post) is True
    
    def test_filter_allowed_domains_blocks(self):
        """Test filter blocks posts from non-allowed domains."""
        allowed_domains = ['imgur.com', 'i.redd.it']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        # Create post from non-allowed domain
        post = PostMetadata(
            id='abc123',
            title='Test Post',
            url='https://unknown.com/image.jpg',
            domain='unknown.com'
        )
        
        assert filter_instance.apply(post) is False
    
    def test_filter_blocked_domains_blocks(self):
        """Test filter blocks posts from blocked domains."""
        blocked_domains = ['spam.com', 'malicious.example', 'ads.domain']
        filter_instance = DomainFilter(blocked_domains=blocked_domains)
        
        # Create post from blocked domain
        post = PostMetadata(
            id='abc123',
            title='Spam Post',
            url='https://spam.com/content',
            domain='spam.com'
        )
        
        assert filter_instance.apply(post) is False
    
    def test_filter_blocked_domains_passes(self):
        """Test filter passes posts from non-blocked domains."""
        blocked_domains = ['spam.com', 'malicious.example']
        filter_instance = DomainFilter(blocked_domains=blocked_domains)
        
        # Create post from non-blocked domain
        post = PostMetadata(
            id='abc123',
            title='Good Post',
            url='https://imgur.com/image.jpg',
            domain='imgur.com'
        )
        
        assert filter_instance.apply(post) is True
    
    def test_filter_allowed_and_blocked_priority(self):
        """Test that blocked domains take priority over allowed domains."""
        allowed_domains = ['example.com', 'test.com']
        blocked_domains = ['example.com']  # Overlap with allowed
        filter_instance = DomainFilter(
            allowed_domains=allowed_domains,
            blocked_domains=blocked_domains
        )
        
        # Create post from domain that's both allowed and blocked
        post = PostMetadata(
            id='abc123',
            title='Conflicted Post',
            url='https://example.com/content',
            domain='example.com'
        )
        
        # Blocked should take priority
        assert filter_instance.apply(post) is False
    
    def test_filter_subdomain_handling(self):
        """Test filter handling of subdomains."""
        allowed_domains = ['imgur.com']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        # Test subdomain of allowed domain
        post = PostMetadata(
            id='abc123',
            title='Subdomain Post',
            url='https://i.imgur.com/image.jpg',
            domain='i.imgur.com'
        )
        
        # Should pass (subdomain of allowed domain)
        assert filter_instance.apply(post) is True
    
    def test_filter_subdomain_strict_mode(self):
        """Test filter subdomain handling in strict mode."""
        allowed_domains = ['imgur.com']
        filter_instance = DomainFilter(
            allowed_domains=allowed_domains,
            strict_mode=True
        )
        
        # Test subdomain of allowed domain in strict mode
        post = PostMetadata(
            id='abc123',
            title='Subdomain Post',
            url='https://i.imgur.com/image.jpg',
            domain='i.imgur.com'
        )
        
        # Should not pass in strict mode (exact match required)
        assert filter_instance.apply(post) is False
        
        # Test exact match
        exact_post = PostMetadata(
            id='def456',
            title='Exact Domain Post',
            url='https://imgur.com/image.jpg',
            domain='imgur.com'
        )
        
        # Should pass (exact match)
        assert filter_instance.apply(exact_post) is True
    
    def test_filter_case_insensitive_matching(self):
        """Test filter performs case-insensitive domain matching."""
        allowed_domains = ['IMGUR.COM', 'Example.Com']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        # Test lowercase domain
        post1 = PostMetadata(
            id='abc123',
            title='Lowercase Post',
            url='https://imgur.com/image.jpg',
            domain='imgur.com'
        )
        
        # Test mixed case domain
        post2 = PostMetadata(
            id='def456',
            title='Mixed Case Post',
            url='https://Example.com/content',
            domain='Example.com'
        )
        
        assert filter_instance.apply(post1) is True
        assert filter_instance.apply(post2) is True
    
    def test_filter_empty_domain(self):
        """Test filter handling of posts with empty domains."""
        allowed_domains = ['imgur.com']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        # Post with empty domain
        post = PostMetadata(
            id='abc123',
            title='No Domain Post',
            url='https://unknown-domain.com/content',
            domain=''
        )
        
        # Should not pass (empty domain not in allowed list)
        assert filter_instance.apply(post) is False
    
    def test_filter_self_posts(self):
        """Test filter handling of Reddit self posts."""
        # Allow self posts
        allowed_domains = ['self.python', 'self.programming']
        filter_instance = DomainFilter(allowed_domains=allowed_domains)
        
        # Self post from allowed subreddit
        post = PostMetadata(
            id='abc123',
            title='Self Post',
            url='https://reddit.com/r/python/comments/abc123/',
            domain='self.python',
            is_self=True
        )
        
        assert filter_instance.apply(post) is True
        
        # Self post from non-allowed subreddit
        post2 = PostMetadata(
            id='def456',
            title='Other Self Post',
            url='https://reddit.com/r/other/comments/def456/',
            domain='self.other',
            is_self=True
        )
        
        assert filter_instance.apply(post2) is False
    
    def test_filter_reddit_domains(self):
        """Test filter handling of various Reddit domains."""
        # Block Reddit media domains
        blocked_domains = ['v.redd.it', 'i.redd.it']
        filter_instance = DomainFilter(blocked_domains=blocked_domains)
        
        # Reddit video
        video_post = PostMetadata(
            id='abc123',
            title='Reddit Video',
            url='https://v.redd.it/abcd1234',
            domain='v.redd.it'
        )
        
        # Reddit image
        image_post = PostMetadata(
            id='def456',
            title='Reddit Image',
            url='https://i.redd.it/image.jpg',
            domain='i.redd.it'
        )
        
        assert filter_instance.apply(video_post) is False
        assert filter_instance.apply(image_post) is False
        
        # External image should pass
        external_post = PostMetadata(
            id='ghi789',
            title='External Image',
            url='https://imgur.com/image.jpg',
            domain='imgur.com'
        )
        
        assert filter_instance.apply(external_post) is True
    
    def test_filter_domain_extraction_from_url(self):
        """Test domain extraction from various URL formats."""
        filter_instance = DomainFilter()
        
        # Test domain extraction
        assert filter_instance._extract_domain('https://example.com/path') == 'example.com'
        assert filter_instance._extract_domain('http://sub.example.com/path') == 'sub.example.com'
        assert filter_instance._extract_domain('https://example.com:8080/path') == 'example.com'
        assert filter_instance._extract_domain('ftp://example.com/file') == 'example.com'
        
        # Test edge cases
        assert filter_instance._extract_domain('invalid-url') == ''
        assert filter_instance._extract_domain('') == ''
        assert filter_instance._extract_domain(None) == ''
    
    def test_filter_domain_normalization(self):
        """Test domain normalization (case, www prefix)."""
        filter_instance = DomainFilter()
        
        # Test case normalization
        assert filter_instance._normalize_domain('EXAMPLE.COM') == 'example.com'
        assert filter_instance._normalize_domain('Example.Com') == 'example.com'
        
        # Test www prefix handling
        assert filter_instance._normalize_domain('www.example.com') == 'example.com'
        assert filter_instance._normalize_domain('WWW.EXAMPLE.COM') == 'example.com'
        
        # Test combined
        assert filter_instance._normalize_domain('WWW.Example.COM') == 'example.com'
    
    def test_filter_wildcard_patterns(self):
        """Test filter support for wildcard domain patterns."""
        # Using wildcard patterns
        blocked_domains = ['*.ads.com', '*.spam.*']
        filter_instance = DomainFilter(blocked_domains=blocked_domains)
        
        # Test wildcard matching
        ad_post = PostMetadata(
            id='abc123',
            title='Ad Post',
            url='https://banner.ads.com/ad',
            domain='banner.ads.com'
        )
        
        spam_post = PostMetadata(
            id='def456',
            title='Spam Post',
            url='https://bad.spam.network/content',
            domain='bad.spam.network'
        )
        
        # Should block wildcard matches
        assert filter_instance.apply(ad_post) is False
        assert filter_instance.apply(spam_post) is False
    
    def test_filter_configuration_from_dict(self):
        """Test filter configuration from dictionary."""
        config = {
            'allowed_domains': ['imgur.com', 'i.redd.it'],
            'blocked_domains': ['spam.com'],
            'strict_mode': True
        }
        
        filter_instance = DomainFilter.from_config(config)
        
        assert filter_instance.allowed_domains == config['allowed_domains']
        assert filter_instance.blocked_domains == config['blocked_domains']
        assert filter_instance.strict_mode is True
    
    def test_filter_get_description(self):
        """Test filter description generation."""
        # Filter with allowed domains
        allowed_filter = DomainFilter(allowed_domains=['imgur.com', 'i.redd.it'])
        description = allowed_filter.get_description()
        assert 'allow' in description.lower()
        assert 'imgur.com' in description
        
        # Filter with blocked domains
        blocked_filter = DomainFilter(blocked_domains=['spam.com'])
        description = blocked_filter.get_description()
        assert 'block' in description.lower()
        assert 'spam.com' in description
        
        # Filter with both
        both_filter = DomainFilter(
            allowed_domains=['imgur.com'],
            blocked_domains=['spam.com']
        )
        description = both_filter.get_description()
        assert 'allow' in description.lower()
        assert 'block' in description.lower()
    
    def test_filter_no_configuration(self):
        """Test filter with no domain configuration (should pass all)."""
        filter_instance = DomainFilter()  # No domains specified
        
        post = PostMetadata(
            id='abc123',
            title='Any Post',
            url='https://random.com/content',
            domain='random.com'
        )
        
        # Should pass when no filtering rules are defined
        assert filter_instance.apply(post) is True
    
    def test_filter_domain_validation(self):
        """Test domain list validation during initialization."""
        # Valid domains
        valid_domains = ['example.com', 'sub.example.com', '*.wildcard.com']
        filter_instance = DomainFilter(allowed_domains=valid_domains)
        assert filter_instance.allowed_domains == valid_domains
        
        # Test with invalid domains (should be cleaned or handled gracefully)
        mixed_domains = ['valid.com', '', 'another.com', None, 'good.domain']
        filter_instance = DomainFilter(allowed_domains=mixed_domains)
        
        # Should filter out invalid entries
        valid_only = [d for d in mixed_domains if d and isinstance(d, str)]
        assert len(filter_instance.allowed_domains) == len(valid_only)