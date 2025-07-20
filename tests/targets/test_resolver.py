"""
Tests for target resolution functionality.

Tests the TargetResolver class for parsing and validating different
Reddit target formats including users, subreddits, URLs, and special targets.
"""

import pytest
import sys
from pathlib import Path

# Add the project root to the Python path
# project_root = Path(__file__).parent.parent.parent
# sys.path.insert(0, str(project_root))

from redditdl.targets.resolver import TargetResolver, TargetInfo, TargetType


class TestTargetResolver:
    """Test target resolution functionality."""
    
    @pytest.fixture
    def resolver(self):
        """Create a TargetResolver instance for testing."""
        return TargetResolver()
    
    def test_resolve_plain_username(self, resolver):
        """Test resolving plain username format."""
        # Use a name that clearly looks like a username (has underscore)
        target = resolver.resolve_target("test_user")
        
        assert target.target_type == TargetType.USER
        assert target.target_value == "test_user"
        assert target.original_input == "test_user"
        assert target.requires_auth is False
        assert "User test_user posts" in target.metadata["description"]
    
    def test_resolve_prefixed_username(self, resolver):
        """Test resolving u/username format."""
        test_cases = ["u/testuser", "/u/testuser"]
        
        for input_target in test_cases:
            target = resolver.resolve_target(input_target)
            
            assert target.target_type == TargetType.USER
            assert target.target_value == "testuser"
            assert target.original_input == input_target
            assert target.requires_auth is False
            assert "User testuser posts" in target.metadata["description"]
    
    def test_resolve_plain_subreddit(self, resolver):
        """Test resolving plain subreddit format."""
        # Use a name that's clearly a subreddit (lowercase, no underscores)
        target = resolver.resolve_target("askreddit")
        
        assert target.target_type == TargetType.SUBREDDIT
        assert target.target_value == "askreddit"
        assert target.original_input == "askreddit"
        assert target.requires_auth is False
        assert "Subreddit r/askreddit posts" in target.metadata["description"]
    
    def test_resolve_prefixed_subreddit(self, resolver):
        """Test resolving r/subreddit format."""
        test_cases = ["r/testsubreddit", "/r/testsubreddit"]
        
        for input_target in test_cases:
            target = resolver.resolve_target(input_target)
            
            assert target.target_type == TargetType.SUBREDDIT
            assert target.target_value == "testsubreddit"
            assert target.original_input == input_target
            assert target.requires_auth is False
            assert "Subreddit r/testsubreddit posts" in target.metadata["description"]
    
    def test_resolve_user_urls(self, resolver):
        """Test resolving Reddit user URLs."""
        test_cases = [
            "https://reddit.com/u/testuser",
            "https://www.reddit.com/u/testuser",
            "https://reddit.com/user/testuser",
            "https://www.reddit.com/user/testuser",
            "http://reddit.com/u/testuser"
        ]
        
        for url in test_cases:
            target = resolver.resolve_target(url)
            
            assert target.target_type == TargetType.USER
            assert target.target_value == "testuser"
            assert target.original_input == url
            assert target.requires_auth is False
            assert "User testuser posts" in target.metadata["description"]
            assert target.metadata["source_url"] == url
    
    def test_resolve_subreddit_urls(self, resolver):
        """Test resolving Reddit subreddit URLs."""
        test_cases = [
            "https://reddit.com/r/testsubreddit",
            "https://www.reddit.com/r/testsubreddit",
            "http://reddit.com/r/testsubreddit"
        ]
        
        for url in test_cases:
            target = resolver.resolve_target(url)
            
            assert target.target_type == TargetType.SUBREDDIT
            assert target.target_value == "testsubreddit"
            assert target.original_input == url
            assert target.requires_auth is False
            assert "Subreddit r/testsubreddit posts" in target.metadata["description"]
            assert target.metadata["source_url"] == url
    
    def test_resolve_post_urls(self, resolver):
        """Test resolving Reddit post URLs."""
        url = "https://reddit.com/r/testsubreddit/comments/abc123/some_post_title/"
        target = resolver.resolve_target(url)
        
        assert target.target_type == TargetType.URL
        assert target.target_value == url
        assert target.original_input == url
        assert target.requires_auth is False
        assert "Reddit post abc123 from r/testsubreddit" in target.metadata["description"]
        assert target.metadata["subreddit"] == "testsubreddit"
        assert target.metadata["post_id"] == "abc123"
        assert target.metadata["source_url"] == url
    
    def test_resolve_authenticated_targets(self, resolver):
        """Test resolving authenticated targets (saved, upvoted)."""
        test_cases = ["saved", "upvoted", "SAVED", "UPVOTED"]
        
        for input_target in test_cases:
            target = resolver.resolve_target(input_target)
            expected_type = TargetType.SAVED if input_target.lower() == "saved" else TargetType.UPVOTED
            
            assert target.target_type == expected_type
            assert target.target_value == input_target.lower()
            assert target.original_input == input_target
            assert target.requires_auth is True
            assert f"User {input_target.lower()} posts" in target.metadata["description"]
    
    def test_resolve_invalid_targets(self, resolver):
        """Test resolving invalid target formats."""
        invalid_targets = [
            "x",  # Too short username
            "a" * 22,  # Too long username  
            "user/with/slashes",  # Invalid characters
            "r/",  # Empty subreddit
            "/u/",  # Empty username
        ]
        
        for invalid_target in invalid_targets:
            target = resolver.resolve_target(invalid_target)
            assert target.target_type == TargetType.UNKNOWN
        
        # Test invalid domain separately since it should also be UNKNOWN
        target = resolver.resolve_target("https://notreddit.com/u/test")
        assert target.target_type == TargetType.UNKNOWN
    
    def test_resolve_empty_target(self, resolver):
        """Test resolving empty or whitespace target."""
        with pytest.raises(ValueError, match="Target input cannot be empty"):
            resolver.resolve_target("")
        
        with pytest.raises(ValueError, match="Target input cannot be empty"):
            resolver.resolve_target("   ")
    
    def test_resolve_multiple_targets(self, resolver):
        """Test resolving multiple targets at once."""
        targets = ["test_user", "r/testsubreddit", "saved"]
        resolved = resolver.resolve_multiple_targets(targets)
        
        assert len(resolved) == 3
        assert resolved[0].target_type == TargetType.USER
        assert resolved[1].target_type == TargetType.SUBREDDIT
        assert resolved[2].target_type == TargetType.SAVED
    
    def test_resolve_multiple_targets_with_invalid(self, resolver):
        """Test resolving multiple targets with some invalid ones."""
        targets = ["test_user", "invalid_target_$%^", "r/testsubreddit"]
        
        # This should raise ValueError due to invalid target
        with pytest.raises(ValueError, match="Failed to resolve targets"):
            resolver.resolve_multiple_targets(targets)
    
    def test_validate_target_accessibility_no_auth(self, resolver):
        """Test target accessibility validation without API auth."""
        # User target without auth
        user_target = resolver.resolve_target("testuser")
        validation = resolver.validate_target_accessibility(user_target, has_api_auth=False)
        
        assert validation['accessible'] is True
        assert validation['auth_required'] is False
        assert len(validation['warnings']) > 0  # Should warn about public scraping
    
    def test_validate_target_accessibility_with_auth(self, resolver):
        """Test target accessibility validation with API auth."""
        # Saved posts target with auth
        saved_target = resolver.resolve_target("saved")
        validation = resolver.validate_target_accessibility(saved_target, has_api_auth=True)
        
        assert validation['accessible'] is True
        assert validation['auth_required'] is True
        assert len(validation['recommendations']) == 0
    
    def test_validate_target_accessibility_auth_required_no_auth(self, resolver):
        """Test target requiring auth but no auth available."""
        saved_target = resolver.resolve_target("saved")
        validation = resolver.validate_target_accessibility(saved_target, has_api_auth=False)
        
        assert validation['accessible'] is False
        assert validation['auth_required'] is True
        assert len(validation['recommendations']) > 0
        assert "Reddit API authentication" in validation['recommendations'][0]
    
    def test_validate_unknown_target(self, resolver):
        """Test validation of unknown target type."""
        # Force unknown target type
        target = resolver.resolve_target("invalid_$%^")
        validation = resolver.validate_target_accessibility(target, has_api_auth=True)
        
        assert validation['accessible'] is False
        assert "format not recognized" in validation['recommendations'][0]
    
    def test_get_supported_formats(self, resolver):
        """Test getting supported target formats documentation."""
        formats = resolver.get_supported_formats()
        
        assert 'users' in formats
        assert 'subreddits' in formats
        assert 'urls' in formats
        assert 'authenticated' in formats
        
        # Check some expected formats are present
        assert 'username' in formats['users']
        assert 'u/username' in formats['users']
        assert 'r/subreddit' in formats['subreddits']
        assert 'saved' in formats['authenticated']
    
    def test_username_validation(self, resolver):
        """Test username format validation."""
        valid_usernames = ["test", "test_user", "test-user", "TestUser123"]
        invalid_usernames = ["", "a", "x" * 21, "test user", "test@user"]
        
        for username in valid_usernames:
            assert resolver._is_valid_username(username)
        
        for username in invalid_usernames:
            assert not resolver._is_valid_username(username)
    
    def test_subreddit_validation(self, resolver):
        """Test subreddit name format validation."""
        valid_subreddits = ["test", "test_sub", "test-sub", "TestSub123"]
        invalid_subreddits = ["", "a", "x" * 22, "test sub", "test@sub"]
        
        for subreddit in valid_subreddits:
            assert resolver._is_valid_subreddit(subreddit)
        
        for subreddit in invalid_subreddits:
            assert not resolver._is_valid_subreddit(subreddit)