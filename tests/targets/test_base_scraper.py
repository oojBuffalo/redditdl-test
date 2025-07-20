"""
Tests for base scraper interface and configuration.

Tests the abstract BaseScraper interface and scraping configuration
to ensure consistent behavior across all scraper implementations.
"""

import pytest
import sys
from pathlib import Path
from typing import List
from unittest.mock import Mock, patch

# Add the project root to the Python path
# project_root = Path(__file__).parent.parent.parent
# sys.path.insert(0, str(project_root))

from redditdl.targets.base_scraper import (
    BaseScraper, 
    ScrapingConfig, 
    ScrapingError, 
    AuthenticationError, 
    TargetNotFoundError,
    RateLimitError
)
from redditdl.targets.resolver import TargetInfo, TargetType


class TestScrapingConfig:
    """Test ScrapingConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = ScrapingConfig()
        
        assert config.post_limit == 20
        assert config.sleep_interval == 1.0
        assert config.user_agent == "RedditDL/2.0"
        assert config.timeout == 30.0
        assert config.retries == 3
        assert config.client_id is None
        assert config.client_secret is None
        assert config.username is None
        assert config.password is None
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = ScrapingConfig(
            post_limit=50,
            sleep_interval=2.0,
            user_agent="CustomAgent/1.0",
            timeout=60.0,
            retries=5,
            client_id="test_id",
            client_secret="test_secret",
            username="test_user",
            password="test_pass"
        )
        
        assert config.post_limit == 50
        assert config.sleep_interval == 2.0
        assert config.user_agent == "CustomAgent/1.0"
        assert config.timeout == 60.0
        assert config.retries == 5
        assert config.client_id == "test_id"
        assert config.client_secret == "test_secret"
        assert config.username == "test_user"
        assert config.password == "test_pass"


class ConcreteScraper(BaseScraper):
    """Concrete implementation of BaseScraper for testing."""
    
    def __init__(self, config: ScrapingConfig, scraper_type: str = "test"):
        super().__init__(config)
        self._scraper_type = scraper_type
        self._requires_auth = False
        self._supported_types = [TargetType.USER]
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        return target_info.target_type in self._supported_types
    
    def fetch_posts(self, target_info: TargetInfo):
        return []
    
    def validate_authentication(self) -> bool:
        return not self._requires_auth
    
    @property
    def scraper_type(self) -> str:
        return self._scraper_type
    
    @property
    def requires_authentication(self) -> bool:
        return self._requires_auth
    
    def get_supported_target_types(self) -> List[TargetType]:
        """Override to return configured supported types."""
        return self._supported_types


class TestBaseScraper:
    """Test BaseScraper abstract interface."""
    
    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ScrapingConfig(post_limit=10, sleep_interval=0.5)
    
    @pytest.fixture
    def scraper(self, config):
        """Create test scraper instance."""
        return ConcreteScraper(config)
    
    def test_scraper_initialization(self, config):
        """Test scraper initialization."""
        scraper = ConcreteScraper(config, "test_scraper")
        
        assert scraper.config == config
        assert scraper.scraper_type == "test_scraper"
        assert scraper.requires_authentication is False
        assert scraper.logger is not None
    
    def test_get_rate_limit_interval(self, scraper):
        """Test rate limit interval getter."""
        assert scraper.get_rate_limit_interval() == 0.5
    
    def test_get_supported_target_types(self, scraper):
        """Test supported target types getter."""
        types = scraper.get_supported_target_types()
        assert TargetType.USER in types
    
    def test_prepare_target(self, scraper):
        """Test target preparation."""
        target_info = TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="testuser",
            requires_auth=False
        )
        
        prepared = scraper.prepare_target(target_info)
        
        assert prepared['target_type'] == 'user'
        assert prepared['target_value'] == 'testuser'
        assert prepared['requires_auth'] is False
    
    def test_string_representations(self, scraper):
        """Test string representation methods."""
        str_repr = str(scraper)
        repr_repr = repr(scraper)
        
        assert "ConcreteScraper" in str_repr
        assert "test" in str_repr
        
        assert "ConcreteScraper" in repr_repr
        assert "type='test'" in repr_repr
        assert "auth=False" in repr_repr
    
    def test_cannot_instantiate_abstract_class(self, config):
        """Test that BaseScraper cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseScraper(config)


class TestScrapingExceptions:
    """Test scraping exception hierarchy."""
    
    def test_scraping_error(self):
        """Test base ScrapingError."""
        error = ScrapingError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)
    
    def test_authentication_error(self):
        """Test AuthenticationError inheritance."""
        error = AuthenticationError("Auth failed")
        assert str(error) == "Auth failed"
        assert isinstance(error, ScrapingError)
        assert isinstance(error, Exception)
    
    def test_target_not_found_error(self):
        """Test TargetNotFoundError inheritance."""
        error = TargetNotFoundError("Target not found")
        assert str(error) == "Target not found"
        assert isinstance(error, ScrapingError)
        assert isinstance(error, Exception)
    
    def test_rate_limit_error(self):
        """Test RateLimitError inheritance."""
        error = RateLimitError("Rate limited")
        assert str(error) == "Rate limited"
        assert isinstance(error, ScrapingError)
        assert isinstance(error, Exception)


class TestScraperWithAuthentication:
    """Test scraper behavior with authentication requirements."""
    
    @pytest.fixture
    def auth_scraper(self):
        """Create scraper that requires authentication."""
        config = ScrapingConfig(client_id="test", client_secret="secret")
        scraper = ConcreteScraper(config)
        scraper._requires_auth = True
        return scraper
    
    def test_requires_authentication(self, auth_scraper):
        """Test authentication requirement property."""
        assert auth_scraper.requires_authentication is True
    
    def test_validate_authentication_fails(self, auth_scraper):
        """Test authentication validation failure."""
        assert auth_scraper.validate_authentication() is False


class TestScraperTargetHandling:
    """Test scraper target type handling."""
    
    @pytest.fixture
    def multi_target_scraper(self):
        """Create scraper supporting multiple target types."""
        config = ScrapingConfig()
        scraper = ConcreteScraper(config)
        scraper._supported_types = [TargetType.USER, TargetType.SUBREDDIT]
        return scraper
    
    def test_can_handle_supported_target(self, multi_target_scraper):
        """Test handling of supported target types."""
        user_target = TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="testuser"
        )
        
        subreddit_target = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="testsubreddit",
            original_input="r/testsubreddit"
        )
        
        assert multi_target_scraper.can_handle_target(user_target) is True
        assert multi_target_scraper.can_handle_target(subreddit_target) is True
    
    def test_cannot_handle_unsupported_target(self, multi_target_scraper):
        """Test rejection of unsupported target types."""
        url_target = TargetInfo(
            target_type=TargetType.URL,
            target_value="https://reddit.com/r/test/comments/123/title/",
            original_input="https://reddit.com/r/test/comments/123/title/"
        )
        
        assert multi_target_scraper.can_handle_target(url_target) is False
    
    def test_get_supported_target_types_multiple(self, multi_target_scraper):
        """Test getting multiple supported target types."""
        types = multi_target_scraper.get_supported_target_types()
        
        assert TargetType.USER in types
        assert TargetType.SUBREDDIT in types
        assert len(types) == 2