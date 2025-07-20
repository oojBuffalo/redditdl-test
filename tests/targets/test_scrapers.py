"""
Tests for enhanced scraper implementations.

Tests the EnhancedPrawScraper, EnhancedYarsScraper, and ScraperFactory
to ensure correct behavior and integration with the target system.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add the project root to the Python path
# project_root = Path(__file__).parent.parent.parent
# sys.path.insert(0, str(project_root))

from redditdl.targets.scrapers import (
    EnhancedPrawScraper, 
    EnhancedYarsScraper, 
    ScraperFactory
)
from redditdl.targets.base_scraper import (
    ScrapingConfig, 
    ScrapingError, 
    AuthenticationError, 
    TargetNotFoundError
)
from redditdl.targets.resolver import TargetInfo, TargetType
from redditdl.scrapers import PostMetadata


class TestEnhancedPrawScraper:
    """Test EnhancedPrawScraper implementation."""
    
    @pytest.fixture
    def praw_config(self):
        """Create configuration for PRAW scraper."""
        return ScrapingConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            user_agent="TestAgent/1.0",
            post_limit=5
        )
    
    @pytest.fixture
    def authenticated_config(self):
        """Create authenticated configuration for PRAW scraper."""
        return ScrapingConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            username="test_user",
            password="test_password",
            user_agent="TestAgent/1.0",
            post_limit=5
        )
    
    def test_scraper_properties(self, praw_config):
        """Test basic scraper properties."""
        with patch('targets.scrapers.praw.Reddit'):
            scraper = EnhancedPrawScraper(praw_config)
            
            assert scraper.scraper_type == "praw"
            assert scraper.requires_authentication is True
    
    def test_missing_credentials(self):
        """Test initialization with missing credentials."""
        config = ScrapingConfig()  # No client_id or client_secret
        
        with pytest.raises(AuthenticationError, match="PRAW scraper requires client_id and client_secret"):
            EnhancedPrawScraper(config)
    
    @patch('targets.scrapers.praw.Reddit')
    def test_initialization_success(self, mock_reddit, praw_config):
        """Test successful initialization."""
        mock_reddit_instance = Mock()
        mock_reddit.return_value = mock_reddit_instance
        
        scraper = EnhancedPrawScraper(praw_config)
        
        assert scraper.reddit == mock_reddit_instance
        assert scraper._authenticated is False
        mock_reddit.assert_called_once()
    
    @patch('targets.scrapers.praw.Reddit')
    def test_authenticated_initialization(self, mock_reddit, authenticated_config):
        """Test initialization with user authentication."""
        mock_reddit_instance = Mock()
        mock_reddit.return_value = mock_reddit_instance
        
        scraper = EnhancedPrawScraper(authenticated_config)
        
        assert scraper._authenticated is True
        mock_reddit.assert_called_once()
    
    def test_can_handle_target_user(self, praw_config):
        """Test handling user targets."""
        with patch('targets.scrapers.praw.Reddit'):
            scraper = EnhancedPrawScraper(praw_config)
            
            user_target = TargetInfo(
                target_type=TargetType.USER,
                target_value="testuser",
                original_input="testuser"
            )
            
            assert scraper.can_handle_target(user_target) is True
    
    def test_can_handle_target_subreddit(self, praw_config):
        """Test handling subreddit targets."""
        with patch('targets.scrapers.praw.Reddit'):
            scraper = EnhancedPrawScraper(praw_config)
            
            subreddit_target = TargetInfo(
                target_type=TargetType.SUBREDDIT,
                target_value="testsubreddit",
                original_input="r/testsubreddit"
            )
            
            assert scraper.can_handle_target(subreddit_target) is True
    
    def test_cannot_handle_unauthenticated_targets(self, praw_config):
        """Test rejection of authenticated targets without user auth."""
        with patch('targets.scrapers.praw.Reddit'):
            scraper = EnhancedPrawScraper(praw_config)
            
            saved_target = TargetInfo(
                target_type=TargetType.SAVED,
                target_value="saved",
                original_input="saved",
                requires_auth=True
            )
            
            assert scraper.can_handle_target(saved_target) is False
    
    def test_can_handle_authenticated_targets_with_auth(self, authenticated_config):
        """Test handling authenticated targets with user auth."""
        with patch('targets.scrapers.praw.Reddit'):
            scraper = EnhancedPrawScraper(authenticated_config)
            
            saved_target = TargetInfo(
                target_type=TargetType.SAVED,
                target_value="saved",
                original_input="saved",
                requires_auth=True
            )
            
            assert scraper.can_handle_target(saved_target) is True
    
    @patch('targets.scrapers.praw.Reddit')
    def test_get_supported_target_types(self, mock_reddit, praw_config):
        """Test getting supported target types."""
        scraper = EnhancedPrawScraper(praw_config)
        types = scraper.get_supported_target_types()
        
        assert TargetType.USER in types
        assert TargetType.SUBREDDIT in types
        assert TargetType.SAVED not in types  # Not authenticated
    
    @patch('targets.scrapers.praw.Reddit')
    def test_get_supported_target_types_authenticated(self, mock_reddit, authenticated_config):
        """Test getting supported target types with authentication."""
        scraper = EnhancedPrawScraper(authenticated_config)
        types = scraper.get_supported_target_types()
        
        assert TargetType.USER in types
        assert TargetType.SUBREDDIT in types
        assert TargetType.SAVED in types
        assert TargetType.UPVOTED in types


class TestEnhancedYarsScraper:
    """Test EnhancedYarsScraper implementation."""
    
    @pytest.fixture
    def yars_config(self):
        """Create configuration for YARS scraper."""
        return ScrapingConfig(
            sleep_interval=0.1,  # Fast for testing
            post_limit=3
        )
    
    @patch('targets.scrapers.YARS')
    def test_scraper_properties(self, mock_yars, yars_config):
        """Test basic scraper properties."""
        scraper = EnhancedYarsScraper(yars_config)
        
        assert scraper.scraper_type == "yars"
        assert scraper.requires_authentication is False
    
    @patch('targets.scrapers.YARS')
    def test_initialization(self, mock_yars, yars_config):
        """Test YARS scraper initialization."""
        mock_yars_instance = Mock()
        mock_yars.return_value = mock_yars_instance
        
        scraper = EnhancedYarsScraper(yars_config)
        
        assert scraper.yars == mock_yars_instance
        mock_yars.assert_called_once()
    
    @patch('targets.scrapers.YARS')
    def test_can_handle_target_user(self, mock_yars, yars_config):
        """Test handling user targets."""
        scraper = EnhancedYarsScraper(yars_config)
        
        user_target = TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="testuser"
        )
        
        assert scraper.can_handle_target(user_target) is True
    
    @patch('targets.scrapers.YARS')
    def test_can_handle_target_subreddit(self, mock_yars, yars_config):
        """Test handling subreddit targets."""
        scraper = EnhancedYarsScraper(yars_config)
        
        subreddit_target = TargetInfo(
            target_type=TargetType.SUBREDDIT,
            target_value="testsubreddit",
            original_input="r/testsubreddit"
        )
        
        assert scraper.can_handle_target(subreddit_target) is True
    
    @patch('targets.scrapers.YARS')
    def test_cannot_handle_authenticated_targets(self, mock_yars, yars_config):
        """Test rejection of authenticated targets."""
        scraper = EnhancedYarsScraper(yars_config)
        
        saved_target = TargetInfo(
            target_type=TargetType.SAVED,
            target_value="saved",
            original_input="saved",
            requires_auth=True
        )
        
        assert scraper.can_handle_target(saved_target) is False
    
    @patch('targets.scrapers.YARS')
    def test_validate_authentication_always_true(self, mock_yars, yars_config):
        """Test that YARS authentication is always valid (no auth required)."""
        scraper = EnhancedYarsScraper(yars_config)
        assert scraper.validate_authentication() is True
    
    @patch('targets.scrapers.YARS')
    def test_get_supported_target_types(self, mock_yars, yars_config):
        """Test getting supported target types."""
        scraper = EnhancedYarsScraper(yars_config)
        types = scraper.get_supported_target_types()
        
        assert TargetType.USER in types
        assert TargetType.SUBREDDIT in types
        assert TargetType.SAVED not in types
        assert TargetType.UPVOTED not in types


class TestScraperFactory:
    """Test ScraperFactory for automatic scraper selection."""
    
    @pytest.fixture
    def api_config(self):
        """Create configuration with API credentials."""
        return ScrapingConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            post_limit=5
        )
    
    @pytest.fixture
    def no_api_config(self):
        """Create configuration without API credentials."""
        return ScrapingConfig(post_limit=5)
    
    def test_create_scraper_for_authenticated_target(self, api_config):
        """Test creating scraper for target requiring authentication."""
        target_info = TargetInfo(
            target_type=TargetType.SAVED,
            target_value="saved",
            original_input="saved",
            requires_auth=True
        )
        
        with patch('targets.scrapers.EnhancedPrawScraper') as mock_praw:
            mock_scraper = Mock()
            mock_praw.return_value = mock_scraper
            
            scraper = ScraperFactory.create_scraper(api_config, target_info)
            
            assert scraper == mock_scraper
            mock_praw.assert_called_once_with(api_config)
    
    def test_create_scraper_authenticated_target_no_credentials(self, no_api_config):
        """Test creating scraper for authenticated target without credentials."""
        target_info = TargetInfo(
            target_type=TargetType.SAVED,
            target_value="saved",
            original_input="saved",
            requires_auth=True
        )
        
        with pytest.raises(ScrapingError, match="requires Reddit API authentication"):
            ScraperFactory.create_scraper(no_api_config, target_info)
    
    def test_create_scraper_prefers_praw_with_credentials(self, api_config):
        """Test that factory prefers PRAW when API credentials are available."""
        target_info = TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="testuser"
        )
        
        with patch('targets.scrapers.EnhancedPrawScraper') as mock_praw:
            mock_scraper = Mock()
            mock_scraper.can_handle_target.return_value = True
            mock_praw.return_value = mock_scraper
            
            scraper = ScraperFactory.create_scraper(api_config, target_info)
            
            assert scraper == mock_scraper
            mock_praw.assert_called_once_with(api_config)
    
    def test_create_scraper_falls_back_to_yars(self, no_api_config):
        """Test that factory falls back to YARS without API credentials."""
        target_info = TargetInfo(
            target_type=TargetType.USER,
            target_value="testuser",
            original_input="testuser"
        )
        
        with patch('targets.scrapers.EnhancedYarsScraper') as mock_yars:
            mock_scraper = Mock()
            mock_scraper.can_handle_target.return_value = True
            mock_yars.return_value = mock_scraper
            
            scraper = ScraperFactory.create_scraper(no_api_config, target_info)
            
            assert scraper == mock_scraper
            mock_yars.assert_called_once_with(no_api_config)
    
    def test_create_scraper_no_suitable_scraper(self, no_api_config):
        """Test error when no suitable scraper is available."""
        # Create unsupported target type
        target_info = TargetInfo(
            target_type=TargetType.URL,
            target_value="https://example.com",
            original_input="https://example.com"
        )
        
        with patch('targets.scrapers.EnhancedYarsScraper') as mock_yars:
            mock_scraper = Mock()
            mock_scraper.can_handle_target.return_value = False
            mock_yars.return_value = mock_scraper
            
            with pytest.raises(ScrapingError, match="No scraper available"):
                ScraperFactory.create_scraper(no_api_config, target_info)
    
    def test_get_available_scrapers_no_api(self, no_api_config):
        """Test getting available scrapers without API credentials."""
        with patch('targets.scrapers.EnhancedYarsScraper') as mock_yars:
            mock_yars_scraper = Mock()
            mock_yars.return_value = mock_yars_scraper
            
            scrapers = ScraperFactory.get_available_scrapers(no_api_config)
            
            assert len(scrapers) == 1
            assert scrapers[0] == mock_yars_scraper
    
    def test_get_available_scrapers_with_api(self, api_config):
        """Test getting available scrapers with API credentials."""
        with patch('targets.scrapers.EnhancedYarsScraper') as mock_yars, \
             patch('targets.scrapers.EnhancedPrawScraper') as mock_praw:
            
            mock_yars_scraper = Mock()
            mock_praw_scraper = Mock()
            mock_yars.return_value = mock_yars_scraper
            mock_praw.return_value = mock_praw_scraper
            
            scrapers = ScraperFactory.get_available_scrapers(api_config)
            
            assert len(scrapers) == 2
            assert mock_yars_scraper in scrapers
            assert mock_praw_scraper in scrapers
    
    def test_get_available_scrapers_praw_auth_fails(self, api_config):
        """Test getting scrapers when PRAW authentication fails."""
        with patch('targets.scrapers.EnhancedYarsScraper') as mock_yars, \
             patch('targets.scrapers.EnhancedPrawScraper') as mock_praw:
            
            mock_yars_scraper = Mock()
            mock_yars.return_value = mock_yars_scraper
            mock_praw.side_effect = AuthenticationError("Auth failed")
            
            scrapers = ScraperFactory.get_available_scrapers(api_config)
            
            # Should only include YARS scraper
            assert len(scrapers) == 1
            assert scrapers[0] == mock_yars_scraper