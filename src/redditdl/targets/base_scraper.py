"""
Base Scraper Interface

Abstract base class and interfaces for all Reddit scrapers to ensure
consistent behavior and plugin compatibility.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
import logging

# Import existing PostMetadata class
from ..scrapers import PostMetadata
from .resolver import TargetInfo, TargetType

# Import enhanced error handling
from ..core.exceptions import (
    RedditDLError, NetworkError, AuthenticationError, ProcessingError,
    ErrorCode, ErrorContext, RecoverySuggestion
)
from ..core.error_context import report_error


@dataclass
class ScrapingConfig:
    """Configuration for scraping operations."""
    post_limit: int = 20
    sleep_interval: float = 1.0
    user_agent: str = "RedditDL/2.0"
    timeout: float = 30.0
    retries: int = 3
    
    # API-specific config
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


# Enhanced exceptions for scraping operations
class ScrapingError(ProcessingError):
    """Exception for general scraping failures."""
    
    def __init__(self, message: str, target: Optional[str] = None, **kwargs):
        error_context = kwargs.get('context') or ErrorContext()
        if target:
            error_context.target = target
        
        kwargs['context'] = error_context
        kwargs.setdefault('error_code', ErrorCode.TARGET_CONTENT_UNAVAILABLE)
        
        super().__init__(message, **kwargs)


class TargetNotFoundError(ScrapingError):
    """Exception when a target (user, subreddit, etc.) is not found."""
    
    def __init__(self, message: str, target: Optional[str] = None, **kwargs):
        kwargs.setdefault('error_code', ErrorCode.TARGET_NOT_FOUND)
        
        super().__init__(message, target=target, **kwargs)
        
        # Add standard recovery suggestions
        self.add_suggestion(RecoverySuggestion(
            action="Check target name",
            description="Verify the target name is spelled correctly and exists",
            automatic=False,
            priority=1
        ))


class AuthenticationError(RedditDLError):
    """Exception for authentication-related scraping issues."""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('error_code', ErrorCode.AUTH_INVALID_CREDENTIALS)
        
        super().__init__(message, **kwargs)
        
        # Add standard recovery suggestions
        self.add_suggestion(RecoverySuggestion(
            action="Check credentials",
            description="Verify your Reddit API credentials are correct and have not expired",
            automatic=False,
            priority=1
        ))
        
        self.add_suggestion(RecoverySuggestion(
            action="Use public mode",
            description="Try scraping without authentication (may have limited access)",
            automatic=False,
            priority=2
        ))


class BaseScraper(ABC):
    """
    Abstract base class for all Reddit scrapers.
    
    Defines the standard interface that all scrapers must implement
    for consistency and plugin compatibility.
    """
    
    def __init__(self, config: ScrapingConfig):
        """
        Initialize the scraper with configuration.
        
        Args:
            config: Scraping configuration parameters
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """
        Check if this scraper can handle the given target type.
        
        Args:
            target_info: Information about the target to scrape
            
        Returns:
            True if this scraper can handle the target, False otherwise
        """
        pass
    
    @abstractmethod
    def fetch_posts(self, target_info: TargetInfo) -> List[PostMetadata]:
        """
        Fetch posts from the specified target.
        
        Args:
            target_info: Information about the target to scrape
            
        Returns:
            List of PostMetadata objects representing the fetched posts
            
        Raises:
            ScrapingError: If scraping fails
            AuthenticationError: If authentication is required but not provided
            TargetNotFoundError: If the target doesn't exist
        """
        pass
    
    @abstractmethod
    def validate_authentication(self) -> bool:
        """
        Validate that the scraper has proper authentication if required.
        
        Returns:
            True if authentication is valid or not required, False otherwise
        """
        pass
    
    @property
    @abstractmethod
    def scraper_type(self) -> str:
        """
        Get the type identifier for this scraper.
        
        Returns:
            String identifier for the scraper type (e.g., 'praw', 'yars')
        """
        pass
    
    @property
    @abstractmethod
    def requires_authentication(self) -> bool:
        """
        Check if this scraper requires authentication.
        
        Returns:
            True if authentication is required, False otherwise
        """
        pass
    
    def get_rate_limit_interval(self) -> float:
        """
        Get the recommended rate limiting interval for this scraper.
        
        Returns:
            Sleep interval in seconds between requests
        """
        return self.config.sleep_interval
    
    def get_supported_target_types(self) -> List[TargetType]:
        """
        Get the list of target types supported by this scraper.
        
        Returns:
            List of TargetType enums this scraper can handle
        """
        # Default implementation - subclasses should override
        return [TargetType.USER]
    
    def prepare_target(self, target_info: TargetInfo) -> Dict[str, Any]:
        """
        Prepare target-specific parameters for scraping.
        
        Args:
            target_info: Information about the target to scrape
            
        Returns:
            Dict containing target-specific scraping parameters
        """
        return {
            'target_type': target_info.target_type.value,
            'target_value': target_info.target_value,
            'requires_auth': target_info.requires_auth
        }
    
    def __str__(self) -> str:
        """String representation of the scraper."""
        return f"{self.__class__.__name__}({self.scraper_type})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the scraper."""
        return f"{self.__class__.__name__}(type='{self.scraper_type}', auth={self.requires_authentication})"


class ScrapingError(Exception):
    """Base exception for scraping-related errors."""
    pass


class AuthenticationError(ScrapingError):
    """Exception raised when authentication fails or is required but not provided."""
    pass


class TargetNotFoundError(ScrapingError):
    """Exception raised when the target (user, subreddit, etc.) is not found."""
    pass


class RateLimitError(ScrapingError):
    """Exception raised when rate limits are exceeded."""
    pass