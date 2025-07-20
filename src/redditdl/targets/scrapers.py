"""
Enhanced Reddit Scrapers

Updated scraper implementations that implement the BaseScraper interface
for consistent behavior and plugin compatibility.
"""

import sys
import time
import logging
from typing import List, Dict, Any, Optional
import praw
import prawcore
from yars.yars import YARS

from .base_scraper import (
    BaseScraper, 
    ScrapingConfig, 
    ScrapingError, 
    AuthenticationError, 
    TargetNotFoundError,
    RateLimitError
)
from .resolver import TargetInfo, TargetType
from ..scrapers import PostMetadata
from ..utils import api_retry, non_api_retry


class EnhancedPrawScraper(BaseScraper):
    """
    Enhanced PRAW scraper implementing BaseScraper interface.
    
    Supports users, subreddits, and authenticated targets like saved/upvoted posts.
    """
    
    def __init__(self, config: ScrapingConfig):
        super().__init__(config)
        self.reddit: Optional[praw.Reddit] = None
        self._authenticated = False
        self._initialize_reddit()
    
    def _initialize_reddit(self):
        """Initialize PRAW Reddit instance."""
        if not self.config.client_id or not self.config.client_secret:
            raise AuthenticationError("PRAW scraper requires client_id and client_secret")
        
        try:
            if self.config.username and self.config.password:
                # Authenticated user session
                self.reddit = praw.Reddit(
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret,
                    username=self.config.username,
                    password=self.config.password,
                    user_agent=self.config.user_agent
                )
                self._authenticated = True
            else:
                # Script application (read-only)
                self.reddit = praw.Reddit(
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret,
                    user_agent=self.config.user_agent
                )
                self._authenticated = False
            
            # Test authentication
            if not self.validate_authentication():
                raise AuthenticationError("Reddit API authentication failed")
                
        except (prawcore.exceptions.OAuthException, prawcore.exceptions.InvalidToken) as e:
            raise AuthenticationError(f"Reddit API authentication failed: {e}")
    
    @property
    def scraper_type(self) -> str:
        return "praw"
    
    @property
    def requires_authentication(self) -> bool:
        return True
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """Check if PRAW can handle this target type."""
        supported_types = {TargetType.USER, TargetType.SUBREDDIT}
        
        # Add authenticated targets if we have user authentication
        if self._authenticated:
            supported_types.update({TargetType.SAVED, TargetType.UPVOTED})
        
        return target_info.target_type in supported_types
    
    def get_supported_target_types(self) -> List[TargetType]:
        """Get supported target types based on authentication level."""
        types = [TargetType.USER, TargetType.SUBREDDIT]
        if self._authenticated:
            types.extend([TargetType.SAVED, TargetType.UPVOTED])
        return types
    
    def validate_authentication(self) -> bool:
        """Validate Reddit API authentication."""
        try:
            if self.reddit is None:
                return False
            
            # Test API access by getting read-only user
            _ = self.reddit.user.me() if self._authenticated else self.reddit.auth.limits
            return True
            
        except (prawcore.exceptions.OAuthException, 
                prawcore.exceptions.InvalidToken,
                prawcore.exceptions.Forbidden) as e:
            self.logger.error(f"Authentication validation failed: {e}")
            return False
        except Exception as e:
            self.logger.warning(f"Authentication validation error: {e}")
            return False
    
    @api_retry(max_retries=3, initial_delay=1.0)
    def fetch_posts(self, target_info: TargetInfo) -> List[PostMetadata]:
        """Fetch posts using PRAW based on target type."""
        if not self.can_handle_target(target_info):
            raise ScrapingError(f"PRAW scraper cannot handle target type: {target_info.target_type}")
        
        if target_info.target_type == TargetType.USER:
            return self._fetch_user_posts(target_info.target_value)
        elif target_info.target_type == TargetType.SUBREDDIT:
            return self._fetch_subreddit_posts(target_info.target_value)
        elif target_info.target_type == TargetType.SAVED:
            return self._fetch_saved_posts()
        elif target_info.target_type == TargetType.UPVOTED:
            return self._fetch_upvoted_posts()
        else:
            raise ScrapingError(f"Unsupported target type: {target_info.target_type}")
    
    def _fetch_user_posts(self, username: str) -> List[PostMetadata]:
        """Fetch posts from a specific user."""
        try:
            user = self.reddit.redditor(username)
            submissions = user.submissions.new(limit=self.config.post_limit)
            return self._process_submissions(submissions)
            
        except prawcore.exceptions.NotFound:
            raise TargetNotFoundError(f"User '{username}' not found")
        except prawcore.exceptions.Forbidden:
            raise AuthenticationError(f"User '{username}' profile is private or restricted")
        except Exception as e:
            raise ScrapingError(f"Failed to fetch posts for user '{username}': {e}")
    
    def _fetch_subreddit_posts(self, subreddit_name: str) -> List[PostMetadata]:
        """Fetch posts from a specific subreddit."""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            submissions = subreddit.new(limit=self.config.post_limit)
            return self._process_submissions(submissions)
            
        except prawcore.exceptions.NotFound:
            raise TargetNotFoundError(f"Subreddit 'r/{subreddit_name}' not found")
        except prawcore.exceptions.Forbidden:
            raise AuthenticationError(f"Subreddit 'r/{subreddit_name}' is private or restricted")
        except Exception as e:
            raise ScrapingError(f"Failed to fetch posts for subreddit 'r/{subreddit_name}': {e}")
    
    def _fetch_saved_posts(self) -> List[PostMetadata]:
        """Fetch user's saved posts (requires authentication)."""
        if not self._authenticated:
            raise AuthenticationError("Saved posts require user authentication")
        
        try:
            saved = self.reddit.user.me().saved(limit=self.config.post_limit)
            return self._process_submissions(saved)
        except Exception as e:
            raise ScrapingError(f"Failed to fetch saved posts: {e}")
    
    def _fetch_upvoted_posts(self) -> List[PostMetadata]:
        """Fetch user's upvoted posts (requires authentication)."""
        if not self._authenticated:
            raise AuthenticationError("Upvoted posts require user authentication")
        
        try:
            upvoted = self.reddit.user.me().upvoted(limit=self.config.post_limit)
            return self._process_submissions(upvoted)
        except Exception as e:
            raise ScrapingError(f"Failed to fetch upvoted posts: {e}")
    
    def _process_submissions(self, submissions) -> List[PostMetadata]:
        """Process PRAW submissions into PostMetadata objects."""
        posts_metadata = []
        
        for submission in submissions:
            try:
                # Convert PRAW submission to raw dict format
                raw_data = self._submission_to_dict(submission)
                
                # Create PostMetadata object
                post_metadata = PostMetadata(raw_data)
                posts_metadata.append(post_metadata)
                
                # Apply rate limiting
                time.sleep(self.get_rate_limit_interval())
                
            except (prawcore.exceptions.OAuthException,
                    prawcore.exceptions.InvalidToken,
                    prawcore.exceptions.Forbidden) as e:
                raise AuthenticationError(f"Authentication failed during operation: {e}")
            except Exception as e:
                self.logger.warning(f"Failed to process post {getattr(submission, 'id', 'unknown')}: {e}")
                continue
        
        return posts_metadata
    
    def _submission_to_dict(self, submission) -> Dict[str, Any]:
        """Convert PRAW submission to dictionary format."""
        try:
            return {
                'id': submission.id,
                'title': submission.title,
                'selftext': getattr(submission, 'selftext', ''),
                'subreddit': str(submission.subreddit),
                'permalink': submission.permalink,
                'url': submission.url,
                'author': str(submission.author) if submission.author else '[deleted]',
                'is_video': getattr(submission, 'is_video', False),
                'created_utc': submission.created_utc,
                'media_url': getattr(submission, 'url_overridden_by_dest', submission.url),
            }
        except Exception as e:
            raise ScrapingError(f"Failed to convert submission to dict: {e}")


class EnhancedYarsScraper(BaseScraper):
    """
    Enhanced YARS scraper implementing BaseScraper interface.
    
    Supports public scraping of users and subreddits without authentication.
    """
    
    def __init__(self, config: ScrapingConfig):
        super().__init__(config)
        self.yars = YARS()
    
    @property
    def scraper_type(self) -> str:
        return "yars"
    
    @property
    def requires_authentication(self) -> bool:
        return False
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """Check if YARS can handle this target type."""
        # YARS can handle users and subreddits publicly
        return target_info.target_type in {TargetType.USER, TargetType.SUBREDDIT}
    
    def get_supported_target_types(self) -> List[TargetType]:
        """Get supported target types for YARS."""
        return [TargetType.USER, TargetType.SUBREDDIT]
    
    def validate_authentication(self) -> bool:
        """YARS doesn't require authentication."""
        return True
    
    @non_api_retry(max_retries=3, initial_delay=6.1)
    def fetch_posts(self, target_info: TargetInfo) -> List[PostMetadata]:
        """Fetch posts using YARS based on target type."""
        if not self.can_handle_target(target_info):
            raise ScrapingError(f"YARS scraper cannot handle target type: {target_info.target_type}")
        
        if target_info.target_type == TargetType.USER:
            return self._fetch_user_posts(target_info.target_value)
        elif target_info.target_type == TargetType.SUBREDDIT:
            return self._fetch_subreddit_posts(target_info.target_value)
        else:
            raise ScrapingError(f"Unsupported target type: {target_info.target_type}")
    
    def _fetch_user_posts(self, username: str) -> List[PostMetadata]:
        """Fetch posts from a specific user using YARS."""
        try:
            posts = self.yars.scrape_user_data(username, limit=self.config.post_limit)
            return self._process_yars_posts(posts)
        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise TargetNotFoundError(f"User '{username}' not found")
            raise ScrapingError(f"Failed to fetch posts for user '{username}': {e}")
    
    def _fetch_subreddit_posts(self, subreddit_name: str) -> List[PostMetadata]:
        """Fetch posts from a specific subreddit using YARS."""
        try:
            posts = self.yars.fetch_subreddit_posts(
                subreddit=subreddit_name,
                limit=self.config.post_limit,
                category='new'  # Use 'new' to match behavior with PRAW
            )
            return self._process_yars_posts(posts)
        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise TargetNotFoundError(f"Subreddit 'r/{subreddit_name}' not found")
            raise ScrapingError(f"Failed to fetch posts for subreddit 'r/{subreddit_name}': {e}")
    
    def _process_yars_posts(self, posts) -> List[PostMetadata]:
        """Process YARS posts into PostMetadata objects."""
        posts_metadata = []
        
        # YARS methods already limit the results, so we don't need manual limiting
        for post in posts:
            try:
                # Create PostMetadata from raw YARS data
                post_metadata = PostMetadata(post)
                posts_metadata.append(post_metadata)
                
                # Apply rate limiting for public access
                time.sleep(self.get_rate_limit_interval())
                
            except Exception as e:
                self.logger.warning(f"Failed to process post {post.get('id', 'unknown')}: {e}")
                continue
        
        return posts_metadata


class ScraperFactory:
    """Factory for creating appropriate scrapers based on configuration and target requirements."""
    
    @staticmethod
    def create_scraper(config: ScrapingConfig, target_info: TargetInfo) -> BaseScraper:
        """
        Create the most appropriate scraper for the given target and configuration.
        
        Args:
            config: Scraping configuration
            target_info: Information about the target to scrape
            
        Returns:
            BaseScraper instance best suited for the target
            
        Raises:
            ScrapingError: If no suitable scraper is available
        """
        # If target requires authentication, we must use PRAW
        if target_info.requires_auth:
            if not config.client_id or not config.client_secret:
                raise ScrapingError(
                    f"Target '{target_info.original_input}' requires Reddit API authentication. "
                    "Please provide client_id and client_secret."
                )
            return EnhancedPrawScraper(config)
        
        # If API credentials are available, prefer PRAW for better rate limits
        if config.client_id and config.client_secret:
            scraper = EnhancedPrawScraper(config)
            if scraper.can_handle_target(target_info):
                return scraper
        
        # Fall back to YARS for public scraping
        scraper = EnhancedYarsScraper(config)
        if scraper.can_handle_target(target_info):
            return scraper
        
        # No suitable scraper found
        raise ScrapingError(
            f"No scraper available for target type '{target_info.target_type.value}'. "
            f"Supported types: user, subreddit"
        )
    
    @staticmethod
    def get_available_scrapers(config: ScrapingConfig) -> List[BaseScraper]:
        """Get list of all available scrapers for the given configuration."""
        scrapers = []
        
        # Always include YARS (no auth required)
        scrapers.append(EnhancedYarsScraper(config))
        
        # Include PRAW if credentials are available
        if config.client_id and config.client_secret:
            try:
                scrapers.append(EnhancedPrawScraper(config))
            except AuthenticationError:
                pass  # Skip if authentication fails
        
        return scrapers