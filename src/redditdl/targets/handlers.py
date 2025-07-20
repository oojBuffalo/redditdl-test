"""
Target Handlers System

Specialized handlers for different Reddit target types with advanced processing
capabilities including listing support, pagination, and concurrent processing.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union, AsyncGenerator
from dataclasses import dataclass
from enum import Enum
import logging

from .resolver import TargetInfo, TargetType
from .base_scraper import BaseScraper, ScrapingConfig
from .scrapers import ScraperFactory, ScrapingError, AuthenticationError, TargetNotFoundError
from ..scrapers import PostMetadata


class ListingType(Enum):
    """Enumeration of Reddit listing types."""
    HOT = "hot"
    NEW = "new"
    TOP = "top"
    CONTROVERSIAL = "controversial"
    RISING = "rising"


class TimePeriod(Enum):
    """Enumeration of time periods for top/controversial listings."""
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"


@dataclass
class TargetProcessingResult:
    """
    Result of processing a single target.
    
    Attributes:
        target_info: Information about the processed target
        posts: List of posts retrieved from the target
        success: Whether processing was successful
        error_message: Error message if processing failed
        processing_time: Time taken to process the target in seconds
        metadata: Additional metadata about the processing
    """
    target_info: TargetInfo
    posts: List[PostMetadata]
    success: bool
    error_message: Optional[str] = None
    processing_time: float = 0.0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass 
class BatchProcessingConfig:
    """Configuration for batch target processing."""
    max_concurrent: int = 3
    rate_limit_delay: float = 1.0
    retry_attempts: int = 3
    retry_delay: float = 2.0
    timeout_per_target: float = 300.0  # 5 minutes per target
    fail_fast: bool = False  # Stop all processing if one target fails


class BaseTargetHandler(ABC):
    """
    Abstract base class for target handlers.
    
    Handles target-specific processing logic including listing support,
    pagination, and specialized retrieval methods.
    """
    
    def __init__(self, config: ScrapingConfig):
        """
        Initialize the target handler.
        
        Args:
            config: Scraping configuration for the handler
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """
        Check if this handler can process the given target.
        
        Args:
            target_info: Information about the target
            
        Returns:
            True if this handler can process the target, False otherwise
        """
        pass
    
    @abstractmethod
    async def process_target(self, target_info: TargetInfo) -> TargetProcessingResult:
        """
        Process a single target and return results.
        
        Args:
            target_info: Information about the target to process
            
        Returns:
            TargetProcessingResult containing posts and processing metadata
        """
        pass
    
    @property
    @abstractmethod
    def supported_target_types(self) -> List[TargetType]:
        """Get list of target types supported by this handler."""
        pass
    
    def get_scraper(self, target_info: TargetInfo) -> BaseScraper:
        """
        Get appropriate scraper for the target.
        
        Args:
            target_info: Information about the target
            
        Returns:
            BaseScraper instance for the target
        """
        return ScraperFactory.create_scraper(self.config, target_info)


class UserTargetHandler(BaseTargetHandler):
    """
    Handler for user targets with enhanced user profile processing.
    
    Supports different user post listing types and user-specific metadata.
    """
    
    @property
    def supported_target_types(self) -> List[TargetType]:
        return [TargetType.USER]
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """Check if this handler can process user targets."""
        return target_info.target_type == TargetType.USER
    
    async def process_target(self, target_info: TargetInfo) -> TargetProcessingResult:
        """
        Process a user target with enhanced user profile handling.
        
        Args:
            target_info: User target information
            
        Returns:
            TargetProcessingResult with user posts and metadata
        """
        start_time = time.time()
        result = TargetProcessingResult(
            target_info=target_info,
            posts=[],
            success=False
        )
        
        try:
            self.logger.info(f"Processing user target: {target_info.target_value}")
            
            # Get appropriate scraper
            scraper = self.get_scraper(target_info)
            
            # Fetch user posts
            posts = scraper.fetch_posts(target_info)
            
            # Add user-specific metadata
            user_metadata = await self._gather_user_metadata(target_info, scraper)
            
            result.posts = posts
            result.success = True
            result.metadata.update({
                'target_type': 'user',
                'username': target_info.target_value,
                'post_count': len(posts),
                'scraper_type': scraper.scraper_type,
                'user_metadata': user_metadata
            })
            
            self.logger.info(f"Successfully processed user {target_info.target_value}: {len(posts)} posts")
            
        except (TargetNotFoundError, AuthenticationError, ScrapingError) as e:
            result.error_message = str(e)
            self.logger.error(f"Failed to process user {target_info.target_value}: {e}")
        except Exception as e:
            result.error_message = f"Unexpected error: {e}"
            self.logger.error(f"Unexpected error processing user {target_info.target_value}: {e}")
        
        result.processing_time = time.time() - start_time
        return result
    
    async def _gather_user_metadata(self, target_info: TargetInfo, scraper: BaseScraper) -> Dict[str, Any]:
        """
        Gather additional metadata about the user.
        
        Args:
            target_info: User target information
            scraper: Scraper instance for additional queries
            
        Returns:
            Dictionary containing user metadata
        """
        metadata = {
            'username': target_info.target_value,
            'target_original': target_info.original_input
        }
        
        # Add scraper-specific metadata if available
        try:
            if hasattr(scraper, 'reddit') and scraper.reddit:
                # PRAW-specific user metadata
                user = scraper.reddit.redditor(target_info.target_value)
                metadata.update({
                    'account_created': getattr(user, 'created_utc', None),
                    'comment_karma': getattr(user, 'comment_karma', None),
                    'link_karma': getattr(user, 'link_karma', None),
                    'is_verified': getattr(user, 'verified', None)
                })
        except Exception as e:
            self.logger.debug(f"Could not gather extended user metadata: {e}")
        
        return metadata


class SubredditTargetHandler(BaseTargetHandler):
    """
    Handler for subreddit targets with comprehensive listing support.
    
    Supports hot, new, top, controversial, and rising listings with time periods.
    """
    
    @property
    def supported_target_types(self) -> List[TargetType]:
        return [TargetType.SUBREDDIT]
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """Check if this handler can process subreddit targets."""
        return target_info.target_type == TargetType.SUBREDDIT
    
    async def process_target(self, target_info: TargetInfo) -> TargetProcessingResult:
        """
        Process a subreddit target with listing support.
        
        Args:
            target_info: Subreddit target information
            
        Returns:
            TargetProcessingResult with subreddit posts and metadata
        """
        start_time = time.time()
        result = TargetProcessingResult(
            target_info=target_info,
            posts=[],
            success=False
        )
        
        try:
            self.logger.info(f"Processing subreddit target: r/{target_info.target_value}")
            
            # Get listing configuration from target metadata
            listing_type = self._get_listing_type(target_info)
            time_period = self._get_time_period(target_info)
            
            # Get appropriate scraper
            scraper = self.get_scraper(target_info)
            
            # Fetch subreddit posts based on listing type
            posts = await self._fetch_subreddit_posts(scraper, target_info, listing_type, time_period)
            
            # Add subreddit-specific metadata
            subreddit_metadata = await self._gather_subreddit_metadata(target_info, scraper)
            
            result.posts = posts
            result.success = True
            result.metadata.update({
                'target_type': 'subreddit',
                'subreddit': target_info.target_value,
                'listing_type': listing_type.value,
                'time_period': time_period.value if time_period else None,
                'post_count': len(posts),
                'scraper_type': scraper.scraper_type,
                'subreddit_metadata': subreddit_metadata
            })
            
            self.logger.info(f"Successfully processed subreddit r/{target_info.target_value}: {len(posts)} posts")
            
        except (TargetNotFoundError, AuthenticationError, ScrapingError) as e:
            result.error_message = str(e)
            self.logger.error(f"Failed to process subreddit r/{target_info.target_value}: {e}")
        except Exception as e:
            result.error_message = f"Unexpected error: {e}"
            self.logger.error(f"Unexpected error processing subreddit r/{target_info.target_value}: {e}")
        
        result.processing_time = time.time() - start_time
        return result
    
    def _get_listing_type(self, target_info: TargetInfo) -> ListingType:
        """Get listing type from target metadata or default to new."""
        listing_str = target_info.metadata.get('listing_type', 'new')
        try:
            return ListingType(listing_str.lower())
        except ValueError:
            self.logger.warning(f"Unknown listing type '{listing_str}', defaulting to 'new'")
            return ListingType.NEW
    
    def _get_time_period(self, target_info: TargetInfo) -> Optional[TimePeriod]:
        """Get time period from target metadata if applicable."""
        time_period_str = target_info.metadata.get('time_period')
        if time_period_str:
            try:
                return TimePeriod(time_period_str.lower())
            except ValueError:
                self.logger.warning(f"Unknown time period '{time_period_str}', ignoring")
        return None
    
    async def _fetch_subreddit_posts(self, scraper: BaseScraper, target_info: TargetInfo, 
                                   listing_type: ListingType, time_period: Optional[TimePeriod]) -> List[PostMetadata]:
        """
        Fetch subreddit posts based on listing type and time period.
        
        Args:
            scraper: Scraper instance to use
            target_info: Target information
            listing_type: Type of listing to fetch
            time_period: Time period for top/controversial listings
            
        Returns:
            List of PostMetadata objects
        """
        # If using enhanced listing features, try to use PRAW directly
        if hasattr(scraper, 'reddit') and scraper.reddit:
            return await self._fetch_with_praw_listings(scraper, target_info, listing_type, time_period)
        else:
            # Fall back to basic fetch for YARS or other scrapers
            return scraper.fetch_posts(target_info)
    
    async def _fetch_with_praw_listings(self, scraper: BaseScraper, target_info: TargetInfo,
                                      listing_type: ListingType, time_period: Optional[TimePeriod]) -> List[PostMetadata]:
        """
        Fetch posts using PRAW with specific listing types.
        
        Args:
            scraper: PRAW scraper instance
            target_info: Target information
            listing_type: Type of listing to fetch
            time_period: Time period for time-based listings
            
        Returns:
            List of PostMetadata objects
        """
        subreddit = scraper.reddit.subreddit(target_info.target_value)
        
        # Get appropriate submission generator based on listing type
        if listing_type == ListingType.HOT:
            submissions = subreddit.hot(limit=self.config.post_limit)
        elif listing_type == ListingType.NEW:
            submissions = subreddit.new(limit=self.config.post_limit)
        elif listing_type == ListingType.TOP:
            time_filter = time_period.value if time_period else TimePeriod.ALL.value
            submissions = subreddit.top(time_filter=time_filter, limit=self.config.post_limit)
        elif listing_type == ListingType.CONTROVERSIAL:
            time_filter = time_period.value if time_period else TimePeriod.ALL.value
            submissions = subreddit.controversial(time_filter=time_filter, limit=self.config.post_limit)
        elif listing_type == ListingType.RISING:
            submissions = subreddit.rising(limit=self.config.post_limit)
        else:
            # Default to new
            submissions = subreddit.new(limit=self.config.post_limit)
        
        # Process submissions similar to existing PRAW scraper
        posts = []
        for submission in submissions:
            try:
                # Convert submission to PostMetadata format
                raw_data = {
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
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'is_nsfw': submission.over_18,
                    'is_self': submission.is_self
                }
                
                post_metadata = PostMetadata.from_raw(raw_data)
                posts.append(post_metadata)
                
                # Apply rate limiting
                time.sleep(scraper.get_rate_limit_interval())
                
            except Exception as e:
                self.logger.warning(f"Failed to process post {submission.id}: {e}")
                continue
        
        return posts
    
    async def _gather_subreddit_metadata(self, target_info: TargetInfo, scraper: BaseScraper) -> Dict[str, Any]:
        """
        Gather additional metadata about the subreddit.
        
        Args:
            target_info: Subreddit target information
            scraper: Scraper instance for additional queries
            
        Returns:
            Dictionary containing subreddit metadata
        """
        metadata = {
            'subreddit': target_info.target_value,
            'target_original': target_info.original_input
        }
        
        # Add scraper-specific metadata if available
        try:
            if hasattr(scraper, 'reddit') and scraper.reddit:
                # PRAW-specific subreddit metadata
                subreddit = scraper.reddit.subreddit(target_info.target_value)
                metadata.update({
                    'display_name': getattr(subreddit, 'display_name', None),
                    'title': getattr(subreddit, 'title', None),
                    'description': getattr(subreddit, 'description', None),
                    'subscribers': getattr(subreddit, 'subscribers', None),
                    'created_utc': getattr(subreddit, 'created_utc', None),
                    'over18': getattr(subreddit, 'over18', None),
                    'subreddit_type': getattr(subreddit, 'subreddit_type', None)
                })
        except Exception as e:
            self.logger.debug(f"Could not gather extended subreddit metadata: {e}")
        
        return metadata


class SavedPostsHandler(BaseTargetHandler):
    """
    Handler for saved posts with pagination support for large collections.
    
    Supports authenticated retrieval of user's saved posts with efficient pagination.
    """
    
    @property
    def supported_target_types(self) -> List[TargetType]:
        return [TargetType.SAVED]
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """Check if this handler can process saved posts targets."""
        return target_info.target_type == TargetType.SAVED
    
    async def process_target(self, target_info: TargetInfo) -> TargetProcessingResult:
        """
        Process saved posts target with pagination support.
        
        Args:
            target_info: Saved posts target information
            
        Returns:
            TargetProcessingResult with saved posts and metadata
        """
        start_time = time.time()
        result = TargetProcessingResult(
            target_info=target_info,
            posts=[],
            success=False
        )
        
        try:
            self.logger.info("Processing saved posts target")
            
            # Get appropriate scraper (must be authenticated)
            scraper = self.get_scraper(target_info)
            
            # Verify authentication
            if not scraper.requires_authentication:
                raise AuthenticationError("Saved posts require authentication")
            
            # Fetch saved posts with pagination
            posts = await self._fetch_saved_posts_paginated(scraper)
            
            result.posts = posts
            result.success = True
            result.metadata.update({
                'target_type': 'saved',
                'post_count': len(posts),
                'scraper_type': scraper.scraper_type,
                'requires_auth': True
            })
            
            self.logger.info(f"Successfully processed saved posts: {len(posts)} posts")
            
        except (TargetNotFoundError, AuthenticationError, ScrapingError) as e:
            result.error_message = str(e)
            self.logger.error(f"Failed to process saved posts: {e}")
        except Exception as e:
            result.error_message = f"Unexpected error: {e}"
            self.logger.error(f"Unexpected error processing saved posts: {e}")
        
        result.processing_time = time.time() - start_time
        return result
    
    async def _fetch_saved_posts_paginated(self, scraper: BaseScraper) -> List[PostMetadata]:
        """
        Fetch saved posts with pagination support for large collections.
        
        Args:
            scraper: Authenticated scraper instance
            
        Returns:
            List of PostMetadata objects
        """
        posts = []
        
        if hasattr(scraper, 'reddit') and scraper.reddit:
            # Use PRAW pagination for efficient retrieval
            saved_generator = scraper.reddit.user.me().saved(limit=None)  # Get all
            
            collected = 0
            for item in saved_generator:
                if collected >= self.config.post_limit:
                    break
                
                # Only process submissions (posts), not comments
                if hasattr(item, 'subreddit'):  # It's a submission
                    try:
                        raw_data = {
                            'id': item.id,
                            'title': item.title,
                            'selftext': getattr(item, 'selftext', ''),
                            'subreddit': str(item.subreddit),
                            'permalink': item.permalink,
                            'url': item.url,
                            'author': str(item.author) if item.author else '[deleted]',
                            'is_video': getattr(item, 'is_video', False),
                            'created_utc': item.created_utc,
                            'media_url': getattr(item, 'url_overridden_by_dest', item.url),
                            'score': item.score,
                            'num_comments': item.num_comments,
                            'is_nsfw': item.over_18,
                            'is_self': item.is_self
                        }
                        
                        post_metadata = PostMetadata.from_raw(raw_data)
                        posts.append(post_metadata)
                        collected += 1
                        
                        # Apply rate limiting
                        time.sleep(scraper.get_rate_limit_interval())
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to process saved post {item.id}: {e}")
                        continue
        else:
            # Fall back to basic scraper fetch method
            posts = scraper.fetch_posts(target_info)
        
        return posts


class UpvotedPostsHandler(BaseTargetHandler):
    """
    Handler for upvoted posts with pagination support for large collections.
    
    Supports authenticated retrieval of user's upvoted posts with efficient pagination.
    """
    
    @property
    def supported_target_types(self) -> List[TargetType]:
        return [TargetType.UPVOTED]
    
    def can_handle_target(self, target_info: TargetInfo) -> bool:
        """Check if this handler can process upvoted posts targets."""
        return target_info.target_type == TargetType.UPVOTED
    
    async def process_target(self, target_info: TargetInfo) -> TargetProcessingResult:
        """
        Process upvoted posts target with pagination support.
        
        Args:
            target_info: Upvoted posts target information
            
        Returns:
            TargetProcessingResult with upvoted posts and metadata
        """
        start_time = time.time()
        result = TargetProcessingResult(
            target_info=target_info,
            posts=[],
            success=False
        )
        
        try:
            self.logger.info("Processing upvoted posts target")
            
            # Get appropriate scraper (must be authenticated)
            scraper = self.get_scraper(target_info)
            
            # Verify authentication
            if not scraper.requires_authentication:
                raise AuthenticationError("Upvoted posts require authentication")
            
            # Fetch upvoted posts with pagination
            posts = await self._fetch_upvoted_posts_paginated(scraper)
            
            result.posts = posts
            result.success = True
            result.metadata.update({
                'target_type': 'upvoted',
                'post_count': len(posts),
                'scraper_type': scraper.scraper_type,
                'requires_auth': True
            })
            
            self.logger.info(f"Successfully processed upvoted posts: {len(posts)} posts")
            
        except (TargetNotFoundError, AuthenticationError, ScrapingError) as e:
            result.error_message = str(e)
            self.logger.error(f"Failed to process upvoted posts: {e}")
        except Exception as e:
            result.error_message = f"Unexpected error: {e}"
            self.logger.error(f"Unexpected error processing upvoted posts: {e}")
        
        result.processing_time = time.time() - start_time
        return result
    
    async def _fetch_upvoted_posts_paginated(self, scraper: BaseScraper) -> List[PostMetadata]:
        """
        Fetch upvoted posts with pagination support for large collections.
        
        Args:
            scraper: Authenticated scraper instance
            
        Returns:
            List of PostMetadata objects
        """
        posts = []
        
        if hasattr(scraper, 'reddit') and scraper.reddit:
            # Use PRAW pagination for efficient retrieval
            upvoted_generator = scraper.reddit.user.me().upvoted(limit=None)  # Get all
            
            collected = 0
            for item in upvoted_generator:
                if collected >= self.config.post_limit:
                    break
                
                # Only process submissions (posts), not comments
                if hasattr(item, 'subreddit'):  # It's a submission
                    try:
                        raw_data = {
                            'id': item.id,
                            'title': item.title,
                            'selftext': getattr(item, 'selftext', ''),
                            'subreddit': str(item.subreddit),
                            'permalink': item.permalink,
                            'url': item.url,
                            'author': str(item.author) if item.author else '[deleted]',
                            'is_video': getattr(item, 'is_video', False),
                            'created_utc': item.created_utc,
                            'media_url': getattr(item, 'url_overridden_by_dest', item.url),
                            'score': item.score,
                            'num_comments': item.num_comments,
                            'is_nsfw': item.over_18,
                            'is_self': item.is_self
                        }
                        
                        post_metadata = PostMetadata.from_raw(raw_data)
                        posts.append(post_metadata)
                        collected += 1
                        
                        # Apply rate limiting
                        time.sleep(scraper.get_rate_limit_interval())
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to process upvoted post {item.id}: {e}")
                        continue
        else:
            # Fall back to basic scraper fetch method
            posts = scraper.fetch_posts(target_info)
        
        return posts


class TargetHandlerRegistry:
    """
    Registry for target handlers with automatic handler selection.
    
    Manages handler registration and provides automatic handler selection
    based on target type and capabilities.
    """
    
    def __init__(self):
        """Initialize the handler registry with default handlers."""
        self.handlers: List[BaseTargetHandler] = []
        self.logger = logging.getLogger(__name__)
    
    def register_handler(self, handler_class: type, config: ScrapingConfig) -> None:
        """
        Register a target handler with the registry.
        
        Args:
            handler_class: Handler class to register
            config: Configuration for handler initialization
        """
        handler = handler_class(config)
        self.handlers.append(handler)
        self.logger.debug(f"Registered handler: {handler_class.__name__}")
    
    def get_handler(self, target_info: TargetInfo) -> Optional[BaseTargetHandler]:
        """
        Get the most appropriate handler for a target.
        
        Args:
            target_info: Information about the target
            
        Returns:
            BaseTargetHandler instance or None if no suitable handler found
        """
        for handler in self.handlers:
            if handler.can_handle_target(target_info):
                return handler
        
        self.logger.warning(f"No handler found for target type: {target_info.target_type}")
        return None
    
    def register_default_handlers(self, config: ScrapingConfig) -> None:
        """
        Register all default target handlers.
        
        Args:
            config: Configuration for handler initialization
        """
        default_handlers = [
            UserTargetHandler,
            SubredditTargetHandler,
            SavedPostsHandler,
            UpvotedPostsHandler
        ]
        
        for handler_class in default_handlers:
            self.register_handler(handler_class, config)
        
        self.logger.info(f"Registered {len(default_handlers)} default handlers")
    
    def get_supported_target_types(self) -> List[TargetType]:
        """Get all target types supported by registered handlers."""
        supported_types = set()
        for handler in self.handlers:
            supported_types.update(handler.supported_target_types)
        return list(supported_types)


class BatchTargetProcessor:
    """
    Batch processor for handling multiple targets concurrently.
    
    Provides concurrent processing of multiple targets with rate limiting,
    error isolation, and progress tracking.
    """
    
    def __init__(self, config: BatchProcessingConfig, scraping_config: ScrapingConfig):
        """
        Initialize the batch processor.
        
        Args:
            config: Batch processing configuration
            scraping_config: Scraping configuration for handlers
        """
        self.config = config
        self.scraping_config = scraping_config
        self.registry = TargetHandlerRegistry()
        self.registry.register_default_handlers(scraping_config)
        self.logger = logging.getLogger(__name__)
    
    async def process_targets(self, target_infos: List[TargetInfo]) -> List[TargetProcessingResult]:
        """
        Process multiple targets concurrently with error isolation.
        
        Args:
            target_infos: List of targets to process
            
        Returns:
            List of TargetProcessingResult objects for each target
        """
        if not target_infos:
            return []
        
        self.logger.info(f"Starting batch processing of {len(target_infos)} targets")
        
        # Create semaphore for concurrent processing
        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        # Create tasks for each target
        tasks = []
        for target_info in target_infos:
            task = asyncio.create_task(
                self._process_single_target_with_semaphore(semaphore, target_info)
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error in batch processing: {e}")
            results = [TargetProcessingResult(
                target_info=target_info,
                posts=[],
                success=False,
                error_message=f"Batch processing error: {e}"
            ) for target_info in target_infos]
        
        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Task raised an exception
                error_result = TargetProcessingResult(
                    target_info=target_infos[i],
                    posts=[],
                    success=False,
                    error_message=f"Task exception: {result}"
                )
                processed_results.append(error_result)
            else:
                processed_results.append(result)
        
        successful_count = sum(1 for r in processed_results if r.success)
        self.logger.info(f"Batch processing completed: {successful_count}/{len(target_infos)} targets successful")
        
        return processed_results
    
    async def _process_single_target_with_semaphore(self, semaphore: asyncio.Semaphore, 
                                                  target_info: TargetInfo) -> TargetProcessingResult:
        """
        Process a single target with semaphore rate limiting.
        
        Args:
            semaphore: Semaphore for concurrent access control
            target_info: Target to process
            
        Returns:
            TargetProcessingResult for the target
        """
        async with semaphore:
            # Apply rate limiting delay
            await asyncio.sleep(self.config.rate_limit_delay)
            
            # Get appropriate handler
            handler = self.registry.get_handler(target_info)
            if not handler:
                return TargetProcessingResult(
                    target_info=target_info,
                    posts=[],
                    success=False,
                    error_message=f"No handler available for target type: {target_info.target_type}"
                )
            
            # Process target with timeout
            try:
                result = await asyncio.wait_for(
                    handler.process_target(target_info),
                    timeout=self.config.timeout_per_target
                )
                return result
            except asyncio.TimeoutError:
                return TargetProcessingResult(
                    target_info=target_info,
                    posts=[],
                    success=False,
                    error_message=f"Target processing timed out after {self.config.timeout_per_target} seconds"
                )
            except Exception as e:
                return TargetProcessingResult(
                    target_info=target_info,
                    posts=[],
                    success=False,
                    error_message=f"Handler error: {e}"
                )