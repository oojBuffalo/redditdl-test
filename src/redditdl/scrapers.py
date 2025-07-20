#!/usr/bin/env python3
"""
Reddit post data scrapers and metadata containers.

This module provides data structures and utilities for handling Reddit post metadata
extracted from PRAW or YARS APIs, including normalization and serialization.
"""

import sys
import time
import random
import functools
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
import praw
import prawcore
import requests
from yars.yars import YARS
from redditdl.utils import get_current_timestamp, api_retry, non_api_retry, auth_retry


@dataclass
class PostMetadata:
    """
    Container for Reddit post metadata extracted from PRAW/YARS raw data.
    
    This dataclass normalizes and stores metadata from Reddit posts, providing
    a consistent interface for accessing post information regardless of the
    data source (PRAW API vs YARS scraper). Now includes comprehensive
    PRD v2.2.1 fields for full Reddit post support.
    """
    
    # Core identification fields
    id: str = ""
    title: str = ""
    selftext: str = ""
    subreddit: str = ""
    permalink: str = ""
    url: str = ""
    author: str = "[deleted]"
    
    # Media and content fields
    is_video: bool = False
    media_url: Optional[str] = None
    date_iso: str = ""
    
    # PRD v2.2.1 Enhanced fields
    score: int = 0
    num_comments: int = 0
    is_nsfw: bool = False
    is_self: bool = False
    domain: str = ""
    awards: List[Dict[str, Any]] = field(default_factory=list)
    media: Optional[Dict[str, Any]] = None
    post_type: str = "link"
    crosspost_parent_id: Optional[str] = None
    gallery_image_urls: List[str] = field(default_factory=list)
    poll_data: Optional[Dict[str, Any]] = None
    created_utc: float = 0.0
    edited: bool = False
    locked: bool = False
    archived: bool = False
    spoiler: bool = False
    stickied: bool = False
    
    def __post_init__(self):
        """
        Post-initialization processing for backward compatibility.
        
        Validates essential fields and handles any necessary normalization.
        """
        # Validate essential fields
        if not self.id:
            raise ValueError("Post ID is required but missing from raw data")
    
    @classmethod
    def from_raw(cls, raw: Dict[str, Any]) -> 'PostMetadata':
        """
        Create PostMetadata from raw Reddit post data (backward compatibility).
        
        Args:
            raw: Raw post data dictionary from PRAW or YARS
            
        Returns:
            PostMetadata instance populated from raw data
            
        Raises:
            ValueError: If essential fields are missing or invalid
        """
        # Extract basic post information with safe defaults
        id_val = str(raw.get('id', '')).strip()
        title_val = str(raw.get('title', '')).strip()
        selftext_val = str(raw.get('selftext', '')).strip()
        subreddit_val = str(raw.get('subreddit', '')).strip()
        permalink_val = str(raw.get('permalink', '')).strip()
        url_val = str(raw.get('url', '')).strip()
        author_val = str(raw.get('author', '[deleted]')).strip()
        
        # Handle video detection
        is_video_val = bool(raw.get('is_video', False))
        
        # Extract media URL - may be different depending on source
        media_url_val = cls._extract_media_url_static(raw)
        
        # Convert timestamp to ISO format
        date_iso_val = cls._convert_timestamp_static(raw.get('created_utc'))
        
        # Extract enhanced fields with safe defaults
        try:
            score_val = int(raw.get('score', 0)) if raw.get('score') is not None else 0
        except (ValueError, TypeError):
            score_val = 0
        
        try:
            num_comments_val = int(raw.get('num_comments', 0)) if raw.get('num_comments') is not None else 0
        except (ValueError, TypeError):
            num_comments_val = 0
        is_nsfw_val = bool(raw.get('over_18', False) or raw.get('is_nsfw', False))
        is_self_val = bool(raw.get('is_self', False))
        domain_val = str(raw.get('domain', '')).strip()
        post_type_val = cls._determine_post_type(raw)
        crosspost_parent_id_val = raw.get('crosspost_parent_list')
        if crosspost_parent_id_val and isinstance(crosspost_parent_id_val, list) and len(crosspost_parent_id_val) > 0:
            crosspost_parent_id_val = crosspost_parent_id_val[0].get('id')
        else:
            crosspost_parent_id_val = raw.get('crosspost_parent_id')
        
        # Handle complex fields
        awards_val = raw.get('all_awardings', []) if raw.get('all_awardings') else []
        if not isinstance(awards_val, list):
            awards_val = []
        
        media_val = raw.get('media') if isinstance(raw.get('media'), dict) else None
        
        gallery_urls_val = cls._extract_gallery_urls(raw)
        poll_data_val = raw.get('poll_data') if isinstance(raw.get('poll_data'), dict) else None
        
        try:
            created_utc_val = float(raw.get('created_utc', 0)) if raw.get('created_utc') is not None else 0.0
        except (ValueError, TypeError):
            created_utc_val = 0.0
        
        # Boolean flags
        edited_val = bool(raw.get('edited', False))
        locked_val = bool(raw.get('locked', False))
        archived_val = bool(raw.get('archived', False))
        spoiler_val = bool(raw.get('spoiler', False))
        stickied_val = bool(raw.get('stickied', False))
        
        return cls(
            id=id_val,
            title=title_val,
            selftext=selftext_val,
            subreddit=subreddit_val,
            permalink=permalink_val,
            url=url_val,
            author=author_val,
            is_video=is_video_val,
            media_url=media_url_val,
            date_iso=date_iso_val,
            score=score_val,
            num_comments=num_comments_val,
            is_nsfw=is_nsfw_val,
            is_self=is_self_val,
            domain=domain_val,
            awards=awards_val,
            media=media_val,
            post_type=post_type_val,
            crosspost_parent_id=crosspost_parent_id_val,
            gallery_image_urls=gallery_urls_val,
            poll_data=poll_data_val,
            created_utc=created_utc_val,
            edited=edited_val,
            locked=locked_val,
            archived=archived_val,
            spoiler=spoiler_val,
            stickied=stickied_val
        )
    
    # Backward compatibility: Use PostMetadata.from_raw(raw_dict) instead of PostMetadata(raw_dict)
    # The dataclass constructor now takes field arguments directly
    
    @staticmethod
    def _extract_media_url_static(raw: Dict[str, Any]) -> Optional[str]:
        """
        Extract media URL from various possible fields in raw data.
        
        Args:
            raw: Raw post data dictionary
            
        Returns:
            Media URL if found, None otherwise
        """
        # Try different possible media URL fields
        potential_urls = [
            raw.get('media_url'),
            raw.get('url_overridden_by_dest'),
            raw.get('url'),
        ]
        
        # Return the first non-empty URL found
        for url in potential_urls:
            if url and isinstance(url, str) and url.strip():
                return url.strip()
        
        return None
    
    @staticmethod
    def _determine_post_type(raw: Dict[str, Any]) -> str:
        """
        Determine the post type based on raw data.
        
        Args:
            raw: Raw post data dictionary
            
        Returns:
            Post type string (link, text, image, video, gallery, poll, crosspost)
        """
        # Check for self post first
        if raw.get('is_self', False):
            return "text"
        
        # Check for gallery (before crosspost to handle crossposted galleries)
        if raw.get('is_gallery', False) or raw.get('gallery_data'):
            return "gallery"
        
        # Check for poll
        if raw.get('poll_data'):
            return "poll"
        
        # Check for video
        if raw.get('is_video', False):
            return "video"
        
        # Check for crosspost
        if raw.get('crosspost_parent_list') or raw.get('crosspost_parent_id'):
            return "crosspost"
        
        # Check for image based on URL or media
        url = raw.get('url', '')
        if url:
            url_lower = url.lower()
            if any(url_lower.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                return "image"
        
        # Default to link
        return "link"
    
    @staticmethod
    def _extract_gallery_urls(raw: Dict[str, Any]) -> List[str]:
        """
        Extract gallery image URLs from raw data.
        
        Args:
            raw: Raw post data dictionary
            
        Returns:
            List of gallery image URLs
        """
        urls = []
        
        # Try to extract from gallery_data
        gallery_data = raw.get('gallery_data', {})
        if isinstance(gallery_data, dict) and 'items' in gallery_data:
            for item in gallery_data['items']:
                if isinstance(item, dict) and 'media_id' in item:
                    media_id = item['media_id']
                    # Construct Reddit gallery URL
                    urls.append(f"https://i.redd.it/{media_id}.jpg")
        
        # Try to extract from media_metadata
        media_metadata = raw.get('media_metadata', {})
        if isinstance(media_metadata, dict):
            for media_id, metadata in media_metadata.items():
                if isinstance(metadata, dict) and 's' in metadata:
                    source = metadata['s']
                    if isinstance(source, dict) and 'u' in source:
                        # Clean up the URL (remove URL encoding)
                        url = source['u'].replace('&amp;', '&')
                        urls.append(url)
        
        return urls
    
    @staticmethod
    def _convert_timestamp_static(created_utc: Any) -> str:
        """
        Convert Unix timestamp to ISO 8601 format.
        
        Args:
            created_utc: Unix timestamp (int, float, or string)
            
        Returns:
            ISO 8601 formatted timestamp string
        """
        if created_utc is None:
            # Use current timestamp as fallback
            return get_current_timestamp()
        
        try:
            # Convert to float if it's a string
            if isinstance(created_utc, str):
                timestamp = float(created_utc)
            else:
                timestamp = float(created_utc)
            
            # Convert Unix timestamp to datetime
            dt = datetime.fromtimestamp(timestamp, timezone.utc)
            
            # Format as ISO 8601 with Z suffix (same format as get_current_timestamp)
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
        except (ValueError, TypeError, OSError):
            # If conversion fails, use current timestamp
            return get_current_timestamp()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert PostMetadata to dictionary for serialization.
        
        Returns:
            Dictionary containing all metadata fields
        """
        return {
            # Core identification fields
            'id': self.id,
            'title': self.title,
            'selftext': self.selftext,
            'date_iso': self.date_iso,
            'subreddit': self.subreddit,
            'permalink': self.permalink,
            'url': self.url,
            'author': self.author,
            
            # Media and content fields
            'is_video': self.is_video,
            'media_url': self.media_url,
            
            # Enhanced PRD v2.2.1 fields
            'score': self.score,
            'num_comments': self.num_comments,
            'is_nsfw': self.is_nsfw,
            'is_self': self.is_self,
            'domain': self.domain,
            'awards': self.awards,
            'media': self.media,
            'post_type': self.post_type,
            'crosspost_parent_id': self.crosspost_parent_id,
            'gallery_image_urls': self.gallery_image_urls,
            'poll_data': self.poll_data,
            'created_utc': self.created_utc,
            'edited': self.edited,
            'locked': self.locked,
            'archived': self.archived,
            'spoiler': self.spoiler,
            'stickied': self.stickied,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PostMetadata':
        """
        Create PostMetadata from dictionary (deserialization).
        
        Args:
            data: Dictionary containing metadata fields
            
        Returns:
            PostMetadata instance
        """
        # Create instance with default values first
        instance = cls.__new__(cls)
        
        # Set all fields from dictionary with defaults
        for field_info in cls.__dataclass_fields__.values():
            field_name = field_info.name
            if field_name in data:
                setattr(instance, field_name, data[field_name])
            else:
                # Use the default value from the field definition
                if field_info.default != field_info.default_factory:
                    setattr(instance, field_name, field_info.default)
                else:
                    setattr(instance, field_name, field_info.default_factory())
        
        # Run post-initialization validation
        instance.__post_init__()
        
        return instance
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"PostMetadata(id='{self.id}', title='{self.title[:50]}...', subreddit='{self.subreddit}')"
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"Post {self.id}: {self.title} (r/{self.subreddit})"


class PrawScraper:
    """
    Reddit scraper using PRAW (Python Reddit API Wrapper) for authenticated API access.
    
    This class handles Reddit API authentication, rate limiting, and fetching user posts
    through the official Reddit API with proper error handling and retry mechanisms.
    """
    
    def __init__(
        self, 
        client_id: str, 
        client_secret: str, 
        user_agent: str,
        login_username: Optional[str] = None,
        login_password: Optional[str] = None,
        sleep_interval: float = 0.7
    ):
        """
        Initialize PrawScraper with Reddit API credentials.
        
        Args:
            client_id: Reddit API client ID
            client_secret: Reddit API client secret
            user_agent: User agent string for API requests
            login_username: Optional username for authenticated access
            login_password: Optional password for authenticated access
            sleep_interval: Time to sleep between API requests (default 0.7s)
            
        Raises:
            SystemExit: If authentication fails with invalid credentials
            ValueError: If required credentials are missing or invalid
        """
        if not client_id or not client_secret or not user_agent:
            print("[ERROR] Missing required Reddit API credentials: client_id, client_secret, and user_agent are required")
            sys.exit(1)
        
        self.sleep_interval = sleep_interval
        
        try:
            # Initialize Reddit instance with script application type
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent
            )
            
            # If login credentials provided, authenticate as user
            if login_username and login_password:
                self.reddit = praw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    username=login_username,
                    password=login_password,
                    user_agent=user_agent
                )
            
            # Test authentication by accessing read-only attribute
            _ = self.reddit.read_only
            
        except (
            prawcore.exceptions.OAuthException,
            prawcore.exceptions.ResponseException,
            prawcore.exceptions.InvalidToken,
            prawcore.exceptions.Forbidden
        ) as e:
            # Authentication failures should cause immediate exit
            print(f"[ERROR] Reddit API authentication failed: {e}")
            print("[ERROR] Please check your client_id, client_secret, username, and password")
            sys.exit(1)
        except Exception as e:
            print(f"[ERROR] Failed to initialize Reddit client: {e}")
            sys.exit(1)
    
    @api_retry(max_retries=3, initial_delay=0.7)
    def fetch_user_posts(self, username: str, limit: int) -> List[PostMetadata]:
        """
        Fetch user posts from Reddit API and convert to PostMetadata objects.
        
        Args:
            username: Reddit username to fetch posts from
            limit: Maximum number of posts to fetch
            
        Returns:
            List of PostMetadata objects containing post information
            
        Raises:
            ValueError: If username is invalid or empty
            prawcore.exceptions.NotFound: If user does not exist
            prawcore.exceptions.Forbidden: If user profile is private/restricted
            SystemExit: If authentication fails during operation
        """
        if not username or not username.strip():
            raise ValueError("Username cannot be empty")
        
        username = username.strip()
        posts_metadata = []
        
        try:
            # Get the redditor (user) object
            redditor = self.reddit.redditor(username)
            
            # Fetch submissions (posts) - new() gets most recent posts
            submissions = redditor.submissions.new(limit=limit)
            
            # Iterate through submissions and convert to PostMetadata
            for submission in submissions:
                try:
                    # Convert PRAW submission to raw dict format
                    raw_data = self._submission_to_dict(submission)
                    
                    # Create PostMetadata object using the new from_raw method
                    post_metadata = PostMetadata.from_raw(raw_data)
                    posts_metadata.append(post_metadata)
                    
                    # Apply rate limiting
                    time.sleep(self.sleep_interval)
                    
                except (
                    prawcore.exceptions.OAuthException,
                    prawcore.exceptions.InvalidToken,
                    prawcore.exceptions.Forbidden
                ) as e:
                    # Authentication errors during operation should cause immediate exit
                    print(f"[ERROR] Authentication failed during operation: {e}")
                    print("[ERROR] Your Reddit API credentials may have expired or been revoked")
                    sys.exit(1)
                except Exception as e:
                    # Log individual post errors but continue processing
                    print(f"[WARN] Failed to process post {getattr(submission, 'id', 'unknown')}: {e}")
                    continue
                    
        except prawcore.exceptions.NotFound:
            # User not found - this is not a retry-able error
            print(f"[ERROR] Reddit user '{username}' not found")
            raise
        except prawcore.exceptions.Forbidden:
            # User profile is private/restricted - this is not a retry-able error
            print(f"[ERROR] Reddit user '{username}' profile is private or restricted")
            raise
        except (
            prawcore.exceptions.OAuthException,
            prawcore.exceptions.InvalidToken
        ) as e:
            # Authentication errors during operation should cause immediate exit
            print(f"[ERROR] Authentication failed during user fetch: {e}")
            print("[ERROR] Your Reddit API credentials may have expired or been revoked")
            sys.exit(1)
        except Exception as e:
            raise ValueError(f"Failed to fetch posts for user '{username}': {e}")
        
        return posts_metadata
    
    def _submission_to_dict(self, submission) -> Dict[str, Any]:
        """
        Convert PRAW submission object to dictionary format compatible with PostMetadata.
        
        Args:
            submission: PRAW submission object
            
        Returns:
            Dictionary containing submission data in PostMetadata-compatible format
        """
        # Extract basic submission attributes
        data = {
            'id': submission.id,
            'title': submission.title,
            'selftext': submission.selftext,
            'subreddit': str(submission.subreddit),
            'permalink': submission.permalink,
            'url': submission.url,
            'author': str(submission.author) if submission.author else '[deleted]',
            'created_utc': submission.created_utc,
            'is_video': submission.is_video,
            
            # Enhanced PRD v2.2.1 fields
            'score': getattr(submission, 'score', 0),
            'num_comments': getattr(submission, 'num_comments', 0),
            'over_18': getattr(submission, 'over_18', False),  # NSFW flag
            'is_self': getattr(submission, 'is_self', False),
            'domain': getattr(submission, 'domain', ''),
            'all_awardings': getattr(submission, 'all_awardings', []),
            'media': getattr(submission, 'media', None),
            'is_gallery': getattr(submission, 'is_gallery', False),
            'gallery_data': getattr(submission, 'gallery_data', None),
            'media_metadata': getattr(submission, 'media_metadata', None),
            'poll_data': getattr(submission, 'poll_data', None),
            'edited': getattr(submission, 'edited', False),
            'locked': getattr(submission, 'locked', False),
            'archived': getattr(submission, 'archived', False),
            'spoiler': getattr(submission, 'spoiler', False),
            'stickied': getattr(submission, 'stickied', False),
            'crosspost_parent_list': getattr(submission, 'crosspost_parent_list', None),
        }
        
        # Handle media URL extraction for different content types
        media_url = None
        
        # Check for direct URL (images, gifs, etc.)
        if hasattr(submission, 'url') and submission.url:
            media_url = submission.url
        
        # Check for Reddit-hosted video
        if submission.is_video and hasattr(submission, 'media') and submission.media:
            if 'reddit_video' in submission.media:
                media_url = submission.media['reddit_video'].get('fallback_url')
        
        # Check for preview images
        if not media_url and hasattr(submission, 'preview') and submission.preview:
            if 'images' in submission.preview and submission.preview['images']:
                media_url = submission.preview['images'][0].get('source', {}).get('url')
        
        # Set media_url if found
        if media_url:
            data['media_url'] = media_url
            data['url_overridden_by_dest'] = media_url
        
        return data 


class YarsScraper:
    """
    Reddit scraper using YARS for non-API mode.
    
    This scraper fetches public user posts using YARS without requiring
    Reddit API authentication, adhering to specified rate limits for
    unauthenticated access.
    """
    
    def __init__(self, sleep_interval: float = 6.1):
        """
        Initialize YarsScraper with YARS client.
        
        Args:
            sleep_interval: Delay between requests in seconds (default 6.1s)
                          for responsible scraping without API rate limits
        """
        self.sleep_interval = sleep_interval
        self.yars = YARS()
    
    @non_api_retry(max_retries=3, initial_delay=6.1)
    def fetch_user_posts(self, username: str, limit: int) -> List[PostMetadata]:
        """
        Fetch public user posts using YARS.
        
        Args:
            username: Reddit username to fetch posts from
            limit: Maximum number of posts to fetch
            
        Returns:
            List of PostMetadata objects containing post information
            
        Raises:
            ValueError: If username is invalid or empty
        """
        if not username or not username.strip():
            raise ValueError("Username cannot be empty")
        
        username = username.strip()
        posts_metadata = []
        
        try:
            # Use YARS to fetch user submissions
            posts = self.yars.user_posts(username)
            
            # Limit the number of posts processed
            count = 0
            for post in posts:
                if count >= limit:
                    break
                
                try:
                    # Create PostMetadata from raw YARS data using the new from_raw method
                    post_metadata = PostMetadata.from_raw(post)
                    posts_metadata.append(post_metadata)
                    count += 1
                    
                    # Apply rate limiting for non-API access
                    time.sleep(self.sleep_interval)
                    
                except Exception as e:
                    # Log individual post errors but continue processing
                    print(f"[WARN] Failed to process post {post.get('id', 'unknown')}: {e}")
                    continue
                    
        except Exception as e:
            raise ValueError(f"Failed to fetch posts for user '{username}' using YARS: {e}")
        
        return posts_metadata 