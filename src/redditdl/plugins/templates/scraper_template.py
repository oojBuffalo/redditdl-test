"""
Scraper Plugin Template

This template provides a complete example for creating custom scraper plugins
for RedditDL. Copy this file and modify it to create your own scraper that
can fetch content from different sources or APIs.

A scraper plugin allows you to:
- Add support for new content sources (beyond Reddit)
- Implement custom authentication methods
- Handle source-specific rate limiting
- Parse source-specific data formats
- Integrate with third-party APIs
"""

import asyncio
import aiohttp
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from core.plugins.hooks import BaseScraper

# Plugin metadata - customize this for your plugin
__plugin_info__ = {
    'name': 'example_scraper',
    'version': '1.0.0',
    'description': 'Example scraper plugin template for custom data sources',
    'author': 'Your Name',
    'source_types': ['example_api', 'custom_feed'],
    'requires_auth': False,
    'rate_limit': 1.0  # Seconds between requests
}


class ExampleScraper(BaseScraper):
    """
    Example scraper plugin implementation.
    
    This scraper demonstrates how to:
    - Check if a source can be scraped
    - Implement async scraping with rate limiting
    - Handle authentication and configuration
    - Parse and normalize data from external sources
    - Provide comprehensive error handling
    """
    
    # Plugin priority (higher numbers = higher priority)
    priority = 100
    
    def __init__(self):
        """Initialize the scraper with default configuration."""
        self.scraper_name = 'example_scraper'
        self.supported_sources = ['example_api', 'custom_feed']
        self.rate_limit = 1.0  # Default rate limit in seconds
        self.session = None
        self.last_request_time = 0
        
        # Configuration schema for this scraper
        self.config_schema = {
            'api_key': {
                'type': 'string',
                'default': None,
                'description': 'API key for authentication (if required)',
                'sensitive': True
            },
            'base_url': {
                'type': 'string',
                'default': 'https://api.example.com',
                'description': 'Base URL for the API endpoint'
            },
            'timeout': {
                'type': 'number',
                'default': 30.0,
                'description': 'Request timeout in seconds'
            },
            'max_retries': {
                'type': 'integer',
                'default': 3,
                'description': 'Maximum number of retry attempts'
            },
            'rate_limit': {
                'type': 'number',
                'default': 1.0,
                'description': 'Minimum seconds between requests'
            },
            'user_agent': {
                'type': 'string',
                'default': 'RedditDL-ExampleScraper/1.0',
                'description': 'User agent string for requests'
            },
            'verify_ssl': {
                'type': 'boolean',
                'default': True,
                'description': 'Whether to verify SSL certificates'
            }
        }
    
    def can_scrape(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        """
        Check if this scraper can handle the given source type.
        
        Args:
            source_type: The type of source to scrape
            source_config: Configuration for the source
            
        Returns:
            bool: True if this scraper can handle the source
        """
        try:
            # Check if source type is supported
            if source_type not in self.supported_sources:
                return False
            
            # Validate required configuration
            if source_type == 'example_api':
                # Example API requires a base URL
                base_url = source_config.get('base_url')
                if not base_url or not isinstance(base_url, str):
                    return False
            
            elif source_type == 'custom_feed':
                # Custom feed requires a feed URL
                feed_url = source_config.get('feed_url')
                if not feed_url or not isinstance(feed_url, str):
                    return False
            
            return True
            
        except Exception as e:
            print(f"Error checking scraper compatibility: {e}")
            return False
    
    async def scrape(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scrape data from the configured source.
        
        Args:
            source_config: Configuration for the scraping operation
            
        Returns:
            List[Dict[str, Any]]: List of scraped posts/items
        """
        try:
            # Extract configuration
            source_type = source_config.get('source_type', 'example_api')
            
            # Initialize session if needed
            if self.session is None:
                await self._initialize_session(source_config)
            
            # Route to appropriate scraping method
            if source_type == 'example_api':
                return await self._scrape_api(source_config)
            elif source_type == 'custom_feed':
                return await self._scrape_feed(source_config)
            else:
                print(f"Unsupported source type: {source_type}")
                return []
                
        except Exception as e:
            print(f"Scraping failed: {e}")
            return []
        finally:
            # Clean up session if needed
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
    
    async def _initialize_session(self, config: Dict[str, Any]) -> None:
        """Initialize HTTP session with configuration."""
        timeout = aiohttp.ClientTimeout(total=config.get('timeout', 30.0))
        
        headers = {
            'User-Agent': config.get('user_agent', 'RedditDL-ExampleScraper/1.0')
        }
        
        # Add authentication header if API key is provided
        api_key = config.get('api_key')
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'
        
        connector = aiohttp.TCPConnector(
            verify_ssl=config.get('verify_ssl', True)
        )
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers=headers,
            connector=connector
        )
        
        # Update rate limit
        self.rate_limit = config.get('rate_limit', 1.0)
    
    async def _scrape_api(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape data from an API endpoint."""
        base_url = config.get('base_url', 'https://api.example.com')
        endpoint = config.get('endpoint', '/posts')
        params = config.get('params', {})
        
        url = f"{base_url.rstrip('/')}{endpoint}"
        
        try:
            # Apply rate limiting
            await self._apply_rate_limit()
            
            # Make request with retries
            data = await self._make_request_with_retries(url, params, config)
            
            # Parse and normalize the response
            return self._parse_api_response(data, config)
            
        except Exception as e:
            print(f"API scraping failed: {e}")
            return []
    
    async def _scrape_feed(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape data from a custom feed."""
        feed_url = config.get('feed_url')
        
        try:
            # Apply rate limiting
            await self._apply_rate_limit()
            
            # Make request with retries
            data = await self._make_request_with_retries(feed_url, {}, config)
            
            # Parse and normalize the response
            return self._parse_feed_response(data, config)
            
        except Exception as e:
            print(f"Feed scraping failed: {e}")
            return []
    
    async def _make_request_with_retries(self, url: str, params: Dict[str, Any], 
                                       config: Dict[str, Any]) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        max_retries = config.get('max_retries', 3)
        
        for attempt in range(max_retries + 1):
            try:
                async with self.session.get(url, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
                    
            except aiohttp.ClientError as e:
                if attempt == max_retries:
                    raise
                
                print(f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception("Max retries exceeded")
    
    async def _apply_rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        now = asyncio.get_event_loop().time()
        elapsed = now - self.last_request_time
        
        if elapsed < self.rate_limit:
            sleep_time = self.rate_limit - elapsed
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = asyncio.get_event_loop().time()
    
    def _parse_api_response(self, data: Dict[str, Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse API response into normalized post format."""
        posts = []
        
        # Assume the API returns data in a 'posts' or 'items' field
        items = data.get('posts', data.get('items', []))
        
        for item in items:
            try:
                # Normalize the item into RedditDL post format
                post = self._normalize_post(item, 'api')
                if post:
                    posts.append(post)
            except Exception as e:
                print(f"Error parsing item: {e}")
                continue
        
        return posts
    
    def _parse_feed_response(self, data: Dict[str, Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse feed response into normalized post format."""
        posts = []
        
        # Assume the feed returns data in a 'feed' or 'entries' field
        entries = data.get('feed', data.get('entries', []))
        
        for entry in entries:
            try:
                # Normalize the entry into RedditDL post format
                post = self._normalize_post(entry, 'feed')
                if post:
                    posts.append(post)
            except Exception as e:
                print(f"Error parsing entry: {e}")
                continue
        
        return posts
    
    def _normalize_post(self, item: Dict[str, Any], source_format: str) -> Optional[Dict[str, Any]]:
        """Normalize an item into RedditDL post format."""
        try:
            # Create normalized post structure
            post = {
                'id': self._extract_id(item, source_format),
                'title': self._extract_title(item, source_format),
                'url': self._extract_url(item, source_format),
                'author': self._extract_author(item, source_format),
                'created_utc': self._extract_timestamp(item, source_format),
                'score': self._extract_score(item, source_format),
                'selftext': self._extract_text(item, source_format),
                'subreddit': self._extract_category(item, source_format),
                'permalink': self._extract_permalink(item, source_format),
                'is_video': self._is_video(item, source_format),
                'is_self': self._is_text_post(item, source_format),
                'num_comments': self._extract_comment_count(item, source_format),
                'upvote_ratio': self._extract_upvote_ratio(item, source_format),
                'source_type': 'example_scraper',
                'source_data': item.copy()  # Preserve original data
            }
            
            return post
            
        except Exception as e:
            print(f"Error normalizing post: {e}")
            return None
    
    def _extract_id(self, item: Dict[str, Any], source_format: str) -> str:
        """Extract unique ID from item."""
        return str(item.get('id', item.get('guid', f"unknown_{hash(str(item))}")))
    
    def _extract_title(self, item: Dict[str, Any], source_format: str) -> str:
        """Extract title from item."""
        return item.get('title', item.get('name', 'Untitled'))
    
    def _extract_url(self, item: Dict[str, Any], source_format: str) -> Optional[str]:
        """Extract URL from item."""
        return item.get('url', item.get('link'))
    
    def _extract_author(self, item: Dict[str, Any], source_format: str) -> str:
        """Extract author from item."""
        return item.get('author', item.get('user', 'Unknown'))
    
    def _extract_timestamp(self, item: Dict[str, Any], source_format: str) -> float:
        """Extract timestamp from item."""
        timestamp = item.get('created_at', item.get('published', item.get('date')))
        
        if isinstance(timestamp, (int, float)):
            return float(timestamp)
        elif isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.timestamp()
            except ValueError:
                pass
        
        # Default to current time
        return datetime.now(timezone.utc).timestamp()
    
    def _extract_score(self, item: Dict[str, Any], source_format: str) -> int:
        """Extract score from item."""
        return item.get('score', item.get('points', item.get('likes', 0)))
    
    def _extract_text(self, item: Dict[str, Any], source_format: str) -> str:
        """Extract text content from item."""
        return item.get('content', item.get('text', item.get('description', '')))
    
    def _extract_category(self, item: Dict[str, Any], source_format: str) -> str:
        """Extract category/subreddit from item."""
        return item.get('category', item.get('subreddit', item.get('channel', 'general')))
    
    def _extract_permalink(self, item: Dict[str, Any], source_format: str) -> str:
        """Extract permalink from item."""
        return item.get('permalink', item.get('url', ''))
    
    def _is_video(self, item: Dict[str, Any], source_format: str) -> bool:
        """Check if item is a video."""
        media_type = item.get('media_type', item.get('type', ''))
        return 'video' in media_type.lower() or item.get('is_video', False)
    
    def _is_text_post(self, item: Dict[str, Any], source_format: str) -> bool:
        """Check if item is a text post."""
        return bool(item.get('content', item.get('text', item.get('selftext'))))
    
    def _extract_comment_count(self, item: Dict[str, Any], source_format: str) -> int:
        """Extract comment count from item."""
        return item.get('num_comments', item.get('comments', item.get('replies', 0)))
    
    def _extract_upvote_ratio(self, item: Dict[str, Any], source_format: str) -> float:
        """Extract upvote ratio from item."""
        ratio = item.get('upvote_ratio', item.get('positive_ratio'))
        if ratio is not None:
            return float(ratio)
        
        # Calculate from upvotes/downvotes if available
        upvotes = item.get('upvotes', item.get('ups'))
        downvotes = item.get('downvotes', item.get('downs'))
        
        if upvotes is not None and downvotes is not None:
            total = upvotes + downvotes
            return upvotes / total if total > 0 else 0.5
        
        return 0.5  # Default neutral ratio
    
    def get_supported_sources(self) -> List[str]:
        """Get list of supported source types."""
        return self.supported_sources.copy()
    
    def get_config_schema(self) -> Dict[str, Any]:
        """Get configuration schema for this scraper."""
        return self.config_schema.copy()
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate scraper configuration."""
        errors = []
        
        # Validate base_url
        base_url = config.get('base_url')
        if base_url and not isinstance(base_url, str):
            errors.append("base_url must be a string")
        elif base_url and not (base_url.startswith('http://') or base_url.startswith('https://')):
            errors.append("base_url must start with http:// or https://")
        
        # Validate timeout
        timeout = config.get('timeout', 30.0)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            errors.append("timeout must be a positive number")
        
        # Validate max_retries
        max_retries = config.get('max_retries', 3)
        if not isinstance(max_retries, int) or max_retries < 0:
            errors.append("max_retries must be a non-negative integer")
        
        # Validate rate_limit
        rate_limit = config.get('rate_limit', 1.0)
        if not isinstance(rate_limit, (int, float)) or rate_limit < 0:
            errors.append("rate_limit must be a non-negative number")
        
        # Validate verify_ssl
        verify_ssl = config.get('verify_ssl', True)
        if not isinstance(verify_ssl, bool):
            errors.append("verify_ssl must be a boolean")
        
        return errors
    
    def get_rate_limit(self) -> float:
        """Get the current rate limit in seconds."""
        return self.rate_limit
    
    def get_scraper_info(self) -> Dict[str, Any]:
        """Get information about this scraper."""
        return {
            'name': self.scraper_name,
            'supported_sources': self.supported_sources,
            'rate_limit': self.rate_limit,
            'requires_auth': any(
                field.get('sensitive', False) for field in self.config_schema.values()
            ),
            'async_capable': True,
            'retry_capable': True
        }


def initialize_plugin():
    """Initialize the example scraper plugin."""
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")


def cleanup_plugin():
    """Clean up the example scraper plugin."""
    print(f"Cleaning up {__plugin_info__['name']}")


# Example usage and configuration
if __name__ == "__main__":
    # Example configuration for testing
    example_config = {
        'source_type': 'example_api',
        'base_url': 'https://api.example.com',
        'endpoint': '/posts',
        'params': {'limit': 25, 'sort': 'new'},
        'api_key': 'your_api_key_here',
        'timeout': 30.0,
        'max_retries': 3,
        'rate_limit': 1.0,
        'user_agent': 'RedditDL-ExampleScraper/1.0',
        'verify_ssl': True
    }
    
    # Create scraper instance
    scraper = ExampleScraper()
    
    # Validate configuration
    errors = scraper.validate_config(example_config)
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("Configuration is valid")
    
    # Check if scraper can handle the source
    can_handle = scraper.can_scrape(
        example_config['source_type'], 
        example_config
    )
    print(f"Can handle source: {can_handle}")
    
    # Show scraper info
    info = scraper.get_scraper_info()
    print(f"Scraper info: {info}")