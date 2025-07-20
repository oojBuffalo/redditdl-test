"""
Filter Plugin Template

This template provides a starting point for creating filter plugins
for RedditDL. Filter plugins process lists of posts to select, exclude,
or modify posts based on specific criteria.

To create a new filter plugin:
1. Copy this template to your plugin file
2. Rename the class and update the plugin information
3. Implement the filter logic in the apply() method
4. Define the configuration schema
5. Test your filter thoroughly
"""

from typing import Any, Dict, List
from core.plugins.hooks import BaseFilter

# Plugin metadata (optional but recommended)
__plugin_info__ = {
    'name': 'example_filter',
    'version': '1.0.0',
    'description': 'Example filter plugin for filtering posts',
    'author': 'Your Name',
    'filter_type': 'content',
    'priority': 100
}


class ExampleFilter(BaseFilter):
    """
    Example filter plugin.
    
    This filter demonstrates how to filter posts based on specific criteria.
    Replace 'example' with your specific filter purpose (e.g., 'score', 'date', etc.).
    """
    
    # Plugin priority (lower = higher priority)
    priority = 100
    
    def __init__(self):
        """Initialize the filter."""
        self.filter_name = 'example_filter'
        self.config_schema = {
            'min_value': {
                'type': 'integer',
                'default': 0,
                'description': 'Minimum value threshold'
            },
            'max_value': {
                'type': 'integer', 
                'default': 1000,
                'description': 'Maximum value threshold'
            },
            'include_keywords': {
                'type': 'list',
                'default': [],
                'description': 'Keywords that must be present'
            },
            'exclude_keywords': {
                'type': 'list',
                'default': [],
                'description': 'Keywords that must not be present'
            },
            'case_sensitive': {
                'type': 'boolean',
                'default': False,
                'description': 'Whether keyword matching is case sensitive'
            },
            'mode': {
                'type': 'string',
                'default': 'include',
                'choices': ['include', 'exclude'],
                'description': 'Filter mode: include or exclude matching posts'
            }
        }
    
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Apply filter to posts and return filtered results.
        
        Args:
            posts: List of post dictionaries to filter
            config: Filter configuration dictionary
            
        Returns:
            Filtered list of post dictionaries
        """
        if not posts:
            return []
        
        try:
            # Extract configuration
            min_value = config.get('min_value', 0)
            max_value = config.get('max_value', 1000)
            include_keywords = config.get('include_keywords', [])
            exclude_keywords = config.get('exclude_keywords', [])
            case_sensitive = config.get('case_sensitive', False)
            mode = config.get('mode', 'include')
            
            filtered_posts = []
            
            for post in posts:
                if self._should_include_post(post, min_value, max_value, 
                                           include_keywords, exclude_keywords,
                                           case_sensitive, mode):
                    filtered_posts.append(post)
            
            return filtered_posts
            
        except Exception as e:
            # Log error and return original posts to be safe
            print(f"Filter error in {self.filter_name}: {e}")
            return posts
    
    def _should_include_post(self, post: Dict[str, Any], min_value: int, max_value: int,
                           include_keywords: List[str], exclude_keywords: List[str],
                           case_sensitive: bool, mode: str) -> bool:
        """
        Determine if a post should be included based on filter criteria.
        
        Args:
            post: Post dictionary to evaluate
            min_value: Minimum value threshold
            max_value: Maximum value threshold
            include_keywords: Keywords that must be present
            exclude_keywords: Keywords that must not be present
            case_sensitive: Whether keyword matching is case sensitive
            mode: Filter mode ('include' or 'exclude')
            
        Returns:
            True if post should be included
        """
        try:
            # Example: Filter by score (you can modify this logic)
            score = post.get('score', 0)
            if score < min_value or score > max_value:
                return mode == 'exclude'
            
            # Prepare text for keyword matching
            title = post.get('title', '')
            selftext = post.get('selftext', '')
            searchable_text = f"{title} {selftext}"
            
            if not case_sensitive:
                searchable_text = searchable_text.lower()
                include_keywords = [kw.lower() for kw in include_keywords]
                exclude_keywords = [kw.lower() for kw in exclude_keywords]
            
            # Check include keywords
            if include_keywords:
                has_include_keyword = any(keyword in searchable_text for keyword in include_keywords)
                if not has_include_keyword:
                    return mode == 'exclude'
            
            # Check exclude keywords
            if exclude_keywords:
                has_exclude_keyword = any(keyword in searchable_text for keyword in exclude_keywords)
                if has_exclude_keyword:
                    return mode == 'exclude'
            
            # Example: Additional filtering logic
            # You can add more criteria here:
            
            # Filter by subreddit
            # subreddit = post.get('subreddit', '')
            # if subreddit in blocked_subreddits:
            #     return mode == 'exclude'
            
            # Filter by post type
            # post_type = post.get('post_type', 'link')
            # if post_type not in allowed_types:
            #     return mode == 'exclude'
            
            # Filter by date
            # created_utc = post.get('created_utc', 0)
            # if created_utc < min_date or created_utc > max_date:
            #     return mode == 'exclude'
            
            # Filter by NSFW status
            # is_nsfw = post.get('is_nsfw', False)
            # if is_nsfw and not allow_nsfw:
            #     return mode == 'exclude'
            
            # If all checks pass, include the post
            return mode == 'include'
            
        except Exception as e:
            # On error, be conservative and include the post
            print(f"Error evaluating post {post.get('id', 'unknown')}: {e}")
            return True
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get configuration schema for this filter.
        
        Returns:
            Configuration schema dictionary describing all options
        """
        return self.config_schema.copy()
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate filter configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate min/max values
        min_value = config.get('min_value', 0)
        max_value = config.get('max_value', 1000)
        
        if not isinstance(min_value, int):
            errors.append("min_value must be an integer")
        if not isinstance(max_value, int):
            errors.append("max_value must be an integer")
        if isinstance(min_value, int) and isinstance(max_value, int) and min_value > max_value:
            errors.append("min_value cannot be greater than max_value")
        
        # Validate keywords
        include_keywords = config.get('include_keywords', [])
        exclude_keywords = config.get('exclude_keywords', [])
        
        if not isinstance(include_keywords, list):
            errors.append("include_keywords must be a list")
        if not isinstance(exclude_keywords, list):
            errors.append("exclude_keywords must be a list")
        
        # Validate mode
        mode = config.get('mode', 'include')
        if mode not in ['include', 'exclude']:
            errors.append("mode must be either 'include' or 'exclude'")
        
        # Validate case_sensitive
        case_sensitive = config.get('case_sensitive', False)
        if not isinstance(case_sensitive, bool):
            errors.append("case_sensitive must be a boolean")
        
        return errors
    
    def get_statistics(self, posts_before: List[Dict[str, Any]], 
                      posts_after: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get filter statistics for reporting.
        
        Args:
            posts_before: Posts before filtering
            posts_after: Posts after filtering
            
        Returns:
            Statistics dictionary
        """
        total_before = len(posts_before)
        total_after = len(posts_after)
        filtered_out = total_before - total_after
        
        return {
            'filter_name': self.filter_name,
            'posts_before': total_before,
            'posts_after': total_after,
            'posts_filtered': filtered_out,
            'filter_percentage': (filtered_out / total_before * 100) if total_before > 0 else 0.0
        }


# Plugin initialization function (optional)
def initialize_plugin():
    """
    Initialize the plugin when it's loaded.
    
    This function is called once when the plugin is loaded.
    Use it for any setup that needs to happen before the plugin is used.
    """
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")
    
    # Example initialization:
    # - Load configuration files
    # - Initialize caches
    # - Validate dependencies


# Plugin cleanup function (optional)
def cleanup_plugin():
    """
    Clean up plugin resources when it's unloaded.
    
    This function is called when the plugin is being unloaded.
    Use it to clean up any resources, close connections, etc.
    """
    print(f"Cleaning up {__plugin_info__['name']}")
    
    # Example cleanup:
    # - Save caches to disk
    # - Close any open files
    # - Clean up temporary data