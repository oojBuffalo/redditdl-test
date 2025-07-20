"""
Score Filter Plugin

A simple example filter plugin that filters posts based on their score
(upvotes minus downvotes). Useful for filtering out low-quality content
or focusing on highly upvoted posts.
"""

from typing import Any, Dict, List
from core.plugins.hooks import BaseFilter

__plugin_info__ = {
    'name': 'score_filter',
    'version': '1.0.0',
    'description': 'Filters posts based on their score (upvotes - downvotes)',
    'author': 'RedditDL Team',
    'filter_type': 'score',
    'priority': 100
}


class ScoreFilter(BaseFilter):
    """
    Score-based filter for Reddit posts.
    
    This filter allows you to filter posts based on their score,
    supporting minimum and maximum thresholds.
    """
    
    priority = 100
    
    def __init__(self):
        self.filter_name = 'score_filter'
        self.config_schema = {
            'min_score': {
                'type': 'integer',
                'default': 0,
                'description': 'Minimum score threshold (inclusive)'
            },
            'max_score': {
                'type': 'integer',
                'default': None,
                'description': 'Maximum score threshold (inclusive), None for no limit'
            },
            'mode': {
                'type': 'string',
                'default': 'include',
                'choices': ['include', 'exclude'],
                'description': 'Whether to include or exclude posts matching criteria'
            },
            'handle_missing_score': {
                'type': 'string',
                'default': 'treat_as_zero',
                'choices': ['treat_as_zero', 'exclude', 'include'],
                'description': 'How to handle posts with missing score data'
            }
        }
    
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply score filter to posts."""
        if not posts:
            return []
        
        try:
            # Extract configuration
            min_score = config.get('min_score', 0)
            max_score = config.get('max_score')  # Can be None
            mode = config.get('mode', 'include')
            handle_missing = config.get('handle_missing_score', 'treat_as_zero')
            
            filtered_posts = []
            
            for post in posts:
                if self._should_include_post(post, min_score, max_score, mode, handle_missing):
                    filtered_posts.append(post)
            
            # Log filter statistics
            filtered_count = len(posts) - len(filtered_posts)
            if filtered_count > 0:
                print(f"Score filter: {filtered_count} posts filtered out "
                      f"({len(filtered_posts)} remaining)")
            
            return filtered_posts
            
        except Exception as e:
            print(f"Score filter error: {e}")
            return posts  # Return original posts if filter fails
    
    def _should_include_post(self, post: Dict[str, Any], min_score: int, 
                           max_score: int, mode: str, handle_missing: str) -> bool:
        """Determine if a post should be included based on its score."""
        try:
            # Get post score
            score = post.get('score')
            
            # Handle missing score
            if score is None:
                if handle_missing == 'treat_as_zero':
                    score = 0
                elif handle_missing == 'exclude':
                    return mode == 'exclude'
                elif handle_missing == 'include':
                    return mode == 'include'
                else:
                    return True  # Conservative default
            
            # Ensure score is numeric
            try:
                score = int(score)
            except (ValueError, TypeError):
                # If score can't be converted to int, treat as missing
                if handle_missing == 'treat_as_zero':
                    score = 0
                elif handle_missing == 'exclude':
                    return mode == 'exclude'
                else:
                    return mode == 'include'
            
            # Apply score thresholds
            passes_filter = True
            
            if score < min_score:
                passes_filter = False
            
            if max_score is not None and score > max_score:
                passes_filter = False
            
            # Apply mode logic
            if mode == 'include':
                return passes_filter
            elif mode == 'exclude':
                return not passes_filter
            else:
                return True  # Conservative default
                
        except Exception as e:
            print(f"Error evaluating post {post.get('id', 'unknown')}: {e}")
            return True  # Conservative default on error
    
    def get_config_schema(self) -> Dict[str, Any]:
        """Get configuration schema for this filter."""
        return self.config_schema.copy()
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate filter configuration."""
        errors = []
        
        # Validate min_score
        min_score = config.get('min_score', 0)
        if not isinstance(min_score, int):
            errors.append("min_score must be an integer")
        
        # Validate max_score
        max_score = config.get('max_score')
        if max_score is not None:
            if not isinstance(max_score, int):
                errors.append("max_score must be an integer or None")
            elif isinstance(min_score, int) and max_score < min_score:
                errors.append("max_score cannot be less than min_score")
        
        # Validate mode
        mode = config.get('mode', 'include')
        if mode not in ['include', 'exclude']:
            errors.append("mode must be either 'include' or 'exclude'")
        
        # Validate handle_missing_score
        handle_missing = config.get('handle_missing_score', 'treat_as_zero')
        valid_options = ['treat_as_zero', 'exclude', 'include']
        if handle_missing not in valid_options:
            errors.append(f"handle_missing_score must be one of: {valid_options}")
        
        return errors
    
    def get_statistics(self, posts_before: List[Dict[str, Any]], 
                      posts_after: List[Dict[str, Any]], 
                      config: Dict[str, Any]) -> Dict[str, Any]:
        """Get detailed statistics about the filtering operation."""
        total_before = len(posts_before)
        total_after = len(posts_after)
        filtered_out = total_before - total_after
        
        # Calculate score statistics
        scores_before = []
        scores_after = []
        
        for post in posts_before:
            score = post.get('score')
            if score is not None:
                try:
                    scores_before.append(int(score))
                except (ValueError, TypeError):
                    pass
        
        for post in posts_after:
            score = post.get('score')
            if score is not None:
                try:
                    scores_after.append(int(score))
                except (ValueError, TypeError):
                    pass
        
        # Calculate statistics
        stats = {
            'filter_name': self.filter_name,
            'posts_before': total_before,
            'posts_after': total_after,
            'posts_filtered': filtered_out,
            'filter_percentage': (filtered_out / total_before * 100) if total_before > 0 else 0.0,
            'config': config.copy()
        }
        
        if scores_before:
            stats['scores_before'] = {
                'min': min(scores_before),
                'max': max(scores_before),
                'avg': sum(scores_before) / len(scores_before),
                'count': len(scores_before)
            }
        
        if scores_after:
            stats['scores_after'] = {
                'min': min(scores_after),
                'max': max(scores_after),
                'avg': sum(scores_after) / len(scores_after),
                'count': len(scores_after)
            }
        
        return stats
    
    def get_filter_summary(self, config: Dict[str, Any]) -> str:
        """Get a human-readable summary of the filter configuration."""
        min_score = config.get('min_score', 0)
        max_score = config.get('max_score')
        mode = config.get('mode', 'include')
        
        if max_score is not None:
            range_desc = f"score between {min_score} and {max_score}"
        else:
            range_desc = f"score >= {min_score}"
        
        action = "include" if mode == 'include' else "exclude"
        
        return f"Score filter: {action} posts with {range_desc}"


def initialize_plugin():
    """Initialize the score filter plugin."""
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")


def cleanup_plugin():
    """Clean up the score filter plugin."""
    print(f"Cleaning up {__plugin_info__['name']}")