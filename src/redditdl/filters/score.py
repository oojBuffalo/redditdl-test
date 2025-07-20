"""
Score-based filtering for Reddit posts.

Filters posts based on their score (upvotes minus downvotes) using minimum
and maximum threshold values. Supports both inclusive and exclusive bounds.
"""

import time
from typing import Any, Dict, List, Optional, Union
from .base import Filter, FilterResult
from redditdl.scrapers import PostMetadata


class ScoreFilter(Filter):
    """
    Filter posts based on their score (upvotes minus downvotes).
    
    Configuration options:
    - min_score: Minimum score threshold (inclusive)
    - max_score: Maximum score threshold (inclusive)
    
    Both thresholds are optional. If neither is specified, all posts pass.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the score filter.
        
        Args:
            config: Configuration dictionary with min_score and/or max_score
        """
        super().__init__(config)
        self.min_score = self.config.get('min_score')
        self.max_score = self.config.get('max_score')
    
    @property
    def name(self) -> str:
        """Human-readable name of the filter."""
        return "Score Filter"
    
    @property
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        if self.min_score is not None and self.max_score is not None:
            return f"Posts with score between {self.min_score} and {self.max_score}"
        elif self.min_score is not None:
            return f"Posts with score >= {self.min_score}"
        elif self.max_score is not None:
            return f"Posts with score <= {self.max_score}"
        else:
            return "No score filtering (all posts pass)"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the score filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        start_time = time.time()
        
        try:
            # Get the post score, defaulting to 0 if not available
            post_score = getattr(post, 'score', 0)
            
            # If no score filtering is configured, pass all posts
            if self.min_score is None and self.max_score is None:
                return FilterResult(
                    passed=True,
                    reason="No score filter configured",
                    metadata={
                        "post_score": post_score,
                        "min_score": None,
                        "max_score": None
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply minimum score threshold
            if self.min_score is not None and post_score < self.min_score:
                return FilterResult(
                    passed=False,
                    reason=f"Score {post_score} below minimum {self.min_score}",
                    metadata={
                        "post_score": post_score,
                        "min_score": self.min_score,
                        "max_score": self.max_score,
                        "failed_threshold": "minimum"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply maximum score threshold
            if self.max_score is not None and post_score > self.max_score:
                return FilterResult(
                    passed=False,
                    reason=f"Score {post_score} above maximum {self.max_score}",
                    metadata={
                        "post_score": post_score,
                        "min_score": self.min_score,
                        "max_score": self.max_score,
                        "failed_threshold": "maximum"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Post passed all score thresholds
            return FilterResult(
                passed=True,
                reason=f"Score {post_score} within bounds",
                metadata={
                    "post_score": post_score,
                    "min_score": self.min_score,
                    "max_score": self.max_score
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Error applying score filter to post {getattr(post, 'id', 'unknown')}: {e}")
            return FilterResult(
                passed=False,
                reason=f"Filter error: {e}",
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def validate_config(self) -> List[str]:
        """
        Validate the score filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate min_score type and value
        if self.min_score is not None:
            if not isinstance(self.min_score, (int, float)):
                errors.append("min_score must be a number")
        
        # Validate max_score type and value
        if self.max_score is not None:
            if not isinstance(self.max_score, (int, float)):
                errors.append("max_score must be a number")
        
        # Validate logical consistency
        if (self.min_score is not None and self.max_score is not None and 
            self.min_score > self.max_score):
            errors.append("min_score cannot be greater than max_score")
        
        return errors
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for the score filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {
                "min_score": {
                    "type": "number",
                    "description": "Minimum score threshold (inclusive)",
                    "examples": [0, 10, 100]
                },
                "max_score": {
                    "type": "number",
                    "description": "Maximum score threshold (inclusive)",
                    "examples": [1000, 10000]
                }
            },
            "additionalProperties": False,
            "examples": [
                {"min_score": 10},
                {"max_score": 1000},
                {"min_score": 5, "max_score": 500}
            ]
        }