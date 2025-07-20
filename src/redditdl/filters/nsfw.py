"""
NSFW (Not Safe For Work) filtering for Reddit posts.

Filters posts based on their NSFW status with options to include, exclude,
or show only NSFW content.
"""

import time
from typing import Any, Dict, List, Optional
from redditdl.filters.base import Filter, FilterResult
from redditdl.scrapers import PostMetadata


class NSFWFilter(Filter):
    """
    Filter posts based on their NSFW (Not Safe For Work) status.
    
    Configuration options:
    - mode: NSFW filtering mode
      - "include": Include both NSFW and non-NSFW posts (default)
      - "exclude": Exclude NSFW posts (only non-NSFW)
      - "only": Include only NSFW posts
    - strict_mode: Whether to treat unknown NSFW status as NSFW (default: False)
    """
    
    VALID_MODES = {'include', 'exclude', 'only'}
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the NSFW filter.
        
        Args:
            config: Configuration dictionary with NSFW criteria
        """
        super().__init__(config)
        
        # NSFW filtering mode
        self.mode = self.config.get('mode', 'include')
        self.strict_mode = self.config.get('strict_mode', False)
        
        # Validate mode
        if self.mode not in self.VALID_MODES:
            self.logger.warning(f"Invalid NSFW mode '{self.mode}', defaulting to 'include'")
            self.mode = 'include'
    
    @property
    def name(self) -> str:
        """Human-readable name of the filter."""
        return "NSFW Filter"
    
    @property
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        if self.mode == 'include':
            return "Include both NSFW and non-NSFW posts"
        elif self.mode == 'exclude':
            return "Exclude NSFW posts (non-NSFW only)"
        elif self.mode == 'only':
            return "Include only NSFW posts"
        else:
            return f"NSFW filtering mode: {self.mode}"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the NSFW filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        start_time = time.time()
        
        try:
            # Determine post NSFW status
            nsfw_status = self._get_nsfw_status(post)
            
            # Apply filtering based on mode
            if self.mode == 'include':
                # Include all posts regardless of NSFW status
                return FilterResult(
                    passed=True,
                    reason="All posts allowed (include mode)",
                    metadata={
                        "nsfw_status": nsfw_status,
                        "mode": self.mode,
                        "strict_mode": self.strict_mode
                    },
                    execution_time=time.time() - start_time
                )
            
            elif self.mode == 'exclude':
                # Exclude NSFW posts
                if nsfw_status is True:
                    return FilterResult(
                        passed=False,
                        reason="NSFW post excluded",
                        metadata={
                            "nsfw_status": nsfw_status,
                            "mode": self.mode,
                            "strict_mode": self.strict_mode,
                            "failed_criteria": "nsfw_excluded"
                        },
                        execution_time=time.time() - start_time
                    )
                elif nsfw_status is None and self.strict_mode:
                    return FilterResult(
                        passed=False,
                        reason="Unknown NSFW status treated as NSFW (strict mode)",
                        metadata={
                            "nsfw_status": nsfw_status,
                            "mode": self.mode,
                            "strict_mode": self.strict_mode,
                            "failed_criteria": "unknown_nsfw_strict"
                        },
                        execution_time=time.time() - start_time
                    )
                else:
                    return FilterResult(
                        passed=True,
                        reason="Non-NSFW post allowed",
                        metadata={
                            "nsfw_status": nsfw_status,
                            "mode": self.mode,
                            "strict_mode": self.strict_mode
                        },
                        execution_time=time.time() - start_time
                    )
            
            elif self.mode == 'only':
                # Include only NSFW posts
                if nsfw_status is True:
                    return FilterResult(
                        passed=True,
                        reason="NSFW post allowed (only mode)",
                        metadata={
                            "nsfw_status": nsfw_status,
                            "mode": self.mode,
                            "strict_mode": self.strict_mode
                        },
                        execution_time=time.time() - start_time
                    )
                elif nsfw_status is None and self.strict_mode:
                    return FilterResult(
                        passed=True,
                        reason="Unknown NSFW status treated as NSFW (strict mode)",
                        metadata={
                            "nsfw_status": nsfw_status,
                            "mode": self.mode,
                            "strict_mode": self.strict_mode
                        },
                        execution_time=time.time() - start_time
                    )
                else:
                    return FilterResult(
                        passed=False,
                        reason="Non-NSFW post excluded (only mode)",
                        metadata={
                            "nsfw_status": nsfw_status,
                            "mode": self.mode,
                            "strict_mode": self.strict_mode,
                            "failed_criteria": "non_nsfw_in_only_mode"
                        },
                        execution_time=time.time() - start_time
                    )
            
            else:
                # Unknown mode (should not happen due to validation)
                self.logger.error(f"Unknown NSFW filter mode: {self.mode}")
                return FilterResult(
                    passed=True,  # Default to allowing posts
                    reason=f"Unknown mode '{self.mode}', allowing post",
                    metadata={
                        "nsfw_status": nsfw_status,
                        "mode": self.mode,
                        "error": "unknown_mode"
                    },
                    execution_time=time.time() - start_time
                )
            
        except Exception as e:
            self.logger.error(f"Error applying NSFW filter to post {getattr(post, 'id', 'unknown')}: {e}")
            return FilterResult(
                passed=False,
                reason=f"Filter error: {e}",
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def _get_nsfw_status(self, post: PostMetadata) -> Optional[bool]:
        """
        Determine the NSFW status of a post.
        
        Args:
            post: Reddit post metadata
            
        Returns:
            True if NSFW, False if not NSFW, None if unknown
        """
        # Try different ways to get NSFW status
        
        # Check is_nsfw attribute
        if hasattr(post, 'is_nsfw'):
            nsfw_value = getattr(post, 'is_nsfw')
            if isinstance(nsfw_value, bool):
                return nsfw_value
            elif isinstance(nsfw_value, str):
                return nsfw_value.lower() in ('true', '1', 'yes', 'nsfw')
            elif isinstance(nsfw_value, (int, float)):
                return bool(nsfw_value)
        
        # Check nsfw attribute (alternative naming)
        if hasattr(post, 'nsfw'):
            nsfw_value = getattr(post, 'nsfw')
            if isinstance(nsfw_value, bool):
                return nsfw_value
            elif isinstance(nsfw_value, str):
                return nsfw_value.lower() in ('true', '1', 'yes', 'nsfw')
            elif isinstance(nsfw_value, (int, float)):
                return bool(nsfw_value)
        
        # Check over_18 attribute (Reddit API field)
        if hasattr(post, 'over_18'):
            over_18_value = getattr(post, 'over_18')
            if isinstance(over_18_value, bool):
                return over_18_value
            elif isinstance(over_18_value, str):
                return over_18_value.lower() in ('true', '1', 'yes')
            elif isinstance(over_18_value, (int, float)):
                return bool(over_18_value)
        
        # Check if NSFW is mentioned in title or selftext
        title = getattr(post, 'title', '') or ''
        selftext = getattr(post, 'selftext', '') or ''
        combined_text = f"{title} {selftext}".lower()
        
        if 'nsfw' in combined_text or '[nsfw]' in combined_text:
            return True
        
        # Check subreddit NSFW status if available
        if hasattr(post, 'subreddit_nsfw'):
            subreddit_nsfw = getattr(post, 'subreddit_nsfw')
            if isinstance(subreddit_nsfw, bool):
                return subreddit_nsfw
        
        # Unknown NSFW status
        return None
    
    def validate_config(self) -> List[str]:
        """
        Validate the NSFW filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate mode
        if self.mode not in self.VALID_MODES:
            errors.append(f"mode must be one of {', '.join(sorted(self.VALID_MODES))}")
        
        # Validate strict_mode
        if not isinstance(self.strict_mode, bool):
            errors.append("strict_mode must be a boolean")
        
        return errors
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for the NSFW filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": list(self.VALID_MODES),
                    "default": "include",
                    "description": "NSFW filtering mode",
                    "examples": ["include", "exclude", "only"]
                },
                "strict_mode": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether to treat unknown NSFW status as NSFW"
                }
            },
            "additionalProperties": False,
            "examples": [
                {"mode": "exclude"},
                {"mode": "only"},
                {"mode": "exclude", "strict_mode": True}
            ]
        }