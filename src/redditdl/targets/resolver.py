"""
Target Resolution System

Handles resolution and validation of different Reddit target types including
users, subreddits, URLs, and special authenticated targets.
"""

import re
import urllib.parse
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Union
import logging


class TargetType(Enum):
    """Enumeration of supported target types."""
    USER = "user"
    SUBREDDIT = "subreddit"
    URL = "url"
    SAVED = "saved"
    UPVOTED = "upvoted"
    UNKNOWN = "unknown"


@dataclass
class TargetInfo:
    """
    Information about a resolved target.
    
    Attributes:
        target_type: Type of target (user, subreddit, etc.)
        target_value: Normalized target value (username, subreddit name, URL)
        original_input: Original input string provided by user
        requires_auth: Whether this target requires Reddit API authentication
        metadata: Additional metadata about the target
    """
    target_type: TargetType
    target_value: str
    original_input: str
    requires_auth: bool = False
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class TargetResolver:
    """
    Resolves and validates Reddit targets from user input.
    
    Supports various target formats:
    - Users: 'username', 'u/username', '/u/username'
    - Subreddits: 'subreddit', 'r/subreddit', '/r/subreddit'
    - URLs: Full Reddit URLs to posts, subreddits, or users
    - Special: 'saved', 'upvoted' for authenticated user content
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Regex patterns for different target types
        self.patterns = {
            'user_prefixed': re.compile(r'^/?u/([a-zA-Z0-9_-]+)$'),
            'subreddit_prefixed': re.compile(r'^/?r/([a-zA-Z0-9_-]+)$'),
            'reddit_user_url': re.compile(r'https?://(?:www\.)?reddit\.com/u(?:ser)?/([a-zA-Z0-9_-]+)'),
            'reddit_subreddit_url': re.compile(r'https?://(?:www\.)?reddit\.com/r/([a-zA-Z0-9_-]+)'),
            'reddit_post_url': re.compile(r'https?://(?:www\.)?reddit\.com/r/([a-zA-Z0-9_-]+)/comments/([a-zA-Z0-9]+)'),
            'valid_username': re.compile(r'^[a-zA-Z0-9_-]{3,20}$'),
            'valid_subreddit': re.compile(r'^[a-zA-Z0-9_-]{2,21}$'),
        }
    
    def resolve_target(self, target_input: str) -> TargetInfo:
        """
        Resolve a target input string to a TargetInfo object.
        
        Args:
            target_input: Raw target string from user input
            
        Returns:
            TargetInfo: Resolved target information
            
        Raises:
            ValueError: If target format is invalid or unsupported
        """
        if not target_input or not target_input.strip():
            raise ValueError("Target input cannot be empty")
        
        target_input = target_input.strip()
        self.logger.debug(f"Resolving target: {target_input}")
        
        # Check for special authenticated targets
        if target_input.lower() in ['saved', 'upvoted']:
            return TargetInfo(
                target_type=TargetType.SAVED if target_input.lower() == 'saved' else TargetType.UPVOTED,
                target_value=target_input.lower(),
                original_input=target_input,
                requires_auth=True,
                metadata={'description': f'User {target_input.lower()} posts'}
            )
        
        # Check for Reddit URLs
        url_result = self._resolve_url_target(target_input)
        if url_result:
            return url_result
        
        # Check for prefixed user format (u/username or /u/username)
        user_match = self.patterns['user_prefixed'].match(target_input)
        if user_match:
            username = user_match.group(1)
            if not self._is_valid_username(username):
                raise ValueError(f"Invalid username format: {username}")
            
            return TargetInfo(
                target_type=TargetType.USER,
                target_value=username,
                original_input=target_input,
                requires_auth=False,
                metadata={'description': f'User {username} posts'}
            )
        
        # Check for prefixed subreddit format (r/subreddit or /r/subreddit)
        subreddit_match = self.patterns['subreddit_prefixed'].match(target_input)
        if subreddit_match:
            subreddit = subreddit_match.group(1)
            if not self._is_valid_subreddit(subreddit):
                raise ValueError(f"Invalid subreddit format: {subreddit}")
            
            return TargetInfo(
                target_type=TargetType.SUBREDDIT,
                target_value=subreddit,
                original_input=target_input,
                requires_auth=False,
                metadata={'description': f'Subreddit r/{subreddit} posts'}
            )
        
        # For plain text that could be either username or subreddit,
        # we need to make a decision. Since usernames are more common
        # and the validation rules are similar, we'll prefer username
        # but both should be valid. Let's check both and prefer the longer one
        # or use a heuristic based on common patterns.
        
        is_valid_user = self._is_valid_username(target_input)
        is_valid_sub = self._is_valid_subreddit(target_input)
        
        if is_valid_user and is_valid_sub:
            # Both are valid - use heuristic: 
            # - If it starts with capital letter or has underscores, likely username
            # - If it's a common word or has dashes, likely subreddit
            # - Default to user for ambiguous cases
            if target_input[0].isupper() or '_' in target_input:
                return TargetInfo(
                    target_type=TargetType.USER,
                    target_value=target_input,
                    original_input=target_input,
                    requires_auth=False,
                    metadata={'description': f'User {target_input} posts'}
                )
            else:
                return TargetInfo(
                    target_type=TargetType.SUBREDDIT,
                    target_value=target_input,
                    original_input=target_input,
                    requires_auth=False,
                    metadata={'description': f'Subreddit r/{target_input} posts'}
                )
        elif is_valid_user:
            return TargetInfo(
                target_type=TargetType.USER,
                target_value=target_input,
                original_input=target_input,
                requires_auth=False,
                metadata={'description': f'User {target_input} posts'}
            )
        elif is_valid_sub:
            return TargetInfo(
                target_type=TargetType.SUBREDDIT,
                target_value=target_input,
                original_input=target_input,
                requires_auth=False,
                metadata={'description': f'Subreddit r/{target_input} posts'}
            )
        
        # If we can't determine the type, mark as unknown
        self.logger.warning(f"Could not resolve target type for: {target_input}")
        return TargetInfo(
            target_type=TargetType.UNKNOWN,
            target_value=target_input,
            original_input=target_input,
            requires_auth=False,
            metadata={'error': 'Unknown target format'}
        )
    
    def _resolve_url_target(self, url: str) -> Optional[TargetInfo]:
        """
        Resolve Reddit URL targets.
        
        Args:
            url: URL string to parse
            
        Returns:
            TargetInfo if URL is a valid Reddit URL, None otherwise
        """
        # Check for post URL first (most specific)
        post_url_match = self.patterns['reddit_post_url'].search(url)
        if post_url_match:
            subreddit = post_url_match.group(1)
            post_id = post_url_match.group(2)
            return TargetInfo(
                target_type=TargetType.URL,
                target_value=url,
                original_input=url,
                requires_auth=False,
                metadata={
                    'description': f'Reddit post {post_id} from r/{subreddit}',
                    'subreddit': subreddit,
                    'post_id': post_id,
                    'source_url': url
                }
            )
        
        # Check for user URL
        user_url_match = self.patterns['reddit_user_url'].search(url)
        if user_url_match:
            username = user_url_match.group(1)
            return TargetInfo(
                target_type=TargetType.USER,
                target_value=username,
                original_input=url,
                requires_auth=False,
                metadata={
                    'description': f'User {username} posts',
                    'source_url': url
                }
            )
        
        # Check for subreddit URL
        subreddit_url_match = self.patterns['reddit_subreddit_url'].search(url)
        if subreddit_url_match:
            subreddit = subreddit_url_match.group(1)
            return TargetInfo(
                target_type=TargetType.SUBREDDIT,
                target_value=subreddit,
                original_input=url,
                requires_auth=False,
                metadata={
                    'description': f'Subreddit r/{subreddit} posts',
                    'source_url': url
                }
            )
        
        # Check if it's a general Reddit URL (but validate it's actually reddit.com)
        try:
            parsed = urllib.parse.urlparse(url.lower())
            if parsed.netloc in ['reddit.com', 'www.reddit.com', 'old.reddit.com']:
                return TargetInfo(
                    target_type=TargetType.URL,
                    target_value=url,
                    original_input=url,
                    requires_auth=False,
                    metadata={
                        'description': 'Reddit URL',
                        'source_url': url
                    }
                )
        except Exception:
            pass  # Invalid URL format
        
        return None
    
    def _is_valid_username(self, username: str) -> bool:
        """Check if username meets Reddit's format requirements."""
        return bool(self.patterns['valid_username'].match(username))
    
    def _is_valid_subreddit(self, subreddit: str) -> bool:
        """Check if subreddit name meets Reddit's format requirements."""
        return bool(self.patterns['valid_subreddit'].match(subreddit))
    
    def resolve_multiple_targets(self, target_inputs: List[str]) -> List[TargetInfo]:
        """
        Resolve multiple targets from a list of input strings.
        
        Args:
            target_inputs: List of target strings to resolve
            
        Returns:
            List of TargetInfo objects for each target
            
        Raises:
            ValueError: If any target is invalid
        """
        resolved_targets = []
        errors = []
        
        for target_input in target_inputs:
            try:
                target_info = self.resolve_target(target_input)
                # Check if target resolved to UNKNOWN type
                if target_info.target_type == TargetType.UNKNOWN:
                    errors.append(f"Target '{target_input}': Unknown target format")
                else:
                    resolved_targets.append(target_info)
            except ValueError as e:
                errors.append(f"Target '{target_input}': {e}")
        
        if errors:
            raise ValueError(f"Failed to resolve targets: {'; '.join(errors)}")
        
        return resolved_targets
    
    def validate_target_accessibility(self, target_info: TargetInfo, has_api_auth: bool = False) -> Dict[str, Any]:
        """
        Validate whether a target is accessible with current authentication level.
        
        Args:
            target_info: Target information to validate
            has_api_auth: Whether Reddit API authentication is available
            
        Returns:
            Dict containing validation results and recommendations
        """
        result = {
            'accessible': True,
            'warnings': [],
            'recommendations': [],
            'auth_required': target_info.requires_auth
        }
        
        # Check authentication requirements
        if target_info.requires_auth and not has_api_auth:
            result['accessible'] = False
            result['recommendations'].append(
                f"Target '{target_info.original_input}' requires Reddit API authentication. "
                "Please provide client_id and client_secret for API access."
            )
        
        # Add warnings for specific target types
        if target_info.target_type == TargetType.USER and not has_api_auth:
            result['warnings'].append(
                "Using public scraping for user posts. API mode provides more reliable access and respects rate limits better."
            )
        
        if target_info.target_type == TargetType.SUBREDDIT and not has_api_auth:
            result['warnings'].append(
                "Using public scraping for subreddit posts. Some private or restricted subreddits may not be accessible."
            )
        
        if target_info.target_type == TargetType.URL:
            result['warnings'].append(
                "URL targets have limited functionality. Consider using user or subreddit targets for better results."
            )
        
        if target_info.target_type == TargetType.UNKNOWN:
            result['accessible'] = False
            result['recommendations'].append(
                f"Target '{target_info.original_input}' format not recognized. "
                "Please use formats like: username, u/username, r/subreddit, or Reddit URLs."
            )
        
        return result
    
    def get_supported_formats(self) -> Dict[str, List[str]]:
        """
        Get documentation of supported target formats.
        
        Returns:
            Dict mapping target types to example formats
        """
        return {
            'users': [
                'username',
                'u/username',
                '/u/username',
                'https://reddit.com/u/username',
                'https://reddit.com/user/username'
            ],
            'subreddits': [
                'subreddit',
                'r/subreddit',
                '/r/subreddit',
                'https://reddit.com/r/subreddit'
            ],
            'urls': [
                'https://reddit.com/r/subreddit/comments/abc123/post_title/',
                'https://reddit.com/r/subreddit'
            ],
            'authenticated': [
                'saved',
                'upvoted'
            ]
        }