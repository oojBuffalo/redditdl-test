"""
Domain-based filtering for Reddit posts.

Filters posts based on the domain of their URL. Supports allowlists and
blocklists with flexible domain matching patterns.
"""

import time
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Set
from redditdl.filters.base import Filter, FilterResult
from redditdl.scrapers import PostMetadata


class DomainFilter(Filter):
    """
    Filter posts based on their URL domain.
    
    Configuration options:
    - domains_allow: List of allowed domains (allowlist)
    - domains_block: List of blocked domains (blocklist)  
    - match_subdomains: Whether to match subdomains (default: True)
    - case_sensitive: Whether domain matching is case-sensitive (default: False)
    - self_posts_action: How to handle self posts ("allow", "block", "ignore") (default: "allow")
    
    Domain matching supports:
    - Exact matches: "example.com"
    - Subdomain matches: "*.example.com" or just "example.com" if match_subdomains=True
    - Top-level domain matches: "*.edu", "*.gov"
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the domain filter.
        
        Args:
            config: Configuration dictionary with domain criteria
        """
        super().__init__(config)
        
        # Domain lists
        self.domains_allow = self.config.get('domains_allow', [])
        self.domains_block = self.config.get('domains_block', [])
        
        # Matching options
        self.match_subdomains = self.config.get('match_subdomains', True)
        self.case_sensitive = self.config.get('case_sensitive', False)
        self.self_posts_action = self.config.get('self_posts_action', 'allow')
        
        # Normalize domain lists
        self._normalize_domains()
    
    @property
    def name(self) -> str:
        """Human-readable name of the filter."""
        return "Domain Filter"
    
    @property
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        criteria = []
        
        if self.domains_allow:
            criteria.append(f"allowed domains: {', '.join(self.domains_allow[:3])}" + 
                          ("..." if len(self.domains_allow) > 3 else ""))
        
        if self.domains_block:
            criteria.append(f"blocked domains: {', '.join(self.domains_block[:3])}" + 
                          ("..." if len(self.domains_block) > 3 else ""))
        
        if criteria:
            subdomain_note = " (including subdomains)" if self.match_subdomains else ""
            return f"Posts from {', '.join(criteria)}{subdomain_note}"
        else:
            return "No domain filtering (all posts pass)"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the domain filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        start_time = time.time()
        
        try:
            # If no domain filtering is configured, pass all posts
            if not self.domains_allow and not self.domains_block:
                return FilterResult(
                    passed=True,
                    reason="No domain filter configured",
                    metadata={
                        "domains_allow": self.domains_allow,
                        "domains_block": self.domains_block
                    },
                    execution_time=time.time() - start_time
                )
            
            # Extract domain from post URL
            post_domain = self._extract_domain(post)
            
            # Handle self posts
            if post_domain is None or post_domain == 'reddit.com':
                if self.self_posts_action == 'allow':
                    return FilterResult(
                        passed=True,
                        reason="Self post allowed by configuration",
                        metadata={
                            "post_domain": post_domain,
                            "self_posts_action": self.self_posts_action,
                            "domains_allow": self.domains_allow,
                            "domains_block": self.domains_block
                        },
                        execution_time=time.time() - start_time
                    )
                elif self.self_posts_action == 'block':
                    return FilterResult(
                        passed=False,
                        reason="Self post blocked by configuration",
                        metadata={
                            "post_domain": post_domain,
                            "self_posts_action": self.self_posts_action,
                            "failed_criteria": "self_posts_blocked"
                        },
                        execution_time=time.time() - start_time
                    )
                # If self_posts_action == 'ignore', continue with normal filtering
            
            if post_domain is None:
                self.logger.warning(f"Could not extract domain from post {getattr(post, 'id', 'unknown')}")
                # If we have an allowlist and can't determine domain, fail
                if self.domains_allow:
                    return FilterResult(
                        passed=False,
                        reason="Could not determine domain and allowlist is active",
                        metadata={
                            "post_domain": None,
                            "domains_allow": self.domains_allow,
                            "failed_criteria": "unknown_domain_with_allowlist"
                        },
                        execution_time=time.time() - start_time
                    )
                # If only blocklist, pass (can't block unknown domain)
                else:
                    return FilterResult(
                        passed=True,
                        reason="Could not determine domain, no allowlist active",
                        metadata={
                            "post_domain": None,
                            "domains_block": self.domains_block
                        },
                        execution_time=time.time() - start_time
                    )
            
            # Apply allowlist filter
            if self.domains_allow:
                if not self._domain_matches_list(post_domain, self.domains_allow):
                    return FilterResult(
                        passed=False,
                        reason=f"Domain '{post_domain}' not in allowlist",
                        metadata={
                            "post_domain": post_domain,
                            "domains_allow": self.domains_allow,
                            "domains_block": self.domains_block,
                            "failed_criteria": "not_in_allowlist"
                        },
                        execution_time=time.time() - start_time
                    )
            
            # Apply blocklist filter
            if self.domains_block:
                if self._domain_matches_list(post_domain, self.domains_block):
                    matched_pattern = self._get_matching_pattern(post_domain, self.domains_block)
                    return FilterResult(
                        passed=False,
                        reason=f"Domain '{post_domain}' matches blocked pattern '{matched_pattern}'",
                        metadata={
                            "post_domain": post_domain,
                            "domains_allow": self.domains_allow,
                            "domains_block": self.domains_block,
                            "matched_block_pattern": matched_pattern,
                            "failed_criteria": "in_blocklist"
                        },
                        execution_time=time.time() - start_time
                    )
            
            # Post passed all domain criteria
            matched_allow_pattern = None
            if self.domains_allow:
                matched_allow_pattern = self._get_matching_pattern(post_domain, self.domains_allow)
            
            return FilterResult(
                passed=True,
                reason=f"Domain '{post_domain}' passed all criteria",
                metadata={
                    "post_domain": post_domain,
                    "domains_allow": self.domains_allow,
                    "domains_block": self.domains_block,
                    "matched_allow_pattern": matched_allow_pattern
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Error applying domain filter to post {getattr(post, 'id', 'unknown')}: {e}")
            return FilterResult(
                passed=False,
                reason=f"Filter error: {e}",
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def _extract_domain(self, post: PostMetadata) -> Optional[str]:
        """
        Extract the domain from a post's URL.
        
        Args:
            post: Reddit post metadata
            
        Returns:
            Domain string or None if not determinable
        """
        # Get the post URL
        url = getattr(post, 'url', '') or ''
        
        if not url or url.strip() == '':
            return None
        
        # Check if it's a self post (Reddit domain)
        if url.startswith('/r/') or 'reddit.com' in url:
            return 'reddit.com'
        
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower() if not self.case_sensitive else parsed.netloc
            
            # Remove port if present
            if ':' in domain:
                domain = domain.split(':')[0]
            
            # Remove www. prefix for consistency
            if domain.startswith('www.'):
                domain = domain[4:]
            
            return domain if domain else None
            
        except Exception as e:
            self.logger.warning(f"Error parsing URL '{url}': {e}")
            return None
    
    def _normalize_domains(self):
        """Normalize domain lists for consistent matching."""
        # Normalize allowlist
        normalized_allow = []
        for domain in self.domains_allow:
            if isinstance(domain, str):
                normalized = domain.lower() if not self.case_sensitive else domain
                # Remove www. prefix
                if normalized.startswith('www.'):
                    normalized = normalized[4:]
                normalized_allow.append(normalized)
        self.domains_allow = normalized_allow
        
        # Normalize blocklist
        normalized_block = []
        for domain in self.domains_block:
            if isinstance(domain, str):
                normalized = domain.lower() if not self.case_sensitive else domain
                # Remove www. prefix
                if normalized.startswith('www.'):
                    normalized = normalized[4:]
                normalized_block.append(normalized)
        self.domains_block = normalized_block
    
    def _domain_matches_list(self, domain: str, domain_list: List[str]) -> bool:
        """
        Check if a domain matches any pattern in the list.
        
        Args:
            domain: Domain to check
            domain_list: List of domain patterns
            
        Returns:
            True if domain matches any pattern
        """
        for pattern in domain_list:
            if self._domain_matches_pattern(domain, pattern):
                return True
        return False
    
    def _domain_matches_pattern(self, domain: str, pattern: str) -> bool:
        """
        Check if a domain matches a specific pattern.
        
        Args:
            domain: Domain to check
            pattern: Pattern to match against
            
        Returns:
            True if domain matches pattern
        """
        # Exact match
        if domain == pattern:
            return True
        
        # Wildcard patterns
        if pattern.startswith('*.'):
            # Pattern like "*.example.com"
            suffix = pattern[2:]
            if domain == suffix or (self.match_subdomains and domain.endswith('.' + suffix)):
                return True
        elif self.match_subdomains:
            # Pattern like "example.com" matches "sub.example.com"
            if domain.endswith('.' + pattern):
                return True
        
        return False
    
    def _get_matching_pattern(self, domain: str, domain_list: List[str]) -> Optional[str]:
        """
        Get the first pattern that matches the domain.
        
        Args:
            domain: Domain to check
            domain_list: List of domain patterns
            
        Returns:
            Matching pattern or None
        """
        for pattern in domain_list:
            if self._domain_matches_pattern(domain, pattern):
                return pattern
        return None
    
    def validate_config(self) -> List[str]:
        """
        Validate the domain filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate domain lists
        if not isinstance(self.domains_allow, list):
            errors.append("domains_allow must be a list")
        else:
            for i, domain in enumerate(self.domains_allow):
                if not isinstance(domain, str):
                    errors.append(f"domains_allow[{i}] must be a string")
                elif not domain.strip():
                    errors.append(f"domains_allow[{i}] cannot be empty")
        
        if not isinstance(self.domains_block, list):
            errors.append("domains_block must be a list")
        else:
            for i, domain in enumerate(self.domains_block):
                if not isinstance(domain, str):
                    errors.append(f"domains_block[{i}] must be a string")
                elif not domain.strip():
                    errors.append(f"domains_block[{i}] cannot be empty")
        
        # Validate boolean options
        for option_name in ['match_subdomains', 'case_sensitive']:
            option_value = self.config.get(option_name)
            if option_value is not None and not isinstance(option_value, bool):
                errors.append(f"{option_name} must be a boolean")
        
        # Validate self_posts_action
        if self.self_posts_action not in ['allow', 'block', 'ignore']:
            errors.append("self_posts_action must be 'allow', 'block', or 'ignore'")
        
        # Check for conflicting domains
        if self.domains_allow and self.domains_block:
            allow_set = set(self.domains_allow)
            block_set = set(self.domains_block)
            conflicts = allow_set.intersection(block_set)
            if conflicts:
                errors.append(f"Domains cannot be in both allow and block lists: {', '.join(conflicts)}")
        
        return errors
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for the domain filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {
                "domains_allow": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of allowed domains (allowlist)",
                    "examples": [["imgur.com", "i.redd.it"], ["*.edu", "*.gov"]]
                },
                "domains_block": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of blocked domains (blocklist)",
                    "examples": [["spam.com", "ads.example.com"], ["*.ads"]]
                },
                "match_subdomains": {
                    "type": "boolean",
                    "default": True,
                    "description": "Whether to match subdomains"
                },
                "case_sensitive": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether domain matching is case-sensitive"
                },
                "self_posts_action": {
                    "type": "string",
                    "enum": ["allow", "block", "ignore"],
                    "default": "allow",
                    "description": "How to handle self posts"
                }
            },
            "additionalProperties": False,
            "examples": [
                {"domains_allow": ["imgur.com", "i.redd.it"]},
                {"domains_block": ["spam.com", "ads.example.com"]},
                {
                    "domains_allow": ["*.edu", "*.gov"],
                    "match_subdomains": True,
                    "self_posts_action": "allow"
                }
            ]
        }