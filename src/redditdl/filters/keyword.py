"""
Keyword-based filtering for Reddit posts.

Filters posts based on keywords in their title and/or selftext content.
Supports inclusion and exclusion lists with flexible matching options.
"""

import re
import time
from typing import Any, Dict, List, Optional, Set, Union
from redditdl.filters.base import Filter, FilterResult
from redditdl.scrapers import PostMetadata


class KeywordFilter(Filter):
    """
    Filter posts based on keywords in title and selftext.
    
    Configuration options:
    - keywords_include: List of keywords that must be present (OR logic)
    - keywords_exclude: List of keywords that must not be present
    - case_sensitive: Whether matching is case-sensitive (default: False)
    - whole_words_only: Whether to match whole words only (default: False)
    - search_title: Whether to search in post title (default: True)
    - search_selftext: Whether to search in selftext (default: True)
    - regex_mode: Whether keywords are regex patterns (default: False)
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the keyword filter.
        
        Args:
            config: Configuration dictionary with keyword criteria
        """
        super().__init__(config)
        
        # Keyword lists
        self.keywords_include = self.config.get('keywords_include', [])
        self.keywords_exclude = self.config.get('keywords_exclude', [])
        
        # Matching options
        self.case_sensitive = self.config.get('case_sensitive', False)
        self.whole_words_only = self.config.get('whole_words_only', False)
        self.search_title = self.config.get('search_title', True)
        self.search_selftext = self.config.get('search_selftext', True)
        self.regex_mode = self.config.get('regex_mode', False)
        
        # Precompile regex patterns for performance
        self._include_patterns = []
        self._exclude_patterns = []
        self._compile_patterns()
    
    @property
    def name(self) -> str:
        """Human-readable name of the filter."""
        return "Keyword Filter"
    
    @property
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        criteria = []
        
        if self.keywords_include:
            criteria.append(f"must include: {', '.join(self.keywords_include[:3])}" + 
                          ("..." if len(self.keywords_include) > 3 else ""))
        
        if self.keywords_exclude:
            criteria.append(f"must exclude: {', '.join(self.keywords_exclude[:3])}" + 
                          ("..." if len(self.keywords_exclude) > 3 else ""))
        
        if criteria:
            search_fields = []
            if self.search_title:
                search_fields.append("title")
            if self.search_selftext:
                search_fields.append("selftext")
            
            field_str = " and ".join(search_fields) if search_fields else "content"
            return f"Posts in {field_str} that {', '.join(criteria)}"
        else:
            return "No keyword filtering (all posts pass)"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the keyword filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        start_time = time.time()
        
        try:
            # If no keyword filtering is configured, pass all posts
            if not self.keywords_include and not self.keywords_exclude:
                return FilterResult(
                    passed=True,
                    reason="No keyword filter configured",
                    metadata={
                        "keywords_include": self.keywords_include,
                        "keywords_exclude": self.keywords_exclude
                    },
                    execution_time=time.time() - start_time
                )
            
            # Extract searchable text from post
            search_text = self._extract_search_text(post)
            
            if not search_text.strip():
                self.logger.debug(f"No searchable text in post {getattr(post, 'id', 'unknown')}")
                # If there's no text to search and we have include keywords, fail
                if self.keywords_include:
                    return FilterResult(
                        passed=False,
                        reason="No searchable text found and include keywords specified",
                        metadata={
                            "search_text_length": 0,
                            "keywords_include": self.keywords_include,
                            "keywords_exclude": self.keywords_exclude
                        },
                        execution_time=time.time() - start_time
                    )
                # If only exclude keywords, pass (nothing to exclude from)
                else:
                    return FilterResult(
                        passed=True,
                        reason="No searchable text found, no include keywords",
                        metadata={
                            "search_text_length": 0,
                            "keywords_exclude": self.keywords_exclude
                        },
                        execution_time=time.time() - start_time
                    )
            
            # Apply inclusion filter
            if self.keywords_include:
                include_result = self._check_include_keywords(search_text)
                if not include_result['passed']:
                    return FilterResult(
                        passed=False,
                        reason=include_result['reason'],
                        metadata={
                            "search_text_length": len(search_text),
                            "keywords_include": self.keywords_include,
                            "keywords_exclude": self.keywords_exclude,
                            "matched_include": include_result.get('matched', []),
                            "failed_criteria": "inclusion"
                        },
                        execution_time=time.time() - start_time
                    )
            
            # Apply exclusion filter
            if self.keywords_exclude:
                exclude_result = self._check_exclude_keywords(search_text)
                if not exclude_result['passed']:
                    return FilterResult(
                        passed=False,
                        reason=exclude_result['reason'],
                        metadata={
                            "search_text_length": len(search_text),
                            "keywords_include": self.keywords_include,
                            "keywords_exclude": self.keywords_exclude,
                            "matched_exclude": exclude_result.get('matched', []),
                            "failed_criteria": "exclusion"
                        },
                        execution_time=time.time() - start_time
                    )
            
            # Post passed all keyword criteria
            matched_include = []
            if self.keywords_include:
                include_result = self._check_include_keywords(search_text)
                matched_include = include_result.get('matched', [])
            
            return FilterResult(
                passed=True,
                reason="All keyword criteria met",
                metadata={
                    "search_text_length": len(search_text),
                    "keywords_include": self.keywords_include,
                    "keywords_exclude": self.keywords_exclude,
                    "matched_include": matched_include
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Error applying keyword filter to post {getattr(post, 'id', 'unknown')}: {e}")
            return FilterResult(
                passed=False,
                reason=f"Filter error: {e}",
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def _extract_search_text(self, post: PostMetadata) -> str:
        """
        Extract searchable text from a post based on configuration.
        
        Args:
            post: Reddit post metadata
            
        Returns:
            Combined text to search in
        """
        text_parts = []
        
        if self.search_title:
            title = getattr(post, 'title', '') or ''
            text_parts.append(title)
        
        if self.search_selftext:
            selftext = getattr(post, 'selftext', '') or ''
            text_parts.append(selftext)
        
        combined_text = ' '.join(text_parts)
        
        # Handle case sensitivity
        if not self.case_sensitive:
            combined_text = combined_text.lower()
        
        return combined_text
    
    def _compile_patterns(self):
        """Precompile regex patterns for better performance."""
        flags = 0 if self.case_sensitive else re.IGNORECASE
        
        # Compile inclusion patterns
        for keyword in self.keywords_include:
            try:
                if self.regex_mode:
                    pattern = re.compile(keyword, flags)
                elif self.whole_words_only:
                    pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', flags)
                else:
                    pattern = re.compile(re.escape(keyword), flags)
                self._include_patterns.append((keyword, pattern))
            except re.error as e:
                self.logger.error(f"Invalid regex pattern '{keyword}': {e}")
        
        # Compile exclusion patterns
        for keyword in self.keywords_exclude:
            try:
                if self.regex_mode:
                    pattern = re.compile(keyword, flags)
                elif self.whole_words_only:
                    pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', flags)
                else:
                    pattern = re.compile(re.escape(keyword), flags)
                self._exclude_patterns.append((keyword, pattern))
            except re.error as e:
                self.logger.error(f"Invalid regex pattern '{keyword}': {e}")
    
    def _check_include_keywords(self, text: str) -> Dict[str, Any]:
        """
        Check if any inclusion keywords match the text.
        
        Args:
            text: Text to search in
            
        Returns:
            Dict with 'passed' boolean and match details
        """
        if not self.keywords_include:
            return {'passed': True}
        
        matched_keywords = []
        
        for keyword, pattern in self._include_patterns:
            if pattern.search(text):
                matched_keywords.append(keyword)
        
        if matched_keywords:
            return {
                'passed': True,
                'reason': f"Found required keywords: {', '.join(matched_keywords)}",
                'matched': matched_keywords
            }
        else:
            return {
                'passed': False,
                'reason': f"None of required keywords found: {', '.join(self.keywords_include)}",
                'matched': []
            }
    
    def _check_exclude_keywords(self, text: str) -> Dict[str, Any]:
        """
        Check if any exclusion keywords match the text.
        
        Args:
            text: Text to search in
            
        Returns:
            Dict with 'passed' boolean and match details
        """
        if not self.keywords_exclude:
            return {'passed': True}
        
        matched_keywords = []
        
        for keyword, pattern in self._exclude_patterns:
            if pattern.search(text):
                matched_keywords.append(keyword)
        
        if matched_keywords:
            return {
                'passed': False,
                'reason': f"Found excluded keywords: {', '.join(matched_keywords)}",
                'matched': matched_keywords
            }
        else:
            return {
                'passed': True,
                'reason': "No excluded keywords found",
                'matched': []
            }
    
    def validate_config(self) -> List[str]:
        """
        Validate the keyword filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate keyword lists
        if not isinstance(self.keywords_include, list):
            errors.append("keywords_include must be a list")
        else:
            for i, keyword in enumerate(self.keywords_include):
                if not isinstance(keyword, str):
                    errors.append(f"keywords_include[{i}] must be a string")
                elif not keyword.strip():
                    errors.append(f"keywords_include[{i}] cannot be empty")
        
        if not isinstance(self.keywords_exclude, list):
            errors.append("keywords_exclude must be a list")
        else:
            for i, keyword in enumerate(self.keywords_exclude):
                if not isinstance(keyword, str):
                    errors.append(f"keywords_exclude[{i}] must be a string")
                elif not keyword.strip():
                    errors.append(f"keywords_exclude[{i}] cannot be empty")
        
        # Validate boolean options
        for option_name in ['case_sensitive', 'whole_words_only', 'search_title', 'search_selftext', 'regex_mode']:
            option_value = self.config.get(option_name)
            if option_value is not None and not isinstance(option_value, bool):
                errors.append(f"{option_name} must be a boolean")
        
        # Validate search fields
        if not self.search_title and not self.search_selftext:
            errors.append("At least one of search_title or search_selftext must be True")
        
        # Validate regex patterns if in regex mode
        if self.regex_mode:
            for keyword in self.keywords_include + self.keywords_exclude:
                try:
                    re.compile(keyword)
                except re.error as e:
                    errors.append(f"Invalid regex pattern '{keyword}': {e}")
        
        return errors
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for the keyword filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {
                "keywords_include": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Keywords that must be present (OR logic)",
                    "examples": [["python", "programming"], ["cat", "dog"]]
                },
                "keywords_exclude": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Keywords that must not be present",
                    "examples": [["spam", "advertisement"], ["politics"]]
                },
                "case_sensitive": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether matching is case-sensitive"
                },
                "whole_words_only": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether to match whole words only"
                },
                "search_title": {
                    "type": "boolean",
                    "default": True,
                    "description": "Whether to search in post title"
                },
                "search_selftext": {
                    "type": "boolean",
                    "default": True,
                    "description": "Whether to search in selftext"
                },
                "regex_mode": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether keywords are regex patterns"
                }
            },
            "additionalProperties": False,
            "examples": [
                {"keywords_include": ["python", "programming"]},
                {"keywords_exclude": ["spam", "advertisement"]},
                {
                    "keywords_include": ["machine learning", "AI"],
                    "keywords_exclude": ["clickbait"],
                    "case_sensitive": False,
                    "whole_words_only": True
                }
            ]
        }