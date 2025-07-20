"""
Date-based filtering for Reddit posts.

Filters posts based on their creation date using date range criteria.
Supports flexible date parsing and timezone handling.
"""

import time
from datetime import datetime, timezone
from dateutil import parser as date_parser
from typing import Any, Dict, List, Optional, Union
from redditdl.filters.base import Filter, FilterResult
from redditdl.scrapers import PostMetadata


class DateFilter(Filter):
    """
    Filter posts based on their creation date.
    
    Configuration options:
    - date_after: Posts created after this date (exclusive)
    - date_before: Posts created before this date (exclusive)
    - date_from: Posts created on or after this date (inclusive)
    - date_to: Posts created on or before this date (inclusive)
    
    Date formats supported:
    - ISO 8601: "2023-01-01T00:00:00Z"
    - Date only: "2023-01-01"
    - Human readable: "January 1, 2023"
    - Relative: "1 week ago", "2 days ago"
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the date filter.
        
        Args:
            config: Configuration dictionary with date criteria
        """
        super().__init__(config)
        
        # Parse date criteria
        self.date_after = self._parse_date(self.config.get('date_after'))
        self.date_before = self._parse_date(self.config.get('date_before'))
        self.date_from = self._parse_date(self.config.get('date_from'))
        self.date_to = self._parse_date(self.config.get('date_to'))
    
    @property
    def name(self) -> str:
        """Human-readable name of the filter."""
        return "Date Filter"
    
    @property
    def description(self) -> str:
        """Human-readable description of what the filter does."""
        criteria = []
        
        if self.date_after:
            criteria.append(f"after {self.date_after.strftime('%Y-%m-%d')}")
        if self.date_before:
            criteria.append(f"before {self.date_before.strftime('%Y-%m-%d')}")
        if self.date_from:
            criteria.append(f"from {self.date_from.strftime('%Y-%m-%d')}")
        if self.date_to:
            criteria.append(f"to {self.date_to.strftime('%Y-%m-%d')}")
        
        if criteria:
            return f"Posts created {', '.join(criteria)}"
        else:
            return "No date filtering (all posts pass)"
    
    def apply(self, post: PostMetadata) -> FilterResult:
        """
        Apply the date filter to a post.
        
        Args:
            post: Reddit post metadata to filter
            
        Returns:
            FilterResult indicating whether the post passed the filter
        """
        start_time = time.time()
        
        try:
            # Get the post creation date
            post_date = self._get_post_date(post)
            
            if post_date is None:
                self.logger.warning(f"Could not determine creation date for post {getattr(post, 'id', 'unknown')}")
                return FilterResult(
                    passed=True,  # Pass posts with unknown dates to be safe
                    reason="Post date unknown, passing by default",
                    metadata={
                        "post_date": None,
                        "date_after": self._format_date(self.date_after),
                        "date_before": self._format_date(self.date_before),
                        "date_from": self._format_date(self.date_from),
                        "date_to": self._format_date(self.date_to)
                    },
                    execution_time=time.time() - start_time
                )
            
            # If no date filtering is configured, pass all posts
            if not any([self.date_after, self.date_before, self.date_from, self.date_to]):
                return FilterResult(
                    passed=True,
                    reason="No date filter configured",
                    metadata={
                        "post_date": self._format_date(post_date)
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply date_after filter (exclusive)
            if self.date_after and post_date <= self.date_after:
                return FilterResult(
                    passed=False,
                    reason=f"Post date {self._format_date(post_date)} not after {self._format_date(self.date_after)}",
                    metadata={
                        "post_date": self._format_date(post_date),
                        "date_after": self._format_date(self.date_after),
                        "failed_criteria": "date_after"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply date_before filter (exclusive)
            if self.date_before and post_date >= self.date_before:
                return FilterResult(
                    passed=False,
                    reason=f"Post date {self._format_date(post_date)} not before {self._format_date(self.date_before)}",
                    metadata={
                        "post_date": self._format_date(post_date),
                        "date_before": self._format_date(self.date_before),
                        "failed_criteria": "date_before"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply date_from filter (inclusive)
            if self.date_from and post_date < self.date_from:
                return FilterResult(
                    passed=False,
                    reason=f"Post date {self._format_date(post_date)} before {self._format_date(self.date_from)}",
                    metadata={
                        "post_date": self._format_date(post_date),
                        "date_from": self._format_date(self.date_from),
                        "failed_criteria": "date_from"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Apply date_to filter (inclusive)
            if self.date_to and post_date > self.date_to:
                return FilterResult(
                    passed=False,
                    reason=f"Post date {self._format_date(post_date)} after {self._format_date(self.date_to)}",
                    metadata={
                        "post_date": self._format_date(post_date),
                        "date_to": self._format_date(self.date_to),
                        "failed_criteria": "date_to"
                    },
                    execution_time=time.time() - start_time
                )
            
            # Post passed all date criteria
            return FilterResult(
                passed=True,
                reason=f"Post date {self._format_date(post_date)} within date range",
                metadata={
                    "post_date": self._format_date(post_date),
                    "date_after": self._format_date(self.date_after),
                    "date_before": self._format_date(self.date_before),
                    "date_from": self._format_date(self.date_from),
                    "date_to": self._format_date(self.date_to)
                },
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Error applying date filter to post {getattr(post, 'id', 'unknown')}: {e}")
            return FilterResult(
                passed=False,
                reason=f"Filter error: {e}",
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def _parse_date(self, date_input: Union[str, datetime, None]) -> Optional[datetime]:
        """
        Parse a date from various input formats.
        
        Args:
            date_input: Date in string, datetime, or None
            
        Returns:
            Parsed datetime object or None
        """
        if date_input is None:
            return None
        
        if isinstance(date_input, datetime):
            # Ensure timezone awareness
            if date_input.tzinfo is None:
                return date_input.replace(tzinfo=timezone.utc)
            return date_input
        
        if isinstance(date_input, str):
            try:
                # Use dateutil parser for flexible date parsing
                parsed = date_parser.parse(date_input)
                # Ensure timezone awareness
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except (ValueError, TypeError) as e:
                self.logger.error(f"Could not parse date '{date_input}': {e}")
                return None
        
        self.logger.error(f"Invalid date format: {type(date_input)}")
        return None
    
    def _get_post_date(self, post: PostMetadata) -> Optional[datetime]:
        """
        Extract the creation date from a post.
        
        Args:
            post: Reddit post metadata
            
        Returns:
            Post creation datetime or None
        """
        # Try to get created_utc timestamp
        created_utc = getattr(post, 'created_utc', None)
        if created_utc is not None:
            try:
                if isinstance(created_utc, (int, float)):
                    return datetime.fromtimestamp(created_utc, tz=timezone.utc)
                elif isinstance(created_utc, str):
                    return self._parse_date(created_utc)
            except (ValueError, OSError) as e:
                self.logger.warning(f"Invalid created_utc timestamp: {created_utc}")
        
        # Try to get created field
        created = getattr(post, 'created', None)
        if created is not None:
            try:
                if isinstance(created, (int, float)):
                    return datetime.fromtimestamp(created, tz=timezone.utc)
                elif isinstance(created, str):
                    return self._parse_date(created)
                elif isinstance(created, datetime):
                    return created
            except (ValueError, OSError) as e:
                self.logger.warning(f"Invalid created timestamp: {created}")
        
        return None
    
    def _format_date(self, date_obj: Optional[datetime]) -> Optional[str]:
        """
        Format a datetime object for display.
        
        Args:
            date_obj: Datetime to format
            
        Returns:
            Formatted date string or None
        """
        if date_obj is None:
            return None
        return date_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    def validate_config(self) -> List[str]:
        """
        Validate the date filter configuration.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Check if dates were parsed successfully
        date_inputs = [
            ('date_after', self.config.get('date_after')),
            ('date_before', self.config.get('date_before')),
            ('date_from', self.config.get('date_from')),
            ('date_to', self.config.get('date_to'))
        ]
        
        for field_name, date_input in date_inputs:
            if date_input is not None:
                parsed = self._parse_date(date_input)
                if parsed is None:
                    errors.append(f"Could not parse {field_name}: {date_input}")
        
        # Validate logical consistency
        if self.date_after and self.date_before and self.date_after >= self.date_before:
            errors.append("date_after must be before date_before")
        
        if self.date_from and self.date_to and self.date_from > self.date_to:
            errors.append("date_from must be on or before date_to")
        
        # Check for conflicting criteria
        if self.date_after and self.date_from:
            errors.append("Cannot specify both date_after and date_from (use one or the other)")
        
        if self.date_before and self.date_to:
            errors.append("Cannot specify both date_before and date_to (use one or the other)")
        
        return errors
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get the configuration schema for the date filter.
        
        Returns:
            JSON schema describing the filter's configuration options
        """
        return {
            "type": "object",
            "properties": {
                "date_after": {
                    "type": "string",
                    "description": "Posts created after this date (exclusive)",
                    "format": "date-time",
                    "examples": ["2023-01-01", "2023-01-01T00:00:00Z", "1 week ago"]
                },
                "date_before": {
                    "type": "string",
                    "description": "Posts created before this date (exclusive)",
                    "format": "date-time",
                    "examples": ["2023-12-31", "2023-12-31T23:59:59Z", "yesterday"]
                },
                "date_from": {
                    "type": "string",
                    "description": "Posts created on or after this date (inclusive)",
                    "format": "date-time",
                    "examples": ["2023-01-01", "2023-01-01T00:00:00Z"]
                },
                "date_to": {
                    "type": "string",
                    "description": "Posts created on or before this date (inclusive)",
                    "format": "date-time",
                    "examples": ["2023-12-31", "2023-12-31T23:59:59Z"]
                }
            },
            "additionalProperties": False,
            "examples": [
                {"date_after": "2023-01-01"},
                {"date_from": "2023-01-01", "date_to": "2023-12-31"},
                {"date_before": "1 week ago"}
            ]
        }