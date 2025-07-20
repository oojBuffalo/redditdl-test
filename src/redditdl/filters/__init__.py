"""
Filtering System for Reddit Posts

This module provides a comprehensive filtering system for Reddit posts based on
various criteria such as score thresholds, date ranges, keywords, domains,
media types, and NSFW status. The filtering system implements the Pipeline &
Filter pattern for composable and chainable filters.

Key Components:
- Filter: Abstract base class for all filters
- FilterFactory: Factory for creating filters from configuration
- Specialized filter implementations for different criteria
- Filter composition utilities for AND/OR logic
"""

from .base import Filter, FilterResult, FilterComposition, FilterChain
from .factory import FilterFactory
from .score import ScoreFilter
from .date import DateFilter
from .keyword import KeywordFilter
from .domain import DomainFilter
from .media_type import MediaTypeFilter
from .nsfw import NSFWFilter

__all__ = [
    "Filter",
    "FilterResult", 
    "FilterComposition",
    "FilterChain",
    "FilterFactory",
    "ScoreFilter",
    "DateFilter",
    "KeywordFilter",
    "DomainFilter",
    "MediaTypeFilter",
    "NSFWFilter",
]