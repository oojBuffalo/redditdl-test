"""
Target Resolution System

This module provides target resolution and validation for different Reddit content sources
including users, subreddits, URLs, and special targets like saved/upvoted posts.

Also provides enhanced scrapers that implement a unified interface for consistent
behavior and plugin compatibility.
"""

from .resolver import TargetResolver, TargetType, TargetInfo
from .base_scraper import (
    BaseScraper, 
    ScrapingConfig, 
    ScrapingError, 
    AuthenticationError, 
    TargetNotFoundError,
    RateLimitError
)
from .scrapers import EnhancedPrawScraper, EnhancedYarsScraper, ScraperFactory

__all__ = [
    # Target resolution
    'TargetResolver',
    'TargetType', 
    'TargetInfo',
    
    # Base scraper interfaces
    'BaseScraper',
    'ScrapingConfig',
    'ScrapingError',
    'AuthenticationError',
    'TargetNotFoundError', 
    'RateLimitError',
    
    # Enhanced scrapers
    'EnhancedPrawScraper',
    'EnhancedYarsScraper',
    'ScraperFactory'
]