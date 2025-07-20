"""
Content Handlers for RedditDL

This package provides content handlers for processing different types of Reddit posts
including media downloads, text posts, galleries, polls, crossposts, and external links.
The handler system supports plugin-based extensions and priority-based selection.
"""

from redditdl.content_handlers.base import (
    BaseContentHandler,
    ContentHandlerRegistry,
    ContentTypeDetector,
    HandlerResult,
    HandlerError
)

__all__ = [
    'BaseContentHandler',
    'ContentHandlerRegistry', 
    'ContentTypeDetector',
    'HandlerResult',
    'HandlerError'
]