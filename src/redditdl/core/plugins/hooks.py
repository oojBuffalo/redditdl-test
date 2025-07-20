"""
Plugin Hook Specifications

This module defines the hook specifications that plugins can implement.
Each hook specification defines the interface and contract for a specific
type of plugin functionality.
"""

import pluggy
from typing import Any, Dict, List, Optional, Union
from abc import ABC, abstractmethod

# Create hook specification markers
hookspec = pluggy.HookspecMarker("redditdl")


class ContentHandlerHooks:
    """Hook specifications for content handler plugins."""
    
    @hookspec
    def register_content_handler(self, handler_class, content_types: List[str], priority: int = 100):
        """Register a content handler for specific content types.
        
        Args:
            handler_class: The handler class implementing BaseContentHandler
            content_types: List of content types this handler supports
            priority: Handler priority (lower = higher priority)
        """
    
    @hookspec
    def get_content_handlers(self) -> List[Dict[str, Any]]:
        """Get all registered content handlers.
        
        Returns:
            List of handler information dictionaries
        """
    
    @hookspec
    def can_handle_content(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        """Check if a handler can process specific content.
        
        Args:
            content_type: Type of content to check
            post_data: Post metadata dictionary
            
        Returns:
            True if handler can process this content
        """
    
    @hookspec
    def process_content(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Process content using registered handlers.
        
        Args:
            post_data: Post data to process
            config: Processing configuration
            
        Returns:
            Processing results
        """


class FilterHooks:
    """Hook specifications for filter plugins."""
    
    @hookspec
    def register_filter(self, filter_class, filter_name: str, priority: int = 100):
        """Register a filter plugin.
        
        Args:
            filter_class: The filter class implementing BaseFilter
            filter_name: Unique name for the filter
            priority: Filter priority (lower = higher priority)
        """
    
    @hookspec
    def get_filters(self) -> List[Dict[str, Any]]:
        """Get all registered filters.
        
        Returns:
            List of filter information dictionaries
        """
    
    @hookspec
    def apply_filters(self, posts: List[Dict[str, Any]], 
                     config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply all registered filters to posts.
        
        Args:
            posts: List of post dictionaries to filter
            config: Filter configuration
            
        Returns:
            Filtered list of posts
        """


class ExporterHooks:
    """Hook specifications for exporter plugins."""
    
    @hookspec
    def register_exporter(self, exporter_class, format_name: str, file_extension: str):
        """Register an exporter plugin.
        
        Args:
            exporter_class: The exporter class implementing BaseExporter
            format_name: Human-readable format name
            file_extension: File extension for this format
        """
    
    @hookspec
    def get_exporters(self) -> List[Dict[str, Any]]:
        """Get all registered exporters.
        
        Returns:
            List of exporter information dictionaries
        """
    
    @hookspec
    def export_data(self, format_name: str, data: Dict[str, Any], 
                   output_path: str, config: Dict[str, Any]) -> bool:
        """Export data using a specific exporter.
        
        Args:
            format_name: Name of export format
            data: Data to export
            output_path: Output file path
            config: Export configuration
            
        Returns:
            True if export was successful
        """


class ScraperHooks:
    """Hook specifications for scraper plugins."""
    
    @hookspec
    def register_scraper(self, scraper_class, source_types: List[str], priority: int = 100):
        """Register a scraper plugin.
        
        Args:
            scraper_class: The scraper class implementing BaseScraper
            source_types: List of source types this scraper supports
            priority: Scraper priority (lower = higher priority)
        """
    
    @hookspec
    def get_scrapers(self) -> List[Dict[str, Any]]:
        """Get all registered scrapers.
        
        Returns:
            List of scraper information dictionaries
        """
    
    @hookspec
    def can_scrape_source(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        """Check if a scraper can handle a specific source.
        
        Args:
            source_type: Type of source to scrape
            source_config: Source configuration
            
        Returns:
            True if scraper can handle this source
        """
    
    @hookspec
    def scrape_source(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape content from a source.
        
        Args:
            source_config: Source configuration
            
        Returns:
            List of scraped posts/items
        """


# Base classes that plugins should implement
class BaseContentHandler(ABC):
    """Base class for content handler plugins."""
    
    @abstractmethod
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        """Check if this handler can process the content."""
        pass
    
    @abstractmethod
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Process the content and return results."""
        pass
    
    @abstractmethod
    def get_supported_types(self) -> List[str]:
        """Get list of supported content types."""
        pass


class BaseFilter(ABC):
    """Base class for filter plugins."""
    
    @abstractmethod
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply filter to posts and return filtered results."""
        pass
    
    @abstractmethod
    def get_config_schema(self) -> Dict[str, Any]:
        """Get configuration schema for this filter."""
        pass


class BaseExporter(ABC):
    """Base class for exporter plugins."""
    
    @abstractmethod
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> bool:
        """Export data to specified format and path."""
        pass
    
    @abstractmethod
    def get_format_info(self) -> Dict[str, str]:
        """Get format information (name, extension, description)."""
        pass


class BaseScraper(ABC):
    """Base class for scraper plugins."""
    
    @abstractmethod
    def can_scrape(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        """Check if this scraper can handle the source."""
        pass
    
    @abstractmethod
    async def scrape(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape data from source and return posts."""
        pass
    
    @abstractmethod
    def get_supported_sources(self) -> List[str]:
        """Get list of supported source types."""
        pass