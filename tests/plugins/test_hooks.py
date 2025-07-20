#!/usr/bin/env python3
"""
Tests for Plugin Hook Specifications

Comprehensive test suite for all plugin hook specifications and base classes.
Tests interface compliance, method signatures, and plugin registration.
"""

import pytest
import asyncio
import sys
from typing import Any, Dict, List
from unittest.mock import Mock, AsyncMock

# Add current directory to path for imports
sys.path.insert(0, '.')

from redditdl.core.plugins.hooks import (
    BaseContentHandler, BaseFilter, BaseExporter, BaseScraper,
    ContentHandlerHooks, FilterHooks, ExporterHooks, ScraperHooks
)


class TestContentHandlerHooks:
    """Test suite for ContentHandlerHooks."""
    
    def test_hook_specification_exists(self):
        """Test that ContentHandlerHooks has required hook specifications."""
        hooks = ContentHandlerHooks()
        
        # Check that hook methods exist
        assert hasattr(hooks, 'register_content_handler')
        assert hasattr(hooks, 'get_content_handlers')
        assert hasattr(hooks, 'can_handle_content')
        assert hasattr(hooks, 'process_content')
    
    def test_content_handler_interface(self):
        """Test that BaseContentHandler has required interface."""
        # Check abstract methods exist
        assert hasattr(BaseContentHandler, 'can_handle')
        assert hasattr(BaseContentHandler, 'process')
        assert hasattr(BaseContentHandler, 'get_supported_types')
        
        # Check that BaseContentHandler cannot be instantiated directly
        with pytest.raises(TypeError):
            BaseContentHandler()


class TestFilterHooks:
    """Test suite for FilterHooks."""
    
    def test_hook_specification_exists(self):
        """Test that FilterHooks has required hook specifications."""
        hooks = FilterHooks()
        
        # Check that hook methods exist
        assert hasattr(hooks, 'register_filter')
        assert hasattr(hooks, 'get_filters')
        assert hasattr(hooks, 'apply_filters')
    
    def test_filter_interface(self):
        """Test that BaseFilter has required interface."""
        # Check abstract methods exist
        assert hasattr(BaseFilter, 'apply')
        assert hasattr(BaseFilter, 'get_config_schema')
        
        # Check that BaseFilter cannot be instantiated directly
        with pytest.raises(TypeError):
            BaseFilter()


class TestExporterHooks:
    """Test suite for ExporterHooks."""
    
    def test_hook_specification_exists(self):
        """Test that ExporterHooks has required hook specifications."""
        hooks = ExporterHooks()
        
        # Check that hook methods exist
        assert hasattr(hooks, 'register_exporter')
        assert hasattr(hooks, 'get_exporters')
        assert hasattr(hooks, 'export_data')
    
    def test_exporter_interface(self):
        """Test that BaseExporter has required interface."""
        # Check abstract methods exist
        assert hasattr(BaseExporter, 'export')
        assert hasattr(BaseExporter, 'get_format_info')
        
        # Check that BaseExporter cannot be instantiated directly
        with pytest.raises(TypeError):
            BaseExporter()


class TestScraperHooks:
    """Test suite for ScraperHooks."""
    
    def test_hook_specification_exists(self):
        """Test that ScraperHooks has required hook specifications."""
        hooks = ScraperHooks()
        
        # Check that hook methods exist
        assert hasattr(hooks, 'register_scraper')
        assert hasattr(hooks, 'get_scrapers')
        assert hasattr(hooks, 'can_scrape_source')
        assert hasattr(hooks, 'scrape_source')
    
    def test_scraper_interface(self):
        """Test that BaseScraper has required interface."""
        # Check abstract methods exist
        assert hasattr(BaseScraper, 'can_scrape')
        assert hasattr(BaseScraper, 'scrape')
        assert hasattr(BaseScraper, 'get_supported_sources')
        
        # Check that BaseScraper cannot be instantiated directly
        with pytest.raises(TypeError):
            BaseScraper()


class ConcreteContentHandler(BaseContentHandler):
    """Concrete implementation for testing."""
    
    priority = 100
    
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        return content_type == "test_type"
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        return {"success": True, "processed": True}
    
    def get_supported_types(self) -> List[str]:
        return ["test_type"]


class ConcreteFilter(BaseFilter):
    """Concrete implementation for testing."""
    
    priority = 100
    
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        return posts  # Pass-through filter
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {"test_param": {"type": "string", "default": "test"}}


class ConcreteExporter(BaseExporter):
    """Concrete implementation for testing."""
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> bool:
        return True
    
    def get_format_info(self) -> Dict[str, str]:
        return {"name": "test", "extension": ".test"}


class ConcreteScraper(BaseScraper):
    """Concrete implementation for testing."""
    
    priority = 100
    
    def can_scrape(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        return source_type == "test_source"
    
    async def scrape(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []
    
    def get_supported_sources(self) -> List[str]:
        return ["test_source"]


class TestBaseContentHandler:
    """Test suite for BaseContentHandler implementations."""
    
    def test_concrete_implementation_works(self):
        """Test that concrete implementations work correctly."""
        handler = ConcreteContentHandler()
        
        # Test can_handle
        assert handler.can_handle("test_type", {})
        assert not handler.can_handle("other_type", {})
        
        # Test get_supported_types
        types = handler.get_supported_types()
        assert "test_type" in types
        
        # Test priority attribute
        assert hasattr(handler, 'priority')
        assert handler.priority == 100
    
    @pytest.mark.asyncio
    async def test_async_process_method(self):
        """Test async process method."""
        handler = ConcreteContentHandler()
        
        result = await handler.process({"test": "data"}, {"config": "value"})
        assert result["success"] is True
        assert result["processed"] is True
    
    def test_optional_methods_exist(self):
        """Test that optional methods exist and have default implementations."""
        handler = ConcreteContentHandler()
        
        # Test validate_config (optional method)
        if hasattr(handler, 'validate_config'):
            errors = handler.validate_config({})
            assert isinstance(errors, list)
        
        # Test get_config_schema (optional method)
        if hasattr(handler, 'get_config_schema'):
            schema = handler.get_config_schema()
            assert isinstance(schema, dict)


class TestBaseFilter:
    """Test suite for BaseFilter implementations."""
    
    def test_concrete_implementation_works(self):
        """Test that concrete implementations work correctly."""
        filter_obj = ConcreteFilter()
        
        # Test apply method
        posts = [{"id": "1"}, {"id": "2"}]
        result = filter_obj.apply(posts, {})
        assert result == posts
        
        # Test get_config_schema
        schema = filter_obj.get_config_schema()
        assert isinstance(schema, dict)
        assert "test_param" in schema
        
        # Test priority attribute
        assert hasattr(filter_obj, 'priority')
        assert filter_obj.priority == 100
    
    def test_filter_validation(self):
        """Test filter configuration validation."""
        filter_obj = ConcreteFilter()
        
        # Test validate_config if it exists
        if hasattr(filter_obj, 'validate_config'):
            errors = filter_obj.validate_config({"test_param": "value"})
            assert isinstance(errors, list)
    
    def test_filter_statistics(self):
        """Test filter statistics if supported."""
        filter_obj = ConcreteFilter()
        
        # Test get_statistics if it exists
        if hasattr(filter_obj, 'get_statistics'):
            posts_before = [{"id": "1"}, {"id": "2"}]
            posts_after = [{"id": "1"}]
            stats = filter_obj.get_statistics(posts_before, posts_after, {})
            assert isinstance(stats, dict)


class TestBaseExporter:
    """Test suite for BaseExporter implementations."""
    
    def test_concrete_implementation_works(self):
        """Test that concrete implementations work correctly."""
        exporter = ConcreteExporter()
        
        # Test export method
        result = exporter.export({"test": "data"}, "/tmp/test.out", {})
        assert result is True
        
        # Test get_format_info
        info = exporter.get_format_info()
        assert isinstance(info, dict)
        assert "name" in info
        assert "extension" in info
    
    def test_exporter_validation(self):
        """Test exporter configuration validation."""
        exporter = ConcreteExporter()
        
        # Test validate_config if it exists
        if hasattr(exporter, 'validate_config'):
            errors = exporter.validate_config({})
            assert isinstance(errors, list)
    
    def test_size_estimation(self):
        """Test output size estimation if supported."""
        exporter = ConcreteExporter()
        
        # Test estimate_output_size if it exists
        if hasattr(exporter, 'estimate_output_size'):
            size = exporter.estimate_output_size({"posts": []}, {})
            assert isinstance(size, (int, float))


class TestBaseScraper:
    """Test suite for BaseScraper implementations."""
    
    def test_concrete_implementation_works(self):
        """Test that concrete implementations work correctly."""
        scraper = ConcreteScraper()
        
        # Test can_scrape
        assert scraper.can_scrape("test_source", {})
        assert not scraper.can_scrape("other_source", {})
        
        # Test get_supported_sources
        sources = scraper.get_supported_sources()
        assert "test_source" in sources
        
        # Test priority attribute
        assert hasattr(scraper, 'priority')
        assert scraper.priority == 100
    
    @pytest.mark.asyncio
    async def test_async_scrape_method(self):
        """Test async scrape method."""
        scraper = ConcreteScraper()
        
        result = await scraper.scrape({"source": "test"})
        assert isinstance(result, list)
    
    def test_scraper_validation(self):
        """Test scraper configuration validation."""
        scraper = ConcreteScraper()
        
        # Test validate_config if it exists
        if hasattr(scraper, 'validate_config'):
            errors = scraper.validate_config({})
            assert isinstance(errors, list)
    
    def test_rate_limiting(self):
        """Test rate limiting if supported."""
        scraper = ConcreteScraper()
        
        # Test get_rate_limit if it exists
        if hasattr(scraper, 'get_rate_limit'):
            rate_limit = scraper.get_rate_limit()
            assert isinstance(rate_limit, (int, float))


class TestPluginInterfaceCompliance:
    """Test suite for plugin interface compliance."""
    
    def test_all_base_classes_are_abstract(self):
        """Test that all base classes are abstract and cannot be instantiated."""
        base_classes = [BaseContentHandler, BaseFilter, BaseExporter, BaseScraper]
        
        for base_class in base_classes:
            with pytest.raises(TypeError):
                base_class()
    
    def test_concrete_implementations_instantiate(self):
        """Test that concrete implementations can be instantiated."""
        concrete_classes = [ConcreteContentHandler, ConcreteFilter, ConcreteExporter, ConcreteScraper]
        
        for concrete_class in concrete_classes:
            instance = concrete_class()
            assert instance is not None
    
    def test_method_signatures_match(self):
        """Test that concrete implementations have correct method signatures."""
        # Test ContentHandler
        handler = ConcreteContentHandler()
        assert callable(handler.can_handle)
        assert callable(handler.process)
        assert callable(handler.get_supported_types)
        
        # Test Filter
        filter_obj = ConcreteFilter()
        assert callable(filter_obj.apply)
        assert callable(filter_obj.get_config_schema)
        
        # Test Exporter
        exporter = ConcreteExporter()
        assert callable(exporter.export)
        assert callable(exporter.get_format_info)
        
        # Test Scraper
        scraper = ConcreteScraper()
        assert callable(scraper.can_scrape)
        assert callable(scraper.scrape)
        assert callable(scraper.get_supported_sources)
    
    @pytest.mark.asyncio
    async def test_async_methods_work(self):
        """Test that async methods work correctly."""
        # Test ContentHandler async method
        handler = ConcreteContentHandler()
        result = await handler.process({}, {})
        assert isinstance(result, dict)
        
        # Test Scraper async method
        scraper = ConcreteScraper()
        result = await scraper.scrape({})
        assert isinstance(result, list)
    
    def test_priority_attributes(self):
        """Test that priority attributes exist and are valid."""
        priority_classes = [ConcreteContentHandler, ConcreteFilter, ConcreteScraper]
        
        for cls in priority_classes:
            instance = cls()
            assert hasattr(instance, 'priority')
            assert isinstance(instance.priority, int)
            assert instance.priority > 0
    
    def test_return_type_compliance(self):
        """Test that methods return expected types."""
        # Test ContentHandler
        handler = ConcreteContentHandler()
        assert isinstance(handler.can_handle("test", {}), bool)
        assert isinstance(handler.get_supported_types(), list)
        
        # Test Filter
        filter_obj = ConcreteFilter()
        assert isinstance(filter_obj.apply([], {}), list)
        assert isinstance(filter_obj.get_config_schema(), dict)
        
        # Test Exporter
        exporter = ConcreteExporter()
        assert isinstance(exporter.export({}, "", {}), bool)
        assert isinstance(exporter.get_format_info(), dict)
        
        # Test Scraper
        scraper = ConcreteScraper()
        assert isinstance(scraper.can_scrape("test", {}), bool)
        assert isinstance(scraper.get_supported_sources(), list)


if __name__ == "__main__":
    pytest.main([__file__])