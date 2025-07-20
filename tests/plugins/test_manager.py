#!/usr/bin/env python3
"""
Tests for the PluginManager class

Comprehensive test suite for plugin discovery, loading, lifecycle management,
validation, sandboxing, and conflict detection.
"""

import pytest
import tempfile
import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch, MagicMock

# Add current directory to path for imports
sys.path.insert(0, '.')

from redditdl.core.plugins.manager import PluginManager, PluginRegistry, PluginValidationError, PluginConflictError
from redditdl.core.plugins.hooks import BaseContentHandler, BaseFilter, BaseExporter, BaseScraper


class MockContentHandler(BaseContentHandler):
    """Mock content handler for testing."""
    
    priority = 100
    
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        return content_type == "mock_type"
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        return {"success": True, "handler": "mock"}
    
    def get_supported_types(self) -> List[str]:
        return ["mock_type"]


class MockFilter(BaseFilter):
    """Mock filter for testing."""
    
    priority = 100
    
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        return posts  # Pass through filter
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {"test_param": {"type": "string", "default": "test"}}


class MockExporter(BaseExporter):
    """Mock exporter for testing."""
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> bool:
        return True
    
    def get_format_info(self) -> Dict[str, str]:
        return {"name": "mock", "extension": ".mock", "description": "Mock format"}


class MockScraper(BaseScraper):
    """Mock scraper for testing."""
    
    priority = 100
    
    def can_scrape(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        return source_type == "mock_source"
    
    async def scrape(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []
    
    def get_supported_sources(self) -> List[str]:
        return ["mock_source"]


class TestPluginRegistry:
    """Test suite for PluginRegistry."""
    
    def test_registry_initialization(self):
        """Test PluginRegistry initialization."""
        registry = PluginRegistry()
        
        assert registry.content_handlers == {}
        assert registry.filters == {}
        assert registry.exporters == {}
        assert registry.scrapers == {}
        assert registry.plugin_metadata == {}
        assert registry._enabled_plugins == set()
        assert registry._plugin_dependencies == {}
    
    def test_plugin_enable_disable(self):
        """Test plugin enable/disable functionality."""
        registry = PluginRegistry()
        
        # Test enabling
        registry.enable_plugin("test_plugin")
        assert registry.is_enabled("test_plugin")
        
        # Test disabling
        registry.disable_plugin("test_plugin")
        assert not registry.is_enabled("test_plugin")
    
    def test_dependency_management(self):
        """Test plugin dependency management."""
        registry = PluginRegistry()
        
        # Add dependencies
        registry.add_dependency("plugin_a", "plugin_b")
        registry.add_dependency("plugin_a", "plugin_c")
        
        dependencies = registry.get_dependencies("plugin_a")
        assert "plugin_b" in dependencies
        assert "plugin_c" in dependencies
        assert len(dependencies) == 2
        
        # Test non-existent plugin
        assert registry.get_dependencies("nonexistent") == []


class TestPluginManager:
    """Test suite for PluginManager."""
    
    def test_manager_initialization(self):
        """Test PluginManager initialization."""
        manager = PluginManager()
        
        assert manager.plugin_dirs == []
        assert isinstance(manager.registry, PluginRegistry)
        assert manager.pm is not None
        assert manager._loaded_plugins == {}
        assert manager._plugin_modules == {}
        assert manager._initialization_order == []
        assert manager._sandbox_enabled is True
    
    def test_manager_with_plugin_dirs(self):
        """Test PluginManager with plugin directories."""
        plugin_dirs = ["/path/one", "/path/two"]
        manager = PluginManager(plugin_dirs=plugin_dirs)
        
        assert manager.plugin_dirs == plugin_dirs
    
    def test_discover_from_nonexistent_directory(self):
        """Test plugin discovery from non-existent directory."""
        manager = PluginManager(plugin_dirs=["/nonexistent/path"])
        
        # Should not raise exception, just log warning
        discovered = manager._discover_from_directory("/nonexistent/path")
        assert discovered == []
    
    def test_discover_from_directory_with_manifest(self):
        """Test plugin discovery from directory with plugin.json manifest."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a plugin directory with manifest
            plugin_dir = temp_path / "test_plugin"
            plugin_dir.mkdir()
            
            manifest = {
                "name": "test_plugin",
                "version": "1.0.0",
                "description": "Test plugin",
                "author": "Test Author"
            }
            
            manifest_file = plugin_dir / "plugin.json"
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f)
            
            manager = PluginManager()
            discovered = manager._discover_from_directory(str(temp_dir))
            
            assert len(discovered) == 1
            plugin_info = discovered[0]
            assert plugin_info['name'] == "test_plugin"
            assert plugin_info['version'] == "1.0.0"
            assert plugin_info['type'] == "directory"
            assert plugin_info['manifest'] == manifest
    
    def test_discover_from_directory_with_python_file(self):
        """Test plugin discovery from directory with Python files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a Python plugin file
            plugin_file = temp_path / "test_plugin.py"
            plugin_content = '''"""Test plugin module."""
__plugin_info__ = {
    "name": "test_plugin",
    "version": "1.0.0",
    "description": "Test plugin from Python file"
}
'''
            with open(plugin_file, 'w') as f:
                f.write(plugin_content)
            
            manager = PluginManager()
            discovered = manager._discover_from_directory(str(temp_dir))
            
            assert len(discovered) == 1
            plugin_info = discovered[0]
            assert plugin_info['name'] == "test_plugin"
            assert plugin_info['version'] == "1.0.0"
            assert plugin_info['type'] == "file"
    
    def test_plugin_validation_name_conflict(self):
        """Test plugin validation detects name conflicts."""
        manager = PluginManager()
        
        # Load first plugin
        plugin_info_1 = {
            "name": "test_plugin",
            "version": "1.0.0",
            "type": "mock"
        }
        manager._loaded_plugins["test_plugin"] = plugin_info_1
        
        # Try to validate second plugin with same name
        plugin_info_2 = {
            "name": "test_plugin",
            "version": "2.0.0",
            "type": "mock"
        }
        
        assert not manager._validate_plugin(plugin_info_2)
    
    def test_plugin_validation_missing_dependency(self):
        """Test plugin validation detects missing dependencies."""
        manager = PluginManager()
        
        plugin_info = {
            "name": "dependent_plugin",
            "version": "1.0.0",
            "type": "directory",
            "manifest": {
                "dependencies": ["missing_plugin"]
            }
        }
        
        assert not manager._validate_plugin(plugin_info)
    
    def test_plugin_validation_success(self):
        """Test successful plugin validation."""
        manager = PluginManager()
        
        plugin_info = {
            "name": "valid_plugin",
            "version": "1.0.0",
            "type": "mock",
            "path": "/tmp/valid"
        }
        
        with patch('pathlib.Path.exists', return_value=True):
            assert manager._validate_plugin(plugin_info)
    
    def test_register_content_handler(self):
        """Test content handler registration."""
        manager = PluginManager()
        
        manager._register_content_handler("test_plugin", MockContentHandler)
        
        handlers = manager.get_content_handlers()
        assert len(handlers) == 1
        
        handler_info = handlers[0]
        assert handler_info['plugin_name'] == "test_plugin"
        assert handler_info['class'] == MockContentHandler
        assert "mock_type" in handler_info['content_types']
    
    def test_register_filter(self):
        """Test filter registration."""
        manager = PluginManager()
        
        manager._register_filter("test_plugin", MockFilter)
        
        filters = manager.get_filters()
        assert len(filters) == 1
        
        filter_info = filters[0]
        assert filter_info['plugin_name'] == "test_plugin"
        assert filter_info['class'] == MockFilter
    
    def test_register_exporter(self):
        """Test exporter registration."""
        manager = PluginManager()
        
        manager._register_exporter("test_plugin", MockExporter)
        
        exporters = manager.get_exporters()
        assert len(exporters) == 1
        
        exporter_info = exporters[0]
        assert exporter_info['plugin_name'] == "test_plugin"
        assert exporter_info['class'] == MockExporter
    
    def test_register_scraper(self):
        """Test scraper registration."""
        manager = PluginManager()
        
        manager._register_scraper("test_plugin", MockScraper)
        
        scrapers = manager.get_scrapers()
        assert len(scrapers) == 1
        
        scraper_info = scrapers[0]
        assert scraper_info['plugin_name'] == "test_plugin"
        assert scraper_info['class'] == MockScraper
    
    def test_plugin_enable_disable_functionality(self):
        """Test plugin enable/disable functionality."""
        manager = PluginManager()
        
        # Create a mock plugin
        plugin_info = {
            "name": "test_plugin",
            "version": "1.0.0"
        }
        manager._loaded_plugins["test_plugin"] = plugin_info
        manager.registry.enable_plugin("test_plugin")
        
        # Test enabling
        assert manager.enable_plugin("test_plugin")
        assert manager.registry.is_enabled("test_plugin")
        
        # Test disabling
        assert manager.disable_plugin("test_plugin")
        assert not manager.registry.is_enabled("test_plugin")
        
        # Test enabling non-existent plugin
        assert not manager.enable_plugin("nonexistent")
        
        # Test disabling non-existent plugin
        assert not manager.disable_plugin("nonexistent")
    
    def test_plugin_status_reporting(self):
        """Test plugin status reporting."""
        manager = PluginManager()
        
        # Create mock plugin
        plugin_info = {
            "name": "test_plugin",
            "version": "1.0.0",
            "description": "Test plugin"
        }
        manager._loaded_plugins["test_plugin"] = plugin_info
        manager.registry.enable_plugin("test_plugin")
        
        # Register some components
        manager._register_content_handler("test_plugin", MockContentHandler)
        manager._register_filter("test_plugin", MockFilter)
        
        status = manager.get_plugin_status()
        
        assert "test_plugin" in status
        plugin_status = status["test_plugin"]
        assert plugin_status['loaded'] is True
        assert plugin_status['enabled'] is True
        assert plugin_status['content_handlers'] == 1
        assert plugin_status['filters'] == 1
        assert plugin_status['exporters'] == 0
        assert plugin_status['scrapers'] == 0
    
    def test_conflict_detection_content_handlers(self):
        """Test conflict detection for content handlers."""
        manager = PluginManager()
        
        # Register two handlers for the same content type
        manager.registry.enable_plugin("plugin1")
        manager.registry.enable_plugin("plugin2")
        
        handler_info_1 = {
            'plugin_name': 'plugin1',
            'class': MockContentHandler,
            'instance': MockContentHandler(),
            'content_types': ['test_type'],
            'priority': 100
        }
        handler_info_2 = {
            'plugin_name': 'plugin2',
            'class': MockContentHandler,
            'instance': MockContentHandler(),
            'content_types': ['test_type'],
            'priority': 100
        }
        
        manager.registry.content_handlers['plugin1.handler'] = handler_info_1
        manager.registry.content_handlers['plugin2.handler'] = handler_info_2
        
        conflicts = manager.detect_conflicts()
        
        assert len(conflicts) == 1
        conflict = conflicts[0]
        assert conflict['type'] == 'content_handler_conflict'
        assert conflict['content_type'] == 'test_type'
        assert len(conflict['conflicting_handlers']) == 2
    
    def test_conflict_detection_exporters(self):
        """Test conflict detection for exporters."""
        manager = PluginManager()
        
        # Register two exporters for the same format
        manager.registry.enable_plugin("plugin1")
        manager.registry.enable_plugin("plugin2")
        
        exporter_info_1 = {
            'plugin_name': 'plugin1',
            'class': MockExporter,
            'instance': MockExporter(),
            'format_info': {'name': 'test_format'}
        }
        exporter_info_2 = {
            'plugin_name': 'plugin2',
            'class': MockExporter,
            'instance': MockExporter(),
            'format_info': {'name': 'test_format'}
        }
        
        manager.registry.exporters['plugin1.exporter'] = exporter_info_1
        manager.registry.exporters['plugin2.exporter'] = exporter_info_2
        
        conflicts = manager.detect_conflicts()
        
        assert len(conflicts) == 1
        conflict = conflicts[0]
        assert conflict['type'] == 'exporter_conflict'
        assert conflict['format'] == 'test_format'
        assert len(conflict['conflicting_exporters']) == 2
    
    def test_plugin_sorting_by_dependencies(self):
        """Test plugin sorting by dependencies."""
        manager = PluginManager()
        
        plugins = [
            {
                "name": "plugin_c",
                "manifest": {"dependencies": ["plugin_a", "plugin_b"]}
            },
            {
                "name": "plugin_b",
                "manifest": {"dependencies": ["plugin_a"]}
            },
            {
                "name": "plugin_a",
                "manifest": {"dependencies": []}
            }
        ]
        
        sorted_plugins = manager._sort_plugins_by_dependencies(plugins)
        
        # plugin_a should come first, then plugin_b, then plugin_c
        names = [p["name"] for p in sorted_plugins]
        assert names.index("plugin_a") < names.index("plugin_b")
        assert names.index("plugin_b") < names.index("plugin_c")
    
    def test_sandbox_import_restriction(self):
        """Test sandbox import restrictions."""
        manager = PluginManager()
        manager._sandbox_enabled = True
        
        # Create a mock module
        mock_module = Mock()
        mock_module.__builtins__ = {}
        
        # Apply sandbox
        manager._apply_sandbox(mock_module)
        
        # Test that restricted import is blocked
        restricted_import = mock_module.__builtins__['__import__']
        
        with pytest.raises(ImportError, match="Import of 'os' is blocked"):
            restricted_import('os')
    
    def test_cleanup_functionality(self):
        """Test plugin manager cleanup."""
        manager = PluginManager()
        
        # Add some mock data
        plugin_info = {"name": "test_plugin", "version": "1.0.0"}
        manager._loaded_plugins["test_plugin"] = plugin_info
        manager._initialization_order.append("test_plugin")
        manager.registry.enable_plugin("test_plugin")
        
        # Cleanup
        manager.cleanup()
        
        # Verify everything is cleaned up
        assert manager._loaded_plugins == {}
        assert manager._initialization_order == []
        assert not manager.registry.is_enabled("test_plugin")


class TestPluginIntegration:
    """Integration tests for plugin system."""
    
    def test_plugin_lifecycle_integration(self):
        """Test complete plugin lifecycle."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create a simple plugin
            plugin_dir = temp_path / "integration_plugin"
            plugin_dir.mkdir()
            
            plugin_code = '''
from core.plugins.hooks import BaseContentHandler

class IntegrationHandler(BaseContentHandler):
    def can_handle(self, content_type, post_data):
        return content_type == "integration"
    
    async def process(self, post_data, config):
        return {"success": True, "integration": True}
    
    def get_supported_types(self):
        return ["integration"]

def initialize_plugin():
    pass

def cleanup_plugin():
    pass
'''
            
            init_file = plugin_dir / "__init__.py"
            with open(init_file, 'w') as f:
                f.write(plugin_code)
            
            manifest = {
                "name": "integration_plugin",
                "version": "1.0.0",
                "description": "Integration test plugin"
            }
            
            manifest_file = plugin_dir / "plugin.json"
            with open(manifest_file, 'w') as f:
                json.dump(manifest, f)
            
            # Test plugin manager with the directory
            manager = PluginManager(plugin_dirs=[str(temp_dir)])
            
            # Discover plugins
            discovered = manager.discover_plugins()
            assert len(discovered) == 1
            
            # Load the plugin
            plugin_info = discovered[0]
            with patch.object(manager, '_apply_sandbox'):
                success = manager.load_plugin(plugin_info)
                assert success
            
            # Check it's loaded and enabled
            assert "integration_plugin" in manager._loaded_plugins
            assert manager.registry.is_enabled("integration_plugin")
            
            # Check handlers are registered
            handlers = manager.get_content_handlers()
            assert len(handlers) == 1
            
            # Test unloading
            success = manager.unload_plugin("integration_plugin")
            assert success
            assert "integration_plugin" not in manager._loaded_plugins


if __name__ == "__main__":
    pytest.main([__file__])