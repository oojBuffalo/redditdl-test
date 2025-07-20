"""
Plugin Manager

Central management system for RedditDL plugins, providing discovery, loading,
validation, lifecycle management, and sandboxing capabilities.
"""

import logging
import sys
import importlib
import importlib.util
import inspect
import json
import weakref
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type, Union, Callable
from collections import defaultdict
import pluggy

from .hooks import (
    ContentHandlerHooks, FilterHooks, ExporterHooks, ScraperHooks,
    BaseContentHandler, BaseFilter, BaseExporter, BaseScraper
)


class PluginValidationError(Exception):
    """Raised when plugin validation fails."""
    pass


class PluginConflictError(Exception):
    """Raised when plugin conflicts are detected."""
    pass


class PluginRegistry:
    """Registry for tracking loaded plugins and their metadata."""
    
    def __init__(self):
        self.content_handlers: Dict[str, Dict[str, Any]] = {}
        self.filters: Dict[str, Dict[str, Any]] = {}
        self.exporters: Dict[str, Dict[str, Any]] = {}
        self.scrapers: Dict[str, Dict[str, Any]] = {}
        self.plugin_metadata: Dict[str, Dict[str, Any]] = {}
        self._enabled_plugins: Set[str] = set()
        self._plugin_dependencies: Dict[str, List[str]] = {}
    
    def is_enabled(self, plugin_name: str) -> bool:
        """Check if a plugin is enabled."""
        return plugin_name in self._enabled_plugins
    
    def enable_plugin(self, plugin_name: str) -> None:
        """Enable a plugin."""
        self._enabled_plugins.add(plugin_name)
    
    def disable_plugin(self, plugin_name: str) -> None:
        """Disable a plugin."""
        self._enabled_plugins.discard(plugin_name)
    
    def add_dependency(self, plugin_name: str, dependency: str) -> None:
        """Add a dependency for a plugin."""
        if plugin_name not in self._plugin_dependencies:
            self._plugin_dependencies[plugin_name] = []
        self._plugin_dependencies[plugin_name].append(dependency)
    
    def get_dependencies(self, plugin_name: str) -> List[str]:
        """Get dependencies for a plugin."""
        return self._plugin_dependencies.get(plugin_name, [])


class PluginManager:
    """
    Central plugin management system for RedditDL.
    
    Provides comprehensive plugin functionality including:
    - Plugin discovery from directories and entry points
    - Plugin loading with validation and sandboxing
    - Lifecycle management (load, initialize, cleanup)
    - Conflict detection and resolution
    - Plugin enable/disable functionality
    """
    
    def __init__(self, plugin_dirs: Optional[List[str]] = None):
        """
        Initialize the plugin manager.
        
        Args:
            plugin_dirs: List of directories to search for plugins
        """
        self.logger = logging.getLogger("plugins.manager")
        self.plugin_dirs = plugin_dirs or []
        self.registry = PluginRegistry()
        
        # Create pluggy plugin manager
        self.pm = pluggy.PluginManager("redditdl")
        self.pm.add_hookspecs(ContentHandlerHooks)
        self.pm.add_hookspecs(FilterHooks)
        self.pm.add_hookspecs(ExporterHooks)
        self.pm.add_hookspecs(ScraperHooks)
        
        # Plugin loading state
        self._loaded_plugins: Dict[str, Any] = {}
        self._plugin_modules: Dict[str, Any] = {}
        self._initialization_order: List[str] = []
        
        # Security settings
        self._sandbox_enabled = True
        self._allowed_imports: Set[str] = {
            'json', 'csv', 'pathlib', 'datetime', 'typing', 'dataclasses',
            'urllib.parse', 'base64', 'hashlib', 'uuid', 're'
        }
        self._blocked_imports: Set[str] = {
            'os', 'subprocess', 'sys', 'socket', 'requests'
        }
    
    def discover_plugins(self) -> List[Dict[str, Any]]:
        """
        Discover all available plugins from configured sources.
        
        Returns:
            List of plugin metadata dictionaries
        """
        discovered = []
        
        # Discover from plugin directories
        for plugin_dir in self.plugin_dirs:
            discovered.extend(self._discover_from_directory(plugin_dir))
        
        # Discover from entry points (if available)
        discovered.extend(self._discover_from_entry_points())
        
        self.logger.info(f"Discovered {len(discovered)} plugins")
        return discovered
    
    def _discover_from_directory(self, plugin_dir: str) -> List[Dict[str, Any]]:
        """Discover plugins from a directory."""
        plugins = []
        plugin_path = Path(plugin_dir)
        
        if not plugin_path.exists():
            self.logger.warning(f"Plugin directory does not exist: {plugin_dir}")
            return plugins
        
        # Look for Python files and plugin manifests
        for item in plugin_path.iterdir():
            if item.is_file() and item.suffix == '.py':
                plugin_info = self._extract_plugin_info(item)
                if plugin_info:
                    plugins.append(plugin_info)
            elif item.is_dir():
                # Check for plugin.json manifest
                manifest_path = item / "plugin.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path, 'r') as f:
                            manifest = json.load(f)
                        
                        plugin_info = {
                            'name': manifest.get('name', item.name),
                            'version': manifest.get('version', '1.0.0'),
                            'description': manifest.get('description', ''),
                            'author': manifest.get('author', ''),
                            'path': str(item),
                            'manifest': manifest,
                            'type': 'directory'
                        }
                        plugins.append(plugin_info)
                        
                    except Exception as e:
                        self.logger.error(f"Failed to read manifest {manifest_path}: {e}")
        
        return plugins
    
    def _discover_from_entry_points(self) -> List[Dict[str, Any]]:
        """Discover plugins from entry points."""
        plugins = []
        
        try:
            # Try to use importlib.metadata for Python 3.8+
            try:
                from importlib.metadata import entry_points
                eps = entry_points(group='redditdl.plugins')
            except ImportError:
                # Fallback for older Python versions
                from pkg_resources import iter_entry_points
                eps = iter_entry_points('redditdl.plugins')
            
            for ep in eps:
                plugin_info = {
                    'name': ep.name,
                    'version': getattr(ep.dist, 'version', '1.0.0'),
                    'description': f"Entry point plugin: {ep.name}",
                    'author': '',
                    'entry_point': ep,
                    'type': 'entry_point'
                }
                plugins.append(plugin_info)
                
        except Exception as e:
            self.logger.debug(f"Failed to discover entry point plugins: {e}")
        
        return plugins
    
    def _extract_plugin_info(self, plugin_file: Path) -> Optional[Dict[str, Any]]:
        """Extract plugin information from a Python file."""
        try:
            # Read the file and look for plugin metadata
            with open(plugin_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Look for plugin metadata in docstring or comments
            plugin_info = {
                'name': plugin_file.stem,
                'version': '1.0.0',
                'description': f"Plugin from {plugin_file.name}",
                'author': '',
                'path': str(plugin_file),
                'type': 'file'
            }
            
            # Try to extract metadata from module docstring
            try:
                spec = importlib.util.spec_from_file_location(plugin_file.stem, plugin_file)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    if hasattr(module, '__plugin_info__'):
                        plugin_info.update(module.__plugin_info__)
                    elif module.__doc__:
                        plugin_info['description'] = module.__doc__.strip().split('\n')[0]
            
            except Exception as e:
                self.logger.debug(f"Could not extract detailed info from {plugin_file}: {e}")
            
            return plugin_info
            
        except Exception as e:
            self.logger.error(f"Failed to extract plugin info from {plugin_file}: {e}")
            return None
    
    def load_plugin(self, plugin_info: Dict[str, Any]) -> bool:
        """
        Load a single plugin with validation and sandboxing.
        
        Args:
            plugin_info: Plugin metadata dictionary
            
        Returns:
            True if plugin was loaded successfully
        """
        plugin_name = plugin_info['name']
        
        if plugin_name in self._loaded_plugins:
            self.logger.warning(f"Plugin '{plugin_name}' is already loaded")
            return False
        
        try:
            self.logger.info(f"Loading plugin: {plugin_name}")
            
            # Validate plugin before loading
            if not self._validate_plugin(plugin_info):
                raise PluginValidationError(f"Plugin validation failed: {plugin_name}")
            
            # Load the plugin module
            plugin_module = self._load_plugin_module(plugin_info)
            if not plugin_module:
                return False
            
            # Register plugin hooks
            self._register_plugin_hooks(plugin_name, plugin_module)
            
            # Store plugin information
            self._loaded_plugins[plugin_name] = plugin_info
            self._plugin_modules[plugin_name] = plugin_module
            self.registry.plugin_metadata[plugin_name] = plugin_info
            self.registry.enable_plugin(plugin_name)
            
            # Initialize plugin if it has an initialization function
            if hasattr(plugin_module, 'initialize_plugin'):
                try:
                    plugin_module.initialize_plugin()
                    self.logger.debug(f"Initialized plugin: {plugin_name}")
                except Exception as e:
                    self.logger.error(f"Plugin initialization failed for {plugin_name}: {e}")
                    self.unload_plugin(plugin_name)
                    return False
            
            self._initialization_order.append(plugin_name)
            self.logger.info(f"Successfully loaded plugin: {plugin_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load plugin '{plugin_name}': {e}")
            return False
    
    def _validate_plugin(self, plugin_info: Dict[str, Any]) -> bool:
        """Validate a plugin before loading."""
        plugin_name = plugin_info['name']
        
        # Check for name conflicts
        if plugin_name in self._loaded_plugins:
            self.logger.error(f"Plugin name conflict: {plugin_name}")
            return False
        
        # Validate plugin path exists
        if 'path' in plugin_info:
            plugin_path = Path(plugin_info['path'])
            if not plugin_path.exists():
                self.logger.error(f"Plugin path does not exist: {plugin_path}")
                return False
        
        # Check dependencies if specified
        if 'manifest' in plugin_info:
            manifest = plugin_info['manifest']
            dependencies = manifest.get('dependencies', [])
            
            for dep in dependencies:
                if dep not in self._loaded_plugins:
                    self.logger.error(f"Plugin {plugin_name} requires unloaded dependency: {dep}")
                    return False
        
        return True
    
    def _load_plugin_module(self, plugin_info: Dict[str, Any]) -> Optional[Any]:
        """Load the plugin module."""
        plugin_name = plugin_info['name']
        
        try:
            if plugin_info['type'] == 'entry_point':
                # Load from entry point
                entry_point = plugin_info['entry_point']
                module = entry_point.load()
                
            elif plugin_info['type'] == 'file':
                # Load from Python file
                plugin_path = Path(plugin_info['path'])
                spec = importlib.util.spec_from_file_location(plugin_name, plugin_path)
                if not spec or not spec.loader:
                    raise ImportError(f"Could not create spec for {plugin_path}")
                
                module = importlib.util.module_from_spec(spec)
                
                # Apply sandboxing if enabled
                if self._sandbox_enabled:
                    self._apply_sandbox(module)
                
                spec.loader.exec_module(module)
                
            elif plugin_info['type'] == 'directory':
                # Load from directory with __init__.py
                plugin_path = Path(plugin_info['path'])
                init_file = plugin_path / "__init__.py"
                
                if not init_file.exists():
                    raise ImportError(f"No __init__.py found in {plugin_path}")
                
                spec = importlib.util.spec_from_file_location(plugin_name, init_file)
                if not spec or not spec.loader:
                    raise ImportError(f"Could not create spec for {init_file}")
                
                module = importlib.util.module_from_spec(spec)
                
                # Apply sandboxing if enabled
                if self._sandbox_enabled:
                    self._apply_sandbox(module)
                
                spec.loader.exec_module(module)
                
            else:
                raise ValueError(f"Unknown plugin type: {plugin_info['type']}")
            
            return module
            
        except Exception as e:
            self.logger.error(f"Failed to load module for plugin {plugin_name}: {e}")
            return None
    
    def _apply_sandbox(self, module: Any) -> None:
        """Apply sandboxing restrictions to a plugin module."""
        # This is a basic sandboxing implementation
        # In production, you might want more sophisticated sandboxing
        
        # Override __import__ to restrict imports
        import builtins
        original_import = builtins.__import__
        
        def restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name in self._blocked_imports:
                raise ImportError(f"Import of '{name}' is blocked for security reasons")
            
            if name not in self._allowed_imports and not name.startswith('redditdl.'):
                self.logger.warning(f"Plugin attempting to import potentially unsafe module: {name}")
            
            return original_import(name, globals, locals, fromlist, level)
        
        # Apply import restriction to module
        module.__builtins__ = module.__builtins__.copy() if hasattr(module, '__builtins__') else {}
        module.__builtins__['__import__'] = restricted_import
    
    def _register_plugin_hooks(self, plugin_name: str, plugin_module: Any) -> None:
        """Register plugin hooks with the pluggy manager."""
        # Register the plugin module with pluggy
        self.pm.register(plugin_module, name=plugin_name)
        
        # Extract and register specific hook implementations
        for attr_name in dir(plugin_module):
            attr = getattr(plugin_module, attr_name)
            
            # Look for classes that implement our base interfaces
            if inspect.isclass(attr):
                if issubclass(attr, BaseContentHandler):
                    self._register_content_handler(plugin_name, attr)
                elif issubclass(attr, BaseFilter):
                    self._register_filter(plugin_name, attr)
                elif issubclass(attr, BaseExporter):
                    self._register_exporter(plugin_name, attr)
                elif issubclass(attr, BaseScraper):
                    self._register_scraper(plugin_name, attr)
    
    def _register_content_handler(self, plugin_name: str, handler_class: Type[BaseContentHandler]) -> None:
        """Register a content handler from a plugin."""
        try:
            handler_instance = handler_class()
            content_types = handler_instance.get_supported_types()
            
            handler_info = {
                'plugin_name': plugin_name,
                'class': handler_class,
                'instance': handler_instance,
                'content_types': content_types,
                'priority': getattr(handler_class, 'priority', 100)
            }
            
            handler_id = f"{plugin_name}.{handler_class.__name__}"
            self.registry.content_handlers[handler_id] = handler_info
            
            # Enable the plugin if not already enabled
            if not self.registry.is_enabled(plugin_name):
                self.registry.enable_plugin(plugin_name)
            
            self.logger.debug(f"Registered content handler: {handler_id} for types {content_types}")
            
        except Exception as e:
            self.logger.error(f"Failed to register content handler from {plugin_name}: {e}")
    
    def _register_filter(self, plugin_name: str, filter_class: Type[BaseFilter]) -> None:
        """Register a filter from a plugin."""
        try:
            filter_instance = filter_class()
            
            filter_info = {
                'plugin_name': plugin_name,
                'class': filter_class,
                'instance': filter_instance,
                'priority': getattr(filter_class, 'priority', 100),
                'schema': filter_instance.get_config_schema()
            }
            
            filter_id = f"{plugin_name}.{filter_class.__name__}"
            self.registry.filters[filter_id] = filter_info
            
            # Enable the plugin if not already enabled
            if not self.registry.is_enabled(plugin_name):
                self.registry.enable_plugin(plugin_name)
            
            self.logger.debug(f"Registered filter: {filter_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to register filter from {plugin_name}: {e}")
    
    def _register_exporter(self, plugin_name: str, exporter_class: Type[BaseExporter]) -> None:
        """Register an exporter from a plugin."""
        try:
            exporter_instance = exporter_class()
            format_info = exporter_instance.get_format_info()
            
            exporter_info = {
                'plugin_name': plugin_name,
                'class': exporter_class,
                'instance': exporter_instance,
                'format_info': format_info
            }
            
            exporter_id = f"{plugin_name}.{exporter_class.__name__}"
            self.registry.exporters[exporter_id] = exporter_info
            
            # Enable the plugin if not already enabled
            if not self.registry.is_enabled(plugin_name):
                self.registry.enable_plugin(plugin_name)
            
            self.logger.debug(f"Registered exporter: {exporter_id} for format {format_info.get('name')}")
            
        except Exception as e:
            self.logger.error(f"Failed to register exporter from {plugin_name}: {e}")
    
    def _register_scraper(self, plugin_name: str, scraper_class: Type[BaseScraper]) -> None:
        """Register a scraper from a plugin."""
        try:
            scraper_instance = scraper_class()
            source_types = scraper_instance.get_supported_sources()
            
            scraper_info = {
                'plugin_name': plugin_name,
                'class': scraper_class,
                'instance': scraper_instance,
                'source_types': source_types,
                'priority': getattr(scraper_class, 'priority', 100)
            }
            
            scraper_id = f"{plugin_name}.{scraper_class.__name__}"
            self.registry.scrapers[scraper_id] = scraper_info
            
            # Enable the plugin if not already enabled
            if not self.registry.is_enabled(plugin_name):
                self.registry.enable_plugin(plugin_name)
            
            self.logger.debug(f"Registered scraper: {scraper_id} for sources {source_types}")
            
        except Exception as e:
            self.logger.error(f"Failed to register scraper from {plugin_name}: {e}")
    
    def unload_plugin(self, plugin_name: str) -> bool:
        """
        Unload a plugin and clean up its resources.
        
        Args:
            plugin_name: Name of plugin to unload
            
        Returns:
            True if plugin was unloaded successfully
        """
        if plugin_name not in self._loaded_plugins:
            self.logger.warning(f"Plugin '{plugin_name}' is not loaded")
            return False
        
        try:
            self.logger.info(f"Unloading plugin: {plugin_name}")
            
            # Call cleanup function if available
            plugin_module = self._plugin_modules.get(plugin_name)
            if plugin_module and hasattr(plugin_module, 'cleanup_plugin'):
                try:
                    plugin_module.cleanup_plugin()
                    self.logger.debug(f"Cleaned up plugin: {plugin_name}")
                except Exception as e:
                    self.logger.error(f"Plugin cleanup failed for {plugin_name}: {e}")
            
            # Unregister from pluggy
            if plugin_module:
                self.pm.unregister(plugin_module, name=plugin_name)
            
            # Remove from registries
            self._remove_plugin_from_registries(plugin_name)
            
            # Clean up references
            del self._loaded_plugins[plugin_name]
            if plugin_name in self._plugin_modules:
                del self._plugin_modules[plugin_name]
            if plugin_name in self.registry.plugin_metadata:
                del self.registry.plugin_metadata[plugin_name]
            
            self.registry.disable_plugin(plugin_name)
            
            if plugin_name in self._initialization_order:
                self._initialization_order.remove(plugin_name)
            
            self.logger.info(f"Successfully unloaded plugin: {plugin_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unload plugin '{plugin_name}': {e}")
            return False
    
    def _remove_plugin_from_registries(self, plugin_name: str) -> None:
        """Remove all entries for a plugin from registries."""
        # Remove content handlers
        to_remove = [k for k, v in self.registry.content_handlers.items() 
                    if v['plugin_name'] == plugin_name]
        for key in to_remove:
            del self.registry.content_handlers[key]
        
        # Remove filters
        to_remove = [k for k, v in self.registry.filters.items() 
                    if v['plugin_name'] == plugin_name]
        for key in to_remove:
            del self.registry.filters[key]
        
        # Remove exporters
        to_remove = [k for k, v in self.registry.exporters.items() 
                    if v['plugin_name'] == plugin_name]
        for key in to_remove:
            del self.registry.exporters[key]
        
        # Remove scrapers
        to_remove = [k for k, v in self.registry.scrapers.items() 
                    if v['plugin_name'] == plugin_name]
        for key in to_remove:
            del self.registry.scrapers[key]
    
    def load_all_plugins(self) -> int:
        """
        Load all discovered plugins.
        
        Returns:
            Number of successfully loaded plugins
        """
        discovered = self.discover_plugins()
        loaded_count = 0
        
        # Sort by dependencies and priority
        sorted_plugins = self._sort_plugins_by_dependencies(discovered)
        
        for plugin_info in sorted_plugins:
            if self.load_plugin(plugin_info):
                loaded_count += 1
        
        self.logger.info(f"Loaded {loaded_count} of {len(discovered)} discovered plugins")
        return loaded_count
    
    def _sort_plugins_by_dependencies(self, plugins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort plugins by dependencies to ensure proper loading order."""
        # Simple topological sort for now
        # In a production system, you'd want a more sophisticated dependency resolver
        
        sorted_plugins = []
        remaining_plugins = plugins.copy()
        
        while remaining_plugins:
            # Find plugins with no unmet dependencies
            loadable = []
            for plugin in remaining_plugins:
                dependencies = []
                if 'manifest' in plugin:
                    dependencies = plugin['manifest'].get('dependencies', [])
                
                if all(dep in [p['name'] for p in sorted_plugins] for dep in dependencies):
                    loadable.append(plugin)
            
            if not loadable:
                # Circular dependency or missing dependency
                self.logger.warning("Circular or missing dependencies detected, loading remaining plugins anyway")
                sorted_plugins.extend(remaining_plugins)
                break
            
            # Add loadable plugins to sorted list
            sorted_plugins.extend(loadable)
            
            # Remove from remaining
            for plugin in loadable:
                remaining_plugins.remove(plugin)
        
        return sorted_plugins
    
    def get_content_handlers(self) -> List[Dict[str, Any]]:
        """Get all registered content handlers."""
        return [info for info in self.registry.content_handlers.values() 
               if self.registry.is_enabled(info['plugin_name'])]
    
    def get_filters(self) -> List[Dict[str, Any]]:
        """Get all registered filters."""
        return [info for info in self.registry.filters.values() 
               if self.registry.is_enabled(info['plugin_name'])]
    
    def get_exporters(self) -> List[Dict[str, Any]]:
        """Get all registered exporters."""
        return [info for info in self.registry.exporters.values() 
               if self.registry.is_enabled(info['plugin_name'])]
    
    def get_scrapers(self) -> List[Dict[str, Any]]:
        """Get all registered scrapers."""
        return [info for info in self.registry.scrapers.values() 
               if self.registry.is_enabled(info['plugin_name'])]
    
    def enable_plugin(self, plugin_name: str) -> bool:
        """Enable a loaded plugin."""
        if plugin_name not in self._loaded_plugins:
            self.logger.error(f"Cannot enable unloaded plugin: {plugin_name}")
            return False
        
        self.registry.enable_plugin(plugin_name)
        self.logger.info(f"Enabled plugin: {plugin_name}")
        return True
    
    def disable_plugin(self, plugin_name: str) -> bool:
        """Disable a plugin without unloading it."""
        if plugin_name not in self._loaded_plugins:
            self.logger.error(f"Cannot disable unloaded plugin: {plugin_name}")
            return False
        
        self.registry.disable_plugin(plugin_name)
        self.logger.info(f"Disabled plugin: {plugin_name}")
        return True
    
    def get_plugin_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status information for all plugins."""
        status = {}
        
        for plugin_name, plugin_info in self._loaded_plugins.items():
            status[plugin_name] = {
                'loaded': True,
                'enabled': self.registry.is_enabled(plugin_name),
                'info': plugin_info,
                'content_handlers': len([h for h in self.registry.content_handlers.values() 
                                       if h['plugin_name'] == plugin_name]),
                'filters': len([f for f in self.registry.filters.values() 
                              if f['plugin_name'] == plugin_name]),
                'exporters': len([e for e in self.registry.exporters.values() 
                                if e['plugin_name'] == plugin_name]),
                'scrapers': len([s for s in self.registry.scrapers.values() 
                               if s['plugin_name'] == plugin_name])
            }
        
        return status
    
    def detect_conflicts(self) -> List[Dict[str, Any]]:
        """Detect conflicts between loaded plugins."""
        conflicts = []
        
        # Check for content type conflicts
        content_type_handlers = defaultdict(list)
        for handler_id, handler_info in self.registry.content_handlers.items():
            if self.registry.is_enabled(handler_info['plugin_name']):
                for content_type in handler_info['content_types']:
                    content_type_handlers[content_type].append(handler_id)
        
        for content_type, handlers in content_type_handlers.items():
            if len(handlers) > 1:
                conflicts.append({
                    'type': 'content_handler_conflict',
                    'content_type': content_type,
                    'conflicting_handlers': handlers
                })
        
        # Check for export format conflicts
        export_formats = defaultdict(list)
        for exporter_id, exporter_info in self.registry.exporters.items():
            if self.registry.is_enabled(exporter_info['plugin_name']):
                format_name = exporter_info['format_info'].get('name', 'unknown')
                export_formats[format_name].append(exporter_id)
        
        for format_name, exporters in export_formats.items():
            if len(exporters) > 1:
                conflicts.append({
                    'type': 'exporter_conflict',
                    'format': format_name,
                    'conflicting_exporters': exporters
                })
        
        return conflicts
    
    def cleanup(self) -> None:
        """Clean up all plugins and resources."""
        self.logger.info("Cleaning up plugin manager")
        
        # Unload all plugins in reverse order
        for plugin_name in reversed(self._initialization_order):
            self.unload_plugin(plugin_name)
        
        # Clear all registries
        self.registry = PluginRegistry()
        self._loaded_plugins.clear()
        self._plugin_modules.clear()
        self._initialization_order.clear()
        
        self.logger.info("Plugin manager cleanup complete")