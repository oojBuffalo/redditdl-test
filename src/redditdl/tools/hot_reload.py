#!/usr/bin/env python3
"""
RedditDL Plugin Hot Reloader

Provides hot reloading capabilities for plugin development.
Monitors plugin files for changes and automatically reloads them
without restarting the entire application.

Features:
- File system watching for plugin changes
- Automatic plugin reregistration
- Development server mode with live reload
- Plugin dependency tracking
- Safe reload with error handling
- Plugin state preservation during reload

Author: RedditDL Plugin Development Kit
License: MIT
Version: 1.0.0
"""

import os
import sys
import time
import logging
import asyncio
import threading
import importlib
import importlib.util
from typing import Dict, List, Set, Optional, Callable, Any
from dataclasses import dataclass, field
from pathlib import Path
import json
import hashlib
from functools import wraps

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent, FileDeletedEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

logger = logging.getLogger(__name__)

@dataclass
class PluginMetadata:
    """Metadata for a loaded plugin."""
    name: str
    file_path: Path
    module_name: str
    last_modified: float
    file_hash: str
    dependencies: Set[str] = field(default_factory=set)
    dependents: Set[str] = field(default_factory=set)
    instances: List[Any] = field(default_factory=list)
    last_reload: Optional[float] = None
    reload_count: int = 0

@dataclass
class ReloadEvent:
    """Represents a plugin reload event."""
    plugin_name: str
    event_type: str  # 'reload', 'unload', 'load'
    timestamp: float
    success: bool
    error: Optional[str] = None
    file_path: Optional[Path] = None

class PluginFileWatcher(FileSystemEventHandler):
    """File system event handler for plugin changes."""
    
    def __init__(self, hot_reloader: 'PluginHotReloader'):
        """Initialize the file watcher."""
        super().__init__()
        self.hot_reloader = hot_reloader
        self.logger = logging.getLogger(f"{__name__}.FileWatcher")
    
    def on_modified(self, event):
        """Handle file modification events."""
        if isinstance(event, FileModifiedEvent) and not event.is_directory:
            file_path = Path(event.src_path)
            if self._is_plugin_file(file_path):
                self.logger.info(f"Plugin file modified: {file_path}")
                asyncio.run_coroutine_threadsafe(
                    self.hot_reloader.reload_plugin_from_file(file_path),
                    self.hot_reloader.event_loop
                )
    
    def on_created(self, event):
        """Handle file creation events."""
        if isinstance(event, FileCreatedEvent) and not event.is_directory:
            file_path = Path(event.src_path)
            if self._is_plugin_file(file_path):
                self.logger.info(f"New plugin file created: {file_path}")
                asyncio.run_coroutine_threadsafe(
                    self.hot_reloader.load_plugin_from_file(file_path),
                    self.hot_reloader.event_loop
                )
    
    def on_deleted(self, event):
        """Handle file deletion events."""
        if isinstance(event, FileDeletedEvent) and not event.is_directory:
            file_path = Path(event.src_path)
            if self._is_plugin_file(file_path):
                self.logger.info(f"Plugin file deleted: {file_path}")
                asyncio.run_coroutine_threadsafe(
                    self.hot_reloader.unload_plugin_from_file(file_path),
                    self.hot_reloader.event_loop
                )
    
    def _is_plugin_file(self, file_path: Path) -> bool:
        """Check if file is a plugin file that should trigger reload."""
        # Check file extension
        if file_path.suffix != '.py':
            return False
        
        # Skip __pycache__ and other system files
        if '__pycache__' in file_path.parts or file_path.name.startswith('.'):
            return False
        
        # Check if file is in plugin directories
        for plugin_dir in self.hot_reloader.plugin_directories:
            try:
                file_path.relative_to(plugin_dir)
                return True
            except ValueError:
                continue
        
        return False

class PluginHotReloader:
    """
    Hot reloader for RedditDL plugins.
    
    Monitors plugin files for changes and automatically reloads them
    during development without restarting the application.
    """
    
    def __init__(self, plugin_directories: List[Path], config: Dict[str, Any] = None):
        """
        Initialize the hot reloader.
        
        Args:
            plugin_directories: Directories to monitor for plugin changes
            config: Configuration dictionary
        """
        if not WATCHDOG_AVAILABLE:
            raise ImportError(
                "watchdog is required for hot reloading. "
                "Install with: pip install watchdog"
            )
        
        self.plugin_directories = [Path(d) for d in plugin_directories]
        self.config = config or {}
        
        # Plugin tracking
        self.loaded_plugins: Dict[str, PluginMetadata] = {}
        self.plugin_registry: Dict[str, Any] = {}  # Plugin instances registry
        self.reload_history: List[ReloadEvent] = []
        
        # File watching
        self.observer: Optional[Observer] = None
        self.file_watcher = PluginFileWatcher(self)
        
        # Event loop for async operations
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None
        self.reload_callbacks: List[Callable] = []
        
        # Hot reload settings
        self.enabled = True
        self.reload_delay = 0.5  # Delay to batch file changes
        self.max_reload_attempts = 3
        self.preserve_state = True
        
        # Thread safety
        self._reload_lock = asyncio.Lock()
        
        logger.info(f"PluginHotReloader initialized for directories: {self.plugin_directories}")
    
    def start(self, event_loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Start the hot reloader."""
        if not self.enabled:
            logger.info("Hot reloader is disabled")
            return
        
        self.event_loop = event_loop or asyncio.get_event_loop()
        
        # Start file system observer
        self.observer = Observer()
        
        for plugin_dir in self.plugin_directories:
            if plugin_dir.exists():
                self.observer.schedule(self.file_watcher, str(plugin_dir), recursive=True)
                logger.info(f"Watching plugin directory: {plugin_dir}")
            else:
                logger.warning(f"Plugin directory does not exist: {plugin_dir}")
        
        self.observer.start()
        logger.info("Plugin hot reloader started")
    
    def stop(self) -> None:
        """Stop the hot reloader."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None
        
        logger.info("Plugin hot reloader stopped")
    
    def add_reload_callback(self, callback: Callable[[ReloadEvent], None]) -> None:
        """Add a callback to be called when plugins are reloaded."""
        self.reload_callbacks.append(callback)
    
    def remove_reload_callback(self, callback: Callable) -> None:
        """Remove a reload callback."""
        if callback in self.reload_callbacks:
            self.reload_callbacks.remove(callback)
    
    async def reload_plugin_from_file(self, file_path: Path) -> bool:
        """Reload a plugin from a specific file."""
        if not self.enabled:
            return False
        
        async with self._reload_lock:
            # Add delay to batch rapid file changes
            await asyncio.sleep(self.reload_delay)
            
            plugin_name = self._get_plugin_name_from_file(file_path)
            if not plugin_name:
                logger.warning(f"Could not determine plugin name from file: {file_path}")
                return False
            
            logger.info(f"Reloading plugin: {plugin_name}")
            
            success = False
            error = None
            
            try:
                # Check if file still exists (might have been deleted)
                if not file_path.exists():
                    return await self.unload_plugin(plugin_name)
                
                # Calculate file hash to detect actual changes
                file_hash = self._calculate_file_hash(file_path)
                
                # Check if plugin is already loaded
                if plugin_name in self.loaded_plugins:
                    old_metadata = self.loaded_plugins[plugin_name]
                    
                    # Skip reload if file hasn't actually changed
                    if old_metadata.file_hash == file_hash:
                        logger.debug(f"Skipping reload of {plugin_name} - no changes detected")
                        return True
                    
                    # Preserve plugin state if configured
                    old_state = None
                    if self.preserve_state:
                        old_state = self._extract_plugin_state(old_metadata)
                    
                    # Unload old plugin
                    await self._unload_plugin_internal(plugin_name)
                    
                    # Load new plugin
                    success = await self._load_plugin_internal(file_path, file_hash)
                    
                    # Restore state if successful
                    if success and old_state and self.preserve_state:
                        await self._restore_plugin_state(plugin_name, old_state)
                else:
                    # Load new plugin
                    success = await self._load_plugin_internal(file_path, file_hash)
                
                if success:
                    # Update metadata
                    if plugin_name in self.loaded_plugins:
                        metadata = self.loaded_plugins[plugin_name]
                        metadata.last_reload = time.time()
                        metadata.reload_count += 1
                
            except Exception as e:
                error = str(e)
                logger.error(f"Failed to reload plugin {plugin_name}: {e}")
                logger.exception(e)
            
            # Create reload event
            event = ReloadEvent(
                plugin_name=plugin_name,
                event_type='reload',
                timestamp=time.time(),
                success=success,
                error=error,
                file_path=file_path
            )
            
            self.reload_history.append(event)
            
            # Notify callbacks
            for callback in self.reload_callbacks:
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Error in reload callback: {e}")
            
            return success
    
    async def load_plugin_from_file(self, file_path: Path) -> bool:
        """Load a new plugin from file."""
        if not self.enabled:
            return False
        
        plugin_name = self._get_plugin_name_from_file(file_path)
        if not plugin_name:
            return False
        
        file_hash = self._calculate_file_hash(file_path)
        
        try:
            success = await self._load_plugin_internal(file_path, file_hash)
            
            event = ReloadEvent(
                plugin_name=plugin_name,
                event_type='load',
                timestamp=time.time(),
                success=success,
                file_path=file_path
            )
            
            self.reload_history.append(event)
            
            for callback in self.reload_callbacks:
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Error in reload callback: {e}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to load plugin from {file_path}: {e}")
            return False
    
    async def unload_plugin_from_file(self, file_path: Path) -> bool:
        """Unload a plugin based on file path."""
        plugin_name = self._get_plugin_name_from_file(file_path)
        if not plugin_name:
            return False
        
        return await self.unload_plugin(plugin_name)
    
    async def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a specific plugin."""
        if not self.enabled or plugin_name not in self.loaded_plugins:
            return False
        
        try:
            success = await self._unload_plugin_internal(plugin_name)
            
            event = ReloadEvent(
                plugin_name=plugin_name,
                event_type='unload',
                timestamp=time.time(),
                success=success
            )
            
            self.reload_history.append(event)
            
            for callback in self.reload_callbacks:
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Error in reload callback: {e}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to unload plugin {plugin_name}: {e}")
            return False
    
    async def _load_plugin_internal(self, file_path: Path, file_hash: str) -> bool:
        """Internal method to load a plugin."""
        plugin_name = self._get_plugin_name_from_file(file_path)
        if not plugin_name:
            return False
        
        try:
            # Create module spec
            module_name = f"hot_reload_plugin_{plugin_name}_{int(time.time())}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            
            if not spec or not spec.loader:
                logger.error(f"Could not create module spec for {file_path}")
                return False
            
            # Load module
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            # Create plugin metadata
            metadata = PluginMetadata(
                name=plugin_name,
                file_path=file_path,
                module_name=module_name,
                last_modified=file_path.stat().st_mtime,
                file_hash=file_hash
            )
            
            # Register plugin
            self.loaded_plugins[plugin_name] = metadata
            self.plugin_registry[plugin_name] = module
            
            logger.info(f"Successfully loaded plugin: {plugin_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load plugin from {file_path}: {e}")
            return False
    
    async def _unload_plugin_internal(self, plugin_name: str) -> bool:
        """Internal method to unload a plugin."""
        if plugin_name not in self.loaded_plugins:
            return False
        
        try:
            metadata = self.loaded_plugins[plugin_name]
            
            # Clean up plugin instances
            for instance in metadata.instances:
                if hasattr(instance, 'cleanup'):
                    try:
                        instance.cleanup()
                    except Exception as e:
                        logger.warning(f"Error cleaning up plugin instance: {e}")
            
            # Remove from registry
            if plugin_name in self.plugin_registry:
                del self.plugin_registry[plugin_name]
            
            # Remove module from sys.modules
            if metadata.module_name in sys.modules:
                del sys.modules[metadata.module_name]
            
            # Remove metadata
            del self.loaded_plugins[plugin_name]
            
            logger.info(f"Successfully unloaded plugin: {plugin_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unload plugin {plugin_name}: {e}")
            return False
    
    def _get_plugin_name_from_file(self, file_path: Path) -> Optional[str]:
        """Extract plugin name from file path."""
        # Try to determine plugin name from file structure
        # This is a simple implementation - can be enhanced based on plugin structure
        
        # Check if it's directly in a plugin directory
        for plugin_dir in self.plugin_directories:
            try:
                relative_path = file_path.relative_to(plugin_dir)
                
                # If it's a direct file in plugin directory
                if len(relative_path.parts) == 1:
                    return relative_path.stem
                
                # If it's in a subdirectory, use directory name
                return relative_path.parts[0]
                
            except ValueError:
                continue
        
        # Fallback to filename without extension
        return file_path.stem
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file contents."""
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            return hashlib.md5(content).hexdigest()
        except Exception:
            return ""
    
    def _extract_plugin_state(self, metadata: PluginMetadata) -> Dict[str, Any]:
        """Extract state from plugin instances for preservation."""
        state = {}
        
        for i, instance in enumerate(metadata.instances):
            instance_state = {}
            
            # Extract basic attributes
            for attr_name in dir(instance):
                if not attr_name.startswith('_') and not callable(getattr(instance, attr_name)):
                    try:
                        attr_value = getattr(instance, attr_name)
                        # Only preserve simple types
                        if isinstance(attr_value, (str, int, float, bool, list, dict)):
                            instance_state[attr_name] = attr_value
                    except Exception:
                        pass
            
            if instance_state:
                state[f'instance_{i}'] = instance_state
        
        return state
    
    async def _restore_plugin_state(self, plugin_name: str, state: Dict[str, Any]) -> None:
        """Restore state to plugin instances after reload."""
        if plugin_name not in self.loaded_plugins:
            return
        
        metadata = self.loaded_plugins[plugin_name]
        
        for i, instance in enumerate(metadata.instances):
            instance_state = state.get(f'instance_{i}', {})
            
            for attr_name, attr_value in instance_state.items():
                try:
                    if hasattr(instance, attr_name):
                        setattr(instance, attr_name, attr_value)
                except Exception as e:
                    logger.warning(f"Failed to restore attribute {attr_name}: {e}")
    
    def get_plugin_status(self) -> Dict[str, Any]:
        """Get status information about loaded plugins."""
        return {
            'enabled': self.enabled,
            'loaded_plugins': len(self.loaded_plugins),
            'plugin_directories': [str(d) for d in self.plugin_directories],
            'plugins': {
                name: {
                    'file_path': str(metadata.file_path),
                    'last_modified': metadata.last_modified,
                    'reload_count': metadata.reload_count,
                    'last_reload': metadata.last_reload
                }
                for name, metadata in self.loaded_plugins.items()
            },
            'reload_history': [
                {
                    'plugin_name': event.plugin_name,
                    'event_type': event.event_type,
                    'timestamp': event.timestamp,
                    'success': event.success,
                    'error': event.error
                }
                for event in self.reload_history[-10:]  # Last 10 events
            ]
        }
    
    def get_reload_statistics(self) -> Dict[str, Any]:
        """Get reload statistics."""
        total_reloads = len(self.reload_history)
        successful_reloads = sum(1 for event in self.reload_history if event.success)
        failed_reloads = total_reloads - successful_reloads
        
        # Group by plugin
        plugin_stats = {}
        for plugin_name, metadata in self.loaded_plugins.items():
            plugin_stats[plugin_name] = {
                'reload_count': metadata.reload_count,
                'last_reload': metadata.last_reload
            }
        
        return {
            'total_reloads': total_reloads,
            'successful_reloads': successful_reloads,
            'failed_reloads': failed_reloads,
            'success_rate': (successful_reloads / total_reloads * 100) if total_reloads > 0 else 0,
            'plugin_statistics': plugin_stats
        }

# Utility functions and decorators
def hot_reloadable(reloader: PluginHotReloader):
    """Decorator to make a plugin class hot-reloadable."""
    def decorator(cls):
        @wraps(cls)
        def wrapper(*args, **kwargs):
            instance = cls(*args, **kwargs)
            
            # Register instance with reloader
            plugin_name = getattr(cls, 'name', cls.__name__)
            if plugin_name in reloader.loaded_plugins:
                reloader.loaded_plugins[plugin_name].instances.append(instance)
            
            return instance
        
        return wrapper
    return decorator

def create_development_server(plugin_directories: List[str], config: Dict[str, Any] = None) -> PluginHotReloader:
    """Create a hot reloader for development server mode."""
    reloader = PluginHotReloader(plugin_directories, config)
    
    # Add logging callback
    def log_reload_events(event: ReloadEvent):
        if event.success:
            logger.info(f"🔄 Plugin {event.plugin_name} {event.event_type}ed successfully")
        else:
            logger.error(f"❌ Failed to {event.event_type} plugin {event.plugin_name}: {event.error}")
    
    reloader.add_reload_callback(log_reload_events)
    
    return reloader

# Example usage and testing
if __name__ == "__main__":
    # Example usage of hot reloader
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        """Example main function."""
        # Create reloader
        plugin_dirs = ["./plugins", "./examples"]
        reloader = create_development_server(plugin_dirs)
        
        # Start reloader
        reloader.start()
        
        print("Hot reloader started. Monitoring plugin directories:")
        for dir_path in plugin_dirs:
            print(f"  • {dir_path}")
        
        print("\nMake changes to plugin files to see hot reloading in action.")
        print("Press Ctrl+C to stop.")
        
        try:
            # Keep running
            while True:
                await asyncio.sleep(1)
                
                # Print status periodically
                if int(time.time()) % 30 == 0:  # Every 30 seconds
                    status = reloader.get_plugin_status()
                    print(f"\nStatus: {status['loaded_plugins']} plugins loaded")
        
        except KeyboardInterrupt:
            print("\nStopping hot reloader...")
            reloader.stop()
    
    if WATCHDOG_AVAILABLE:
        asyncio.run(main())
    else:
        print("❌ Watchdog not available. Install with: pip install watchdog")