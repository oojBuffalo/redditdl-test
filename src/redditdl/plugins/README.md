# RedditDL Plugin Development Guide

This directory contains the plugin system for RedditDL, allowing you to extend functionality with custom content handlers, filters, exporters, and scrapers.

## Quick Start

1. **Copy a template** from `templates/` directory
2. **Customize** the plugin for your needs
3. **Test** your plugin with the provided examples
4. **Deploy** by placing in a plugin directory

## Plugin Types

### Content Handlers
Process specific types of media content (images, videos, audio, etc.)

**Template:** `templates/content_handler_template.py`
**Example:** `examples/simple_text_handler.py`

```python
class MyContentHandler(BaseContentHandler):
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        return content_type == "my_type"
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        # Your processing logic here
        return {"success": True, "processed": True}
    
    def get_supported_types(self) -> List[str]:
        return ["my_type"]
```

### Filters
Filter posts based on criteria like score, content type, keywords, etc.

**Template:** `templates/filter_template.py`
**Example:** `examples/score_filter.py`

```python
class MyFilter(BaseFilter):
    def apply(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Your filtering logic here
        return filtered_posts
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {"param": {"type": "string", "default": "value"}}
```

### Exporters
Export collected data to different formats (CSV, JSON, XML, databases, etc.)

**Template:** `templates/exporter_template.py`
**Example:** `examples/csv_exporter.py`

```python
class MyExporter(BaseExporter):
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> bool:
        # Your export logic here
        return True
    
    def get_format_info(self) -> Dict[str, str]:
        return {"name": "myformat", "extension": ".myext"}
```

### Scrapers
Scrape content from custom sources beyond Reddit

**Template:** `templates/scraper_template.py`

```python
class MyScraper(BaseScraper):
    def can_scrape(self, source_type: str, source_config: Dict[str, Any]) -> bool:
        return source_type == "my_source"
    
    async def scrape(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Your scraping logic here
        return posts
    
    def get_supported_sources(self) -> List[str]:
        return ["my_source"]
```

## Plugin Structure

### Directory-based Plugins
Create a directory with `plugin.json` manifest:

```
my_plugin/
├── plugin.json          # Plugin metadata
├── __init__.py          # Plugin implementation
├── requirements.txt     # Dependencies (optional)
└── README.md           # Documentation (optional)
```

**plugin.json example:**
```json
{
    "name": "my_plugin",
    "version": "1.0.0",
    "description": "My awesome plugin",
    "author": "Your Name",
    "redditdl_version": ">=0.2.0",
    "dependencies": [],
    "permissions": ["network"],
    "entry_points": {
        "content_handlers": ["MyContentHandler"],
        "filters": ["MyFilter"]
    }
}
```

### Single-file Plugins
Create a `.py` file with `__plugin_info__` dictionary:

```python
__plugin_info__ = {
    'name': 'my_plugin',
    'version': '1.0.0',
    'description': 'My awesome plugin',
    'author': 'Your Name'
}

class MyContentHandler(BaseContentHandler):
    # Implementation here
    pass

def initialize_plugin():
    """Called when plugin is loaded."""
    pass

def cleanup_plugin():
    """Called when plugin is unloaded."""
    pass
```

## Configuration Schema

Define configuration parameters for your plugins:

```python
def get_config_schema(self) -> Dict[str, Any]:
    return {
        'param_name': {
            'type': 'string',           # string, integer, number, boolean, array, object
            'default': 'default_value',
            'description': 'Parameter description',
            'choices': ['option1', 'option2'],  # For enum values
            'minimum': 0,               # For numbers
            'maximum': 100,
            'sensitive': True           # Mark as sensitive (password, API key)
        }
    }
```

## Plugin Lifecycle

1. **Discovery** - RedditDL finds plugins in configured directories
2. **Validation** - Plugin structure and dependencies are checked
3. **Loading** - Plugin module is imported and instantiated
4. **Registration** - Plugin components are registered with the system
5. **Execution** - Plugin methods are called during processing
6. **Cleanup** - Plugin resources are cleaned up on shutdown

## Best Practices

### Error Handling
Always handle exceptions gracefully:

```python
def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    try:
        # Your logic here
        return {"success": True}
    except Exception as e:
        self.logger.error(f"Processing failed: {e}")
        return {"success": False, "error": str(e)}
```

### Logging
Use the provided logger:

```python
import logging

class MyPlugin:
    def __init__(self):
        self.logger = logging.getLogger(f"plugins.{self.__class__.__name__}")
    
    def process(self, data):
        self.logger.info("Processing started")
        self.logger.debug(f"Processing data: {data}")
```

### Configuration Validation
Validate configuration before use:

```python
def validate_config(self, config: Dict[str, Any]) -> List[str]:
    errors = []
    
    required_param = config.get('required_param')
    if not required_param:
        errors.append("required_param is missing")
    
    return errors
```

### Resource Management
Clean up resources properly:

```python
class MyPlugin:
    def __init__(self):
        self.session = None
    
    async def process(self, data):
        if not self.session:
            self.session = aiohttp.ClientSession()
        # Use session...
    
    def cleanup(self):
        if self.session:
            asyncio.create_task(self.session.close())
```

## Testing Your Plugin

### Unit Tests
Create tests for your plugin components:

```python
import pytest
from my_plugin import MyContentHandler

class TestMyContentHandler:
    def test_can_handle(self):
        handler = MyContentHandler()
        assert handler.can_handle("my_type", {})
        assert not handler.can_handle("other_type", {})
    
    @pytest.mark.asyncio
    async def test_process(self):
        handler = MyContentHandler()
        result = await handler.process({"test": "data"}, {})
        assert result["success"] is True
```

### Integration Tests
Test with the plugin system:

```python
from core.plugins.manager import PluginManager

def test_plugin_loading():
    manager = PluginManager(plugin_dirs=["path/to/my/plugin"])
    discovered = manager.discover_plugins()
    assert len(discovered) == 1
    
    success = manager.load_plugin(discovered[0])
    assert success
    
    handlers = manager.get_content_handlers()
    assert len(handlers) == 1
```

## Deployment

### Development
Place plugins in the `plugins/` directory:
```
plugins/
├── my_plugin/
│   ├── plugin.json
│   └── __init__.py
└── my_other_plugin.py
```

### Production
1. Package your plugin as a Python package
2. Install via pip: `pip install my-redditdl-plugin`
3. Configure plugin directories in RedditDL settings

## Security Considerations

### Sandboxing
Plugins run in a sandboxed environment with restricted imports:

- File system access is limited
- Network access requires permission
- Dangerous modules are blocked

### Permissions
Request permissions in your plugin manifest:

```json
{
    "permissions": [
        "network",      # HTTP/HTTPS requests
        "filesystem",   # File operations
        "subprocess"    # Running external commands
    ]
}
```

### Input Validation
Always validate external input:

```python
def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    # Validate post_data structure
    if not isinstance(post_data.get('url'), str):
        raise ValueError("Invalid URL in post data")
    
    # Sanitize config values
    max_size = min(config.get('max_size', 1000), 10000)  # Cap at 10K
```

## Advanced Features

### Async Support
Use async/await for I/O operations:

```python
async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    async with aiohttp.ClientSession() as session:
        async with session.get(post_data['url']) as response:
            content = await response.read()
            return {"content": content, "success": True}
```

### Progress Reporting
Report progress for long-running operations:

```python
def process_batch(self, posts: List[Dict], config: Dict, progress_callback=None):
    for i, post in enumerate(posts):
        # Process post...
        
        if progress_callback:
            progress_callback(i + 1, len(posts), f"Processed {post['title']}")
```

### Caching
Implement caching for expensive operations:

```python
from functools import lru_cache

class MyPlugin:
    @lru_cache(maxsize=128)
    def expensive_operation(self, input_data: str) -> str:
        # Expensive computation here
        return result
```

## Examples and Templates

- **Content Handler Template:** `templates/content_handler_template.py`
- **Filter Template:** `templates/filter_template.py`
- **Exporter Template:** `templates/exporter_template.py`
- **Scraper Template:** `templates/scraper_template.py`
- **Plugin Manifest:** `templates/plugin_manifest.json`

- **Simple Text Handler:** `examples/simple_text_handler.py`
- **Score Filter:** `examples/score_filter.py`
- **CSV Exporter:** `examples/csv_exporter.py`

## Troubleshooting

### Common Issues

**Plugin not discovered:**
- Check plugin directory is configured
- Verify `plugin.json` or `__plugin_info__` is present
- Ensure file permissions are correct

**Import errors:**
- Check all dependencies are installed
- Verify import paths are correct
- Check for circular imports

**Plugin not loading:**
- Check plugin validation errors in logs
- Verify plugin implements required interfaces
- Check for missing dependencies

**Runtime errors:**
- Enable debug logging: `logging.getLogger("plugins").setLevel(logging.DEBUG)`
- Check for configuration errors
- Verify input data format

### Debug Mode
Enable detailed plugin logging:

```python
import logging
logging.getLogger("plugins").setLevel(logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add your plugin in `plugins/contrib/`
4. Include tests and documentation
5. Submit a pull request

## Community Plugins

Share your plugins with the community:

- Submit to the official plugin registry
- Share on GitHub with `redditdl-plugin` topic
- Discuss on the community forum

## API Reference

### Base Classes
- `BaseContentHandler` - For content processing plugins
- `BaseFilter` - For post filtering plugins  
- `BaseExporter` - For data export plugins
- `BaseScraper` - For content scraping plugins

### Plugin Manager
- `PluginManager` - Main plugin management interface
- `PluginRegistry` - Plugin registration and tracking

### Hook Specifications
- `ContentHandlerHooks` - Content handler plugin hooks
- `FilterHooks` - Filter plugin hooks
- `ExporterHooks` - Exporter plugin hooks
- `ScraperHooks` - Scraper plugin hooks

For detailed API documentation, see the source code and docstrings in `core/plugins/`.