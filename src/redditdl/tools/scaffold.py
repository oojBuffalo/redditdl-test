"""
Plugin Scaffolding Generator

Generates plugin boilerplate code from templates to accelerate plugin development.

Usage:
    python -m tools.scaffold create <plugin_name> --type <plugin_type>
    python -m tools.scaffold init-directory <directory>
    python -m tools.scaffold list-templates
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import string


class PluginScaffold:
    """
    Plugin scaffolding generator for RedditDL plugins.
    
    Creates plugin boilerplate from templates with customization options.
    """
    
    PLUGIN_TYPES = {
        'content_handler': {
            'description': 'Process and download specific content types',
            'template': 'content_handler_template.py',
            'base_class': 'BaseContentHandler',
            'required_methods': ['can_handle', 'process', 'get_supported_types']
        },
        'filter': {
            'description': 'Filter posts based on custom criteria',
            'template': 'filter_template.py',
            'base_class': 'BaseFilter',
            'required_methods': ['apply', 'get_config_schema']
        },
        'exporter': {
            'description': 'Export data to custom formats',
            'template': 'exporter_template.py',
            'base_class': 'BaseExporter',
            'required_methods': ['export', 'get_format_info']
        },
        'scraper': {
            'description': 'Scrape content from custom sources',
            'template': 'scraper_template.py',
            'base_class': 'BaseScraper',
            'required_methods': ['can_scrape', 'scrape', 'get_supported_sources']
        },
        'multi': {
            'description': 'Multi-type plugin with multiple handlers',
            'template': 'multi_plugin_template.py',
            'base_class': 'Multiple',
            'required_methods': []
        }
    }
    
    def __init__(self, templates_dir: Optional[Path] = None):
        """
        Initialize scaffolding generator.
        
        Args:
            templates_dir: Directory containing plugin templates
        """
        if templates_dir is None:
            # Default to plugins/templates directory
            self.templates_dir = Path(__file__).parent.parent / 'plugins' / 'templates'
        else:
            self.templates_dir = templates_dir
        
        if not self.templates_dir.exists():
            raise FileNotFoundError(f"Templates directory not found: {self.templates_dir}")
    
    def create_plugin(self, plugin_name: str, plugin_type: str, output_dir: Path,
                     author: str = "Your Name", email: str = "your.email@example.com",
                     description: str = "", advanced: bool = False) -> bool:
        """
        Create a new plugin from template.
        
        Args:
            plugin_name: Name of the plugin
            plugin_type: Type of plugin to create
            output_dir: Directory to create plugin in
            author: Plugin author name
            email: Author email address
            description: Plugin description
            advanced: Include advanced features and examples
            
        Returns:
            True if plugin created successfully
        """
        if plugin_type not in self.PLUGIN_TYPES:
            print(f"❌ Unknown plugin type: {plugin_type}")
            print(f"Available types: {', '.join(self.PLUGIN_TYPES.keys())}")
            return False
        
        # Create plugin directory
        plugin_dir = output_dir / plugin_name
        if plugin_dir.exists():
            print(f"❌ Plugin directory already exists: {plugin_dir}")
            return False
        
        try:
            plugin_dir.mkdir(parents=True, exist_ok=False)
            print(f"📁 Created plugin directory: {plugin_dir}")
        except Exception as e:
            print(f"❌ Failed to create plugin directory: {e}")
            return False
        
        # Generate plugin files
        try:
            self._create_plugin_manifest(plugin_dir, plugin_name, plugin_type, author, email, description)
            self._create_plugin_implementation(plugin_dir, plugin_name, plugin_type, advanced)
            self._create_plugin_readme(plugin_dir, plugin_name, plugin_type, description)
            self._create_plugin_tests(plugin_dir, plugin_name, plugin_type)
            self._create_requirements_file(plugin_dir, plugin_type, advanced)
            
            if advanced:
                self._create_advanced_files(plugin_dir, plugin_name, plugin_type)
            
            print(f"✅ Plugin '{plugin_name}' created successfully!")
            print(f"📂 Location: {plugin_dir}")
            self._print_next_steps(plugin_name, plugin_type)
            return True
            
        except Exception as e:
            print(f"❌ Failed to create plugin: {e}")
            # Clean up on failure
            import shutil
            if plugin_dir.exists():
                shutil.rmtree(plugin_dir)
            return False
    
    def _create_plugin_manifest(self, plugin_dir: Path, name: str, plugin_type: str,
                               author: str, email: str, description: str):
        """Create plugin.json manifest file."""
        if not description:
            description = f"A {plugin_type} plugin for RedditDL"
        
        type_info = self.PLUGIN_TYPES[plugin_type]
        
        manifest = {
            "name": name,
            "version": "1.0.0",
            "description": description,
            "author": author,
            "email": email,
            "license": "MIT",
            "homepage": f"https://github.com/{author.lower().replace(' ', '')}/{name}",
            "redditdl_version": ">=0.2.0",
            "python_version": ">=3.8",
            "plugin_type": plugin_type,
            "categories": [plugin_type] if plugin_type != 'multi' else ["content_handler", "filter", "exporter"],
            "dependencies": [],
            "optional_dependencies": {},
            "entry_points": self._generate_entry_points(name, plugin_type),
            "configuration": {
                "default_config": {
                    "enabled": True,
                    "log_level": "INFO"
                }
            },
            "resources": {
                "documentation": "README.md",
                "tests": "tests/",
                "examples": "examples/"
            },
            "permissions": {
                "network_access": plugin_type in ['content_handler', 'scraper'],
                "file_system_access": True,
                "subprocess_access": False
            },
            "metadata": {
                "tags": [plugin_type, "redditdl", "plugin"],
                "keywords": ["redditdl", "plugin", plugin_type],
                "created_date": datetime.now().isoformat()
            }
        }
        
        manifest_path = plugin_dir / 'plugin.json'
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"📄 Created manifest: {manifest_path}")
    
    def _generate_entry_points(self, name: str, plugin_type: str) -> Dict[str, List]:
        """Generate entry points based on plugin type."""
        class_name = self._to_class_name(name, plugin_type)
        
        if plugin_type == 'content_handler':
            return {
                "content_handlers": [{
                    "class": class_name,
                    "content_types": ["custom_type"],
                    "priority": 100
                }]
            }
        elif plugin_type == 'filter':
            return {
                "filters": [{
                    "class": class_name,
                    "filter_name": name.lower(),
                    "priority": 100
                }]
            }
        elif plugin_type == 'exporter':
            return {
                "exporters": [{
                    "class": class_name,
                    "format_name": name.lower(),
                    "file_extension": f".{name.lower()}"
                }]
            }
        elif plugin_type == 'scraper':
            return {
                "scrapers": [{
                    "class": class_name,
                    "source_type": name.lower(),
                    "priority": 100
                }]
            }
        elif plugin_type == 'multi':
            return {
                "content_handlers": [f"{class_name}ContentHandler"],
                "filters": [f"{class_name}Filter"],
                "exporters": [f"{class_name}Exporter"]
            }
        else:
            return {}
    
    def _create_plugin_implementation(self, plugin_dir: Path, name: str, plugin_type: str, advanced: bool):
        """Create main plugin implementation file."""
        template_file = self.PLUGIN_TYPES[plugin_type]['template']
        template_path = self.templates_dir / template_file
        
        if not template_path.exists():
            # Create a basic implementation if template doesn't exist
            self._create_basic_implementation(plugin_dir, name, plugin_type)
            return
        
        # Read template and customize it
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        # Replace template variables
        replacements = {
            'ExampleContentHandler': self._to_class_name(name, plugin_type),
            'ExampleFilter': self._to_class_name(name, plugin_type),
            'ExampleExporter': self._to_class_name(name, plugin_type),
            'ExampleScraper': self._to_class_name(name, plugin_type),
            'example_content_handler': name.lower(),
            'example_filter': name.lower(),
            'example_exporter': name.lower(),
            'example_scraper': name.lower(),
            'Example content handler plugin': f"{name} {plugin_type} plugin",
            'Your Name': "Your Name",  # Will be replaced by user input
            'example_type': f"{name.lower()}_type",
            'another_type': f"{name.lower()}_alt_type"
        }
        
        customized_content = template_content
        for old, new in replacements.items():
            customized_content = customized_content.replace(old, new)
        
        # Write to __init__.py
        init_path = plugin_dir / '__init__.py'
        with open(init_path, 'w', encoding='utf-8') as f:
            f.write(customized_content)
        
        print(f"🐍 Created implementation: {init_path}")
    
    def _create_basic_implementation(self, plugin_dir: Path, name: str, plugin_type: str):
        """Create basic implementation when template is not available."""
        class_name = self._to_class_name(name, plugin_type)
        type_info = self.PLUGIN_TYPES[plugin_type]
        
        content = f'''"""
{name} - A {plugin_type} plugin for RedditDL

This plugin was generated using the RedditDL plugin scaffolding tool.
"""

from typing import Any, Dict, List
from core.plugins.hooks import {type_info['base_class']}

__plugin_info__ = {{
    'name': '{name.lower()}',
    'version': '1.0.0',
    'description': 'A {plugin_type} plugin for RedditDL',
    'author': 'Your Name'
}}


class {class_name}({type_info['base_class']}):
    """
    {name} {plugin_type} implementation.
    
    TODO: Implement the required methods and customize this plugin.
    """
    
    def __init__(self):
        """Initialize the {plugin_type}."""
        pass
    
    # TODO: Implement required methods:
    # {chr(10).join(f"    # - {method}" for method in type_info['required_methods'])}


def initialize_plugin():
    """Initialize the plugin when it's loaded."""
    print(f"Loading {{__plugin_info__['name']}} v{{__plugin_info__['version']}}")


def cleanup_plugin():
    """Clean up plugin resources when it's unloaded."""
    print(f"Unloading {{__plugin_info__['name']}}")
'''
        
        init_path = plugin_dir / '__init__.py'
        with open(init_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"🐍 Created basic implementation: {init_path}")
    
    def _create_plugin_readme(self, plugin_dir: Path, name: str, plugin_type: str, description: str):
        """Create README.md file."""
        type_info = self.PLUGIN_TYPES[plugin_type]
        
        if not description:
            description = f"A {plugin_type} plugin for RedditDL"
        
        readme_content = f"""# {name}

{description}

## Overview

This is a {plugin_type} plugin for RedditDL that {type_info['description'].lower()}.

## Installation

1. Copy this plugin directory to your RedditDL plugins folder
2. Install any required dependencies: `pip install -r requirements.txt`
3. Enable the plugin in your RedditDL configuration

## Configuration

```yaml
plugins:
  {name.lower()}:
    enabled: true
    # Add configuration options here
```

## Usage

<!-- Add usage examples here -->

## Development

### Testing

```bash
# Run plugin tests
python -m pytest tests/

# Validate plugin
python -m tools.validator validate .
```

### Building

```bash
# Install development dependencies
pip install -e .[dev]

# Run linting
python -m tools.validator lint .
```

## API

### {self._to_class_name(name, plugin_type)}

{type_info['description']}.

**Required Methods:**
{chr(10).join(f"- `{method}()`: TODO - Add description" for method in type_info['required_methods'])}

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

- Report issues: [GitHub Issues](https://github.com/yourusername/{name}/issues)
- Documentation: [Plugin Documentation](https://redditdl.readthedocs.io/plugins/)
"""
        
        readme_path = plugin_dir / 'README.md'
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        
        print(f"📖 Created README: {readme_path}")
    
    def _create_plugin_tests(self, plugin_dir: Path, name: str, plugin_type: str):
        """Create basic test structure."""
        tests_dir = plugin_dir / 'tests'
        tests_dir.mkdir(exist_ok=True)
        
        # Create __init__.py
        (tests_dir / '__init__.py').write_text('')
        
        # Create test file
        class_name = self._to_class_name(name, plugin_type)
        test_content = f'''"""
Tests for {name} plugin.
"""

import pytest
from unittest.mock import Mock, patch
from {class_name.lower()} import {class_name}


class Test{class_name}:
    """Test suite for {class_name}."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.{plugin_type} = {class_name}()
    
    def test_initialization(self):
        """Test {plugin_type} initialization."""
        assert self.{plugin_type} is not None
    
    # TODO: Add specific tests for your plugin methods
    
    @pytest.mark.asyncio
    async def test_basic_functionality(self):
        """Test basic plugin functionality."""
        # TODO: Implement actual tests
        pass


def test_plugin_info():
    """Test plugin metadata."""
    from {class_name.lower()} import __plugin_info__
    
    assert __plugin_info__['name'] == '{name.lower()}'
    assert __plugin_info__['version']
    assert __plugin_info__['description']
    assert __plugin_info__['author']


# TODO: Add integration tests, performance tests, etc.
'''
        
        test_file = tests_dir / f'test_{name.lower()}.py'
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(test_content)
        
        print(f"🧪 Created tests: {tests_dir}")
    
    def _create_requirements_file(self, plugin_dir: Path, plugin_type: str, advanced: bool):
        """Create requirements.txt file."""
        base_requirements = [
            "# Base requirements for RedditDL plugins",
            "pydantic>=1.8.0",
            "typing-extensions>=3.7.0",
        ]
        
        type_requirements = {
            'content_handler': [
                "requests>=2.25.0",
                "aiohttp>=3.7.0",
            ],
            'filter': [
                "python-dateutil>=2.8.0",
            ],
            'exporter': [
                "jinja2>=3.0.0",
            ],
            'scraper': [
                "requests>=2.25.0",
                "beautifulsoup4>=4.9.0",
                "aiohttp>=3.7.0",
            ]
        }
        
        dev_requirements = [
            "",
            "# Development dependencies",
            "pytest>=6.0.0",
            "pytest-asyncio>=0.15.0",
            "pytest-cov>=2.10.0",
            "black>=21.0.0",
            "isort>=5.0.0",
            "mypy>=0.910",
        ]
        
        if advanced:
            advanced_requirements = [
                "",
                "# Advanced/optional dependencies",
                "pillow>=8.0.0  # For image processing",
                "opencv-python>=4.5.0  # For advanced image/video processing",
                "numpy>=1.20.0  # For numerical operations",
            ]
        else:
            advanced_requirements = []
        
        requirements = (
            base_requirements + 
            type_requirements.get(plugin_type, []) + 
            dev_requirements + 
            advanced_requirements
        )
        
        requirements_path = plugin_dir / 'requirements.txt'
        with open(requirements_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(requirements))
        
        print(f"📦 Created requirements: {requirements_path}")
    
    def _create_advanced_files(self, plugin_dir: Path, name: str, plugin_type: str):
        """Create advanced plugin files."""
        # Create examples directory
        examples_dir = plugin_dir / 'examples'
        examples_dir.mkdir(exist_ok=True)
        
        example_content = f'''"""
Example usage of the {name} plugin.
"""

from {self._to_class_name(name, plugin_type).lower()} import {self._to_class_name(name, plugin_type)}


def main():
    """Example usage of the plugin."""
    # TODO: Add example usage
    {plugin_type} = {self._to_class_name(name, plugin_type)}()
    print(f"Created {{type({plugin_type}).__name__}} instance")


if __name__ == '__main__':
    main()
'''
        
        (examples_dir / f'{name.lower()}_example.py').write_text(example_content)
        
        # Create configuration schema
        schema_content = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": f"{name} Plugin Configuration",
            "type": "object",
            "properties": {
                "enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Enable/disable the plugin"
                },
                "log_level": {
                    "type": "string",
                    "enum": ["DEBUG", "INFO", "WARNING", "ERROR"],
                    "default": "INFO",
                    "description": "Plugin logging level"
                }
            },
            "required": ["enabled"]
        }
        
        schema_path = plugin_dir / 'config_schema.json'
        with open(schema_path, 'w', encoding='utf-8') as f:
            json.dump(schema_content, f, indent=2)
        
        # Create changelog
        changelog_content = f"""# Changelog

All notable changes to the {name} plugin will be documented in this file.

## [1.0.0] - {datetime.now().strftime('%Y-%m-%d')}

### Added
- Initial plugin implementation
- Basic {plugin_type} functionality
- Configuration schema
- Test suite
- Documentation

### TODO
- Implement core functionality
- Add comprehensive tests
- Optimize performance
- Add advanced features
"""
        
        (plugin_dir / 'CHANGELOG.md').write_text(changelog_content)
        
        print(f"📚 Created advanced files: examples/, config_schema.json, CHANGELOG.md")
    
    def _to_class_name(self, name: str, plugin_type: str) -> str:
        """Convert plugin name to class name."""
        # Convert to PascalCase
        clean_name = ''.join(word.capitalize() for word in name.replace('-', '_').split('_'))
        
        # Add type suffix if not already present
        type_suffixes = {
            'content_handler': 'ContentHandler',
            'filter': 'Filter',
            'exporter': 'Exporter',
            'scraper': 'Scraper'
        }
        
        suffix = type_suffixes.get(plugin_type, '')
        if suffix and not clean_name.endswith(suffix):
            clean_name += suffix
        
        return clean_name
    
    def _print_next_steps(self, name: str, plugin_type: str):
        """Print next steps for the user."""
        print(f"""
🚀 Next Steps:

1. 📝 Edit the plugin implementation in __init__.py
2. ⚙️  Configure the plugin in plugin.json
3. 🧪 Write tests in tests/test_{name.lower()}.py
4. 📖 Update README.md with usage examples
5. 🔍 Validate your plugin: python -m tools.validator validate {name}/
6. 🏃 Test your plugin: python -m pytest {name}/tests/

📚 Resources:
- Plugin development guide: plugins/README.md
- API documentation: core/plugins/hooks.py
- Example plugins: plugins/examples/

Happy coding! 🎉
""")
    
    def list_templates(self) -> List[str]:
        """List available plugin templates."""
        templates = []
        for plugin_type, info in self.PLUGIN_TYPES.items():
            templates.append(f"{plugin_type}: {info['description']}")
        return templates
    
    def init_development_directory(self, directory: Path) -> bool:
        """Initialize a directory for plugin development."""
        if not directory.exists():
            directory.mkdir(parents=True)
        
        # Create basic development structure
        (directory / 'plugins').mkdir(exist_ok=True)
        (directory / 'tests').mkdir(exist_ok=True)
        
        # Create development configuration
        dev_config = {
            "development": True,
            "plugin_directories": ["./plugins"],
            "logging": {
                "level": "DEBUG",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        }
        
        config_path = directory / 'dev_config.json'
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(dev_config, f, indent=2)
        
        # Create development README
        readme_content = """# RedditDL Plugin Development Environment

This directory is set up for RedditDL plugin development.

## Structure

- `plugins/` - Your plugin development directory
- `tests/` - Integration tests for your plugins
- `dev_config.json` - Development configuration

## Getting Started

1. Create a new plugin:
   ```bash
   python -m tools.scaffold create my_plugin --type content_handler
   ```

2. Validate your plugin:
   ```bash
   python -m tools.validator validate plugins/my_plugin/
   ```

3. Test your plugin:
   ```bash
   python -m pytest plugins/my_plugin/tests/
   ```

## Resources

- [Plugin Development Guide](../plugins/README.md)
- [Plugin Templates](../plugins/templates/)
- [Example Plugins](../plugins/examples/)
"""
        
        (directory / 'README.md').write_text(readme_content)
        
        print(f"✅ Initialized development directory: {directory}")
        return True


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="RedditDL Plugin Scaffolding Generator")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create command
    create_parser = subparsers.add_parser('create', help='Create a new plugin')
    create_parser.add_argument('name', help='Plugin name')
    create_parser.add_argument('--type', choices=list(PluginScaffold.PLUGIN_TYPES.keys()),
                              default='content_handler', help='Plugin type')
    create_parser.add_argument('--output', type=Path, default=Path.cwd(),
                              help='Output directory')
    create_parser.add_argument('--author', default='Your Name', help='Plugin author')
    create_parser.add_argument('--email', default='your.email@example.com', help='Author email')
    create_parser.add_argument('--description', default='', help='Plugin description')
    create_parser.add_argument('--advanced', action='store_true',
                              help='Include advanced features and examples')
    
    # List templates command
    list_parser = subparsers.add_parser('list-templates', help='List available plugin templates')
    
    # Init directory command
    init_parser = subparsers.add_parser('init-directory', help='Initialize plugin development directory')
    init_parser.add_argument('directory', type=Path, help='Directory to initialize')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    scaffold = PluginScaffold()
    
    if args.command == 'create':
        success = scaffold.create_plugin(
            args.name, args.type, args.output,
            args.author, args.email, args.description, args.advanced
        )
        sys.exit(0 if success else 1)
    
    elif args.command == 'list-templates':
        print("📋 Available Plugin Templates:")
        for template in scaffold.list_templates():
            print(f"  • {template}")
    
    elif args.command == 'init-directory':
        success = scaffold.init_development_directory(args.directory)
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()