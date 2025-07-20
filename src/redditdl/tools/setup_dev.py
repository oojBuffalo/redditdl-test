"""
Plugin Development Environment Setup

Automates the setup of a RedditDL plugin development environment.

Usage:
    python -m tools.setup_dev init [directory]
    python -m tools.setup_dev install-deps
    python -m tools.setup_dev check-environment
    python -m tools.setup_dev create-config
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import shutil
import venv


class DevEnvironmentSetup:
    """
    Setup and manage plugin development environments for RedditDL.
    
    Handles virtual environment creation, dependency installation,
    configuration setup, and development tool preparation.
    """
    
    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize development environment setup.
        
        Args:
            base_dir: Base directory for development setup
        """
        self.base_dir = base_dir or Path.cwd()
        self.venv_dir = self.base_dir / '.venv'
        self.config_dir = self.base_dir / '.redditdl-dev'
        
    def init_environment(self, directory: Path, with_examples: bool = True) -> bool:
        """
        Initialize a complete plugin development environment.
        
        Args:
            directory: Directory to initialize
            with_examples: Include example plugins and templates
            
        Returns:
            True if initialization successful
        """
        print(f"🚀 Initializing RedditDL plugin development environment in {directory}")
        
        try:
            # Create directory structure
            self._create_directory_structure(directory)
            
            # Create virtual environment
            if self._create_virtual_environment(directory):
                print("✅ Virtual environment created")
            
            # Install dependencies
            if self._install_development_dependencies(directory):
                print("✅ Development dependencies installed")
            
            # Create development configuration
            self._create_development_config(directory)
            print("✅ Development configuration created")
            
            # Create development scripts
            self._create_development_scripts(directory)
            print("✅ Development scripts created")
            
            # Copy examples and templates if requested
            if with_examples:
                self._setup_examples_and_templates(directory)
                print("✅ Examples and templates set up")
            
            # Create initial documentation
            self._create_development_docs(directory)
            print("✅ Development documentation created")
            
            print(f"\n🎉 Development environment initialized successfully!")
            self._print_getting_started_guide(directory)
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize development environment: {e}")
            return False
    
    def _create_directory_structure(self, directory: Path):
        """Create the basic directory structure for plugin development."""
        directories = [
            'plugins',           # Plugin development directory
            'tests',            # Integration tests
            'examples',         # Example plugins
            'docs',             # Local documentation
            'tools',            # Development tools
            'templates',        # Custom templates
            'dist',             # Built plugins
            '.redditdl-dev'     # Development configuration
        ]
        
        for dir_name in directories:
            (directory / dir_name).mkdir(parents=True, exist_ok=True)
            
        # Create __init__.py files where needed
        for init_dir in ['plugins', 'tests', 'examples']:
            init_file = directory / init_dir / '__init__.py'
            if not init_file.exists():
                init_file.write_text('"""Plugin development directory."""\n')
    
    def _create_virtual_environment(self, directory: Path) -> bool:
        """Create a virtual environment for plugin development."""
        venv_path = directory / '.venv'
        
        if venv_path.exists():
            print(f"Virtual environment already exists at {venv_path}")
            return True
        
        try:
            print(f"Creating virtual environment at {venv_path}")
            venv.create(venv_path, with_pip=True)
            return True
        except Exception as e:
            print(f"Failed to create virtual environment: {e}")
            return False
    
    def _install_development_dependencies(self, directory: Path) -> bool:
        """Install development dependencies in the virtual environment."""
        venv_path = directory / '.venv'
        
        if not venv_path.exists():
            print("Virtual environment not found, skipping dependency installation")
            return False
        
        # Determine pip executable path
        if sys.platform == "win32":
            pip_path = venv_path / 'Scripts' / 'pip.exe'
            python_path = venv_path / 'Scripts' / 'python.exe'
        else:
            pip_path = venv_path / 'bin' / 'pip'
            python_path = venv_path / 'bin' / 'python'
        
        # Base dependencies for plugin development
        dependencies = [
            # Core RedditDL dependencies
            'pydantic>=1.8.0',
            'typing-extensions>=3.7.0',
            'pluggy>=1.0.0',
            
            # Development and testing
            'pytest>=6.0.0',
            'pytest-asyncio>=0.15.0',
            'pytest-cov>=2.10.0',
            'pytest-mock>=3.6.0',
            
            # Code quality
            'black>=21.0.0',
            'isort>=5.0.0',
            'mypy>=0.910',
            'flake8>=4.0.0',
            
            # Documentation
            'sphinx>=4.0.0',
            'sphinx-rtd-theme>=1.0.0',
            
            # Common plugin dependencies
            'requests>=2.25.0',
            'aiohttp>=3.7.0',
            'pillow>=8.0.0',
            'jinja2>=3.0.0',
            'pyyaml>=6.0.0',
        ]
        
        try:
            print("Installing development dependencies...")
            subprocess.run([
                str(pip_path), 'install', '--upgrade', 'pip'
            ], check=True, capture_output=True)
            
            subprocess.run([
                str(pip_path), 'install'
            ] + dependencies, check=True, capture_output=True)
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Failed to install dependencies: {e}")
            return False
    
    def _create_development_config(self, directory: Path):
        """Create development configuration files."""
        config_dir = directory / '.redditdl-dev'
        
        # Main development config
        dev_config = {
            "environment": "development",
            "debug": True,
            "plugin_directories": ["./plugins", "./examples"],
            "auto_reload": True,
            "development_mode": {
                "enabled": True,
                "hot_reload": True,
                "sandbox_isolation": True,
                "debug_profiling": True,
                "verbose_errors": True,
                "plugin_state_tracking": True,
                "performance_monitoring": True,
                "memory_profiling": True
            },
            "validation": {
                "strict": True,
                "security_checks": True,
                "performance_monitoring": True,
                "auto_validate_on_change": True,
                "fail_fast": False
            },
            "logging": {
                "level": "DEBUG",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file": ".redditdl-dev/development.log",
                "plugin_trace_file": ".redditdl-dev/plugin_traces.log",
                "performance_log": ".redditdl-dev/performance.log"
            },
            "testing": {
                "auto_discover": True,
                "coverage_threshold": 80,
                "test_directories": ["./tests", "./plugins/*/tests"],
                "auto_test_on_change": True,
                "parallel_testing": False
            },
            "debugging": {
                "enabled": True,
                "trace_execution": True,
                "profile_performance": True,
                "capture_plugin_state": True,
                "export_debug_data": True,
                "debug_output_dir": ".redditdl-dev/debug"
            }
        }
        
        with open(config_dir / 'config.json', 'w') as f:
            json.dump(dev_config, f, indent=2)
        
        # pytest configuration
        pytest_config = """[tool:pytest]
testpaths = tests plugins
python_files = test_*.py *_test.py
python_classes = Test*
python_functions = test_*
addopts = 
    --verbose
    --tb=short
    --cov=plugins
    --cov-report=html
    --cov-report=term-missing
    --cov-fail-under=80
asyncio_mode = auto

[coverage:run]
source = plugins
omit = 
    */tests/*
    */venv/*
    */.venv/*
    */build/*
    */dist/*

[coverage:report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise AssertionError
    raise NotImplementedError
"""
        
        with open(directory / 'pytest.ini', 'w') as f:
            f.write(pytest_config)
        
        # mypy configuration
        mypy_config = """[mypy]
python_version = 3.8
warn_return_any = True
warn_unused_configs = True
disallow_untyped_defs = True
disallow_incomplete_defs = True
check_untyped_defs = True
disallow_untyped_decorators = True
no_implicit_optional = True
warn_redundant_casts = True
warn_unused_ignores = True
warn_no_return = True
warn_unreachable = True
strict_equality = True

[mypy-tests.*]
disallow_untyped_defs = False
"""
        
        with open(directory / 'mypy.ini', 'w') as f:
            f.write(mypy_config)
        
        # Black configuration
        black_config = {
            "line-length": 88,
            "target-version": ["py38"],
            "include": r"\\.pyi?$",
            "exclude": r"(\\.git|\\.venv|build|dist)"
        }
        
        with open(directory / 'pyproject.toml', 'w') as f:
            f.write("[tool.black]\n")
            for key, value in black_config.items():
                if isinstance(value, str):
                    f.write(f'{key} = "{value}"\n')
                elif isinstance(value, list):
                    f.write(f'{key} = {value}\n')
                else:
                    f.write(f'{key} = {value}\n')
    
    def _create_development_scripts(self, directory: Path):
        """Create helpful development scripts."""
        scripts_dir = directory / 'tools'
        
        # Plugin validation script
        validate_script = '''#!/usr/bin/env python3
"""
Validate all plugins in the development environment.
"""

import sys
from pathlib import Path
import subprocess

def main():
    """Run validation on all plugins."""
    plugins_dir = Path("plugins")
    
    if not plugins_dir.exists():
        print("No plugins directory found")
        return 1
    
    failed = 0
    for plugin_dir in plugins_dir.iterdir():
        if plugin_dir.is_dir() and not plugin_dir.name.startswith('.'):
            print(f"\\n🔍 Validating {plugin_dir.name}...")
            try:
                result = subprocess.run([
                    sys.executable, "-m", "tools.validator", "validate", str(plugin_dir)
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"✅ {plugin_dir.name} passed validation")
                else:
                    print(f"❌ {plugin_dir.name} failed validation:")
                    print(result.stdout)
                    failed += 1
            except Exception as e:
                print(f"❌ Error validating {plugin_dir.name}: {e}")
                failed += 1
    
    if failed:
        print(f"\\n⚠️  {failed} plugin(s) failed validation")
        return 1
    else:
        print("\\n🎉 All plugins passed validation!")
        return 0

if __name__ == "__main__":
    sys.exit(main())
'''
        
        with open(scripts_dir / 'validate_all.py', 'w') as f:
            f.write(validate_script)
        
        # Test runner script
        test_script = '''#!/usr/bin/env python3
"""
Run tests for all plugins in the development environment.
"""

import sys
import subprocess
from pathlib import Path

def main():
    """Run tests for all plugins."""
    print("🧪 Running plugin tests...")
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", "plugins/", "tests/", "-v"
        ], cwd=Path.cwd())
        
        return result.returncode
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
'''
        
        with open(scripts_dir / 'test_all.py', 'w') as f:
            f.write(test_script)
        
        # Development server script
        dev_server_script = '''#!/usr/bin/env python3
"""
Start RedditDL in development mode with plugin hot reloading.
"""

import sys
import os
import json
from pathlib import Path

def main():
    """Start development server."""
    config_file = Path(".redditdl-dev/config.json")
    
    if not config_file.exists():
        print("❌ Development configuration not found")
        print("Run 'python -m tools.setup_dev init' first")
        return 1
    
    # Set development environment variables
    os.environ["REDDITDL_DEV_MODE"] = "1"
    os.environ["REDDITDL_CONFIG"] = str(config_file)
    
    print("🚀 Starting RedditDL in development mode...")
    print("Plugin hot reloading enabled")
    print("Press Ctrl+C to stop")
    
    try:
        # Import and start RedditDL with development settings
        # This would integrate with the main RedditDL application
        import redditdl
        redditdl.run_development_mode()
    except ImportError:
        print("❌ RedditDL not found in Python path")
        print("Make sure you're in the RedditDL project directory")
        return 1
    except KeyboardInterrupt:
        print("\\n👋 Development server stopped")
        return 0

if __name__ == "__main__":
    sys.exit(main())
'''
        
        with open(scripts_dir / 'dev_server.py', 'w') as f:
            f.write(dev_server_script)
        
        # Make scripts executable on Unix-like systems
        if hasattr(os, 'chmod'):
            for script in scripts_dir.glob('*.py'):
                os.chmod(script, 0o755)
    
    def _setup_examples_and_templates(self, directory: Path):
        """Set up example plugins and templates."""
        # Copy templates from the main RedditDL installation
        redditdl_root = Path(__file__).parent.parent
        templates_source = redditdl_root / 'plugins' / 'templates'
        examples_source = redditdl_root / 'plugins' / 'examples'
        
        templates_dest = directory / 'templates'
        examples_dest = directory / 'examples'
        
        if templates_source.exists():
            shutil.copytree(templates_source, templates_dest, dirs_exist_ok=True)
        
        if examples_source.exists():
            shutil.copytree(examples_source, examples_dest, dirs_exist_ok=True)
    
    def _create_development_docs(self, directory: Path):
        """Create development documentation."""
        docs_dir = directory / 'docs'
        
        readme_content = f"""# RedditDL Plugin Development Environment

Welcome to your RedditDL plugin development environment! This directory contains everything you need to develop, test, and distribute RedditDL plugins.

## Quick Start

1. **Activate the virtual environment:**
   ```bash
   # Linux/macOS
   source .venv/bin/activate
   
   # Windows
   .venv\\Scripts\\activate
   ```

2. **Create a new plugin:**
   ```bash
   python -m tools.scaffold create my_plugin --type content_handler
   ```

3. **Validate your plugin:**
   ```bash
   python -m tools.validator validate plugins/my_plugin/
   ```

4. **Test your plugin:**
   ```bash
   python tools/test_all.py
   ```

## Directory Structure

```
{directory.name}/
├── .venv/              # Virtual environment
├── .redditdl-dev/      # Development configuration
├── plugins/            # Your plugin development
├── examples/           # Example plugins
├── templates/          # Plugin templates
├── tests/              # Integration tests
├── tools/              # Development scripts
├── docs/               # Documentation
└── dist/               # Built plugins
```

## Development Workflow

### Creating a Plugin

1. Use the scaffolding tool:
   ```bash
   python -m tools.scaffold create my_plugin --type content_handler --advanced
   ```

2. Edit the generated files in `plugins/my_plugin/`

3. Implement the required methods

4. Write tests in `plugins/my_plugin/tests/`

### Testing

```bash
# Test a specific plugin
cd plugins/my_plugin && python -m pytest tests/

# Test all plugins
python tools/test_all.py

# Run with coverage
pytest --cov=plugins --cov-report=html
```

### Validation

```bash
# Validate a specific plugin
python -m tools.validator validate plugins/my_plugin/

# Validate all plugins
python tools/validate_all.py

# Lint plugin code
python -m tools.validator lint plugins/my_plugin/
```

### Development Server

```bash
# Start RedditDL in development mode (with hot reloading)
python tools/dev_server.py
```

## Available Tools

- **Scaffolding:** `python -m tools.scaffold create <name> --type <type>`
- **Validation:** `python -m tools.validator validate <plugin>`
- **Testing:** `python tools/test_all.py`
- **Development Server:** `python tools/dev_server.py`

## Plugin Types

- **content_handler:** Process and download specific content types
- **filter:** Filter posts based on custom criteria  
- **exporter:** Export data to custom formats
- **scraper:** Scrape content from custom sources
- **multi:** Multi-type plugin with multiple handlers

## Configuration

Development configuration is in `.redditdl-dev/config.json`. Key settings:

- `debug`: Enable debug mode
- `auto_reload`: Automatically reload plugins when files change
- `plugin_directories`: Directories to search for plugins
- `validation.strict`: Enable strict validation mode

## Resources

- [Plugin Development Guide](../plugins/README.md)
- [Plugin Templates](./templates/)
- [Example Plugins](./examples/)
- [RedditDL Documentation](https://redditdl.readthedocs.io/)

## Troubleshooting

### Common Issues

**Plugin not loading:**
- Check `python tools/validate_all.py` for validation errors
- Verify plugin.json manifest is valid
- Check development.log for detailed error messages

**Import errors:**
- Ensure virtual environment is activated
- Check that all dependencies are installed
- Verify Python path includes plugin directories

**Tests failing:**
- Run `pytest -v` for detailed test output
- Check test dependencies are installed
- Verify mock objects match actual interfaces

### Getting Help

1. Check the development log: `.redditdl-dev/development.log`
2. Run validation: `python tools/validate_all.py`
3. Check the [Plugin Development Guide](../plugins/README.md)
4. Open an issue on the RedditDL GitHub repository

Happy plugin development! 🚀
"""
        
        with open(docs_dir / 'README.md', 'w') as f:
            f.write(readme_content)
    
    def _print_getting_started_guide(self, directory: Path):
        """Print getting started instructions."""
        print(f"""
📚 Getting Started:

1. Activate the virtual environment:
   {'source .venv/bin/activate' if os.name != 'nt' else '.venv\\Scripts\\activate'}

2. Create your first plugin:
   python -m tools.scaffold create my_plugin --type content_handler

3. Validate your plugin:
   python -m tools.validator validate plugins/my_plugin/

4. Test your plugin:
   python tools/test_all.py

5. Start development server:
   python tools/dev_server.py

📖 Documentation: {directory}/docs/README.md
🔧 Configuration: {directory}/.redditdl-dev/config.json
🧪 Examples: {directory}/examples/

Happy plugin development! 🎉
""")
    
    def check_environment(self) -> bool:
        """Check if the development environment is properly set up."""
        print("🔍 Checking development environment...")
        
        issues = []
        
        # Check Python version
        if sys.version_info < (3, 8):
            issues.append(f"Python 3.8+ required, found {sys.version}")
        else:
            print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor}")
        
        # Check virtual environment
        venv_path = self.base_dir / '.venv'
        if venv_path.exists():
            print(f"✅ Virtual environment: {venv_path}")
        else:
            issues.append("Virtual environment not found")
        
        # Check configuration
        config_path = self.base_dir / '.redditdl-dev' / 'config.json'
        if config_path.exists():
            print(f"✅ Development config: {config_path}")
        else:
            issues.append("Development configuration not found")
        
        # Check required tools
        required_tools = ['pytest', 'black', 'isort', 'mypy']
        for tool in required_tools:
            try:
                subprocess.run([sys.executable, '-m', tool, '--version'], 
                             capture_output=True, check=True)
                print(f"✅ {tool} available")
            except (subprocess.CalledProcessError, FileNotFoundError):
                issues.append(f"{tool} not available")
        
        if issues:
            print(f"\n❌ Found {len(issues)} issue(s):")
            for issue in issues:
                print(f"  • {issue}")
            print("\nRun 'python -m tools.setup_dev init' to fix these issues")
            return False
        else:
            print("\n✅ Development environment is ready!")
            return True
    
    def install_dependencies(self) -> bool:
        """Install or update development dependencies."""
        print("📦 Installing development dependencies...")
        
        venv_path = self.base_dir / '.venv'
        if not venv_path.exists():
            print("❌ Virtual environment not found")
            print("Run 'python -m tools.setup_dev init' first")
            return False
        
        return self._install_development_dependencies(self.base_dir)
    
    def enable_development_mode(self) -> bool:
        """Enable development mode with advanced debugging features."""
        print("🛠️  Enabling RedditDL development mode...")
        
        config_path = self.base_dir / '.redditdl-dev' / 'config.json'
        if not config_path.exists():
            print("❌ Development configuration not found")
            print("Run 'python -m tools.setup_dev init' first")
            return False
        
        try:
            # Load existing config
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Enable development mode features
            config['development_mode']['enabled'] = True
            config['development_mode']['hot_reload'] = True
            config['development_mode']['sandbox_isolation'] = True
            config['development_mode']['debug_profiling'] = True
            config['development_mode']['verbose_errors'] = True
            config['development_mode']['plugin_state_tracking'] = True
            config['development_mode']['performance_monitoring'] = True
            config['development_mode']['memory_profiling'] = True
            
            # Enable debugging features
            config['debugging']['enabled'] = True
            config['debugging']['trace_execution'] = True
            config['debugging']['profile_performance'] = True
            config['debugging']['capture_plugin_state'] = True
            config['debugging']['export_debug_data'] = True
            
            # Enable validation features
            config['validation']['auto_validate_on_change'] = True
            config['validation']['strict'] = True
            
            # Enable testing features
            config['testing']['auto_test_on_change'] = True
            
            # Set debug logging
            config['logging']['level'] = 'DEBUG'
            
            # Save updated config
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Create debug output directory
            debug_dir = self.base_dir / '.redditdl-dev' / 'debug'
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            print("✅ Development mode enabled with features:")
            print("  • Hot reloading for plugins")
            print("  • Sandbox isolation for testing")
            print("  • Debug profiling and tracing")
            print("  • Verbose error reporting")
            print("  • Plugin state tracking")
            print("  • Performance monitoring")
            print("  • Memory profiling")
            print("  • Auto-validation on changes")
            print("  • Auto-testing on changes")
            
            print(f"\n📊 Debug data will be saved to: {debug_dir}")
            print("🔄 Use 'python tools/dev_server.py' to start with hot reloading")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to enable development mode: {e}")
            return False
    
    def disable_development_mode(self) -> bool:
        """Disable development mode and return to normal operation."""
        print("⚙️  Disabling RedditDL development mode...")
        
        config_path = self.base_dir / '.redditdl-dev' / 'config.json'
        if not config_path.exists():
            print("❌ Development configuration not found")
            return False
        
        try:
            # Load existing config
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Disable development mode features
            config['development_mode']['enabled'] = False
            config['development_mode']['hot_reload'] = False
            config['development_mode']['sandbox_isolation'] = False
            config['development_mode']['debug_profiling'] = False
            config['development_mode']['verbose_errors'] = False
            config['development_mode']['plugin_state_tracking'] = False
            config['development_mode']['performance_monitoring'] = False
            config['development_mode']['memory_profiling'] = False
            
            # Disable debugging features
            config['debugging']['enabled'] = False
            config['debugging']['trace_execution'] = False
            config['debugging']['profile_performance'] = False
            config['debugging']['capture_plugin_state'] = False
            config['debugging']['export_debug_data'] = False
            
            # Disable auto-features
            config['validation']['auto_validate_on_change'] = False
            config['testing']['auto_test_on_change'] = False
            
            # Set normal logging
            config['logging']['level'] = 'INFO'
            
            # Save updated config
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            print("✅ Development mode disabled")
            print("🔄 Normal operation mode enabled")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to disable development mode: {e}")
            return False
    
    def show_development_status(self) -> None:
        """Show current development mode status and configuration."""
        print("📊 RedditDL Development Environment Status")
        print("=" * 45)
        
        config_path = self.base_dir / '.redditdl-dev' / 'config.json'
        if not config_path.exists():
            print("❌ Development configuration not found")
            print("Run 'python -m tools.setup_dev init' to create it")
            return
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Show general status
            dev_mode = config.get('development_mode', {})
            debugging = config.get('debugging', {})
            validation = config.get('validation', {})
            testing = config.get('testing', {})
            logging_config = config.get('logging', {})
            
            print(f"Environment: {config.get('environment', 'unknown')}")
            print(f"Debug Mode: {'✅ Enabled' if config.get('debug', False) else '❌ Disabled'}")
            print(f"Development Mode: {'✅ Enabled' if dev_mode.get('enabled', False) else '❌ Disabled'}")
            
            print("\n🔧 Development Features:")
            features = [
                ("Hot Reload", dev_mode.get('hot_reload', False)),
                ("Sandbox Isolation", dev_mode.get('sandbox_isolation', False)),
                ("Debug Profiling", dev_mode.get('debug_profiling', False)),
                ("Verbose Errors", dev_mode.get('verbose_errors', False)),
                ("Plugin State Tracking", dev_mode.get('plugin_state_tracking', False)),
                ("Performance Monitoring", dev_mode.get('performance_monitoring', False)),
                ("Memory Profiling", dev_mode.get('memory_profiling', False))
            ]
            
            for feature, enabled in features:
                status = "✅" if enabled else "❌"
                print(f"  {status} {feature}")
            
            print("\n🐛 Debugging Features:")
            debug_features = [
                ("Execution Tracing", debugging.get('trace_execution', False)),
                ("Performance Profiling", debugging.get('profile_performance', False)),
                ("Plugin State Capture", debugging.get('capture_plugin_state', False)),
                ("Debug Data Export", debugging.get('export_debug_data', False))
            ]
            
            for feature, enabled in debug_features:
                status = "✅" if enabled else "❌"
                print(f"  {status} {feature}")
            
            print("\n✅ Validation & Testing:")
            validation_features = [
                ("Strict Validation", validation.get('strict', False)),
                ("Auto-validate on Change", validation.get('auto_validate_on_change', False)),
                ("Auto-test on Change", testing.get('auto_test_on_change', False)),
                ("Security Checks", validation.get('security_checks', False))
            ]
            
            for feature, enabled in validation_features:
                status = "✅" if enabled else "❌"
                print(f"  {status} {feature}")
            
            print(f"\n📝 Logging Level: {logging_config.get('level', 'INFO')}")
            print(f"📁 Plugin Directories: {', '.join(config.get('plugin_directories', []))}")
            
            # Show file locations
            debug_dir = self.base_dir / '.redditdl-dev' / 'debug'
            print(f"\n📂 Configuration: {config_path}")
            print(f"📂 Debug Output: {debug_dir}")
            print(f"📂 Logs: {self.base_dir / '.redditdl-dev'}")
            
        except Exception as e:
            print(f"❌ Error reading configuration: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="RedditDL Plugin Development Environment Setup")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize development environment')
    init_parser.add_argument('directory', nargs='?', type=Path, default=Path.cwd(),
                            help='Directory to initialize (default: current)')
    init_parser.add_argument('--no-examples', action='store_true',
                            help='Skip example plugins and templates')
    
    # Install deps command
    deps_parser = subparsers.add_parser('install-deps', help='Install development dependencies')
    
    # Check environment command
    check_parser = subparsers.add_parser('check-environment', help='Check development environment')
    
    # Create config command
    config_parser = subparsers.add_parser('create-config', help='Create development configuration')
    config_parser.add_argument('directory', nargs='?', type=Path, default=Path.cwd(),
                              help='Directory for configuration')
    
    # Development mode commands
    dev_mode_parser = subparsers.add_parser('dev-mode', help='Manage development mode')
    dev_mode_subparsers = dev_mode_parser.add_subparsers(dest='dev_command', help='Development mode commands')
    
    # Enable development mode
    enable_parser = dev_mode_subparsers.add_parser('enable', help='Enable development mode')
    
    # Disable development mode
    disable_parser = dev_mode_subparsers.add_parser('disable', help='Disable development mode')
    
    # Show development status
    status_parser = dev_mode_subparsers.add_parser('status', help='Show development mode status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    setup = DevEnvironmentSetup()
    
    if args.command == 'init':
        success = setup.init_environment(args.directory, not args.no_examples)
        sys.exit(0 if success else 1)
    
    elif args.command == 'install-deps':
        success = setup.install_dependencies()
        sys.exit(0 if success else 1)
    
    elif args.command == 'check-environment':
        success = setup.check_environment()
        sys.exit(0 if success else 1)
    
    elif args.command == 'create-config':
        setup._create_development_config(args.directory)
        print(f"✅ Development configuration created in {args.directory}")
    
    elif args.command == 'dev-mode':
        if not args.dev_command:
            print("Usage: python -m tools.setup_dev dev-mode {enable|disable|status}")
            return
        
        if args.dev_command == 'enable':
            success = setup.enable_development_mode()
            sys.exit(0 if success else 1)
        
        elif args.dev_command == 'disable':
            success = setup.disable_development_mode()
            sys.exit(0 if success else 1)
        
        elif args.dev_command == 'status':
            setup.show_development_status()


if __name__ == '__main__':
    main()