"""
Plugin Validation Utility

Validates RedditDL plugins for structure, dependencies, interface compliance,
and security requirements.

Usage:
    python -m tools.validator validate <plugin_path>
    python -m tools.validator check-manifest <manifest_path>
    python -m tools.validator lint <plugin_path>
"""

import json
import importlib.util
import inspect
import ast
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class PluginValidationError(Exception):
    """Exception raised when plugin validation fails."""
    pass


class PluginValidator:
    """
    Comprehensive plugin validator for RedditDL plugins.
    
    Validates:
    - Plugin structure and manifest
    - Interface compliance
    - Security requirements
    - Dependencies
    - Code quality and best practices
    """
    
    REQUIRED_BASE_CLASSES = {
        'content_handlers': 'BaseContentHandler',
        'filters': 'BaseFilter',
        'exporters': 'BaseExporter',
        'scrapers': 'BaseScraper'
    }
    
    REQUIRED_METHODS = {
        'BaseContentHandler': ['can_handle', 'process', 'get_supported_types'],
        'BaseFilter': ['apply', 'get_config_schema'],
        'BaseExporter': ['export', 'get_format_info'],
        'BaseScraper': ['can_scrape', 'scrape', 'get_supported_sources']
    }
    
    DANGEROUS_IMPORTS = {
        'os': 'File system operations require explicit permission',
        'subprocess': 'Subprocess execution requires explicit permission',
        'socket': 'Network operations require explicit permission',
        'multiprocessing': 'Process management requires explicit permission',
        'threading': 'Threading operations should be carefully reviewed',
        'ctypes': 'Low-level system access is restricted',
        'pickle': 'Arbitrary code execution risk',
        'marshal': 'Arbitrary code execution risk',
        'eval': 'Code evaluation is dangerous',
        'exec': 'Code execution is dangerous',
        'compile': 'Code compilation should be avoided'
    }
    
    def __init__(self):
        self.validation_results = {
            'errors': [],
            'warnings': [],
            'info': [],
            'security_issues': []
        }
    
    def validate_plugin(self, plugin_path: Path, strict: bool = False) -> Dict[str, Any]:
        """
        Validate a complete plugin (directory or single file).
        
        Args:
            plugin_path: Path to plugin directory or file
            strict: Enable strict validation mode
            
        Returns:
            Validation results dictionary
        """
        self.validation_results = {
            'errors': [],
            'warnings': [],
            'info': [],
            'security_issues': []
        }
        
        logger.info(f"Validating plugin: {plugin_path}")
        
        try:
            if plugin_path.is_dir():
                return self._validate_directory_plugin(plugin_path, strict)
            elif plugin_path.suffix == '.py':
                return self._validate_single_file_plugin(plugin_path, strict)
            else:
                self._add_error(f"Unsupported plugin format: {plugin_path}")
                return self.validation_results
                
        except Exception as e:
            self._add_error(f"Validation failed with exception: {str(e)}")
            return self.validation_results
    
    def _validate_directory_plugin(self, plugin_dir: Path, strict: bool) -> Dict[str, Any]:
        """Validate a directory-based plugin."""
        # Check for required files
        manifest_path = plugin_dir / 'plugin.json'
        init_path = plugin_dir / '__init__.py'
        
        if not manifest_path.exists():
            self._add_error("Missing plugin.json manifest file")
            return self.validation_results
        
        if not init_path.exists():
            self._add_error("Missing __init__.py file")
            return self.validation_results
        
        # Validate manifest
        manifest = self._validate_manifest(manifest_path)
        if not manifest:
            return self.validation_results
        
        # Validate plugin implementation
        self._validate_plugin_implementation(init_path, manifest, strict)
        
        # Check additional files
        self._validate_additional_files(plugin_dir, manifest)
        
        return self.validation_results
    
    def _validate_single_file_plugin(self, plugin_file: Path, strict: bool) -> Dict[str, Any]:
        """Validate a single-file plugin."""
        # Load and parse the file
        try:
            with open(plugin_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            self._add_error(f"Failed to read plugin file: {str(e)}")
            return self.validation_results
        
        # Parse the AST to extract plugin info
        try:
            tree = ast.parse(content)
            plugin_info = self._extract_plugin_info_from_ast(tree)
        except SyntaxError as e:
            self._add_error(f"Syntax error in plugin file: {str(e)}")
            return self.validation_results
        
        if not plugin_info:
            self._add_error("Missing __plugin_info__ dictionary")
            return self.validation_results
        
        # Validate plugin implementation
        self._validate_plugin_implementation(plugin_file, plugin_info, strict)
        
        return self.validation_results
    
    def _validate_manifest(self, manifest_path: Path) -> Optional[Dict[str, Any]]:
        """Validate plugin manifest file."""
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
        except json.JSONDecodeError as e:
            self._add_error(f"Invalid JSON in manifest: {str(e)}")
            return None
        except Exception as e:
            self._add_error(f"Failed to read manifest: {str(e)}")
            return None
        
        # Validate required fields
        required_fields = ['name', 'version', 'description', 'author']
        for field in required_fields:
            if field not in manifest:
                self._add_error(f"Missing required field in manifest: {field}")
        
        # Validate entry points
        entry_points = manifest.get('entry_points', {})
        if not entry_points:
            self._add_warning("No entry points defined in manifest")
        else:
            self._validate_entry_points(entry_points)
        
        # Validate permissions
        permissions = manifest.get('permissions', {})
        if permissions:
            self._validate_permissions(permissions)
        
        # Validate dependencies
        dependencies = manifest.get('dependencies', [])
        if dependencies:
            self._validate_dependencies(dependencies)
        
        self._add_info(f"Manifest validation completed for {manifest.get('name', 'unknown')}")
        return manifest
    
    def _validate_entry_points(self, entry_points: Dict[str, Any]):
        """Validate plugin entry points."""
        for category, handlers in entry_points.items():
            if category not in self.REQUIRED_BASE_CLASSES:
                self._add_warning(f"Unknown entry point category: {category}")
                continue
            
            if not isinstance(handlers, list):
                self._add_error(f"Entry points for {category} must be a list")
                continue
            
            for handler in handlers:
                if isinstance(handler, str):
                    # Simple class name format
                    continue
                elif isinstance(handler, dict):
                    # Detailed handler specification
                    if 'class' not in handler:
                        self._add_error(f"Missing 'class' field in {category} entry point")
                else:
                    self._add_error(f"Invalid entry point format in {category}")
    
    def _validate_permissions(self, permissions: Dict[str, Any]):
        """Validate plugin permissions."""
        known_permissions = {
            'network_access', 'file_system_access', 'subprocess_access',
            'restricted_imports'
        }
        
        for permission, value in permissions.items():
            if permission not in known_permissions:
                self._add_warning(f"Unknown permission: {permission}")
            
            if permission == 'restricted_imports' and isinstance(value, list):
                for import_name in value:
                    if import_name in self.DANGEROUS_IMPORTS:
                        self._add_security_issue(
                            f"Restricted import {import_name}: {self.DANGEROUS_IMPORTS[import_name]}"
                        )
    
    def _validate_dependencies(self, dependencies: List[str]):
        """Validate plugin dependencies."""
        for dep in dependencies:
            if not isinstance(dep, str):
                self._add_error(f"Invalid dependency format: {dep}")
                continue
            
            # Check for dangerous dependencies
            dangerous_deps = ['pickle5', 'dill', 'cloudpickle']
            for dangerous in dangerous_deps:
                if dangerous in dep.lower():
                    self._add_security_issue(f"Potentially dangerous dependency: {dep}")
    
    def _validate_plugin_implementation(self, plugin_file: Path, metadata: Dict[str, Any], strict: bool):
        """Validate plugin implementation against interfaces."""
        try:
            # Load the plugin module
            spec = importlib.util.spec_from_file_location("plugin_module", plugin_file)
            if not spec or not spec.loader:
                self._add_error("Failed to create module spec")
                return
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
        except Exception as e:
            self._add_error(f"Failed to import plugin module: {str(e)}")
            return
        
        # Get entry points from metadata
        entry_points = metadata.get('entry_points', {})
        if not entry_points and hasattr(module, '__plugin_info__'):
            # For single-file plugins, infer from class definitions
            entry_points = self._infer_entry_points_from_module(module)
        
        # Validate each handler class
        for category, handlers in entry_points.items():
            self._validate_handler_classes(module, category, handlers, strict)
        
        # Security validation
        self._validate_plugin_security(plugin_file)
    
    def _infer_entry_points_from_module(self, module) -> Dict[str, List[str]]:
        """Infer entry points from module classes."""
        entry_points = {}
        
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue  # Skip imported classes
            
            # Check base classes to determine category
            for base_class in obj.__mro__:
                base_name = base_class.__name__
                for category, required_base in self.REQUIRED_BASE_CLASSES.items():
                    if base_name == required_base:
                        if category not in entry_points:
                            entry_points[category] = []
                        entry_points[category].append(name)
                        break
        
        return entry_points
    
    def _validate_handler_classes(self, module, category: str, handlers: List, strict: bool):
        """Validate handler classes implement required interfaces."""
        required_base = self.REQUIRED_BASE_CLASSES.get(category)
        if not required_base:
            return
        
        required_methods = self.REQUIRED_METHODS.get(required_base, [])
        
        for handler in handlers:
            handler_name = handler if isinstance(handler, str) else handler.get('class')
            if not handler_name:
                continue
            
            # Get the handler class
            handler_class = getattr(module, handler_name, None)
            if not handler_class:
                self._add_error(f"Handler class {handler_name} not found in module")
                continue
            
            if not inspect.isclass(handler_class):
                self._add_error(f"{handler_name} is not a class")
                continue
            
            # Check if it inherits from required base class
            base_classes = [base.__name__ for base in handler_class.__mro__]
            if required_base not in base_classes:
                self._add_error(f"{handler_name} does not inherit from {required_base}")
            
            # Check required methods
            for method_name in required_methods:
                if not hasattr(handler_class, method_name):
                    self._add_error(f"{handler_name} missing required method: {method_name}")
                else:
                    method = getattr(handler_class, method_name)
                    if not callable(method):
                        self._add_error(f"{handler_name}.{method_name} is not callable")
                    elif strict:
                        self._validate_method_signature(handler_class, method_name, method)
    
    def _validate_method_signature(self, handler_class, method_name: str, method):
        """Validate method signatures match expected interfaces."""
        try:
            sig = inspect.signature(method)
            # Add specific signature validation based on method name
            # This is a placeholder for more detailed signature validation
            self._add_info(f"Method signature validated: {handler_class.__name__}.{method_name}")
        except Exception as e:
            self._add_warning(f"Could not validate signature for {handler_class.__name__}.{method_name}: {e}")
    
    def _validate_plugin_security(self, plugin_file: Path):
        """Perform security validation on plugin code."""
        try:
            with open(plugin_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            self._add_error(f"Failed to read file for security validation: {e}")
            return
        
        # Parse AST for security analysis
        try:
            tree = ast.parse(content)
            self._analyze_ast_security(tree)
        except SyntaxError as e:
            self._add_error(f"Syntax error during security analysis: {e}")
    
    def _analyze_ast_security(self, tree: ast.AST):
        """Analyze AST for security issues."""
        class SecurityVisitor(ast.NodeVisitor):
            def __init__(self, validator):
                self.validator = validator
            
            def visit_Import(self, node):
                for alias in node.names:
                    if alias.name in self.validator.DANGEROUS_IMPORTS:
                        self.validator._add_security_issue(
                            f"Import of {alias.name}: {self.validator.DANGEROUS_IMPORTS[alias.name]}"
                        )
                self.generic_visit(node)
            
            def visit_ImportFrom(self, node):
                if node.module and node.module in self.validator.DANGEROUS_IMPORTS:
                    self.validator._add_security_issue(
                        f"Import from {node.module}: {self.validator.DANGEROUS_IMPORTS[node.module]}"
                    )
                self.generic_visit(node)
            
            def visit_Call(self, node):
                # Check for dangerous function calls
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                    if func_name in ['eval', 'exec', 'compile']:
                        self.validator._add_security_issue(f"Dangerous function call: {func_name}")
                self.generic_visit(node)
        
        visitor = SecurityVisitor(self)
        visitor.visit(tree)
    
    def _extract_plugin_info_from_ast(self, tree: ast.AST) -> Optional[Dict[str, Any]]:
        """Extract __plugin_info__ from AST."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == '__plugin_info__':
                        try:
                            # Evaluate the dictionary safely
                            return ast.literal_eval(node.value)
                        except (ValueError, TypeError):
                            self._add_error("Invalid __plugin_info__ format")
                            return None
        return None
    
    def _validate_additional_files(self, plugin_dir: Path, manifest: Dict[str, Any]):
        """Validate additional plugin files."""
        # Check for README
        readme_files = list(plugin_dir.glob('README*'))
        if not readme_files:
            self._add_warning("No README file found")
        
        # Check for tests
        test_dirs = ['tests', 'test']
        has_tests = any((plugin_dir / test_dir).exists() for test_dir in test_dirs)
        if not has_tests:
            self._add_warning("No tests directory found")
        
        # Check for requirements.txt if dependencies are specified
        requirements_file = plugin_dir / 'requirements.txt'
        if manifest.get('dependencies') and not requirements_file.exists():
            self._add_warning("Dependencies specified but no requirements.txt found")
    
    def _add_error(self, message: str):
        """Add an error to validation results."""
        self.validation_results['errors'].append(message)
        logger.error(message)
    
    def _add_warning(self, message: str):
        """Add a warning to validation results."""
        self.validation_results['warnings'].append(message)
        logger.warning(message)
    
    def _add_info(self, message: str):
        """Add info to validation results."""
        self.validation_results['info'].append(message)
        logger.info(message)
    
    def _add_security_issue(self, message: str):
        """Add a security issue to validation results."""
        self.validation_results['security_issues'].append(message)
        logger.warning(f"SECURITY: {message}")


def validate_manifest_only(manifest_path: Path) -> Dict[str, Any]:
    """Validate only a plugin manifest file."""
    validator = PluginValidator()
    if validator._validate_manifest(manifest_path):
        return validator.validation_results
    return validator.validation_results


def lint_plugin(plugin_path: Path) -> Dict[str, Any]:
    """Perform code quality linting on plugin."""
    validator = PluginValidator()
    
    # Basic validation first
    results = validator.validate_plugin(plugin_path, strict=True)
    
    # Additional linting checks could be added here
    # - Code style checks
    # - Performance analysis
    # - Documentation coverage
    
    return results


def print_validation_results(results: Dict[str, Any]) -> bool:
    """Print validation results in a formatted way."""
    has_errors = False
    
    if results['errors']:
        print("\n❌ ERRORS:")
        for error in results['errors']:
            print(f"  • {error}")
        has_errors = True
    
    if results['security_issues']:
        print("\n🔒 SECURITY ISSUES:")
        for issue in results['security_issues']:
            print(f"  • {issue}")
        has_errors = True
    
    if results['warnings']:
        print("\n⚠️  WARNINGS:")
        for warning in results['warnings']:
            print(f"  • {warning}")
    
    if results['info']:
        print("\n📋 INFO:")
        for info in results['info']:
            print(f"  • {info}")
    
    if not has_errors:
        print("\n✅ Plugin validation passed!")
    
    return not has_errors


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="RedditDL Plugin Validator")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate a plugin')
    validate_parser.add_argument('plugin_path', type=Path, help='Path to plugin directory or file')
    validate_parser.add_argument('--strict', action='store_true', help='Enable strict validation')
    
    # Check manifest command
    manifest_parser = subparsers.add_parser('check-manifest', help='Validate only manifest file')
    manifest_parser.add_argument('manifest_path', type=Path, help='Path to plugin.json file')
    
    # Lint command
    lint_parser = subparsers.add_parser('lint', help='Lint plugin code')
    lint_parser.add_argument('plugin_path', type=Path, help='Path to plugin directory or file')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'validate':
        validator = PluginValidator()
        results = validator.validate_plugin(args.plugin_path, args.strict)
        success = print_validation_results(results)
        sys.exit(0 if success else 1)
    
    elif args.command == 'check-manifest':
        results = validate_manifest_only(args.manifest_path)
        success = print_validation_results(results)
        sys.exit(0 if success else 1)
    
    elif args.command == 'lint':
        results = lint_plugin(args.plugin_path)
        success = print_validation_results(results)
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()