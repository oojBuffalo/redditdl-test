"""
Plugin Testing Framework

Specialized testing utilities for RedditDL plugin development, validation,
and quality assurance.

Usage:
    python -m tools.test_framework run <plugin_path>
    python -m tools.test_framework discover
    python -m tools.test_framework benchmark <plugin_path>
    python -m tools.test_framework integration <plugin_path>
"""

import argparse
import asyncio
import inspect
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, Tuple
from unittest.mock import Mock, AsyncMock, patch
import tempfile
import shutil
import importlib.util


class PluginTestCase:
    """
    Base class for plugin test cases.
    
    Provides common testing utilities and fixtures for plugin testing.
    """
    
    def __init__(self, plugin_path: Path):
        """
        Initialize test case for a specific plugin.
        
        Args:
            plugin_path: Path to the plugin directory or file
        """
        self.plugin_path = plugin_path
        self.plugin_module = None
        self.plugin_classes = {}
        self.test_results = []
        self.setup_done = False
    
    def setup(self):
        """Set up test environment and load plugin."""
        if self.setup_done:
            return
        
        try:
            # Load the plugin module
            if self.plugin_path.is_dir():
                init_file = self.plugin_path / '__init__.py'
                if not init_file.exists():
                    raise FileNotFoundError(f"No __init__.py found in {self.plugin_path}")
                spec = importlib.util.spec_from_file_location("test_plugin", init_file)
            else:
                spec = importlib.util.spec_from_file_location("test_plugin", self.plugin_path)
            
            if not spec or not spec.loader:
                raise ImportError(f"Failed to load plugin spec from {self.plugin_path}")
            
            self.plugin_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.plugin_module)
            
            # Extract plugin classes
            self._extract_plugin_classes()
            
            self.setup_done = True
            
        except Exception as e:
            raise Exception(f"Failed to setup test environment: {e}")
    
    def teardown(self):
        """Clean up test environment."""
        self.plugin_module = None
        self.plugin_classes = {}
        self.setup_done = False
    
    def _extract_plugin_classes(self):
        """Extract plugin classes from the module."""
        for name, obj in inspect.getmembers(self.plugin_module, inspect.isclass):
            if obj.__module__ == self.plugin_module.__name__:
                # Check if it's a plugin class by looking at base classes
                base_names = [base.__name__ for base in obj.__mro__]
                
                if 'BaseContentHandler' in base_names:
                    self.plugin_classes['content_handler'] = obj
                elif 'BaseFilter' in base_names:
                    self.plugin_classes['filter'] = obj
                elif 'BaseExporter' in base_names:
                    self.plugin_classes['exporter'] = obj
                elif 'BaseScraper' in base_names:
                    self.plugin_classes['scraper'] = obj
    
    def create_mock_post_data(self, post_type: str = "link") -> Dict[str, Any]:
        """Create mock post data for testing."""
        return {
            'id': 'test_post_123',
            'title': 'Test Post Title',
            'url': 'https://example.com/test.jpg',
            'author': 'test_author',
            'subreddit': 'test_subreddit',
            'score': 100,
            'num_comments': 25,
            'created_utc': time.time(),
            'is_nsfw': False,
            'is_self': post_type == 'self',
            'domain': 'example.com',
            'post_type': post_type,
            'selftext': 'Test post content' if post_type == 'self' else '',
            'gallery_image_urls': ['https://example.com/1.jpg', 'https://example.com/2.jpg'] if post_type == 'gallery' else [],
            'poll_data': {
                'question': 'Test poll?',
                'options': [{'text': 'Option 1', 'votes': 10}, {'text': 'Option 2', 'votes': 15}]
            } if post_type == 'poll' else None
        }
    
    def create_mock_config(self) -> Dict[str, Any]:
        """Create mock configuration for testing."""
        return {
            'enabled': True,
            'debug': True,
            'quality': 'high',
            'max_size': 10485760,
            'output_format': 'original',
            'timeout': 30,
            'retries': 3
        }
    
    async def run_test(self, test_func: Callable, *args, **kwargs) -> Dict[str, Any]:
        """
        Run a single test function and capture results.
        
        Args:
            test_func: Test function to run
            *args: Arguments to pass to test function
            **kwargs: Keyword arguments to pass to test function
            
        Returns:
            Test result dictionary
        """
        result = {
            'test_name': test_func.__name__,
            'success': False,
            'error': None,
            'duration': 0,
            'output': None
        }
        
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(test_func):
                output = await test_func(*args, **kwargs)
            else:
                output = test_func(*args, **kwargs)
            
            result['success'] = True
            result['output'] = output
            
        except Exception as e:
            result['error'] = str(e)
            result['traceback'] = traceback.format_exc()
        
        finally:
            result['duration'] = time.time() - start_time
        
        self.test_results.append(result)
        return result


class ContentHandlerTestCase(PluginTestCase):
    """Test case specifically for content handler plugins."""
    
    async def test_can_handle_method(self):
        """Test the can_handle method with various inputs."""
        if 'content_handler' not in self.plugin_classes:
            raise Exception("No content handler class found")
        
        handler = self.plugin_classes['content_handler']()
        
        # Test with different content types
        test_cases = [
            ('image', True),
            ('video', False),
            ('text', False),
            ('unknown', False)
        ]
        
        results = []
        for content_type, expected in test_cases:
            post_data = self.create_mock_post_data()
            result = handler.can_handle(content_type, post_data)
            results.append({
                'content_type': content_type,
                'expected': expected,
                'actual': result,
                'passed': result == expected
            })
        
        return results
    
    async def test_process_method(self):
        """Test the process method with mock data."""
        if 'content_handler' not in self.plugin_classes:
            raise Exception("No content handler class found")
        
        handler = self.plugin_classes['content_handler']()
        post_data = self.create_mock_post_data()
        config = self.create_mock_config()
        
        # Mock any external dependencies
        with patch('aiohttp.ClientSession'), \
             patch('pathlib.Path.write_bytes'), \
             patch('pathlib.Path.exists', return_value=False):
            
            result = await handler.process(post_data, config)
        
        # Validate result structure
        required_keys = ['success', 'processed_files', 'errors', 'metadata']
        for key in required_keys:
            if key not in result:
                raise Exception(f"Missing required key in result: {key}")
        
        return result
    
    async def test_get_supported_types(self):
        """Test the get_supported_types method."""
        if 'content_handler' not in self.plugin_classes:
            raise Exception("No content handler class found")
        
        handler = self.plugin_classes['content_handler']()
        supported_types = handler.get_supported_types()
        
        if not isinstance(supported_types, list):
            raise Exception("get_supported_types must return a list")
        
        if not supported_types:
            raise Exception("get_supported_types returned empty list")
        
        return supported_types


class FilterTestCase(PluginTestCase):
    """Test case specifically for filter plugins."""
    
    async def test_apply_method(self):
        """Test the apply method with various post lists."""
        if 'filter' not in self.plugin_classes:
            raise Exception("No filter class found")
        
        filter_instance = self.plugin_classes['filter']()
        
        # Create test posts
        posts = [
            self.create_mock_post_data(),
            {**self.create_mock_post_data(), 'score': 50, 'id': 'test_post_456'},
            {**self.create_mock_post_data(), 'score': 200, 'id': 'test_post_789'}
        ]
        
        config = self.create_mock_config()
        
        # Test filtering
        filtered_posts = filter_instance.apply(posts, config)
        
        if not isinstance(filtered_posts, list):
            raise Exception("apply method must return a list")
        
        # Filtered list should be subset of original
        if len(filtered_posts) > len(posts):
            raise Exception("Filtered list cannot be larger than original")
        
        return {
            'original_count': len(posts),
            'filtered_count': len(filtered_posts),
            'filter_ratio': len(filtered_posts) / len(posts) if posts else 0
        }
    
    async def test_get_config_schema(self):
        """Test the get_config_schema method."""
        if 'filter' not in self.plugin_classes:
            raise Exception("No filter class found")
        
        filter_instance = self.plugin_classes['filter']()
        schema = filter_instance.get_config_schema()
        
        if not isinstance(schema, dict):
            raise Exception("get_config_schema must return a dictionary")
        
        return schema


class ExporterTestCase(PluginTestCase):
    """Test case specifically for exporter plugins."""
    
    async def test_export_method(self):
        """Test the export method with mock data."""
        if 'exporter' not in self.plugin_classes:
            raise Exception("No exporter class found")
        
        exporter = self.plugin_classes['exporter']()
        
        # Create test data
        test_data = {
            'posts': [self.create_mock_post_data() for _ in range(3)],
            'metadata': {
                'export_time': time.time(),
                'total_posts': 3,
                'source': 'test'
            }
        }
        
        config = self.create_mock_config()
        
        # Use temporary file for testing
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            output_path = tmp_file.name
        
        try:
            result = exporter.export(test_data, output_path, config)
            
            if not isinstance(result, bool):
                raise Exception("export method must return a boolean")
            
            # Check if file was created
            output_file = Path(output_path)
            file_exists = output_file.exists()
            file_size = output_file.stat().st_size if file_exists else 0
            
            return {
                'export_success': result,
                'file_created': file_exists,
                'file_size': file_size
            }
        
        finally:
            # Clean up
            Path(output_path).unlink(missing_ok=True)
    
    async def test_get_format_info(self):
        """Test the get_format_info method."""
        if 'exporter' not in self.plugin_classes:
            raise Exception("No exporter class found")
        
        exporter = self.plugin_classes['exporter']()
        format_info = exporter.get_format_info()
        
        if not isinstance(format_info, dict):
            raise Exception("get_format_info must return a dictionary")
        
        required_keys = ['name', 'extension']
        for key in required_keys:
            if key not in format_info:
                raise Exception(f"Missing required key in format_info: {key}")
        
        return format_info


class PluginTestRunner:
    """
    Main test runner for plugin testing framework.
    
    Orchestrates test execution, result collection, and reporting.
    """
    
    def __init__(self):
        """Initialize the test runner."""
        self.test_results = []
        self.plugins_tested = 0
        self.total_tests_run = 0
        self.total_tests_passed = 0
    
    async def run_plugin_tests(self, plugin_path: Path) -> Dict[str, Any]:
        """
        Run comprehensive tests for a single plugin.
        
        Args:
            plugin_path: Path to plugin directory or file
            
        Returns:
            Test results dictionary
        """
        print(f"🧪 Testing plugin: {plugin_path}")
        
        # Determine plugin type and create appropriate test case
        test_cases = []
        
        try:
            # Try to determine plugin type from manifest or code inspection
            plugin_types = self._detect_plugin_types(plugin_path)
            
            for plugin_type in plugin_types:
                if plugin_type == 'content_handler':
                    test_cases.append(ContentHandlerTestCase(plugin_path))
                elif plugin_type == 'filter':
                    test_cases.append(FilterTestCase(plugin_path))
                elif plugin_type == 'exporter':
                    test_cases.append(ExporterTestCase(plugin_path))
                else:
                    test_cases.append(PluginTestCase(plugin_path))
            
            if not test_cases:
                test_cases.append(PluginTestCase(plugin_path))
            
            plugin_results = {
                'plugin_path': str(plugin_path),
                'plugin_types': plugin_types,
                'test_cases': [],
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0,
                'duration': 0
            }
            
            start_time = time.time()
            
            for test_case in test_cases:
                try:
                    test_case.setup()
                    case_results = await self._run_test_case(test_case)
                    plugin_results['test_cases'].append(case_results)
                    
                    plugin_results['total_tests'] += case_results['total_tests']
                    plugin_results['passed_tests'] += case_results['passed_tests']
                    plugin_results['failed_tests'] += case_results['failed_tests']
                    
                except Exception as e:
                    error_result = {
                        'test_case_type': test_case.__class__.__name__,
                        'setup_error': str(e),
                        'total_tests': 0,
                        'passed_tests': 0,
                        'failed_tests': 1
                    }
                    plugin_results['test_cases'].append(error_result)
                    plugin_results['failed_tests'] += 1
                
                finally:
                    test_case.teardown()
            
            plugin_results['duration'] = time.time() - start_time
            
            # Update global counters
            self.plugins_tested += 1
            self.total_tests_run += plugin_results['total_tests']
            self.total_tests_passed += plugin_results['passed_tests']
            
            return plugin_results
            
        except Exception as e:
            return {
                'plugin_path': str(plugin_path),
                'error': str(e),
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 1,
                'duration': 0
            }
    
    async def _run_test_case(self, test_case: PluginTestCase) -> Dict[str, Any]:
        """Run all tests in a test case."""
        case_results = {
            'test_case_type': test_case.__class__.__name__,
            'tests': [],
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0
        }
        
        # Get all test methods
        test_methods = [
            method for method in dir(test_case)
            if method.startswith('test_') and callable(getattr(test_case, method))
        ]
        
        for method_name in test_methods:
            test_method = getattr(test_case, method_name)
            result = await test_case.run_test(test_method)
            case_results['tests'].append(result)
            case_results['total_tests'] += 1
            
            if result['success']:
                case_results['passed_tests'] += 1
            else:
                case_results['failed_tests'] += 1
        
        return case_results
    
    def _detect_plugin_types(self, plugin_path: Path) -> List[str]:
        """Detect what types of plugins are in the given path."""
        plugin_types = []
        
        # Check manifest file for plugin type information
        if plugin_path.is_dir():
            manifest_path = plugin_path / 'plugin.json'
            if manifest_path.exists():
                try:
                    with open(manifest_path, 'r') as f:
                        manifest = json.load(f)
                    
                    # Get categories from manifest
                    categories = manifest.get('categories', [])
                    plugin_types.extend(categories)
                    
                    # Check entry points
                    entry_points = manifest.get('entry_points', {})
                    plugin_types.extend(entry_points.keys())
                    
                except Exception:
                    pass
        
        # If no types detected, try to inspect the code
        if not plugin_types:
            try:
                # Load the module and check for base classes
                if plugin_path.is_dir():
                    init_file = plugin_path / '__init__.py'
                    if init_file.exists():
                        with open(init_file, 'r') as f:
                            content = f.read()
                else:
                    with open(plugin_path, 'r') as f:
                        content = f.read()
                
                # Simple string matching for base classes
                if 'BaseContentHandler' in content:
                    plugin_types.append('content_handler')
                if 'BaseFilter' in content:
                    plugin_types.append('filter')
                if 'BaseExporter' in content:
                    plugin_types.append('exporter')
                if 'BaseScraper' in content:
                    plugin_types.append('scraper')
                    
            except Exception:
                pass
        
        return list(set(plugin_types))  # Remove duplicates
    
    def discover_plugins(self, directory: Path) -> List[Path]:
        """Discover all plugins in a directory."""
        plugins = []
        
        if not directory.exists():
            return plugins
        
        for item in directory.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Check for plugin.json or __init__.py
                if (item / 'plugin.json').exists() or (item / '__init__.py').exists():
                    plugins.append(item)
            elif item.suffix == '.py' and not item.name.startswith('_'):
                # Check for __plugin_info__ in file
                try:
                    with open(item, 'r') as f:
                        content = f.read()
                    if '__plugin_info__' in content:
                        plugins.append(item)
                except Exception:
                    pass
        
        return plugins
    
    async def benchmark_plugin(self, plugin_path: Path) -> Dict[str, Any]:
        """Run performance benchmarks on a plugin."""
        print(f"📊 Benchmarking plugin: {plugin_path}")
        
        benchmark_results = {
            'plugin_path': str(plugin_path),
            'benchmarks': {},
            'overall_score': 0
        }
        
        try:
            test_case = PluginTestCase(plugin_path)
            test_case.setup()
            
            # Memory usage benchmark
            memory_result = await self._benchmark_memory_usage(test_case)
            benchmark_results['benchmarks']['memory'] = memory_result
            
            # Performance benchmark
            performance_result = await self._benchmark_performance(test_case)
            benchmark_results['benchmarks']['performance'] = performance_result
            
            # Calculate overall score
            scores = [result.get('score', 0) for result in benchmark_results['benchmarks'].values()]
            benchmark_results['overall_score'] = sum(scores) / len(scores) if scores else 0
            
            test_case.teardown()
            
        except Exception as e:
            benchmark_results['error'] = str(e)
        
        return benchmark_results
    
    async def _benchmark_memory_usage(self, test_case: PluginTestCase) -> Dict[str, Any]:
        """Benchmark memory usage of plugin operations."""
        import tracemalloc
        
        tracemalloc.start()
        
        try:
            # Run plugin operations multiple times
            for _ in range(10):
                for plugin_class in test_case.plugin_classes.values():
                    instance = plugin_class()
                    
                    # Try to call main methods
                    if hasattr(instance, 'can_handle'):
                        instance.can_handle('test', {})
                    elif hasattr(instance, 'apply'):
                        instance.apply([test_case.create_mock_post_data()], {})
                    elif hasattr(instance, 'export'):
                        instance.export({}, '/tmp/test', {})
            
            current, peak = tracemalloc.get_traced_memory()
            
            return {
                'current_memory_mb': current / 1024 / 1024,
                'peak_memory_mb': peak / 1024 / 1024,
                'score': max(0, 100 - (peak / 1024 / 1024))  # Score based on peak memory
            }
        
        finally:
            tracemalloc.stop()
    
    async def _benchmark_performance(self, test_case: PluginTestCase) -> Dict[str, Any]:
        """Benchmark performance of plugin operations."""
        times = []
        
        for _ in range(100):  # Run 100 iterations
            start_time = time.perf_counter()
            
            for plugin_class in test_case.plugin_classes.values():
                instance = plugin_class()
                
                # Try to call main methods
                if hasattr(instance, 'can_handle'):
                    instance.can_handle('test', test_case.create_mock_post_data())
                elif hasattr(instance, 'apply'):
                    instance.apply([test_case.create_mock_post_data()], {})
            
            times.append(time.perf_counter() - start_time)
        
        avg_time = sum(times) / len(times)
        
        return {
            'average_time_ms': avg_time * 1000,
            'min_time_ms': min(times) * 1000,
            'max_time_ms': max(times) * 1000,
            'score': max(0, 100 - (avg_time * 1000))  # Score based on average time
        }
    
    def print_results(self, results: List[Dict[str, Any]]):
        """Print formatted test results."""
        print("\n" + "="*60)
        print("PLUGIN TEST RESULTS")
        print("="*60)
        
        for result in results:
            plugin_name = Path(result['plugin_path']).name
            
            if 'error' in result:
                print(f"\n❌ {plugin_name}: ERROR")
                print(f"   {result['error']}")
                continue
            
            total = result['total_tests']
            passed = result['passed_tests']
            failed = result['failed_tests']
            
            status = "✅ PASSED" if failed == 0 else f"❌ FAILED ({failed}/{total})"
            print(f"\n{status} {plugin_name}")
            print(f"   Tests: {passed}/{total} passed ({result['duration']:.2f}s)")
            
            # Show failed tests
            for test_case in result.get('test_cases', []):
                for test in test_case.get('tests', []):
                    if not test['success']:
                        print(f"   ❌ {test['test_name']}: {test['error']}")
        
        print(f"\n📊 SUMMARY:")
        print(f"   Plugins tested: {self.plugins_tested}")
        print(f"   Total tests: {self.total_tests_run}")
        print(f"   Passed: {self.total_tests_passed}")
        print(f"   Failed: {self.total_tests_run - self.total_tests_passed}")
        
        if self.total_tests_run > 0:
            pass_rate = (self.total_tests_passed / self.total_tests_run) * 100
            print(f"   Pass rate: {pass_rate:.1f}%")


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="RedditDL Plugin Testing Framework")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run tests for a specific plugin')
    run_parser.add_argument('plugin_path', type=Path, help='Path to plugin directory or file')
    
    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover and test all plugins')
    discover_parser.add_argument('directory', nargs='?', type=Path, default=Path('plugins'),
                                help='Directory to search for plugins')
    
    # Benchmark command
    benchmark_parser = subparsers.add_parser('benchmark', help='Benchmark plugin performance')
    benchmark_parser.add_argument('plugin_path', type=Path, help='Path to plugin directory or file')
    
    # Integration command
    integration_parser = subparsers.add_parser('integration', help='Run integration tests')
    integration_parser.add_argument('plugin_path', type=Path, help='Path to plugin directory or file')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    runner = PluginTestRunner()
    
    if args.command == 'run':
        result = await runner.run_plugin_tests(args.plugin_path)
        runner.print_results([result])
        sys.exit(0 if result.get('failed_tests', 1) == 0 else 1)
    
    elif args.command == 'discover':
        plugins = runner.discover_plugins(args.directory)
        if not plugins:
            print(f"No plugins found in {args.directory}")
            return
        
        print(f"Found {len(plugins)} plugin(s) in {args.directory}")
        results = []
        
        for plugin_path in plugins:
            result = await runner.run_plugin_tests(plugin_path)
            results.append(result)
        
        runner.print_results(results)
        total_failed = sum(r.get('failed_tests', 0) for r in results)
        sys.exit(0 if total_failed == 0 else 1)
    
    elif args.command == 'benchmark':
        result = await runner.benchmark_plugin(args.plugin_path)
        print(f"\n📊 Benchmark Results for {args.plugin_path}:")
        print(f"Overall Score: {result['overall_score']:.1f}/100")
        
        for name, benchmark in result.get('benchmarks', {}).items():
            print(f"\n{name.title()}:")
            for key, value in benchmark.items():
                if key != 'score':
                    print(f"  {key}: {value}")
    
    elif args.command == 'integration':
        # Integration tests would test plugin interaction with RedditDL core
        print(f"Running integration tests for {args.plugin_path}")
        print("Integration testing not yet implemented")


if __name__ == '__main__':
    asyncio.run(main())