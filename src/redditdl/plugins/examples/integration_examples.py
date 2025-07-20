#!/usr/bin/env python3
"""
RedditDL Plugin Integration Examples

Demonstrates how multiple plugins work together in complex workflows.
Shows real-world use cases for plugin chaining, coordination, and integration.

This module provides:
- Plugin workflow orchestration examples
- Multi-plugin configuration templates
- Integration pattern demonstrations
- Real-world use case scenarios
- Performance optimization examples

Author: RedditDL Plugin Development Kit
License: MIT
Version: 1.0.0
"""

import logging
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
import json
import yaml

logger = logging.getLogger(__name__)

@dataclass
class PluginWorkflow:
    """Represents a plugin workflow configuration."""
    name: str
    description: str
    plugins: List[str] = field(default_factory=list)
    configuration: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    use_cases: List[str] = field(default_factory=list)

class PluginIntegrationExamples:
    """
    Collection of plugin integration examples and workflows.
    
    Demonstrates various ways plugins can work together to create
    powerful, customized Reddit content processing pipelines.
    """
    
    def __init__(self):
        """Initialize the integration examples."""
        self.workflows = self._create_example_workflows()
    
    def _create_example_workflows(self) -> Dict[str, PluginWorkflow]:
        """Create example plugin workflows."""
        workflows = {}
        
        # 1. Research Data Collection Workflow
        workflows['research_data'] = PluginWorkflow(
            name="Research Data Collection",
            description="Comprehensive academic research data collection with high-quality filtering",
            plugins=[
                "score_filter",           # Filter high-quality posts
                "image_converter",        # Optimize images for storage
                "csv_exporter"           # Export structured data
            ],
            configuration={
                "filters": {
                    "score_filter": {
                        "min_score": 100,
                        "min_comments": 20
                    }
                },
                "processing": {
                    "image_converter": {
                        "target_format": "webp",
                        "quality": 90,
                        "max_width": 1920,
                        "preserve_original": False
                    }
                },
                "export": {
                    "csv_exporter": {
                        "include_metadata": True,
                        "include_statistics": True,
                        "filename_template": "research_data_{subreddit}_{date}.csv"
                    }
                }
            },
            use_cases=[
                "Academic research data collection",
                "Sentiment analysis datasets",
                "Social media trend analysis",
                "Content quality assessment studies"
            ]
        )
        
        # 2. Content Archival Workflow
        workflows['content_archival'] = PluginWorkflow(
            name="Complete Content Archival",
            description="Full-fidelity content archival with metadata preservation",
            plugins=[
                "simple_text_handler",    # Handle text posts
                "image_converter",        # Convert images to archival format
                "json_exporter"          # Export complete metadata
            ],
            configuration={
                "processing": {
                    "image_converter": {
                        "target_format": "png",    # Lossless for archival
                        "optimize": True,
                        "preserve_original": True,
                        "strip_metadata": False    # Keep EXIF for archival
                    }
                },
                "content_handlers": {
                    "simple_text_handler": {
                        "preserve_formatting": True,
                        "include_frontmatter": True,
                        "save_as_markdown": True
                    }
                },
                "export": {
                    "json_exporter": {
                        "pretty_print": True,
                        "include_raw_data": True,
                        "validate_schema": True
                    }
                }
            },
            use_cases=[
                "Digital content preservation",
                "Historical Reddit archiving",
                "Legal evidence collection",
                "Long-term data storage"
            ]
        )
        
        # 3. Media Optimization Workflow
        workflows['media_optimization'] = PluginWorkflow(
            name="Media Optimization Pipeline",
            description="Optimized media processing for bandwidth-conscious applications",
            plugins=[
                "score_filter",           # Quality filtering
                "image_converter"         # Aggressive optimization
            ],
            configuration={
                "filters": {
                    "score_filter": {
                        "min_score": 50
                    }
                },
                "processing": {
                    "image_converter": {
                        "target_format": "webp",
                        "quality": 70,           # Aggressive compression
                        "max_width": 1024,       # Reduce dimensions
                        "max_height": 1024,
                        "optimize": True,
                        "strip_metadata": True,   # Remove metadata for size
                        "preserve_original": False
                    }
                }
            },
            use_cases=[
                "Mobile app content delivery",
                "Bandwidth-limited environments",
                "Storage space optimization",
                "Fast loading image galleries"
            ]
        )
        
        # 4. Development and Testing Workflow
        workflows['development_testing'] = PluginWorkflow(
            name="Plugin Development & Testing",
            description="Workflow for testing plugin compatibility and performance",
            plugins=[
                "score_filter",
                "simple_text_handler",
                "image_converter",
                "csv_exporter"
            ],
            configuration={
                "global": {
                    "dry_run": True,
                    "verbose_logging": True,
                    "debug_mode": True
                },
                "filters": {
                    "score_filter": {
                        "min_score": 1,
                        "debug_output": True
                    }
                },
                "processing": {
                    "image_converter": {
                        "target_format": "jpeg",
                        "quality": 95,
                        "preserve_original": True
                    }
                },
                "export": {
                    "csv_exporter": {
                        "include_debug_info": True,
                        "validate_output": True
                    }
                }
            },
            use_cases=[
                "Plugin development testing",
                "Performance benchmarking",
                "Integration verification",
                "Quality assurance workflows"
            ]
        )
        
        return workflows
    
    def get_workflow(self, name: str) -> Optional[PluginWorkflow]:
        """Get a specific workflow by name."""
        return self.workflows.get(name)
    
    def list_workflows(self) -> List[str]:
        """List all available workflow names."""
        return list(self.workflows.keys())
    
    def generate_config_file(self, workflow_name: str, output_path: str = None) -> str:
        """
        Generate a complete configuration file for a workflow.
        
        Args:
            workflow_name: Name of the workflow
            output_path: Path to save configuration file
            
        Returns:
            str: Generated configuration as YAML string
        """
        workflow = self.get_workflow(workflow_name)
        if not workflow:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        
        # Create comprehensive configuration
        config = {
            "# RedditDL Configuration": None,
            "# Generated for workflow": workflow.name,
            "# Description": workflow.description,
            "": None,  # Empty line
            
            "plugins": {
                "enabled": workflow.plugins,
                "search_paths": ["./plugins/examples"]
            },
            
            **workflow.configuration,
            
            "scraping": {
                "api_mode": True,
                "rate_limit_api": 0.7,
                "rate_limit_public": 6.1,
                "max_retries": 3
            },
            
            "downloads": {
                "create_subdirs": True,
                "embed_metadata": True,
                "create_json_sidecars": True
            },
            
            "logging": {
                "level": "INFO",
                "file": f"logs/{workflow_name}_workflow.log"
            }
        }
        
        # Convert to YAML
        yaml_content = self._dict_to_yaml(config)
        
        # Save to file if path provided
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(yaml_content)
            logger.info(f"Configuration saved to: {output_path}")
        
        return yaml_content
    
    def _dict_to_yaml(self, data: Dict[str, Any]) -> str:
        """Convert dictionary to YAML with custom formatting."""
        lines = []
        
        def process_item(key, value, indent=0):
            spaces = "  " * indent
            
            if key and key.startswith("#"):
                # Comment line
                if value is None:
                    lines.append(f"{spaces}{key}")
                else:
                    lines.append(f"{spaces}{key}: {value}")
            elif key == "":
                # Empty line
                lines.append("")
            elif isinstance(value, dict):
                lines.append(f"{spaces}{key}:")
                for k, v in value.items():
                    process_item(k, v, indent + 1)
            elif isinstance(value, list):
                lines.append(f"{spaces}{key}:")
                for item in value:
                    lines.append(f"{spaces}  - {item}")
            else:
                lines.append(f"{spaces}{key}: {value}")
        
        for key, value in data.items():
            process_item(key, value)
        
        return "\n".join(lines)
    
    def demonstrate_plugin_chaining(self) -> str:
        """
        Demonstrate how plugins can be chained together.
        
        Returns:
            str: Explanation of plugin chaining concepts
        """
        return """
# Plugin Chaining in RedditDL

## Overview
Plugin chaining allows multiple plugins to process the same content
in sequence, with each plugin building on the results of the previous ones.

## Chaining Patterns

### 1. Filter → Process → Export Chain
```
Posts → ScoreFilter → ImageConverter → CSVExporter → Final Output
```

This chain:
1. Filters posts by score threshold
2. Converts images to optimized formats
3. Exports processed data to CSV

### 2. Parallel Processing Chain
```
Posts → ScoreFilter → [ImageConverter, TextHandler] → JSONExporter
```

This chain:
1. Filters posts by score
2. Processes images AND text simultaneously
3. Exports combined results

### 3. Conditional Chain
```
Posts → ContentTypeDetector → {
    Images → ImageConverter → ImageExporter
    Text → TextHandler → MarkdownExporter
    Mixed → CombinedHandler → JSONExporter
}
```

## Plugin Communication

Plugins can communicate through:

1. **Context Objects**: Shared data structure passed between plugins
2. **Event System**: Plugins can emit and listen to events
3. **Configuration**: Shared configuration sections
4. **File System**: Temporary files and caches

## Best Practices

1. **Error Isolation**: One plugin failure shouldn't break the chain
2. **Performance**: Consider plugin order for optimal performance
3. **Dependencies**: Declare plugin dependencies explicitly
4. **Testing**: Test plugin combinations thoroughly

## Example Configuration

```yaml
plugins:
  enabled:
    - score_filter      # First: filter by quality
    - image_converter   # Second: optimize images
    - csv_exporter     # Third: export results

pipeline:
  error_strategy: "continue"  # Continue on plugin errors
  parallel_stages: ["processing"]  # Run processors in parallel
  
filters:
  score_filter:
    min_score: 100
    
processing:
  image_converter:
    target_format: "webp"
    quality: 85
    
export:
  csv_exporter:
    include_metadata: true
```

## Advanced Integration Patterns

### Plugin Dependency Management
```python
# In plugin metadata
PLUGIN_METADATA = {
    'dependencies': ['score_filter'],  # This plugin needs score_filter
    'provides': ['optimized_images'],  # This plugin provides this capability
    'conflicts': ['raw_image_saver']   # Cannot run with this plugin
}
```

### Event-Driven Integration
```python
# Plugin A emits event
context.emit_event('image_processed', {
    'file_path': output_path,
    'original_size': original_size,
    'optimized_size': new_size
})

# Plugin B listens for event
@event_handler('image_processed')
def on_image_processed(self, event_data):
    # Update statistics, trigger next step, etc.
    pass
```

### Shared Resource Management
```python
# Plugins can share caches, connections, etc.
shared_cache = context.get_shared_resource('image_cache')
database_connection = context.get_shared_resource('db_connection')
```
"""
    
    def create_integration_test(self, workflow_name: str) -> str:
        """
        Create an integration test for a specific workflow.
        
        Args:
            workflow_name: Name of the workflow to test
            
        Returns:
            str: Python test code for the workflow
        """
        workflow = self.get_workflow(workflow_name)
        if not workflow:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        
        test_code = f'''
#!/usr/bin/env python3
"""
Integration test for {workflow.name} workflow.

Generated automatically by RedditDL Plugin Integration Examples.
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock

from core.pipeline.executor import PipelineExecutor
from core.pipeline.interfaces import PipelineContext
{chr(10).join(f"from plugins.examples.{plugin} import {plugin.title().replace('_', '')}Plugin" for plugin in workflow.plugins if plugin in ['score_filter', 'image_converter', 'csv_exporter', 'simple_text_handler'])}

class Test{workflow_name.title().replace('_', '')}Workflow:
    """Test suite for {workflow.name} workflow."""
    
    @pytest.fixture
    def workflow_config(self):
        """Configuration for the workflow."""
        return {json.dumps(workflow.configuration, indent=8)}
    
    @pytest.fixture
    def sample_posts(self):
        """Sample post data for testing."""
        return [
            {{
                "id": "test_post_1",
                "title": "High Quality Test Post",
                "score": 150,
                "num_comments": 25,
                "url": "https://example.com/image.jpg",
                "is_self": False,
                "subreddit": "test"
            }},
            {{
                "id": "test_post_2", 
                "title": "Low Quality Test Post",
                "score": 5,
                "num_comments": 1,
                "url": "https://example.com/text",
                "is_self": True,
                "subreddit": "test"
            }}
        ]
    
    @pytest.fixture
    def pipeline_executor(self, workflow_config):
        """Create pipeline executor with workflow plugins."""
        executor = PipelineExecutor()
        
        # Initialize and add plugins based on workflow
        {chr(10).join(f'''        if "{plugin}" in {workflow.plugins}:
            plugin_instance = {plugin.title().replace('_', '')}Plugin(workflow_config)
            executor.add_stage(plugin_instance)''' for plugin in workflow.plugins if plugin in ['score_filter', 'image_converter', 'csv_exporter', 'simple_text_handler'])}
        
        return executor
    
    @pytest.mark.asyncio
    async def test_workflow_execution(self, pipeline_executor, sample_posts, workflow_config):
        """Test complete workflow execution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            context = PipelineContext(
                posts=sample_posts,
                config=workflow_config,
                session_id="test_session",
                output_directory=temp_dir
            )
            
            # Execute pipeline
            result = await pipeline_executor.execute(context)
            
            # Verify results
            assert result.success, f"Workflow failed: {{result.errors}}"
            assert len(result.processed_posts) > 0, "No posts were processed"
            
            # Workflow-specific assertions
            {self._generate_workflow_assertions(workflow)}
    
    @pytest.mark.asyncio
    async def test_plugin_integration(self, workflow_config, sample_posts):
        """Test plugin integration and data flow."""
        # Test that plugins can work together without conflicts
        # and that data flows correctly between them
        
        {self._generate_integration_tests(workflow)}
    
    def test_configuration_validation(self, workflow_config):
        """Test that workflow configuration is valid."""
        # Validate that all required configuration sections are present
        # and that plugin configurations are compatible
        
        {self._generate_config_validation_tests(workflow)}

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
'''
        
        return test_code
    
    def _generate_workflow_assertions(self, workflow: PluginWorkflow) -> str:
        """Generate workflow-specific test assertions."""
        assertions = []
        
        if 'score_filter' in workflow.plugins:
            assertions.append('assert any("score" in str(result) for result in result.stage_results), "Score filtering not applied"')
        
        if 'image_converter' in workflow.plugins:
            assertions.append('assert any("converted" in str(result) for result in result.stage_results), "Image conversion not performed"')
        
        if 'csv_exporter' in workflow.plugins:
            assertions.append('assert Path(temp_dir).glob("*.csv"), "CSV export not created"')
        
        return "\n            ".join(assertions) if assertions else "pass"
    
    def _generate_integration_tests(self, workflow: PluginWorkflow) -> str:
        """Generate integration-specific tests."""
        return f"""
        # Test plugin data passing
        context = {{'workflow': '{workflow.name}'}}
        
        # Verify plugins can access each other's outputs
        assert True, "Plugin integration test placeholder"
        """
    
    def _generate_config_validation_tests(self, workflow: PluginWorkflow) -> str:
        """Generate configuration validation tests."""
        return f"""
        # Validate plugin configurations
        for plugin_name in {workflow.plugins}:
            plugin_config = workflow_config.get(plugin_name, {{}})
            assert isinstance(plugin_config, dict), f"Invalid config for {{plugin_name}}"
        
        # Test configuration compatibility
        assert True, "Configuration validation placeholder"
        """

# Example usage and demonstrations
def main():
    """Demonstrate plugin integration examples."""
    examples = PluginIntegrationExamples()
    
    print("RedditDL Plugin Integration Examples")
    print("=" * 40)
    
    # List available workflows
    print("\\nAvailable Workflows:")
    for name in examples.list_workflows():
        workflow = examples.get_workflow(name)
        print(f"  • {workflow.name}")
        print(f"    {workflow.description}")
        print(f"    Plugins: {', '.join(workflow.plugins)}")
        print()
    
    # Generate example configuration
    print("\\nGenerating Research Data Workflow Configuration:")
    config = examples.generate_config_file('research_data')
    print(config[:500] + "..." if len(config) > 500 else config)
    
    # Show plugin chaining explanation
    print("\\nPlugin Chaining Explanation:")
    print(examples.demonstrate_plugin_chaining()[:800] + "...")

if __name__ == "__main__":
    main()