"""
Exporter Plugin Template

This template provides a starting point for creating exporter plugins
for RedditDL. Exporter plugins handle data export in different formats
(JSON, CSV, XML, custom formats, etc.).

To create a new exporter plugin:
1. Copy this template to your plugin file
2. Rename the class and update the plugin information
3. Implement the export logic in the export() method
4. Define the format information
5. Test your exporter thoroughly
"""

import json
from pathlib import Path
from typing import Any, Dict, List
from core.plugins.hooks import BaseExporter

# Plugin metadata (optional but recommended)
__plugin_info__ = {
    'name': 'example_exporter',
    'version': '1.0.0',
    'description': 'Example exporter plugin for custom format',
    'author': 'Your Name',
    'export_format': 'example',
    'file_extension': '.example'
}


class ExampleExporter(BaseExporter):
    """
    Example exporter plugin.
    
    This exporter demonstrates how to export data in a custom format.
    Replace 'example' with your specific format (e.g., 'xml', 'yaml', etc.).
    """
    
    def __init__(self):
        """Initialize the exporter."""
        self.format_info = {
            'name': 'example',
            'extension': '.example',
            'description': 'Example custom format exporter',
            'mime_type': 'application/x-example',
            'supports_compression': True,
            'supports_streaming': False
        }
        
        self.config_schema = {
            'pretty_print': {
                'type': 'boolean',
                'default': True,
                'description': 'Format output for readability'
            },
            'include_metadata': {
                'type': 'boolean',
                'default': True,
                'description': 'Include export metadata in output'
            },
            'compression': {
                'type': 'string',
                'default': 'none',
                'choices': ['none', 'gzip', 'zip'],
                'description': 'Compression method to use'
            },
            'encoding': {
                'type': 'string',
                'default': 'utf-8',
                'choices': ['utf-8', 'utf-16', 'ascii'],
                'description': 'Text encoding for output file'
            },
            'max_field_length': {
                'type': 'integer',
                'default': 10000,
                'description': 'Maximum length for text fields'
            }
        }
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> bool:
        """
        Export data to specified format and path.
        
        Args:
            data: Data dictionary to export
            output_path: Output file path
            config: Export configuration dictionary
            
        Returns:
            True if export was successful, False otherwise
        """
        try:
            # Extract configuration
            pretty_print = config.get('pretty_print', True)
            include_metadata = config.get('include_metadata', True)
            compression = config.get('compression', 'none')
            encoding = config.get('encoding', 'utf-8')
            max_field_length = config.get('max_field_length', 10000)
            
            # Prepare data for export
            export_data = self._prepare_export_data(data, include_metadata, max_field_length)
            
            # Generate export content
            content = self._generate_content(export_data, pretty_print)
            
            # Write to file
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            if compression == 'none':
                self._write_plain_file(output_file, content, encoding)
            elif compression == 'gzip':
                self._write_gzip_file(output_file, content, encoding)
            elif compression == 'zip':
                self._write_zip_file(output_file, content, encoding)
            else:
                raise ValueError(f"Unsupported compression: {compression}")
            
            return True
            
        except Exception as e:
            print(f"Export failed: {e}")
            return False
    
    def _prepare_export_data(self, data: Dict[str, Any], include_metadata: bool, 
                           max_field_length: int) -> Dict[str, Any]:
        """
        Prepare data for export by cleaning and structuring it.
        
        Args:
            data: Raw data dictionary
            include_metadata: Whether to include metadata
            max_field_length: Maximum field length
            
        Returns:
            Prepared data dictionary
        """
        export_data = {}
        
        # Add metadata if requested
        if include_metadata:
            export_data['export_info'] = {
                'format': self.format_info['name'],
                'version': __plugin_info__['version'],
                'exported_at': self._get_current_timestamp(),
                'exporter': 'ExampleExporter'
            }
        
        # Process posts data
        posts = data.get('posts', [])
        processed_posts = []
        
        for post in posts:
            processed_post = self._process_post(post, max_field_length)
            processed_posts.append(processed_post)
        
        export_data['posts'] = processed_posts
        
        # Add summary statistics
        if include_metadata:
            export_data['summary'] = {
                'total_posts': len(processed_posts),
                'export_size': len(str(export_data)),
                'fields_truncated': sum(1 for post in processed_posts 
                                      if post.get('_truncated_fields', []))
            }
        
        return export_data
    
    def _process_post(self, post: Dict[str, Any], max_field_length: int) -> Dict[str, Any]:
        """
        Process a single post for export.
        
        Args:
            post: Post dictionary
            max_field_length: Maximum field length
            
        Returns:
            Processed post dictionary
        """
        processed = {}
        truncated_fields = []
        
        for key, value in post.items():
            if isinstance(value, str) and len(value) > max_field_length:
                processed[key] = value[:max_field_length] + "..."
                truncated_fields.append(key)
            else:
                processed[key] = value
        
        if truncated_fields:
            processed['_truncated_fields'] = truncated_fields
        
        return processed
    
    def _generate_content(self, data: Dict[str, Any], pretty_print: bool) -> str:
        """
        Generate the actual export content in the target format.
        
        Args:
            data: Prepared data dictionary
            pretty_print: Whether to format for readability
            
        Returns:
            Export content as string
        """
        # TODO: Implement your custom format generation here
        # This is a placeholder implementation using JSON
        
        if pretty_print:
            # Example: Pretty-printed JSON format
            return json.dumps(data, indent=2, ensure_ascii=False, default=str)
        else:
            # Example: Compact JSON format
            return json.dumps(data, separators=(',', ':'), ensure_ascii=False, default=str)
        
        # For custom formats, you might do something like:
        # if self.format_info['name'] == 'xml':
        #     return self._generate_xml(data, pretty_print)
        # elif self.format_info['name'] == 'yaml':
        #     return self._generate_yaml(data, pretty_print)
        # else:
        #     return self._generate_custom_format(data, pretty_print)
    
    def _write_plain_file(self, output_file: Path, content: str, encoding: str) -> None:
        """Write content to a plain text file."""
        with open(output_file, 'w', encoding=encoding) as f:
            f.write(content)
    
    def _write_gzip_file(self, output_file: Path, content: str, encoding: str) -> None:
        """Write content to a gzip-compressed file."""
        import gzip
        
        with gzip.open(f"{output_file}.gz", 'wt', encoding=encoding) as f:
            f.write(content)
    
    def _write_zip_file(self, output_file: Path, content: str, encoding: str) -> None:
        """Write content to a zip-compressed file."""
        import zipfile
        
        with zipfile.ZipFile(f"{output_file}.zip", 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(output_file.name, content.encode(encoding))
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def get_format_info(self) -> Dict[str, str]:
        """
        Get format information (name, extension, description).
        
        Returns:
            Format information dictionary
        """
        return self.format_info.copy()
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get configuration schema for this exporter.
        
        Returns:
            Configuration schema dictionary
        """
        return self.config_schema.copy()
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate exporter configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate compression
        compression = config.get('compression', 'none')
        if compression not in ['none', 'gzip', 'zip']:
            errors.append(f"Invalid compression method: {compression}")
        
        # Validate encoding
        encoding = config.get('encoding', 'utf-8')
        if encoding not in ['utf-8', 'utf-16', 'ascii']:
            errors.append(f"Invalid encoding: {encoding}")
        
        # Validate max_field_length
        max_field_length = config.get('max_field_length', 10000)
        if not isinstance(max_field_length, int) or max_field_length <= 0:
            errors.append("max_field_length must be a positive integer")
        
        # Validate boolean fields
        for field in ['pretty_print', 'include_metadata']:
            value = config.get(field)
            if value is not None and not isinstance(value, bool):
                errors.append(f"{field} must be a boolean")
        
        return errors
    
    def get_supported_compressions(self) -> List[str]:
        """Get list of supported compression methods."""
        return ['none', 'gzip', 'zip']
    
    def estimate_output_size(self, data: Dict[str, Any], config: Dict[str, Any]) -> int:
        """
        Estimate the output file size for given data and config.
        
        Args:
            data: Data to be exported
            config: Export configuration
            
        Returns:
            Estimated file size in bytes
        """
        # Simple estimation based on JSON serialization
        test_content = json.dumps(data, default=str)
        base_size = len(test_content.encode('utf-8'))
        
        # Adjust for compression
        compression = config.get('compression', 'none')
        if compression == 'gzip':
            return int(base_size * 0.3)  # Rough gzip compression ratio
        elif compression == 'zip':
            return int(base_size * 0.4)  # Rough zip compression ratio
        else:
            return base_size


# Plugin initialization function (optional)
def initialize_plugin():
    """
    Initialize the plugin when it's loaded.
    
    This function is called once when the plugin is loaded.
    Use it for any setup that needs to happen before the plugin is used.
    """
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")
    
    # Example initialization:
    # - Validate required libraries are available
    # - Create output directories
    # - Load format-specific configuration


# Plugin cleanup function (optional)
def cleanup_plugin():
    """
    Clean up plugin resources when it's unloaded.
    
    This function is called when the plugin is being unloaded.
    Use it to clean up any resources, close connections, etc.
    """
    print(f"Cleaning up {__plugin_info__['name']}")
    
    # Example cleanup:
    # - Close any open files
    # - Clean up temporary files
    # - Save any cached data