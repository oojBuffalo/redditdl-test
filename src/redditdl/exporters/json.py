"""
JSON Exporter

Enhanced JSON exporter with schema validation, pretty printing,
and compression support for Reddit post metadata.
"""

import json
import gzip
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base import BaseExporter, ExportResult, FormatInfo


class JsonExporter(BaseExporter):
    """
    Enhanced JSON exporter for Reddit post data.
    
    Features:
    - Schema validation for consistent output structure
    - Pretty printing with configurable indentation
    - Optional compression support
    - Custom date serialization
    - Field filtering and selection
    - Metadata inclusion controls
    """
    
    def _create_format_info(self) -> FormatInfo:
        """Create format information for JSON export."""
        return FormatInfo(
            name="json",
            extension=".json",
            description="JavaScript Object Notation with schema validation",
            mime_type="application/json",
            supports_compression=True,
            supports_streaming=False,
            supports_incremental=True,
            schema_required=False
        )
    
    def _create_config_schema(self) -> Dict[str, Any]:
        """Create configuration schema for JSON export."""
        return {
            'indent': {
                'type': 'integer',
                'default': 2,
                'minimum': 0,
                'maximum': 8,
                'description': 'Number of spaces for indentation (0 for compact)'
            },
            'ensure_ascii': {
                'type': 'boolean',
                'default': False,
                'description': 'Escape non-ASCII characters'
            },
            'sort_keys': {
                'type': 'boolean',
                'default': True,
                'description': 'Sort object keys alphabetically'
            },
            'compress': {
                'type': 'boolean',
                'default': False,
                'description': 'Compress output with gzip'
            },
            'include_metadata': {
                'type': 'boolean',
                'default': True,
                'description': 'Include pipeline metadata in export'
            },
            'include_posts': {
                'type': 'boolean',
                'default': True,
                'description': 'Include post data in export'
            },
            'include_empty_fields': {
                'type': 'boolean',
                'default': True,
                'description': 'Include fields with null/empty values'
            },
            'field_filter': {
                'type': 'array',
                'default': [],
                'description': 'List of field names to include (empty = all fields)'
            },
            'exclude_fields': {
                'type': 'array',
                'default': [],
                'description': 'List of field names to exclude'
            },
            'date_format': {
                'type': 'string',
                'default': 'iso',
                'choices': ['iso', 'timestamp', 'readable'],
                'description': 'Format for date fields'
            },
            'schema_version': {
                'type': 'string',
                'default': '2.0',
                'description': 'Schema version for output validation'
            },
            'validate_output': {
                'type': 'boolean',
                'default': True,
                'description': 'Validate output against schema'
            }
        }
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> ExportResult:
        """Export data to JSON format with enhanced features."""
        start_time = time.time()
        result = ExportResult(format_name="json")
        
        try:
            # Validate input data
            validation_errors = self.validate_data(data)
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            # Prepare output path
            output_file = self.prepare_output_path(output_path, config)
            
            # Process data according to configuration
            processed_data = self._process_data(data, config)
            
            # Apply field filtering
            if config.get('field_filter') or config.get('exclude_fields'):
                processed_data = self._apply_field_filtering(processed_data, config)
            
            # Add export metadata
            processed_data = self._add_export_metadata(processed_data, config)
            
            # Validate output if requested
            if config.get('validate_output', True):
                schema_errors = self._validate_output_schema(processed_data, config)
                if schema_errors:
                    for error in schema_errors:
                        result.add_warning(f"Schema validation: {error}")
            
            # Write output file
            self._write_json_file(processed_data, output_file, config, result)
            
            result.output_path = str(output_file)
            result.records_exported = len(data.get('posts', []))
            result.execution_time = time.time() - start_time
            
            # Get file size
            if output_file.exists():
                result.file_size = output_file.stat().st_size
            
            self.logger.info(f"JSON export completed: {result.records_exported} records to {output_file}")
            
        except Exception as e:
            result.add_error(f"JSON export failed: {e}")
            self.logger.error(f"JSON export error: {e}")
        
        return result
    
    def _process_data(self, data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Process data according to configuration options."""
        processed = {}
        
        # Include posts if requested
        if config.get('include_posts', True) and 'posts' in data:
            posts = data['posts']
            processed_posts = []
            
            for post in posts:
                processed_post = self._process_post(post, config)
                if processed_post:  # Only include non-empty posts
                    processed_posts.append(processed_post)
            
            processed['posts'] = processed_posts
        
        # Include metadata if requested
        if config.get('include_metadata', True):
            # Copy non-post data
            for key, value in data.items():
                if key != 'posts':
                    processed[key] = self._process_value(value, config)
        
        return processed
    
    def _process_post(self, post: Dict[str, Any], config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single post according to configuration."""
        if not isinstance(post, dict):
            return None
        
        processed = {}
        include_empty = config.get('include_empty_fields', True)
        
        for key, value in post.items():
            processed_value = self._process_value(value, config)
            
            # Skip empty fields if not including them
            if not include_empty and self._is_empty_value(processed_value):
                continue
            
            processed[key] = processed_value
        
        return processed if processed else None
    
    def _process_value(self, value: Any, config: Dict[str, Any]) -> Any:
        """Process a single value according to configuration."""
        date_format = config.get('date_format', 'iso')
        
        # Handle datetime objects
        if isinstance(value, datetime):
            if date_format == 'iso':
                return value.isoformat()
            elif date_format == 'timestamp':
                return value.timestamp()
            elif date_format == 'readable':
                return value.strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Handle numeric timestamps
        if isinstance(value, (int, float)) and str(value).replace('.', '').isdigit():
            if 1000000000 <= value <= 9999999999:  # Unix timestamp range
                try:
                    dt = datetime.fromtimestamp(value)
                    if date_format == 'iso':
                        return dt.isoformat()
                    elif date_format == 'readable':
                        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                    # For timestamp format, keep original value
                except (ValueError, OSError):
                    pass
        
        # Handle lists recursively
        if isinstance(value, list):
            return [self._process_value(item, config) for item in value]
        
        # Handle dictionaries recursively
        if isinstance(value, dict):
            processed = {}
            include_empty = config.get('include_empty_fields', True)
            
            for k, v in value.items():
                processed_v = self._process_value(v, config)
                if include_empty or not self._is_empty_value(processed_v):
                    processed[k] = processed_v
            
            return processed
        
        # Return value as-is for other types
        return value
    
    def _apply_field_filtering(self, data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply field filtering to the data."""
        field_filter = config.get('field_filter', [])
        exclude_fields = config.get('exclude_fields', [])
        
        if not field_filter and not exclude_fields:
            return data
        
        # Apply filtering to posts
        if 'posts' in data and isinstance(data['posts'], list):
            filtered_posts = []
            
            for post in data['posts']:
                if isinstance(post, dict):
                    filtered_post = self._filter_post_fields(post, field_filter, exclude_fields)
                    if filtered_post:
                        filtered_posts.append(filtered_post)
            
            data['posts'] = filtered_posts
        
        return data
    
    def _filter_post_fields(self, post: Dict[str, Any], include_fields: List[str], 
                           exclude_fields: List[str]) -> Dict[str, Any]:
        """Filter fields in a single post."""
        filtered = {}
        
        # Always include essential fields
        essential_fields = {'id', 'title', 'url'}
        
        for key, value in post.items():
            # Include if in essential fields
            if key in essential_fields:
                filtered[key] = value
            # Include if in include_fields (when specified)
            elif include_fields and key in include_fields:
                filtered[key] = value
            # Include if not in exclude_fields and no include_fields specified
            elif not include_fields and key not in exclude_fields:
                filtered[key] = value
        
        return filtered
    
    def _add_export_metadata(self, data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Add export metadata to the output."""
        export_info = {
            'timestamp': datetime.now().isoformat(),
            'format': 'json',
            'schema_version': config.get('schema_version', '2.0'),
            'exporter': 'JsonExporter',
            'post_count': len(data.get('posts', [])),
            'configuration': {
                'include_metadata': config.get('include_metadata', True),
                'include_posts': config.get('include_posts', True),
                'date_format': config.get('date_format', 'iso'),
                'field_filtering_applied': bool(config.get('field_filter') or config.get('exclude_fields'))
            }
        }
        
        # Create new data structure with export_info first
        output_data = {'export_info': export_info}
        output_data.update(data)
        
        return output_data
    
    def _validate_output_schema(self, data: Dict[str, Any], config: Dict[str, Any]) -> List[str]:
        """Validate output data against expected schema."""
        errors = []
        
        # Check required top-level fields
        if 'export_info' not in data:
            errors.append("Missing export_info field")
        
        # Validate export_info structure
        export_info = data.get('export_info', {})
        required_export_fields = ['timestamp', 'format', 'schema_version', 'post_count']
        
        for field in required_export_fields:
            if field not in export_info:
                errors.append(f"Missing export_info.{field}")
        
        # Validate posts structure if present
        if 'posts' in data:
            posts = data['posts']
            if not isinstance(posts, list):
                errors.append("Posts field must be a list")
            else:
                # Check first few posts for basic structure
                for i, post in enumerate(posts[:5]):
                    if not isinstance(post, dict):
                        errors.append(f"Post {i} is not a dictionary")
                    elif 'id' not in post:
                        errors.append(f"Post {i} missing required 'id' field")
        
        return errors
    
    def _write_json_file(self, data: Dict[str, Any], output_file: Path, 
                        config: Dict[str, Any], result: ExportResult) -> None:
        """Write data to JSON file with optional compression."""
        # Prepare JSON serialization options
        json_options = {
            'indent': config.get('indent', 2),
            'ensure_ascii': config.get('ensure_ascii', False),
            'sort_keys': config.get('sort_keys', True),
            'default': self._json_serializer
        }
        
        # Compact output if indent is 0
        if json_options['indent'] == 0:
            json_options['indent'] = None
            json_options['separators'] = (',', ':')
        
        try:
            # Serialize to JSON string
            json_str = json.dumps(data, **json_options)
            
            # Write to file (with optional compression)
            if config.get('compress', False):
                output_file = output_file.with_suffix(output_file.suffix + '.gz')
                with gzip.open(output_file, 'wt', encoding='utf-8') as f:
                    f.write(json_str)
                result.metadata['compressed'] = True
            else:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(json_str)
                result.metadata['compressed'] = False
            
            result.metadata['json_options'] = json_options
            
        except Exception as e:
            raise IOError(f"Failed to write JSON file: {e}")
    
    def _json_serializer(self, obj: Any) -> Any:
        """Custom JSON serializer for non-standard types."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return str(obj)
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)
    
    def _is_empty_value(self, value: Any) -> bool:
        """Check if a value is considered empty."""
        return value is None or value == "" or value == [] or value == {}
    
    def _get_size_factor(self) -> float:
        """Get size factor for JSON format."""
        return 1.0  # JSON is our baseline
    
    def estimate_output_size(self, data: Dict[str, Any], config: Dict[str, Any]) -> int:
        """Estimate JSON output file size."""
        try:
            # Create a small sample to estimate size per record
            posts = data.get('posts', [])
            if not posts:
                return 1024  # Minimum size for metadata
            
            # Estimate based on first post
            sample_post = posts[0]
            sample_output = {
                'export_info': {
                    'timestamp': datetime.now().isoformat(),
                    'format': 'json',
                    'post_count': len(posts)
                },
                'posts': [sample_post]
            }
            
            # Apply configuration options
            processed_sample = self._process_data({'posts': [sample_post]}, config)
            sample_json = json.dumps(processed_sample, indent=config.get('indent', 2))
            
            # Calculate size per post
            base_size = 200  # Metadata overhead
            per_post_size = len(sample_json) - base_size
            
            total_size = base_size + (per_post_size * len(posts))
            
            # Apply compression factor if enabled
            if config.get('compress', False):
                total_size = int(total_size * 0.3)  # Rough gzip compression ratio
            
            return max(total_size, 1024)  # Minimum 1KB
            
        except Exception:
            # Fallback estimation
            post_count = len(data.get('posts', []))
            return max(post_count * 1500, 1024)  # 1.5KB per post estimate