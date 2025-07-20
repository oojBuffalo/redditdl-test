"""
CSV Exporter

Full-featured CSV exporter with metadata flattening, custom formatting,
and data analysis optimization for Reddit post data.
"""

import csv
import json
import gzip
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime
from io import StringIO

from .base import BaseExporter, ExportResult, FormatInfo


class CsvExporter(BaseExporter):
    """
    Full-featured CSV exporter for Reddit post data.
    
    Features:
    - Automatic metadata flattening for tabular format
    - Custom field mapping and selection
    - Data type conversion and formatting
    - Multiple CSV dialects support
    - Optional compression
    - Streaming support for large datasets
    - Excel compatibility mode
    """
    
    def _create_format_info(self) -> FormatInfo:
        """Create format information for CSV export."""
        return FormatInfo(
            name="csv",
            extension=".csv",
            description="Comma-separated values with metadata flattening",
            mime_type="text/csv",
            supports_compression=True,
            supports_streaming=True,
            supports_incremental=True,
            max_records=1000000,  # Excel limit
            schema_required=False
        )
    
    def _create_config_schema(self) -> Dict[str, Any]:
        """Create configuration schema for CSV export."""
        return {
            'delimiter': {
                'type': 'string',
                'default': ',',
                'description': 'Field delimiter character'
            },
            'quote_char': {
                'type': 'string',
                'default': '"',
                'description': 'Quote character for text fields'
            },
            'escape_char': {
                'type': 'string',
                'default': None,
                'description': 'Escape character (None for quote doubling)'
            },
            'line_terminator': {
                'type': 'string',
                'default': '\r\n',
                'choices': ['\r\n', '\n', '\r'],
                'description': 'Line terminator sequence'
            },
            'quoting': {
                'type': 'string',
                'default': 'minimal',
                'choices': ['minimal', 'all', 'nonnumeric', 'none'],
                'description': 'Quoting style for fields'
            },
            'include_header': {
                'type': 'boolean',
                'default': True,
                'description': 'Include column headers in output'
            },
            'flatten_nested': {
                'type': 'boolean',
                'default': True,
                'description': 'Flatten nested objects into separate columns'
            },
            'flatten_arrays': {
                'type': 'boolean',
                'default': True,
                'description': 'Flatten arrays into pipe-separated values'
            },
            'max_text_length': {
                'type': 'integer',
                'default': 1000,
                'minimum': 100,
                'maximum': 32767,
                'description': 'Maximum length for text fields'
            },
            'date_format': {
                'type': 'string',
                'default': '%Y-%m-%d %H:%M:%S',
                'description': 'Date format for timestamp fields'
            },
            'encoding': {
                'type': 'string',
                'default': 'utf-8',
                'choices': ['utf-8', 'utf-16', 'latin-1', 'cp1252'],
                'description': 'Text encoding for output file'
            },
            'excel_compatible': {
                'type': 'boolean',
                'default': False,
                'description': 'Generate Excel-compatible CSV'
            },
            'compress': {
                'type': 'boolean',
                'default': False,
                'description': 'Compress output with gzip'
            },
            'field_mapping': {
                'type': 'object',
                'default': {},
                'description': 'Custom field name mappings (old_name: new_name)'
            },
            'include_fields': {
                'type': 'array',
                'default': [],
                'description': 'List of field names to include (empty = all)'
            },
            'exclude_fields': {
                'type': 'array',
                'default': [],
                'description': 'List of field names to exclude'
            },
            'null_value': {
                'type': 'string',
                'default': '',
                'description': 'Representation for null/empty values'
            },
            'boolean_format': {
                'type': 'string',
                'default': 'true_false',
                'choices': ['true_false', 'yes_no', '1_0', 'TRUE_FALSE'],
                'description': 'Format for boolean values'
            },
            'array_separator': {
                'type': 'string',
                'default': '|',
                'description': 'Separator for array values when flattened'
            }
        }
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> ExportResult:
        """Export data to CSV format with metadata flattening."""
        start_time = time.time()
        result = ExportResult(format_name="csv")
        
        try:
            # Validate input data
            validation_errors = self.validate_data(data)
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            # Prepare output path
            output_file = self.prepare_output_path(output_path, config)
            
            # Extract and prepare posts data
            posts = data.get('posts', [])
            if not posts:
                result.add_warning("No posts to export")
                # Create empty file with headers only
                self._write_empty_csv(output_file, config)
                result.output_path = str(output_file)
                return result
            
            # Flatten and process posts
            flattened_posts = self._flatten_posts(posts, config)
            
            # Apply field filtering and mapping
            processed_posts = self._apply_field_processing(flattened_posts, config)
            
            # Write CSV file
            self._write_csv_file(processed_posts, output_file, config, result)
            
            result.output_path = str(output_file)
            result.records_exported = len(processed_posts)
            result.execution_time = time.time() - start_time
            
            # Get file size
            if output_file.exists():
                result.file_size = output_file.stat().st_size
            
            self.logger.info(f"CSV export completed: {result.records_exported} records to {output_file}")
            
        except Exception as e:
            result.add_error(f"CSV export failed: {e}")
            self.logger.error(f"CSV export error: {e}")
        
        return result
    
    def _flatten_posts(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten post metadata for tabular format."""
        flattened = []
        flatten_nested = config.get('flatten_nested', True)
        flatten_arrays = config.get('flatten_arrays', True)
        max_text_length = config.get('max_text_length', 1000)
        
        for post in posts:
            flattened_post = {}
            
            for key, value in post.items():
                processed_values = self._flatten_value(
                    key, value, flatten_nested, flatten_arrays, max_text_length, config
                )
                flattened_post.update(processed_values)
            
            flattened.append(flattened_post)
        
        return flattened
    
    def _flatten_value(self, key: str, value: Any, flatten_nested: bool, 
                      flatten_arrays: bool, max_text_length: int, 
                      config: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Flatten a single value into key-value pairs."""
        result = {}
        full_key = f"{prefix}.{key}" if prefix else key
        
        if value is None:
            result[full_key] = config.get('null_value', '')
        
        elif isinstance(value, dict) and flatten_nested:
            # Flatten nested dictionaries
            for nested_key, nested_value in value.items():
                nested_result = self._flatten_value(
                    nested_key, nested_value, flatten_nested, flatten_arrays,
                    max_text_length, config, full_key
                )
                result.update(nested_result)
        
        elif isinstance(value, list) and flatten_arrays:
            # Flatten arrays to pipe-separated values
            array_separator = config.get('array_separator', '|')
            if value:
                # Convert all items to strings and join
                string_items = []
                for item in value:
                    if isinstance(item, dict):
                        # For complex objects, use JSON representation
                        string_items.append(json.dumps(item, default=str))
                    else:
                        string_items.append(str(item))
                
                flattened_value = array_separator.join(string_items)
                result[full_key] = self._truncate_text(flattened_value, max_text_length)
            else:
                result[full_key] = config.get('null_value', '')
        
        elif isinstance(value, bool):
            # Format boolean values
            boolean_format = config.get('boolean_format', 'true_false')
            if boolean_format == 'yes_no':
                result[full_key] = 'yes' if value else 'no'
            elif boolean_format == '1_0':
                result[full_key] = '1' if value else '0'
            elif boolean_format == 'TRUE_FALSE':
                result[full_key] = 'TRUE' if value else 'FALSE'
            else:  # true_false
                result[full_key] = 'true' if value else 'false'
        
        elif isinstance(value, (int, float)):
            # Handle numeric timestamps
            if isinstance(value, (int, float)) and 1000000000 <= value <= 9999999999:
                try:
                    dt = datetime.fromtimestamp(value)
                    date_format = config.get('date_format', '%Y-%m-%d %H:%M:%S')
                    result[full_key] = dt.strftime(date_format)
                except (ValueError, OSError):
                    result[full_key] = str(value)
            else:
                result[full_key] = value
        
        elif isinstance(value, str):
            # Process text values
            result[full_key] = self._process_text_value(value, max_text_length, config)
        
        else:
            # Convert other types to string
            if isinstance(value, (list, dict)):
                # For complex objects that weren't flattened, use JSON
                json_str = json.dumps(value, default=str, ensure_ascii=False)
                result[full_key] = self._truncate_text(json_str, max_text_length)
            else:
                result[full_key] = str(value)
        
        return result
    
    def _process_text_value(self, text: str, max_length: int, config: Dict[str, Any]) -> str:
        """Process text values for CSV output."""
        if not text:
            return config.get('null_value', '')
        
        # Clean the text
        cleaned = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
        cleaned = cleaned.replace('\t', ' ')
        
        # Truncate if necessary
        return self._truncate_text(cleaned, max_length)
    
    def _truncate_text(self, text: str, max_length: int) -> str:
        """Truncate text to maximum length."""
        if len(text) <= max_length:
            return text
        return text[:max_length - 3] + "..."
    
    def _apply_field_processing(self, posts: List[Dict[str, Any]], 
                               config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply field filtering and mapping to processed posts."""
        field_mapping = config.get('field_mapping', {})
        include_fields = config.get('include_fields', [])
        exclude_fields = config.get('exclude_fields', [])
        
        processed = []
        
        for post in posts:
            processed_post = {}
            
            for key, value in post.items():
                # Apply field mapping
                mapped_key = field_mapping.get(key, key)
                
                # Apply field filtering
                if self._should_include_field(key, include_fields, exclude_fields):
                    processed_post[mapped_key] = value
            
            if processed_post:  # Only include non-empty posts
                processed.append(processed_post)
        
        return processed
    
    def _should_include_field(self, field_name: str, include_fields: List[str], 
                             exclude_fields: List[str]) -> bool:
        """Determine if a field should be included in output."""
        # Always include essential fields
        essential_fields = {'id', 'title', 'url', 'subreddit', 'author'}
        
        if field_name in essential_fields:
            return True
        
        # Exclude if in exclude list
        if field_name in exclude_fields:
            return False
        
        # Include if in include list (when specified)
        if include_fields:
            return field_name in include_fields
        
        # Include by default if no filters specified
        return True
    
    def _write_csv_file(self, posts: List[Dict[str, Any]], output_file: Path,
                       config: Dict[str, Any], result: ExportResult) -> None:
        """Write posts to CSV file."""
        if not posts:
            self._write_empty_csv(output_file, config)
            return
        
        # Get CSV writer parameters
        csv_params = self._get_csv_params(config)
        
        # Get all column names from posts
        all_columns = set()
        for post in posts:
            all_columns.update(post.keys())
        
        # Sort columns for consistency
        columns = sorted(list(all_columns))
        
        # Write CSV file
        if config.get('compress', False):
            output_file = output_file.with_suffix(output_file.suffix + '.gz')
            result.metadata['compressed'] = True
            
            with gzip.open(output_file, 'wt', encoding=config.get('encoding', 'utf-8'), newline='') as f:
                self._write_csv_content(f, posts, columns, config, csv_params)
        else:
            result.metadata['compressed'] = False
            
            with open(output_file, 'w', encoding=config.get('encoding', 'utf-8'), newline='') as f:
                self._write_csv_content(f, posts, columns, config, csv_params)
        
        result.metadata['columns'] = columns
        result.metadata['column_count'] = len(columns)
        result.metadata['csv_params'] = csv_params
    
    def _write_csv_content(self, file_obj, posts: List[Dict[str, Any]], 
                          columns: List[str], config: Dict[str, Any], 
                          csv_params: Dict[str, Any]) -> None:
        """Write CSV content to file object."""
        writer = csv.DictWriter(file_obj, fieldnames=columns, **csv_params)
        
        # Write header if requested
        if config.get('include_header', True):
            writer.writeheader()
        
        # Write data rows
        null_value = config.get('null_value', '')
        
        for post in posts:
            # Ensure all columns are present
            row = {}
            for column in columns:
                row[column] = post.get(column, null_value)
            
            writer.writerow(row)
    
    def _write_empty_csv(self, output_file: Path, config: Dict[str, Any]) -> None:
        """Write an empty CSV file with headers only."""
        default_columns = ['id', 'title', 'author', 'subreddit', 'url', 'date_iso']
        
        with open(output_file, 'w', encoding=config.get('encoding', 'utf-8'), newline='') as f:
            if config.get('include_header', True):
                csv_params = self._get_csv_params(config)
                writer = csv.DictWriter(f, fieldnames=default_columns, **csv_params)
                writer.writeheader()
    
    def _get_csv_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get CSV writer parameters from configuration."""
        # Map quoting string to csv constants
        quoting_map = {
            'minimal': csv.QUOTE_MINIMAL,
            'all': csv.QUOTE_ALL,
            'nonnumeric': csv.QUOTE_NONNUMERIC,
            'none': csv.QUOTE_NONE
        }
        
        params = {
            'delimiter': config.get('delimiter', ','),
            'quotechar': config.get('quote_char', '"'),
            'quoting': quoting_map.get(config.get('quoting', 'minimal'), csv.QUOTE_MINIMAL),
            'lineterminator': config.get('line_terminator', '\r\n')
        }
        
        # Add escape character if specified
        escape_char = config.get('escape_char')
        if escape_char:
            params['escapechar'] = escape_char
        
        # Excel compatibility adjustments
        if config.get('excel_compatible', False):
            params['delimiter'] = ','
            params['quotechar'] = '"'
            params['quoting'] = csv.QUOTE_MINIMAL
            params['lineterminator'] = '\r\n'
        
        return params
    
    def _get_size_factor(self) -> float:
        """Get size factor for CSV format."""
        return 0.6  # CSV is typically smaller than JSON
    
    def estimate_output_size(self, data: Dict[str, Any], config: Dict[str, Any]) -> int:
        """Estimate CSV output file size."""
        posts = data.get('posts', [])
        if not posts:
            return 1024  # Minimum size for headers
        
        try:
            # Flatten a sample post to estimate column count and size
            sample_posts = self._flatten_posts(posts[:1], config)
            if not sample_posts:
                return 1024
            
            sample_post = sample_posts[0]
            
            # Estimate row size
            row_size = 0
            for key, value in sample_post.items():
                # Column name + value + delimiter/quotes
                row_size += len(str(key)) + len(str(value)) + 3
            
            # Calculate total size
            header_size = sum(len(key) + 1 for key in sample_post.keys()) + 10
            total_rows = len(posts)
            total_size = header_size + (row_size * total_rows)
            
            # Apply compression factor if enabled
            if config.get('compress', False):
                total_size = int(total_size * 0.4)  # CSV compresses well
            
            return max(total_size, 1024)
            
        except Exception:
            # Fallback estimation
            return max(len(posts) * 500, 1024)  # 500 bytes per post estimate