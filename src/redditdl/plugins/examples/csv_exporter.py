"""
CSV Exporter Plugin

A simple example exporter plugin that exports post data to CSV format.
Useful for data analysis and importing into spreadsheet applications.
"""

import csv
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List
from core.plugins.hooks import BaseExporter

__plugin_info__ = {
    'name': 'csv_exporter',
    'version': '1.0.0',
    'description': 'Exports post data to CSV format for analysis',
    'author': 'RedditDL Team',
    'export_format': 'csv',
    'file_extension': '.csv'
}


class CsvExporter(BaseExporter):
    """
    CSV exporter for Reddit post data.
    
    This exporter creates CSV files that can be easily imported into
    spreadsheet applications or data analysis tools.
    """
    
    def __init__(self):
        self.format_info = {
            'name': 'csv',
            'extension': '.csv',
            'description': 'Comma-separated values format',
            'mime_type': 'text/csv',
            'supports_compression': True,
            'supports_streaming': True
        }
        
        self.config_schema = {
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
            'max_text_length': {
                'type': 'integer',
                'default': 1000,
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
                'choices': ['utf-8', 'utf-16', 'latin-1'],
                'description': 'Text encoding for output file'
            }
        }
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> bool:
        """Export data to CSV format."""
        try:
            # Extract configuration
            delimiter = config.get('delimiter', ',')
            quote_char = config.get('quote_char', '"')
            include_header = config.get('include_header', True)
            flatten_nested = config.get('flatten_nested', True)
            max_text_length = config.get('max_text_length', 1000)
            date_format = config.get('date_format', '%Y-%m-%d %H:%M:%S')
            encoding = config.get('encoding', 'utf-8')
            
            # Get posts data
            posts = data.get('posts', [])
            if not posts:
                print("No posts to export")
                return True
            
            # Prepare data for CSV export
            csv_data = self._prepare_csv_data(posts, flatten_nested, max_text_length, date_format)
            
            # Ensure output directory exists
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write CSV file
            with open(output_file, 'w', newline='', encoding=encoding) as csvfile:
                if not csv_data:
                    return True
                
                # Get fieldnames from first row
                fieldnames = list(csv_data[0].keys())
                
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=fieldnames,
                    delimiter=delimiter,
                    quotechar=quote_char,
                    quoting=csv.QUOTE_MINIMAL
                )
                
                # Write header if requested
                if include_header:
                    writer.writeheader()
                
                # Write data rows
                for row in csv_data:
                    writer.writerow(row)
            
            print(f"Exported {len(csv_data)} posts to {output_file}")
            return True
            
        except Exception as e:
            print(f"CSV export failed: {e}")
            return False
    
    def _prepare_csv_data(self, posts: List[Dict[str, Any]], flatten_nested: bool,
                         max_text_length: int, date_format: str) -> List[Dict[str, Any]]:
        """Prepare post data for CSV export."""
        csv_rows = []
        
        for post in posts:
            row = {}
            
            for key, value in post.items():
                processed_value = self._process_field_value(
                    value, flatten_nested, max_text_length, date_format
                )
                
                if flatten_nested and isinstance(processed_value, dict):
                    # Flatten nested dictionaries
                    for nested_key, nested_value in processed_value.items():
                        flattened_key = f"{key}.{nested_key}"
                        row[flattened_key] = self._process_field_value(
                            nested_value, False, max_text_length, date_format
                        )
                else:
                    row[key] = processed_value
            
            csv_rows.append(row)
        
        return csv_rows
    
    def _process_field_value(self, value: Any, flatten_nested: bool,
                           max_text_length: int, date_format: str) -> str:
        """Process a field value for CSV output."""
        if value is None:
            return ""
        
        # Handle timestamps
        if isinstance(value, (int, float)) and str(value).replace('.', '').isdigit():
            # Check if this looks like a Unix timestamp
            if 1000000000 <= value <= 9999999999:  # Reasonable timestamp range
                try:
                    dt = datetime.fromtimestamp(value)
                    return dt.strftime(date_format)
                except (ValueError, OSError):
                    pass
        
        # Handle lists and complex objects
        if isinstance(value, (list, dict)):
            if flatten_nested and isinstance(value, dict) and len(value) <= 5:
                # Return as flattened dict for small dictionaries
                return value
            else:
                # Convert to JSON string for complex objects
                json_str = json.dumps(value, default=str, ensure_ascii=False)
                if len(json_str) > max_text_length:
                    json_str = json_str[:max_text_length] + "..."
                return json_str
        
        # Handle strings
        if isinstance(value, str):
            if len(value) > max_text_length:
                value = value[:max_text_length] + "..."
            # Escape newlines and tabs for CSV
            value = value.replace('\n', '\\n').replace('\t', '\\t').replace('\r', '\\r')
            return value
        
        # Handle booleans
        if isinstance(value, bool):
            return "true" if value else "false"
        
        # Convert everything else to string
        return str(value)
    
    def get_format_info(self) -> Dict[str, str]:
        """Get format information."""
        return self.format_info.copy()
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate exporter configuration."""
        errors = []
        
        # Validate delimiter
        delimiter = config.get('delimiter', ',')
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            errors.append("delimiter must be a single character")
        
        # Validate quote_char
        quote_char = config.get('quote_char', '"')
        if not isinstance(quote_char, str) or len(quote_char) != 1:
            errors.append("quote_char must be a single character")
        
        # Validate max_text_length
        max_text_length = config.get('max_text_length', 1000)
        if not isinstance(max_text_length, int) or max_text_length <= 0:
            errors.append("max_text_length must be a positive integer")
        
        # Validate encoding
        encoding = config.get('encoding', 'utf-8')
        valid_encodings = ['utf-8', 'utf-16', 'latin-1']
        if encoding not in valid_encodings:
            errors.append(f"encoding must be one of: {valid_encodings}")
        
        # Validate boolean fields
        for field in ['include_header', 'flatten_nested']:
            value = config.get(field)
            if value is not None and not isinstance(value, bool):
                errors.append(f"{field} must be a boolean")
        
        # Validate date_format
        date_format = config.get('date_format', '%Y-%m-%d %H:%M:%S')
        if not isinstance(date_format, str):
            errors.append("date_format must be a string")
        else:
            try:
                # Test the date format
                datetime.now().strftime(date_format)
            except (ValueError, TypeError):
                errors.append("invalid date_format string")
        
        return errors
    
    def estimate_output_size(self, data: Dict[str, Any], config: Dict[str, Any]) -> int:
        """Estimate the output file size."""
        posts = data.get('posts', [])
        if not posts:
            return 0
        
        # Estimate based on a sample post
        sample_post = posts[0]
        max_text_length = config.get('max_text_length', 1000)
        
        # Rough estimation: number of fields * average field length
        field_count = len(sample_post)
        avg_field_length = min(100, max_text_length // 10)  # Conservative estimate
        
        row_size = field_count * avg_field_length
        total_rows = len(posts)
        
        # Add header size if enabled
        header_size = field_count * 20 if config.get('include_header', True) else 0
        
        return header_size + (total_rows * row_size)
    
    def get_column_info(self, data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Get information about the columns that will be generated."""
        posts = data.get('posts', [])
        if not posts:
            return {'columns': [], 'total_columns': 0}
        
        # Analyze first few posts to determine columns
        sample_size = min(5, len(posts))
        all_columns = set()
        
        flatten_nested = config.get('flatten_nested', True)
        
        for post in posts[:sample_size]:
            for key, value in post.items():
                if flatten_nested and isinstance(value, dict):
                    for nested_key in value.keys():
                        all_columns.add(f"{key}.{nested_key}")
                else:
                    all_columns.add(key)
        
        column_list = sorted(list(all_columns))
        
        return {
            'columns': column_list,
            'total_columns': len(column_list),
            'sample_size': sample_size,
            'flattened': flatten_nested
        }


def initialize_plugin():
    """Initialize the CSV exporter plugin."""
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")


def cleanup_plugin():
    """Clean up the CSV exporter plugin."""
    print(f"Cleaning up {__plugin_info__['name']}")