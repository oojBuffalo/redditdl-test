"""
Base Exporter Classes

Abstract base classes and utilities for the pluggable export system.
Defines the interface that all exporters must implement and provides
registration and discovery mechanisms.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type, Union
from dataclasses import dataclass, field
from datetime import datetime
import json


@dataclass
class ExportResult:
    """Result of an export operation."""
    success: bool = True
    output_path: Optional[str] = None
    file_size: int = 0
    records_exported: int = 0
    format_name: str = ""
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, error: str) -> None:
        """Add an error to the result."""
        self.errors.append(error)
        self.success = False
    
    def add_warning(self, warning: str) -> None:
        """Add a warning to the result."""
        self.warnings.append(warning)


@dataclass 
class FormatInfo:
    """Information about an export format."""
    name: str
    extension: str
    description: str
    mime_type: str = ""
    supports_compression: bool = False
    supports_streaming: bool = False
    supports_incremental: bool = False
    max_records: Optional[int] = None
    schema_required: bool = False


class BaseExporter(ABC):
    """
    Abstract base class for all exporters.
    
    Exporters are responsible for converting processed Reddit post data
    into specific output formats. Each exporter must implement the core
    export method and provide format information.
    
    Exporters should be stateless and thread-safe, with all configuration
    passed through the export method parameters.
    """
    
    def __init__(self):
        """Initialize the exporter."""
        self.logger = logging.getLogger(f"exporters.{self.__class__.__name__.lower()}")
        self._format_info = self._create_format_info()
        self._config_schema = self._create_config_schema()
    
    @abstractmethod
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> ExportResult:
        """
        Export data to the specified format and path.
        
        Args:
            data: Data to export, containing 'posts' list and metadata
            output_path: Path where the export file should be created
            config: Export configuration options
            
        Returns:
            ExportResult: Details about the export operation
        """
        pass
    
    @abstractmethod
    def get_format_info(self) -> FormatInfo:
        """
        Get information about this export format.
        
        Returns:
            FormatInfo: Format metadata and capabilities
        """
        pass
    
    @abstractmethod
    def _create_format_info(self) -> FormatInfo:
        """Create the format info for this exporter."""
        pass
    
    @abstractmethod
    def _create_config_schema(self) -> Dict[str, Any]:
        """Create the configuration schema for this exporter."""
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate export configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Basic type checking against schema
        for key, schema_def in self._config_schema.items():
            if key in config:
                value = config[key]
                expected_type = schema_def.get('type')
                
                if expected_type == 'string' and not isinstance(value, str):
                    errors.append(f"{key} must be a string")
                elif expected_type == 'integer' and not isinstance(value, int):
                    errors.append(f"{key} must be an integer")
                elif expected_type == 'boolean' and not isinstance(value, bool):
                    errors.append(f"{key} must be a boolean")
                elif expected_type == 'array' and not isinstance(value, list):
                    errors.append(f"{key} must be a list")
                
                # Check choices if specified
                choices = schema_def.get('choices')
                if choices and value not in choices:
                    errors.append(f"{key} must be one of: {choices}")
                
                # Check range if specified
                minimum = schema_def.get('minimum')
                maximum = schema_def.get('maximum')
                if minimum is not None and isinstance(value, (int, float)) and value < minimum:
                    errors.append(f"{key} must be >= {minimum}")
                if maximum is not None and isinstance(value, (int, float)) and value > maximum:
                    errors.append(f"{key} must be <= {maximum}")
        
        return errors
    
    def estimate_output_size(self, data: Dict[str, Any], config: Dict[str, Any]) -> int:
        """
        Estimate the size of the output file in bytes.
        
        Args:
            data: Data to be exported
            config: Export configuration
            
        Returns:
            Estimated file size in bytes
        """
        # Default implementation: rough estimate based on input data size
        posts = data.get('posts', [])
        if not posts:
            return 0
        
        # Rough calculation: JSON size of data * format overhead factor
        try:
            json_size = len(json.dumps(data, default=str))
            return int(json_size * self._get_size_factor())
        except Exception:
            # Fallback: number of posts * average post size estimate
            return len(posts) * 2048  # 2KB per post estimate
    
    def _get_size_factor(self) -> float:
        """Get the size factor for this format relative to JSON."""
        # Override in subclasses for format-specific estimates
        return 1.0
    
    def prepare_output_path(self, output_path: str, config: Dict[str, Any]) -> Path:
        """
        Prepare and validate the output path.
        
        Args:
            output_path: Requested output path
            config: Export configuration
            
        Returns:
            Path: Validated and prepared output path
            
        Raises:
            ValueError: If output path is invalid
            OSError: If directory cannot be created
        """
        path = Path(output_path)
        
        # Ensure path has correct extension
        format_info = self.get_format_info()
        if not path.suffix:
            path = path.with_suffix(format_info.extension)
        elif path.suffix != format_info.extension:
            self.logger.warning(f"Path extension {path.suffix} doesn't match format {format_info.extension}")
        
        # Create parent directory if needed
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check for overwrites
        if path.exists() and not config.get('overwrite', True):
            # Generate unique filename
            counter = 1
            base_path = path.with_suffix('')
            while path.exists():
                path = base_path.with_name(f"{base_path.name}_{counter}").with_suffix(format_info.extension)
                counter += 1
        
        return path
    
    def get_config_schema(self) -> Dict[str, Any]:
        """Get the configuration schema for this exporter."""
        return self._config_schema.copy()
    
    def supports_incremental(self) -> bool:
        """Check if this exporter supports incremental exports."""
        return self.get_format_info().supports_incremental
    
    def supports_streaming(self) -> bool:
        """Check if this exporter supports streaming exports."""
        return self.get_format_info().supports_streaming
    
    def validate_data(self, data: Dict[str, Any]) -> List[str]:
        """
        Validate input data before export.
        
        Args:
            data: Data to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        if not isinstance(data, dict):
            errors.append("Data must be a dictionary")
            return errors
        
        # Check for required fields
        if 'export_info' not in data:
            errors.append("Data missing export_info")
        
        posts = data.get('posts', [])
        if not isinstance(posts, list):
            errors.append("Posts must be a list")
        elif not posts:
            self.logger.warning("No posts to export")
        
        # Validate post structure
        if posts:
            for i, post in enumerate(posts[:5]):  # Check first 5 posts
                if not isinstance(post, dict):
                    errors.append(f"Post {i} is not a dictionary")
                elif 'id' not in post:
                    errors.append(f"Post {i} missing required 'id' field")
        
        return errors


class ExporterRegistry:
    """
    Registry for managing available exporters.
    
    Provides functionality to register, discover, and instantiate exporters
    both from the core system and from plugins.
    """
    
    def __init__(self):
        """Initialize the exporter registry."""
        self.logger = logging.getLogger("exporters.registry")
        self._exporters: Dict[str, Type[BaseExporter]] = {}
        self._instances: Dict[str, BaseExporter] = {}
        self._format_aliases: Dict[str, str] = {}
    
    def register_exporter(self, exporter_class: Type[BaseExporter], 
                         format_name: Optional[str] = None,
                         aliases: Optional[List[str]] = None) -> None:
        """
        Register an exporter class.
        
        Args:
            exporter_class: Exporter class to register
            format_name: Optional custom format name (uses class format_info.name if not provided)
            aliases: Optional list of format name aliases
        """
        try:
            # Get format name from class if not provided
            if format_name is None:
                instance = exporter_class()
                format_info = instance.get_format_info()
                format_name = format_info.name
            
            # Register the exporter
            self._exporters[format_name] = exporter_class
            self.logger.debug(f"Registered exporter: {format_name} -> {exporter_class.__name__}")
            
            # Register aliases
            if aliases:
                for alias in aliases:
                    self._format_aliases[alias] = format_name
                    self.logger.debug(f"Registered alias: {alias} -> {format_name}")
                    
        except Exception as e:
            self.logger.error(f"Failed to register exporter {exporter_class.__name__}: {e}")
    
    def get_exporter(self, format_name: str) -> Optional[BaseExporter]:
        """
        Get an exporter instance for the specified format.
        
        Args:
            format_name: Name of the export format
            
        Returns:
            Exporter instance or None if not found
        """
        # Resolve alias if needed
        actual_format = self._format_aliases.get(format_name, format_name)
        
        # Return cached instance if available
        if actual_format in self._instances:
            return self._instances[actual_format]
        
        # Create new instance
        exporter_class = self._exporters.get(actual_format)
        if exporter_class:
            try:
                instance = exporter_class()
                self._instances[actual_format] = instance
                return instance
            except Exception as e:
                self.logger.error(f"Failed to instantiate exporter {actual_format}: {e}")
        
        return None
    
    def list_formats(self) -> List[str]:
        """Get list of available export formats."""
        return list(self._exporters.keys())
    
    def list_format_info(self) -> Dict[str, FormatInfo]:
        """Get format information for all registered exporters."""
        info = {}
        for format_name in self._exporters.keys():
            exporter = self.get_exporter(format_name)
            if exporter:
                info[format_name] = exporter.get_format_info()
        return info
    
    def is_format_supported(self, format_name: str) -> bool:
        """Check if a format is supported."""
        actual_format = self._format_aliases.get(format_name, format_name)
        return actual_format in self._exporters
    
    def validate_format_config(self, format_name: str, config: Dict[str, Any]) -> List[str]:
        """
        Validate configuration for a specific format.
        
        Args:
            format_name: Export format name
            config: Configuration to validate
            
        Returns:
            List of validation errors
        """
        exporter = self.get_exporter(format_name)
        if not exporter:
            return [f"Unknown export format: {format_name}"]
        
        return exporter.validate_config(config)
    
    def get_format_schema(self, format_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration schema for a format."""
        exporter = self.get_exporter(format_name)
        if exporter:
            return exporter.get_config_schema()
        return None
    
    def clear(self) -> None:
        """Clear all registered exporters."""
        self._exporters.clear()
        self._instances.clear()
        self._format_aliases.clear()
        self.logger.debug("Cleared exporter registry")


# Global registry instance
registry = ExporterRegistry()


def register_core_exporters() -> None:
    """Register all core exporters with the global registry."""
    from .json import JsonExporter
    from .csv import CsvExporter
    from .sqlite import SqliteExporter
    from .markdown import MarkdownExporter
    
    registry.register_exporter(JsonExporter, aliases=['json'])
    registry.register_exporter(CsvExporter, aliases=['csv'])
    registry.register_exporter(SqliteExporter, aliases=['sqlite', 'db'])
    registry.register_exporter(MarkdownExporter, aliases=['markdown', 'md'])


def get_exporter(format_name: str) -> Optional[BaseExporter]:
    """Get an exporter instance from the global registry."""
    return registry.get_exporter(format_name)


def list_exporters() -> List[str]:
    """List all available export formats."""
    return registry.list_formats()