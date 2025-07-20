"""
Enhanced Export Pipeline Stage

Handles the export of metadata and processing results to various formats using
a pluggable exporter system. Supports multiple output formats with full
configuration control and plugin integration.
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.exporters.base import registry, register_core_exporters, ExportResult
from redditdl.scrapers import PostMetadata


class ExportStage(PipelineStage):
    """
    Enhanced pipeline stage for exporting metadata and results to multiple formats.
    
    This stage uses a pluggable exporter system to support various output formats
    including JSON, CSV, SQLite, and Markdown. Exporters can be configured with
    format-specific options and support both core and plugin-based exporters.
    
    Features:
    - Pluggable exporter architecture
    - Multiple concurrent export formats
    - Format-specific configuration
    - Export validation and error handling
    - Plugin exporter support
    - Incremental export capabilities
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("export", config)
        self._ensure_exporters_registered()
    
    def _ensure_exporters_registered(self) -> None:
        """Ensure core exporters are registered."""
        try:
            # Register core exporters if not already done
            if not registry.list_formats():
                register_core_exporters()
                self.logger.debug("Registered core exporters")
        except Exception as e:
            self.logger.error(f"Failed to register core exporters: {e}")
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """
        Export metadata and results to configured formats using pluggable exporters.
        
        Args:
            context: Pipeline context containing posts and metadata
            
        Returns:
            PipelineResult: Results of the export process
        """
        result = PipelineResult(stage_name=self.name)
        start_time = time.time()
        
        try:
            posts_count = len(context.posts)
            self.logger.info(f"Exporting data for {posts_count} posts")
            
            # Get export configuration
            export_formats = context.get_config("export_formats", self.get_config("export_formats", ["json"]))
            export_dir = context.get_config("export_dir", self.get_config("export_dir", "exports"))
            
            if not export_formats:
                self.logger.info("No export formats configured")
                result.processed_count = posts_count
                result.set_data("exports_created", 0)
                return result
            
            # Ensure export directory exists
            export_path = Path(export_dir)
            export_path.mkdir(parents=True, exist_ok=True)
            
            # Prepare export data
            export_data = self._prepare_export_data(context)
            
            # Validate available exporters
            available_formats = registry.list_formats()
            invalid_formats = [fmt for fmt in export_formats if fmt not in available_formats]
            
            if invalid_formats:
                error_msg = f"Unknown export formats: {invalid_formats}. Available: {available_formats}"
                result.add_error(error_msg)
                self.logger.error(error_msg)
                # Continue with valid formats
                export_formats = [fmt for fmt in export_formats if fmt in available_formats]
            
            exports_created = 0
            export_files = []
            export_results = {}
            
            # Process each export format
            for export_format in export_formats:
                try:
                    export_result = await self._export_format(
                        export_format, export_data, export_path, context
                    )
                    
                    if export_result and export_result.success:
                        export_files.append(export_result.output_path)
                        exports_created += 1
                        export_results[export_format] = export_result
                        self.logger.info(f"Created {export_format} export: {export_result.output_path}")
                    else:
                        error_msg = f"Failed to create {export_format} export"
                        if export_result and export_result.errors:
                            error_msg += f": {'; '.join(export_result.errors)}"
                        result.add_warning(error_msg)
                        
                except Exception as e:
                    self.logger.error(f"Error creating {export_format} export: {e}")
                    result.add_error(f"Export format '{export_format}' failed: {e}")
            
            # Store results
            result.processed_count = posts_count
            result.set_data("exports_created", exports_created)
            result.set_data("export_files", export_files)
            result.set_data("export_formats", export_formats)
            result.set_data("export_directory", str(export_path))
            result.set_data("export_results", export_results)
            
            # Calculate total file size
            total_size = 0
            for file_path in export_files:
                try:
                    total_size += Path(file_path).stat().st_size
                except OSError:
                    pass
            
            result.set_data("total_export_size", total_size)
            
            self.logger.info(
                f"Export completed: {exports_created} files created in {len(export_formats)} formats "
                f"({total_size:,} bytes total)"
            )
            
        except Exception as e:
            self.logger.error(f"Error during export: {e}")
            result.add_error(f"Export failed: {e}")
        
        result.execution_time = time.time() - start_time
        return result
    
    def _prepare_export_data(self, context: PipelineContext) -> Dict[str, Any]:
        """
        Prepare data for export from pipeline context.
        
        Args:
            context: Pipeline context with posts and metadata
            
        Returns:
            Dictionary containing structured export data
        """
        export_data = {
            "export_info": {
                "timestamp": datetime.now().isoformat(),
                "format": "redditdl",
                "version": "2.0",
                "post_count": len(context.posts),
                "exporter": "ExportStage"
            }
        }
        
        # Include posts data
        if context.posts:
            posts_data = []
            for post in context.posts:
                try:
                    if hasattr(post, 'to_dict'):
                        post_data = post.to_dict()
                    else:
                        # Fallback for non-PostMetadata objects
                        post_data = dict(post) if isinstance(post, dict) else str(post)
                    posts_data.append(post_data)
                except Exception as e:
                    self.logger.warning(f"Error serializing post {getattr(post, 'id', 'unknown')}: {e}")
                    # Include basic post info even if full serialization fails
                    posts_data.append({
                        "id": getattr(post, 'id', 'unknown'),
                        "title": getattr(post, 'title', 'Unknown'),
                        "error": f"Serialization failed: {e}"
                    })
            
            export_data["posts"] = posts_data
        
        # Include pipeline metadata
        if context.metadata:
            export_data["pipeline_metadata"] = {
                "session_metadata": context.metadata,
                "stage_results": getattr(context, 'stage_results', {}),
                "config": context.config
            }
        
        return export_data
    
    async def _export_format(self, format_name: str, data: Dict[str, Any], 
                           export_path: Path, context: PipelineContext) -> Optional[ExportResult]:
        """
        Export data using a specific format exporter.
        
        Args:
            format_name: Name of the export format
            data: Data to export
            export_path: Directory for export files
            context: Pipeline context for configuration
            
        Returns:
            ExportResult or None if export failed
        """
        try:
            # Get exporter instance
            exporter = registry.get_exporter(format_name)
            if not exporter:
                self.logger.error(f"No exporter available for format: {format_name}")
                return None
            
            # Get format-specific configuration
            format_config = self._get_format_config(format_name, context)
            
            # Validate configuration
            config_errors = exporter.validate_config(format_config)
            if config_errors:
                self.logger.warning(f"Configuration errors for {format_name}: {config_errors}")
                # Continue with defaults for invalid options
            
            # Generate output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            format_info = exporter.get_format_info()
            output_filename = f"redditdl_export_{timestamp}{format_info.extension}"
            output_path = export_path / output_filename
            
            # Perform export
            self.logger.debug(f"Starting {format_name} export to {output_path}")
            export_result = exporter.export(data, str(output_path), format_config)
            
            # Log warnings from export
            if export_result.warnings:
                for warning in export_result.warnings:
                    self.logger.warning(f"{format_name} export warning: {warning}")
            
            return export_result
            
        except Exception as e:
            self.logger.error(f"Failed to export {format_name}: {e}")
            return None
    
    def _get_format_config(self, format_name: str, context: PipelineContext) -> Dict[str, Any]:
        """
        Get configuration for a specific export format.
        
        Args:
            format_name: Export format name
            context: Pipeline context
            
        Returns:
            Configuration dictionary for the format
        """
        # Start with global export configuration
        base_config = {
            "include_metadata": context.get_config("export_include_metadata", True),
            "include_posts": context.get_config("export_include_posts", True),
            "overwrite": context.get_config("export_overwrite", True)
        }
        
        # Add format-specific configuration
        format_config_key = f"export_{format_name}_config"
        format_specific = context.get_config(format_config_key, {})
        
        if format_specific:
            base_config.update(format_specific)
        
        # Add stage-level configuration
        stage_config = self.get_config(format_name, {})
        if stage_config:
            base_config.update(stage_config)
        
        return base_config
    
    def validate_config(self) -> List[str]:
        """
        Validate the export stage configuration.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate export formats
        export_formats = self.get_config("export_formats", [])
        if export_formats and not isinstance(export_formats, list):
            errors.append("export_formats must be a list")
        else:
            available_formats = registry.list_formats()
            for fmt in export_formats:
                if fmt not in available_formats:
                    errors.append(f"Unsupported export format: {fmt}")
        
        # Validate export directory
        export_dir = self.get_config("export_dir")
        if export_dir:
            try:
                export_path = Path(export_dir)
                # Check if parent directory exists (don't create it here)
                if not export_path.parent.exists():
                    errors.append(f"Parent directory does not exist: {export_path.parent}")
            except Exception as e:
                errors.append(f"Invalid export directory path: {e}")
        
        # Validate format-specific configurations
        export_formats = self.get_config("export_formats", [])
        for format_name in export_formats:
            exporter = registry.get_exporter(format_name)
            if exporter:
                format_config = self.get_config(format_name, {})
                format_errors = exporter.validate_config(format_config)
                for error in format_errors:
                    errors.append(f"{format_name} config: {error}")
        
        return errors
    
    async def pre_process(self, context: PipelineContext) -> None:
        """Pre-processing setup for export stage."""
        self.logger.debug("Export stage pre-processing")
        
        # Ensure exporters are available
        self._ensure_exporters_registered()
        
        # Log configuration for debugging
        export_formats = context.get_config("export_formats", self.get_config("export_formats", ["json"]))
        export_dir = context.get_config("export_dir", self.get_config("export_dir", "exports"))
        
        self.logger.debug(
            f"Export configuration - Formats: {export_formats}, Directory: {export_dir}"
        )
        
        # Validate formats are available
        available_formats = registry.list_formats()
        invalid_formats = [fmt for fmt in export_formats if fmt not in available_formats]
        
        if invalid_formats:
            self.logger.warning(
                f"Invalid export formats configured: {invalid_formats}. "
                f"Available formats: {available_formats}"
            )
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """Post-processing cleanup for export stage."""
        self.logger.debug("Export stage post-processing")
        
        # Store export metadata in context
        if result.success:
            context.set_metadata("export_completed", True)
            context.set_metadata("exports_created", result.get_data("exports_created", 0))
            context.set_metadata("export_files", result.get_data("export_files", []))
            context.set_metadata("export_directory", result.get_data("export_directory"))
            context.set_metadata("total_export_size", result.get_data("total_export_size", 0))
        else:
            context.set_metadata("export_completed", False)
        
        # Log export results
        exports_created = result.get_data("exports_created", 0)
        export_files = result.get_data("export_files", [])
        total_size = result.get_data("total_export_size", 0)
        
        if exports_created > 0:
            self.logger.info(f"Export completed: {exports_created} files created ({total_size:,} bytes)")
            for export_file in export_files:
                self.logger.info(f"  - {export_file}")
        else:
            self.logger.info("No exports created")
        
        # Emit export events if event system is available
        if context.events:
            try:
                # Import here to avoid circular imports
                from redditdl.core.events.types import PostProcessedEvent
                
                for export_file in export_files:
                    event = PostProcessedEvent(
                        session_id=context.session_id or "",
                        post_id="export_stage",
                        processing_type="export",
                        output_path=export_file,
                        success=True
                    )
                    await context.emit_event_async(event)
                    
            except ImportError:
                self.logger.debug("Event system not available for export events")
            except Exception as e:
                self.logger.warning(f"Failed to emit export events: {e}")
    
    def get_available_formats(self) -> List[str]:
        """Get list of available export formats."""
        return registry.list_formats()
    
    def get_format_info(self) -> Dict[str, Any]:
        """Get information about all available export formats."""
        return {
            format_name: {
                "name": info.name,
                "extension": info.extension, 
                "description": info.description,
                "mime_type": info.mime_type,
                "supports_compression": info.supports_compression,
                "supports_streaming": info.supports_streaming,
                "supports_incremental": info.supports_incremental
            }
            for format_name, info in registry.list_format_info().items()
        }
    
    def estimate_export_sizes(self, data: Dict[str, Any], 
                             formats: List[str]) -> Dict[str, int]:
        """
        Estimate export file sizes for given formats.
        
        Args:
            data: Data to be exported
            formats: List of export format names
            
        Returns:
            Dictionary mapping format names to estimated sizes in bytes
        """
        estimates = {}
        
        for format_name in formats:
            exporter = registry.get_exporter(format_name)
            if exporter:
                try:
                    config = self.get_config(format_name, {})
                    estimates[format_name] = exporter.estimate_output_size(data, config)
                except Exception as e:
                    self.logger.warning(f"Failed to estimate size for {format_name}: {e}")
                    estimates[format_name] = 0
            else:
                estimates[format_name] = 0
        
        return estimates