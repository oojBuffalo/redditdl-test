"""
Configuration Manager

Handles hierarchical configuration loading, validation, and management
with support for CLI args → environment variables → config files → defaults.
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from pydantic import ValidationError

from redditdl.core.config.models import AppConfig, ScrapingConfig, ProcessingConfig, OutputConfig, FilterConfig


class ConfigManager:
    """
    Manages application configuration with hierarchical loading and validation.
    
    Configuration sources in order of precedence:
    1. CLI arguments (highest priority)
    2. Environment variables  
    3. Configuration files
    4. Default values (lowest priority)
    """
    
    def __init__(self, config_file: Optional[Union[str, Path]] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_file: Optional path to configuration file
        """
        self.config_file = Path(config_file) if config_file else None
        self._config: Optional[AppConfig] = None
        self._config_paths = self._get_default_config_paths()
    
    def _get_default_config_paths(self) -> List[Path]:
        """Get default configuration file search paths."""
        search_paths = [
            Path.cwd() / "redditdl.yaml",
            Path.cwd() / "redditdl.yml", 
            Path.cwd() / ".redditdl.yaml",
            Path.cwd() / ".redditdl.yml",
            Path.home() / ".redditdl" / "config.yaml",
            Path.home() / ".config" / "redditdl" / "config.yaml",
        ]
        
        # Add XDG config directory if available
        xdg_config = os.environ.get('XDG_CONFIG_HOME')
        if xdg_config:
            search_paths.append(Path(xdg_config) / "redditdl" / "config.yaml")
        
        return search_paths
    
    def load_config(
        self,
        cli_args: Optional[Dict[str, Any]] = None,
        env_prefix: str = "REDDITDL_"
    ) -> AppConfig:
        """
        Load and validate configuration from all sources.
        
        Args:
            cli_args: Dictionary of CLI arguments
            env_prefix: Prefix for environment variables
            
        Returns:
            Validated AppConfig instance
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Start with defaults
        config_data = {}
        
        # Load from configuration file
        file_config = self._load_config_file()
        if file_config:
            config_data.update(file_config)
        
        # Override with environment variables
        env_config = self._load_env_config(env_prefix)
        if env_config:
            config_data = self._deep_merge(config_data, env_config)
        
        # Override with CLI arguments
        if cli_args:
            cli_config = self._normalize_cli_args(cli_args)
            config_data = self._deep_merge(config_data, cli_config)
        
        # Validate and create configuration
        try:
            self._config = AppConfig(**config_data)
            return self._config
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}")
    
    def _load_config_file(self) -> Optional[Dict[str, Any]]:
        """Load configuration from file."""
        config_file = self.config_file
        
        # If no specific file provided, search default locations
        if not config_file:
            for path in self._config_paths:
                if path.exists() and path.is_file():
                    config_file = path
                    break
        
        if not config_file or not config_file.exists():
            return None
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                if config_file.suffix.lower() in {'.yaml', '.yml'}:
                    return yaml.safe_load(f) or {}
                elif config_file.suffix.lower() == '.json':
                    return json.load(f)
                else:
                    # Try YAML first, then JSON
                    content = f.read()
                    try:
                        return yaml.safe_load(content) or {}
                    except yaml.YAMLError:
                        return json.loads(content)
        except (IOError, yaml.YAMLError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"Failed to load config file {config_file}: {e}")
    
    def _load_env_config(self, prefix: str) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        env_config = {}
        
        # Define environment variable mappings
        env_mappings = {
            # Scraping configuration
            f"{prefix}API_MODE": ("scraping", "api_mode", self._parse_bool),
            f"{prefix}CLIENT_ID": ("scraping", "client_id", str),
            f"{prefix}CLIENT_SECRET": ("scraping", "client_secret", str),
            f"{prefix}USER_AGENT": ("scraping", "user_agent", str),
            f"{prefix}USERNAME": ("scraping", "username", str),
            f"{prefix}PASSWORD": ("scraping", "password", str),
            f"{prefix}SLEEP_INTERVAL": ("scraping", "sleep_interval", float),
            f"{prefix}POST_LIMIT": ("scraping", "post_limit", int),
            f"{prefix}TIMEOUT": ("scraping", "timeout", int),
            f"{prefix}MAX_RETRIES": ("scraping", "max_retries", int),
            
            # Output configuration
            f"{prefix}OUTPUT_DIR": ("output", "output_dir", str),
            f"{prefix}FILENAME_TEMPLATE": ("output", "filename_template", str),
            f"{prefix}ORGANIZE_BY_SUBREDDIT": ("output", "organize_by_subreddit", self._parse_bool),
            f"{prefix}ORGANIZE_BY_DATE": ("output", "organize_by_date", self._parse_bool),
            f"{prefix}EXPORT_FORMATS": ("output", "export_formats", self._parse_list),
            
            # Processing configuration
            f"{prefix}EMBED_METADATA": ("processing", "embed_metadata", self._parse_bool),
            f"{prefix}CREATE_JSON_SIDECARS": ("processing", "create_json_sidecars", self._parse_bool),
            f"{prefix}CONCURRENT_DOWNLOADS": ("processing", "concurrent_downloads", int),
            f"{prefix}IMAGE_QUALITY": ("processing", "image_quality", int),
            
            # Filter configuration
            f"{prefix}MIN_SCORE": ("filters", "min_score", int),
            f"{prefix}MAX_SCORE": ("filters", "max_score", int),
            f"{prefix}INCLUDE_NSFW": ("filters", "include_nsfw", self._parse_bool),
            f"{prefix}INCLUDE_KEYWORDS": ("filters", "include_keywords", self._parse_list),
            f"{prefix}EXCLUDE_KEYWORDS": ("filters", "exclude_keywords", self._parse_list),
            
            # General settings
            f"{prefix}DRY_RUN": ("dry_run", None, self._parse_bool),
            f"{prefix}VERBOSE": ("verbose", None, self._parse_bool),
            f"{prefix}DEBUG": ("debug", None, self._parse_bool),
            f"{prefix}USE_PIPELINE": ("use_pipeline", None, self._parse_bool),
        }
        
        for env_var, (section, key, parser) in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                try:
                    parsed_value = parser(value)
                    if key is None:
                        # Top-level setting
                        env_config[section] = parsed_value
                    else:
                        # Nested setting
                        if section not in env_config:
                            env_config[section] = {}
                        env_config[section][key] = parsed_value
                except (ValueError, TypeError) as e:
                    raise ConfigurationError(f"Invalid value for {env_var}: {value} ({e})")
        
        return env_config
    
    def _normalize_cli_args(self, cli_args: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CLI arguments to configuration structure."""
        normalized = {}
        
        # Map CLI arguments to configuration sections
        cli_mappings = {
            # Direct mappings to top level
            'dry_run': 'dry_run',
            'verbose': 'verbose', 
            'debug': 'debug',
            'use_pipeline': 'use_pipeline',
            
            # Scraping section
            'api': ('scraping', 'api_mode'),
            'client_id': ('scraping', 'client_id'),
            'client_secret': ('scraping', 'client_secret'),
            'user_agent': ('scraping', 'user_agent'),
            'username': ('scraping', 'username'),
            'password': ('scraping', 'password'),
            'sleep': ('scraping', 'sleep_interval'),
            'limit': ('scraping', 'post_limit'),
            'timeout': ('scraping', 'timeout'),
            'retries': ('scraping', 'max_retries'),
            
            # Output section
            'outdir': ('output', 'output_dir'),
            'output': ('output', 'output_dir'),
            'export_formats': ('output', 'export_formats'),
            'organize_by_subreddit': ('output', 'organize_by_subreddit'),
            'organize_by_date': ('output', 'organize_by_date'),
            'organize_by_author': ('output', 'organize_by_author'),
            'filename_template': ('output', 'filename_template'),
            
            # Processing section
            'embed_metadata': ('processing', 'embed_metadata'),
            'json_sidecars': ('processing', 'create_json_sidecars'),
            'concurrent': ('processing', 'concurrent_downloads'),
            'quality': ('processing', 'image_quality'),
            
            # Filter section
            'min_score': ('filters', 'min_score'),
            'max_score': ('filters', 'max_score'),
            'include_nsfw': ('filters', 'include_nsfw'),
            'nsfw_only': ('filters', 'nsfw_only'),
            'after_date': ('filters', 'after_date'),
            'before_date': ('filters', 'before_date'),
            'include_keywords': ('filters', 'include_keywords'),
            'exclude_keywords': ('filters', 'exclude_keywords'),
        }
        
        for cli_key, value in cli_args.items():
            if value is None:
                continue
                
            mapping = cli_mappings.get(cli_key)
            if mapping:
                if isinstance(mapping, tuple):
                    section, key = mapping
                    if section not in normalized:
                        normalized[section] = {}
                    normalized[section][key] = value
                else:
                    normalized[mapping] = value
        
        return normalized
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries, with override taking precedence."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    @staticmethod
    def _parse_bool(value: Union[str, bool]) -> bool:
        """Parse boolean value from string."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in {'true', '1', 'yes', 'on', 'enabled'}
        return bool(value)
    
    @staticmethod
    def _parse_list(value: Union[str, List[str]]) -> List[str]:
        """Parse list value from string."""
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            # Split by comma and strip whitespace
            return [item.strip() for item in value.split(',') if item.strip()]
        return []
    
    def validate_config(self, config: Optional[AppConfig] = None) -> List[str]:
        """
        Validate configuration and return list of warnings/issues.
        
        Args:
            config: Configuration to validate (uses loaded config if None)
            
        Returns:
            List of validation warnings/issues
        """
        if config is None:
            config = self._config
        
        if config is None:
            return ["No configuration loaded"]
        
        warnings = []
        
        # Validate API credentials
        if config.scraping.api_mode:
            if not config.scraping.client_id:
                warnings.append("API mode enabled but client_id not provided")
            if not config.scraping.client_secret:
                warnings.append("API mode enabled but client_secret not provided")
        
        # Validate output directory
        try:
            config.output.output_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError):
            warnings.append(f"Cannot create output directory: {config.output.output_dir}")
        
        # Validate export directory
        try:
            export_dir = config.get_export_dir()
            export_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError):
            warnings.append(f"Cannot create export directory: {export_dir}")
        
        # Validate plugin directories
        for plugin_dir in config.plugin_dirs:
            if not plugin_dir.exists():
                warnings.append(f"Plugin directory does not exist: {plugin_dir}")
        
        # Check for conflicting settings
        if config.filters.nsfw_only and not config.filters.include_nsfw:
            warnings.append("nsfw_only is True but include_nsfw is False")
        
        if config.processing.video_format_conversion:
            warnings.append("Video conversion requires FFmpeg to be installed")
        
        return warnings
    
    def generate_schema(self, output_file: Optional[Path] = None) -> Dict[str, Any]:
        """
        Generate JSON schema for configuration.
        
        Args:
            output_file: Optional file to write schema to
            
        Returns:
            JSON schema dictionary
        """
        schema = AppConfig.model_json_schema()
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(schema, f, indent=2)
        
        return schema
    
    def create_example_config(self, output_file: Path, profile: str = "default") -> None:
        """
        Create example configuration file.
        
        Args:
            output_file: Path to write configuration file
            profile: Configuration profile (default, images-only, full-archive, research)
        """
        if profile == "images-only":
            config = AppConfig(
                scraping=ScrapingConfig(post_limit=50),
                filters=FilterConfig(
                    include_images=True,
                    include_videos=False,
                    include_gifs=True,
                    include_galleries=True,
                    include_external_links=False
                ),
                processing=ProcessingConfig(
                    image_format_conversion=True,
                    target_image_format="jpeg",
                    image_quality=90,
                    generate_thumbnails=True
                ),
                output=OutputConfig(
                    organize_by_subreddit=True,
                    organize_by_content_type=True,
                    export_formats=["json", "csv"]
                )
            )
        elif profile == "full-archive":
            config = AppConfig(
                scraping=ScrapingConfig(
                    post_limit=1000,
                    sleep_interval=2.0
                ),
                processing=ProcessingConfig(
                    embed_metadata=True,
                    create_json_sidecars=True,
                    preserve_original_metadata=True
                ),
                output=OutputConfig(
                    organize_by_date=True,
                    organize_by_subreddit=True,
                    export_formats=["json", "sqlite", "csv"]
                ),
                filters=FilterConfig(
                    include_nsfw=True,
                    min_score=1
                )
            )
        elif profile == "research":
            config = AppConfig(
                scraping=ScrapingConfig(
                    post_limit=5000,
                    sleep_interval=1.5
                ),
                filters=FilterConfig(
                    min_score=10,
                    include_external_links=True
                ),
                output=OutputConfig(
                    export_formats=["json", "csv", "sqlite"],
                    organize_by_date=True
                ),
                dry_run=True  # Start with dry run for research
            )
        else:
            # Default configuration
            config = AppConfig()
        
        # Convert to dictionary and write as YAML
        # Use mode='json' to ensure proper serialization of Path objects as strings
        config_dict = config.model_dump(mode='json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
    
    @property
    def config(self) -> Optional[AppConfig]:
        """Get the loaded configuration."""
        return self._config


class ConfigurationError(Exception):
    """Exception raised for configuration errors."""
    pass