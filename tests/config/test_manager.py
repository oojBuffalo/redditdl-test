"""
Tests for Configuration Manager

Tests the ConfigManager class for hierarchical configuration loading,
validation, and CLI integration.
"""

import pytest
import os
import json
import yaml
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from redditdl.core.config.manager import ConfigManager, ConfigurationError
from redditdl.core.config.models import AppConfig, ScrapingConfig, OutputConfig
from pydantic import ValidationError


class TestConfigManager:
    """Test ConfigManager basic functionality."""
    
    def test_init_default(self):
        """Test ConfigManager initialization with defaults."""
        manager = ConfigManager()
        assert manager.config_file is None
        assert manager._config is None
        assert len(manager._config_paths) > 0
    
    def test_init_with_config_file(self):
        """Test ConfigManager initialization with specific config file."""
        config_path = Path("test_config.yaml")
        manager = ConfigManager(config_file=config_path)
        assert manager.config_file == config_path
    
    def test_load_config_defaults_only(self):
        """Test loading configuration with defaults only."""
        manager = ConfigManager()
        
        # Mock file loading to return None (no config files)
        with patch.object(manager, '_load_config_file', return_value=None):
            with patch.object(manager, '_load_env_config', return_value={}):
                config = manager.load_config()
                
                assert isinstance(config, AppConfig)
                assert config.scraping.post_limit == 20  # Default value
                assert config.output.output_dir == Path("downloads")
    
    def test_load_config_with_cli_args(self):
        """Test loading configuration with CLI argument overrides."""
        manager = ConfigManager()
        
        cli_args = {
            'limit': 50,
            'api': True,
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'verbose': True
        }
        
        with patch.object(manager, '_load_config_file', return_value=None):
            with patch.object(manager, '_load_env_config', return_value={}):
                config = manager.load_config(cli_args=cli_args)
                
                assert config.scraping.post_limit == 50
                assert config.scraping.api_mode is True
                assert config.scraping.client_id == 'test_id'
                assert config.verbose is True


class TestConfigFileLoading:
    """Test configuration file loading functionality."""
    
    def test_load_yaml_config_file(self):
        """Test loading YAML configuration file."""
        config_data = {
            'scraping': {
                'api_mode': True,
                'post_limit': 100
            },
            'verbose': True
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            manager = ConfigManager(config_file=temp_path)
            loaded_data = manager._load_config_file()
            
            assert loaded_data['scraping']['api_mode'] is True
            assert loaded_data['scraping']['post_limit'] == 100
            assert loaded_data['verbose'] is True
        finally:
            os.unlink(temp_path)
    
    def test_load_json_config_file(self):
        """Test loading JSON configuration file."""
        config_data = {
            'scraping': {
                'api_mode': False,
                'post_limit': 200
            },
            'dry_run': True
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name
        
        try:
            manager = ConfigManager(config_file=temp_path)
            loaded_data = manager._load_config_file()
            
            assert loaded_data['scraping']['api_mode'] is False
            assert loaded_data['scraping']['post_limit'] == 200
            assert loaded_data['dry_run'] is True
        finally:
            os.unlink(temp_path)
    
    def test_load_nonexistent_config_file(self):
        """Test handling of nonexistent configuration file."""
        manager = ConfigManager(config_file="nonexistent.yaml")
        loaded_data = manager._load_config_file()
        assert loaded_data is None
    
    def test_load_invalid_config_file(self):
        """Test handling of invalid configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = f.name
        
        try:
            manager = ConfigManager(config_file=temp_path)
            
            with pytest.raises(ConfigurationError) as exc_info:
                manager._load_config_file()
            assert "Failed to load config file" in str(exc_info.value)
        finally:
            os.unlink(temp_path)
    
    def test_search_default_config_paths(self):
        """Test searching for configuration in default paths."""
        # Create a temporary config file in current directory
        config_data = {'verbose': True}
        config_path = Path.cwd() / "redditdl.yaml"
        
        try:
            with open(config_path, 'w') as f:
                yaml.dump(config_data, f)
            
            manager = ConfigManager()  # No specific config file
            loaded_data = manager._load_config_file()
            
            assert loaded_data is not None
            assert loaded_data['verbose'] is True
        finally:
            if config_path.exists():
                config_path.unlink()


class TestEnvironmentVariableLoading:
    """Test environment variable configuration loading."""
    
    def test_load_env_config_basic(self):
        """Test loading basic environment variables."""
        env_vars = {
            'REDDITDL_API_MODE': 'true',
            'REDDITDL_CLIENT_ID': 'env_client_id',
            'REDDITDL_POST_LIMIT': '75',
            'REDDITDL_VERBOSE': 'true',
            'REDDITDL_DRY_RUN': 'false'
        }
        
        manager = ConfigManager()
        
        with patch.dict(os.environ, env_vars):
            env_config = manager._load_env_config("REDDITDL_")
            
            assert env_config['scraping']['api_mode'] is True
            assert env_config['scraping']['client_id'] == 'env_client_id'
            assert env_config['scraping']['post_limit'] == 75
            assert env_config['verbose'] is True
            assert env_config['dry_run'] is False
    
    def test_parse_bool_values(self):
        """Test parsing boolean values from environment variables."""
        manager = ConfigManager()
        
        # Test truthy values
        for value in ['true', 'True', '1', 'yes', 'on', 'enabled']:
            assert manager._parse_bool(value) is True
        
        # Test falsy values
        for value in ['false', 'False', '0', 'no', 'off', 'disabled']:
            assert manager._parse_bool(value) is False
        
        # Test actual boolean values
        assert manager._parse_bool(True) is True
        assert manager._parse_bool(False) is False
    
    def test_parse_list_values(self):
        """Test parsing list values from environment variables."""
        manager = ConfigManager()
        
        # Test comma-separated string
        result = manager._parse_list("json,csv,sqlite")
        assert result == ["json", "csv", "sqlite"]
        
        # Test string with spaces
        result = manager._parse_list("json, csv , sqlite ")
        assert result == ["json", "csv", "sqlite"]
        
        # Test empty string
        result = manager._parse_list("")
        assert result == []
        
        # Test already list
        result = manager._parse_list(["json", "csv"])
        assert result == ["json", "csv"]
    
    def test_env_config_error_handling(self):
        """Test error handling in environment variable parsing."""
        env_vars = {
            'REDDITDL_POST_LIMIT': 'invalid_number',
        }
        
        manager = ConfigManager()
        
        with patch.dict(os.environ, env_vars):
            with pytest.raises(ConfigurationError) as exc_info:
                manager._load_env_config("REDDITDL_")
            assert "Invalid value for REDDITDL_POST_LIMIT" in str(exc_info.value)


class TestCLIArgsNormalization:
    """Test CLI arguments normalization."""
    
    def test_normalize_cli_args_basic(self):
        """Test basic CLI arguments normalization."""
        manager = ConfigManager()
        
        cli_args = {
            'api': True,
            'client_id': 'cli_client_id',
            'limit': 30,
            'outdir': 'cli_downloads',
            'verbose': True,
            'min_score': 10
        }
        
        normalized = manager._normalize_cli_args(cli_args)
        
        assert normalized['scraping']['api_mode'] is True
        assert normalized['scraping']['client_id'] == 'cli_client_id'
        assert normalized['scraping']['post_limit'] == 30
        assert normalized['output']['output_dir'] == 'cli_downloads'
        assert normalized['verbose'] is True
        assert normalized['filters']['min_score'] == 10
    
    def test_normalize_cli_args_none_values(self):
        """Test that None values are excluded from normalization."""
        manager = ConfigManager()
        
        cli_args = {
            'api': True,
            'client_id': None,  # Should be excluded
            'limit': 30,
            'outdir': None,     # Should be excluded
        }
        
        normalized = manager._normalize_cli_args(cli_args)
        
        assert normalized['scraping']['api_mode'] is True
        assert 'client_id' not in normalized['scraping']
        assert normalized['scraping']['post_limit'] == 30
        assert 'output_dir' not in normalized.get('output', {})


class TestConfigurationHierarchy:
    """Test hierarchical configuration loading and merging."""
    
    def test_deep_merge(self):
        """Test deep merging of configuration dictionaries."""
        manager = ConfigManager()
        
        base = {
            'scraping': {
                'api_mode': False,
                'post_limit': 20
            },
            'verbose': False
        }
        
        override = {
            'scraping': {
                'api_mode': True,
                'client_id': 'new_id'
            },
            'dry_run': True
        }
        
        result = manager._deep_merge(base, override)
        
        # Should merge nested dictionaries
        assert result['scraping']['api_mode'] is True  # Overridden
        assert result['scraping']['post_limit'] == 20  # Preserved
        assert result['scraping']['client_id'] == 'new_id'  # Added
        assert result['verbose'] is False  # Preserved
        assert result['dry_run'] is True  # Added
    
    def test_configuration_precedence(self):
        """Test configuration precedence: CLI > env > file > defaults."""
        config_file_data = {
            'scraping': {'post_limit': 100},
            'verbose': True
        }
        
        env_vars = {
            'REDDITDL_POST_LIMIT': '50',  # Should override file
            'REDDITDL_API_MODE': 'true',   # Should add to config
            'REDDITDL_CLIENT_ID': 'env_id',
            'REDDITDL_CLIENT_SECRET': 'env_secret'
        }
        
        cli_args = {
            'limit': 25,     # Should override env and file
            'dry_run': True  # Should add to config
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_file_data, f)
            temp_path = f.name
        
        try:
            manager = ConfigManager(config_file=temp_path)
            
            with patch.dict(os.environ, env_vars):
                config = manager.load_config(cli_args=cli_args)
                
                # CLI should win
                assert config.scraping.post_limit == 25
                
                # Env should override file
                assert config.scraping.api_mode is True
                
                # File value should be preserved if not overridden
                assert config.verbose is True
                
                # CLI addition should be present
                assert config.dry_run is True
        finally:
            os.unlink(temp_path)


class TestConfigValidation:
    """Test configuration validation functionality."""
    
    def test_validate_config_success(self):
        """Test successful configuration validation."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id='valid_id',
                client_secret='valid_secret'
            )
        )
        
        manager = ConfigManager()
        warnings = manager.validate_config(config)
        
        # Should have no warnings for valid configuration
        assert len([w for w in warnings if 'API mode' in w]) == 0
    
    def test_validate_config_api_warnings(self):
        """Test configuration validation with API warnings."""
        with pytest.raises(ValidationError):
            AppConfig(
                scraping=ScrapingConfig(
                    api_mode=True,
                    client_id=None,  # Missing
                    client_secret='valid_secret'
                )
            )
    
    def test_validate_config_directory_warnings(self):
        """Test configuration validation with directory warnings."""
        import tempfile
        
        # Test with invalid output directory
        config = AppConfig(
            output=OutputConfig(output_dir="/invalid/nonexistent/path")
        )
        
        manager = ConfigManager()
        warnings = manager.validate_config(config)
        
        # Should warn about directory creation issues
        dir_warnings = [w for w in warnings if 'directory' in w]
        assert len(dir_warnings) > 0
    
    def test_validate_config_nsfw_warning(self):
        """Test configuration validation with NSFW setting conflicts."""
        from redditdl.core.config.models import FilterConfig

        with pytest.raises(ValidationError):
            AppConfig(
                filters=FilterConfig(
                    include_nsfw=False,
                    nsfw_only=True  # Conflicting setting (should be caught by Pydantic)
                )
            )


class TestConfigGeneration:
    """Test configuration generation and schema functionality."""
    
    def test_generate_schema(self):
        """Test configuration schema generation."""
        manager = ConfigManager()
        schema = manager.generate_schema()
        
        assert isinstance(schema, dict)
        assert 'properties' in schema
        assert 'scraping' in schema['properties']
        assert 'processing' in schema['properties']
        assert 'output' in schema['properties']
        assert 'filters' in schema['properties']
    
    def test_generate_schema_to_file(self):
        """Test writing configuration schema to file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = Path(f.name)
        
        try:
            manager = ConfigManager()
            schema = manager.generate_schema(output_file=temp_path)
            
            # Should write to file
            assert temp_path.exists()
            
            # Should return the schema
            assert isinstance(schema, dict)
            
            # File should contain valid JSON
            with open(temp_path, 'r') as f:
                loaded_schema = json.load(f)
            assert loaded_schema == schema
        finally:
            if temp_path.exists():
                temp_path.unlink()
    
    def test_create_example_config_default(self):
        """Test creating default example configuration."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = Path(f.name)
        
        try:
            manager = ConfigManager()
            manager.create_example_config(temp_path, profile="default")
            
            assert temp_path.exists()
            
            # Load and verify the generated config
            with open(temp_path, 'r') as f:
                config_data = yaml.safe_load(f)
            
            assert isinstance(config_data, dict)
            assert 'scraping' in config_data
            assert 'output' in config_data
        finally:
            if temp_path.exists():
                temp_path.unlink()
    
    def test_create_example_config_profiles(self):
        """Test creating example configurations for different profiles."""
        profiles = ["default", "images-only", "full-archive", "research"]
        
        for profile in profiles:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                temp_path = Path(f.name)
            
            try:
                manager = ConfigManager()
                manager.create_example_config(temp_path, profile=profile)
                
                assert temp_path.exists()
                
                # Load and verify the generated config
                with open(temp_path, 'r') as f:
                    config_data = yaml.safe_load(f)
                
                assert isinstance(config_data, dict)
                
                # Profile-specific assertions
                if profile == "images-only":
                    assert config_data['filters']['include_videos'] is False
                    assert config_data['filters']['include_images'] is True
                elif profile == "research":
                    assert config_data['dry_run'] is True
                elif profile == "full-archive":
                    assert config_data['scraping']['post_limit'] >= 1000
            finally:
                if temp_path.exists():
                    temp_path.unlink()


class TestConfigManagerIntegration:
    """Test ConfigManager integration scenarios."""
    
    def test_complete_configuration_workflow(self):
        """Test complete configuration loading workflow."""
        # Create a config file
        config_file_data = {
            'scraping': {
                'post_limit': 100,
                'sleep_interval': 2.0
            },
            'output': {
                'organize_by_date': True
            }
        }
        
        # Set up environment variables
        env_vars = {
            'REDDITDL_API_MODE': 'true',
            'REDDITDL_CLIENT_ID': 'env_client_id',
            'REDDITDL_CLIENT_SECRET': 'env_client_secret'
        }
        
        # Set up CLI arguments
        cli_args = {
            'limit': 50,  # Should override file and env
            'verbose': True,
            'outdir': 'cli_output'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_file_data, f)
            temp_path = f.name
        
        try:
            manager = ConfigManager(config_file=temp_path)
            
            with patch.dict(os.environ, env_vars):
                config = manager.load_config(cli_args=cli_args)
                
                # Verify hierarchical loading worked correctly
                assert config.scraping.post_limit == 50  # CLI override
                assert config.scraping.api_mode is True  # From env
                assert config.scraping.client_id == 'env_client_id'  # From env
                assert config.scraping.sleep_interval == 2.0  # From file
                assert config.output.organize_by_date is True  # From file
                assert config.verbose is True  # From CLI
                assert str(config.output.output_dir) == 'cli_output'  # CLI override
                
                # Verify validation passes
                warnings = manager.validate_config(config)
                # Should not have API credential warnings since they're provided
                api_warnings = [w for w in warnings if 'client_id' in w or 'client_secret' in w]
                assert len(api_warnings) == 0
        finally:
            os.unlink(temp_path)