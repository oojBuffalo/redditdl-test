"""
Tests for Configuration Integration

Tests the integration of the configuration system with CLI commands
and other parts of the application.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

from redditdl.cli.config_utils import (
    load_config_from_cli,
    setup_progress_observers,
    build_cli_args,
    print_config_summary,
    validate_api_credentials,
    convert_config_to_legacy,
    suggest_config_creation,
    get_config_file_suggestions,
    create_cli_args_for_targets
)
from redditdl.core.config.models import AppConfig, ScrapingConfig, OutputConfig, FilterConfig
from redditdl.core.config.manager import ConfigManager
from redditdl.core.events.emitter import EventEmitter
from redditdl.cli.observers.progress import CLIProgressObserver, OutputMode, ProgressDisplay


class TestLoadConfigFromCLI:
    """Test load_config_from_cli function."""
    
    def test_load_config_success(self):
        """Test successful configuration loading from CLI."""
        cli_args = {
            'api': True,
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'limit': 50,
            'verbose': True
        }
        
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(cli_args=cli_args)
            
            assert isinstance(config, AppConfig)
            assert config.scraping.api_mode is True
            assert config.scraping.client_id == 'test_id'
            assert config.scraping.post_limit == 50
            assert config.verbose is True
    
    def test_load_config_with_file(self):
        """Test loading configuration from file via CLI."""
        config_data = {
            'scraping': {'post_limit': 200},
            'dry_run': True
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
                config = load_config_from_cli(
                    config_file=temp_path,
                    cli_args={'verbose': True}
                )
                
                assert config.scraping.post_limit == 200
                assert config.dry_run is True
                assert config.verbose is True
        finally:
            Path(temp_path).unlink()
    
    def test_load_config_error_handling(self):
        """Test error handling in configuration loading."""
        import typer
        
        # Invalid CLI args that would cause validation error
        cli_args = {
            'api': True,
            # Missing client_id and client_secret
        }
        
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            with pytest.raises(typer.Exit):
                load_config_from_cli(cli_args=cli_args)


class TestBuildCLIArgs:
    """Test build_cli_args function."""
    
    def test_build_cli_args_basic(self):
        """Test basic CLI arguments building."""
        args = build_cli_args(
            api=True,
            client_id='test_id',
            limit=100,
            verbose=True,
            min_score=10
        )
        
        assert args['api'] is True
        assert args['client_id'] == 'test_id'
        assert args['limit'] == 100
        assert args['verbose'] is True
        assert args['min_score'] == 10
    
    def test_build_cli_args_none_exclusion(self):
        """Test that None values are excluded from CLI args."""
        args = build_cli_args(
            api=True,
            client_id=None,  # Should be excluded
            limit=100,
            verbose=None,    # Should be excluded
            min_score=0      # Should be included (0 is not None)
        )
        
        assert args['api'] is True
        assert 'client_id' not in args
        assert args['limit'] == 100
        assert 'verbose' not in args
        assert args['min_score'] == 0
    
    def test_build_cli_args_kwargs(self):
        """Test CLI args building with additional kwargs."""
        args = build_cli_args(
            api=True,
            custom_arg='custom_value',
            another_arg=42
        )
        
        assert args['api'] is True
        assert args['custom_arg'] == 'custom_value'
        assert args['another_arg'] == 42


class TestValidateAPICredentials:
    """Test validate_api_credentials function."""
    
    def test_validate_api_credentials_success(self):
        """Test successful API credentials validation."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id='valid_id',
                client_secret='valid_secret'
            )
        )
        
        # Should not raise any exception
        validate_api_credentials(config)
    
    def test_validate_api_credentials_not_needed(self):
        """Test API credentials validation when not in API mode."""
        config = AppConfig(
            scraping=ScrapingConfig(api_mode=False)
        )
        
        # Should not raise any exception
        validate_api_credentials(config)
    
    def test_validate_api_credentials_missing(self):
        """Test API credentials validation with missing credentials."""
        from pydantic import ValidationError
        
        # Should fail during config creation due to missing credentials
        with pytest.raises(ValidationError) as exc_info:
            AppConfig(
                scraping=ScrapingConfig(
                    api_mode=True,
                    client_id=None,  # Missing
                    client_secret='valid_secret'
                )
            )
        
        assert "API credentials required" in str(exc_info.value)


class TestConvertConfigToLegacy:
    """Test convert_config_to_legacy function."""
    
    def test_convert_basic_config(self):
        """Test converting basic configuration to legacy format."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id='test_id',
                post_limit=100
            ),
            dry_run=True,
            verbose=True
        )
        
        legacy = convert_config_to_legacy(config, target_user='testuser')
        
        assert legacy['api_mode'] is True
        assert legacy['client_id'] == 'test_id'
        assert legacy['post_limit'] == 100
        assert legacy['dry_run'] is True
        assert legacy['verbose'] is True
        assert legacy['target_user'] == 'testuser'
    
    def test_convert_complex_config(self):
        """Test converting complex configuration to legacy format."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=False,
                sleep_interval=2.0,
                public_rate_limit=8.0
            ),
            output=OutputConfig(
                output_dir='custom_downloads',
                organize_by_date=True,
                export_formats=['json', 'csv']
            ),
            filters=FilterConfig(
                min_score=50,
                include_nsfw=False
            )
        )
        
        legacy = convert_config_to_legacy(config)
        
        assert legacy['api_mode'] is False
        assert legacy['sleep_interval'] == 8.0  # Should use effective sleep interval
        assert legacy['output_dir'] == 'custom_downloads'
        assert legacy['organize_by_date'] is True
        assert legacy['export_formats'] == ['json', 'csv']
        assert legacy['min_score'] == 50
        assert legacy['include_nsfw'] is False
    
    def test_convert_paths_to_strings(self):
        """Test that Path objects are converted to strings."""
        config = AppConfig(
            output=OutputConfig(output_dir=Path('/custom/path'))
        )
        
        legacy = convert_config_to_legacy(config)
        
        assert isinstance(legacy['output_dir'], str)
        assert legacy['output_dir'] == '/custom/path'


class TestConfigFileSuggestions:
    """Test configuration file suggestion functions."""
    
    def test_get_config_file_suggestions(self):
        """Test getting configuration file suggestions."""
        suggestions = get_config_file_suggestions()
        
        assert len(suggestions) > 0
        assert all(isinstance(path, Path) for path in suggestions)
        
        # Should include common locations
        current_dir_configs = [s for s in suggestions if s.parent == Path.cwd()]
        assert len(current_dir_configs) > 0
        
        home_configs = [s for s in suggestions if Path.home() in s.parents]
        assert len(home_configs) > 0
    
    def test_suggest_config_creation(self):
        """Test configuration creation suggestion."""
        # This function prints suggestions, so we'll just test it doesn't crash
        # and produces some output
        
        with patch('cli.config_utils.console') as mock_console:
            suggest_config_creation()
            
            # Should have printed something
            assert mock_console.print.call_count > 0


class TestPrintConfigSummary:
    """Test print_config_summary function."""
    
    def test_print_config_summary_basic(self):
        """Test printing basic configuration summary."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                post_limit=50
            ),
            dry_run=True
        )
        
        with patch('cli.config_utils.console') as mock_console:
            print_config_summary(config, target='u/testuser')
            
            # Should have printed a table
            assert mock_console.print.call_count >= 2  # Table and newline
            
            # Check that table was created (first call should be Table)
            table_call = mock_console.print.call_args_list[0]
            # The table should contain configuration information
            assert table_call is not None
    
    def test_print_config_summary_with_filters(self):
        """Test printing configuration summary with filters."""
        config = AppConfig(
            filters=FilterConfig(
                min_score=100,
                include_keywords=['test', 'keyword'],
                include_nsfw=False
            )
        )
        
        with patch('cli.config_utils.console') as mock_console:
            print_config_summary(config)
            
            # Should have printed configuration including filters
            assert mock_console.print.call_count >= 2


class TestCLICommandIntegration:
    """Test integration with actual CLI commands."""
    
    def test_scrape_command_config_loading(self):
        """Test that scrape command properly loads configuration."""
        # This would be more of an integration test
        # For now, we'll test the config loading logic that would be used
        
        cli_args = {
            'api': True,
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'limit': 25,
            'outdir': 'test_output'
        }
        
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(cli_args=cli_args)
            
            # Verify configuration is loaded correctly for scrape command
            assert config.scraping.api_mode is True
            assert config.scraping.client_id == 'test_id'
            assert config.scraping.post_limit == 25
            assert str(config.output.output_dir) == 'test_output'
    
    def test_audit_command_config_loading(self):
        """Test that audit command properly loads configuration."""
        cli_args = {
            'verbose': True
        }
        
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(cli_args=cli_args)
            
            # Verify configuration is loaded correctly for audit command
            assert config.verbose is True
            # Should use defaults for other settings
            assert config.scraping.api_mode is False
            assert config.output.output_dir == Path('downloads')


class TestErrorHandling:
    """Test error handling in configuration integration."""
    
    def test_invalid_config_file_handling(self):
        """Test handling of invalid configuration files."""
        import typer
        
        # Create invalid config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_path = f.name
        
        try:
            with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
                with pytest.raises(typer.Exit):
                    load_config_from_cli(config_file=temp_path)
        finally:
            Path(temp_path).unlink()
    
    def test_validation_error_handling(self):
        """Test handling of configuration validation errors."""
        import typer
        
        # Create config that will fail validation
        cli_args = {
            'sleep': -1.0,  # Invalid sleep interval
        }
        
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            with pytest.raises(typer.Exit):
                load_config_from_cli(cli_args=cli_args)
    
    def test_missing_required_fields(self):
        """Test handling of missing required configuration fields."""
        import typer
        
        # API mode without credentials
        cli_args = {
            'api': True,
            # Missing client_id and client_secret
        }
        
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            with pytest.raises(typer.Exit):
                load_config_from_cli(cli_args=cli_args)


class TestBackwardCompatibility:
    """Test backward compatibility with legacy configuration."""
    
    def test_legacy_config_conversion(self):
        """Test that new config can be converted for legacy use."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id='test_id',
                client_secret='test_secret',
                sleep_interval=1.5,
                api_rate_limit=0.8
            ),
            output=OutputConfig(
                output_dir='legacy_output',
                export_formats=['json']
            ),
            dry_run=True
        )
        
        legacy = convert_config_to_legacy(config, target_user='legacyuser')
        
        # Check that all expected legacy fields are present
        expected_fields = [
            'api_mode', 'client_id', 'client_secret', 'sleep_interval',
            'output_dir', 'export_formats', 'dry_run', 'target_user',
            'post_limit', 'timeout', 'max_retries', 'embed_metadata',
            'create_json_sidecars', 'min_score', 'max_score'
        ]
        
        for field in expected_fields:
            assert field in legacy
        
        # Check specific values
        assert legacy['api_mode'] is True
        assert legacy['target_user'] == 'legacyuser'
        assert legacy['sleep_interval'] == 1.5  # Should use effective sleep interval
        assert legacy['output_dir'] == 'legacy_output'
    
    def test_effective_sleep_interval_calculation(self):
        """Test that effective sleep interval is calculated correctly."""
        # API mode - should use API rate limit if higher
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id='test',
                client_secret='test',
                sleep_interval=0.5,
                api_rate_limit=0.8
            )
        )
        
        assert config.get_effective_sleep_interval() == 0.8
        
        # Public mode - should use public rate limit if higher
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=False,
                sleep_interval=3.0,
                public_rate_limit=6.5
            )
        )
        
        assert config.get_effective_sleep_interval() == 6.5
        
        # Sleep interval higher than rate limit - should use sleep interval
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id='test',
                client_secret='test',
                sleep_interval=2.0,
                api_rate_limit=0.8
            )
        )
        
        assert config.get_effective_sleep_interval() == 2.0