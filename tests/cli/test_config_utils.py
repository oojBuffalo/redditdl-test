"""
Tests for CLI Configuration Utilities

Tests the configuration utilities used by CLI commands including
configuration loading, validation, and CLI argument processing.
"""

import os
import pytest
import tempfile
import typer
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

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


@pytest.fixture
def temp_config():
    """Create a temporary configuration for testing."""
    return AppConfig()


@pytest.fixture
def temp_config_file(tmp_path):
    """Create a temporary YAML config file."""
    config_data = {
        'scraping': {'post_limit': 100},
        'verbose': True
    }
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_data, f)
    return str(config_file)


class TestLoadConfigFromCli:
    """Test configuration loading from CLI arguments."""

    def test_load_config_basic(self, temp_config_file):
        """Test basic configuration loading."""
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(config_file=temp_config_file)

            assert isinstance(config, AppConfig)
            assert config.scraping.post_limit == 100
            assert config.verbose is True

    def test_load_config_with_cli_args(self, temp_config_file):
        """Test configuration loading with CLI argument overrides."""
        cli_args = {
            'verbose': False,
            'dry_run': True,
            'limit': 50
        }

        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(
                config_file=temp_config_file,
                cli_args=cli_args
            )

            assert config.verbose is False
            assert config.dry_run is True
            assert config.scraping.post_limit == 50

    def test_load_config_no_file(self):
        """Test configuration loading without config file."""
        cli_args = {
            'verbose': True,
            'outdir': 'test_output'
        }

        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(cli_args=cli_args)

            assert isinstance(config, AppConfig)
            assert config.verbose is True
            assert str(config.output.output_dir) == 'test_output'

    def test_load_config_invalid_file(self):
        """Test configuration loading with invalid config file."""
        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            with pytest.raises(typer.Exit):
                load_config_from_cli(config_file='nonexistent.yaml')

    @patch('redditdl.cli.config_utils.console')
    def test_load_config_with_warnings(self, mock_console, temp_config_file):
        """Test configuration loading with validation warnings."""
        cli_args = {
            'api_mode': True,
            # Missing client_id and client_secret should trigger warnings
        }

        with patch('redditdl.cli.config_utils.load_credentials_from_dotenv'):
            config = load_config_from_cli(
                config_file=temp_config_file,
                cli_args=cli_args
            )

            assert isinstance(config, AppConfig)
            # Should have printed warnings
            assert mock_console.print.called


class TestSetupProgressObservers:
    """Test progress observer setup functionality."""

    def test_setup_observers_normal_mode(self, temp_config):
        """Test setting up observers in normal mode."""
        with patch('redditdl.cli.config_utils.CLIProgressObserver'):
            emitter = setup_progress_observers(temp_config)

            assert isinstance(emitter, EventEmitter)
            assert any(isinstance(obs, CLIProgressObserver) for obs in emitter._observers['*'])

    def test_setup_observers_quiet_mode(self, temp_config):
        """Test setting up observers in quiet mode."""
        temp_config.ui_config['quiet_mode'] = True
        temp_config.ui_config['output_mode'] = 'quiet'

        emitter = setup_progress_observers(temp_config)

        cli_observers = [obs for obs in emitter.observers
                        if isinstance(obs, CLIProgressObserver)]
        assert len(cli_observers) == 1
        assert cli_observers[0].output_mode == OutputMode.QUIET

    def test_setup_observers_verbose_mode(self, temp_config):
        """Test setting up observers in verbose mode."""
        temp_config.verbose = True

        emitter = setup_progress_observers(temp_config)

        cli_observers = [obs for obs in emitter.observers
                        if isinstance(obs, CLIProgressObserver)]
        assert len(cli_observers) == 1
        assert cli_observers[0].output_mode == OutputMode.VERBOSE

    def test_setup_observers_json_mode(self, temp_config):
        """Test setting up observers in JSON mode."""
        temp_config.ui_config['output_mode'] = 'json'
        temp_config.ui_config['json_output'] = 'test_output.json'

        emitter = setup_progress_observers(temp_config)

        cli_observers = [obs for obs in emitter.observers
                        if isinstance(obs, CLIProgressObserver)]
        assert len(cli_observers) == 1
        assert cli_observers[0].output_mode == OutputMode.JSON

    def test_setup_observers_no_progress(self, temp_config):
        """Test setting up observers with progress disabled."""
        temp_config.ui_config['no_progress'] = True
        temp_config.ui_config['progress_display'] = 'none'

        emitter = setup_progress_observers(temp_config)

        cli_observers = [obs for obs in emitter.observers
                        if isinstance(obs, CLIProgressObserver)]
        assert len(cli_observers) == 1
        assert cli_observers[0].progress_display == ProgressDisplay.NONE

    @patch('redditdl.cli.config_utils.console')
    def test_setup_observers_fallback_on_error(self, mock_console, temp_config):
        """Test observer setup falls back to basic console on error."""
        # Mock CLIProgressObserver to raise exception
        with patch('redditdl.cli.config_utils.CLIProgressObserver', side_effect=Exception("Test error")):
            emitter = setup_progress_observers(temp_config)

            assert isinstance(emitter, EventEmitter)
            # Should have printed warning about fallback
            assert mock_console.print.called


class TestBuildCliArgs:
    """Test CLI arguments dictionary building."""

    def test_build_cli_args_basic(self):
        """Test building CLI args with basic parameters."""
        args = build_cli_args(
            verbose=True,
            dry_run=False,
            limit=25
        )

        assert args['verbose'] is True
        assert args['dry_run'] is False
        assert args['scraping']['limit'] == 25
        assert 'config' not in args  # None values should be excluded

    def test_build_cli_args_comprehensive(self):
        """Test building CLI args with many parameters."""
        args = build_cli_args(
            verbose=True,
            api=True,
            client_id='test_id',
            client_secret='test_secret',
            outdir='test_dir',
            export_formats=['json', 'csv'],
            min_score=10,
            include_keywords=['test', 'keyword'],
            quiet=True,
            progress_display='rich'
        )

        assert args['verbose'] is True
        assert args['api'] is True
        assert args['client_id'] == 'test_id'
        assert args['client_secret'] == 'test_secret'
        assert args['outdir'] == 'test_dir'
        assert args['export_formats'] == ['json', 'csv']
        assert args['min_score'] == 10
        assert args['include_keywords'] == ['test', 'keyword']
        assert args['quiet'] is True
        assert args['progress_display'] == 'rich'

    def test_build_cli_args_none_values_excluded(self):
        """Test that None values are properly excluded."""
        args = build_cli_args(
            verbose=True,
            config=None,
            limit=None,
            timeout=30
        )

        assert 'verbose' in args
        assert 'config' not in args
        assert 'limit' not in args
        assert 'timeout' in args

    def test_build_cli_args_with_kwargs(self):
        """Test building CLI args with additional kwargs."""
        args = build_cli_args(
            verbose=True,
            custom_arg='custom_value',
            another_arg=42
        )

        assert args['verbose'] is True
        assert args['custom_arg'] == 'custom_value'
        assert args['another_arg'] == 42


class TestPrintConfigSummary:
    """Test configuration summary printing."""

    @patch('redditdl.cli.config_utils.console')
    def test_print_config_summary_basic(self, mock_console, temp_config):
        """Test basic configuration summary printing."""
        print_config_summary(temp_config)

        # Should have called console.print to display table
        assert mock_console.print.called

        # Verify table creation with some expected content
        call_args = mock_console.print.call_args_list
        assert any('Configuration Summary' in str(call) for call in call_args)

    @patch('redditdl.cli.config_utils.console')
    def test_print_config_summary_with_target(self, mock_console, temp_config):
        """Test configuration summary with target specified."""
        print_config_summary(temp_config, target='testuser')

        assert mock_console.print.called
        # Target should be included in output
        call_args = str(mock_console.print.call_args_list)
        assert 'testuser' in call_args

    @patch('redditdl.cli.config_utils.console')
    def test_print_config_summary_api_mode(self, mock_console, temp_config):
        """Test configuration summary in API mode."""
        temp_config.scraping.api_mode = True

        print_config_summary(temp_config)

        assert mock_console.print.called
        call_args = str(mock_console.print.call_args_list)
        assert 'API' in call_args


class TestValidateApiCredentials:
    """Test API credentials validation."""

    def test_validate_api_credentials_valid(self, temp_config):
        """Test validation with valid API credentials."""
        temp_config.scraping.api_mode = True
        temp_config.scraping.client_id = 'valid_id'
        temp_config.scraping.client_secret = 'valid_secret'

        # Should not raise exception
        validate_api_credentials(temp_config)

    def test_validate_api_credentials_not_api_mode(self, temp_config):
        """Test validation when not in API mode."""
        temp_config.scraping.api_mode = False

        # Should not raise exception even without credentials
        validate_api_credentials(temp_config)

    @patch('redditdl.cli.config_utils.console')
    def test_validate_api_credentials_missing_id(self, mock_console, temp_config):
        """Test validation with missing client ID."""
        temp_config.scraping.api_mode = True
        temp_config.scraping.client_id = ''
        temp_config.scraping.client_secret = 'valid_secret'

        with pytest.raises(typer.Exit):
            validate_api_credentials(temp_config)

        assert mock_console.print.called

    @patch('redditdl.cli.config_utils.console')
    def test_validate_api_credentials_missing_secret(self, mock_console, temp_config):
        """Test validation with missing client secret."""
        temp_config.scraping.api_mode = True
        temp_config.scraping.client_id = 'valid_id'
        temp_config.scraping.client_secret = ''

        with pytest.raises(typer.Exit):
            validate_api_credentials(temp_config)

        assert mock_console.print.called


class TestConvertConfigToLegacy:
    """Test configuration conversion to legacy format."""

    def test_convert_config_basic(self, temp_config):
        """Test basic configuration conversion."""
        legacy_config = convert_config_to_legacy(temp_config)

        assert isinstance(legacy_config, dict)
        assert legacy_config['dry_run'] == temp_config.dry_run
        assert legacy_config['verbose'] == temp_config.verbose
        assert legacy_config['api_mode'] == temp_config.scraping.api_mode
        assert legacy_config['output_dir'] == str(temp_config.output.output_dir)

    def test_convert_config_with_target_user(self, temp_config):
        """Test configuration conversion with target user."""
        legacy_config = convert_config_to_legacy(temp_config, target_user='testuser')

        assert legacy_config['target_user'] == 'testuser'
        assert 'dry_run' in legacy_config
        assert 'verbose' in legacy_config

    def test_convert_config_all_fields(self, temp_config):
        """Test that all expected legacy fields are present."""
        legacy_config = convert_config_to_legacy(temp_config)

        expected_fields = [
            'dry_run', 'verbose', 'use_pipeline',
            'api_mode', 'client_id', 'client_secret', 'user_agent',
            'username', 'password', 'sleep_interval', 'post_limit',
            'timeout', 'max_retries', 'output_dir', 'export_formats',
            'organize_by_date', 'organize_by_author', 'organize_by_subreddit',
            'filename_template', 'embed_metadata', 'create_json_sidecars',
            'concurrent_downloads', 'image_quality', 'min_score', 'max_score',
            'include_nsfw', 'nsfw_only', 'include_keywords', 'exclude_keywords',
            'after_date', 'before_date'
        ]

        for field in expected_fields:
            assert field in legacy_config


class TestConfigFileSuggestions:
    """Test configuration file suggestion utilities."""

    def test_get_config_file_suggestions(self):
        """Test getting configuration file suggestions."""
        suggestions = get_config_file_suggestions()

        assert isinstance(suggestions, list)
        assert len(suggestions) > 0

        # Should include common locations
        suggestion_strs = [str(s) for s in suggestions]
        assert any('redditdl.yaml' in s for s in suggestion_strs)
        assert any('.redditdl.yaml' in s for s in suggestion_strs)

    @patch.dict(os.environ, {'XDG_CONFIG_HOME': '/test/xdg'})
    def test_get_config_file_suggestions_with_xdg(self):
        """Test suggestions include XDG config directory when available."""
        suggestions = get_config_file_suggestions()

        suggestion_strs = [str(s) for s in suggestions]
        assert any('xdg' in s for s in suggestion_strs)

    @patch('redditdl.cli.config_utils.console')
    def test_suggest_config_creation_no_existing(self, mock_console):
        """Test suggesting config creation when no configs exist."""
        with patch('pathlib.Path.exists', return_value=False):
            suggest_config_creation()

            assert mock_console.print.called
            # Should suggest creating a config file
            call_args = str(mock_console.print.call_args_list)
            assert 'config' in call_args.lower()

    @patch('redditdl.cli.config_utils.console')
    def test_suggest_config_creation_existing_config(self, mock_console):
        """Test no suggestion when config already exists."""
        with patch('pathlib.Path.exists', return_value=True):
            suggest_config_creation()

            # Should not print suggestions
            assert not mock_console.print.called


class TestCreateCliArgsForTargets:
    """Test CLI args creation for multi-target operations."""

    def test_create_cli_args_for_targets_basic(self):
        """Test creating CLI args for basic targets."""
        args = create_cli_args_for_targets(
            targets=['user1', 'user2'],
            verbose=True
        )

        assert args['targets'] == ['user1', 'user2']
        assert args['verbose'] is True

    def test_create_cli_args_for_targets_with_file(self):
        """Test creating CLI args with targets file."""
        args = create_cli_args_for_targets(
            targets_file='targets.txt',
            concurrent_targets=3,
            listing_type='hot'
        )

        assert args['targets_file'] == 'targets.txt'
        assert args['concurrent_targets'] == 3
        assert args['listing_type'] == 'hot'

    def test_create_cli_args_for_targets_subreddit_options(self):
        """Test creating CLI args with subreddit-specific options."""
        args = create_cli_args_for_targets(
            targets=['r/python', 'r/programming'],
            listing_type='top',
            time_period='week',
            limit=100
        )

        assert args['targets'] == ['r/python', 'r/programming']
        assert args['listing_type'] == 'top'
        assert args['time_period'] == 'week'
        assert args['limit'] == 100

    def test_create_cli_args_for_targets_all_options(self):
        """Test creating CLI args with all multi-target options."""
        args = create_cli_args_for_targets(
            targets=['user1', 'r/test'],
            targets_file='extra_targets.txt',
            concurrent_targets=2,
            listing_type='new',
            time_period='day',
            verbose=True,
            dry_run=True,
            api=True
        )

        assert args['targets'] == ['user1', 'r/test']
        assert args['targets_file'] == 'extra_targets.txt'
        assert args['concurrent_targets'] == 2
        assert args['listing_type'] == 'new'
        assert args['time_period'] == 'day'
        assert args['verbose'] is True
        assert args['dry_run'] is True
        assert args['api'] is True