"""
Tests for Configuration Models

Tests the Pydantic configuration models for validation, defaults,
and serialization/deserialization.
"""

import pytest
from datetime import datetime
from pathlib import Path
from pydantic import ValidationError

from redditdl.core.config.models import (
    ScrapingConfig,
    ProcessingConfig,
    OutputConfig, 
    FilterConfig,
    AppConfig
)


class TestScrapingConfig:
    """Test ScrapingConfig model validation and defaults."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = ScrapingConfig()
        
        assert config.api_mode is False
        assert config.client_id is None
        assert config.client_secret is None
        assert config.user_agent == "RedditDL/2.0 by u/redditdl"
        assert config.sleep_interval == 1.0
        assert config.api_rate_limit == 0.7
        assert config.public_rate_limit == 6.1
        assert config.timeout == 30
        assert config.max_retries == 3
        assert config.post_limit == 20
    
    def test_api_credentials_validation(self):
        """Test API credentials validation when API mode is enabled."""
        # Should pass without API mode
        config = ScrapingConfig(api_mode=False)
        assert config.api_mode is False
        
        # Should fail with API mode but no credentials
        with pytest.raises(ValidationError) as exc_info:
            ScrapingConfig(api_mode=True)
        assert "API credentials required" in str(exc_info.value)
        
        # Should pass with API mode and credentials
        config = ScrapingConfig(
            api_mode=True,
            client_id="test_id",
            client_secret="test_secret"
        )
        assert config.api_mode is True
        assert config.client_id == "test_id"
        assert config.client_secret == "test_secret"
    
    def test_sleep_interval_validation(self):
        """Test sleep interval validation and adjustment."""
        # Test minimum values
        config = ScrapingConfig(sleep_interval=0.1)
        assert config.sleep_interval == 0.1
        
        # Test API mode rate limiting
        config = ScrapingConfig(
            api_mode=True,
            client_id="test",
            client_secret="test",
            sleep_interval=0.5,
            api_rate_limit=0.7
        )
        # Should be adjusted to at least api_rate_limit
        assert config.sleep_interval >= config.api_rate_limit
        
        # Test invalid values
        with pytest.raises(ValidationError):
            ScrapingConfig(sleep_interval=-1.0)
        
        with pytest.raises(ValidationError):
            ScrapingConfig(sleep_interval=0.05)  # Below minimum
    
    def test_rate_limiting_bounds(self):
        """Test rate limiting parameter bounds."""
        with pytest.raises(ValidationError):
            ScrapingConfig(api_rate_limit=0.05)  # Too low
        
        with pytest.raises(ValidationError):
            ScrapingConfig(api_rate_limit=15.0)  # Too high
        
        with pytest.raises(ValidationError):
            ScrapingConfig(public_rate_limit=0.5)  # Too low
        
        with pytest.raises(ValidationError):
            ScrapingConfig(public_rate_limit=35.0)  # Too high


class TestProcessingConfig:
    """Test ProcessingConfig model validation and defaults."""
    
    def test_default_values(self):
        """Test default processing configuration values."""
        config = ProcessingConfig()
        
        assert config.chunk_size == 8192
        assert config.concurrent_downloads == 3
        assert config.image_format_conversion is False
        assert config.target_image_format == "jpeg"
        assert config.image_quality == 85
        assert config.max_image_resolution is None
        assert config.generate_thumbnails is False
        assert config.thumbnail_size == 256
        assert config.embed_metadata is True
        assert config.create_json_sidecars is True
    
    def test_image_format_validation(self):
        """Test image format validation."""
        # Valid formats
        for fmt in ['jpeg', 'jpg', 'png', 'webp', 'bmp', 'tiff']:
            config = ProcessingConfig(target_image_format=fmt)
            assert config.target_image_format == fmt.lower()
        
        # Invalid format
        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig(target_image_format="invalid")
        assert "Unsupported image format" in str(exc_info.value)
    
    def test_video_format_validation(self):
        """Test video format validation."""
        # Valid formats
        for fmt in ['mp4', 'avi', 'mkv', 'webm', 'mov']:
            config = ProcessingConfig(target_video_format=fmt)
            assert config.target_video_format == fmt.lower()
        
        # Invalid format
        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig(target_video_format="invalid")
        assert "Unsupported video format" in str(exc_info.value)
    
    def test_quality_bounds(self):
        """Test quality parameter bounds."""
        # Valid quality values
        config = ProcessingConfig(image_quality=1)
        assert config.image_quality == 1
        
        config = ProcessingConfig(image_quality=100)
        assert config.image_quality == 100
        
        # Invalid quality values
        with pytest.raises(ValidationError):
            ProcessingConfig(image_quality=0)
        
        with pytest.raises(ValidationError):
            ProcessingConfig(image_quality=101)
    
    def test_concurrent_downloads_bounds(self):
        """Test concurrent downloads bounds."""
        config = ProcessingConfig(concurrent_downloads=1)
        assert config.concurrent_downloads == 1
        
        config = ProcessingConfig(concurrent_downloads=20)
        assert config.concurrent_downloads == 20
        
        with pytest.raises(ValidationError):
            ProcessingConfig(concurrent_downloads=0)
        
        with pytest.raises(ValidationError):
            ProcessingConfig(concurrent_downloads=25)


class TestOutputConfig:
    """Test OutputConfig model validation and defaults."""
    
    def test_default_values(self):
        """Test default output configuration values."""
        config = OutputConfig()
        
        assert config.output_dir == Path("downloads")
        assert config.create_subdirs is True
        assert config.filename_template == "{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}"
        assert config.max_filename_length == 200
        assert config.organize_by_subreddit is True
        assert config.organize_by_date is False
        assert config.export_formats == ["json"]
        assert config.export_dir is None
    
    def test_filename_template_validation(self):
        """Test filename template validation."""
        # Valid template with required variables
        config = OutputConfig(
            filename_template="{{ subreddit }}/{{ post_id }}.{{ ext }}"
        )
        assert "{{ ext }}" in config.filename_template
        
        # Template missing required variable should fail
        with pytest.raises(ValidationError) as exc_info:
            OutputConfig(filename_template="{{ subreddit }}/{{ post_id }}")
        assert "ext" in str(exc_info.value)
    
    def test_export_formats_validation(self):
        """Test export formats validation."""
        # Valid formats
        config = OutputConfig(export_formats=["json", "csv", "sqlite"])
        assert config.export_formats == ["json", "csv", "sqlite"]
        
        # Invalid format
        with pytest.raises(ValidationError) as exc_info:
            OutputConfig(export_formats=["json", "invalid"])
        assert "Unsupported export format" in str(exc_info.value)
        
        # Case insensitive
        config = OutputConfig(export_formats=["JSON", "CSV"])
        assert config.export_formats == ["json", "csv"]
    
    def test_path_handling(self):
        """Test Path object handling."""
        config = OutputConfig(output_dir="/custom/path")
        assert isinstance(config.output_dir, Path)
        assert str(config.output_dir) == "/custom/path"


class TestFilterConfig:
    """Test FilterConfig model validation and defaults."""
    
    def test_default_values(self):
        """Test default filter configuration values."""
        config = FilterConfig()
        
        assert config.min_score is None
        assert config.max_score is None
        assert config.date_after is None
        assert config.date_before is None
        assert config.include_nsfw is True
        assert config.nsfw_only is False
        assert config.keywords_include == []
        assert config.keywords_exclude == []
        assert config.keyword_case_sensitive is False
        assert config.domains_allow == []
        assert config.domains_block == []
        assert config.include_images is True
        assert config.include_videos is True
        assert config.include_gifs is True
        assert config.include_galleries is True
        assert config.include_external_links is False
    
    def test_score_range_validation(self):
        """Test score range validation."""
        # Valid ranges
        config = FilterConfig(min_score=10, max_score=100)
        assert config.min_score == 10
        assert config.max_score == 100
        
        # Invalid range (max < min)
        with pytest.raises(ValidationError) as exc_info:
            FilterConfig(min_score=100, max_score=10)
        assert "max_score must be greater than min_score" in str(exc_info.value)
    
    def test_nsfw_settings_validation(self):
        """Test NSFW settings validation."""
        # Valid combinations
        config = FilterConfig(include_nsfw=True, nsfw_only=False)
        assert config.include_nsfw is True
        assert config.nsfw_only is False
        
        config = FilterConfig(include_nsfw=True, nsfw_only=True)
        assert config.include_nsfw is True
        assert config.nsfw_only is True
        
        # Invalid combination
        with pytest.raises(ValidationError) as exc_info:
            FilterConfig(include_nsfw=False, nsfw_only=True)
        assert "nsfw_only requires include_nsfw to be True" in str(exc_info.value)


class TestAppConfig:
    """Test AppConfig root model and integration."""
    
    def test_default_values(self):
        """Test default application configuration values."""
        config = AppConfig()
        
        assert config.version == "0.2.0"
        assert isinstance(config.created, datetime)
        assert isinstance(config.scraping, ScrapingConfig)
        assert isinstance(config.processing, ProcessingConfig)
        assert isinstance(config.output, OutputConfig)
        assert isinstance(config.filters, FilterConfig)
        assert config.dry_run is False
        assert config.verbose is False
        assert config.debug is False
        assert config.use_pipeline is True
        assert config.max_workers == 4
        assert config.enable_plugins is True
        assert config.plugin_dirs == [Path("plugins")]
        assert config.disabled_plugins == []
        assert config.session_dir == Path(".redditdl")
        assert config.auto_resume is True
    
    def test_nested_config_override(self):
        """Test overriding nested configuration sections."""
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id="test_id",
                client_secret="test_secret",
                post_limit=100
            ),
            processing=ProcessingConfig(
                image_quality=95,
                generate_thumbnails=True
            ),
            output=OutputConfig(
                output_dir="custom_downloads",
                organize_by_date=True
            ),
            dry_run=True,
            verbose=True
        )
        
        assert config.scraping.api_mode is True
        assert config.scraping.post_limit == 100
        assert config.processing.image_quality == 95
        assert config.processing.generate_thumbnails is True
        assert config.output.organize_by_date is True
        assert config.dry_run is True
        assert config.verbose is True
    
    def test_get_effective_sleep_interval(self):
        """Test effective sleep interval calculation."""
        # API mode
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id="test",
                client_secret="test",
                sleep_interval=0.5,
                api_rate_limit=0.7
            )
        )
        assert config.get_effective_sleep_interval() == 0.7  # Should use API rate limit
        
        # Public mode
        config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=False,
                sleep_interval=2.0,
                public_rate_limit=6.1
            )
        )
        assert config.get_effective_sleep_interval() == 6.1  # Should use public rate limit
    
    def test_get_export_dir(self):
        """Test export directory calculation."""
        # Default export dir
        config = AppConfig()
        expected_dir = config.output.output_dir / "exports"
        assert config.get_export_dir() == expected_dir
        
        # Custom export dir
        config = AppConfig(
            output=OutputConfig(export_dir="custom_exports")
        )
        assert config.get_export_dir() == Path("custom_exports")
    
    def test_plugin_dirs_validation(self):
        """Test plugin directories validation and creation."""
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = Path(temp_dir) / "test_plugins"
            
            config = AppConfig(plugin_dirs=[test_dir])
            
            # Directory should be created and validated
            assert test_dir in config.plugin_dirs
            assert test_dir.exists()
    
    def test_serialization(self):
        """Test configuration serialization and deserialization."""
        original_config = AppConfig(
            scraping=ScrapingConfig(
                api_mode=True,
                client_id="test_id", 
                client_secret="test_secret"
            ),
            dry_run=True,
            verbose=True
        )
        
        # Serialize to dict
        config_dict = original_config.model_dump()
        assert isinstance(config_dict, dict)
        assert config_dict["dry_run"] is True
        assert config_dict["scraping"]["api_mode"] is True
        
        # Deserialize from dict
        restored_config = AppConfig(**config_dict)
        assert restored_config.dry_run == original_config.dry_run
        assert restored_config.scraping.api_mode == original_config.scraping.api_mode
        assert restored_config.scraping.client_id == original_config.scraping.client_id
    
    def test_json_serialization(self):
        """Test JSON serialization with custom encoders."""
        config = AppConfig(
            output=OutputConfig(output_dir="/custom/path")
        )
        
        config_json = config.model_dump_json()
        assert isinstance(config_json, str)
        assert "/custom/path" in config_json
        
        # Test datetime serialization
        assert config.created.isoformat() in config_json
    
    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            AppConfig(unknown_field="value")
        assert "Extra inputs are not permitted" in str(exc_info.value)
    
    def test_field_validation_on_assignment(self):
        """Test that validation occurs on field assignment."""
        config = AppConfig()
        
        # Should validate on assignment
        with pytest.raises(ValidationError):
            config.max_workers = -1  # Invalid value
        
        # Valid assignment should work
        config.max_workers = 8
        assert config.max_workers == 8