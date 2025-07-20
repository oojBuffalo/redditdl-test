"""
Tests for FilenameTemplateEngine

Tests Jinja2 template rendering, custom filters, validation,
and backward compatibility with simple {variable} templates.
"""

import pytest
from datetime import datetime
from pathlib import Path

from redditdl.core.templates import FilenameTemplateEngine
from jinja2 import TemplateSyntaxError, UndefinedError


class TestFilenameTemplateEngine:
    """Test the FilenameTemplateEngine class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.engine = FilenameTemplateEngine()
        self.sample_variables = {
            'subreddit': 'python',
            'post_id': 'abc123',
            'title': 'Amazing Python Tutorial!',
            'author': 'python_guru',
            'date': '2024-01-15T10:30:00Z',
            'ext': 'jpg',
            'content_type': 'image',
            'url': 'https://reddit.com/r/python/abc123',
            'is_video': False,
            'score': 150
        }
    
    def test_basic_template_rendering(self):
        """Test basic Jinja2 template rendering."""
        template = "{{ subreddit }}/{{ post_id }}.{{ ext }}"
        result = self.engine.render(template, self.sample_variables)
        assert result == "python/abc123.jpg"
    
    def test_slugify_filter(self):
        """Test the slugify custom filter."""
        template = "{{ title|slugify }}.{{ ext }}"
        result = self.engine.render(template, self.sample_variables)
        assert result == "amazing-python-tutorial.jpg"
    
    def test_slugify_filter_with_special_chars(self):
        """Test slugify with special characters."""
        variables = self.sample_variables.copy()
        variables['title'] = "Test!@#$%^&*()Title_123"
        template = "{{ title|slugify }}.{{ ext }}"
        result = self.engine.render(template, variables)
        assert result == "testtitle-123.jpg"
    
    def test_slugify_filter_max_length(self):
        """Test slugify filter with max length parameter."""
        variables = self.sample_variables.copy()
        variables['title'] = "This is a very long title that should be truncated"
        template = "{{ title|slugify(20) }}.{{ ext }}"
        result = self.engine.render(template, variables)
        assert len(Path(result).stem) <= 20
    
    def test_sanitize_filter(self):
        """Test the sanitize custom filter."""
        variables = self.sample_variables.copy()
        variables['title'] = 'File/with\\invalid*chars?.jpg'
        template = "{{ title|sanitize }}.{{ ext }}"
        result = self.engine.render(template, variables)
        assert '/' not in result and '\\' not in result
        assert '*' not in result and '?' not in result
    
    def test_strftime_filter_with_iso_date(self):
        """Test strftime filter with ISO date string."""
        template = "{{ date|strftime('%Y/%m/%d') }}/{{ post_id }}.{{ ext }}"
        result = self.engine.render(template, self.sample_variables)
        assert result == "2024/01/15/abc123.jpg"
    
    def test_strftime_filter_custom_format(self):
        """Test strftime filter with custom format."""
        template = "{{ date|strftime('%Y-%m') }}-{{ post_id }}.{{ ext }}"
        result = self.engine.render(template, self.sample_variables)
        assert result == "2024-01-abc123.jpg"
    
    def test_strftime_filter_with_timestamp(self):
        """Test strftime filter with Unix timestamp."""
        variables = self.sample_variables.copy()
        variables['date'] = 1705316200  # 2024-01-15 10:30:00 UTC
        template = "{{ date|strftime('%Y-%m-%d') }}/{{ post_id }}.{{ ext }}"
        result = self.engine.render(template, variables)
        assert "2024-01-15" in result
    
    def test_truncate_filter(self):
        """Test the truncate filter."""
        template = "{{ title|truncate(10) }}.{{ ext }}"
        result = self.engine.render(template, self.sample_variables)
        stem = Path(result).stem
        assert len(stem) <= 10
    
    def test_extension_filter(self):
        """Test the extension filter."""
        variables = self.sample_variables.copy()
        variables['media_url'] = 'https://example.com/image.png?v=1'
        template = "{{ post_id }}.{{ media_url|extension }}"
        result = self.engine.render(template, variables)
        assert result == "abc123.png"
    
    def test_simple_template_conversion(self):
        """Test conversion of simple {variable} templates to Jinja2."""
        template = "{subreddit}/{post_id}-{title}.{ext}"
        result = self.engine.render(template, self.sample_variables)
        expected = "python/abc123-Amazing Python Tutorial!.jpg"
        # Note: Without slugify filter, special characters remain
        assert "python/abc123" in result
        assert ".jpg" in result
    
    def test_mixed_template_formats(self):
        """Test templates with both simple and Jinja2 syntax."""
        template = "{subreddit}/{{ post_id }}-{{ title|slugify }}.{ext}"
        result = self.engine.render(template, self.sample_variables)
        assert result == "python/abc123-amazing-python-tutorial.jpg"
    
    def test_preset_templates(self):
        """Test predefined template presets."""
        presets = self.engine.list_presets()
        assert 'default' in presets
        assert 'date_organized' in presets
        assert 'user_organized' in presets
        
        # Test default preset
        default_template = self.engine.get_preset('default')
        assert default_template is not None
        result = self.engine.render(default_template, self.sample_variables)
        assert "python" in result and "abc123" in result
    
    def test_date_organized_preset(self):
        """Test date-organized preset."""
        template = self.engine.get_preset('date_organized')
        result = self.engine.render(template, self.sample_variables)
        assert "2024/01/15" in result
        assert "abc123.jpg" in result
    
    def test_user_organized_preset(self):
        """Test user-organized preset."""
        template = self.engine.get_preset('user_organized')
        result = self.engine.render(template, self.sample_variables)
        assert "python_guru" in result
        assert "abc123" in result
    
    def test_template_validation_valid(self):
        """Test validation of valid templates."""
        valid_templates = [
            "{{ subreddit }}/{{ post_id }}.{{ ext }}",
            "{{ date|strftime('%Y') }}/{{ post_id }}.{{ ext }}",
            "{subreddit}/{post_id}.{ext}"  # Simple format
        ]
        
        for template in valid_templates:
            errors = self.engine.validate_template(template)
            assert len(errors) == 0, f"Template should be valid: {template}"
    
    def test_template_validation_missing_extension(self):
        """Test validation fails for templates missing extension."""
        invalid_template = "{{ subreddit }}/{{ post_id }}"
        errors = self.engine.validate_template(invalid_template)
        assert len(errors) > 0
        assert any("ext" in error for error in errors)
    
    def test_template_validation_syntax_error(self):
        """Test validation catches syntax errors."""
        invalid_template = "{{ subreddit }}/{{ post_id }.{{ ext }}"  # Missing closing brace
        errors = self.engine.validate_template(invalid_template)
        assert len(errors) > 0
        assert any("syntax" in error.lower() for error in errors)
    
    def test_template_validation_dangerous_patterns(self):
        """Test validation catches potentially dangerous patterns."""
        dangerous_templates = [
            "../{{ post_id }}.{{ ext }}",  # Path traversal
            "{{ subreddit }}//{{ post_id }}.{{ ext }}",  # Double slashes
        ]
        
        for template in dangerous_templates:
            errors = self.engine.validate_template(template)
            assert len(errors) > 0, f"Template should be invalid: {template}"
    
    def test_render_with_missing_variables(self):
        """Test rendering fails gracefully with missing variables."""
        template = "{{ missing_var }}/{{ post_id }}.{{ ext }}"
        with pytest.raises(UndefinedError):
            self.engine.render(template, self.sample_variables)
    
    def test_render_with_invalid_syntax(self):
        """Test rendering fails with invalid syntax."""
        template = "{{ subreddit }/{{ post_id }}.{{ ext }}"  # Missing closing brace
        with pytest.raises(TemplateSyntaxError):
            self.engine.render(template, self.sample_variables)
    
    def test_filename_length_limiting(self):
        """Test filename length is properly limited."""
        variables = self.sample_variables.copy()
        variables['title'] = "A" * 300  # Very long title
        template = "{{ title }}.{{ ext }}"
        
        result = self.engine.render(template, variables, max_length=50)
        assert len(result) <= 50
    
    def test_filename_length_preserves_extension(self):
        """Test length limiting preserves file extension."""
        variables = self.sample_variables.copy()
        variables['title'] = "A" * 300
        template = "{{ title }}.{{ ext }}"
        
        result = self.engine.render(template, variables, max_length=20)
        assert result.endswith('.jpg')
        assert len(result) <= 20
    
    def test_fallback_filename_generation(self):
        """Test fallback filename when rendering fails."""
        # This tests the private method indirectly
        variables = self.sample_variables.copy()
        fallback = self.engine._generate_fallback_filename(variables)
        assert variables['post_id'] in fallback
        assert variables['ext'] in fallback
    
    def test_default_variables_handling(self):
        """Test handling of missing variables with defaults."""
        minimal_vars = {'post_id': 'test123'}
        template = "{{ subreddit }}/{{ post_id }}.{{ ext }}"
        
        result = self.engine.render(template, minimal_vars)
        # Should use defaults for missing variables
        assert "test123" in result
        assert result.endswith('.unknown')  # Default extension
    
    def test_variable_extraction(self):
        """Test extraction of variables from templates."""
        template = "{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}"
        variables = self.engine._extract_template_variables(template)
        
        expected_vars = {'subreddit', 'post_id', 'title', 'ext'}
        assert expected_vars.issubset(set(variables))
    
    def test_post_processing_safety(self):
        """Test post-processing makes filenames safe."""
        unsafe_filename = "test/file\\with:invalid*chars?.jpg"
        safe_filename = self.engine._post_process_filename(unsafe_filename, 100)
        
        # Should not contain unsafe characters
        unsafe_chars = ['/', '\\', ':', '*', '?']
        for char in unsafe_chars:
            assert char not in safe_filename
    
    def test_complex_template_with_all_features(self):
        """Test complex template using multiple features."""
        template = "{{ date|strftime('%Y/%m') }}/{{ subreddit }}/{{ score }}-{{ title|slugify(30) }}-{{ post_id }}.{{ ext }}"
        result = self.engine.render(template, self.sample_variables)
        
        # Verify all components are present
        assert "2024/01" in result
        assert "python" in result
        assert "150" in result
        assert "abc123" in result
        assert result.endswith('.jpg')
    
    def test_empty_template_handling(self):
        """Test handling of empty or whitespace templates."""
        empty_templates = ["", "   ", "\t\n"]
        
        for template in empty_templates:
            result = self.engine.render(template, self.sample_variables)
            assert result  # Should not be empty
            assert "unnamed" in result.lower() or "fallback" in result.lower()


class TestTemplateEngineIntegration:
    """Integration tests for template engine with various scenarios."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.engine = FilenameTemplateEngine()
    
    def test_real_world_post_data(self):
        """Test with realistic Reddit post data."""
        post_data = {
            'subreddit': 'MachineLearning',
            'post_id': 'ml_abc123',
            'title': '[D] How to improve deep learning model accuracy? Looking for tips!',
            'author': 'data_scientist_2024',
            'date': '2024-01-15T14:22:33Z',
            'ext': 'pdf',
            'content_type': 'document',
            'score': 89,
            'url': 'https://arxiv.org/paper.pdf',
            'is_video': False
        }
        
        template = "{{ subreddit }}/{{ date|strftime('%Y-%m') }}/{{ score }}-{{ title|slugify(40) }}.{{ ext }}"
        result = self.engine.render(template, post_data)
        
        expected_parts = ["MachineLearning", "2024-01", "89", ".pdf"]
        for part in expected_parts:
            assert part in result
    
    def test_video_post_template(self):
        """Test template for video posts."""
        video_data = {
            'subreddit': 'videos',
            'post_id': 'vid123',
            'title': 'Amazing Cat Video!!!',
            'author': 'cat_lover',
            'date': '2024-01-15T09:15:22Z',
            'ext': 'mp4',
            'content_type': 'video',
            'is_video': True,
            'score': 2500
        }
        
        template = "{{ content_type }}/{{ subreddit }}/{{ title|slugify }}.{{ ext }}"
        result = self.engine.render(template, video_data)
        
        assert result == "video/videos/amazing-cat-video.mp4"
    
    def test_backward_compatibility_scenarios(self):
        """Test various backward compatibility scenarios."""
        old_style_templates = [
            "{subreddit}/{post_id}.{ext}",
            "{date}_{post_id}_{title}.{ext}",
            "archive/{subreddit}_{author}/{post_id}.{ext}"
        ]
        
        sample_data = {
            'subreddit': 'test',
            'post_id': '123',
            'title': 'Test Post',
            'author': 'user',
            'date': '2024-01-15',
            'ext': 'jpg'
        }
        
        for template in old_style_templates:
            result = self.engine.render(template, sample_data)
            # Basic verification that conversion worked
            assert sample_data['post_id'] in result
            assert sample_data['ext'] in result
    
    def test_error_recovery_scenarios(self):
        """Test error recovery in various failure scenarios."""
        problematic_data = {
            'post_id': 'test123',
            'ext': 'jpg',
            # Missing other common variables
        }
        
        templates = [
            "{{ missing_var }}/{{ post_id }}.{{ ext }}",  # Missing variable
            "{{ subreddit }/{{ post_id }}.{{ ext }}",     # Syntax error
        ]
        
        for template in templates:
            try:
                result = self.engine.render(template, problematic_data)
                # If no exception, should get fallback filename
                assert "test123" in result
                assert "jpg" in result
            except (UndefinedError, TemplateSyntaxError):
                # Expected for some cases
                pass