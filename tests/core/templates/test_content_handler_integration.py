"""
Tests for content handler integration with filename templates

Tests that content handlers properly use the FilenameTemplateEngine
and handle template rendering correctly.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from redditdl.content_handlers.media import MediaContentHandler
from redditdl.content_handlers.text import TextContentHandler
from redditdl.content_handlers.poll import PollContentHandler
from redditdl.scrapers import PostMetadata


class TestContentHandlerTemplateIntegration:
    """Test content handlers use templates correctly."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.sample_post_data = {
            'id': 'test123',
            'title': 'Test Post Title',
            'subreddit': 'python',
            'author': 'test_user',
            'url': 'https://example.com/image.jpg',
            'created_utc': 1705316200,
            'selftext': '',
            'permalink': '/r/python/comments/test123'
        }
        self.post = PostMetadata(self.sample_post_data)
        
        self.config = {
            'filename_template': '{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}',
            'max_filename_length': 200
        }
    
    def test_media_handler_template_rendering(self):
        """Test MediaContentHandler uses templates correctly."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/image.jpg'
        
        filename = handler._construct_filename(self.post, media_url, self.config)
        
        expected_parts = ['python', 'test123', 'test-post-title', '.jpg']
        for part in expected_parts:
            assert part in filename
    
    def test_media_handler_fallback_on_template_error(self):
        """Test MediaContentHandler falls back gracefully on template errors."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/image.jpg'
        
        # Use invalid template
        bad_config = self.config.copy()
        bad_config['filename_template'] = '{{ invalid_syntax }'
        
        # Should not raise exception, should fallback to default
        filename = handler._construct_filename(self.post, media_url, bad_config)
        assert filename  # Should have some filename
        assert self.post.id in filename
    
    def test_text_handler_template_rendering(self):
        """Test TextContentHandler uses templates correctly."""
        handler = TextContentHandler()
        
        filename = handler._construct_filename(self.post, self.config)
        
        expected_parts = ['python', 'test123', 'test-post-title', '.md']
        for part in expected_parts:
            assert part in filename
    
    def test_text_handler_enforces_md_extension(self):
        """Test TextContentHandler enforces .md extension."""
        handler = TextContentHandler()
        
        # Template that might specify different extension
        config_with_wrong_ext = self.config.copy()
        config_with_wrong_ext['filename_template'] = '{{ subreddit }}/{{ post_id }}.txt'
        
        filename = handler._construct_filename(self.post, config_with_wrong_ext)
        assert filename.endswith('.md')
    
    def test_poll_handler_template_rendering(self):
        """Test PollContentHandler uses templates correctly."""
        handler = PollContentHandler()
        
        filename = handler._construct_filename(self.post, self.config)
        
        expected_parts = ['python', 'test123', 'test-post-title', '.json']
        for part in expected_parts:
            assert part in filename
    
    def test_poll_handler_enforces_json_extension(self):
        """Test PollContentHandler enforces .json extension."""
        handler = PollContentHandler()
        
        # Template with different extension
        config_with_wrong_ext = self.config.copy()
        config_with_wrong_ext['filename_template'] = '{{ subreddit }}/{{ post_id }}.xml'
        
        filename = handler._construct_filename(self.post, config_with_wrong_ext)
        assert filename.endswith('.json')
    
    def test_handler_template_variables_preparation(self):
        """Test handlers prepare template variables correctly."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/video.mp4'
        
        template_vars = handler._prepare_template_variables(self.post, media_url, self.config)
        
        # Check required variables are present
        required_vars = ['subreddit', 'post_id', 'title', 'author', 'date', 'ext', 'content_type']
        for var in required_vars:
            assert var in template_vars
        
        # Check content type detection
        assert template_vars['content_type'] == 'video'
        assert template_vars['ext'] == 'mp4'
    
    def test_handler_without_template_uses_default(self):
        """Test handlers use default filename when no template provided."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/image.jpg'
        
        # Config without template
        config_no_template = {'max_filename_length': 200}
        
        filename = handler._construct_filename(self.post, media_url, config_no_template)
        
        # Should use default filename pattern
        assert self.post.id in filename
        assert filename.endswith('.jpg')
    
    def test_template_engine_initialization_lazy(self):
        """Test template engines are initialized lazily."""
        handler = MediaContentHandler()
        
        # Initially no template engine
        assert handler._template_engine is None
        
        # After using template, should be initialized
        handler._construct_filename(self.post, 'https://example.com/test.jpg', self.config)
        assert handler._template_engine is not None
    
    def test_template_variable_content_type_detection(self):
        """Test content type detection for different media types."""
        handler = MediaContentHandler()
        
        test_cases = [
            ('image.jpg', 'image'),
            ('video.mp4', 'video'),
            ('audio.mp3', 'audio'),
            ('document.pdf', 'image'),  # Default fallback
        ]
        
        for url, expected_type in test_cases:
            template_vars = handler._prepare_template_variables(
                self.post, f'https://example.com/{url}', self.config
            )
            assert template_vars['content_type'] == expected_type
    
    def test_complex_template_with_all_variables(self):
        """Test complex template using all available variables."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/test.jpg'
        
        complex_config = {
            'filename_template': (
                '{{ subreddit }}/{{ date|strftime("%Y-%m") }}/'
                '{{ content_type }}/{{ author }}/{{ post_id }}-{{ title|slugify(20) }}.{{ ext }}'
            ),
            'max_filename_length': 500
        }
        
        filename = handler._construct_filename(self.post, media_url, complex_config)
        
        # Verify all components are present
        expected_parts = ['python', '2024-01', 'image', 'test_user', 'test123']
        for part in expected_parts:
            assert part in filename
        
        assert filename.endswith('.jpg')
    
    def test_template_backward_compatibility(self):
        """Test backward compatibility with simple {variable} templates."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/test.jpg'
        
        # Old-style template
        old_style_config = {
            'filename_template': '{subreddit}/{post_id}-{title}.{ext}',
            'max_filename_length': 200
        }
        
        filename = handler._construct_filename(self.post, media_url, old_style_config)
        
        # Should work correctly
        assert 'python' in filename
        assert 'test123' in filename
        assert filename.endswith('.jpg')
    
    def test_template_max_length_enforcement(self):
        """Test template engines respect max filename length."""
        handler = MediaContentHandler()
        media_url = 'https://example.com/test.jpg'
        
        # Very long title
        long_post_data = self.sample_post_data.copy()
        long_post_data['title'] = 'A' * 300
        long_post = PostMetadata(long_post_data)
        
        config_short_limit = {
            'filename_template': '{{ title }}.{{ ext }}',
            'max_filename_length': 50
        }
        
        filename = handler._construct_filename(long_post, media_url, config_short_limit)
        assert len(filename) <= 50
        assert filename.endswith('.jpg')  # Extension should be preserved
    
    def test_template_extension_handling(self):
        """Test different handlers handle extensions correctly."""
        handlers_and_extensions = [
            (MediaContentHandler(), 'jpg'),
            (TextContentHandler(), 'md'),
            (PollContentHandler(), 'json'),
        ]
        
        for handler, expected_ext in handlers_and_extensions:
            if hasattr(handler, '_construct_filename'):
                if isinstance(handler, MediaContentHandler):
                    filename = handler._construct_filename(
                        self.post, 'https://example.com/test.jpg', self.config
                    )
                else:
                    filename = handler._construct_filename(self.post, self.config)
                
                assert filename.endswith(f'.{expected_ext}')
    
    @patch('core.templates.filename.FilenameTemplateEngine.render')
    def test_template_engine_error_handling(self, mock_render):
        """Test error handling when template engine fails."""
        mock_render.side_effect = Exception("Template error")
        
        handler = MediaContentHandler()
        media_url = 'https://example.com/test.jpg'
        
        # Should not raise exception, should fall back
        filename = handler._construct_filename(self.post, media_url, self.config)
        
        # Should get default filename
        assert filename
        assert self.post.id in filename
        assert filename.endswith('.jpg')
    
    def test_template_variable_sanitization(self):
        """Test template variables are properly sanitized."""
        # Post with problematic characters
        problem_post_data = self.sample_post_data.copy()
        problem_post_data['title'] = 'Title/with\\bad:chars*?.ext'
        problem_post_data['author'] = 'user|with<bad>chars'
        problem_post = PostMetadata(problem_post_data)
        
        handler = MediaContentHandler()
        media_url = 'https://example.com/test.jpg'
        
        filename = handler._construct_filename(problem_post, media_url, self.config)
        
        # Should not contain problematic characters
        problematic_chars = ['/', '\\', ':', '*', '?', '|', '<', '>']
        for char in problematic_chars:
            assert char not in filename
    
    def test_different_content_types_use_correct_extensions(self):
        """Test different content handlers enforce their extensions."""
        test_data = [
            (MediaContentHandler(), 'https://example.com/test.png', 'png'),
            (TextContentHandler(), None, 'md'),
            (PollContentHandler(), None, 'json'),
        ]
        
        for handler, media_url, expected_ext in test_data:
            if media_url:
                filename = handler._construct_filename(self.post, media_url, self.config)
            else:
                filename = handler._construct_filename(self.post, self.config)
            
            assert filename.endswith(f'.{expected_ext}')