"""
Tests for Text Content Handler

Tests the TextContentHandler functionality including selftext saving as Markdown,
YAML frontmatter creation, Unicode handling, and JSON sidecar generation.
"""

import pytest
import json
import yaml
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from typing import Dict, Any

from redditdl.content_handlers.text import TextContentHandler
from redditdl.content_handlers.base import HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.core.templates import FilenameTemplateEngine


class TestTextContentHandler:
    """Test TextContentHandler functionality."""
    
    @pytest.fixture
    def handler(self):
        """Create a TextContentHandler instance."""
        return TextContentHandler(priority=60)
    
    @pytest.fixture
    def text_post(self):
        """Create a sample text post."""
        return PostMetadata(
            id="text123",
            title="This is a great text post about programming",
            author="text_user",
            subreddit="programming",
            url="https://reddit.com/r/programming/comments/text123",
            selftext="# Introduction\n\nThis is a comprehensive text post about Python programming.\n\n## Key Points\n\n- Python is versatile\n- Easy to learn\n- Great community\n\n**Thank you for reading!**",
            date_iso="2023-06-15T10:30:00Z",
            is_self=True,
            score=1250,
            num_comments=89,
            is_nsfw=False,
            spoiler=False,
            locked=False,
            archived=False,
            edited=False,
            created_utc=1687000000.0
        )
    
    @pytest.fixture
    def unicode_text_post(self):
        """Create a text post with Unicode content."""
        return PostMetadata(
            id="unicode123",
            title="Unicode Test: 中文 • Русский • العربية",
            author="unicode_user",
            subreddit="internationalization",
            url="https://reddit.com/comments/unicode123",
            selftext="This post contains Unicode characters:\n\n- Chinese: 你好世界\n- Russian: Привет мир\n- Arabic: مرحبا بالعالم\n- Emoji: 🚀🎉💻\n\n## Mathematical symbols\n\n∀x ∈ ℝ: x² ≥ 0\n\nα + β = γ",
            date_iso="2023-06-15T11:00:00Z",
            is_self=True,
            score=567,
            num_comments=23
        )
    
    @pytest.fixture
    def empty_text_post(self):
        """Create a text post with no selftext."""
        return PostMetadata(
            id="empty_text",
            title="Empty Text Post",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/empty_text",
            selftext="",
            date_iso="2023-06-15T10:30:00Z",
            is_self=True
        )
    
    @pytest.fixture
    def non_self_post(self):
        """Create a non-self post (link post)."""
        return PostMetadata(
            id="link123",
            title="Link Post",
            author="user",
            subreddit="test", 
            url="https://example.com/article",
            selftext="",
            date_iso="2023-06-15T10:30:00Z",
            is_self=False
        )
    
    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create a temporary output directory."""
        return tmp_path / "text_test"
    
    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return {
            'create_sidecars': True,
            'include_preview': True,
            'max_filename_length': 200
        }

    def test_supported_content_types(self, handler):
        """Test that handler supports text content type."""
        assert 'text' in handler.supported_content_types
        assert len(handler.supported_content_types) == 1

    def test_can_handle_text_post(self, handler, text_post):
        """Test that handler can handle text posts."""
        assert handler.can_handle(text_post, 'text')

    def test_can_handle_non_text_post(self, handler):
        """Test that handler rejects non-text posts."""
        image_post = PostMetadata(
            id="image123",
            title="Image Post",
            author="user",
            subreddit="pics",
            url="https://i.redd.it/example.jpg",
            date_iso="2023-06-15T10:30:00Z"
        )
        assert not handler.can_handle(image_post, 'image')
        assert not handler.can_handle(image_post, 'text')

    def test_can_handle_non_self_post(self, handler, non_self_post):
        """Test that handler rejects non-self posts."""
        assert not handler.can_handle(non_self_post, 'text')

    def test_can_handle_empty_text_post(self, handler, empty_text_post):
        """Test that handler rejects posts with no selftext."""
        assert not handler.can_handle(empty_text_post, 'text')

    @pytest.mark.asyncio
    async def test_process_text_success(self, handler, text_post, temp_output_dir, config):
        """Test successful text post processing."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(text_post, temp_output_dir, config)
        
        # Verify result
        assert result.success
        assert result.handler_name == "text"
        assert result.content_type == "text"
        assert "text_save" in result.operations_performed
        assert "sidecar_creation" in result.operations_performed
        assert result.sidecar_created
        
        # Should have created 2 files: Markdown and JSON sidecar
        assert len(result.files_created) == 2
        
        # Verify Markdown file was created
        md_files = [f for f in result.files_created if f.suffix == '.md']
        assert len(md_files) == 1
        
        md_file = md_files[0]
        assert md_file.exists()
        
        # Verify Markdown content
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Should have YAML frontmatter
        assert content.startswith('---\n')
        assert '---\n\n# This is a great text post about programming' in content
        assert 'This is a comprehensive text post about Python programming.' in content
        
        # Parse YAML frontmatter
        yaml_end = content.find('---\n', 4)
        yaml_content = content[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        
        assert metadata['title'] == text_post.title
        assert metadata['author'] == text_post.author
        assert metadata['subreddit'] == text_post.subreddit
        assert metadata['id'] == text_post.id
        assert metadata['score'] == 1250
        assert metadata['num_comments'] == 89

    @pytest.mark.asyncio
    async def test_process_unicode_text(self, handler, unicode_text_post, temp_output_dir, config):
        """Test processing text with Unicode characters."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(unicode_text_post, temp_output_dir, config)
        
        assert result.success
        
        # Verify Unicode content is preserved
        md_file = [f for f in result.files_created if f.suffix == '.md'][0]
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert '你好世界' in content
        assert 'Привет мир' in content
        assert 'مرحبا بالعالم' in content
        assert '🚀🎉💻' in content
        assert '∀x ∈ ℝ: x² ≥ 0' in content

    @pytest.mark.asyncio
    async def test_process_without_sidecars(self, handler, text_post, temp_output_dir):
        """Test text processing without JSON sidecars."""
        config = {
            'create_sidecars': False,
            'include_preview': True
        }
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(text_post, temp_output_dir, config)
        
        assert result.success
        assert "text_save" in result.operations_performed
        assert "sidecar_creation" not in result.operations_performed
        assert not result.sidecar_created
        
        # Should have only 1 file: Markdown (no sidecar)
        assert len(result.files_created) == 1
        assert result.files_created[0].suffix == '.md'

    @pytest.mark.asyncio
    async def test_process_without_preview(self, handler, text_post, temp_output_dir):
        """Test text processing without preview in metadata."""
        config = {
            'create_sidecars': False,
            'include_preview': False
        }
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(text_post, temp_output_dir, config)
        
        assert result.success
        
        # Verify no preview in YAML frontmatter
        md_file = result.files_created[0]
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        yaml_end = content.find('---\n', 4)
        yaml_content = content[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        
        assert 'preview' not in metadata

    def test_construct_filename_default(self, handler, text_post, config):
        """Test default filename construction."""
        filename = handler._construct_filename(text_post, config)
        
        assert filename.endswith('.md')
        assert 'text123' in filename
        assert 'This_is_a_great_text_post_about_programming' in filename or 'This_is_a_great' in filename
        assert '2023-06-15T10_30_00Z' in filename

    def test_construct_filename_with_template(self, handler, text_post):
        """Test filename construction with custom template."""
        config = {
            'filename_template': '{{ subreddit }}_{{ post_id }}_{{ author }}.{{ ext }}'
        }
        
        # Mock template engine
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'programming_text123_text_user.md'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._construct_filename(text_post, config)
        
        assert filename == 'programming_text123_text_user.md'

    def test_apply_template_success(self, handler, text_post, config):
        """Test successful template rendering."""
        template = '{{ date }}/{{ subreddit }}/{{ post_id }}.{{ ext }}'
        
        # Mock template engine
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = '2023-06-15T10:30:00Z/programming/text123.md'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(template, text_post, config)
        
        assert filename == '2023-06-15T10:30:00Z/programming/text123.md'

    def test_apply_template_failure_fallback(self, handler, text_post, config):
        """Test template rendering failure with fallback."""
        template = '{{ invalid_template }}'
        
        # Mock template engine to raise exception
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.side_effect = Exception("Template error")
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(template, text_post, config)
        
        # Should fallback to default filename
        assert filename.endswith('.md')
        assert 'text123' in filename

    def test_create_markdown_content_with_all_fields(self, handler, text_post, config):
        """Test Markdown content creation with all metadata fields."""
        markdown = handler._create_markdown_content(text_post, config)
        
        # Should start with YAML frontmatter
        assert markdown.startswith('---\n')
        
        # Parse YAML frontmatter
        yaml_end = markdown.find('---\n', 4)
        yaml_content = markdown[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        
        # Verify all fields
        assert metadata['title'] == text_post.title
        assert metadata['author'] == text_post.author
        assert metadata['subreddit'] == text_post.subreddit
        assert metadata['url'] == text_post.url
        assert metadata['id'] == text_post.id
        assert metadata['date'] == text_post.date_iso
        assert metadata['score'] == 1250
        assert metadata['num_comments'] == 89
        assert metadata['nsfw'] == False
        assert metadata['spoiler'] == False
        assert metadata['locked'] == False
        assert metadata['archived'] == False
        assert metadata['edited'] == False
        assert metadata['created_utc'] == 1687000000.0
        
        # Should include preview
        assert 'preview' in metadata
        assert metadata['preview'].startswith('# Introduction')
        assert len(metadata['preview']) <= 203  # 200 chars + "..."
        
        # Verify content after frontmatter
        content_start = markdown.find('# This is a great text post about programming')
        assert content_start > 0
        assert '# Introduction' in markdown[content_start:]

    def test_create_markdown_content_minimal_fields(self, handler, config):
        """Test Markdown content creation with minimal metadata fields."""
        minimal_post = PostMetadata(
            id="minimal123",
            title="Minimal Post",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/minimal123",
            selftext="Simple content",
            date_iso="2023-06-15T10:30:00Z",
            is_self=True
        )
        
        markdown = handler._create_markdown_content(minimal_post, config)
        
        # Parse YAML frontmatter
        yaml_end = markdown.find('---\n', 4)
        yaml_content = markdown[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        
        # Should have basic fields
        assert metadata['title'] == "Minimal Post"
        assert metadata['author'] == "user"
        assert metadata['id'] == "minimal123"
        
        # Should not have optional fields that weren't set
        assert 'score' not in metadata
        assert 'num_comments' not in metadata

    def test_create_markdown_content_long_text_preview(self, handler, config):
        """Test preview truncation for long text content."""
        long_text = "A" * 300  # Text longer than 200 chars
        
        long_post = PostMetadata(
            id="long123",
            title="Long Post",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/long123",
            selftext=long_text,
            date_iso="2023-06-15T10:30:00Z",
            is_self=True
        )
        
        markdown = handler._create_markdown_content(long_post, config)
        
        # Parse YAML frontmatter
        yaml_end = markdown.find('---\n', 4)
        yaml_content = markdown[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        
        # Preview should be truncated
        assert 'preview' in metadata
        assert len(metadata['preview']) == 203  # 200 chars + "..."
        assert metadata['preview'].endswith('...')

    def test_create_json_sidecar(self, handler, text_post, temp_output_dir):
        """Test JSON sidecar creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        md_path = temp_output_dir / "test.md"
        
        sidecar_path = handler._create_json_sidecar(text_post, md_path)
        
        assert sidecar_path is not None
        assert sidecar_path.exists()
        assert sidecar_path.suffix == '.json'
        
        # Verify sidecar content
        with open(sidecar_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert data['id'] == "text123"
        assert data['title'] == text_post.title
        assert data['author'] == text_post.author
        assert data['selftext'] == text_post.selftext

    def test_create_json_sidecar_error_handling(self, handler, text_post, temp_output_dir):
        """Test error handling in JSON sidecar creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        md_path = temp_output_dir / "test.md"
        
        # Mock file operations to raise an exception
        with patch('builtins.open', side_effect=PermissionError("Cannot write file")):
            sidecar_path = handler._create_json_sidecar(text_post, md_path)
        
        # Should return None on error
        assert sidecar_path is None

    def test_validate_config_valid(self, handler):
        """Test configuration validation with valid config."""
        valid_config = {
            'create_sidecars': True,
            'include_preview': False
        }
        errors = handler.validate_config(valid_config)
        assert errors == []

    def test_validate_config_invalid_sidecars(self, handler):
        """Test configuration validation with invalid create_sidecars."""
        invalid_config = {
            'create_sidecars': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "create_sidecars must be a boolean" in errors[0]

    def test_validate_config_invalid_preview(self, handler):
        """Test configuration validation with invalid include_preview."""
        invalid_config = {
            'include_preview': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "include_preview must be a boolean" in errors[0]

    @pytest.mark.asyncio
    async def test_process_exception_handling(self, handler, text_post, temp_output_dir, config):
        """Test exception handling during processing."""
        # Make output directory non-writable to trigger an exception
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Mock file creation to raise an exception
        with patch('builtins.open', side_effect=PermissionError("No write permission")):
            with pytest.raises(HandlerError, match="Text processing failed"):
                await handler.process(text_post, temp_output_dir, config)

    def test_handler_properties(self, handler):
        """Test handler basic properties."""
        assert handler.name == "text"
        assert handler.priority == 60
        assert isinstance(handler.supported_content_types, set)

    def test_filename_extension_handling(self, handler, text_post):
        """Test that .md extension is properly handled in templates."""
        # Test template without extension
        config = {'filename_template': '{{ post_id }}_text'}
        
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'text123_text'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(config['filename_template'], text_post, config)
        
        assert filename == 'text123_text.md'
        
        # Test template with different extension
        mock_engine.render.return_value = 'text123_text.txt'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(config['filename_template'], text_post, config)
        
        assert filename == 'text123_text.md'  # Should replace with .md

    def test_yaml_frontmatter_special_characters(self, handler, config):
        """Test YAML frontmatter handling with special characters."""
        special_post = PostMetadata(
            id="special123",
            title="Post with: colons, \"quotes\", and 'apostrophes'",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/special123",
            selftext="Content with special chars: {}[]()!@#$%^&*",
            date_iso="2023-06-15T10:30:00Z",
            is_self=True
        )
        
        markdown = handler._create_markdown_content(special_post, config)
        
        # Should be valid YAML despite special characters
        yaml_end = markdown.find('---\n', 4)
        yaml_content = markdown[4:yaml_end]
        
        # Should not raise exception
        metadata = yaml.safe_load(yaml_content)
        assert metadata['title'] == special_post.title

    @pytest.mark.asyncio
    async def test_process_minimal_config(self, handler, text_post, temp_output_dir):
        """Test processing with minimal configuration."""
        config = {}  # Empty config - should use defaults
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(text_post, temp_output_dir, config)
        
        assert result.success
        assert "text_save" in result.operations_performed
        # Should not create sidecars by default (create_sidecars defaults to False)
        assert "sidecar_creation" not in result.operations_performed
        # Should include preview by default (include_preview defaults to True)
        
        # Verify preview is included in frontmatter
        md_file = result.files_created[0]
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        yaml_end = content.find('---\n', 4)
        yaml_content = content[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        assert 'preview' in metadata

    def test_markdown_structure_validity(self, handler, text_post, config):
        """Test that generated Markdown has proper structure."""
        markdown = handler._create_markdown_content(text_post, config)
        
        lines = markdown.split('\n')
        
        # Should start with YAML frontmatter
        assert lines[0] == '---'
        
        # Find end of frontmatter
        yaml_end_line = -1
        for i, line in enumerate(lines[1:], 1):
            if line == '---':
                yaml_end_line = i
                break
        
        assert yaml_end_line > 0
        
        # Should have empty line after frontmatter
        assert lines[yaml_end_line + 1] == ''
        
        # Should have title heading
        assert lines[yaml_end_line + 2] == f'# {text_post.title}'
        
        # Should have empty line before content
        assert lines[yaml_end_line + 3] == ''
        
        # Content should start after that
        assert lines[yaml_end_line + 4].startswith('# Introduction')

    def test_empty_selftext_edge_case(self, handler, config):
        """Test handling of posts with empty or None selftext."""
        empty_post = PostMetadata(
            id="empty123",
            title="Empty Content",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/empty123",
            selftext=None,  # None instead of empty string
            date_iso="2023-06-15T10:30:00Z",
            is_self=True
        )
        
        markdown = handler._create_markdown_content(empty_post, config)
        
        # Should handle None selftext gracefully
        assert markdown.endswith('# Empty Content\n\n\n')
        
        # Preview should be empty
        yaml_end = markdown.find('---\n', 4)
        yaml_content = markdown[4:yaml_end]
        metadata = yaml.safe_load(yaml_content)
        assert metadata['preview'] == ""