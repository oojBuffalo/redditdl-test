"""
Tests for Crosspost Content Handler

Tests the CrosspostContentHandler functionality including relationship tracking,
duplicate detection, circular reference prevention, and metadata preservation.
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from typing import Dict, Any

from redditdl.content_handlers.crosspost import CrosspostContentHandler
from redditdl.content_handlers.base import HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.core.templates import FilenameTemplateEngine


class TestCrosspostContentHandler:
    """Test CrosspostContentHandler functionality."""
    
    @pytest.fixture
    def handler(self):
        """Create a CrosspostContentHandler instance."""
        return CrosspostContentHandler(priority=30)
    
    @pytest.fixture
    def crosspost_post(self):
        """Create a sample crosspost."""
        return PostMetadata(
            id="crosspost123",
            title="Crosspost from r/originalsubreddit",
            author="crosspost_user",
            subreddit="secondsubreddit",
            url="https://reddit.com/r/secondsubreddit/comments/crosspost123",
            date_iso="2023-06-15T10:30:00Z",
            crosspost_parent_id="original456",
            score=234,
            num_comments=18,
            is_nsfw=False
        )
    
    @pytest.fixture
    def crosspost_no_parent(self):
        """Create a crosspost without parent ID."""
        return PostMetadata(
            id="no_parent",
            title="Crosspost without parent",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/no_parent",
            date_iso="2023-06-15T10:30:00Z",
            crosspost_parent_id=None
        )
    
    @pytest.fixture
    def circular_crosspost(self):
        """Create a crosspost that would create circular reference."""
        return PostMetadata(
            id="circular123",
            title="Circular Crosspost",
            author="circular_user",
            subreddit="circulartest",
            url="https://reddit.com/comments/circular123",
            date_iso="2023-06-15T10:30:00Z",
            crosspost_parent_id="original789"
        )
    
    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create a temporary output directory."""
        return tmp_path / "crosspost_test"
    
    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return {
            'create_relationships': True,
            'create_summary': True,
            'max_filename_length': 200
        }

    def test_supported_content_types(self, handler):
        """Test that handler supports crosspost content type."""
        assert 'crosspost' in handler.supported_content_types
        assert len(handler.supported_content_types) == 1

    def test_can_handle_crosspost_post(self, handler, crosspost_post):
        """Test that handler can handle crosspost posts."""
        assert handler.can_handle(crosspost_post, 'crosspost')

    def test_can_handle_non_crosspost_post(self, handler):
        """Test that handler rejects non-crosspost posts."""
        text_post = PostMetadata(
            id="text123",
            title="Text Post",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/text123",
            selftext="This is text content",
            date_iso="2023-06-15T10:30:00Z"
        )
        assert not handler.can_handle(text_post, 'text')
        assert not handler.can_handle(text_post, 'crosspost')

    def test_can_handle_crosspost_no_parent(self, handler, crosspost_no_parent):
        """Test that handler rejects crossposts with no parent ID."""
        assert not handler.can_handle(crosspost_no_parent, 'crosspost')

    @pytest.mark.asyncio
    async def test_process_crosspost_success(self, handler, crosspost_post, temp_output_dir, config):
        """Test successful crosspost processing."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(crosspost_post, temp_output_dir, config)
        
        # Verify result
        assert result.success
        assert result.handler_name == "crosspost"
        assert result.content_type == "crosspost"
        assert "crosspost_tracking" in result.operations_performed
        assert "relationship_mapping" in result.operations_performed
        assert "crosspost_summary" in result.operations_performed
        
        # Should have created 3 files: metadata JSON, relationships JSON, summary MD
        assert len(result.files_created) == 3
        
        # Verify metadata file was created
        json_files = [f for f in result.files_created if f.suffix == '.json' and 'relationships' not in f.name]
        assert len(json_files) == 1
        
        metadata_file = json_files[0]
        assert metadata_file.exists()
        
        # Verify metadata content
        with open(metadata_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert 'crosspost_metadata' in data
        assert 'post_data' in data
        assert 'relationships' in data
        assert data['crosspost_metadata']['crosspost_id'] == "crosspost123"
        assert data['crosspost_metadata']['original_post_id'] == "original456"
        assert data['relationships']['parent_id'] == "original456"

    @pytest.mark.asyncio
    async def test_process_crosspost_no_parent_error(self, handler, crosspost_no_parent, temp_output_dir, config):
        """Test that crossposts without parent ID raise an error."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        with pytest.raises(HandlerError, match="No crosspost parent ID found"):
            await handler.process(crosspost_no_parent, temp_output_dir, config)

    @pytest.mark.asyncio
    async def test_process_circular_crosspost(self, handler, circular_crosspost, temp_output_dir, config):
        """Test circular crosspost detection."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        # First, add the post ID to processed set to simulate circular reference
        handler._processed_crossposts.add("circular123")
        
        result = await handler.process(circular_crosspost, temp_output_dir, config)
        
        # Should succeed but skip processing
        assert result.success
        assert "circular_detection" in result.operations_performed
        assert "crosspost_tracking" not in result.operations_performed

    @pytest.mark.asyncio
    async def test_process_without_relationships(self, handler, crosspost_post, temp_output_dir):
        """Test crosspost processing without relationships."""
        config = {
            'create_relationships': False,
            'create_summary': True
        }
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(crosspost_post, temp_output_dir, config)
        
        assert result.success
        assert "crosspost_tracking" in result.operations_performed
        assert "crosspost_summary" in result.operations_performed
        assert "relationship_mapping" not in result.operations_performed
        
        # Should have 2 files: metadata and summary (no relationships)
        assert len(result.files_created) == 2

    @pytest.mark.asyncio
    async def test_process_without_summary(self, handler, crosspost_post, temp_output_dir):
        """Test crosspost processing without summary."""
        config = {
            'create_relationships': True,
            'create_summary': False
        }
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(crosspost_post, temp_output_dir, config)
        
        assert result.success
        assert "crosspost_tracking" in result.operations_performed
        assert "relationship_mapping" in result.operations_performed
        assert "crosspost_summary" not in result.operations_performed
        
        # Should have 2 files: metadata and relationships (no summary)
        assert len(result.files_created) == 2

    def test_construct_metadata_filename_default(self, handler, crosspost_post, config):
        """Test default metadata filename construction."""
        filename = handler._construct_metadata_filename(crosspost_post, config)
        
        assert filename.endswith('.json')
        assert 'crosspost123' in filename
        assert 'Crosspost_from_r_originalsubreddit' in filename or 'Crosspost' in filename
        assert '2023-06-15T10_30_00Z' in filename

    def test_construct_metadata_filename_with_template(self, handler, crosspost_post):
        """Test metadata filename construction with custom template."""
        config = {
            'filename_template': '{{ subreddit }}_{{ post_id }}_crosspost.{{ ext }}'
        }
        
        # Mock template engine
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'secondsubreddit_crosspost123_crosspost.json'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._construct_metadata_filename(crosspost_post, config)
        
        assert filename == 'secondsubreddit_crosspost123_crosspost.json'

    def test_apply_template_success(self, handler, crosspost_post, config):
        """Test successful template rendering."""
        template = '{{ subreddit }}/{{ post_id }}_{{ crosspost_parent_id }}.{{ ext }}'
        
        # Mock template engine
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'secondsubreddit/crosspost123_original456.json'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(template, crosspost_post, config)
        
        assert filename == 'secondsubreddit/crosspost123_original456.json'

    def test_apply_template_failure_fallback(self, handler, crosspost_post, config):
        """Test template rendering failure with fallback."""
        template = '{{ invalid_template }}'
        
        # Mock template engine to raise exception
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.side_effect = Exception("Template error")
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(template, crosspost_post, config)
        
        # Should fallback to default filename
        assert filename.endswith('.json')
        assert 'crosspost123' in filename

    def test_create_crosspost_metadata(self, handler, crosspost_post, config):
        """Test creation of crosspost metadata."""
        metadata = handler._create_crosspost_metadata(crosspost_post, config)
        
        # Verify structure
        assert 'crosspost_metadata' in metadata
        assert 'post_data' in metadata
        assert 'relationships' in metadata
        
        # Verify crosspost metadata
        cp_meta = metadata['crosspost_metadata']
        assert cp_meta['crosspost_id'] == "crosspost123"
        assert cp_meta['original_post_id'] == "original456"
        assert cp_meta['crosspost_subreddit'] == "secondsubreddit"
        assert cp_meta['crosspost_author'] == "crosspost_user"
        assert cp_meta['handler'] == "crosspost"
        assert cp_meta['score'] == 234
        assert cp_meta['comments'] == 18
        
        # Verify relationships
        relationships = metadata['relationships']
        assert relationships['parent_id'] == "original456"
        assert relationships['relationship_type'] == "crosspost"
        assert relationships['chain_depth'] == 1
        assert not relationships['is_duplicate']  # Should be False initially

    def test_calculate_chain_depth(self, handler, crosspost_post):
        """Test chain depth calculation."""
        # Current implementation returns 1
        depth = handler._calculate_chain_depth(crosspost_post)
        assert depth == 1

    def test_is_circular_crosspost_not_processed(self, handler, crosspost_post, config):
        """Test circular detection for unprocessed post."""
        # Post not in processed set
        assert not handler._is_circular_crosspost(crosspost_post, config)

    def test_is_circular_crosspost_already_processed(self, handler, crosspost_post, config):
        """Test circular detection for already processed post."""
        # Add post to processed set
        handler._processed_crossposts.add("crosspost123")
        
        assert handler._is_circular_crosspost(crosspost_post, config)

    def test_create_relationship_file_new(self, handler, crosspost_post, temp_output_dir, config):
        """Test creation of new relationship file."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create crosspost metadata
        crosspost_data = handler._create_crosspost_metadata(crosspost_post, config)
        
        rel_path = handler._create_relationship_file(crosspost_data, temp_output_dir, config)
        
        assert rel_path is not None
        assert rel_path.exists()
        assert rel_path.name == "crosspost_relationships.json"
        
        # Verify relationship content
        with open(rel_path, 'r', encoding='utf-8') as f:
            relationships = json.load(f)
        
        assert "crosspost123" in relationships
        rel_data = relationships["crosspost123"]
        assert rel_data['parent_id'] == "original456"
        assert rel_data['subreddit'] == "secondsubreddit"
        assert rel_data['author'] == "crosspost_user"
        assert rel_data['chain_depth'] == 1

    def test_create_relationship_file_existing(self, handler, crosspost_post, temp_output_dir, config):
        """Test updating existing relationship file."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        rel_path = temp_output_dir / "crosspost_relationships.json"
        
        # Create existing relationships file
        existing_data = {
            "existing123": {
                "parent_id": "parent789",
                "subreddit": "existingsub",
                "author": "existinguser",
                "chain_depth": 1
            }
        }
        
        with open(rel_path, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f)
        
        # Create crosspost metadata
        crosspost_data = handler._create_crosspost_metadata(crosspost_post, config)
        
        updated_path = handler._create_relationship_file(crosspost_data, temp_output_dir, config)
        
        assert updated_path == rel_path
        
        # Verify both old and new relationships exist
        with open(rel_path, 'r', encoding='utf-8') as f:
            relationships = json.load(f)
        
        assert "existing123" in relationships
        assert "crosspost123" in relationships
        assert len(relationships) == 2

    def test_create_crosspost_summary(self, handler, crosspost_post, temp_output_dir, config):
        """Test crosspost summary creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = temp_output_dir / "crosspost.json"
        
        # Create crosspost metadata
        crosspost_data = handler._create_crosspost_metadata(crosspost_post, config)
        
        summary_path = handler._create_crosspost_summary(crosspost_data, metadata_path, config)
        
        assert summary_path is not None
        assert summary_path.exists()
        assert summary_path.suffix == '.md'
        
        # Verify summary content
        with open(summary_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert '# Crosspost: Crosspost from r/originalsubreddit' in content
        assert '**Crosspost ID:** crosspost123' in content
        assert '**Original Post ID:** original456' in content
        assert '**Subreddit:** r/secondsubreddit' in content
        assert '**Author:** u/crosspost_user' in content
        assert '- **Relationship Type:** crosspost' in content
        assert '- **Chain Depth:** 1' in content
        assert 'https://reddit.com/comments/original456' in content

    def test_validate_config_valid(self, handler):
        """Test configuration validation with valid config."""
        valid_config = {
            'create_relationships': True,
            'create_summary': False
        }
        errors = handler.validate_config(valid_config)
        assert errors == []

    def test_validate_config_invalid_relationships(self, handler):
        """Test configuration validation with invalid create_relationships."""
        invalid_config = {
            'create_relationships': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "create_relationships must be a boolean" in errors[0]

    def test_validate_config_invalid_summary(self, handler):
        """Test configuration validation with invalid create_summary."""
        invalid_config = {
            'create_summary': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "create_summary must be a boolean" in errors[0]

    @pytest.mark.asyncio
    async def test_process_exception_handling(self, handler, crosspost_post, temp_output_dir, config):
        """Test exception handling during processing."""
        # Make output directory non-writable to trigger an exception
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Mock file creation to raise an exception
        with patch('builtins.open', side_effect=PermissionError("No write permission")):
            with pytest.raises(HandlerError, match="Crosspost processing failed"):
                await handler.process(crosspost_post, temp_output_dir, config)

    def test_handler_properties(self, handler):
        """Test handler basic properties."""
        assert handler.name == "crosspost"
        assert handler.priority == 30
        assert isinstance(handler.supported_content_types, set)

    def test_crosspost_metadata_with_missing_fields(self, handler, config):
        """Test crosspost metadata creation with missing optional fields."""
        # Create crosspost without score and num_comments
        minimal_post = PostMetadata(
            id="minimal123",
            title="Minimal Crosspost",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/minimal123",
            date_iso="2023-06-15T10:30:00Z",
            crosspost_parent_id="parent456"
        )
        
        metadata = handler._create_crosspost_metadata(minimal_post, config)
        
        # Should still work without optional fields
        assert 'crosspost_metadata' in metadata
        assert metadata['crosspost_metadata']['crosspost_id'] == "minimal123"
        # Optional fields should not be present
        assert 'score' not in metadata['crosspost_metadata']
        assert 'comments' not in metadata['crosspost_metadata']

    def test_filename_extension_handling(self, handler, crosspost_post):
        """Test that .json extension is properly handled in templates."""
        # Test template without extension
        config = {'filename_template': '{{ post_id }}_crosspost'}
        
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'crosspost123_crosspost'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(config['filename_template'], crosspost_post, config)
        
        assert filename == 'crosspost123_crosspost.json'
        
        # Test template with different extension
        mock_engine.render.return_value = 'crosspost123_crosspost.txt'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(config['filename_template'], crosspost_post, config)
        
        assert filename == 'crosspost123_crosspost.json'  # Should replace with .json

    @pytest.mark.asyncio
    async def test_processed_crossposts_tracking(self, handler, crosspost_post, temp_output_dir, config):
        """Test that processed crossposts are tracked to prevent duplicates."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initial set should be empty
        assert "crosspost123" not in handler._processed_crossposts
        
        result = await handler.process(crosspost_post, temp_output_dir, config)
        
        # After processing, post should be tracked
        assert result.success
        assert "crosspost123" in handler._processed_crossposts

    @pytest.mark.asyncio
    async def test_process_minimal_config(self, handler, crosspost_post, temp_output_dir):
        """Test processing with minimal configuration."""
        config = {}  # Empty config - should use defaults
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(crosspost_post, temp_output_dir, config)
        
        assert result.success
        assert "crosspost_tracking" in result.operations_performed
        # Should create relationships and summary by default
        assert "relationship_mapping" in result.operations_performed
        assert "crosspost_summary" in result.operations_performed

    def test_relationship_file_error_handling(self, handler, crosspost_post, temp_output_dir, config):
        """Test error handling in relationship file creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        crosspost_data = handler._create_crosspost_metadata(crosspost_post, config)
        
        # Mock file operations to raise an exception
        with patch('builtins.open', side_effect=PermissionError("Cannot write file")):
            rel_path = handler._create_relationship_file(crosspost_data, temp_output_dir, config)
        
        # Should return None on error and not raise exception
        assert rel_path is None

    def test_summary_file_error_handling(self, handler, crosspost_post, temp_output_dir, config):
        """Test error handling in summary file creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = temp_output_dir / "crosspost.json"
        crosspost_data = handler._create_crosspost_metadata(crosspost_post, config)
        
        # Mock file operations to raise an exception
        with patch('builtins.open', side_effect=PermissionError("Cannot write file")):
            summary_path = handler._create_crosspost_summary(crosspost_data, metadata_path, config)
        
        # Should return None on error and not raise exception
        assert summary_path is None