"""
Tests for Poll Content Handler

Tests the PollContentHandler functionality including poll data extraction,
structured JSON creation, visualization generation, and summary creation.
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from typing import Dict, Any

from redditdl.content_handlers.poll import PollContentHandler
from redditdl.content_handlers.base import HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.core.templates import FilenameTemplateEngine


class TestPollContentHandler:
    """Test PollContentHandler functionality."""
    
    @pytest.fixture
    def handler(self):
        """Create a PollContentHandler instance."""
        return PollContentHandler(priority=70)
    
    @pytest.fixture
    def poll_post(self):
        """Create a sample poll post."""
        poll_data = {
            'question': 'What is your favorite programming language?',
            'total_vote_count': 1500,
            'voting_end_timestamp': 1687000000,
            'options': [
                {
                    'id': 'option_1',
                    'text': 'Python',
                    'vote_count': 600
                },
                {
                    'id': 'option_2', 
                    'text': 'JavaScript',
                    'vote_count': 450
                },
                {
                    'id': 'option_3',
                    'text': 'Rust',
                    'vote_count': 300
                },
                {
                    'id': 'option_4',
                    'text': 'Go',
                    'vote_count': 150
                }
            ]
        }
        
        return PostMetadata(
            id="poll123",
            title="Programming Language Poll",
            author="poll_user",
            subreddit="programming",
            url="https://reddit.com/r/programming/comments/poll123",
            date_iso="2023-06-15T10:30:00Z",
            poll_data=poll_data,
            score=850,
            num_comments=120,
            is_nsfw=False
        )
    
    @pytest.fixture
    def empty_poll_post(self):
        """Create a poll post with no poll data."""
        return PostMetadata(
            id="empty_poll",
            title="Empty Poll",
            author="test_user",
            subreddit="test",
            url="https://reddit.com/comments/empty_poll",
            date_iso="2023-06-15T10:30:00Z",
            poll_data=None
        )
    
    @pytest.fixture
    def simple_poll_post(self):
        """Create a simple poll post with minimal data."""
        poll_data = {
            'question': 'Yes or No?',
            'total_vote_count': 100,
            'options': [
                {'id': 'yes', 'text': 'Yes', 'vote_count': 70},
                {'id': 'no', 'text': 'No', 'vote_count': 30}
            ]
        }
        
        return PostMetadata(
            id="simple_poll",
            title="Simple Poll",
            author="simple_user",
            subreddit="polls",
            url="https://reddit.com/r/polls/comments/simple_poll",
            date_iso="2023-06-15T12:00:00Z",
            poll_data=poll_data
        )
    
    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create a temporary output directory."""
        return tmp_path / "poll_test"
    
    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return {
            'create_visualization': True,
            'create_summary': True,
            'max_filename_length': 200
        }

    def test_supported_content_types(self, handler):
        """Test that handler supports poll content type."""
        assert 'poll' in handler.supported_content_types
        assert len(handler.supported_content_types) == 1

    def test_can_handle_poll_post(self, handler, poll_post):
        """Test that handler can handle poll posts."""
        assert handler.can_handle(poll_post, 'poll')

    def test_can_handle_non_poll_post(self, handler):
        """Test that handler rejects non-poll posts."""
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
        assert not handler.can_handle(text_post, 'poll')

    def test_can_handle_empty_poll(self, handler, empty_poll_post):
        """Test that handler rejects poll posts with no poll data."""
        assert not handler.can_handle(empty_poll_post, 'poll')

    @pytest.mark.asyncio
    async def test_process_poll_success(self, handler, poll_post, temp_output_dir, config):
        """Test successful poll processing."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(poll_post, temp_output_dir, config)
        
        # Verify result
        assert result.success
        assert result.handler_name == "poll"
        assert result.content_type == "poll"
        assert "poll_save" in result.operations_performed
        assert "poll_visualization" in result.operations_performed
        assert "poll_summary" in result.operations_performed
        
        # Should have created 3 files: JSON, visualization, summary
        assert len(result.files_created) == 3
        
        # Verify JSON file was created
        json_files = [f for f in result.files_created if f.suffix == '.json']
        assert len(json_files) == 1
        
        json_file = json_files[0]
        assert json_file.exists()
        
        # Verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert 'post_metadata' in data
        assert 'poll_data' in data
        assert 'summary' in data
        assert data['poll_data']['question'] == 'What is your favorite programming language?'
        assert len(data['poll_data']['options']) == 4
        assert data['poll_data']['total_votes'] == 1500

    @pytest.mark.asyncio
    async def test_process_empty_poll_error(self, handler, empty_poll_post, temp_output_dir, config):
        """Test that empty polls raise an error."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        with pytest.raises(HandlerError, match="No poll data found"):
            await handler.process(empty_poll_post, temp_output_dir, config)

    @pytest.mark.asyncio
    async def test_process_without_visualization(self, handler, poll_post, temp_output_dir):
        """Test poll processing without visualization."""
        config = {
            'create_visualization': False,
            'create_summary': True
        }
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(poll_post, temp_output_dir, config)
        
        assert result.success
        assert "poll_save" in result.operations_performed
        assert "poll_summary" in result.operations_performed
        assert "poll_visualization" not in result.operations_performed
        
        # Should have 2 files: JSON and summary (no visualization)
        assert len(result.files_created) == 2

    @pytest.mark.asyncio
    async def test_process_without_summary(self, handler, poll_post, temp_output_dir):
        """Test poll processing without summary."""
        config = {
            'create_visualization': True,
            'create_summary': False
        }
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(poll_post, temp_output_dir, config)
        
        assert result.success
        assert "poll_save" in result.operations_performed
        assert "poll_visualization" in result.operations_performed
        assert "poll_summary" not in result.operations_performed
        
        # Should have 2 files: JSON and visualization (no summary)  
        assert len(result.files_created) == 2

    def test_construct_filename_default(self, handler, poll_post, config):
        """Test default filename construction."""
        filename = handler._construct_filename(poll_post, config)
        
        assert filename.endswith('.json')
        assert 'poll123' in filename
        assert 'Programming_Language_Poll' in filename or 'Programming' in filename
        assert '2023-06-15T10_30_00Z' in filename

    def test_construct_filename_with_template(self, handler, poll_post):
        """Test filename construction with custom template."""
        config = {
            'filename_template': '{{ subreddit }}_{{ post_id }}_poll.{{ ext }}'
        }
        
        # Mock template engine
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'programming_poll123_poll.json'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._construct_filename(poll_post, config)
        
        assert filename == 'programming_poll123_poll.json'

    def test_apply_template_success(self, handler, poll_post, config):
        """Test successful template rendering."""
        template = '{{ subreddit }}/{{ post_id }}_{{ title|slugify }}.{{ ext }}'
        
        # Mock template engine
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'programming/poll123_programming-language-poll.json'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(template, poll_post, config)
        
        assert filename == 'programming/poll123_programming-language-poll.json'

    def test_apply_template_failure_fallback(self, handler, poll_post, config):
        """Test template rendering failure with fallback."""
        template = '{{ invalid_template }}'
        
        # Mock template engine to raise exception
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.side_effect = Exception("Template error")
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(template, poll_post, config)
        
        # Should fallback to default filename
        assert filename.endswith('.json')
        assert 'poll123' in filename

    def test_create_structured_poll_data(self, handler, poll_post, config):
        """Test creation of structured poll data."""
        poll_data = poll_post.poll_data
        structured = handler._create_structured_poll_data(poll_post, poll_data, config)
        
        # Verify structure
        assert 'post_metadata' in structured
        assert 'poll_data' in structured
        assert 'summary' in structured
        
        # Verify poll data
        poll_info = structured['poll_data']
        assert poll_info['question'] == 'What is your favorite programming language?'
        assert poll_info['total_votes'] == 1500
        assert len(poll_info['options']) == 4
        assert poll_info['handler'] == 'poll'
        
        # Verify percentages calculated correctly
        python_option = next(opt for opt in poll_info['options'] if opt['text'] == 'Python')
        assert python_option['percentage'] == 40.0  # 600/1500 * 100
        
        # Verify summary statistics
        summary = structured['summary']
        assert summary['total_options'] == 4
        assert summary['most_voted_option']['text'] == 'Python'
        assert summary['least_voted_option']['text'] == 'Go'

    def test_create_structured_poll_data_zero_votes(self, handler, simple_poll_post, config):
        """Test structured data creation with zero total votes."""
        # Modify poll data to have zero votes
        poll_data = simple_poll_post.poll_data.copy()
        poll_data['total_vote_count'] = 0
        for option in poll_data['options']:
            option['vote_count'] = 0
        
        structured = handler._create_structured_poll_data(simple_poll_post, poll_data, config)
        
        # Percentages should be 0.0 when total votes is 0
        for option in structured['poll_data']['options']:
            assert option['percentage'] == 0.0

    def test_create_poll_visualization(self, handler, poll_post, temp_output_dir):
        """Test poll visualization creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        json_path = temp_output_dir / "poll.json"
        
        # Create structured poll data
        poll_data = handler._create_structured_poll_data(poll_post, poll_post.poll_data, {})
        
        viz_path = handler._create_poll_visualization(poll_data, json_path, {})
        
        assert viz_path is not None
        assert viz_path.exists()
        assert viz_path.suffix == '.txt'
        
        # Verify visualization content
        with open(viz_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert 'What is your favorite programming language?' in content
        assert 'Total Votes: 1500' in content
        assert 'Python' in content
        assert '600 votes (40.0%)' in content
        assert '#' in content  # Should have bar chart characters

    def test_create_poll_summary(self, handler, poll_post, temp_output_dir):
        """Test poll summary creation."""
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        json_path = temp_output_dir / "poll.json"
        
        # Create structured poll data
        poll_data = handler._create_structured_poll_data(poll_post, poll_post.poll_data, {})
        
        summary_path = handler._create_poll_summary(poll_data, json_path, {})
        
        assert summary_path is not None
        assert summary_path.exists()
        assert summary_path.suffix == '.md'
        
        # Verify summary content
        with open(summary_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert '# Poll: What is your favorite programming language?' in content
        assert '**Subreddit:** r/programming' in content
        assert '**Author:** u/poll_user' in content
        assert '**Total Votes:** 1500' in content
        assert '- **Python**: 600 votes (40.0%)' in content
        assert '- **Most voted option:** Python' in content

    def test_validate_config_valid(self, handler):
        """Test configuration validation with valid config."""
        valid_config = {
            'create_visualization': True,
            'create_summary': False
        }
        errors = handler.validate_config(valid_config)
        assert errors == []

    def test_validate_config_invalid_visualization(self, handler):
        """Test configuration validation with invalid create_visualization."""
        invalid_config = {
            'create_visualization': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "create_visualization must be a boolean" in errors[0]

    def test_validate_config_invalid_summary(self, handler):
        """Test configuration validation with invalid create_summary.""" 
        invalid_config = {
            'create_summary': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "create_summary must be a boolean" in errors[0]

    @pytest.mark.asyncio
    async def test_process_exception_handling(self, handler, poll_post, temp_output_dir, config):
        """Test exception handling during processing."""
        # Make output directory non-writable to trigger an exception
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Mock file creation to raise an exception
        with patch('builtins.open', side_effect=PermissionError("No write permission")):
            with pytest.raises(HandlerError, match="Poll processing failed"):
                await handler.process(poll_post, temp_output_dir, config)

    def test_handler_properties(self, handler):
        """Test handler basic properties."""
        assert handler.name == "poll"
        assert handler.priority == 70
        assert isinstance(handler.supported_content_types, set)

    def test_poll_with_missing_options(self, handler, config):
        """Test handling of poll data with missing options."""
        # Create poll post with empty options
        poll_data = {
            'question': 'Question without options',
            'total_vote_count': 0,
            'options': []
        }
        
        poll_post = PostMetadata(
            id="no_options",
            title="Poll without options",
            author="user",
            subreddit="test",
            url="https://reddit.com/comments/no_options",
            date_iso="2023-06-15T10:30:00Z",
            poll_data=poll_data
        )
        
        structured = handler._create_structured_poll_data(poll_post, poll_data, config)
        
        assert structured['poll_data']['options'] == []
        assert structured['summary']['total_options'] == 0
        assert structured['summary']['most_voted_option'] is None
        assert structured['summary']['least_voted_option'] is None

    def test_filename_extension_handling(self, handler, poll_post):
        """Test that .json extension is properly handled in templates."""
        # Test template without extension
        config = {'filename_template': '{{ post_id }}_poll'}
        
        mock_engine = Mock(spec=FilenameTemplateEngine)
        mock_engine.render.return_value = 'poll123_poll'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(config['filename_template'], poll_post, config)
        
        assert filename == 'poll123_poll.json'
        
        # Test template with different extension
        mock_engine.render.return_value = 'poll123_poll.txt'
        
        with patch.object(handler, '_template_engine', mock_engine):
            filename = handler._apply_template(config['filename_template'], poll_post, config)
        
        assert filename == 'poll123_poll.json'  # Should replace with .json

    @pytest.mark.asyncio
    async def test_process_minimal_config(self, handler, simple_poll_post, temp_output_dir):
        """Test processing with minimal configuration."""
        config = {}  # Empty config - should use defaults
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        
        result = await handler.process(simple_poll_post, temp_output_dir, config)
        
        assert result.success
        assert "poll_save" in result.operations_performed
        # Should create summary by default (create_summary defaults to True)
        assert "poll_summary" in result.operations_performed
        # Should not create visualization by default (create_visualization defaults to False)
        assert "poll_visualization" not in result.operations_performed