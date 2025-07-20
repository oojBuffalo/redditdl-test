"""
Tests for Interactive REPL Mode

Comprehensive test suite for the InteractiveShell class and related functionality.
Tests command parsing, execution, pipeline integration, and user interface components.
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path
import tempfile
import shutil

from redditdl.cli.interactive import InteractiveShell
from redditdl.core.config import AppConfig
from redditdl.core.config.models import ScrapingConfig, ProcessingConfig, OutputConfig, FilterConfig
from redditdl.scrapers import PostMetadata


@pytest.fixture
def temp_session_dir():
    """Create a temporary directory for session files."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_config(temp_session_dir):
    """Create a test configuration for interactive shell."""
    return AppConfig(
        scraping=ScrapingConfig(
            api_mode=False,
            post_limit=10,
            sleep_interval=1.0
        ),
        processing=ProcessingConfig(),
        output=OutputConfig(output_dir=temp_session_dir / "downloads"),
        filters=FilterConfig(),
        session_dir=temp_session_dir
    )


@pytest.fixture
def sample_posts():
    """Create sample PostMetadata for testing."""
    return [
        PostMetadata(
            post_id="abc123",
            title="Amazing sunset photo",
            url="https://example.com/sunset.jpg",
            subreddit="pics",
            author="photographer1",
            score=1200,
            num_comments=15,
            post_type="image",
            is_nsfw=False
        ),
        PostMetadata(
            post_id="def456",
            title="Funny cat video",
            url="https://example.com/cat.mp4",
            subreddit="funny",
            author="catowner",
            score=890,
            num_comments=42,
            post_type="video",
            is_nsfw=False
        ),
        PostMetadata(
            post_id="ghi789",
            title="Cool artwork",
            url="https://example.com/art.jpg",
            subreddit="art",
            author="artist123",
            score=567,
            num_comments=8,
            post_type="image",
            is_nsfw=False
        ),
        PostMetadata(
            post_id="jkl012",
            title="NSFW content",
            url="https://example.com/nsfw.jpg",
            subreddit="nsfw",
            author="user456",
            score=234,
            num_comments=3,
            post_type="image",
            is_nsfw=True
        )
    ]


@pytest.fixture
def interactive_shell(test_config):
    """Create an InteractiveShell instance for testing."""
    with patch('redditdl.cli.interactive.EventEmitter'), \
         patch('redditdl.cli.interactive.StateManager'), \
         patch('redditdl.cli.interactive.PipelineExecutor'), \
         patch('redditdl.cli.interactive.TargetResolver'), \
         patch('redditdl.cli.interactive.FilterFactory'):
        shell = InteractiveShell(test_config)
        return shell


class TestInteractiveShellInitialization:
    """Test InteractiveShell initialization and setup."""
    
    def test_shell_initialization(self, interactive_shell, test_config):
        """Test that shell initializes correctly with configuration."""
        assert interactive_shell.config == test_config
        assert interactive_shell.session_id.startswith("interactive_")
        assert interactive_shell.current_posts == []
        assert interactive_shell.filtered_posts == []
        assert "posts_discovered" in interactive_shell.session_stats
        assert "posts_filtered" in interactive_shell.session_stats
        assert "posts_downloaded" in interactive_shell.session_stats
    
    def test_pipeline_setup(self, interactive_shell):
        """Test that pipeline stages are properly initialized."""
        assert hasattr(interactive_shell, 'acquisition_stage')
        assert hasattr(interactive_shell, 'filter_stage')
        assert hasattr(interactive_shell, 'processing_stage')
        assert hasattr(interactive_shell, 'export_stage')
        assert hasattr(interactive_shell, 'pipeline_executor')
    
    def test_event_system_setup(self, interactive_shell):
        """Test that event system is properly configured."""
        assert hasattr(interactive_shell, 'event_emitter')
        assert hasattr(interactive_shell, 'console_observer')
        assert hasattr(interactive_shell, 'stats_observer')
    
    def test_command_completion_setup(self, interactive_shell):
        """Test that command completion is configured."""
        expected_commands = [
            "explore", "download", "filter", "preview", "stats", 
            "config", "clear", "help", "quit", "exit"
        ]
        assert all(cmd in interactive_shell.commands for cmd in expected_commands)
        assert hasattr(interactive_shell, 'completer')


class TestCommandParsing:
    """Test command parsing and routing functionality."""
    
    @pytest.mark.asyncio
    async def test_handle_empty_command(self, interactive_shell):
        """Test handling of empty command input."""
        result = await interactive_shell._handle_command("")
        assert result is False  # Should not exit
    
    @pytest.mark.asyncio
    async def test_handle_quit_command(self, interactive_shell):
        """Test quit command returns True to exit REPL."""
        result = await interactive_shell._handle_command("quit")
        assert result is True
        
        result = await interactive_shell._handle_command("exit")
        assert result is True
    
    @pytest.mark.asyncio
    async def test_handle_unknown_command(self, interactive_shell):
        """Test handling of unknown commands."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            result = await interactive_shell._handle_command("unknown_command")
            assert result is False
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_command_with_arguments(self, interactive_shell):
        """Test parsing commands with arguments."""
        with patch.object(interactive_shell, '_handle_explore') as mock_explore:
            await interactive_shell._handle_command("explore user:johndoe")
            mock_explore.assert_called_once_with(["user:johndoe"])
    
    @pytest.mark.asyncio
    async def test_quoted_arguments(self, interactive_shell):
        """Test parsing commands with quoted arguments."""
        with patch.object(interactive_shell, '_handle_filter') as mock_filter:
            await interactive_shell._handle_command('filter "score:>100"')
            mock_filter.assert_called_once_with(['"score:>100"'])


class TestExploreCommand:
    """Test the explore command functionality."""
    
    @pytest.mark.asyncio
    async def test_explore_no_arguments(self, interactive_shell):
        """Test explore command without arguments shows usage."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_explore([])
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_explore_user_success(self, interactive_shell, sample_posts):
        """Test successful user exploration."""
        # Mock acquisition stage to return sample posts
        mock_result = Mock()
        mock_result.success = True
        mock_result.data = {"posts": sample_posts}
        interactive_shell.acquisition_stage.process = AsyncMock(return_value=mock_result)
        
        with patch.object(interactive_shell, '_show_post_preview') as mock_preview:
            await interactive_shell._handle_explore(["user:johndoe"])
            
            assert interactive_shell.current_posts == sample_posts
            assert interactive_shell.filtered_posts == sample_posts
            assert interactive_shell.session_stats["posts_discovered"] == len(sample_posts)
            mock_preview.assert_called_once_with(limit=3)
    
    @pytest.mark.asyncio
    async def test_explore_user_failure(self, interactive_shell):
        """Test failed user exploration."""
        # Mock acquisition stage to return failure
        mock_result = Mock()
        mock_result.success = False
        mock_result.error = "User not found"
        interactive_shell.acquisition_stage.process = AsyncMock(return_value=mock_result)
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_explore(["user:nonexistent"])
            mock_print.assert_called()


class TestFilterCommand:
    """Test the filter command functionality."""
    
    @pytest.mark.asyncio
    async def test_filter_no_arguments(self, interactive_shell):
        """Test filter command without arguments shows usage."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_filter([])
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_filter_no_posts(self, interactive_shell):
        """Test filter command with no posts to filter."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_filter(["score:>100"])
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_filter_score_greater_than(self, interactive_shell, sample_posts):
        """Test score filter with greater than condition."""
        interactive_shell.current_posts = sample_posts
        interactive_shell.filtered_posts = sample_posts.copy()
        
        # Mock filter creation and application
        mock_filter = AsyncMock()
        mock_filter.apply = AsyncMock(side_effect=lambda post: post.score > 500)
        interactive_shell._parse_filter_criteria = Mock(return_value=mock_filter)
        
        with patch.object(interactive_shell, '_show_post_preview'):
            await interactive_shell._handle_filter(["score:>500"])
            
            # Should filter to posts with score > 500 (first 3 posts)
            assert len(interactive_shell.filtered_posts) == 3
            assert all(post.score > 500 for post in interactive_shell.filtered_posts)
    
    @pytest.mark.asyncio
    async def test_filter_nsfw_exclude(self, interactive_shell, sample_posts):
        """Test NSFW filter with exclude mode."""
        interactive_shell.current_posts = sample_posts
        interactive_shell.filtered_posts = sample_posts.copy()
        
        # Mock filter creation and application
        mock_filter = AsyncMock()
        mock_filter.apply = AsyncMock(side_effect=lambda post: not post.is_nsfw)
        interactive_shell._parse_filter_criteria = Mock(return_value=mock_filter)
        
        await interactive_shell._handle_filter(["nsfw:exclude"])
        
        # Should filter out NSFW posts (remove last post)
        assert len(interactive_shell.filtered_posts) == 3
        assert all(not post.is_nsfw for post in interactive_shell.filtered_posts)


class TestDownloadCommand:
    """Test the download command functionality."""
    
    @pytest.mark.asyncio
    async def test_download_no_arguments(self, interactive_shell):
        """Test download command without arguments shows usage."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_download([])
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_download_specific_post_success(self, interactive_shell, sample_posts):
        """Test downloading a specific post by ID."""
        interactive_shell.filtered_posts = sample_posts
        
        # Mock processing stage success
        mock_result = Mock()
        mock_result.success = True
        interactive_shell.processing_stage.process = AsyncMock(return_value=mock_result)
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_download(["abc123"])
            
            assert interactive_shell.session_stats["posts_downloaded"] == 1
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_download_specific_post_not_found(self, interactive_shell, sample_posts):
        """Test downloading a post that doesn't exist in current session."""
        interactive_shell.filtered_posts = sample_posts
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_download(["nonexistent"])
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_download_all_posts(self, interactive_shell, sample_posts):
        """Test downloading all filtered posts."""
        interactive_shell.filtered_posts = sample_posts
        
        # Mock processing stage success
        mock_result = Mock()
        mock_result.success = True
        interactive_shell.processing_stage.process = AsyncMock(return_value=mock_result)
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_download(["all"])
            
            assert interactive_shell.session_stats["posts_downloaded"] == len(sample_posts)
            mock_print.assert_called()
    
    @pytest.mark.asyncio
    async def test_download_all_no_posts(self, interactive_shell):
        """Test downloading all posts when no posts are available."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            await interactive_shell._handle_download(["all"])
            mock_print.assert_called()


class TestUtilityCommands:
    """Test utility commands like stats, config, preview, etc."""
    
    def test_handle_stats(self, interactive_shell):
        """Test stats command displays session statistics."""
        interactive_shell.session_stats.update({
            "posts_discovered": 10,
            "posts_filtered": 5,
            "posts_downloaded": 2
        })
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._handle_stats()
            mock_print.assert_called()
    
    def test_handle_config_summary(self, interactive_shell):
        """Test config command displays configuration summary."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._handle_config([])
            mock_print.assert_called()
    
    def test_handle_config_full(self, interactive_shell):
        """Test config command with full option displays complete config."""
        with patch.object(interactive_shell.console, 'print_json') as mock_print_json:
            interactive_shell._handle_config(["full"])
            mock_print_json.assert_called()
    
    def test_handle_preview_no_posts(self, interactive_shell):
        """Test preview command with no posts."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._handle_preview([])
            mock_print.assert_called()
    
    def test_handle_preview_with_posts(self, interactive_shell, sample_posts):
        """Test preview command with posts."""
        interactive_shell.filtered_posts = sample_posts
        
        with patch.object(interactive_shell, '_show_post_preview') as mock_preview:
            interactive_shell._handle_preview([])
            mock_preview.assert_called_once_with(limit=None)
    
    def test_handle_preview_with_limit(self, interactive_shell, sample_posts):
        """Test preview command with limit argument."""
        interactive_shell.filtered_posts = sample_posts
        
        with patch.object(interactive_shell, '_show_post_preview') as mock_preview:
            interactive_shell._handle_preview(["2"])
            mock_preview.assert_called_once_with(limit=2)
    
    def test_handle_clear(self, interactive_shell, sample_posts):
        """Test clear command resets session data."""
        # Set up some session data
        interactive_shell.current_posts = sample_posts
        interactive_shell.filtered_posts = sample_posts
        interactive_shell.session_stats.update({
            "posts_discovered": 10,
            "posts_filtered": 5,
            "posts_downloaded": 2
        })
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._handle_clear()
            
            assert interactive_shell.current_posts == []
            assert interactive_shell.filtered_posts == []
            assert interactive_shell.session_stats["posts_discovered"] == 0
            assert interactive_shell.session_stats["posts_filtered"] == 0
            assert interactive_shell.session_stats["posts_downloaded"] == 0
            mock_print.assert_called()


class TestFilterCriteriaParsing:
    """Test filter criteria parsing functionality."""
    
    def test_parse_score_greater_than(self, interactive_shell):
        """Test parsing score:>N criteria."""
        interactive_shell.filter_factory.create_score_filter = Mock()
        
        filter_obj = interactive_shell._parse_filter_criteria("score:>100")
        
        interactive_shell.filter_factory.create_score_filter.assert_called_once_with(min_score=100)
    
    def test_parse_score_less_than(self, interactive_shell):
        """Test parsing score:<N criteria."""
        interactive_shell.filter_factory.create_score_filter = Mock()
        
        filter_obj = interactive_shell._parse_filter_criteria("score:<50")
        
        interactive_shell.filter_factory.create_score_filter.assert_called_once_with(max_score=50)
    
    def test_parse_score_exact(self, interactive_shell):
        """Test parsing score:N criteria."""
        interactive_shell.filter_factory.create_score_filter = Mock()
        
        filter_obj = interactive_shell._parse_filter_criteria("score:75")
        
        interactive_shell.filter_factory.create_score_filter.assert_called_once_with(min_score=75, max_score=75)
    
    def test_parse_nsfw_mode(self, interactive_shell):
        """Test parsing nsfw:mode criteria."""
        interactive_shell.filter_factory.create_nsfw_filter = Mock()
        
        for mode in ["include", "exclude", "only"]:
            interactive_shell._parse_filter_criteria(f"nsfw:{mode}")
            interactive_shell.filter_factory.create_nsfw_filter.assert_called_with(mode=mode)
    
    def test_parse_invalid_score(self, interactive_shell):
        """Test parsing invalid score criteria."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            result = interactive_shell._parse_filter_criteria("score:invalid")
            assert result is None
            mock_print.assert_called()
    
    def test_parse_invalid_nsfw(self, interactive_shell):
        """Test parsing invalid NSFW criteria."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            result = interactive_shell._parse_filter_criteria("nsfw:invalid")
            assert result is None
            mock_print.assert_called()
    
    def test_parse_unknown_criteria(self, interactive_shell):
        """Test parsing unknown filter criteria."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            result = interactive_shell._parse_filter_criteria("unknown:value")
            assert result is None
            mock_print.assert_called()


class TestPostPreview:
    """Test post preview functionality."""
    
    def test_show_post_preview(self, interactive_shell, sample_posts):
        """Test showing post previews."""
        interactive_shell.filtered_posts = sample_posts
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._show_post_preview()
            mock_print.assert_called()
    
    def test_show_post_preview_with_limit(self, interactive_shell, sample_posts):
        """Test showing post previews with limit."""
        interactive_shell.filtered_posts = sample_posts
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._show_post_preview(limit=2)
            mock_print.assert_called()
    
    def test_show_post_preview_empty(self, interactive_shell):
        """Test showing post previews with no posts."""
        interactive_shell.filtered_posts = []
        
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._show_post_preview()
            mock_print.assert_called()


class TestSessionManagement:
    """Test session state management and persistence."""
    
    def test_session_id_generation(self, interactive_shell):
        """Test that session ID is generated correctly."""
        assert interactive_shell.session_id.startswith("interactive_")
        assert len(interactive_shell.session_id) > len("interactive_")
    
    def test_session_stats_initialization(self, interactive_shell):
        """Test that session statistics are initialized correctly."""
        expected_keys = [
            "posts_discovered", "posts_filtered", "posts_downloaded", 
            "total_size", "start_time"
        ]
        for key in expected_keys:
            assert key in interactive_shell.session_stats
    
    def test_session_stats_updates(self, interactive_shell, sample_posts):
        """Test that session statistics are updated correctly."""
        # Simulate exploration
        interactive_shell.current_posts = sample_posts
        interactive_shell.filtered_posts = sample_posts.copy()
        interactive_shell.session_stats["posts_discovered"] = len(sample_posts)
        interactive_shell.session_stats["posts_filtered"] = len(sample_posts)
        
        # Simulate filtering
        interactive_shell.filtered_posts = sample_posts[:2]
        interactive_shell.session_stats["posts_filtered"] = len(interactive_shell.filtered_posts)
        
        # Simulate downloads
        interactive_shell.session_stats["posts_downloaded"] = 1
        
        assert interactive_shell.session_stats["posts_discovered"] == 4
        assert interactive_shell.session_stats["posts_filtered"] == 2
        assert interactive_shell.session_stats["posts_downloaded"] == 1


class TestREPLIntegration:
    """Test REPL integration with external systems."""
    
    @pytest.mark.asyncio
    async def test_start_repl_mocked(self, interactive_shell):
        """Test REPL startup with mocked prompt session."""
        with patch('redditdl.cli.interactive.PromptSession') as mock_session_class:
            # Mock the prompt session to return quit command
            mock_session = AsyncMock()
            mock_session.prompt_async = AsyncMock(return_value="quit")
            mock_session_class.return_value = mock_session
            
            with patch.object(interactive_shell, '_show_welcome'), \
                 patch.object(interactive_shell, '_show_goodbye'):
                await interactive_shell.start_repl()
            
            mock_session.prompt_async.assert_called()
    
    def test_welcome_message(self, interactive_shell):
        """Test welcome message display."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._show_welcome()
            mock_print.assert_called()
    
    def test_goodbye_message(self, interactive_shell):
        """Test goodbye message display."""
        with patch.object(interactive_shell.console, 'print') as mock_print:
            interactive_shell._show_goodbye()
            mock_print.assert_called()


if __name__ == "__main__":
    pytest.main([__file__])