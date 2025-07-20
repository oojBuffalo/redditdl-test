#!/usr/bin/env python3
"""
Tests for main module including CLI parsing and orchestration.
"""

import pytest
import sys
import os
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, call, MagicMock
from argparse import Namespace

# Add the project root to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import sys
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, call, MagicMock
from argparse import Namespace

from redditdl.main import (
    parse_args, _validate_arguments, construct_filename, 
    process_posts, _is_media_url, main, setup_logging
)
from redditdl.scrapers import PostMetadata
from redditdl.metadata import MetadataEmbedder
from redditdl.downloader import MediaDownloader


class TestArgumentParsing:
    """Test CLI argument parsing and validation."""
    
    def test_parse_args_minimal_required(self):
        """Test parsing with only required arguments."""
        test_args = ["--user", "testuser"]
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = parse_args()
            assert args.user == "testuser"
            assert args.outdir == "downloads"
            assert args.limit == 20
            assert args.sleep == 1.0
            assert not args.api
            assert not args.login
    
    def test_parse_args_all_options(self):
        """Test parsing with all optional arguments."""
        test_args = [
            "--user", "testuser",
            "--outdir", "/tmp/test",
            "--limit", "50",
            "--sleep", "2.5",
            "--api",
            "--client_id", "test_id",
            "--client_secret", "test_secret",
            "--user_agent", "Custom Agent/1.0"
        ]
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = parse_args()
            assert args.user == "testuser"
            assert args.outdir == "/tmp/test"
            assert args.limit == 50
            assert args.sleep == 2.5
            assert args.api
            assert args.client_id == "test_id"
            assert args.client_secret == "test_secret"
            assert args.user_agent == "Custom Agent/1.0"
    
    def test_parse_args_login_mode(self):
        """Test parsing login mode arguments."""
        test_args = [
            "--user", "testuser",
            "--login",
            "--username", "login_user",
            "--password", "login_pass"
        ]
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = parse_args()
            assert args.user == "testuser"
            assert args.login
            assert args.username == "login_user"
            assert args.password == "login_pass"
            assert not args.api
    
    def test_parse_args_environment_variables(self):
        """Test that environment variables are used as defaults."""
        env_vars = {
            'REDDIT_CLIENT_ID': 'env_client_id',
            'REDDIT_CLIENT_SECRET': 'env_client_secret',
            'REDDIT_USERNAME': 'env_username',
            'REDDIT_PASSWORD': 'env_password'
        }
        
        with patch.dict(os.environ, env_vars):
            test_args = ["--user", "testuser", "--api"]
            with patch.object(sys, 'argv', ['main.py'] + test_args):
                args = parse_args()
                assert args.client_id == "env_client_id"
                assert args.client_secret == "env_client_secret"
    
    def test_missing_required_user_argument(self):
        """Test that missing required --user argument causes SystemExit."""
        with patch.object(sys, 'argv', ['main.py']):
            with pytest.raises(SystemExit):
                parse_args()


class TestArgumentValidation:
    """Test argument validation logic."""
    
    def test_validate_negative_sleep(self):
        """Test validation fails with negative sleep interval."""
        args = Namespace(sleep=-1.0, api=False, login=False)
        with pytest.raises(SystemExit):
            _validate_arguments(args)
    
    def test_validate_api_mode_missing_credentials(self):
        """Test validation fails for API mode without credentials."""
        args = Namespace(
            sleep=1.0, api=True, login=False,
            client_id=None, client_secret="secret"
        )
        with pytest.raises(SystemExit):
            _validate_arguments(args)
        
        args.client_id = "id"
        args.client_secret = None
        with pytest.raises(SystemExit):
            _validate_arguments(args)
    
    def test_validate_login_mode_missing_credentials(self):
        """Test validation fails for login mode without credentials."""
        args = Namespace(
            sleep=1.0, api=False, login=True,
            username=None, password="pass"
        )
        with pytest.raises(SystemExit):
            _validate_arguments(args)
        
        args.username = "user"
        args.password = None
        with pytest.raises(SystemExit):
            _validate_arguments(args)
    
    def test_validate_valid_arguments(self):
        """Test validation passes with valid arguments."""
        # Test non-API mode
        args = Namespace(sleep=1.0, api=False, login=False)
        _validate_arguments(args)  # Should not raise
        
        # Test API mode with credentials
        args = Namespace(
            sleep=1.0, api=True, login=False,
            client_id="id", client_secret="secret"
        )
        _validate_arguments(args)  # Should not raise
        
        # Test login mode with credentials
        args = Namespace(
            sleep=1.0, api=False, login=True,
            username="user", password="pass"
        )
        _validate_arguments(args)  # Should not raise


class TestFilenameConstruction:
    """Test filename construction logic."""
    
    def test_construct_filename_with_extension(self):
        """Test filename construction with URL that has extension."""
        post = PostMetadata({
            'id': 'abc123',
            'title': 'Test Post Title',
            'created_utc': 1640995200  # 2022-01-01T00:00:00Z
        })
        media_url = "https://i.redd.it/example.jpg"
        
        filename = construct_filename(post, media_url)
        
        # Note: sanitize_filename replaces : with _ in timestamps
        assert filename.startswith("2022-01-01T00_00_00Z_abc123_")
        assert filename.endswith(".jpg")
        assert "Test Post Title" in filename
    
    def test_construct_filename_without_extension(self):
        """Test filename construction with URL that lacks extension."""
        post = PostMetadata({
            'id': 'def456',
            'title': 'Another Test',
            'created_utc': 1640995200
        })
        media_url = "https://i.redd.it/example"
        
        filename = construct_filename(post, media_url)
        
        # Note: sanitize_filename replaces : with _ in timestamps
        assert filename.startswith("2022-01-01T00_00_00Z_def456_")
        assert filename.endswith(".jpg")  # Default for i.redd.it
    
    def test_construct_filename_video_url(self):
        """Test filename construction for video URLs."""
        post = PostMetadata({
            'id': 'ghi789',
            'title': 'Video Post',
            'created_utc': 1640995200
        })
        media_url = "https://v.redd.it/example"
        
        filename = construct_filename(post, media_url)
        
        assert filename.endswith(".mp4")  # Default for v.redd.it
    
    def test_construct_filename_long_title(self):
        """Test filename construction with very long title."""
        post = PostMetadata({
            'id': 'jkl012',
            'title': 'A' * 100,  # Very long title
            'created_utc': 1640995200
        })
        media_url = "https://i.redd.it/example.png"
        
        filename = construct_filename(post, media_url)
        
        # Title should be truncated to 50 characters
        # After sanitization, the filename structure is: timestamp_postid_title.ext
        # We need to look at the actual title part which is after the second underscore in postid
        parts = filename.split('_')
        # Find where the post ID ends and title begins
        title_part = None
        for i, part in enumerate(parts):
            if part == 'jkl012' and i + 1 < len(parts):
                # The title part might span multiple segments due to spaces becoming underscores
                title_part = '_'.join(parts[i+1:]).replace('.png', '')
                break
        
        assert title_part is not None
        assert len(title_part) == 50  # Should be exactly 50 A's
    
    def test_construct_filename_empty_title(self):
        """Test filename construction with empty title."""
        post = PostMetadata({
            'id': 'mno345',
            'title': '',
            'created_utc': 1640995200
        })
        media_url = "https://i.redd.it/example.gif"
        
        filename = construct_filename(post, media_url)
        
        assert "untitled" in filename


class TestMediaUrlDetection:
    """Test media URL detection logic."""
    
    def test_is_media_url_known_hosts(self):
        """Test detection of known media hosting URLs."""
        media_urls = [
            "https://i.redd.it/example.jpg",
            "https://v.redd.it/example",
            "https://imgur.com/example.png",
            "https://i.imgur.com/example.gif",
            "https://gfycat.com/example",
            "https://redgifs.com/example"
        ]
        
        for url in media_urls:
            assert _is_media_url(url), f"Failed to detect media URL: {url}"
    
    def test_is_media_url_file_extensions(self):
        """Test detection based on file extensions."""
        media_urls = [
            "https://example.com/image.jpg",
            "https://example.com/image.jpeg",
            "https://example.com/image.png",
            "https://example.com/image.gif",
            "https://example.com/image.webp",
            "https://example.com/video.mp4",
            "https://example.com/video.webm",
            "https://example.com/video.mov",
            "https://example.com/video.avi",
            "https://example.com/video.mkv"
        ]
        
        for url in media_urls:
            assert _is_media_url(url), f"Failed to detect media URL: {url}"
    
    def test_is_media_url_non_media(self):
        """Test rejection of non-media URLs."""
        non_media_urls = [
            "https://reddit.com/r/test",
            "https://example.com/page.html",
            "https://example.com/document.pdf",
            "https://example.com/data.json",
            "",
            None
        ]
        
        for url in non_media_urls:
            assert not _is_media_url(url), f"Incorrectly detected as media URL: {url}"


class TestPostProcessing:
    """Test post processing and download orchestration."""
    
    def test_process_posts_successful_downloads(self):
        """Test processing posts with successful downloads."""
        # Create mock posts
        posts = [
            PostMetadata({
                'id': 'post1',
                'title': 'Test Post 1',
                'url': 'https://i.redd.it/example1.jpg',
                'created_utc': 1640995200
            }),
            PostMetadata({
                'id': 'post2', 
                'title': 'Test Post 2',
                'url': 'https://i.redd.it/example2.png',
                'created_utc': 1640995200
            })
        ]
        
        # Mock downloader
        mock_downloader = Mock(spec=MediaDownloader)
        mock_path = Mock()
        mock_path.exists.return_value = True
        mock_path.name = "test_file.jpg"
        mock_downloader.download.return_value = mock_path
        
        with patch('main.logging') as mock_logging:
            process_posts(posts, mock_downloader)
            
            # Verify downloader was called for each post
            assert mock_downloader.download.call_count == 2
            
            # Verify success messages were logged
            mock_logging.info.assert_any_call("✓ Saved: test_file.jpg")
    
    def test_process_posts_skip_non_media(self):
        """Test processing posts with non-media URLs."""
        posts = [
            PostMetadata({
                'id': 'post1',
                'title': 'Text Post',
                'url': 'https://reddit.com/r/test',
                'created_utc': 1640995200
            })
        ]
        
        mock_downloader = Mock(spec=MediaDownloader)
        
        with patch('main.logging') as mock_logging:
            process_posts(posts, mock_downloader)
            
            # Verify downloader was not called
            mock_downloader.download.assert_not_called()
            
            # Verify skip message was logged
            mock_logging.info.assert_any_call(
                "Skipping post post1 - not a media URL: https://reddit.com/r/test"
            )
    
    def test_process_posts_missing_url(self):
        """Test processing posts with missing URLs."""
        posts = [
            PostMetadata({
                'id': 'post1',
                'title': 'No URL Post',
                'created_utc': 1640995200
            })
        ]
        
        mock_downloader = Mock(spec=MediaDownloader)
        
        with patch('main.logging') as mock_logging:
            process_posts(posts, mock_downloader)
            
            # Verify downloader was not called
            mock_downloader.download.assert_not_called()
            
            # Verify warning was logged
            mock_logging.warning.assert_any_call(
                "Skipping post post1 - no media URL found"
            )
    
    def test_process_posts_download_error(self):
        """Test processing posts with download errors."""
        posts = [
            PostMetadata({
                'id': 'post1',
                'title': 'Error Post',
                'url': 'https://i.redd.it/example.jpg',
                'created_utc': 1640995200
            })
        ]
        
        mock_downloader = Mock(spec=MediaDownloader)
        mock_downloader.download.side_effect = Exception("Download failed")
        
        with patch('main.logging') as mock_logging:
            process_posts(posts, mock_downloader)
            
            # Verify error was logged
            mock_logging.error.assert_any_call(
                "✗ Error processing post post1: Download failed"
            )


class TestSetupLogging:
    """Test logging configuration."""
    
    def test_setup_logging_configuration(self):
        """Test that logging is configured correctly."""
        with patch('main.logging.basicConfig') as mock_config:
            setup_logging()
            
            mock_config.assert_called_once_with(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )


class TestMainIntegration:
    """Test main function integration."""
    
    @patch('main.setup_logging')
    @patch('main.parse_args')
    @patch('main.MetadataEmbedder')
    @patch('main.MediaDownloader')
    @patch('main.YarsScraper')
    @patch('main.process_posts')
    def test_main_non_api_mode(self, mock_process, mock_yars, mock_downloader, 
                               mock_embedder, mock_parse, mock_logging):
        """Test main function in non-API mode."""
        # Setup mocks
        mock_args = Mock()
        mock_args.user = "testuser"
        mock_args.outdir = "downloads"
        mock_args.limit = 20
        mock_args.sleep = 1.0
        mock_args.api = False
        mock_args.login = False
        mock_parse.return_value = mock_args
        
        mock_scraper = Mock()
        mock_posts = [Mock(), Mock()]
        mock_scraper.fetch_user_posts.return_value = mock_posts
        mock_yars.return_value = mock_scraper
        
        # Run main function
        main()
        
        # Verify components were initialized
        mock_embedder.assert_called_once()
        mock_downloader.assert_called_once()
        mock_yars.assert_called_once_with(sleep_interval=1.0)
        
        # Verify posts were fetched and processed
        mock_scraper.fetch_user_posts.assert_called_once_with("testuser", 20)
        mock_process.assert_called_once_with(mock_posts, mock_downloader.return_value)
    
    @patch('main.setup_logging')
    @patch('main.parse_args')
    @patch('main.MetadataEmbedder')
    @patch('main.MediaDownloader')
    @patch('main.PrawScraper')
    @patch('main.process_posts')
    def test_main_api_mode(self, mock_process, mock_praw, mock_downloader,
                           mock_embedder, mock_parse, mock_logging):
        """Test main function in API mode."""
        # Setup mocks
        mock_args = Mock()
        mock_args.user = "testuser"
        mock_args.outdir = "downloads"
        mock_args.limit = 20
        mock_args.sleep = 1.0
        mock_args.api = True
        mock_args.login = False
        mock_args.client_id = "test_id"
        mock_args.client_secret = "test_secret"
        mock_args.user_agent = "Test Agent"
        mock_args.username = None
        mock_args.password = None
        mock_parse.return_value = mock_args
        
        mock_scraper = Mock()
        mock_posts = [Mock(), Mock()]
        mock_scraper.fetch_user_posts.return_value = mock_posts
        mock_praw.return_value = mock_scraper
        
        # Run main function
        main()
        
        # Verify PrawScraper was initialized correctly
        mock_praw.assert_called_once_with(
            client_id="test_id",
            client_secret="test_secret",
            user_agent="Test Agent",
            login_username=None,
            login_password=None,
            sleep_interval=1.0
        )
        
        # Verify posts were fetched and processed
        mock_scraper.fetch_user_posts.assert_called_once_with("testuser", 20)
        mock_process.assert_called_once_with(mock_posts, mock_downloader.return_value)
    
    @patch('main.setup_logging')
    @patch('main.parse_args')
    @patch('main.YarsScraper')
    def test_main_no_posts_found(self, mock_yars, mock_parse, mock_logging):
        """Test main function when no posts are found."""
        # Setup mocks
        mock_args = Mock()
        mock_args.user = "testuser"
        mock_args.outdir = "downloads"
        mock_args.limit = 20
        mock_args.sleep = 1.0
        mock_args.api = False
        mock_parse.return_value = mock_args
        
        mock_scraper = Mock()
        mock_scraper.fetch_user_posts.return_value = []  # No posts
        mock_yars.return_value = mock_scraper
        
        with patch('main.logging') as mock_log:
            # Run main function
            main()
            
            # Verify warning was logged
            mock_log.warning.assert_called_with(
                "No posts found or accessible for this user"
            )
    
    @patch('main.setup_logging')
    @patch('main.parse_args')
    def test_main_keyboard_interrupt(self, mock_parse, mock_logging):
        """Test main function handles KeyboardInterrupt gracefully."""
        mock_parse.side_effect = KeyboardInterrupt()
        
        with patch('main.logging') as mock_log:
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == 1
            mock_log.info.assert_called_with("Operation cancelled by user")
    
    @patch('main.setup_logging')
    @patch('main.parse_args')
    def test_main_generic_exception(self, mock_parse, mock_logging):
        """Test main function handles generic exceptions gracefully."""
        mock_parse.side_effect = Exception("Test error")
        
        with patch('main.logging') as mock_log:
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == 1
            mock_log.error.assert_called_with("Fatal error: Test error")


if __name__ == "__main__":
    pytest.main([__file__]) 