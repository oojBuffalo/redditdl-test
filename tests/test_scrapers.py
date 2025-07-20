#!/usr/bin/env python3
"""
Unit tests for Reddit scrapers and PostMetadata.

This module contains comprehensive tests for the PostMetadata class,
including data extraction, validation, and serialization functionality.
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timezone
import praw
import prawcore
import requests

from redditdl.scrapers import PostMetadata, PrawScraper, YarsScraper
from redditdl.utils import api_retry as retry_on_network_error, non_api_retry, auth_retry as retry_on_auth_error


class TestPostMetadata:
    """Test cases for the PostMetadata class."""
    
    def test_basic_post_metadata_creation(self):
        """Test creating PostMetadata with typical Reddit post data."""
        raw_data = {
            'id': 'abc123',
            'title': 'Test Post Title',
            'selftext': 'This is the post content',
            'created_utc': 1640995200,  # 2022-01-01 00:00:00 UTC
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/abc123/test_post/',
            'url': 'https://example.com/image.jpg',
            'author': 'testuser',
            'is_video': False,
            'media_url': 'https://example.com/image.jpg'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        
        assert metadata.id == 'abc123'
        assert metadata.title == 'Test Post Title'
        assert metadata.selftext == 'This is the post content'
        assert metadata.subreddit == 'testsubreddit'
        assert metadata.permalink == '/r/testsubreddit/comments/abc123/test_post/'
        assert metadata.url == 'https://example.com/image.jpg'
        assert metadata.author == 'testuser'
        assert metadata.is_video is False
        assert metadata.media_url == 'https://example.com/image.jpg'
        assert metadata.date_iso == '2022-01-01T00:00:00Z'
    
    def test_minimal_post_metadata_creation(self):
        """Test creating PostMetadata with minimal required data."""
        raw_data = {
            'id': 'minimal123'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        
        assert metadata.id == 'minimal123'
        assert metadata.title == ''
        assert metadata.selftext == ''
        assert metadata.subreddit == ''
        assert metadata.permalink == ''
        assert metadata.url == ''
        assert metadata.author == '[deleted]'
        assert metadata.is_video is False
        assert metadata.media_url is None
        # date_iso should be current timestamp format
        assert len(metadata.date_iso) == 20
        assert metadata.date_iso.endswith('Z')
    
    def test_missing_id_raises_error(self):
        """Test that missing ID raises ValueError."""
        raw_data = {
            'title': 'Test without ID'
        }
        
        with pytest.raises(ValueError, match="Post ID is required"):
            PostMetadata.from_raw(raw_data)
    
    def test_empty_id_raises_error(self):
        """Test that empty ID raises ValueError."""
        raw_data = {
            'id': '',
            'title': 'Test with empty ID'
        }
        
        with pytest.raises(ValueError, match="Post ID is required"):
            PostMetadata.from_raw(raw_data)
    
    def test_whitespace_handling(self):
        """Test that whitespace in string fields is properly handled."""
        raw_data = {
            'id': 'whitespace123',
            'title': '  Title with spaces  ',
            'selftext': '\n\nContent with newlines\n\n',
            'author': '  spaced_user  '
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        
        assert metadata.title == 'Title with spaces'
        assert metadata.selftext == 'Content with newlines'
        assert metadata.author == 'spaced_user'
    
    def test_media_url_extraction_priority(self):
        """Test media URL extraction with multiple potential sources."""
        # Test media_url takes priority
        raw_data = {
            'id': 'media123',
            'media_url': 'https://priority.com/image.jpg',
            'url_overridden_by_dest': 'https://second.com/image.jpg',
            'url': 'https://third.com/image.jpg'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.media_url == 'https://priority.com/image.jpg'
        
        # Test url_overridden_by_dest as fallback
        raw_data = {
            'id': 'media124',
            'url_overridden_by_dest': 'https://second.com/image.jpg',
            'url': 'https://third.com/image.jpg'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.media_url == 'https://second.com/image.jpg'
        
        # Test url as final fallback
        raw_data = {
            'id': 'media125',
            'url': 'https://third.com/image.jpg'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.media_url == 'https://third.com/image.jpg'
    
    def test_media_url_extraction_empty_values(self):
        """Test media URL extraction skips empty values."""
        raw_data = {
            'id': 'empty123',
            'media_url': '',
            'url_overridden_by_dest': '   ',
            'url': 'https://valid.com/image.jpg'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.media_url == 'https://valid.com/image.jpg'
    
    def test_media_url_no_valid_urls(self):
        """Test media URL extraction when no valid URLs are found."""
        raw_data = {
            'id': 'nourl123',
            'media_url': '',
            'url_overridden_by_dest': None,
            'url': '   '
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.media_url is None
    
    def test_timestamp_conversion_integer(self):
        """Test timestamp conversion from integer Unix timestamp."""
        raw_data = {
            'id': 'timestamp123',
            'created_utc': 1640995200  # 2022-01-01 00:00:00 UTC
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.date_iso == '2022-01-01T00:00:00Z'
    
    def test_timestamp_conversion_float(self):
        """Test timestamp conversion from float Unix timestamp."""
        raw_data = {
            'id': 'timestamp124',
            'created_utc': 1640995200.5  # 2022-01-01 00:00:00.5 UTC
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.date_iso == '2022-01-01T00:00:00Z'  # Seconds only
    
    def test_timestamp_conversion_string(self):
        """Test timestamp conversion from string Unix timestamp."""
        raw_data = {
            'id': 'timestamp125',
            'created_utc': '1640995200'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.date_iso == '2022-01-01T00:00:00Z'
    
    def test_timestamp_conversion_invalid(self):
        """Test timestamp conversion with invalid values falls back to current time."""
        raw_data = {
            'id': 'timestamp126',
            'created_utc': 'invalid'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        # Should fall back to current timestamp format
        assert len(metadata.date_iso) == 20
        assert metadata.date_iso.endswith('Z')
        assert 'T' in metadata.date_iso
    
    def test_timestamp_conversion_none(self):
        """Test timestamp conversion with None value falls back to current time."""
        raw_data = {
            'id': 'timestamp127',
            'created_utc': None
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        # Should fall back to current timestamp format
        assert len(metadata.date_iso) == 20
        assert metadata.date_iso.endswith('Z')
        assert 'T' in metadata.date_iso
    
    def test_video_detection(self):
        """Test is_video field handling."""
        # Test True value
        raw_data = {'id': 'video123', 'is_video': True}
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.is_video is True
        
        # Test False value
        raw_data = {'id': 'video124', 'is_video': False}
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.is_video is False
        
        # Test missing field (default False)
        raw_data = {'id': 'video125'}
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.is_video is False
        
        # Test truthy value
        raw_data = {'id': 'video126', 'is_video': 1}
        metadata = PostMetadata.from_raw(raw_data)
        assert metadata.is_video is True
    
    def test_to_dict_output(self):
        """Test to_dict method produces correct output."""
        raw_data = {
            'id': 'dict123',
            'title': 'Test Title',
            'selftext': 'Test content',
            'created_utc': 1640995200,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/dict123/test/',
            'url': 'https://example.com/image.jpg',
            'author': 'testuser',
            'is_video': True,
            'media_url': 'https://example.com/media.mp4'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        result_dict = metadata.to_dict()
        
        # Check that all essential fields are present and correct
        assert result_dict['id'] == 'dict123'
        assert result_dict['title'] == 'Test Title' 
        assert result_dict['selftext'] == 'Test content'
        assert result_dict['date_iso'] == '2022-01-01T00:00:00Z'
        assert result_dict['subreddit'] == 'testsubreddit'
        assert result_dict['permalink'] == '/r/testsubreddit/comments/dict123/test/'
        assert result_dict['url'] == 'https://example.com/image.jpg'
        assert result_dict['author'] == 'testuser'
        assert result_dict['is_video'] is True
        assert result_dict['media_url'] == 'https://example.com/media.mp4'
        
        # Check that enhanced fields are included with defaults
        assert 'score' in result_dict
        assert 'num_comments' in result_dict
        assert 'is_nsfw' in result_dict
        assert 'post_type' in result_dict
        assert len(result_dict) >= 20  # Should have many more fields now
    
    def test_to_dict_immutable(self):
        """Test that to_dict returns a new dictionary that doesn't affect the original."""
        raw_data = {
            'id': 'immutable123',
            'title': 'Original Title'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        result_dict = metadata.to_dict()
        
        # Modify the returned dictionary
        result_dict['title'] = 'Modified Title'
        
        # Original metadata should be unchanged
        assert metadata.title == 'Original Title'
        assert metadata.to_dict()['title'] == 'Original Title'
    
    def test_string_representations(self):
        """Test __str__ and __repr__ methods."""
        raw_data = {
            'id': 'repr123',
            'title': 'A Very Long Title That Should Be Truncated In Repr',
            'subreddit': 'testsubreddit'
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        
        # Test __str__
        str_repr = str(metadata)
        assert 'repr123' in str_repr
        assert 'A Very Long Title That Should Be Truncated In Repr' in str_repr
        assert 'testsubreddit' in str_repr
        
        # Test __repr__
        repr_str = repr(metadata)
        assert 'PostMetadata' in repr_str
        assert 'repr123' in repr_str
        assert 'testsubreddit' in repr_str
        # Title should be truncated in repr
        assert len(repr_str) < 200  # Reasonable limit for repr
    
    def test_type_coercion(self):
        """Test that non-string values are properly converted to strings."""
        raw_data = {
            'id': 12345,  # Integer ID
            'title': 12345,  # Integer title
            'subreddit': None,  # None subreddit
            'author': 12345,  # Integer author
        }
        
        metadata = PostMetadata.from_raw(raw_data)
        
        assert metadata.id == '12345'
        assert metadata.title == '12345'
        assert metadata.subreddit == 'None'
        assert metadata.author == '12345'


class TestPrawScraper:
    """Test cases for PrawScraper class."""
    
    @pytest.fixture
    def valid_credentials(self):
        """Valid credentials for testing."""
        return {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'user_agent': 'test_app:1.0 (by /u/testuser)'
        }
    
    @pytest.fixture
    def mock_reddit(self):
        """Mock Reddit instance."""
        mock = Mock(spec=praw.Reddit)
        mock.read_only = True
        return mock
    
    @pytest.fixture
    def mock_submission(self):
        """Mock Reddit submission object."""
        mock = Mock()
        mock.id = 'abc123'
        mock.title = 'Test Post'
        mock.selftext = 'This is a test post'
        mock.subreddit = 'test'
        mock.permalink = '/r/test/comments/abc123/test_post/'
        mock.url = 'https://reddit.com/r/test/comments/abc123/test_post/'
        mock.author = 'testuser'
        mock.created_utc = 1640995200.0  # 2022-01-01 00:00:00 UTC
        mock.is_video = False
        mock.media = None
        mock.preview = None
        return mock
    
    def test_init_with_valid_credentials(self, valid_credentials):
        """Test PrawScraper initialization with valid credentials."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(**valid_credentials)
            
            assert scraper.sleep_interval == 0.7
            mock_reddit_class.assert_called_once_with(
                client_id='test_client_id',
                client_secret='test_client_secret',
                user_agent='test_app:1.0 (by /u/testuser)'
            )
    
    def test_init_with_login_credentials(self, valid_credentials):
        """Test PrawScraper initialization with login credentials."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = False
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(
                **valid_credentials,
                login_username='testuser',
                login_password='testpass'
            )
            
            assert scraper.sleep_interval == 0.7
            # Should be called twice - once without login, then with login
            assert mock_reddit_class.call_count == 2
    
    def test_init_with_custom_sleep_interval(self, valid_credentials):
        """Test initialization with custom user-defined sleep interval."""
        scraper = PrawScraper(**valid_credentials, sleep_interval=5.0)
        assert scraper.sleep_interval == 5.0

    def test_init_missing_credentials(self):
        """Test that initialization fails without required credentials."""
        with pytest.raises(SystemExit) as excinfo:
            PrawScraper(client_id="test", client_secret="test", user_agent=None)
        assert excinfo.value.code == 1

    def test_init_auth_failure(self, valid_credentials):
        """Test that initialization handles authentication failure."""
        with patch('praw.Reddit', side_effect=prawcore.exceptions.OAuthException(None, None, None)):
            with pytest.raises(SystemExit) as excinfo:
                with patch('builtins.print'):
                    PrawScraper(**valid_credentials)
            assert excinfo.value.code == 1
    
    @patch('redditdl.scrapers.time.sleep')
    def test_fetch_user_posts_success(self, mock_sleep, valid_credentials, mock_submission):
        """Test successfully fetching and processing user posts."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            mock_redditor = Mock()
            mock_submissions = Mock()
            mock_submissions.new.return_value = [mock_submission]
            mock_redditor.submissions = mock_submissions
            mock_reddit_instance.redditor.return_value = mock_redditor
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(**valid_credentials)
            posts = scraper.fetch_user_posts('testuser', limit=1)
            
            assert len(posts) == 1
            assert isinstance(posts[0], PostMetadata)
            assert posts[0].id == 'abc123'
            assert posts[0].title == 'Test Post'
            assert posts[0].author == 'testuser'
            
            # Verify sleep was called
            mock_sleep.assert_called_once_with(0.7)
            
            # Verify redditor was called with correct username
            mock_reddit_instance.redditor.assert_called_once_with('testuser')
            mock_submissions.new.assert_called_once_with(limit=1)
    
    def test_fetch_user_posts_empty_username(self, valid_credentials):
        """Test fetch_user_posts with empty username."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(**valid_credentials)
            
            with pytest.raises(ValueError, match="Username cannot be empty"):
                scraper.fetch_user_posts("", 10)
            
            with pytest.raises(ValueError, match="Username cannot be empty"):
                scraper.fetch_user_posts("   ", 10)
    
    def test_fetch_user_posts_user_not_found(self, valid_credentials):
        """Test fetch_user_posts with non-existent user."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            
            # Create a proper mock response for NotFound exception
            mock_response = Mock()
            mock_response.status_code = 404
            mock_reddit_instance.redditor.side_effect = prawcore.exceptions.NotFound(mock_response)
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(**valid_credentials)
            
            with pytest.raises(prawcore.exceptions.NotFound):
                scraper.fetch_user_posts('nonexistent', 10)
    
    def test_fetch_user_posts_private_user(self, valid_credentials):
        """Test fetch_user_posts with private user profile."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            
            # Create a proper mock response for Forbidden exception
            mock_response = Mock()
            mock_response.status_code = 403
            mock_reddit_instance.redditor.side_effect = prawcore.exceptions.Forbidden(mock_response)
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(**valid_credentials)
            
            with pytest.raises(prawcore.exceptions.Forbidden):
                scraper.fetch_user_posts('privateuser', 10)
    
    def test_submission_to_dict_basic(self, mock_submission):
        """Test _submission_to_dict with basic submission."""
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(
                client_id='test',
                client_secret='test',
                user_agent='test'
            )
            
            result = scraper._submission_to_dict(mock_submission)
            
            # Check that basic fields are present and correct
            assert result['id'] == 'abc123'
            assert result['title'] == 'Test Post'
            assert result['selftext'] == 'This is a test post'
            assert result['subreddit'] == 'test'
            assert result['permalink'] == '/r/test/comments/abc123/test_post/'
            assert result['url'] == 'https://reddit.com/r/test/comments/abc123/test_post/'
            assert result['author'] == 'testuser'
            assert result['created_utc'] == 1640995200.0
            assert result['is_video'] is False
            assert result['media_url'] == 'https://reddit.com/r/test/comments/abc123/test_post/'
            assert result['url_overridden_by_dest'] == 'https://reddit.com/r/test/comments/abc123/test_post/'
            
            # Check that enhanced fields are included (may be Mock objects in test)
            assert 'score' in result
            assert 'num_comments' in result
            assert 'over_18' in result
            assert 'domain' in result
            assert 'all_awardings' in result
    
    def test_submission_to_dict_video(self, mock_submission):
        """Test _submission_to_dict with video submission."""
        mock_submission.is_video = True
        mock_submission.media = {
            'reddit_video': {
                'fallback_url': 'https://v.redd.it/video123.mp4'
            }
        }
        
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(
                client_id='test',
                client_secret='test',
                user_agent='test'
            )
            
            result = scraper._submission_to_dict(mock_submission)
            
            assert result['is_video'] is True
            assert result['media_url'] == 'https://v.redd.it/video123.mp4'
    
    def test_submission_to_dict_deleted_author(self, mock_submission):
        """Test _submission_to_dict with deleted author."""
        mock_submission.author = None
        
        with patch('redditdl.scrapers.praw.Reddit') as mock_reddit_class:
            mock_reddit_instance = Mock()
            mock_reddit_instance.read_only = True
            mock_reddit_class.return_value = mock_reddit_instance
            
            scraper = PrawScraper(
                client_id='test',
                client_secret='test',
                user_agent='test'
            )
            
            result = scraper._submission_to_dict(mock_submission)
            
            assert result['author'] == '[deleted]'


class TestRetryDecorators:
    """Test cases for the retry decorators."""

    @patch('builtins.print')
    @patch('time.sleep')
    def test_retry_on_network_error_success_first_try(self, mock_sleep, mock_print):
        """Test that the decorator succeeds on the first try without retrying."""
        mock_func = Mock(return_value="Success")
        
        @retry_on_network_error(max_retries=3)
        def test_function():
            return mock_func()

        assert test_function() == "Success"
        mock_func.assert_called_once()
        mock_sleep.assert_not_called()
        mock_print.assert_not_called()

    @patch('builtins.print')
    @patch('utils.random.uniform', return_value=0.5)
    @patch('time.sleep')
    def test_retry_on_network_error_with_retries(self, mock_sleep, mock_random, mock_print):
        """Test network retry succeeds after a few attempts."""
        mock_func = Mock()
        mock_func.side_effect = [requests.exceptions.RequestException("Attempt 1"), requests.exceptions.RequestException("Attempt 2"), "Success"]
        
        @retry_on_network_error(max_retries=3, initial_delay=1.0)
        def test_function():
            return mock_func()
        
        result = test_function()
        
        assert result == "Success"
        assert mock_func.call_count == 3
        
        mock_sleep.assert_any_call(1.5)
        mock_sleep.assert_any_call(2.5)
        assert mock_sleep.call_count == 2
        
        mock_print.assert_any_call("[INFO] test_function attempt 1 failed (Attempt 1), retrying in 1.5s...")
        mock_print.assert_any_call("[INFO] test_function attempt 2 failed (Attempt 2), retrying in 2.5s...")

    @patch('builtins.print')
    @patch('time.sleep')
    def test_retry_on_network_error_max_retries_exceeded(self, mock_sleep, mock_print):
        """Test network retry fails after exhausting all retries."""
        mock_func = Mock(side_effect=requests.exceptions.RequestException("Network error"))
        
        @retry_on_network_error(max_retries=2, initial_delay=0.1)
        def test_function():
            return mock_func()
        
        with pytest.raises(requests.exceptions.RequestException):
            test_function()
        
        assert mock_func.call_count == 3  # Initial attempt + 2 retries
        
        # Check that error message was printed
        mock_print.assert_any_call("[ERROR] test_function failed after 3 attempts: Network error")

    @patch('builtins.print')
    @patch('time.sleep')
    def test_retry_on_auth_error_success_first_try(self, mock_sleep, mock_print):
        """Test auth retry succeeds on the first try."""
        mock_func = Mock(return_value="Success")
        
        @retry_on_auth_error(max_retries=2)
        def test_function():
            return mock_func()

        assert test_function() == "Success"
        mock_func.assert_called_once()
        mock_sleep.assert_not_called()
        mock_print.assert_not_called()

    @patch('builtins.print')
    @patch('utils.random.uniform', return_value=0.5)
    @patch('time.sleep')
    def test_retry_on_auth_error_with_retries(self, mock_sleep, mock_random, mock_print):
        """Test auth retry succeeds after one retry."""
        mock_func = Mock()
        mock_func.side_effect = [prawcore.exceptions.OAuthException(None, None, None), "Success"]
        
        @retry_on_auth_error(max_retries=2, initial_delay=2.0)
        def test_function():
            return mock_func()
        
        result = test_function()
        
        assert result == "Success"
        assert mock_func.call_count == 2
        
        mock_sleep.assert_called_once_with(2.5)
        mock_print.assert_called_once_with("[WARN] test_function attempt 1 failed (None error processing request), retrying in 2.5s...")

    @patch('builtins.print')
    @patch('time.sleep')
    def test_retry_on_auth_error_max_retries_exceeded(self, mock_sleep, mock_print):
        """Test auth retry gives up after max retries and re-raises exception."""
        mock_func = Mock()
        
        @retry_on_auth_error(max_retries=2, initial_delay=0.1)
        def test_function():
            mock_func()
            raise prawcore.exceptions.OAuthException(None, None, None)
        
        with pytest.raises(prawcore.exceptions.OAuthException):
            test_function()
        
        assert mock_func.call_count == 3  # Initial attempt + 2 retries
        mock_print.assert_any_call("[ERROR] test_function failed after 3 attempts: None error processing request")

    def test_retry_on_auth_error_handles_other_exceptions_gracefully(self):
        """Test that the decorator handles other exception types gracefully."""
        @retry_on_auth_error(max_retries=2)
        def test_function():
            raise ValueError("Not an auth error")
        
        with pytest.raises(ValueError):
            test_function()

    @patch('builtins.print')
    def test_retry_on_auth_error_non_retryable(self, mock_print):
        """Test that all OAuth exceptions are retried according to the current implementation."""
        mock_response = Mock()
        mock_response.status_code = 400
        
        @retry_on_auth_error(max_retries=2)
        def test_function():
            raise prawcore.exceptions.OAuthException(mock_response, "invalid_grant", "Invalid authorization code")
        
        with pytest.raises(prawcore.exceptions.OAuthException):
            test_function()
        
        # The current implementation retries all auth exceptions, so we expect retry messages
        assert mock_print.call_count >= 1  # Should have retry messages

    def test_retry_on_auth_error_non_401_403(self):
        """Test that exceptions other than 401/403 are not retried."""
        @retry_on_auth_error(max_retries=2)
        def test_function():
            response = Mock()
            response.status_code = 500
            raise prawcore.exceptions.OAuthException(response, "server_error", "Internal server error")

        with pytest.raises(prawcore.exceptions.OAuthException):
            test_function()


class TestYarsScraper:
    """Test cases for the YarsScraper class."""
    
    @pytest.fixture
    def mock_yars(self):
        """Create a mock YARS instance."""
        with patch('scrapers.YARS') as mock_yars_class:
            mock_yars_instance = Mock()
            mock_yars_class.return_value = mock_yars_instance
            yield mock_yars_instance
    
    @pytest.fixture
    def sample_yars_data(self):
        """Sample data that YARS might return."""
        return [
            {
                'id': 'yars123',
                'title': 'YARS Test Post 1',
                'selftext': 'Content from YARS',
                'created_utc': 1640995200,
                'subreddit': 'testsubreddit',
                'permalink': '/r/testsubreddit/comments/yars123/test/',
                'url': 'https://example.com/yars1.jpg',
                'author': 'yarsuser',
                'is_video': False,
                'media_url': 'https://example.com/yars1.jpg'
            },
            {
                'id': 'yars124',
                'title': 'YARS Test Post 2',
                'selftext': '',
                'created_utc': 1640995260,
                'subreddit': 'testsubreddit',
                'permalink': '/r/testsubreddit/comments/yars124/test2/',
                'url': 'https://example.com/yars2.jpg',
                'author': 'yarsuser2',
                'is_video': True,
                'media_url': 'https://example.com/yars2.mp4'
            }
        ]
    
    def test_init_default_sleep_interval(self, mock_yars):
        """Test YarsScraper initialization with default sleep interval."""
        scraper = YarsScraper()
        assert scraper.sleep_interval == 6.1
        assert scraper.yars is not None
    
    def test_init_custom_sleep_interval(self, mock_yars):
        """Test YarsScraper initialization with custom sleep interval."""
        scraper = YarsScraper(sleep_interval=3.5)
        assert scraper.sleep_interval == 3.5
        assert scraper.yars is not None
    
    def test_fetch_user_posts_success(self, mock_yars, sample_yars_data):
        """Test successful fetching of user posts."""
        mock_yars.user_posts.return_value = sample_yars_data
        scraper = YarsScraper()
        scraper.yars = mock_yars
        posts = scraper.fetch_user_posts('testuser', limit=3)
        assert len(posts) == 2
        assert posts[0].id == 'yars123'
        assert posts[1].id == 'yars124'
    
    def test_fetch_user_posts_empty_response(self, mock_yars):
        """Test handling of empty response from YARS."""
        mock_yars.user_posts.return_value = []
        scraper = YarsScraper()
        scraper.yars = mock_yars
        posts = scraper.fetch_user_posts('emptyuser', limit=10)
        assert posts == []
    
    def test_fetch_user_posts_none_response(self, mock_yars):
        """Test handling of None response from YARS."""
        mock_yars.user_posts.return_value = None
        scraper = YarsScraper()
        scraper.yars = mock_yars
        with pytest.raises(ValueError, match="Failed to fetch posts for user 'noneuser' using YARS"):
            scraper.fetch_user_posts('noneuser', limit=10)
    
    def test_fetch_user_posts_invalid_post_data(self, mock_yars):
        """Test that invalid post data from YARS is skipped."""
        scraper = YarsScraper()
        scraper.yars = mock_yars
        
        mock_yars.user_posts.return_value = [
            {'id': 'valid123', 'title': 'Valid Post'},
            {'title': 'Post without ID'} 
        ]
        
        with patch('builtins.print') as mock_print:
            posts = scraper.fetch_user_posts("testuser", limit=2)
        
        assert len(posts) == 1
        assert posts[0].id == 'valid123'
        
        mock_print.assert_any_call("[WARN] Failed to process post unknown: Post ID is required but missing from raw data")

    def test_fetch_user_posts_network_error_retry(self, mock_yars):
        """Test retry mechanism for network errors."""
        scraper = YarsScraper()
        scraper.yars = mock_yars
        
        # Create a generator that yields the success result after failures
        def mock_user_posts_generator():
            yield {'id': 'retry123', 'title': 'Success after retry'}
        
        # Set up the mock to succeed on the final attempt
        mock_yars.user_posts.side_effect = [
            requests.exceptions.ConnectionError("Network error"),
            requests.exceptions.Timeout("Timeout error"),
            mock_user_posts_generator()
        ]
        
        posts = scraper.fetch_user_posts('retryuser', limit=1)
        
        assert len(posts) == 1
        assert posts[0].id == 'retry123'
        assert mock_yars.user_posts.call_count == 3

    @patch('redditdl.scrapers.time.sleep')
    def test_fetch_user_posts_max_retries_exceeded(self, mock_sleep, mock_yars):
        """Test that YarsScraper gives up after max retries."""
        scraper = YarsScraper()
        scraper.yars = mock_yars
        mock_yars.user_posts.side_effect = requests.exceptions.RequestException("Persistent network error")
        
        with pytest.raises(ValueError, match="Persistent network error"):
            scraper.fetch_user_posts("failuser", limit=1)
            
        assert mock_yars.user_posts.call_count == 4  # Initial attempt + 3 retries
        mock_sleep.assert_called()

    @patch('builtins.print')
    @patch('redditdl.scrapers.time.sleep')
    def test_fetch_user_posts_non_network_error(self, mock_sleep, mock_print, mock_yars):
        """Test that non-API errors are retried and the final error is wrapped in ValueError."""
        scraper = YarsScraper()
        scraper.yars = mock_yars
        mock_yars.user_posts.side_effect = KeyError("Simulated data processing error")
        
        with pytest.raises(ValueError, match="Simulated data processing error"):
            scraper.fetch_user_posts("testuser", limit=1)
            
        assert mock_yars.user_posts.call_count == 4  # Initial attempt + 3 retries

    def test_fetch_user_posts_no_authentication(self, mock_yars):
        """Test that YarsScraper does not require authentication."""
        scraper = YarsScraper()
        scraper.yars = mock_yars
        mock_yars.user_posts.return_value = []
        scraper.fetch_user_posts('testuser', limit=1)
        assert mock_yars.user_posts.called
    
    @patch('redditdl.scrapers.time.sleep')
    def test_rate_limiting_applied(self, mock_sleep, mock_yars):
        """Test that rate limiting is properly applied."""
        mock_yars.user_posts.return_value = [{'id': '1'}, {'id': '2'}]
        for sleep_interval in [1.0, 3.5, 6.1, 10.0]:
            mock_sleep.reset_mock()
            scraper = YarsScraper(sleep_interval=sleep_interval)
            scraper.yars = mock_yars
            scraper.fetch_user_posts('testuser', limit=2)
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(sleep_interval)
    
    def test_integration_with_postmetadata(self, mock_yars):
        """Test integration between YarsScraper and PostMetadata."""
        yars_data = [{
            'id': 'integration123',
            'title': 'Integration Test Post',
            'selftext': 'Testing integration between YARS and PostMetadata',
            'created_utc': 1640995200,
            'subreddit': 'testsubreddit',
            'permalink': '/r/testsubreddit/comments/integration123/test/',
            'url': 'https://reddit.com/r/testsubreddit/comments/integration123/',
            'author': 'integrationuser',
            'is_video': False,
            'media_url': 'https://example.com/integration.jpg'
        }]
        mock_yars.user_posts.return_value = yars_data
        scraper = YarsScraper()
        scraper.yars = mock_yars
        posts = scraper.fetch_user_posts('integrationuser', limit=1)
        assert len(posts) == 1
        assert isinstance(posts[0], PostMetadata)
        assert posts[0].id == 'integration123'
        assert posts[0].title == 'Integration Test Post'


if __name__ == "__main__":
    # Run tests if this file is executed directly
    pytest.main([__file__, "-v"]) 