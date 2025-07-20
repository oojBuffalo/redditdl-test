#!/usr/bin/env python3
"""
Tests for MediaDownloader class.

Comprehensive test suite covering MediaDownloader functionality including
download operations, metadata embedding, error handling, and rate limiting.
"""

import sys
from pathlib import Path

# Add project root to path
# sys.path.insert(0, str(Path(__file__).resolve().parents[1])) # This line is no longer needed

import os
import time
import tempfile
from unittest.mock import Mock, patch, mock_open, MagicMock
import pytest
import requests

# Import the classes we're testing
from redditdl.downloader import MediaDownloader
from redditdl.metadata import MetadataEmbedder


class TestMediaDownloaderInit:
    """Test MediaDownloader initialization."""
    
    def test_init_with_valid_params(self, tmp_path):
        """Test initialization with valid parameters."""
        embedder = Mock(spec=MetadataEmbedder)
        downloader = MediaDownloader(
            outdir=tmp_path,
            sleep_interval=2.0,
            embedder=embedder
        )
        
        assert downloader.outdir == tmp_path
        assert downloader.sleep_interval == 2.0
        assert downloader.embedder is embedder
        assert tmp_path.exists()
    
    def test_init_creates_output_directory(self, tmp_path):
        """Test that output directory is created if it doesn't exist."""
        new_dir = tmp_path / "downloads" / "test"
        
        downloader = MediaDownloader(outdir=new_dir)
        
        assert new_dir.exists()
        assert downloader.outdir == new_dir
    
    def test_init_with_none_embedder(self, tmp_path):
        """Test initialization with None embedder."""
        downloader = MediaDownloader(outdir=tmp_path, embedder=None)
        
        assert downloader.embedder is None
        assert downloader.sleep_interval == 1.0  # Default value
    
    def test_init_default_sleep_interval(self, tmp_path):
        """Test default sleep interval value."""
        downloader = MediaDownloader(outdir=tmp_path)
        
        assert downloader.sleep_interval == 1.0
    
    @patch('os.makedirs')
    def test_init_directory_creation_failure(self, mock_makedirs, tmp_path):
        """Test handling of directory creation failure."""
        mock_makedirs.side_effect = OSError("Permission denied")
        
        with pytest.raises(OSError, match="Failed to create output directory"):
            MediaDownloader(outdir=tmp_path / "restricted")


class TestMediaDownloaderDownload:
    """Test MediaDownloader download functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.embedder = Mock(spec=MetadataEmbedder)
        self.downloader = MediaDownloader(
            outdir=self.tmp_dir,
            sleep_interval=0.1,  # Fast tests
            embedder=self.embedder
        )
        self.test_metadata = {
            'id': 'test123',
            'title': 'Test Post',
            'author': 'test_user'
        }
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
    
    @patch('downloader.requests.get')
    @patch('downloader.time.sleep')
    def test_successful_download(self, mock_sleep, mock_get):
        """Test successful file download."""
        # Mock successful response
        mock_response = Mock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b'test content chunk 1', b'test content chunk 2']
        mock_get.return_value = mock_response
        
        # Mock file operations
        with patch('builtins.open', mock_open()) as mock_file:
            result = self.downloader.download(
                'https://example.com/test.jpg',
                'test_image.jpg',
                self.test_metadata
            )
        
        # Verify download was attempted
        mock_get.assert_called_once_with(
            'https://example.com/test.jpg',
            stream=True,
            headers={'User-Agent': 'RedditDL/1.0 (Media Downloader Bot)'},
            timeout=30
        )
        
        # Verify file was written
        mock_file.assert_called_once()
        handle = mock_file()
        assert handle.write.call_count == 2  # Two chunks
        handle.write.assert_any_call(b'test content chunk 1')
        handle.write.assert_any_call(b'test content chunk 2')
        
        # Verify sleep was called for rate limiting
        mock_sleep.assert_called_once_with(0.1)
        
        # Verify result path
        expected_path = self.tmp_dir / 'test_image.jpg'
        assert result == expected_path
    
    @patch('downloader.requests.get')
    @patch('downloader.time.sleep')
    def test_download_404_error(self, mock_sleep, mock_get):
        """Test handling of 404 errors."""
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        with patch('builtins.print') as mock_print:
            result = self.downloader.download(
                'https://example.com/missing.jpg',
                'missing_image.jpg',
                self.test_metadata
            )
        
        # Verify warning was printed
        mock_print.assert_called_with('[WARN] Media not found (404): https://example.com/missing.jpg')
        
        # Still returns expected path
        expected_path = self.tmp_dir / 'missing_image.jpg'
        assert result == expected_path
        
        # Sleep should still be called
        mock_sleep.assert_called_once_with(0.1)
    
    @patch('downloader.requests.get')
    @patch('downloader.time.sleep')
    def test_download_503_error(self, mock_sleep, mock_get):
        """Test handling of 503 service unavailable errors."""
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 503
        mock_get.return_value = mock_response
        
        with patch('builtins.print') as mock_print:
            result = self.downloader.download(
                'https://example.com/unavailable.jpg',
                'unavailable_image.jpg',
                self.test_metadata
            )
        
        # Verify warning was printed
        mock_print.assert_called_with('[WARN] Service unavailable (503): https://example.com/unavailable.jpg')
        
        # Still returns expected path
        expected_path = self.tmp_dir / 'unavailable_image.jpg'
        assert result == expected_path
    
    @patch('downloader.requests.get')
    def test_download_network_error(self, mock_get):
        """Test handling of network errors."""
        mock_get.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(requests.RequestException, match="Network error"):
            self.downloader.download(
                'https://example.com/test.jpg',
                'test_image.jpg',
                self.test_metadata
            )
    
    @patch('downloader.requests.get')
    def test_download_file_write_error(self, mock_get):
        """Test handling of file write errors."""
        mock_response = Mock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b'test content']
        mock_get.return_value = mock_response
        
        # Mock file operations to raise error
        with patch('builtins.open', side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Permission denied"):
                self.downloader.download(
                    'https://example.com/test.jpg',
                    'test_image.jpg',
                    self.test_metadata
                )
    
    def test_download_empty_url(self):
        """Test error handling for empty URL."""
        with pytest.raises(ValueError, match="Media URL cannot be empty"):
            self.downloader.download('', 'test.jpg', self.test_metadata)
        
        with pytest.raises(ValueError, match="Media URL cannot be empty"):
            self.downloader.download('   ', 'test.jpg', self.test_metadata)
    
    def test_download_empty_filename(self):
        """Test error handling for empty filename."""
        with pytest.raises(ValueError, match="Filename cannot be empty"):
            self.downloader.download('https://example.com/test.jpg', '', self.test_metadata)
        
        with pytest.raises(ValueError, match="Filename cannot be empty"):
            self.downloader.download('https://example.com/test.jpg', '   ', self.test_metadata)
    
    @patch('downloader.sanitize_filename')
    def test_filename_sanitization(self, mock_sanitize):
        """Test that filename is sanitized."""
        mock_sanitize.return_value = 'safe_filename.jpg'
        
        with patch('downloader.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.ok = False
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            
            self.downloader.download(
                'https://example.com/test.jpg',
                'unsafe/filename?.jpg',
                self.test_metadata
            )
        
        mock_sanitize.assert_called_once_with('unsafe/filename?.jpg')


class TestMediaDownloaderMetadataProcessing:
    """Test metadata processing and embedding."""
    
    def setup_method(self):
        """Set up test fixtures for metadata processing."""
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.embedder = Mock(spec=MetadataEmbedder)
        self.downloader = MediaDownloader(outdir=self.tmp_dir, embedder=self.embedder)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        
    def test_process_metadata_image_success(self):
        """Test successful metadata processing for an image."""
        filepath = self.tmp_dir / 'image.jpg'
        filepath.touch()
        
        with patch('builtins.print') as mock_print:
            self.downloader._process_metadata(
                filepath, {'is_video': False, 'id': 'test'}
            )
            
        self.embedder.embed_into_image.assert_called_once_with(filepath, {'is_video': False, 'id': 'test'})
        mock_print.assert_any_call('[INFO] Successfully embedded metadata into image: image.jpg')

    def test_process_metadata_image_fallback(self):
        """Test metadata processing fallback for jpeg."""
        filepath = self.tmp_dir / 'image.jpeg'
        filepath.touch()
        
        # Mock embed_into_image to raise exception to trigger fallback
        self.embedder.embed_into_image.side_effect = Exception("EXIF embedding failed")
        
        with patch('builtins.print') as mock_print:
            self.downloader._process_metadata(
                filepath, {'is_video': False, 'id': 'test'}
            )
            
        self.embedder.embed_into_image.assert_called_once_with(filepath, {'is_video': False, 'id': 'test'})
        self.embedder.write_sidecar.assert_called_once_with(filepath, {'is_video': False, 'id': 'test'})
        mock_print.assert_any_call('[INFO] EXIF embedding failed for image.jpeg, falling back to JSON sidecar: EXIF embedding failed')

    def test_process_metadata_video(self):
        """Test metadata processing for a video."""
        filepath = self.tmp_dir / 'video.mp4'
        filepath.touch()
        
        with patch('builtins.print') as mock_print:
            self.downloader._process_metadata(
                filepath, {'is_video': True, 'id': 'test'}
            )
            
        self.embedder.embed_into_image.assert_not_called()
        self.embedder.write_sidecar.assert_called_once_with(filepath, {'is_video': True, 'id': 'test'})
        mock_print.assert_any_call('[INFO] Created JSON metadata sidecar for: video.mp4')

    def test_process_metadata_other_format(self):
        """Test that other formats use sidecar files."""
        filepath = self.tmp_dir / 'data.gif'
        filepath.touch()
        
        with patch('builtins.print') as mock_print:
            self.downloader._process_metadata(
                filepath, {'is_video': False, 'id': 'test'}
            )
        
        self.embedder.embed_into_image.assert_not_called()
        self.embedder.write_sidecar.assert_called_once_with(filepath, {'is_video': False, 'id': 'test'})
        mock_print.assert_any_call('[INFO] Created JSON metadata sidecar for: data.gif')

    def test_process_metadata_no_embedder(self):
        """Test that nothing happens if embedder is None."""
        self.downloader.embedder = None
        filepath = self.tmp_dir / 'image.jpg'
        filepath.touch()
        
        with patch('builtins.print') as mock_print:
            self.downloader._process_metadata(
                filepath, {'is_video': False, 'id': 'test'}
            )
        
        # No calls should be made to the mock from previous setup
        self.embedder.embed_into_image.assert_not_called()
        mock_print.assert_any_call('[INFO] No metadata embedder configured, skipping metadata for image.jpg')

    def test_process_metadata_embedder_error(self):
        """Test handling of embedder errors."""
        filepath = self.tmp_dir / 'image.jpg'
        filepath.touch()
        
        # Mock embedder to raise an error - both methods fail
        self.embedder.embed_into_image.side_effect = Exception("Embedding failed")
        self.embedder.write_sidecar.side_effect = OSError("Sidecar failed")
        
        with patch('builtins.print') as mock_print:
            self.downloader._process_metadata(
                filepath, {'is_video': False, 'id': 'test'}
            )
            
        self.embedder.embed_into_image.assert_called_once()
        self.embedder.write_sidecar.assert_called_once()
        mock_print.assert_any_call('[INFO] EXIF embedding failed for image.jpg, falling back to JSON sidecar: Embedding failed')
        mock_print.assert_any_call('[ERROR] File system error processing metadata for ' + str(filepath) + ': Sidecar failed')
        mock_print.assert_any_call('[WARN] Continuing without metadata for file: image.jpg')


class TestFileExtensionDetermination:
    """Test logic for determining file extensions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.downloader = MediaDownloader(outdir=self.tmp_dir)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
    
    def test_determine_extension_from_url(self):
        """Test extension determination from URL."""
        mock_response = Mock()
        mock_response.headers = {}
        
        result = self.downloader._determine_file_extension(
            'https://example.com/image.jpg',
            mock_response
        )
        
        assert result == '.jpg'
    
    def test_determine_extension_from_content_type(self):
        """Test extension determination from Content-Type header."""
        mock_response = Mock()
        mock_response.headers = {'content-type': 'image/png'}
        
        result = self.downloader._determine_file_extension(
            'https://example.com/noextension',
            mock_response
        )
        
        assert result == '.png'
    
    def test_determine_extension_content_type_with_charset(self):
        """Test extension determination from Content-Type with charset."""
        mock_response = Mock()
        mock_response.headers = {'content-type': 'image/jpeg; charset=utf-8'}
        
        result = self.downloader._determine_file_extension(
            'https://example.com/image',
            mock_response
        )
        
        assert result == '.jpg'
    
    def test_determine_extension_fallback(self):
        """Test fallback to .bin when no extension can be determined."""
        mock_response = Mock()
        mock_response.headers = {}
        
        result = self.downloader._determine_file_extension(
            'https://example.com/unknown',
            mock_response
        )
        
        assert result == '.bin'
    
    def test_determine_extension_video_content_type(self):
        """Test video content type mapping."""
        mock_response = Mock()
        mock_response.headers = {'content-type': 'video/mp4'}
        
        result = self.downloader._determine_file_extension(
            'https://example.com/video',
            mock_response
        )
        
        assert result == '.mp4'


class TestSleepInterval:
    """Test sleep interval functionality."""
    
    def setup_method(self):
        """Set up test fixtures for sleep interval tests."""
        self.tmp_dir = tempfile.mkdtemp()
        self.downloader = MediaDownloader(outdir=self.tmp_dir, sleep_interval=0.1)
        self.test_metadata = {'id': 'sleep_test'}

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.tmp_dir)

    @patch('downloader.time.sleep')
    @patch('downloader.requests.get')
    def test_sleep_interval_respected(self, mock_get, mock_sleep):
        """Test that sleep interval is respected."""
        downloader = MediaDownloader(outdir=self.tmp_dir, sleep_interval=2.5)
        
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        downloader.download('https://example.com/test.jpg', 'test.jpg', {})
        
        # Verify sleep was called with correct interval
        mock_sleep.assert_called_once_with(2.5)
    
    @patch('downloader.time.sleep')
    @patch('downloader.requests.get')
    def test_sleep_called_even_on_error(self, mock_get, mock_sleep):
        """Test that sleep is called even if download fails."""
        mock_get.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(requests.RequestException):
            self.downloader.download(
                'https://example.com/test.jpg',
                'test_image.jpg',
                self.test_metadata
            )
        
        # Assert that sleep was called (will be called multiple times due to retry mechanism)
        # The retry decorator calls sleep for backoff, plus the final sleep in the finally block
        assert mock_sleep.call_count >= 4  # At least 3 retries + 1 final sleep
        # Check that the final sleep call uses the configured interval
        mock_sleep.assert_any_call(0.1)


if __name__ == '__main__':
    pytest.main([__file__])