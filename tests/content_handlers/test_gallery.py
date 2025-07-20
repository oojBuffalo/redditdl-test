"""
Tests for Gallery Content Handler

Tests the GalleryContentHandler functionality including multi-image download,
gallery directory creation, metadata preservation, and error handling.
"""

import pytest
import asyncio
import json
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path
from typing import List, Dict, Any

from redditdl.content_handlers.gallery import GalleryContentHandler
from redditdl.content_handlers.base import HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.downloader import MediaDownloader
from redditdl.metadata import MetadataEmbedder


class TestGalleryContentHandler:
    """Test GalleryContentHandler functionality."""
    
    @pytest.fixture
    def handler(self):
        """Create a GalleryContentHandler instance."""
        return GalleryContentHandler(priority=40)
    
    @pytest.fixture
    def gallery_post(self):
        """Create a sample gallery post."""
        return PostMetadata(
            id="gallery123",
            title="Amazing Gallery Post",
            author="gallery_user",
            subreddit="pics",
            url="https://reddit.com/gallery/gallery123",
            date_iso="2023-06-15T10:30:00Z",
            gallery_image_urls=[
                "https://i.redd.it/image1.jpg",
                "https://i.redd.it/image2.png", 
                "https://i.redd.it/image3.gif",
                "https://i.redd.it/image4.webp"
            ],
            score=1500,
            num_comments=45,
            is_nsfw=False
        )
    
    @pytest.fixture
    def empty_gallery_post(self):
        """Create a gallery post with no image URLs."""
        return PostMetadata(
            id="empty_gallery",
            title="Empty Gallery",
            author="test_user",
            subreddit="test",
            url="https://reddit.com/gallery/empty_gallery",
            date_iso="2023-06-15T10:30:00Z",
            gallery_image_urls=[],
            score=10,
            num_comments=2
        )
    
    @pytest.fixture
    def temp_output_dir(self, tmp_path):
        """Create a temporary output directory."""
        return tmp_path / "gallery_test"
    
    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return {
            'sleep_interval': 0.1,
            'embed_metadata': True,
            'max_filename_length': 200
        }

    def test_supported_content_types(self, handler):
        """Test that handler supports gallery content type."""
        assert 'gallery' in handler.supported_content_types
        assert len(handler.supported_content_types) == 1

    def test_can_handle_gallery_post(self, handler, gallery_post):
        """Test that handler can handle gallery posts."""
        assert handler.can_handle(gallery_post, 'gallery')

    def test_can_handle_non_gallery_post(self, handler):
        """Test that handler rejects non-gallery posts."""
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
        assert not handler.can_handle(text_post, 'gallery')

    def test_can_handle_empty_gallery(self, handler, empty_gallery_post):
        """Test that handler rejects gallery posts with no URLs."""
        assert not handler.can_handle(empty_gallery_post, 'gallery')

    @pytest.mark.asyncio
    async def test_process_gallery_success(self, handler, gallery_post, temp_output_dir, config):
        """Test successful gallery processing."""
        # Mock MediaDownloader
        mock_downloader = Mock(spec=MediaDownloader)
        mock_downloader.embedder = Mock(spec=MetadataEmbedder)
        
        # Mock successful downloads
        def mock_download(url, filename, metadata):
            file_path = temp_output_dir / f"gallery_dir/{filename}"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch()
            return file_path
        
        mock_downloader.download = Mock(side_effect=mock_download)
        
        # Mock the downloader creation
        with patch.object(handler, '_get_or_create_downloader', return_value=mock_downloader):
            result = await handler.process(gallery_post, temp_output_dir, config)
        
        # Verify result
        assert result.success
        assert result.handler_name == "gallery"
        assert result.content_type == "gallery"
        assert "gallery_download" in result.operations_performed
        assert result.metadata_embedded
        assert len(result.files_created) >= 4  # 4 images + metadata file

    @pytest.mark.asyncio
    async def test_process_empty_gallery_error(self, handler, empty_gallery_post, temp_output_dir, config):
        """Test that empty galleries raise an error."""
        with pytest.raises(HandlerError, match="No gallery URLs found"):
            await handler.process(empty_gallery_post, temp_output_dir, config)

    @pytest.mark.asyncio
    async def test_process_with_download_failures(self, handler, gallery_post, temp_output_dir, config):
        """Test gallery processing with some download failures."""
        # Mock MediaDownloader with partial failures
        mock_downloader = Mock(spec=MediaDownloader)
        mock_downloader.embedder = Mock(spec=MetadataEmbedder)
        
        # Mock mixed success/failure downloads
        download_results = [
            temp_output_dir / "gallery_dir/01_image.jpg",  # Success
            None,  # Failure
            temp_output_dir / "gallery_dir/03_image.gif",  # Success  
            None   # Failure
        ]
        
        def mock_download(url, filename, metadata):
            result = download_results.pop(0)
            if result:
                result.parent.mkdir(parents=True, exist_ok=True)
                result.touch()
            return result
        
        mock_downloader.download = Mock(side_effect=mock_download)
        
        with patch.object(handler, '_get_or_create_downloader', return_value=mock_downloader):
            result = await handler.process(gallery_post, temp_output_dir, config)
        
        # Should still succeed if at least one download worked
        assert result.success
        assert len([f for f in result.files_created if f.suffix in ['.jpg', '.gif']]) == 2

    def test_create_gallery_directory(self, handler, gallery_post, temp_output_dir, config):
        """Test gallery directory creation."""
        gallery_dir = handler._create_gallery_directory(gallery_post, temp_output_dir, config)
        
        assert gallery_dir.exists()
        assert gallery_dir.is_dir()
        assert gallery_post.id in gallery_dir.name
        assert "Amazing_Gallery_Post" in gallery_dir.name or "Amazing" in gallery_dir.name

    def test_construct_image_filename(self, handler, gallery_post, config):
        """Test image filename construction with proper numbering."""
        # Test different image indices with proper padding
        filename1 = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image1.jpg", 1, 4, config)
        filename2 = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image2.png", 2, 4, config)
        filename10 = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image10.gif", 10, 15, config)
        
        # Verify proper padding and extensions
        assert filename1 == "1_image.jpg"
        assert filename2 == "2_image.png"
        assert filename10 == "10_image.gif"
        
        # Test with larger numbers requiring more padding
        filename_large = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image5.webp", 5, 100, config)
        assert filename_large == "005_image.webp"

    def test_construct_image_filename_no_extension(self, handler, gallery_post, config):
        """Test filename construction when URL has no extension."""
        filename = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/no_extension", 1, 3, config)
        
        assert filename == "1_image.jpg"  # Default extension

    def test_create_gallery_metadata(self, handler, gallery_post, temp_output_dir, config):
        """Test gallery metadata file creation."""
        gallery_dir = temp_output_dir / "gallery_test"
        gallery_dir.mkdir(parents=True, exist_ok=True)
        
        gallery_urls = gallery_post.gallery_image_urls
        metadata_path = handler._create_gallery_metadata(gallery_post, gallery_urls, gallery_dir, config)
        
        assert metadata_path.exists()
        assert metadata_path.name == "gallery_metadata.json"
        
        # Verify metadata content
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        assert 'post_data' in metadata
        assert 'gallery_info' in metadata
        assert metadata['gallery_info']['total_images'] == 4
        assert len(metadata['gallery_info']['image_urls']) == 4
        assert metadata['gallery_info']['handler'] == "gallery"
        assert metadata['post_data']['id'] == "gallery123"

    def test_get_or_create_downloader(self, handler, temp_output_dir, config):
        """Test downloader creation and configuration."""
        downloader = handler._get_or_create_downloader(temp_output_dir, config)
        
        assert isinstance(downloader, MediaDownloader)
        assert downloader.outdir == temp_output_dir
        assert downloader.sleep_interval == config['sleep_interval']
        assert downloader.embedder is not None  # Should have embedder when embed_metadata=True

    def test_get_or_create_downloader_no_metadata(self, handler, temp_output_dir):
        """Test downloader creation without metadata embedding."""
        config = {'embed_metadata': False, 'sleep_interval': 0.5}
        downloader = handler._get_or_create_downloader(temp_output_dir, config)
        
        assert isinstance(downloader, MediaDownloader)
        assert downloader.embedder is None

    def test_validate_config_valid(self, handler):
        """Test configuration validation with valid config."""
        valid_config = {
            'sleep_interval': 1.0,
            'embed_metadata': True
        }
        errors = handler.validate_config(valid_config)
        assert errors == []

    def test_validate_config_invalid_sleep_interval(self, handler):
        """Test configuration validation with invalid sleep interval."""
        invalid_config = {
            'sleep_interval': -1.0,
            'embed_metadata': True
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "sleep_interval must be non-negative" in errors[0]

    def test_validate_config_invalid_embed_metadata(self, handler):
        """Test configuration validation with invalid embed_metadata."""
        invalid_config = {
            'sleep_interval': 1.0,
            'embed_metadata': "not_a_boolean"
        }
        errors = handler.validate_config(invalid_config)
        assert len(errors) == 1
        assert "embed_metadata must be a boolean" in errors[0]

    @pytest.mark.asyncio
    async def test_process_with_metadata_creation_failure(self, handler, gallery_post, temp_output_dir, config):
        """Test handling of metadata creation failure."""
        # Mock MediaDownloader 
        mock_downloader = Mock(spec=MediaDownloader)
        mock_downloader.embedder = Mock(spec=MetadataEmbedder)
        
        def mock_download(url, filename, metadata):
            file_path = temp_output_dir / f"gallery_dir/{filename}"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch()
            return file_path
        
        mock_downloader.download = Mock(side_effect=mock_download)
        
        # Mock metadata creation failure
        with patch.object(handler, '_get_or_create_downloader', return_value=mock_downloader), \
             patch.object(handler, '_create_gallery_metadata', return_value=None):
            
            result = await handler.process(gallery_post, temp_output_dir, config)
        
        # Should still succeed even if metadata creation fails
        assert result.success
        assert "gallery_download" in result.operations_performed
        # Should not have gallery_metadata operation since it failed
        assert "gallery_metadata" not in result.operations_performed

    @pytest.mark.asyncio 
    async def test_process_complete_failure(self, handler, gallery_post, temp_output_dir, config):
        """Test complete failure scenario."""
        # Mock MediaDownloader with all downloads failing
        mock_downloader = Mock(spec=MediaDownloader)
        mock_downloader.embedder = None
        mock_downloader.download = Mock(return_value=None)  # All downloads fail
        
        with patch.object(handler, '_get_or_create_downloader', return_value=mock_downloader):
            result = await handler.process(gallery_post, temp_output_dir, config)
        
        # Should fail when no downloads succeed
        assert not result.success
        assert "No images downloaded from gallery" in result.error_message

    def test_handler_properties(self, handler):
        """Test handler basic properties."""
        assert handler.name == "gallery"
        assert handler.priority == 40
        assert isinstance(handler.supported_content_types, set)

    @pytest.mark.asyncio
    async def test_process_exception_handling(self, handler, gallery_post, temp_output_dir, config):
        """Test exception handling during processing."""
        # Mock downloader creation to raise an exception
        with patch.object(handler, '_get_or_create_downloader', side_effect=Exception("Downloader creation failed")):
            with pytest.raises(HandlerError, match="Gallery processing failed"):
                await handler.process(gallery_post, temp_output_dir, config)

    def test_large_gallery_filename_construction(self, handler, gallery_post, config):
        """Test filename construction for large galleries (proper padding)."""
        # Test gallery with 100+ images
        filename_5_of_150 = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image5.jpg", 5, 150, config)
        filename_50_of_150 = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image50.jpg", 50, 150, config)
        filename_150_of_150 = handler._construct_image_filename(
            gallery_post, "https://i.redd.it/image150.jpg", 150, 150, config)
        
        # Verify proper 3-digit padding
        assert filename_5_of_150 == "005_image.jpg"
        assert filename_50_of_150 == "050_image.jpg"
        assert filename_150_of_150 == "150_image.jpg"

    def test_gallery_directory_name_sanitization(self, handler, temp_output_dir, config):
        """Test that gallery directory names are properly sanitized."""
        # Create post with problematic characters in title
        problematic_post = PostMetadata(
            id="test123",
            title="Gallery/with\\dangerous:characters*?<>|",
            author="user",
            subreddit="test",
            url="https://reddit.com/gallery/test123",
            date_iso="2023-06-15T10:30:00Z",
            gallery_image_urls=["https://i.redd.it/test.jpg"]
        )
        
        gallery_dir = handler._create_gallery_directory(problematic_post, temp_output_dir, config)
        
        # Verify directory was created and name is sanitized
        assert gallery_dir.exists()
        assert gallery_dir.is_dir()
        # Should not contain dangerous characters
        dangerous_chars = ['/', '\\', ':', '*', '?', '<', '>', '|']
        for char in dangerous_chars:
            assert char not in gallery_dir.name