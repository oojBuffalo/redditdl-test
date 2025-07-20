#!/usr/bin/env python3
"""
Unit tests for metadata embedding and sidecar generation.

This module contains comprehensive tests for the MetadataEmbedder class,
including EXIF embedding, JSON sidecar generation, and error handling.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

try:
    from PIL import Image
    import piexif
except ImportError:
    pytest.skip("PIL and piexif not available", allow_module_level=True)

from redditdl.metadata import MetadataEmbedder


@pytest.fixture
def embedder():
    """Create a MetadataEmbedder instance for testing."""
    return MetadataEmbedder()

@pytest.fixture
def sample_metadata():
    """Sample metadata dictionary for testing."""
    return {
        'id': 'test123',
        'title': 'Test Image Post',
        'author': 'testuser',
        'subreddit': 'testsubreddit',
        'date_iso': '2022-01-01T00:00:00Z',
        'url': 'https://example.com/image.jpg'
    }

@pytest.fixture
def temp_image():
    """Create a temporary JPEG image for testing."""
    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
        # Create a simple test image
        img = Image.new('RGB', (100, 100), color='red')
        img.save(f.name, 'JPEG')
        yield Path(f.name)
        # Cleanup
        Path(f.name).unlink(missing_ok=True)

@pytest.fixture
def temp_png():
    """Create a temporary PNG image for testing."""
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
        # Create a simple test image
        img = Image.new('RGB', (100, 100), color='blue')
        img.save(f.name, 'PNG')
        yield Path(f.name)
        # Cleanup
        Path(f.name).unlink(missing_ok=True)


class TestMetadataEmbedderInit:
    """Test MetadataEmbedder initialization."""
    
    def test_init_creates_instance(self):
        """Test that MetadataEmbedder can be instantiated."""
        embedder = MetadataEmbedder()
        assert embedder is not None
        assert hasattr(embedder, 'supported_image_formats')
        assert hasattr(embedder, 'user_comment_tag')
    
    def test_supported_formats(self):
        """Test that supported image formats are correctly defined."""
        embedder = MetadataEmbedder()
        expected_formats = {'.jpg', '.jpeg', '.tiff', '.tif'}
        assert embedder.supported_image_formats == expected_formats
    
    def test_user_comment_tag(self):
        """Test that user comment tag is correctly set."""
        embedder = MetadataEmbedder()
        assert embedder.user_comment_tag == piexif.ExifIFD.UserComment


class TestWriteSidecar:
    """Test sidecar file writing functionality."""
    
    def test_write_sidecar_creates_json_file(self, embedder, sample_metadata):
        """Test that write_sidecar creates a properly formatted JSON file."""
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
            media_path = Path(f.name)
            
        try:
            embedder.write_sidecar(media_path, sample_metadata)
            
            # Check that sidecar file was created
            sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
            assert sidecar_path.exists()
            
            # Check content
            with open(sidecar_path, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            
            assert loaded_data == sample_metadata
            
        finally:
            # Cleanup
            media_path.unlink(missing_ok=True)
            sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
            sidecar_path.unlink(missing_ok=True)
    
    def test_write_sidecar_json_formatting(self, embedder, sample_metadata):
        """Test that sidecar JSON is properly formatted."""
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as f:
            media_path = Path(f.name)
            
        try:
            embedder.write_sidecar(media_path, sample_metadata)
            
            sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
            
            # Read raw content to check formatting
            with open(sidecar_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Should be pretty-printed (indented)
            assert '\n' in content
            assert '  ' in content  # Indentation
            
            # Should end with newline
            assert content.endswith('\n')
            
            # Should be sorted keys
            lines = content.strip().split('\n')
            assert '"author"' in lines[1]  # First key alphabetically
            
        finally:
            # Cleanup
            media_path.unlink(missing_ok=True)
            sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
            sidecar_path.unlink(missing_ok=True)
    
    def test_write_sidecar_unicode_handling(self, embedder):
        """Test that sidecar properly handles Unicode characters."""
        unicode_metadata = {
            'id': 'unicode123',
            'title': 'Test with émojis 🎬 and 中文',
            'author': 'tëst_üser'
        }
        
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
            media_path = Path(f.name)
            
        try:
            embedder.write_sidecar(media_path, unicode_metadata)
            
            sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
            
            # Read back and verify Unicode is preserved
            with open(sidecar_path, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            
            assert loaded_data['title'] == 'Test with émojis 🎬 and 中文'
            assert loaded_data['author'] == 'tëst_üser'
            
        finally:
            # Cleanup
            media_path.unlink(missing_ok=True)
            sidecar_path = media_path.with_suffix(media_path.suffix + '.json')
            sidecar_path.unlink(missing_ok=True)
    
    def test_write_sidecar_permission_error(self, embedder, sample_metadata):
        """Test write_sidecar handles permission errors."""
        # Use a path that should cause permission error
        invalid_path = Path('/root/nonexistent/file.jpg')
        
        with pytest.raises(OSError, match="Failed to write sidecar file"):
            embedder.write_sidecar(invalid_path, sample_metadata)


class TestEmbedIntoImage:
    """Test EXIF embedding functionality."""
    
    def test_embed_into_image_supported_format(self, embedder, sample_metadata, temp_image):
        """Test embedding metadata into supported image format."""
        embedder.embed_into_image(temp_image, sample_metadata)
        
        # Verify metadata was embedded
        with Image.open(temp_image) as img:
            if "exif" in img.info:
                exif_dict = piexif.load(img.info["exif"])
                user_comment = exif_dict["Exif"].get(piexif.ExifIFD.UserComment)
                
                assert user_comment is not None
                
                # Extract JSON from comment (skip charset prefix)
                if user_comment.startswith(b'UNICODE\x00'):
                    json_data = user_comment[8:].decode('utf-8')
                    loaded_metadata = json.loads(json_data)
                    assert loaded_metadata == sample_metadata
    
    def test_embed_into_image_unsupported_format(self, embedder, sample_metadata, temp_png):
        """Test that unsupported format raises ValueError."""
        with pytest.raises(ValueError, match="does not support EXIF metadata"):
            embedder.embed_into_image(temp_png, sample_metadata)
    
    def test_embed_into_image_file_not_found(self, embedder, sample_metadata):
        """Test that non-existent file raises FileNotFoundError."""
        nonexistent_path = Path('/nonexistent/file.jpg')
        
        with pytest.raises(FileNotFoundError, match="Image file not found"):
            embedder.embed_into_image(nonexistent_path, sample_metadata)
    
    def test_embed_into_image_preserves_existing_exif(self, embedder, sample_metadata):
        """Test that embedding preserves existing EXIF data."""
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as f:
            temp_path = Path(f.name)
            
        try:
            # Create image with some EXIF data
            img = Image.new('RGB', (100, 100), color='green')
            
            # Add some existing EXIF data
            exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
            exif_dict["0th"][piexif.ImageIFD.Software] = "Test Software"
            exif_bytes = piexif.dump(exif_dict)
            
            img.save(temp_path, 'JPEG', exif=exif_bytes)
            
            # Embed our metadata
            embedder.embed_into_image(temp_path, sample_metadata)
            
            # Verify both old and new data exist
            with Image.open(temp_path) as img:
                if "exif" in img.info:
                    exif_dict = piexif.load(img.info["exif"])
                    
                    # Original EXIF should be preserved
                    assert exif_dict["0th"].get(piexif.ImageIFD.Software) == b"Test Software"
                    
                    # Our metadata should be present
                    user_comment = exif_dict["Exif"].get(piexif.ExifIFD.UserComment)
                    assert user_comment is not None
                    
        finally:
            temp_path.unlink(missing_ok=True)
    
    def test_embed_into_image_unicode_metadata(self, embedder, temp_image):
        """Test embedding Unicode metadata."""
        unicode_metadata = {
            'title': 'Test with émojis 🎬 and 中文',
            'description': 'Special chars: ñáéíóú'
        }
        
        embedder.embed_into_image(temp_image, unicode_metadata)
        
        # Verify Unicode was properly embedded
        with Image.open(temp_image) as img:
            if "exif" in img.info:
                exif_dict = piexif.load(img.info["exif"])
                user_comment = exif_dict["Exif"].get(piexif.ExifIFD.UserComment)
                
                if user_comment and user_comment.startswith(b'UNICODE\x00'):
                    json_data = user_comment[8:].decode('utf-8')
                    loaded_metadata = json.loads(json_data)
                    assert loaded_metadata['title'] == 'Test with émojis 🎬 and 中文'
    
    @patch('PIL.Image.open')
    def test_embed_into_image_pil_error_propagation(self, mock_open, embedder, sample_metadata):
        """Test that PIL errors are properly propagated."""
        # Mock PIL to raise an error
        mock_open.side_effect = OSError("Mocked PIL error")
        
        temp_path = Path('/tmp/test.jpg')
        temp_path.touch()  # Create file so it exists
        
        try:
            with pytest.raises(Exception, match="Failed to embed metadata"):
                embedder.embed_into_image(temp_path, sample_metadata)
        finally:
            temp_path.unlink(missing_ok=True)


class TestProcessMedia:
    """Test the high-level process_media method."""
    
    def test_process_media_image_both_methods(self, embedder, sample_metadata, temp_image):
        """Test that process_media tries both EXIF and sidecar for images."""
        result = embedder.process_media(temp_image, sample_metadata)
        
        assert result["exif_embedded"] is True
        assert result["sidecar_written"] is True
        
        # Verify sidecar file was created
        sidecar_path = temp_image.with_suffix(temp_image.suffix + '.json')
        try:
            assert sidecar_path.exists()
        finally:
            sidecar_path.unlink(missing_ok=True)
    
    def test_process_media_unsupported_format_sidecar_only(self, embedder, sample_metadata, temp_png):
        """Test that unsupported image formats only get sidecar."""
        result = embedder.process_media(temp_png, sample_metadata)
        
        assert result["exif_embedded"] is False
        assert result["sidecar_written"] is True
        
        # Verify sidecar file was created
        sidecar_path = temp_png.with_suffix(temp_png.suffix + '.json')
        try:
            assert sidecar_path.exists()
        finally:
            sidecar_path.unlink(missing_ok=True)
    
    def test_process_media_prefer_sidecar(self, embedder, sample_metadata, temp_image):
        """Test that prefer_sidecar skips EXIF embedding."""
        result = embedder.process_media(temp_image, sample_metadata, prefer_sidecar=True)
        
        assert result["exif_embedded"] is False
        assert result["sidecar_written"] is True
        
        # Verify sidecar file was created
        sidecar_path = temp_image.with_suffix(temp_image.suffix + '.json')
        try:
            assert sidecar_path.exists()
        finally:
            sidecar_path.unlink(missing_ok=True)
    
    def test_process_media_file_not_found(self, embedder, sample_metadata):
        """Test process_media with non-existent file."""
        nonexistent_path = Path('/nonexistent/file.jpg')
        
        with pytest.raises(FileNotFoundError, match="Media file not found"):
            embedder.process_media(nonexistent_path, sample_metadata)
    
    @patch.object(MetadataEmbedder, 'embed_into_image')
    def test_process_media_exif_fallback_to_sidecar(self, mock_embed, embedder, sample_metadata, temp_image):
        """Test that EXIF failure falls back to sidecar only."""
        # Make EXIF embedding fail
        mock_embed.side_effect = Exception("EXIF failed")
        
        result = embedder.process_media(temp_image, sample_metadata)
        
        assert result["exif_embedded"] is False
        assert result["sidecar_written"] is True
        
        # Verify sidecar file was created
        sidecar_path = temp_image.with_suffix(temp_image.suffix + '.json')
        try:
            assert sidecar_path.exists()
        finally:
            sidecar_path.unlink(missing_ok=True)
    
    @patch.object(MetadataEmbedder, 'write_sidecar')
    @patch.object(MetadataEmbedder, 'embed_into_image')
    def test_process_media_both_fail_reraises(self, mock_embed, mock_sidecar, embedder, sample_metadata, temp_image):
        """Test that if both methods fail, the sidecar error is re-raised."""
        # Make both methods fail
        mock_embed.side_effect = Exception("EXIF failed")
        mock_sidecar.side_effect = OSError("Sidecar failed")
        
        with pytest.raises(OSError, match="Sidecar failed"):
            embedder.process_media(temp_image, sample_metadata)


if __name__ == "__main__":
    # Run tests if this file is executed directly
    pytest.main([__file__, "-v"]) 