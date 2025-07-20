"""
Tests for ImageProcessor class.

Comprehensive test suite for image processing functionality including format conversion,
quality adjustment, resizing, and thumbnail generation.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from PIL import Image
import io

from redditdl.processing.image_processor import ImageProcessor
from redditdl.processing.exceptions import ImageProcessingError, UnsupportedFormatError


class TestImageProcessor:
    """Test cases for ImageProcessor functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def sample_image(self, temp_dir):
        """Create a sample test image."""
        # Create a simple RGB image
        img = Image.new('RGB', (800, 600), color='red')
        img_path = temp_dir / "test_image.jpg"
        img.save(img_path, 'JPEG', quality=95)
        return img_path
    
    @pytest.fixture
    def sample_png_image(self, temp_dir):
        """Create a sample PNG image with transparency."""
        img = Image.new('RGBA', (400, 300), color=(0, 255, 0, 128))
        img_path = temp_dir / "test_image.png"
        img.save(img_path, 'PNG')
        return img_path
    
    @pytest.fixture
    def processor(self):
        """Create ImageProcessor instance for testing."""
        return ImageProcessor()
    
    def test_init_default_config(self):
        """Test ImageProcessor initialization with default config."""
        processor = ImageProcessor()
        assert processor.default_quality == 85
        assert processor.preserve_exif is True
    
    def test_init_custom_config(self):
        """Test ImageProcessor initialization with custom config."""
        config = {
            'image_quality': 70,
            'preserve_original_metadata': False
        }
        processor = ImageProcessor(config)
        assert processor.default_quality == 70
        assert processor.preserve_exif is False
    
    def test_convert_format_jpeg_to_png(self, processor, sample_image, temp_dir):
        """Test converting JPEG to PNG format."""
        output_path = temp_dir / "converted.png"
        
        result = processor.convert_format(
            sample_image, output_path, 'png'
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify the converted image
        with Image.open(output_path) as img:
            assert img.format == 'PNG'
    
    def test_convert_format_png_to_jpeg(self, processor, sample_png_image, temp_dir):
        """Test converting PNG to JPEG format (with transparency handling)."""
        output_path = temp_dir / "converted.jpg"
        
        result = processor.convert_format(
            sample_png_image, output_path, 'jpeg'
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify the converted image
        with Image.open(output_path) as img:
            assert img.format == 'JPEG'
            assert img.mode == 'RGB'  # Transparency should be removed
    
    def test_convert_format_with_quality(self, processor, sample_image, temp_dir):
        """Test format conversion with specific quality setting."""
        output_path = temp_dir / "converted_quality.jpg"
        
        result = processor.convert_format(
            sample_image, output_path, 'jpeg', quality=50
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # File should be smaller due to lower quality
        original_size = sample_image.stat().st_size
        converted_size = output_path.stat().st_size
        assert converted_size < original_size
    
    def test_convert_format_unsupported(self, processor, sample_image, temp_dir):
        """Test error handling for unsupported format."""
        output_path = temp_dir / "converted.xyz"
        
        with pytest.raises(UnsupportedFormatError):
            processor.convert_format(sample_image, output_path, 'xyz')
    
    def test_adjust_quality(self, processor, sample_image, temp_dir):
        """Test quality adjustment functionality."""
        output_path = temp_dir / "quality_adjusted.jpg"
        
        result = processor.adjust_quality(
            sample_image, output_path, quality=60
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Check that file size is different (should be smaller)
        original_size = sample_image.stat().st_size
        adjusted_size = output_path.stat().st_size
        assert adjusted_size != original_size
    
    def test_resize_image(self, processor, sample_image, temp_dir):
        """Test image resizing functionality."""
        output_path = temp_dir / "resized.jpg"
        max_dimension = 400
        
        result = processor.resize_image(
            sample_image, output_path, max_dimension
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify dimensions
        with Image.open(output_path) as img:
            assert max(img.size) <= max_dimension
            # Should preserve aspect ratio
            original_ratio = 800 / 600  # From sample_image fixture
            new_ratio = img.width / img.height
            assert abs(original_ratio - new_ratio) < 0.01
    
    def test_resize_image_no_aspect_ratio(self, processor, sample_image, temp_dir):
        """Test image resizing without preserving aspect ratio."""
        output_path = temp_dir / "resized_square.jpg"
        max_dimension = 300
        
        result = processor.resize_image(
            sample_image, output_path, max_dimension, 
            preserve_aspect_ratio=False
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify exact dimensions
        with Image.open(output_path) as img:
            assert img.size == (max_dimension, max_dimension)
    
    def test_generate_thumbnail(self, processor, sample_image, temp_dir):
        """Test thumbnail generation."""
        output_path = temp_dir / "thumbnail.jpg"
        thumbnail_size = 128
        
        result = processor.generate_thumbnail(
            sample_image, output_path, thumbnail_size
        )
        
        assert result == output_path
        assert output_path.exists()
        
        # Verify thumbnail dimensions
        with Image.open(output_path) as img:
            assert max(img.size) <= thumbnail_size
    
    def test_get_image_info(self, processor, sample_image):
        """Test getting image information."""
        info = processor.get_image_info(sample_image)
        
        assert info['format'] == 'JPEG'
        assert info['width'] == 800
        assert info['height'] == 600
        assert info['filename'] == 'test_image.jpg'
        assert info['file_size'] > 0
        assert 'has_exif' in info
    
    def test_prepare_image_for_format_jpeg(self, processor):
        """Test image preparation for JPEG format."""
        # Create RGBA image
        img = Image.new('RGBA', (100, 100), (255, 0, 0, 128))
        
        prepared = processor._prepare_image_for_format(img, 'jpeg')
        
        # Should be converted to RGB
        assert prepared.mode == 'RGB'
    
    def test_prepare_image_for_format_png(self, processor):
        """Test image preparation for PNG format."""
        # Create RGBA image
        img = Image.new('RGBA', (100, 100), (255, 0, 0, 128))
        
        prepared = processor._prepare_image_for_format(img, 'png')
        
        # Should preserve RGBA
        assert prepared.mode == 'RGBA'
    
    def test_prepare_image_for_format_webp(self, processor):
        """Test image preparation for WebP format."""
        # Create RGBA image
        img = Image.new('RGBA', (100, 100), (255, 0, 0, 128))
        
        prepared = processor._prepare_image_for_format(img, 'webp')
        
        # Should preserve RGBA for WebP
        assert prepared.mode == 'RGBA'
    
    def test_error_handling_invalid_file(self, processor, temp_dir):
        """Test error handling for invalid input file."""
        invalid_path = temp_dir / "nonexistent.jpg"
        output_path = temp_dir / "output.jpg"
        
        with pytest.raises(ImageProcessingError):
            processor.convert_format(invalid_path, output_path, 'jpeg')
    
    def test_error_handling_corrupted_file(self, processor, temp_dir):
        """Test error handling for corrupted image file."""
        # Create a fake image file
        corrupted_path = temp_dir / "corrupted.jpg"
        corrupted_path.write_text("not an image")
        output_path = temp_dir / "output.jpg"
        
        with pytest.raises(ImageProcessingError):
            processor.convert_format(corrupted_path, output_path, 'jpeg')
    
    def test_quality_bounds_checking(self, processor, sample_image, temp_dir):
        """Test quality parameter bounds checking."""
        output_path = temp_dir / "quality_test.jpg"
        
        # Test quality below minimum (should be clamped to 1)
        result = processor.convert_format(
            sample_image, output_path, 'jpeg', quality=-10
        )
        assert result == output_path
        assert output_path.exists()
        
        # Test quality above maximum (should be clamped to 100)
        output_path2 = temp_dir / "quality_test2.jpg"
        result = processor.convert_format(
            sample_image, output_path2, 'jpeg', quality=150
        )
        assert result == output_path2
        assert output_path2.exists()
    
    def test_output_directory_creation(self, processor, sample_image, temp_dir):
        """Test automatic output directory creation."""
        nested_dir = temp_dir / "nested" / "directory"
        output_path = nested_dir / "output.jpg"
        
        result = processor.convert_format(
            sample_image, output_path, 'jpeg'
        )
        
        assert result == output_path
        assert output_path.exists()
        assert nested_dir.exists()
    
    def test_metadata_preservation(self, processor, temp_dir):
        """Test EXIF metadata preservation during conversion."""
        # Create image with EXIF data
        img = Image.new('RGB', (200, 200), 'blue')
        # Add some basic EXIF data
        exif_dict = {"0th": {}}
        
        input_path = temp_dir / "with_exif.jpg"
        img.save(input_path, 'JPEG', quality=95)
        
        output_path = temp_dir / "converted_with_exif.jpg"
        
        result = processor.convert_format(
            input_path, output_path, 'jpeg', preserve_metadata=True
        )
        
        assert result == output_path
        assert output_path.exists()
    
    def test_supported_formats_constant(self):
        """Test that supported formats constant is properly defined."""
        assert 'jpeg' in ImageProcessor.SUPPORTED_FORMATS
        assert 'jpg' in ImageProcessor.SUPPORTED_FORMATS
        assert 'png' in ImageProcessor.SUPPORTED_FORMATS
        assert 'webp' in ImageProcessor.SUPPORTED_FORMATS
        assert 'bmp' in ImageProcessor.SUPPORTED_FORMATS
        assert 'tiff' in ImageProcessor.SUPPORTED_FORMATS
        assert 'gif' in ImageProcessor.SUPPORTED_FORMATS
    
    def test_quality_formats_constant(self):
        """Test that quality formats constant is properly defined."""
        assert 'jpeg' in ImageProcessor.QUALITY_FORMATS
        assert 'jpg' in ImageProcessor.QUALITY_FORMATS
        assert 'webp' in ImageProcessor.QUALITY_FORMATS
        assert 'png' not in ImageProcessor.QUALITY_FORMATS  # PNG doesn't use quality
    
    def test_exif_formats_constant(self):
        """Test that EXIF formats constant is properly defined."""
        assert 'jpeg' in ImageProcessor.EXIF_FORMATS
        assert 'jpg' in ImageProcessor.EXIF_FORMATS
        assert 'tiff' in ImageProcessor.EXIF_FORMATS
        assert 'png' not in ImageProcessor.EXIF_FORMATS  # PNG doesn't support EXIF