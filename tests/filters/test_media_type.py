"""
Tests for Media Type Filter

Tests the media type filter functionality including content type
detection, file extension filtering, and media format handling.
"""

import pytest
from unittest.mock import Mock

from redditdl.filters.media_type import MediaTypeFilter
from redditdl.scrapers import PostMetadata


class TestMediaTypeFilter:
    """Test suite for MediaTypeFilter."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.filter = MediaTypeFilter()
    
    def test_filter_initialization_default(self):
        """Test filter initialization with default settings."""
        assert isinstance(self.filter, MediaTypeFilter)
        assert self.filter.allowed_types == []
        assert self.filter.blocked_types == []
        assert self.filter.allowed_extensions == []
        assert self.filter.blocked_extensions == []
    
    def test_filter_initialization_with_allowed_types(self):
        """Test filter initialization with allowed media types."""
        allowed_types = ['image', 'video']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        assert filter_instance.allowed_types == allowed_types
        assert filter_instance.blocked_types == []
    
    def test_filter_initialization_with_blocked_types(self):
        """Test filter initialization with blocked media types."""
        blocked_types = ['audio', 'document']
        filter_instance = MediaTypeFilter(blocked_types=blocked_types)
        
        assert filter_instance.allowed_types == []
        assert filter_instance.blocked_types == blocked_types
    
    def test_filter_initialization_with_extensions(self):
        """Test filter initialization with file extensions."""
        allowed_extensions = ['.jpg', '.png', '.gif']
        blocked_extensions = ['.exe', '.zip']
        filter_instance = MediaTypeFilter(
            allowed_extensions=allowed_extensions,
            blocked_extensions=blocked_extensions
        )
        
        assert filter_instance.allowed_extensions == allowed_extensions
        assert filter_instance.blocked_extensions == blocked_extensions
    
    def test_filter_allowed_types_images(self):
        """Test filter allows specified image types."""
        allowed_types = ['image']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # JPEG image
        jpeg_post = PostMetadata(
            id='abc123',
            title='JPEG Image',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        # PNG image
        png_post = PostMetadata(
            id='def456',
            title='PNG Image',
            url='https://example.com/image.png',
            domain='example.com'
        )
        
        # GIF image
        gif_post = PostMetadata(
            id='ghi789',
            title='GIF Image',
            url='https://example.com/animation.gif',
            domain='example.com'
        )
        
        assert filter_instance.apply(jpeg_post) is True
        assert filter_instance.apply(png_post) is True
        assert filter_instance.apply(gif_post) is True
    
    def test_filter_allowed_types_videos(self):
        """Test filter allows specified video types."""
        allowed_types = ['video']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # MP4 video
        mp4_post = PostMetadata(
            id='abc123',
            title='MP4 Video',
            url='https://example.com/video.mp4',
            domain='example.com',
            is_video=True
        )
        
        # Reddit video
        reddit_video_post = PostMetadata(
            id='def456',
            title='Reddit Video',
            url='https://v.redd.it/abcd1234',
            domain='v.redd.it',
            is_video=True
        )
        
        assert filter_instance.apply(mp4_post) is True
        assert filter_instance.apply(reddit_video_post) is True
    
    def test_filter_blocks_non_allowed_types(self):
        """Test filter blocks types not in allowed list."""
        allowed_types = ['image']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # Video (not allowed)
        video_post = PostMetadata(
            id='abc123',
            title='Video Post',
            url='https://example.com/video.mp4',
            domain='example.com',
            is_video=True
        )
        
        # Text post (not allowed)
        text_post = PostMetadata(
            id='def456',
            title='Text Post',
            url='https://reddit.com/r/test/comments/def456/',
            domain='self.test',
            is_self=True
        )
        
        assert filter_instance.apply(video_post) is False
        assert filter_instance.apply(text_post) is False
    
    def test_filter_blocked_types(self):
        """Test filter blocks specified media types."""
        blocked_types = ['video', 'audio']
        filter_instance = MediaTypeFilter(blocked_types=blocked_types)
        
        # Video (blocked)
        video_post = PostMetadata(
            id='abc123',
            title='Video Post',
            url='https://example.com/video.mp4',
            domain='example.com',
            is_video=True
        )
        
        # Audio (blocked)
        audio_post = PostMetadata(
            id='def456',
            title='Audio Post',
            url='https://example.com/audio.mp3',
            domain='example.com'
        )
        
        # Image (not blocked)
        image_post = PostMetadata(
            id='ghi789',
            title='Image Post',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        assert filter_instance.apply(video_post) is False
        assert filter_instance.apply(audio_post) is False
        assert filter_instance.apply(image_post) is True
    
    def test_filter_allowed_extensions(self):
        """Test filter allows specified file extensions."""
        allowed_extensions = ['.jpg', '.png', '.mp4']
        filter_instance = MediaTypeFilter(allowed_extensions=allowed_extensions)
        
        # Allowed extensions
        jpg_post = PostMetadata(
            id='abc123',
            title='JPEG Post',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        png_post = PostMetadata(
            id='def456',
            title='PNG Post',
            url='https://example.com/image.png',
            domain='example.com'
        )
        
        mp4_post = PostMetadata(
            id='ghi789',
            title='MP4 Post',
            url='https://example.com/video.mp4',
            domain='example.com',
            is_video=True
        )
        
        # Not allowed extension
        gif_post = PostMetadata(
            id='jkl012',
            title='GIF Post',
            url='https://example.com/animation.gif',
            domain='example.com'
        )
        
        assert filter_instance.apply(jpg_post) is True
        assert filter_instance.apply(png_post) is True
        assert filter_instance.apply(mp4_post) is True
        assert filter_instance.apply(gif_post) is False
    
    def test_filter_blocked_extensions(self):
        """Test filter blocks specified file extensions."""
        blocked_extensions = ['.exe', '.zip', '.rar']
        filter_instance = MediaTypeFilter(blocked_extensions=blocked_extensions)
        
        # Blocked extensions
        exe_post = PostMetadata(
            id='abc123',
            title='Executable',
            url='https://example.com/file.exe',
            domain='example.com'
        )
        
        zip_post = PostMetadata(
            id='def456',
            title='Archive',
            url='https://example.com/archive.zip',
            domain='example.com'
        )
        
        # Allowed extension
        jpg_post = PostMetadata(
            id='ghi789',
            title='Image',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        assert filter_instance.apply(exe_post) is False
        assert filter_instance.apply(zip_post) is False
        assert filter_instance.apply(jpg_post) is True
    
    def test_filter_case_insensitive_extensions(self):
        """Test filter handles case-insensitive file extensions."""
        allowed_extensions = ['.jpg', '.png']
        filter_instance = MediaTypeFilter(allowed_extensions=allowed_extensions)
        
        # Uppercase extension
        upper_post = PostMetadata(
            id='abc123',
            title='Upper Case',
            url='https://example.com/image.JPG',
            domain='example.com'
        )
        
        # Mixed case extension
        mixed_post = PostMetadata(
            id='def456',
            title='Mixed Case',
            url='https://example.com/image.Png',
            domain='example.com'
        )
        
        assert filter_instance.apply(upper_post) is True
        assert filter_instance.apply(mixed_post) is True
    
    def test_filter_extension_detection_with_parameters(self):
        """Test extension detection with URL parameters."""
        allowed_extensions = ['.jpg']
        filter_instance = MediaTypeFilter(allowed_extensions=allowed_extensions)
        
        # URL with parameters
        param_post = PostMetadata(
            id='abc123',
            title='Parameterized URL',
            url='https://example.com/image.jpg?size=large&format=original',
            domain='example.com'
        )
        
        # URL with fragment
        fragment_post = PostMetadata(
            id='def456',
            title='Fragment URL',
            url='https://example.com/image.jpg#section1',
            domain='example.com'
        )
        
        assert filter_instance.apply(param_post) is True
        assert filter_instance.apply(fragment_post) is True
    
    def test_filter_media_type_detection_from_metadata(self):
        """Test media type detection from post metadata."""
        allowed_types = ['video']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # Video with metadata
        video_post = PostMetadata(
            id='abc123',
            title='Video with Metadata',
            url='https://v.redd.it/abcd1234',
            domain='v.redd.it',
            is_video=True,
            media={
                'type': 'video',
                'reddit_video': {
                    'fallback_url': 'https://v.redd.it/abcd1234/DASH_720.mp4'
                }
            }
        )
        
        assert filter_instance.apply(video_post) is True
    
    def test_filter_gallery_type_detection(self):
        """Test detection of gallery media types."""
        allowed_types = ['gallery']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # Gallery post
        gallery_post = PostMetadata(
            id='abc123',
            title='Gallery Post',
            url='https://www.reddit.com/gallery/abc123',
            domain='reddit.com',
            post_type='gallery',
            gallery_image_urls=[
                'https://i.redd.it/image1.jpg',
                'https://i.redd.it/image2.png'
            ]
        )
        
        assert filter_instance.apply(gallery_post) is True
    
    def test_filter_text_post_detection(self):
        """Test detection of text posts."""
        blocked_types = ['text']
        filter_instance = MediaTypeFilter(blocked_types=blocked_types)
        
        # Self text post
        text_post = PostMetadata(
            id='abc123',
            title='Text Post',
            url='https://reddit.com/r/test/comments/abc123/',
            domain='self.test',
            is_self=True,
            selftext='This is a text post content.'
        )
        
        assert filter_instance.apply(text_post) is False
    
    def test_filter_unknown_type_handling(self):
        """Test handling of unknown media types."""
        allowed_types = ['image', 'video']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # Unknown file type
        unknown_post = PostMetadata(
            id='abc123',
            title='Unknown Type',
            url='https://example.com/file.xyz',
            domain='example.com'
        )
        
        # Should not pass (unknown type not in allowed list)
        assert filter_instance.apply(unknown_post) is False
    
    def test_filter_multiple_criteria_combination(self):
        """Test filter with multiple criteria combined."""
        # Allow images and videos, but block .gif and .exe
        filter_instance = MediaTypeFilter(
            allowed_types=['image', 'video'],
            blocked_extensions=['.gif', '.exe']
        )
        
        # Image (allowed type) with .jpg (not blocked)
        good_image = PostMetadata(
            id='abc123',
            title='Good Image',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        # Image (allowed type) with .gif (blocked extension)
        blocked_gif = PostMetadata(
            id='def456',
            title='Blocked GIF',
            url='https://example.com/animation.gif',
            domain='example.com'
        )
        
        # Video (allowed type) with .mp4 (not blocked)
        good_video = PostMetadata(
            id='ghi789',
            title='Good Video',
            url='https://example.com/video.mp4',
            domain='example.com',
            is_video=True
        )
        
        # Audio (not allowed type) with .mp3 (not blocked)
        audio_file = PostMetadata(
            id='jkl012',
            title='Audio File',
            url='https://example.com/audio.mp3',
            domain='example.com'
        )
        
        assert filter_instance.apply(good_image) is True
        assert filter_instance.apply(blocked_gif) is False
        assert filter_instance.apply(good_video) is True
        assert filter_instance.apply(audio_file) is False
    
    def test_filter_priority_blocked_over_allowed(self):
        """Test that blocked criteria take priority over allowed criteria."""
        # Allow images but block .jpg
        filter_instance = MediaTypeFilter(
            allowed_types=['image'],
            blocked_extensions=['.jpg']
        )
        
        # Image (allowed) with .jpg extension (blocked)
        conflicted_post = PostMetadata(
            id='abc123',
            title='Conflicted Image',
            url='https://example.com/image.jpg',
            domain='example.com'
        )
        
        # Blocked should take priority
        assert filter_instance.apply(conflicted_post) is False
    
    def test_filter_reddit_specific_media(self):
        """Test filter handling of Reddit-specific media formats."""
        allowed_types = ['image', 'video']
        filter_instance = MediaTypeFilter(allowed_types=allowed_types)
        
        # Reddit hosted image
        reddit_image = PostMetadata(
            id='abc123',
            title='Reddit Image',
            url='https://i.redd.it/abcd1234.jpg',
            domain='i.redd.it'
        )
        
        # Reddit hosted video
        reddit_video = PostMetadata(
            id='def456',
            title='Reddit Video',
            url='https://v.redd.it/efgh5678',
            domain='v.redd.it',
            is_video=True
        )
        
        assert filter_instance.apply(reddit_image) is True
        assert filter_instance.apply(reddit_video) is True
    
    def test_filter_configuration_from_dict(self):
        """Test filter configuration from dictionary."""
        config = {
            'allowed_types': ['image', 'video'],
            'blocked_types': ['audio'],
            'allowed_extensions': ['.jpg', '.png', '.mp4'],
            'blocked_extensions': ['.exe', '.zip']
        }
        
        filter_instance = MediaTypeFilter.from_config(config)
        
        assert filter_instance.allowed_types == config['allowed_types']
        assert filter_instance.blocked_types == config['blocked_types']
        assert filter_instance.allowed_extensions == config['allowed_extensions']
        assert filter_instance.blocked_extensions == config['blocked_extensions']
    
    def test_filter_get_description(self):
        """Test filter description generation."""
        # Filter with allowed types
        allowed_filter = MediaTypeFilter(allowed_types=['image', 'video'])
        description = allowed_filter.get_description()
        assert 'allow' in description.lower()
        assert 'image' in description
        assert 'video' in description
        
        # Filter with blocked extensions
        blocked_filter = MediaTypeFilter(blocked_extensions=['.exe', '.zip'])
        description = blocked_filter.get_description()
        assert 'block' in description.lower()
        assert '.exe' in description
    
    def test_filter_no_configuration(self):
        """Test filter with no media type configuration (should pass all)."""
        filter_instance = MediaTypeFilter()  # No types specified
        
        post = PostMetadata(
            id='abc123',
            title='Any Post',
            url='https://example.com/file.unknown',
            domain='example.com'
        )
        
        # Should pass when no filtering rules are defined
        assert filter_instance.apply(post) is True
    
    def test_media_type_detection_methods(self):
        """Test internal media type detection methods."""
        filter_instance = MediaTypeFilter()
        
        # Test extension-based detection
        assert filter_instance._detect_type_from_extension('.jpg') == 'image'
        assert filter_instance._detect_type_from_extension('.mp4') == 'video'
        assert filter_instance._detect_type_from_extension('.mp3') == 'audio'
        assert filter_instance._detect_type_from_extension('.unknown') == 'unknown'
        
        # Test URL-based detection
        assert filter_instance._detect_type_from_url('https://i.redd.it/image.jpg') == 'image'
        assert filter_instance._detect_type_from_url('https://v.redd.it/video') == 'video'
    
    def test_extension_extraction(self):
        """Test file extension extraction from URLs."""
        filter_instance = MediaTypeFilter()
        
        # Standard URLs
        assert filter_instance._extract_extension('https://example.com/file.jpg') == '.jpg'
        assert filter_instance._extract_extension('https://example.com/video.MP4') == '.mp4'
        
        # URLs with parameters
        assert filter_instance._extract_extension('https://example.com/file.png?size=large') == '.png'
        
        # URLs without extensions
        assert filter_instance._extract_extension('https://example.com/file') == ''
        assert filter_instance._extract_extension('https://v.redd.it/abcd1234') == ''