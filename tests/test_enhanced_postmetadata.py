#!/usr/bin/env python3
"""
Comprehensive tests for enhanced PostMetadata with PRD v2.2.1 fields.

This module tests the new dataclass-based PostMetadata implementation
with all the enhanced fields required by PRD v2.2.1.
"""
import sys
from pathlib import Path

# Add project root to path
# sys.path.insert(0, str(Path(__file__).resolve().parents[1])) # This line is no longer needed

import pytest
from redditdl.scrapers import PostMetadata


class TestEnhancedPostMetadata:
    """Test cases for the enhanced PostMetadata class with PRD v2.2.1 fields."""
    
    def test_enhanced_fields_default_values(self):
        """Test that all enhanced fields have proper default values."""
        post = PostMetadata.from_raw({'id': 'test123'})
        
        # Check enhanced field defaults
        assert post.score == 0
        assert post.num_comments == 0
        assert post.is_nsfw is False
        assert post.is_self is False
        assert post.domain == ""
        assert post.awards == []
        assert post.media is None
        assert post.post_type == "link"
        assert post.crosspost_parent_id is None
        assert post.gallery_image_urls == []
        assert post.poll_data is None
        assert post.created_utc == 0.0
        assert post.edited is False
        assert post.locked is False
        assert post.archived is False
        assert post.spoiler is False
        assert post.stickied is False
    
    def test_enhanced_fields_from_praw_data(self):
        """Test enhanced fields extraction from PRAW-style data."""
        praw_data = {
            'id': 'praw123',
            'title': 'PRAW Post',
            'subreddit': 'test',
            'author': 'testuser',
            'created_utc': 1640995200,
            'score': 150,
            'num_comments': 25,
            'over_18': True,  # NSFW flag in PRAW
            'is_self': False,
            'domain': 'self.test',
            'all_awardings': [
                {'name': 'gold', 'count': 1},
                {'name': 'silver', 'count': 2}
            ],
            'media': {'type': 'video', 'url': 'https://example.com/video.mp4'},
            'is_gallery': True,
            'gallery_data': {
                'items': [
                    {'media_id': 'img1'},
                    {'media_id': 'img2'}
                ]
            },
            'poll_data': {
                'options': [
                    {'text': 'Option 1', 'vote_count': 10},
                    {'text': 'Option 2', 'vote_count': 15}
                ]
            },
            'edited': True,
            'locked': True,
            'archived': False,
            'spoiler': True,
            'stickied': False,
            'crosspost_parent_list': [{'id': 'parent123'}]
        }
        
        post = PostMetadata.from_raw(praw_data)
        
        # Check enhanced fields populated correctly
        assert post.score == 150
        assert post.num_comments == 25
        assert post.is_nsfw is True
        assert post.is_self is False
        assert post.domain == 'self.test'
        assert len(post.awards) == 2
        assert post.awards[0]['name'] == 'gold'
        assert post.media['type'] == 'video'
        assert post.post_type == 'gallery'  # Should detect gallery type
        assert len(post.gallery_image_urls) == 2
        assert 'img1' in post.gallery_image_urls[0]
        assert post.poll_data['options'][1]['vote_count'] == 15
        assert post.edited is True
        assert post.locked is True
        assert post.archived is False
        assert post.spoiler is True
        assert post.stickied is False
        assert post.crosspost_parent_id == 'parent123'
    
    def test_post_type_detection(self):
        """Test post type detection logic."""
        test_cases = [
            ({'id': 'text1', 'is_self': True}, 'text'),
            ({'id': 'video1', 'is_video': True}, 'video'),
            ({'id': 'gallery1', 'is_gallery': True}, 'gallery'),
            ({'id': 'poll1', 'poll_data': {'options': []}}, 'poll'),
            ({'id': 'cross1', 'crosspost_parent_list': [{'id': 'parent'}]}, 'crosspost'),
            ({'id': 'img1', 'url': 'https://example.com/image.jpg'}, 'image'),
            ({'id': 'link1', 'url': 'https://example.com/page'}, 'link'),
        ]
        
        for raw_data, expected_type in test_cases:
            post = PostMetadata.from_raw(raw_data)
            assert post.post_type == expected_type, f"Expected {expected_type} for {raw_data}"
    
    def test_gallery_url_extraction(self):
        """Test gallery URL extraction from various formats."""
        # Test with gallery_data format
        gallery_data = {
            'id': 'gallery1',
            'gallery_data': {
                'items': [
                    {'media_id': 'abc123'},
                    {'media_id': 'def456'}
                ]
            }
        }
        
        post = PostMetadata.from_raw(gallery_data)
        assert len(post.gallery_image_urls) == 2
        assert 'abc123' in post.gallery_image_urls[0]
        assert 'def456' in post.gallery_image_urls[1]
        
        # Test with media_metadata format
        media_metadata = {
            'id': 'gallery2',
            'media_metadata': {
                'img1': {'s': {'u': 'https://preview.redd.it/img1.jpg?width=640&amp;crop=smart'}},
                'img2': {'s': {'u': 'https://preview.redd.it/img2.jpg?width=640&amp;crop=smart'}}
            }
        }
        
        post = PostMetadata.from_raw(media_metadata)
        assert len(post.gallery_image_urls) == 2
        assert 'https://preview.redd.it/img1.jpg?width=640&crop=smart' in post.gallery_image_urls
        assert 'https://preview.redd.it/img2.jpg?width=640&crop=smart' in post.gallery_image_urls
    
    def test_nsfw_detection(self):
        """Test NSFW flag detection from various sources."""
        test_cases = [
            ({'id': 'nsfw1', 'over_18': True}, True),
            ({'id': 'nsfw2', 'is_nsfw': True}, True),
            ({'id': 'nsfw3', 'over_18': True, 'is_nsfw': False}, True),  # over_18 takes precedence
            ({'id': 'sfw1', 'over_18': False, 'is_nsfw': False}, False),
            ({'id': 'sfw2'}, False),  # Default
        ]
        
        for raw_data, expected_nsfw in test_cases:
            post = PostMetadata.from_raw(raw_data)
            assert post.is_nsfw == expected_nsfw, f"NSFW detection failed for {raw_data}"
    
    def test_crosspost_parent_extraction(self):
        """Test crosspost parent ID extraction."""
        # Test with crosspost_parent_list
        crosspost_data = {
            'id': 'cross1',
            'crosspost_parent_list': [{'id': 'original123', 'title': 'Original Post'}]
        }
        
        post = PostMetadata.from_raw(crosspost_data)
        assert post.crosspost_parent_id == 'original123'
        
        # Test with direct crosspost_parent_id
        direct_crosspost = {
            'id': 'cross2',
            'crosspost_parent_id': 'direct456'
        }
        
        post = PostMetadata.from_raw(direct_crosspost)
        assert post.crosspost_parent_id == 'direct456'
        
        # Test with empty crosspost_parent_list
        empty_crosspost = {
            'id': 'cross3',
            'crosspost_parent_list': []
        }
        
        post = PostMetadata.from_raw(empty_crosspost)
        assert post.crosspost_parent_id is None
    
    def test_to_dict_enhanced_fields(self):
        """Test that to_dict includes all enhanced fields."""
        test_data = {
            'id': 'enhanced123',
            'title': 'Enhanced Post',
            'score': 100,
            'num_comments': 50,
            'over_18': True,
            'domain': 'example.com',
            'all_awardings': [{'name': 'award', 'count': 1}],
            'created_utc': 1640995200,
        }
        
        post = PostMetadata.from_raw(test_data)
        result_dict = post.to_dict()
        
        # Check all enhanced fields are present
        enhanced_fields = [
            'score', 'num_comments', 'is_nsfw', 'is_self', 'domain',
            'awards', 'media', 'post_type', 'crosspost_parent_id',
            'gallery_image_urls', 'poll_data', 'created_utc',
            'edited', 'locked', 'archived', 'spoiler', 'stickied'
        ]
        
        for field in enhanced_fields:
            assert field in result_dict, f"Enhanced field '{field}' missing from to_dict output"
        
        # Check values are correct
        assert result_dict['score'] == 100
        assert result_dict['num_comments'] == 50
        assert result_dict['is_nsfw'] is True
        assert result_dict['domain'] == 'example.com'
        assert result_dict['awards'][0]['name'] == 'award'
    
    def test_from_dict_deserialization(self):
        """Test from_dict class method for deserialization."""
        original_dict = {
            'id': 'deser123',
            'title': 'Deserialization Test',
            'score': 200,
            'num_comments': 75,
            'is_nsfw': False,
            'domain': 'reddit.com',
            'awards': [{'name': 'platinum', 'count': 1}],
            'post_type': 'image',
            'created_utc': 1640995200.0,
            'locked': True,
        }
        
        post = PostMetadata.from_dict(original_dict)
        
        # Check fields are correctly deserialized
        assert post.id == 'deser123'
        assert post.title == 'Deserialization Test'
        assert post.score == 200
        assert post.num_comments == 75
        assert post.is_nsfw is False
        assert post.domain == 'reddit.com'
        assert post.awards[0]['name'] == 'platinum'
        assert post.post_type == 'image'
        assert post.created_utc == 1640995200.0
        assert post.locked is True
        
        # Check round-trip serialization
        serialized = post.to_dict()
        deserialized = PostMetadata.from_dict(serialized)
        
        assert post.id == deserialized.id
        assert post.score == deserialized.score
        assert post.awards == deserialized.awards
    
    def test_error_handling_with_invalid_values(self):
        """Test error handling with invalid values for enhanced fields."""
        # Test with invalid score (non-numeric)
        invalid_data = {
            'id': 'invalid123',
            'score': 'not-a-number',
            'num_comments': None,
            'created_utc': 'invalid-timestamp',
            'all_awardings': 'not-a-list',
            'media': 'not-a-dict'
        }
        
        # Should not raise exception, should use defaults
        post = PostMetadata.from_raw(invalid_data)
        
        assert post.score == 0  # Default due to conversion error
        assert post.num_comments == 0  # Default due to None
        assert post.created_utc == 0.0  # Default due to conversion error
        assert post.awards == []  # Default due to invalid type
        assert post.media is None  # Default due to invalid type
    
    def test_dataclass_field_access(self):
        """Test that PostMetadata works as a proper dataclass."""
        post = PostMetadata.from_raw({
            'id': 'dataclass123',
            'title': 'Dataclass Test',
            'score': 42
        })
        
        # Test field access
        assert post.id == 'dataclass123'
        assert post.score == 42
        
        # Test field modification
        post.score = 100
        assert post.score == 100
        
        # Test that the instance has dataclass fields
        assert hasattr(post, '__dataclass_fields__')
        assert 'score' in post.__dataclass_fields__
        assert 'num_comments' in post.__dataclass_fields__
    
    def test_backward_compatibility(self):
        """Test that existing code patterns still work."""
        # Test that basic usage still works
        post = PostMetadata.from_raw({
            'id': 'compat123',
            'title': 'Compatibility Test',
            'subreddit': 'test',
            'author': 'testuser',
            'url': 'https://example.com',
            'created_utc': 1640995200
        })
        
        # Test that original fields work the same way
        assert post.id == 'compat123'
        assert post.title == 'Compatibility Test'
        assert post.subreddit == 'test'
        assert post.author == 'testuser'
        assert post.url == 'https://example.com'
        assert post.date_iso == '2022-01-01T00:00:00Z'
        
        # Test string representations
        assert 'compat123' in str(post)
        assert 'Compatibility Test' in str(post)
        assert 'r/test' in str(post)