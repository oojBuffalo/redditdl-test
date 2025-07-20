#!/usr/bin/env python3
"""
Unit tests for RedditDL utility functions.

This module contains comprehensive tests for the utility functions defined in utils.py,
including edge cases and cross-platform compatibility tests.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
import re
from datetime import datetime, timezone
from redditdl.utils import sanitize_filename, get_current_timestamp, merge_metadata


class TestSanitizeFilename:
    """Test cases for the sanitize_filename function."""
    
    def test_sanitize_basic_invalid_characters(self):
        """Test replacement of basic invalid characters."""
        test_cases = [
            ("my/file.txt", "my_file.txt"),
            ("file\\name.txt", "file_name.txt"),
            ("file?name.txt", "file_name.txt"),
            ("file*name.txt", "file_name.txt"),
            ("file:name.txt", "file_name.txt"),
            ("file|name.txt", "file_name.txt"),
            ('file"name.txt', "file_name.txt"),
            ("file<name.txt", "file_name.txt"),
            ("file>name.txt", "file_name.txt"),
        ]
        
        for input_filename, expected in test_cases:
            result = sanitize_filename(input_filename)
            assert result == expected, f"Failed for input: {input_filename}"
    
    def test_sanitize_multiple_invalid_characters(self):
        """Test replacement of multiple invalid characters in one filename."""
        filename = "my/file\\name?with*many:invalid|chars\"<>.txt"
        expected = "my_file_name_with_many_invalid_chars___.txt"
        result = sanitize_filename(filename)
        assert result == expected
    
    def test_sanitize_empty_and_whitespace(self):
        """Test handling of empty strings and whitespace-only strings."""
        test_cases = [
            ("", "unnamed_file"),
            ("   ", "unnamed_file"),
            ("\t\n", "unnamed_file"),
        ]
        
        for input_filename, expected in test_cases:
            result = sanitize_filename(input_filename)
            assert result == expected, f"Failed for input: '{input_filename}'"
    
    def test_sanitize_all_invalid_characters(self):
        """Test filename consisting entirely of invalid characters."""
        filename = "/\\?*:|\"<>"
        result = sanitize_filename(filename)
        assert result == "unnamed_file"
    
    def test_sanitize_long_filename(self):
        """Test truncation of long filenames to 128 characters."""
        # Test without extension
        long_name = "a" * 200
        result = sanitize_filename(long_name)
        assert len(result) == 128
        assert result == "a" * 128
        
        # Test with extension
        long_name_with_ext = "a" * 200 + ".txt"
        result = sanitize_filename(long_name_with_ext)
        assert len(result) == 128
        assert result.endswith(".txt")
        assert result == "a" * 124 + ".txt"  # 124 + 4 (.txt) = 128
    
    def test_sanitize_long_extension(self):
        """Test handling of very long file extensions."""
        # Extension longer than filename space available
        filename = "short" + "." + "x" * 130
        result = sanitize_filename(filename)
        assert len(result) == 128
    
    def test_sanitize_preserve_valid_characters(self):
        """Test that valid characters are preserved."""
        filename = "valid_filename-123.txt"
        result = sanitize_filename(filename)
        assert result == filename
    
    def test_sanitize_unicode_characters(self):
        """Test handling of Unicode characters (should be preserved)."""
        filename = "файл_測試_🎬.txt"
        result = sanitize_filename(filename)
        assert result == filename
    
    def test_sanitize_multiple_dots(self):
        """Test handling of filenames with multiple dots."""
        filename = "my.file.name.txt"
        result = sanitize_filename(filename)
        assert result == filename
        
        # With invalid characters
        filename = "my/file.name.txt"
        result = sanitize_filename(filename)
        assert result == "my_file.name.txt"


class TestGetCurrentTimestamp:
    """Test cases for the get_current_timestamp function."""
    
    def test_timestamp_format(self):
        """Test that timestamp follows ISO 8601 format with Z suffix."""
        timestamp = get_current_timestamp()
        
        # Check basic format: YYYY-MM-DDTHH:MM:SSZ
        pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
        assert re.match(pattern, timestamp), f"Timestamp format invalid: {timestamp}"
    
    def test_timestamp_length(self):
        """Test that timestamp has correct length (20 characters)."""
        timestamp = get_current_timestamp()
        assert len(timestamp) == 20, f"Expected length 20, got {len(timestamp)}"
    
    def test_timestamp_components(self):
        """Test individual components of the timestamp."""
        timestamp = get_current_timestamp()
        
        # Should contain T separator
        assert 'T' in timestamp
        
        # Should end with Z
        assert timestamp.endswith('Z')
        
        # Should not contain microseconds
        assert '.' not in timestamp
    
    def test_timestamp_parseable(self):
        """Test that generated timestamp can be parsed back to datetime."""
        timestamp = get_current_timestamp()
        
        # Remove Z and parse
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Should be a valid datetime
        assert isinstance(dt, datetime)
        
        # Should be in UTC (timezone aware)
        assert dt.tzinfo is not None
    
    def test_timestamp_current_time(self):
        """Test that timestamp represents approximately current time."""
        before = datetime.now(timezone.utc).replace(microsecond=0)
        timestamp = get_current_timestamp()
        after = datetime.now(timezone.utc).replace(microsecond=0)
        
        # Parse the timestamp
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Should be between before and after (allowing for execution time)
        assert before <= dt <= after or (dt - before).total_seconds() <= 1
    
    def test_timestamp_consistency(self):
        """Test that consecutive calls produce different but similar timestamps."""
        timestamp1 = get_current_timestamp()
        timestamp2 = get_current_timestamp()
        
        # Should be different (unless called in same second)
        # Both should be valid format
        pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
        assert re.match(pattern, timestamp1)
        assert re.match(pattern, timestamp2)


class TestMergeMetadata:
    """Test cases for the merge_metadata function."""
    
    def test_merge_empty_dictionaries(self):
        """Test merging empty dictionaries."""
        result = merge_metadata({}, {})
        assert result == {}
    
    def test_merge_one_empty_dictionary(self):
        """Test merging when one dictionary is empty."""
        dict1 = {"key1": "value1", "key2": "value2"}
        dict2 = {}
        
        result = merge_metadata(dict1, dict2)
        assert result == dict1
        
        result = merge_metadata(dict2, dict1)
        assert result == dict1
    
    def test_merge_non_overlapping_keys(self):
        """Test merging dictionaries with non-overlapping keys."""
        dict1 = {"key1": "value1", "key2": "value2"}
        dict2 = {"key3": "value3", "key4": "value4"}
        
        expected = {"key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"}
        result = merge_metadata(dict1, dict2)
        assert result == expected
    
    def test_merge_overlapping_keys(self):
        """Test that dict2 values take precedence for overlapping keys."""
        dict1 = {"key1": "value1", "key2": "value2"}
        dict2 = {"key2": "new_value2", "key3": "value3"}
        
        expected = {"key1": "value1", "key2": "new_value2", "key3": "value3"}
        result = merge_metadata(dict1, dict2)
        assert result == expected
    
    def test_merge_different_value_types(self):
        """Test merging dictionaries with different value types."""
        dict1 = {"string": "text", "number": 42, "list": [1, 2, 3]}
        dict2 = {"boolean": True, "dict": {"nested": "value"}, "number": 100}
        
        expected = {
            "string": "text",
            "number": 100,  # Overridden by dict2
            "list": [1, 2, 3],
            "boolean": True,
            "dict": {"nested": "value"}
        }
        result = merge_metadata(dict1, dict2)
        assert result == expected
    
    def test_merge_does_not_modify_originals(self):
        """Test that merge operation doesn't modify original dictionaries."""
        dict1 = {"key1": "value1"}
        dict2 = {"key2": "value2"}
        
        dict1_copy = dict1.copy()
        dict2_copy = dict2.copy()
        
        merge_metadata(dict1, dict2)
        
        # Original dictionaries should be unchanged
        assert dict1 == dict1_copy
        assert dict2 == dict2_copy
    
    def test_merge_complex_metadata(self):
        """Test merging complex metadata structures."""
        dict1 = {
            "filename": "image.jpg",
            "size": 1024,
            "tags": ["nature", "landscape"],
            "metadata": {"camera": "Canon"}
        }
        dict2 = {
            "size": 2048,  # Override
            "tags": ["sunset"],  # Override (not append)
            "metadata": {"location": "Beach"},  # Override (not merge)
            "timestamp": "2024-01-01T00:00:00Z"
        }
        
        expected = {
            "filename": "image.jpg",
            "size": 2048,
            "tags": ["sunset"],
            "metadata": {"location": "Beach"},
            "timestamp": "2024-01-01T00:00:00Z"
        }
        result = merge_metadata(dict1, dict2)
        assert result == expected


if __name__ == "__main__":
    # Run tests if this file is executed directly
    pytest.main([__file__, "-v"]) 