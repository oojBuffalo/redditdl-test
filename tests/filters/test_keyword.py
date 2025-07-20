"""
Tests for KeywordFilter functionality.

This module tests keyword-based filtering with inclusion/exclusion and various options.
"""

import pytest
from datetime import datetime
from redditdl.filters.keyword import KeywordFilter
from redditdl.scrapers import PostMetadata


class TestKeywordFilter:
    """Test KeywordFilter functionality."""
    
    def create_post_with_text(self, title: str, selftext: str = "") -> PostMetadata:
        """Create a test post with specified title and selftext."""
        return PostMetadata(
            id="test_keyword",
            title=title,
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            created_utc=datetime.now().timestamp(),
            score=10,
            is_nsfw=False,
            selftext=selftext
        )
    
    def test_include_keywords_pass(self):
        """Test include keywords filter with matching post."""
        filter_obj = KeywordFilter(keywords_include=["python", "programming"])
        post = self.create_post_with_text("Learning Python Programming", "This is about Python programming")
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "contains required keywords" in result.reason
        assert result.metadata["matched_include_keywords"] == ["python", "programming"]
    
    def test_include_keywords_fail(self):
        """Test include keywords filter with non-matching post."""
        filter_obj = KeywordFilter(keywords_include=["python", "programming"])
        post = self.create_post_with_text("Learning JavaScript", "This is about web development")
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "does not contain required keywords" in result.reason
        assert result.metadata["matched_include_keywords"] == []
    
    def test_exclude_keywords_pass(self):
        """Test exclude keywords filter with non-matching post."""
        filter_obj = KeywordFilter(keywords_exclude=["spam", "advertisement"])
        post = self.create_post_with_text("Genuine Python Tutorial", "Learn Python programming")
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "does not contain excluded keywords" in result.reason
        assert result.metadata["matched_exclude_keywords"] == []
    
    def test_exclude_keywords_fail(self):
        """Test exclude keywords filter with matching post."""
        filter_obj = KeywordFilter(keywords_exclude=["spam", "advertisement"])
        post = self.create_post_with_text("Great Advertisement Deal!", "This is spam content")
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "contains excluded keywords" in result.reason
        assert result.metadata["matched_exclude_keywords"] == ["advertisement", "spam"]
    
    def test_combined_keywords_pass(self):
        """Test combined include/exclude keywords with passing post."""
        filter_obj = KeywordFilter(
            keywords_include=["python", "tutorial"],
            keywords_exclude=["spam", "advertisement"]
        )
        post = self.create_post_with_text("Python Tutorial for Beginners", "Learn Python programming")
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "contains required keywords" in result.reason
        assert "does not contain excluded keywords" in result.reason
    
    def test_combined_keywords_fail_include(self):
        """Test combined keywords failing on include requirement."""
        filter_obj = KeywordFilter(
            keywords_include=["python", "tutorial"],
            keywords_exclude=["spam", "advertisement"]
        )
        post = self.create_post_with_text("JavaScript Basics", "Learn web development")
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "does not contain required keywords" in result.reason
    
    def test_combined_keywords_fail_exclude(self):
        """Test combined keywords failing on exclude requirement."""
        filter_obj = KeywordFilter(
            keywords_include=["python", "tutorial"],
            keywords_exclude=["spam", "advertisement"]
        )
        post = self.create_post_with_text("Python Tutorial - Advertisement", "Python programming spam")
        
        result = filter_obj.apply(post)
        
        assert result.passed is False
        assert "contains excluded keywords" in result.reason
    
    def test_case_sensitive_matching(self):
        """Test case-sensitive keyword matching."""
        filter_obj = KeywordFilter(
            keywords_include=["Python"],
            case_sensitive=True
        )
        
        # Should pass - exact case match
        post = self.create_post_with_text("Python Programming", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Should fail - case mismatch
        post = self.create_post_with_text("python programming", "")
        result = filter_obj.apply(post)
        assert result.passed is False
    
    def test_case_insensitive_matching(self):
        """Test case-insensitive keyword matching (default)."""
        filter_obj = KeywordFilter(keywords_include=["Python"])
        
        # Both should pass
        post = self.create_post_with_text("Python Programming", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        post = self.create_post_with_text("python programming", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        post = self.create_post_with_text("PYTHON PROGRAMMING", "")
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_regex_mode(self):
        """Test regex mode keyword matching."""
        filter_obj = KeywordFilter(
            keywords_include=[r"python\\d+", r"version\\s+\\d+"],
            regex_mode=True
        )
        
        # Should pass - matches regex patterns
        post = self.create_post_with_text("python3 version 3.9", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Should fail - doesn't match regex
        post = self.create_post_with_text("python programming", "")
        result = filter_obj.apply(post)
        assert result.passed is False
    
    def test_whole_words_mode(self):
        """Test whole words only matching."""
        filter_obj = KeywordFilter(
            keywords_include=["cat"],
            whole_words_only=True
        )
        
        # Should pass - whole word match
        post = self.create_post_with_text("I have a cat", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Should fail - substring match only
        post = self.create_post_with_text("I have a catch", "")
        result = filter_obj.apply(post)
        assert result.passed is False
        
        # Should fail - substring match only
        post = self.create_post_with_text("concatenation", "")
        result = filter_obj.apply(post)
        assert result.passed is False
    
    def test_substring_mode(self):
        """Test substring matching (default)."""
        filter_obj = KeywordFilter(keywords_include=["cat"])
        
        # All should pass - substring matches
        post = self.create_post_with_text("I have a cat", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        post = self.create_post_with_text("I have a catch", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        post = self.create_post_with_text("concatenation", "")
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_selftext_matching(self):
        """Test keyword matching in selftext."""
        filter_obj = KeywordFilter(keywords_include=["tutorial"])
        
        # Should pass - keyword in selftext
        post = self.create_post_with_text("Python Post", "This is a tutorial about Python")
        result = filter_obj.apply(post)
        assert result.passed is True
        
        # Should pass - keyword in title
        post = self.create_post_with_text("Python Tutorial", "Learn programming")
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_empty_text_handling(self):
        """Test handling of posts with empty or missing text."""
        filter_obj = KeywordFilter(keywords_include=["python"])
        
        # Post with empty title and selftext
        post = self.create_post_with_text("", "")
        result = filter_obj.apply(post)
        assert result.passed is False
        
        # Post with None selftext
        post = PostMetadata(
            id="test_empty",
            title="",
            url="https://example.com/test",
            author="testuser",
            subreddit="testsubreddit",
            created_utc=datetime.now().timestamp(),
            score=10,
            is_nsfw=False,
            selftext=None
        )
        result = filter_obj.apply(post)
        assert result.passed is False
    
    def test_no_keywords(self):
        """Test filter with no keywords configured."""
        filter_obj = KeywordFilter()
        post = self.create_post_with_text("Any content", "Any selftext")
        
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert "No keyword constraints" in result.reason
    
    def test_special_characters(self):
        """Test keyword matching with special characters."""
        filter_obj = KeywordFilter(keywords_include=["C++", "C#", ".NET"])
        
        post = self.create_post_with_text("Learning C++ and C# with .NET", "")
        result = filter_obj.apply(post)
        assert result.passed is True
        assert len(result.metadata["matched_include_keywords"]) == 3
    
    def test_unicode_support(self):
        """Test keyword matching with Unicode characters."""
        filter_obj = KeywordFilter(keywords_include=["programmation", "développement"])
        
        post = self.create_post_with_text("Cours de programmation", "Apprenez le développement")
        result = filter_obj.apply(post)
        assert result.passed is True
    
    def test_filter_properties(self):
        """Test filter properties and metadata."""
        filter_obj = KeywordFilter(
            keywords_include=["python", "tutorial"],
            keywords_exclude=["spam"],
            case_sensitive=True,
            regex_mode=True,
            whole_words_only=True
        )
        
        assert filter_obj.name == "keyword"
        assert "Keyword Filter" in filter_obj.description
        assert "python" in filter_obj.description
        assert "tutorial" in filter_obj.description
        assert "spam" in filter_obj.description
        assert "case_sensitive=True" in filter_obj.description
        assert "regex_mode=True" in filter_obj.description
        assert "whole_words_only=True" in filter_obj.description
    
    def test_filter_validation(self):
        """Test filter configuration validation."""
        # Valid configuration
        filter_obj = KeywordFilter(
            keywords_include=["python", "tutorial"],
            keywords_exclude=["spam"]
        )
        errors = filter_obj.validate_config()
        assert errors == []
        
        # Empty keywords (valid)
        filter_obj = KeywordFilter()
        assert filter_obj.validate_config() == []
        
        # Invalid regex patterns
        filter_obj = KeywordFilter(
            keywords_include=["[invalid_regex"],
            regex_mode=True
        )
        errors = filter_obj.validate_config()
        assert len(errors) > 0
        assert "Invalid regex pattern" in errors[0]
    
    def test_performance_with_many_keywords(self):
        """Test filter performance with many keywords."""
        # Create filter with many keywords
        many_keywords = [f"keyword{i}" for i in range(100)]
        filter_obj = KeywordFilter(keywords_include=many_keywords)
        
        post = self.create_post_with_text("This contains keyword50", "")
        result = filter_obj.apply(post)
        
        assert result.passed is True
        assert result.execution_time < 1.0  # Should be fast
        assert "keyword50" in result.metadata["matched_include_keywords"]


if __name__ == "__main__":
    pytest.main([__file__])