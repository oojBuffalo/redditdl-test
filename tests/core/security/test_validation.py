"""
Tests for Security Validation Module

Comprehensive tests for input validation, path security, URL validation,
and content verification functionality.
"""

import pytest
import tempfile
from pathlib import Path

from redditdl.core.security.validation import InputValidator, SecurityValidationError
from redditdl.core.exceptions import ValidationError, ErrorCode


class TestInputValidator:
    """Test suite for InputValidator class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.validator = InputValidator()
    
    def test_validate_path_basic(self):
        """Test basic path validation."""
        # Valid absolute path
        with tempfile.TemporaryDirectory() as tmpdir:
            valid_path = Path(tmpdir) / "test.txt"
            result = self.validator.validate_path(valid_path)
            assert isinstance(result, Path)
    
    def test_validate_path_traversal_attack(self):
        """Test path traversal attack prevention."""
        malicious_paths = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32",
            "/var/../../etc/passwd",
            "test/../../../etc/passwd"
        ]
        
        for path in malicious_paths:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_path(path)
            assert "path_traversal" in str(exc_info.value.security_concern)
    
    def test_validate_path_with_base_path(self):
        """Test path validation with base path restriction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            
            # Valid path within base
            valid_path = base_path / "subdir" / "file.txt"
            result = self.validator.validate_path(valid_path, base_path=base_path)
            assert str(result).startswith(str(base_path))
            
            # Invalid path outside base
            invalid_path = Path("/tmp/outside.txt")
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_path(invalid_path, base_path=base_path)
            assert "path_escape" in str(exc_info.value.security_concern)
    
    def test_validate_path_suspicious_patterns(self):
        """Test detection of suspicious path patterns."""
        suspicious_paths = [
            "/dev/null",
            "/proc/self/exe",
            "\\\\.\\.\\pipe\\test",
            "CON",
            "AUX",
            "PRN"
        ]
        
        for path in suspicious_paths:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_path(path)
            assert "suspicious_path" in str(exc_info.value.security_concern)
    
    def test_sanitize_filename_basic(self):
        """Test basic filename sanitization."""
        test_cases = [
            ("normal_file.txt", "normal_file.txt"),
            ("file with spaces.txt", "file_with_spaces.txt"),
            ("file/with\\bad:chars?.txt", "file_with_bad_chars_.txt"),
            ("", "unnamed_file"),
            (None, "unnamed_file")
        ]
        
        for input_name, expected in test_cases:
            if input_name is None:
                with pytest.raises(AttributeError):
                    self.validator.sanitize_filename(input_name)
            else:
                result = self.validator.sanitize_filename(input_name)
                assert result == expected
    
    def test_sanitize_filename_dangerous_extensions(self):
        """Test detection of dangerous file extensions."""
        dangerous_files = [
            "malware.exe",
            "script.bat",
            "virus.scr",
            "trojan.com"
        ]
        
        for filename in dangerous_files:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.sanitize_filename(filename)
            assert "dangerous_extension" in str(exc_info.value.security_concern)
    
    def test_sanitize_filename_control_characters(self):
        """Test detection of control characters in filenames."""
        malicious_names = [
            "file\x00name.txt",  # Null byte
            "file\x01name.txt",  # Control character
            "file\x1fname.txt"   # Unit separator
        ]
        
        for filename in malicious_names:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.sanitize_filename(filename)
            assert "control_characters" in str(exc_info.value.security_concern)
    
    def test_sanitize_filename_reserved_names(self):
        """Test handling of Windows reserved names."""
        reserved_files = [
            "CON.txt",
            "PRN.doc",
            "AUX.log",
            "NUL.dat",
            "COM1.txt",
            "LPT1.txt"
        ]
        
        for filename in reserved_files:
            result = self.validator.sanitize_filename(filename)
            assert result.startswith("safe_")
    
    def test_validate_url_basic(self):
        """Test basic URL validation."""
        valid_urls = [
            "https://www.reddit.com",
            "http://reddit.com/r/test",
            "https://old.reddit.com/user/test"
        ]
        
        for url in valid_urls:
            result = self.validator.validate_url(url)
            assert result == url
    
    def test_validate_url_invalid_schemes(self):
        """Test rejection of invalid URL schemes."""
        invalid_urls = [
            "ftp://example.com",
            "file:///etc/passwd",
            "javascript:alert('xss')",
            "data:text/html,<script>alert('xss')</script>"
        ]
        
        for url in invalid_urls:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_url(url)
            assert "unsupported_scheme" in str(exc_info.value.security_concern)
    
    def test_validate_url_blocked_domains(self):
        """Test blocking of prohibited domains."""
        blocked_urls = [
            "https://localhost/test",
            "http://127.0.0.1/malicious",
            "https://0.0.0.0/test"
        ]
        
        for url in blocked_urls:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_url(url)
            assert "blocked_domain" in str(exc_info.value.security_concern)
    
    def test_validate_url_private_ips(self):
        """Test handling of private IP addresses."""
        private_urls = [
            "https://192.168.1.1/test",
            "http://10.0.0.1/malicious",
            "https://172.16.0.1/test"
        ]
        
        # Should fail without allow_private_ips
        for url in private_urls:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_url(url, allow_private_ips=False)
            assert "private_ip" in str(exc_info.value.security_concern)
        
        # Should pass with allow_private_ips=True
        for url in private_urls:
            result = self.validator.validate_url(url, allow_private_ips=True)
            assert result == url
    
    def test_validate_url_suspicious_patterns(self):
        """Test detection of suspicious URL patterns."""
        suspicious_urls = [
            "https://example.com/test?param=%3Cscript%3E",
            "http://test.com/javascript:alert(1)",
            "https://site.com/data:text/html,malicious"
        ]
        
        for url in suspicious_urls:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_url(url)
            assert "suspicious_url_pattern" in str(exc_info.value.security_concern)
    
    def test_validate_target_users(self):
        """Test Reddit user target validation."""
        valid_users = [
            "u/validuser",
            "/u/validuser",
            "validuser"
        ]
        
        for target in valid_users:
            target_type, normalized = self.validator.validate_target(target)
            assert target_type in ['user', 'subreddit']
            assert isinstance(normalized, str)
    
    def test_validate_target_subreddits(self):
        """Test Reddit subreddit target validation."""
        valid_subreddits = [
            "r/test",
            "/r/test",
            "test"
        ]
        
        for target in valid_subreddits:
            target_type, normalized = self.validator.validate_target(target)
            assert target_type in ['subreddit', 'user']
            assert isinstance(normalized, str)
    
    def test_validate_target_urls(self):
        """Test Reddit URL target validation."""
        valid_urls = [
            "https://www.reddit.com/r/test",
            "https://old.reddit.com/user/test",
            "https://reddit.com/r/programming"
        ]
        
        for target in valid_urls:
            target_type, normalized = self.validator.validate_target(target)
            assert target_type == 'url'
            assert normalized == target
        
        # Test non-Reddit URLs
        invalid_urls = [
            "https://www.google.com",
            "https://facebook.com/test"
        ]
        
        for target in invalid_urls:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_target(target)
            assert "non_reddit_url" in str(exc_info.value.security_concern)
    
    def test_validate_target_invalid_formats(self):
        """Test rejection of invalid target formats."""
        invalid_targets = [
            "",
            "   ",
            "invalid/format/with/too/many/slashes",
            "user@domain.com",
            "r/test with spaces"
        ]
        
        for target in invalid_targets:
            with pytest.raises(SecurityValidationError):
                self.validator.validate_target(target)
    
    def test_validate_reddit_name_length(self):
        """Test Reddit name length validation."""
        # Too long
        with pytest.raises(SecurityValidationError) as exc_info:
            self.validator._validate_reddit_name("a" * 21, "username")
        assert "username_too_long" in str(exc_info.value.security_concern)
        
        # Valid length
        result = self.validator._validate_reddit_name("validname", "username")
        assert result == "validname"
    
    def test_validate_reddit_name_characters(self):
        """Test Reddit name character validation."""
        invalid_names = [
            "user with spaces",
            "user@domain",
            "user.name",
            "user#tag"
        ]
        
        for name in invalid_names:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator._validate_reddit_name(name, "username")
            assert "invalid_username_chars" in str(exc_info.value.security_concern)
    
    def test_validate_reddit_name_reserved(self):
        """Test Reddit reserved name detection."""
        reserved_names = ["admin", "reddit", "mod", "null"]
        
        for name in reserved_names:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator._validate_reddit_name(name, "username")
            assert "reserved_username" in str(exc_info.value.security_concern)
    
    def test_validate_file_type_basic(self):
        """Test basic file type validation."""
        with tempfile.NamedTemporaryFile(suffix='.txt') as f:
            f.write(b'test content')
            f.flush()
            
            mime_type = self.validator.validate_file_type(f.name)
            assert 'text' in mime_type or 'document' in mime_type
    
    def test_validate_file_type_with_content(self):
        """Test file type validation with content checking."""
        # JPEG header
        jpeg_header = b'\xff\xd8\xff\xe0'
        
        with tempfile.NamedTemporaryFile(suffix='.jpg') as f:
            mime_type = self.validator.validate_file_type(
                f.name, 
                content=jpeg_header + b'fake jpeg data'
            )
            assert 'image/jpeg' == mime_type
    
    def test_validate_file_type_extension_mismatch(self):
        """Test detection of extension/content mismatch."""
        # PNG content but JPG extension
        png_header = b'\x89PNG\r\n\x1a\n'
        
        with tempfile.NamedTemporaryFile(suffix='.jpg') as f:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_file_type(
                    f.name,
                    content=png_header + b'fake png data'
                )
            assert "extension_mismatch" in str(exc_info.value.security_concern)
    
    def test_validate_config_value_type_validation(self):
        """Test configuration value type validation."""
        # Valid type
        result = self.validator.validate_config_value('test', 42, int)
        assert result == 42
        
        # Invalid type (should convert)
        result = self.validator.validate_config_value('test', '42', int)
        assert result == 42
        
        # Unconvertible type
        with pytest.raises(SecurityValidationError) as exc_info:
            self.validator.validate_config_value('test', 'invalid', int)
        assert "type_conversion_failed" in str(exc_info.value.security_concern)
    
    def test_validate_config_value_range_validation(self):
        """Test configuration value range validation."""
        constraints = {'min_value': 10, 'max_value': 100}
        
        # Valid range
        result = self.validator.validate_config_value('test', 50, int, constraints)
        assert result == 50
        
        # Below minimum
        with pytest.raises(SecurityValidationError) as exc_info:
            self.validator.validate_config_value('test', 5, int, constraints)
        assert "value_below_minimum" in str(exc_info.value.security_concern)
        
        # Above maximum
        with pytest.raises(SecurityValidationError) as exc_info:
            self.validator.validate_config_value('test', 150, int, constraints)
        assert "value_above_maximum" in str(exc_info.value.security_concern)
    
    def test_validate_config_value_choices(self):
        """Test configuration value choice validation."""
        constraints = {'choices': ['option1', 'option2', 'option3']}
        
        # Valid choice
        result = self.validator.validate_config_value('test', 'option1', str, constraints)
        assert result == 'option1'
        
        # Invalid choice
        with pytest.raises(SecurityValidationError) as exc_info:
            self.validator.validate_config_value('test', 'invalid', str, constraints)
        assert "invalid_choice" in str(exc_info.value.security_concern)
    
    def test_validate_config_value_string_patterns(self):
        """Test string pattern validation."""
        constraints = {'pattern': r'^[a-zA-Z0-9_]+$'}
        
        # Valid pattern
        result = self.validator.validate_config_value('test', 'valid_name', str, constraints)
        assert result == 'valid_name'
        
        # Invalid pattern
        with pytest.raises(SecurityValidationError) as exc_info:
            self.validator.validate_config_value('test', 'invalid-name!', str, constraints)
        assert "pattern_mismatch" in str(exc_info.value.security_concern)
    
    def test_validate_config_value_suspicious_content(self):
        """Test detection of suspicious content in config values."""
        suspicious_values = [
            "<script>alert('xss')</script>",
            "javascript:alert(1)",
            "eval('malicious code')",
            "../../../etc/passwd",
            "cmd.exe /c malicious"
        ]
        
        for value in suspicious_values:
            with pytest.raises(SecurityValidationError) as exc_info:
                self.validator.validate_config_value('test', value, str)
            assert "suspicious_content" in str(exc_info.value.security_concern)
    
    def test_contains_suspicious_content(self):
        """Test internal suspicious content detection."""
        clean_values = [
            "normal string",
            "file.txt",
            "user@example.com",
            "https://safe.com"
        ]
        
        for value in clean_values:
            assert not self.validator._contains_suspicious_content(value)
        
        suspicious_values = [
            "<script>",
            "javascript:",
            "eval(",
            "../",
            "\\..\\",
            "/etc/passwd",
            "powershell"
        ]
        
        for value in suspicious_values:
            assert self.validator._contains_suspicious_content(value)


@pytest.mark.integration
class TestValidationIntegration:
    """Integration tests for validation components."""
    
    def test_validation_with_real_paths(self):
        """Test validation with real file system paths."""
        validator = InputValidator()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test structure
            test_dir = Path(tmpdir) / "test"
            test_dir.mkdir()
            test_file = test_dir / "file.txt"
            test_file.write_text("test content")
            
            # Validate real paths
            validated_dir = validator.validate_path(test_dir)
            assert validated_dir.exists()
            
            validated_file = validator.validate_path(test_file)
            assert validated_file.exists()
    
    def test_end_to_end_security_validation(self):
        """Test complete security validation workflow."""
        validator = InputValidator()
        
        # Simulate user input processing
        user_target = "r/test"
        user_filename = "my file (2023).txt"
        user_url = "https://reddit.com/r/test"
        
        # Validate all inputs
        target_type, normalized_target = validator.validate_target(user_target)
        safe_filename = validator.sanitize_filename(user_filename)
        validated_url = validator.validate_url(user_url)
        
        # Verify results
        assert target_type == 'subreddit'
        assert normalized_target == 'test'
        assert safe_filename == "my_file__2023_.txt"
        assert validated_url == user_url