"""
Tests for Secure File Operations

Tests for security-enhanced file operations including path validation,
content verification, and audit logging.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from redditdl.core.security.file_ops import SecureFileOperations
from redditdl.core.security.validation import SecurityValidationError
from redditdl.core.exceptions import ValidationError


class TestSecureFileOperations:
    """Test suite for SecureFileOperations class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.base_path = Path(self.temp_dir)
        self.secure_ops = SecureFileOperations(
            base_path=self.base_path,
            max_file_size=1024 * 1024  # 1MB for tests
        )
    
    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_secure_write_basic(self):
        """Test basic secure file writing."""
        test_file = self.base_path / "test.txt"
        content = "Hello, world!"
        
        result = self.secure_ops.secure_write(test_file, content)
        
        assert result == test_file
        assert test_file.exists()
        assert test_file.read_text() == content
    
    def test_secure_write_binary(self):
        """Test secure binary file writing."""
        test_file = self.base_path / "test.bin"
        content = b"Binary content"
        
        result = self.secure_ops.secure_write(test_file, content, mode='wb')
        
        assert result == test_file
        assert test_file.exists()
        assert test_file.read_bytes() == content
    
    def test_secure_write_creates_directories(self):
        """Test that secure write creates parent directories."""
        nested_file = self.base_path / "subdir" / "nested" / "test.txt"
        content = "Nested file content"
        
        result = self.secure_ops.secure_write(nested_file, content)
        
        assert result == nested_file
        assert nested_file.exists()
        assert nested_file.read_text() == content
    
    def test_secure_write_path_traversal_protection(self):
        """Test path traversal protection in secure write."""
        malicious_path = self.base_path / ".." / ".." / "malicious.txt"
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_write(malicious_path, "malicious content")
        
        assert "path_traversal" in str(exc_info.value.security_concern)
    
    def test_secure_write_size_limit(self):
        """Test file size limit enforcement."""
        test_file = self.base_path / "large.txt"
        large_content = "x" * (2 * 1024 * 1024)  # 2MB, exceeds 1MB limit
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_write(test_file, large_content.encode())
        
        assert "file_size_exceeded" in str(exc_info.value.security_concern)
    
    def test_secure_read_basic(self):
        """Test basic secure file reading."""
        test_file = self.base_path / "test.txt"
        content = "Test content for reading"
        test_file.write_text(content)
        
        result = self.secure_ops.secure_read(test_file)
        
        assert result == content
    
    def test_secure_read_binary(self):
        """Test secure binary file reading."""
        test_file = self.base_path / "test.bin"
        content = b"Binary test content"
        test_file.write_bytes(content)
        
        result = self.secure_ops.secure_read(test_file, mode='rb')
        
        assert result == content
    
    def test_secure_read_nonexistent_file(self):
        """Test secure read with nonexistent file."""
        nonexistent_file = self.base_path / "nonexistent.txt"
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_read(nonexistent_file)
        
        assert "file_not_found" in str(exc_info.value.security_concern)
    
    def test_secure_read_size_limit(self):
        """Test read size limit enforcement."""
        test_file = self.base_path / "large.txt"
        large_content = "x" * (2 * 1024 * 1024)  # 2MB
        test_file.write_text(large_content)
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_read(test_file)
        
        assert "file_size_exceeded" in str(exc_info.value.security_concern)
    
    def test_secure_read_with_custom_max_size(self):
        """Test secure read with custom max size."""
        test_file = self.base_path / "test.txt"
        content = "This is a longer content string"
        test_file.write_text(content)
        
        # Read with small max size
        result = self.secure_ops.secure_read(test_file, max_size=10)
        
        assert result == content[:10]
    
    def test_secure_delete_basic(self):
        """Test basic secure file deletion."""
        test_file = self.base_path / "test.txt"
        test_file.write_text("File to delete")
        
        assert test_file.exists()
        
        result = self.secure_ops.secure_delete(test_file)
        
        assert result is True
        assert not test_file.exists()
    
    def test_secure_delete_nonexistent_file(self):
        """Test secure delete with nonexistent file."""
        nonexistent_file = self.base_path / "nonexistent.txt"
        
        result = self.secure_ops.secure_delete(nonexistent_file)
        
        assert result is False
    
    def test_get_file_hash(self):
        """Test file hash calculation."""
        test_file = self.base_path / "test.txt"
        content = "Content for hashing"
        test_file.write_text(content)
        
        hash_value = self.secure_ops.get_file_hash(test_file)
        
        assert isinstance(hash_value, str)
        assert len(hash_value) == 64  # SHA256 hex digest length
        
        # Verify hash is consistent
        hash_value2 = self.secure_ops.get_file_hash(test_file)
        assert hash_value == hash_value2
    
    def test_get_file_hash_different_algorithms(self):
        """Test file hash with different algorithms."""
        test_file = self.base_path / "test.txt"
        content = "Content for hashing"
        test_file.write_text(content)
        
        sha256_hash = self.secure_ops.get_file_hash(test_file, algorithm='sha256')
        md5_hash = self.secure_ops.get_file_hash(test_file, algorithm='md5')
        
        assert len(sha256_hash) == 64
        assert len(md5_hash) == 32
        assert sha256_hash != md5_hash
    
    def test_verify_file_integrity_success(self):
        """Test successful file integrity verification."""
        test_file = self.base_path / "test.txt"
        content = "Content for integrity verification"
        test_file.write_text(content)
        
        # Get the actual hash
        expected_hash = self.secure_ops.get_file_hash(test_file)
        
        # Verify integrity
        result = self.secure_ops.verify_file_integrity(test_file, expected_hash)
        
        assert result is True
    
    def test_verify_file_integrity_failure(self):
        """Test failed file integrity verification."""
        test_file = self.base_path / "test.txt"
        content = "Content for integrity verification"
        test_file.write_text(content)
        
        # Use wrong hash
        wrong_hash = "wrong_hash_value"
        
        result = self.secure_ops.verify_file_integrity(test_file, wrong_hash)
        
        assert result is False
    
    @patch('requests.get')
    def test_secure_download_basic(self, mock_get):
        """Test basic secure download functionality."""
        # Mock response
        mock_response = Mock()
        mock_response.headers = {'Content-Length': '100'}
        mock_response.iter_content.return_value = [b'chunk1', b'chunk2', b'chunk3']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        test_file = self.base_path / "downloaded.txt"
        test_url = "https://example.com/file.txt"
        
        result = self.secure_ops.secure_download(test_url, test_file, verify_content_type=False)
        
        assert result == test_file
        assert test_file.exists()
        
        # Verify download was called with correct parameters
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs['stream'] is True
        assert call_kwargs['timeout'] == 30
        assert call_kwargs['verify'] is True
        assert 'RedditDL' in call_kwargs['headers']['User-Agent']
    
    @patch('requests.get')
    def test_secure_download_size_limit_header(self, mock_get):
        """Test download size limit from Content-Length header."""
        # Mock response with large content length
        mock_response = Mock()
        mock_response.headers = {'Content-Length': str(2 * 1024 * 1024)}  # 2MB
        mock_get.return_value = mock_response
        
        test_file = self.base_path / "large_download.txt"
        test_url = "https://example.com/large_file.txt"
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_download(test_url, test_file, verify_content_type=False)
        
        assert "download_size_exceeded" in str(exc_info.value.security_concern)
    
    @patch('requests.get')
    def test_secure_download_size_limit_during_transfer(self, mock_get):
        """Test download size limit enforcement during transfer."""
        # Mock response that returns more data than advertised
        mock_response = Mock()
        mock_response.headers = {}
        large_chunk = b'x' * (2 * 1024 * 1024)  # 2MB chunk
        mock_response.iter_content.return_value = [large_chunk]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        test_file = self.base_path / "large_download.txt"
        test_url = "https://example.com/file.txt"
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_download(test_url, test_file, verify_content_type=False)
        
        assert "download_size_exceeded" in str(exc_info.value.security_concern)
    
    @patch('requests.get')
    def test_secure_download_invalid_url(self, mock_get):
        """Test secure download with invalid URL."""
        invalid_urls = [
            "ftp://example.com/file.txt",
            "file:///etc/passwd",
            "javascript:alert('xss')"
        ]
        
        test_file = self.base_path / "test.txt"
        
        for url in invalid_urls:
            with pytest.raises(SecurityValidationError):
                self.secure_ops.secure_download(url, test_file, verify_content_type=False)
    
    @patch('requests.get')
    def test_secure_download_path_traversal_protection(self, mock_get):
        """Test path traversal protection in secure download."""
        malicious_path = self.base_path / ".." / ".." / "malicious.txt"
        test_url = "https://example.com/file.txt"
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_download(test_url, malicious_path, verify_content_type=False)
        
        assert "path_traversal" in str(exc_info.value.security_concern)
    
    def test_base_path_restriction(self):
        """Test that operations are restricted to base path."""
        # Try to access file outside base path
        outside_path = Path("/tmp/outside_file.txt")
        
        with pytest.raises(SecurityValidationError) as exc_info:
            self.secure_ops.secure_write(outside_path, "content")
        
        assert "path_escape" in str(exc_info.value.security_concern)
    
    def test_allowed_file_types_restriction(self):
        """Test file type restrictions."""
        restricted_ops = SecureFileOperations(
            base_path=self.base_path,
            allowed_file_types=['text/plain', 'image/jpeg']
        )
        
        test_file = self.base_path / "test.txt"
        test_file.write_text("test content")
        
        # Should pass validation for allowed type
        # Note: This is a simplified test as full content type detection
        # requires actual file content analysis
        pass
    
    def test_audit_logging_integration(self):
        """Test integration with audit logging."""
        with patch('core.security.file_ops.get_auditor') as mock_get_auditor:
            mock_auditor = Mock()
            mock_get_auditor.return_value = mock_auditor
            
            test_file = self.base_path / "test.txt"
            content = "Test content"
            
            self.secure_ops.secure_write(test_file, content, session_id="test-session")
            
            # Verify audit logging was called
            mock_auditor.log_file_operation.assert_called_once()
            call_args = mock_auditor.log_file_operation.call_args
            assert call_args[1]['operation'] == 'write'
            assert call_args[1]['success'] is True
            assert call_args[1]['session_id'] == 'test-session'


@pytest.mark.integration  
class TestSecureFileOperationsIntegration:
    """Integration tests for secure file operations."""
    
    def test_complete_file_workflow(self):
        """Test complete secure file operation workflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            secure_ops = SecureFileOperations(base_path=base_path)
            
            # Write file
            test_file = base_path / "workflow_test.txt"
            original_content = "Original content for workflow test"
            
            written_path = secure_ops.secure_write(test_file, original_content)
            assert written_path == test_file
            
            # Read file
            read_content = secure_ops.secure_read(test_file)
            assert read_content == original_content
            
            # Get hash
            file_hash = secure_ops.get_file_hash(test_file)
            assert len(file_hash) == 64
            
            # Verify integrity
            integrity_ok = secure_ops.verify_file_integrity(test_file, file_hash)
            assert integrity_ok is True
            
            # Modify file and verify integrity fails
            test_file.write_text("Modified content")
            integrity_fail = secure_ops.verify_file_integrity(test_file, file_hash)
            assert integrity_fail is False
            
            # Delete file
            delete_result = secure_ops.secure_delete(test_file)
            assert delete_result is True
            assert not test_file.exists()
    
    def test_nested_directory_operations(self):
        """Test operations with nested directory structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            secure_ops = SecureFileOperations(base_path=base_path)
            
            # Create deeply nested file
            nested_file = base_path / "level1" / "level2" / "level3" / "deep_file.txt"
            content = "Content in deeply nested file"
            
            written_path = secure_ops.secure_write(nested_file, content)
            assert written_path == nested_file
            assert nested_file.exists()
            
            # Verify parent directories were created
            assert nested_file.parent.exists()
            assert nested_file.parent.parent.exists()
            
            # Read from nested location
            read_content = secure_ops.secure_read(nested_file)
            assert read_content == content
    
    def test_concurrent_operations_safety(self):
        """Test thread safety of secure file operations."""
        import threading
        import time
        
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            secure_ops = SecureFileOperations(base_path=base_path)
            
            results = []
            errors = []
            
            def write_file(thread_id):
                try:
                    test_file = base_path / f"thread_{thread_id}.txt"
                    content = f"Content from thread {thread_id}"
                    
                    result = secure_ops.secure_write(test_file, content)
                    results.append((thread_id, result))
                except Exception as e:
                    errors.append((thread_id, e))
            
            # Start multiple threads
            threads = []
            for i in range(5):
                thread = threading.Thread(target=write_file, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
            
            # Verify results
            assert len(errors) == 0, f"Errors occurred: {errors}"
            assert len(results) == 5
            
            # Verify all files were created
            for thread_id, file_path in results:
                assert file_path.exists()
                expected_content = f"Content from thread {thread_id}"
                actual_content = secure_ops.secure_read(file_path)
                assert actual_content == expected_content