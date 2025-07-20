"""
Secure File Operations

Provides security-enhanced file operations with path validation, type checking,
and audit logging to prevent common security vulnerabilities.
"""

import os
import shutil
import hashlib
import tempfile
from pathlib import Path
from typing import Union, Optional, BinaryIO, Any, Dict, List
import mimetypes

from .validation import InputValidator, SecurityValidationError
from .audit import get_auditor, SecurityEvent, EventType, Severity
from redditdl.core.exceptions import ValidationError, ErrorCode


class SecureFileOperations:
    """
    Security-enhanced file operations with comprehensive validation and logging.
    
    Provides secure wrappers around common file operations with path traversal
    protection, file type validation, and comprehensive audit logging.
    """
    
    def __init__(self, base_path: Optional[Union[str, Path]] = None,
                 allowed_file_types: Optional[List[str]] = None,
                 max_file_size: int = 100 * 1024 * 1024):  # 100MB default
        """
        Initialize secure file operations.
        
        Args:
            base_path: Base directory to restrict operations to
            allowed_file_types: List of allowed MIME types
            max_file_size: Maximum allowed file size in bytes
        """
        self.validator = InputValidator()
        self.auditor = get_auditor()
        self.base_path = Path(base_path).resolve() if base_path else None
        self.allowed_file_types = allowed_file_types
        self.max_file_size = max_file_size
    
    def secure_write(self, file_path: Union[str, Path], 
                    content: Union[str, bytes],
                    mode: str = 'w',
                    encoding: str = 'utf-8',
                    session_id: Optional[str] = None,
                    user_id: Optional[str] = None) -> Path:
        """
        Securely write content to a file with validation and logging.
        
        Args:
            file_path: Path to write to
            content: Content to write
            mode: File mode ('w', 'wb', etc.)
            encoding: Text encoding (for text mode)
            session_id: Optional session ID for logging
            user_id: Optional user ID for logging
            
        Returns:
            Path object of written file
            
        Raises:
            SecurityValidationError: If path or content validation fails
        """
        # Validate path
        validated_path = self.validator.validate_path(
            file_path, 
            base_path=self.base_path,
            allow_create=True
        )
        
        # Check file size for binary content
        if isinstance(content, bytes) and len(content) > self.max_file_size:
            self.auditor.log_security_violation(
                violation_type="file_size_exceeded",
                description=f"File size {len(content)} exceeds limit {self.max_file_size}",
                severity=Severity.MEDIUM,
                user_id=user_id,
                session_id=session_id,
                resource=str(validated_path)
            )
            raise SecurityValidationError(
                f"File size {len(content)} exceeds maximum {self.max_file_size}",
                security_concern="file_size_exceeded",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # Create parent directories securely
        parent_dir = validated_path.parent
        if not parent_dir.exists():
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                self.auditor.log_file_operation(
                    operation="write",
                    file_path=str(validated_path),
                    success=False,
                    user_id=user_id,
                    session_id=session_id,
                    error_message=f"Failed to create directory: {e}"
                )
                raise SecurityValidationError(
                    f"Failed to create directory {parent_dir}: {e}",
                    security_concern="directory_creation_failed",
                    error_code=ErrorCode.FS_PERMISSION_DENIED
                )
        
        # Perform write operation
        try:
            if 'b' in mode:
                with open(validated_path, mode) as f:
                    f.write(content)
            else:
                with open(validated_path, mode, encoding=encoding) as f:
                    f.write(content)
            
            # Log successful operation
            self.auditor.log_file_operation(
                operation="write",
                file_path=str(validated_path),
                success=True,
                user_id=user_id,
                session_id=session_id
            )
            
            return validated_path
            
        except Exception as e:
            self.auditor.log_file_operation(
                operation="write",
                file_path=str(validated_path),
                success=False,
                user_id=user_id,
                session_id=session_id,
                error_message=str(e)
            )
            raise
    
    def secure_read(self, file_path: Union[str, Path],
                   mode: str = 'r',
                   encoding: str = 'utf-8',
                   max_size: Optional[int] = None,
                   session_id: Optional[str] = None,
                   user_id: Optional[str] = None) -> Union[str, bytes]:
        """
        Securely read content from a file with validation and logging.
        
        Args:
            file_path: Path to read from
            mode: File mode ('r', 'rb', etc.)
            encoding: Text encoding (for text mode)
            max_size: Maximum bytes to read (None for no limit)
            session_id: Optional session ID for logging
            user_id: Optional user ID for logging
            
        Returns:
            File content as string or bytes
            
        Raises:
            SecurityValidationError: If path validation fails
        """
        # Validate path
        validated_path = self.validator.validate_path(
            file_path,
            base_path=self.base_path,
            allow_create=False
        )
        
        if not validated_path.exists():
            self.auditor.log_file_operation(
                operation="read",
                file_path=str(validated_path),
                success=False,
                user_id=user_id,
                session_id=session_id,
                error_message="File not found"
            )
            raise SecurityValidationError(
                f"File not found: {validated_path}",
                security_concern="file_not_found",
                error_code=ErrorCode.FS_FILE_NOT_FOUND
            )
        
        # Check file size
        file_size = validated_path.stat().st_size
        effective_max_size = max_size or self.max_file_size
        
        if file_size > effective_max_size:
            self.auditor.log_security_violation(
                violation_type="file_size_exceeded",
                description=f"File size {file_size} exceeds read limit {effective_max_size}",
                severity=Severity.MEDIUM,
                user_id=user_id,
                session_id=session_id,
                resource=str(validated_path)
            )
            raise SecurityValidationError(
                f"File size {file_size} exceeds read limit {effective_max_size}",
                security_concern="file_size_exceeded",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # Perform read operation
        try:
            if 'b' in mode:
                with open(validated_path, mode) as f:
                    content = f.read(max_size)
            else:
                with open(validated_path, mode, encoding=encoding) as f:
                    content = f.read(max_size)
            
            # Log successful operation
            self.auditor.log_file_operation(
                operation="read",
                file_path=str(validated_path),
                success=True,
                user_id=user_id,
                session_id=session_id
            )
            
            return content
            
        except Exception as e:
            self.auditor.log_file_operation(
                operation="read",
                file_path=str(validated_path),
                success=False,
                user_id=user_id,
                session_id=session_id,
                error_message=str(e)
            )
            raise
    
    def secure_download(self, url: str, file_path: Union[str, Path],
                       verify_content_type: bool = True,
                       session_id: Optional[str] = None,
                       user_id: Optional[str] = None,
                       chunk_size: int = 8192) -> Path:
        """
        Securely download file from URL with validation and logging.
        
        Args:
            url: URL to download from
            file_path: Local path to save to
            verify_content_type: Whether to verify content type matches extension
            session_id: Optional session ID for logging
            user_id: Optional user ID for logging
            chunk_size: Download chunk size
            
        Returns:
            Path object of downloaded file
            
        Raises:
            SecurityValidationError: If validation fails
        """
        import requests
        
        # Validate URL
        validated_url = self.validator.validate_url(url)
        
        # Validate path
        validated_path = self.validator.validate_path(
            file_path,
            base_path=self.base_path,
            allow_create=True
        )
        
        # Use temporary file for download
        temp_file = None
        try:
            # Create temporary file in same directory as target
            temp_file = tempfile.NamedTemporaryFile(
                dir=validated_path.parent,
                delete=False,
                suffix='.tmp'
            )
            
            # Download with security headers
            headers = {
                'User-Agent': 'RedditDL/0.2.0 (Secure Downloader)',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
            
            response = requests.get(
                validated_url, 
                stream=True, 
                headers=headers,
                timeout=30,
                allow_redirects=True,
                verify=True  # Verify SSL certificates
            )
            response.raise_for_status()
            
            # Check Content-Length header
            content_length = response.headers.get('Content-Length')
            if content_length:
                size = int(content_length)
                if size > self.max_file_size:
                    self.auditor.log_security_violation(
                        violation_type="download_size_exceeded",
                        description=f"Download size {size} exceeds limit {self.max_file_size}",
                        severity=Severity.MEDIUM,
                        user_id=user_id,
                        session_id=session_id,
                        resource=validated_url
                    )
                    raise SecurityValidationError(
                        f"Download size {size} exceeds limit {self.max_file_size}",
                        security_concern="download_size_exceeded",
                        error_code=ErrorCode.VALIDATION_RANGE_ERROR
                    )
            
            # Download content with size monitoring
            downloaded_size = 0
            content_buffer = b''
            
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    downloaded_size += len(chunk)
                    
                    # Check size limit during download
                    if downloaded_size > self.max_file_size:
                        self.auditor.log_security_violation(
                            violation_type="download_size_exceeded",
                            description=f"Download size exceeded during transfer: {downloaded_size}",
                            severity=Severity.MEDIUM,
                            user_id=user_id,
                            session_id=session_id,
                            resource=validated_url
                        )
                        raise SecurityValidationError(
                            f"Download size exceeded during transfer: {downloaded_size}",
                            security_concern="download_size_exceeded",
                            error_code=ErrorCode.VALIDATION_RANGE_ERROR
                        )
                    
                    temp_file.write(chunk)
                    content_buffer += chunk
                    
                    # Keep buffer size reasonable for content type checking
                    if len(content_buffer) > 1024:
                        content_buffer = content_buffer[:1024]
            
            temp_file.close()
            
            # Verify content type if requested
            if verify_content_type:
                detected_type = self.validator.validate_file_type(
                    validated_path,
                    allowed_types=self.allowed_file_types,
                    content=content_buffer
                )
                
                # Log content type verification
                self.auditor.log_event(SecurityEvent(
                    event_type=EventType.FILE_DOWNLOAD,
                    severity=Severity.LOW,
                    message=f"Content type verified: {detected_type}",
                    session_id=session_id,
                    user_id=user_id,
                    resource=validated_url,
                    context={"detected_type": detected_type, "file_size": downloaded_size}
                ))
            
            # Move temporary file to final location
            shutil.move(temp_file.name, validated_path)
            temp_file = None  # Prevent cleanup
            
            # Log successful download
            self.auditor.log_file_operation(
                operation="download",
                file_path=str(validated_path),
                success=True,
                user_id=user_id,
                session_id=session_id
            )
            
            return validated_path
            
        except Exception as e:
            # Log failed download
            self.auditor.log_file_operation(
                operation="download",
                file_path=str(validated_path),
                success=False,
                user_id=user_id,
                session_id=session_id,
                error_message=str(e)
            )
            raise
        finally:
            # Clean up temporary file if it exists
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except OSError:
                    pass
    
    def secure_delete(self, file_path: Union[str, Path],
                     session_id: Optional[str] = None,
                     user_id: Optional[str] = None) -> bool:
        """
        Securely delete a file with validation and logging.
        
        Args:
            file_path: Path to delete
            session_id: Optional session ID for logging
            user_id: Optional user ID for logging
            
        Returns:
            True if successful
            
        Raises:
            SecurityValidationError: If path validation fails
        """
        # Validate path
        validated_path = self.validator.validate_path(
            file_path,
            base_path=self.base_path,
            allow_create=False
        )
        
        if not validated_path.exists():
            self.auditor.log_file_operation(
                operation="delete",
                file_path=str(validated_path),
                success=False,
                user_id=user_id,
                session_id=session_id,
                error_message="File not found"
            )
            return False
        
        try:
            validated_path.unlink()
            
            # Log successful deletion
            self.auditor.log_file_operation(
                operation="delete",
                file_path=str(validated_path),
                success=True,
                user_id=user_id,
                session_id=session_id
            )
            
            return True
            
        except Exception as e:
            self.auditor.log_file_operation(
                operation="delete",
                file_path=str(validated_path),
                success=False,
                user_id=user_id,
                session_id=session_id,
                error_message=str(e)
            )
            raise
    
    def get_file_hash(self, file_path: Union[str, Path], 
                     algorithm: str = 'sha256') -> str:
        """
        Calculate file hash for integrity verification.
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm ('sha256', 'md5', etc.)
            
        Returns:
            Hex digest of file hash
        """
        validated_path = self.validator.validate_path(
            file_path,
            base_path=self.base_path,
            allow_create=False
        )
        
        hash_obj = hashlib.new(algorithm)
        
        with open(validated_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_obj.update(chunk)
        
        return hash_obj.hexdigest()
    
    def verify_file_integrity(self, file_path: Union[str, Path],
                            expected_hash: str,
                            algorithm: str = 'sha256') -> bool:
        """
        Verify file integrity using hash comparison.
        
        Args:
            file_path: Path to file
            expected_hash: Expected hash value
            algorithm: Hash algorithm used
            
        Returns:
            True if hash matches
        """
        actual_hash = self.get_file_hash(file_path, algorithm)
        return actual_hash.lower() == expected_hash.lower()


# Global secure file operations instance
_secure_ops: Optional[SecureFileOperations] = None


def get_secure_ops() -> SecureFileOperations:
    """Get the global secure file operations instance."""
    global _secure_ops
    if _secure_ops is None:
        _secure_ops = SecureFileOperations()
    return _secure_ops


def set_secure_ops(ops: SecureFileOperations) -> None:
    """Set the global secure file operations instance."""
    global _secure_ops
    _secure_ops = ops