"""
Input Validation and Security Hardening

Provides comprehensive input validation, path security, URL validation,
and content verification to protect against security vulnerabilities.
"""

import os
import re
import mimetypes
import ipaddress
from pathlib import Path, PurePath
from typing import Dict, Any, List, Optional, Union, Tuple
from urllib.parse import urlparse, unquote
import logging

from redditdl.core.exceptions import ValidationError, ErrorCode


class SecurityValidationError(ValidationError):
    """Specialized validation error for security-related validation failures."""
    
    def __init__(self, message: str, security_concern: str, **kwargs):
        super().__init__(message, **kwargs)
        self.security_concern = security_concern


class InputValidator:
    """
    Comprehensive input validation system with security hardening.
    
    Provides validation for paths, URLs, filenames, targets, and file types
    with protection against common security vulnerabilities including
    path traversal, injection attacks, and malicious content.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # File type validation mappings
        self.allowed_extensions = {
            'image': {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff', '.ico'},
            'video': {'.mp4', '.avi', '.mkv', '.webm', '.mov', '.wmv', '.flv', '.m4v'},
            'audio': {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a'},
            'document': {'.pdf', '.txt', '.md', '.json', '.xml', '.csv', '.yaml', '.yml'},
            'archive': {'.zip', '.tar', '.gz', '.rar', '.7z'}
        }
        
        # Magic number signatures for file type detection
        self.magic_signatures = {
            # Images
            b'\xff\xd8\xff': 'image/jpeg',
            b'\x89PNG\r\n\x1a\n': 'image/png',
            b'GIF87a': 'image/gif',
            b'GIF89a': 'image/gif',
            b'RIFF': 'image/webp',  # Will be validated further
            b'BM': 'image/bmp',
            
            # Videos  
            b'\x00\x00\x00\x14ftypmp4': 'video/mp4',
            b'\x00\x00\x00\x18ftypmp4': 'video/mp4',
            b'\x1aE\xdf\xa3': 'video/webm',
            
            # Archives
            b'PK\x03\x04': 'application/zip',
            b'PK\x05\x06': 'application/zip',
            b'\x1f\x8b\x08': 'application/gzip',
            b'Rar!\x1a\x07\x00': 'application/rar',
            
            # Documents
            b'%PDF': 'application/pdf',
        }
        
        # Dangerous file patterns
        self.dangerous_patterns = [
            r'\.exe$', r'\.scr$', r'\.bat$', r'\.cmd$', r'\.com$',
            r'\.pif$', r'\.vbs$', r'\.vbe$', r'\.js$', r'\.jse$',
            r'\.wsf$', r'\.wsh$', r'\.msi$', r'\.dll$', r'\.scf$'
        ]
        
        # Allowed URL schemes
        self.allowed_schemes = {'http', 'https'}
        
        # Blocked domains (example list)
        self.blocked_domains = {
            'localhost', '127.0.0.1', '0.0.0.0', '::1',
            # Add other blocked domains as needed
        }
    
    def validate_path(self, path: Union[str, Path], 
                     base_path: Optional[Union[str, Path]] = None,
                     allow_create: bool = True) -> Path:
        """
        Validate a file or directory path for security and accessibility.
        
        Args:
            path: Path to validate
            base_path: Optional base path to restrict access to
            allow_create: Whether to allow creation of non-existent paths
            
        Returns:
            Normalized and validated Path object
            
        Raises:
            SecurityValidationError: If path is unsafe or inaccessible
        """
        if not path:
            raise SecurityValidationError(
                "Path cannot be empty",
                security_concern="empty_path",
                error_code=ErrorCode.VALIDATION_MISSING_FIELD
            )
        
        # Convert to Path object and normalize
        try:
            path_obj = Path(path).resolve()
        except (OSError, ValueError) as e:
            raise SecurityValidationError(
                f"Invalid path format: {e}",
                security_concern="invalid_path_format",
                error_code=ErrorCode.VALIDATION_FORMAT_ERROR
            )
        
        # Check for path traversal attempts
        path_str = str(path_obj)
        dangerous_components = ['..', '.\\', '/./', '\\..\\', '/../']
        
        for component in dangerous_components:
            if component in str(path) or component in path_str:
                raise SecurityValidationError(
                    f"Path traversal attempt detected: {path}",
                    security_concern="path_traversal",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
        
        # Validate against base path if provided
        if base_path:
            try:
                base_path_obj = Path(base_path).resolve()
                if not str(path_obj).startswith(str(base_path_obj)):
                    raise SecurityValidationError(
                        f"Path outside allowed base directory: {path}",
                        security_concern="path_escape",
                        error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                    )
            except (OSError, ValueError) as e:
                raise SecurityValidationError(
                    f"Invalid base path: {e}",
                    security_concern="invalid_base_path",
                    error_code=ErrorCode.VALIDATION_FORMAT_ERROR
                )
        
        # Check for suspicious path patterns
        suspicious_patterns = [
            r'/dev/', r'/proc/', r'/sys/', r'\\Device\\',
            r'\\.\\.', r'aux', r'con', r'prn', r'nul'
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, path_str, re.IGNORECASE):
                raise SecurityValidationError(
                    f"Suspicious path pattern detected: {path}",
                    security_concern="suspicious_path",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
        
        # Check path length (prevent filesystem issues)
        if len(path_str) > 260:  # Windows MAX_PATH limit
            raise SecurityValidationError(
                f"Path too long ({len(path_str)} chars): {path}",
                security_concern="path_too_long",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # Check if path exists or can be created
        if not allow_create and not path_obj.exists():
            raise SecurityValidationError(
                f"Path does not exist: {path}",
                security_concern="path_not_found",
                error_code=ErrorCode.FS_FILE_NOT_FOUND
            )
        
        return path_obj
    
    def sanitize_filename(self, filename: str, max_length: int = 128) -> str:
        """
        Enhanced filename sanitization with additional security checks.
        
        Args:
            filename: Filename to sanitize
            max_length: Maximum allowed filename length
            
        Returns:
            Sanitized filename safe for filesystem use
            
        Raises:
            SecurityValidationError: If filename contains dangerous patterns
        """
        if not filename or not filename.strip():
            return "unnamed_file"
        
        filename = filename.strip()
        
        # Check for dangerous file extensions
        for pattern in self.dangerous_patterns:
            if re.search(pattern, filename, re.IGNORECASE):
                raise SecurityValidationError(
                    f"Dangerous file extension detected: {filename}",
                    security_concern="dangerous_extension",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
        
        # Check for null bytes and control characters
        if '\x00' in filename or any(ord(c) < 32 for c in filename if c not in '\t\n\r'):
            raise SecurityValidationError(
                f"Control characters detected in filename: {filename}",
                security_concern="control_characters",
                error_code=ErrorCode.VALIDATION_FORMAT_ERROR
            )
        
        # Enhanced character replacement for cross-platform safety
        unsafe_chars = r'[/\\?*:|"<>]'
        sanitized = re.sub(unsafe_chars, '_', filename)
        
        # Remove or replace Unicode control characters
        sanitized = ''.join(c if ord(c) >= 32 or c in '\t\n\r' else '_' for c in sanitized)
        
        # Handle reserved names on Windows
        reserved_names = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }
        
        name_without_ext = sanitized.split('.')[0].upper()
        if name_without_ext in reserved_names:
            sanitized = f"safe_{sanitized}"
        
        # Truncate to maximum length while preserving extension
        if len(sanitized) > max_length:
            if '.' in sanitized:
                name, ext = sanitized.rsplit('.', 1)
                available_length = max_length - len(ext) - 1
                if available_length > 0:
                    sanitized = name[:available_length] + '.' + ext
                else:
                    sanitized = sanitized[:max_length]
            else:
                sanitized = sanitized[:max_length]
        
        # Ensure filename doesn't start or end with problematic characters
        sanitized = sanitized.strip('. ')
        
        if not sanitized:
            return "unnamed_file"
        
        return sanitized
    
    def validate_url(self, url: str, allow_private_ips: bool = False) -> str:
        """
        Validate URL for security and format compliance.
        
        Args:
            url: URL to validate
            allow_private_ips: Whether to allow private IP addresses
            
        Returns:
            Validated and normalized URL
            
        Raises:
            SecurityValidationError: If URL is malformed or unsafe
        """
        if not url or not url.strip():
            raise SecurityValidationError(
                "URL cannot be empty",
                security_concern="empty_url",
                error_code=ErrorCode.VALIDATION_MISSING_FIELD
            )
        
        url = url.strip()
        
        # Check URL length
        if len(url) > 2048:  # Common URL length limit
            raise SecurityValidationError(
                f"URL too long ({len(url)} chars)",
                security_concern="url_too_long",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # Parse URL
        try:
            parsed = urlparse(url)
        except Exception as e:
            raise SecurityValidationError(
                f"Invalid URL format: {e}",
                security_concern="invalid_url_format",
                error_code=ErrorCode.VALIDATION_FORMAT_ERROR
            )
        
        # Validate scheme
        if parsed.scheme.lower() not in self.allowed_schemes:
            raise SecurityValidationError(
                f"Unsupported URL scheme: {parsed.scheme}",
                security_concern="unsupported_scheme",
                error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
            )
        
        # Validate hostname
        if not parsed.hostname:
            raise SecurityValidationError(
                "URL must contain a valid hostname",
                security_concern="missing_hostname",
                error_code=ErrorCode.VALIDATION_MISSING_FIELD
            )
        
        hostname = parsed.hostname.lower()
        
        # Check for blocked domains
        if hostname in self.blocked_domains:
            raise SecurityValidationError(
                f"Access to domain blocked: {hostname}",
                security_concern="blocked_domain",
                error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
            )
        
        # Check for IP addresses
        try:
            ip = ipaddress.ip_address(hostname)
            
            if not allow_private_ips and ip.is_private:
                raise SecurityValidationError(
                    f"Private IP address not allowed: {hostname}",
                    security_concern="private_ip",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
            
            if ip.is_loopback:
                raise SecurityValidationError(
                    f"Loopback address not allowed: {hostname}",
                    security_concern="loopback_ip",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
                
        except ValueError:
            # Not an IP address, continue with domain validation
            pass
        
        # Check for suspicious URL patterns
        suspicious_patterns = [
            r'javascript:', r'data:', r'vbscript:', r'file:',
            r'\\x[0-9a-f]{2}', r'%[0-9a-f]{2}%[0-9a-f]{2}'
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                raise SecurityValidationError(
                    f"Suspicious URL pattern detected: {pattern}",
                    security_concern="suspicious_url_pattern",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
        
        return url
    
    def validate_target(self, target: str) -> Tuple[str, str]:
        """
        Validate Reddit target (user, subreddit, or URL).
        
        Args:
            target: Target string to validate
            
        Returns:
            Tuple of (target_type, normalized_target)
            
        Raises:
            SecurityValidationError: If target is invalid or unsafe
        """
        if not target or not target.strip():
            raise SecurityValidationError(
                "Target cannot be empty",
                security_concern="empty_target",
                error_code=ErrorCode.VALIDATION_MISSING_FIELD
            )
        
        target = target.strip()
        
        # Check target length
        if len(target) > 100:  # Reasonable limit for Reddit usernames/subreddits
            raise SecurityValidationError(
                f"Target too long ({len(target)} chars)",
                security_concern="target_too_long",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # URL target
        if target.startswith(('http://', 'https://')):
            validated_url = self.validate_url(target)
            
            # Ensure it's a Reddit URL
            parsed = urlparse(validated_url)
            reddit_domains = {'reddit.com', 'www.reddit.com', 'old.reddit.com', 'new.reddit.com'}
            
            if parsed.hostname.lower() not in reddit_domains:
                raise SecurityValidationError(
                    f"URL must be from Reddit domain: {parsed.hostname}",
                    security_concern="non_reddit_url",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
            
            return ('url', validated_url)
        
        # User target (starts with u/ or /u/)
        if target.startswith(('u/', '/u/')):
            username = target.replace('u/', '').replace('/u/', '').strip('/')
            return ('user', self._validate_reddit_name(username, 'username'))
        
        # Subreddit target (starts with r/ or /r/)
        if target.startswith(('r/', '/r/')):
            subreddit = target.replace('r/', '').replace('/r/', '').strip('/')
            return ('subreddit', self._validate_reddit_name(subreddit, 'subreddit'))
        
        # Plain username or subreddit
        if target.startswith('/'):
            target = target[1:]
        
        # Check if it looks like a subreddit (contains only valid characters)
        if re.match(r'^[a-zA-Z0-9_]+$', target):
            return ('subreddit', self._validate_reddit_name(target, 'subreddit'))
        
        raise SecurityValidationError(
            f"Invalid target format: {target}",
            security_concern="invalid_target_format",
            error_code=ErrorCode.VALIDATION_FORMAT_ERROR
        )
    
    def _validate_reddit_name(self, name: str, name_type: str) -> str:
        """
        Validate Reddit username or subreddit name.
        
        Args:
            name: Name to validate
            name_type: Type of name ('username' or 'subreddit')
            
        Returns:
            Validated name
            
        Raises:
            SecurityValidationError: If name is invalid
        """
        if not name:
            raise SecurityValidationError(
                f"Reddit {name_type} cannot be empty",
                security_concern=f"empty_{name_type}",
                error_code=ErrorCode.VALIDATION_MISSING_FIELD
            )
        
        # Check length (Reddit limits)
        if len(name) > 20:
            raise SecurityValidationError(
                f"Reddit {name_type} too long ({len(name)} chars, max 20)",
                security_concern=f"{name_type}_too_long",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        if len(name) < 1:
            raise SecurityValidationError(
                f"Reddit {name_type} too short",
                security_concern=f"{name_type}_too_short",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # Validate character set
        if not re.match(r'^[a-zA-Z0-9_-]+$', name):
            raise SecurityValidationError(
                f"Reddit {name_type} contains invalid characters: {name}",
                security_concern=f"invalid_{name_type}_chars",
                error_code=ErrorCode.VALIDATION_FORMAT_ERROR
            )
        
        # Check for reserved names
        reserved_names = {'reddit', 'admin', 'null', 'undefined', 'mod', 'moderator'}
        if name.lower() in reserved_names:
            raise SecurityValidationError(
                f"Reserved {name_type} name: {name}",
                security_concern=f"reserved_{name_type}",
                error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
            )
        
        return name
    
    def validate_file_type(self, file_path: Union[str, Path], 
                          allowed_types: Optional[List[str]] = None,
                          content: Optional[bytes] = None) -> str:
        """
        Validate file type based on content and extension.
        
        Args:
            file_path: Path to file for extension checking
            allowed_types: List of allowed content types
            content: Optional file content for magic number checking
            
        Returns:
            Detected MIME type
            
        Raises:
            SecurityValidationError: If file type is not allowed or suspicious
        """
        file_path = Path(file_path)
        
        # Get extension-based MIME type
        mime_type, _ = mimetypes.guess_type(str(file_path))
        
        # Check content if provided
        if content:
            detected_type = self._detect_file_type_by_content(content)
            if detected_type:
                # Verify content matches extension
                if mime_type and not self._types_compatible(mime_type, detected_type):
                    raise SecurityValidationError(
                        f"File extension {file_path.suffix} doesn't match content type {detected_type}",
                        security_concern="extension_mismatch",
                        error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                    )
                mime_type = detected_type
        
        if not mime_type:
            # Try to determine from extension
            ext = file_path.suffix.lower()
            for category, extensions in self.allowed_extensions.items():
                if ext in extensions:
                    mime_type = f"{category}/{ext[1:]}"
                    break
        
        if not mime_type:
            raise SecurityValidationError(
                f"Unable to determine file type for: {file_path}",
                security_concern="unknown_file_type",
                error_code=ErrorCode.VALIDATION_FORMAT_ERROR
            )
        
        # Check against allowed types
        if allowed_types and mime_type not in allowed_types:
            main_type = mime_type.split('/')[0]
            if main_type not in allowed_types:
                raise SecurityValidationError(
                    f"File type not allowed: {mime_type}",
                    security_concern="disallowed_file_type",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
        
        return mime_type
    
    def _detect_file_type_by_content(self, content: bytes) -> Optional[str]:
        """Detect file type by examining file content magic numbers."""
        if not content:
            return None
        
        for signature, mime_type in self.magic_signatures.items():
            if content.startswith(signature):
                # Special handling for WebP
                if signature == b'RIFF' and len(content) > 12:
                    if content[8:12] == b'WEBP':
                        return 'image/webp'
                    else:
                        continue
                return mime_type
        
        return None
    
    def _types_compatible(self, mime_type1: str, mime_type2: str) -> bool:
        """Check if two MIME types are compatible."""
        # Extract main types
        main1 = mime_type1.split('/')[0]
        main2 = mime_type2.split('/')[0]
        
        # Same main type is usually compatible
        if main1 == main2:
            return True
        
        # Special compatibility rules
        compatible_groups = [
            {'image/jpeg', 'image/jpg'},
            {'application/x-gzip', 'application/gzip'},
        ]
        
        for group in compatible_groups:
            if mime_type1 in group and mime_type2 in group:
                return True
        
        return False
    
    def validate_config_value(self, key: str, value: Any, 
                            expected_type: type, 
                            constraints: Optional[Dict[str, Any]] = None) -> Any:
        """
        Validate configuration value with security checks.
        
        Args:
            key: Configuration key name
            value: Value to validate
            expected_type: Expected value type
            constraints: Optional validation constraints
            
        Returns:
            Validated value
            
        Raises:
            SecurityValidationError: If value is invalid or unsafe
        """
        if constraints is None:
            constraints = {}
        
        # Type validation
        if not isinstance(value, expected_type):
            # Try to convert basic types
            try:
                if expected_type in (int, float, str, bool):
                    value = expected_type(value)
                else:
                    raise SecurityValidationError(
                        f"Config value {key} must be {expected_type.__name__}, got {type(value).__name__}",
                        security_concern="type_mismatch",
                        error_code=ErrorCode.VALIDATION_TYPE_MISMATCH
                    )
            except (ValueError, TypeError):
                raise SecurityValidationError(
                    f"Cannot convert config value {key} to {expected_type.__name__}",
                    security_concern="type_conversion_failed",
                    error_code=ErrorCode.VALIDATION_TYPE_MISMATCH
                )
        
        # Range validation
        if 'min_value' in constraints and value < constraints['min_value']:
            raise SecurityValidationError(
                f"Config value {key} below minimum: {value} < {constraints['min_value']}",
                security_concern="value_below_minimum",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        if 'max_value' in constraints and value > constraints['max_value']:
            raise SecurityValidationError(
                f"Config value {key} above maximum: {value} > {constraints['max_value']}",
                security_concern="value_above_maximum",
                error_code=ErrorCode.VALIDATION_RANGE_ERROR
            )
        
        # Choice validation
        if 'choices' in constraints and value not in constraints['choices']:
            raise SecurityValidationError(
                f"Config value {key} not in allowed choices: {value}",
                security_concern="invalid_choice",
                error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
            )
        
        # String-specific validation
        if isinstance(value, str):
            # Length validation
            if 'min_length' in constraints and len(value) < constraints['min_length']:
                raise SecurityValidationError(
                    f"Config string {key} too short: {len(value)} < {constraints['min_length']}",
                    security_concern="string_too_short",
                    error_code=ErrorCode.VALIDATION_RANGE_ERROR
                )
            
            if 'max_length' in constraints and len(value) > constraints['max_length']:
                raise SecurityValidationError(
                    f"Config string {key} too long: {len(value)} > {constraints['max_length']}",
                    security_concern="string_too_long",
                    error_code=ErrorCode.VALIDATION_RANGE_ERROR
                )
            
            # Pattern validation
            if 'pattern' in constraints:
                if not re.match(constraints['pattern'], value):
                    raise SecurityValidationError(
                        f"Config value {key} doesn't match required pattern",
                        security_concern="pattern_mismatch",
                        error_code=ErrorCode.VALIDATION_FORMAT_ERROR
                    )
            
            # Check for suspicious content in string values
            if self._contains_suspicious_content(value):
                raise SecurityValidationError(
                    f"Config value {key} contains suspicious content",
                    security_concern="suspicious_content",
                    error_code=ErrorCode.VALIDATION_CONSTRAINT_VIOLATION
                )
        
        return value
    
    def _contains_suspicious_content(self, value: str) -> bool:
        """Check if string contains potentially malicious content."""
        suspicious_patterns = [
            r'<script', r'javascript:', r'eval\s*\(',
            r'exec\s*\(', r'system\s*\(', r'__import__',
            r'\.\./', r'\\.\\.\\', r'/etc/passwd',
            r'cmd\.exe', r'powershell', r'bash\s*-c'
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        
        return False