#!/usr/bin/env python3
"""
Utility functions for RedditDL application.

This module provides common utility functions used across the RedditDL application,
including filename sanitization, timestamp generation, and metadata handling.
"""

import re
import time
import random
import functools
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Type, Union, Callable, Optional


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename by replacing invalid characters and truncating to safe length.
    
    Replaces filesystem-unsafe characters (/, \\, ?, *, :, <, >, |, ") with underscores
    and truncates the filename to a maximum of 128 characters for cross-platform 
    compatibility.
    
    Args:
        filename: The filename string to sanitize
        
    Returns:
        str: A sanitized filename safe for use across different filesystems
        
    Examples:
        >>> sanitize_filename("my/file:name?.txt")
        'my_file_name_.txt'
        >>> sanitize_filename("a" * 200 + ".txt")
        'aaaa...aaa.txt'  # Truncated to 128 chars
        >>> sanitize_filename("")
        'unnamed_file'
    """
    if not filename or not filename.strip():
        return "unnamed_file"
    
    # Define invalid characters that need to be replaced
    # These characters are problematic across Windows, macOS, and Linux filesystems
    invalid_chars = r'[/\\?*:|"<>]'
    
    # Replace invalid characters with underscores
    sanitized = re.sub(invalid_chars, '_', filename.strip())
    
    # Handle case where filename consists entirely of invalid characters
    if not sanitized or sanitized.replace('_', '').strip() == '':
        return "unnamed_file"
    
    # Truncate to maximum safe length (128 characters)
    # This ensures compatibility across filesystems while preserving file extensions
    if len(sanitized) > 128:
        # Try to preserve file extension if present
        if '.' in sanitized:
            name, ext = sanitized.rsplit('.', 1)
            # Reserve space for extension plus dot
            max_name_length = 128 - len(ext) - 1
            if max_name_length > 0:
                sanitized = name[:max_name_length] + '.' + ext
            else:
                # Extension is too long, just truncate everything
                sanitized = sanitized[:128]
        else:
            sanitized = sanitized[:128]
    
    return sanitized


def get_current_timestamp() -> str:
    """
    Get the current UTC timestamp in ISO 8601 format.
    
    Returns the current time as a UTC timestamp string in ISO 8601 format
    (YYYY-MM-DDTHH:MM:SSZ) without microseconds.
    
    Returns:
        str: Current UTC timestamp in ISO 8601 format with 'Z' suffix
        
    Examples:
        >>> timestamp = get_current_timestamp()
        >>> len(timestamp)
        20
        >>> timestamp.endswith('Z')
        True
        >>> 'T' in timestamp
        True
    """
    # Get current UTC time and format as ISO 8601 without microseconds
    utc_now = datetime.now(timezone.utc)
    # Format: YYYY-MM-DDTHH:MM:SSZ
    return utc_now.strftime('%Y-%m-%dT%H:%M:%SZ')


def merge_metadata(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple metadata dictionaries (placeholder implementation).
    
    This is a placeholder function for future metadata merging functionality.
    Currently performs a simple dictionary merge with dict2 values taking precedence.
    
    Args:
        dict1: First metadata dictionary
        dict2: Second metadata dictionary
        
    Returns:
        Dict[str, Any]: Merged metadata dictionary
        
    Note:
        This is a basic implementation that will be expanded in future tasks
        to handle complex metadata merging scenarios.
        
    Examples:
        >>> merge_metadata({'a': 1}, {'b': 2})
        {'a': 1, 'b': 2}
        >>> merge_metadata({'a': 1}, {'a': 2})
        {'a': 2}
    """
    if not dict1 and not dict2:
        return {}
    if not dict1:
        return dict2.copy()
    if not dict2:
        return dict1.copy()
    
    # Simple merge with dict2 taking precedence
    merged = dict1.copy()
    merged.update(dict2)
    return merged 


def exponential_backoff_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True,
    logger_prefix: str = "[INFO]"
) -> Callable:
    """
    Decorator that implements exponential backoff retry logic with configurable parameters.
    
    This decorator provides a flexible retry mechanism with exponential backoff for any
    function that may encounter transient failures. It supports configurable delays,
    exception types, and includes proper logging with standardized prefixes.
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)
        backoff_factor: Multiplier for exponential backoff (default: 2.0)
        exceptions: Tuple of exception types to retry on (default: (Exception,))
        jitter: Whether to add random jitter to delays (default: True)
        logger_prefix: Prefix for log messages (default: "[INFO]")
        
    Returns:
        Decorated function with retry logic
        
    Examples:
        # Basic usage
        @exponential_backoff_retry(max_retries=3, initial_delay=0.7)
        def api_call():
            pass
            
        # Custom exceptions and logging
        @exponential_backoff_retry(
            max_retries=6, 
            initial_delay=6.1,
            exceptions=(ConnectionError, TimeoutError),
            logger_prefix="[WARN]"
        )
        def non_api_call():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed, log error and re-raise
                        print(f"[ERROR] {func.__name__} failed after {max_retries + 1} attempts: {e}")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = initial_delay * (backoff_factor ** attempt)
                    
                    # Add jitter to prevent thundering herd
                    if jitter:
                        delay += random.uniform(0, min(1.0, delay * 0.1))
                    
                    # Log retry attempt
                    print(f"{logger_prefix} {func.__name__} attempt {attempt + 1} failed ({e}), retrying in {delay:.1f}s...")
                    
                    time.sleep(delay)
                    
            # This should not be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def api_retry(max_retries: int = 3, initial_delay: float = 0.7) -> Callable:
    """
    Convenience decorator for API calls with standard retry pattern.
    
    Implements the specified API retry pattern: 0.7s -> 1.4s -> 2.8s
    for network and authentication errors.
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds (default: 0.7)
        
    Returns:
        Decorated function with API retry logic
    """
    import requests
    import prawcore
    
    api_exceptions = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.RequestException,
        prawcore.exceptions.RequestException,
        prawcore.exceptions.ServerError,
        prawcore.exceptions.ResponseException,
    )
    
    return exponential_backoff_retry(
        max_retries=max_retries,
        initial_delay=initial_delay,
        backoff_factor=2.0,
        exceptions=api_exceptions,
        logger_prefix="[INFO]"
    )


def non_api_retry(max_retries: int = 3, initial_delay: float = 6.1) -> Callable:
    """
    Convenience decorator for non-API calls with standard retry pattern.
    
    Implements the specified non-API retry pattern: 6.1s -> 12.2s -> 24.4s
    for all exceptions (since non-API calls are more prone to various failures).
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds (default: 6.1)
        
    Returns:
        Decorated function with non-API retry logic
    """
    # For non-API calls, retry on all exceptions since they can fail for various reasons
    return exponential_backoff_retry(
        max_retries=max_retries,
        initial_delay=initial_delay,
        backoff_factor=2.0,
        exceptions=(Exception,),
        logger_prefix="[INFO]"
    )


def auth_retry(max_retries: int = 2, initial_delay: float = 2.0) -> Callable:
    """
    Convenience decorator for authentication-related retries.
    
    Implements retry logic specifically for authentication failures with
    appropriate delays and exception handling.
    
    Args:
        max_retries: Maximum number of retry attempts (default: 2)
        initial_delay: Initial delay in seconds (default: 2.0)
        
    Returns:
        Decorated function with authentication retry logic
    """
    import prawcore
    
    auth_exceptions = (
        prawcore.exceptions.OAuthException,
        prawcore.exceptions.InvalidToken,
        prawcore.exceptions.Forbidden,
    )
    
    return exponential_backoff_retry(
        max_retries=max_retries,
        initial_delay=initial_delay,
        backoff_factor=2.0,
        exceptions=auth_exceptions,
        logger_prefix="[WARN]"
    ) 