"""
Processing Exceptions

Custom exception classes for media processing operations with detailed
error context and recovery suggestions.
"""


class ProcessingError(Exception):
    """Base exception for all media processing errors."""
    pass


class ImageProcessingError(ProcessingError):
    """Exception raised during image processing operations."""
    pass


class VideoProcessingError(ProcessingError):
    """Exception raised during video processing operations."""
    pass


class FFmpegNotFoundError(ProcessingError):
    """Exception raised when FFmpeg is not found or not accessible."""
    
    def __init__(self, message=None):
        if message is None:
            message = (
                "FFmpeg not found. Video processing requires FFmpeg to be installed. "
                "Please install FFmpeg and ensure it's available in your system PATH. "
                "Visit https://ffmpeg.org/download.html for installation instructions."
            )
        super().__init__(message)


class UnsupportedFormatError(ProcessingError):
    """Exception raised when attempting to process an unsupported file format."""
    
    def __init__(self, format_name, supported_formats):
        message = (
            f"Unsupported format: {format_name}. "
            f"Supported formats: {', '.join(supported_formats)}"
        )
        super().__init__(message)
        self.format_name = format_name
        self.supported_formats = supported_formats