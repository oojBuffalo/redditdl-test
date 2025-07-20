"""
Content Handler Plugin Template

This template provides a starting point for creating content handler plugins
for RedditDL. Content handlers process different types of media content
(images, videos, text, galleries, etc.).

To create a new content handler plugin:
1. Copy this template to your plugin file
2. Rename the class and update the plugin information
3. Implement the required methods
4. Test your plugin thoroughly
"""

from typing import Any, Dict, List
from core.plugins.hooks import BaseContentHandler

# Plugin metadata (optional but recommended)
__plugin_info__ = {
    'name': 'example_content_handler',
    'version': '1.0.0',
    'description': 'Example content handler plugin',
    'author': 'Your Name',
    'content_types': ['example_type'],
    'priority': 100
}


class ExampleContentHandler(BaseContentHandler):
    """
    Example content handler plugin.
    
    This handler demonstrates how to process a specific type of content.
    Replace 'example' with your specific content type (e.g., 'gif', 'video', etc.).
    """
    
    # Plugin priority (lower = higher priority)
    priority = 100
    
    def __init__(self):
        """Initialize the content handler."""
        self.supported_types = ['example_type', 'another_type']
        self.config_schema = {
            'quality': {'type': 'string', 'default': 'high', 'choices': ['low', 'medium', 'high']},
            'max_size': {'type': 'integer', 'default': 10485760},  # 10MB
            'output_format': {'type': 'string', 'default': 'original'}
        }
    
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        """
        Check if this handler can process the given content.
        
        Args:
            content_type: Type of content (e.g., 'image', 'video', 'gallery')
            post_data: Complete post metadata dictionary
            
        Returns:
            True if this handler can process the content
        """
        # Check if content type is supported
        if content_type not in self.supported_types:
            return False
        
        # Additional checks based on post data
        url = post_data.get('url', '')
        
        # Example: Check URL patterns
        if 'example.com' in url:
            return True
        
        # Example: Check file extensions
        if url.lower().endswith(('.example', '.ext')):
            return True
        
        # Example: Check domain-specific patterns
        domain = post_data.get('domain', '')
        if domain in ['example.com', 'another-example.com']:
            return True
        
        return False
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process the content and return results.
        
        Args:
            post_data: Complete post metadata dictionary
            config: Handler configuration dictionary
            
        Returns:
            Processing results dictionary with status and output information
        """
        result = {
            'success': False,
            'processed_files': [],
            'errors': [],
            'metadata': {},
            'handler': self.__class__.__name__
        }
        
        try:
            # Extract processing configuration
            quality = config.get('quality', 'high')
            max_size = config.get('max_size', 10485760)
            output_format = config.get('output_format', 'original')
            
            # Get content URL and basic info
            url = post_data.get('url', '')
            post_id = post_data.get('id', 'unknown')
            title = post_data.get('title', 'untitled')
            
            if not url:
                result['errors'].append("No URL found in post data")
                return result
            
            # TODO: Implement your content processing logic here
            # This is where you would:
            # 1. Download the content from the URL
            # 2. Process it according to your handler's purpose
            # 3. Save processed files to the output directory
            # 4. Extract and preserve metadata
            
            # Example processing steps:
            processed_file = await self._download_and_process(url, post_id, title, config)
            
            if processed_file:
                result['processed_files'].append(processed_file)
                result['success'] = True
                result['metadata'] = {
                    'original_url': url,
                    'processed_format': output_format,
                    'quality_setting': quality,
                    'processing_time': 0.0  # Add actual timing
                }
            else:
                result['errors'].append("Failed to process content")
            
        except Exception as e:
            result['errors'].append(f"Processing error: {str(e)}")
        
        return result
    
    async def _download_and_process(self, url: str, post_id: str, title: str, 
                                   config: Dict[str, Any]) -> str:
        """
        Download and process content from URL.
        
        Args:
            url: Content URL to download
            post_id: Reddit post ID
            title: Post title for filename generation
            config: Processing configuration
            
        Returns:
            Path to processed file or None if failed
        """
        # TODO: Implement actual download and processing logic
        # This is a placeholder implementation
        
        try:
            # Example download logic:
            # 1. Create appropriate filename
            # 2. Download content with proper headers
            # 3. Validate downloaded content
            # 4. Apply processing based on configuration
            # 5. Save to output directory
            # 6. Return path to processed file
            
            # Placeholder return
            return f"processed_{post_id}.example"
            
        except Exception as e:
            # Log error and return None
            print(f"Download/processing failed: {e}")
            return None
    
    def get_supported_types(self) -> List[str]:
        """
        Get list of content types this handler supports.
        
        Returns:
            List of supported content type strings
        """
        return self.supported_types.copy()
    
    def get_config_schema(self) -> Dict[str, Any]:
        """
        Get configuration schema for this handler.
        
        Returns:
            Configuration schema dictionary
        """
        return self.config_schema.copy()
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate handler configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate quality setting
        quality = config.get('quality', 'high')
        if quality not in ['low', 'medium', 'high']:
            errors.append(f"Invalid quality setting: {quality}")
        
        # Validate max_size
        max_size = config.get('max_size', 10485760)
        if not isinstance(max_size, int) or max_size <= 0:
            errors.append("max_size must be a positive integer")
        
        # Add more validation as needed
        
        return errors


# Plugin initialization function (optional)
def initialize_plugin():
    """
    Initialize the plugin when it's loaded.
    
    This function is called once when the plugin is loaded.
    Use it for any setup that needs to happen before the plugin is used.
    """
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")
    
    # Example initialization:
    # - Create necessary directories
    # - Load configuration files
    # - Initialize external libraries
    # - Register additional hooks


# Plugin cleanup function (optional)
def cleanup_plugin():
    """
    Clean up plugin resources when it's unloaded.
    
    This function is called when the plugin is being unloaded.
    Use it to clean up any resources, close connections, etc.
    """
    print(f"Cleaning up {__plugin_info__['name']}")
    
    # Example cleanup:
    # - Close file handles
    # - Disconnect from services
    # - Save any pending state
    # - Clean up temporary files