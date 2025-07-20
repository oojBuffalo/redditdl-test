"""
Simple Text Handler Plugin

A simple example plugin that handles text-only posts by saving
them as Markdown files with YAML frontmatter metadata.
"""

import yaml
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List
from core.plugins.hooks import BaseContentHandler

__plugin_info__ = {
    'name': 'simple_text_handler',
    'version': '1.0.0',
    'description': 'Handles text posts by saving as Markdown with YAML frontmatter',
    'author': 'RedditDL Team',
    'content_types': ['text', 'selftext'],
    'priority': 200
}


class SimpleTextHandler(BaseContentHandler):
    """
    Simple text content handler that saves text posts as Markdown files.
    
    This handler processes self-text posts and saves them as .md files
    with YAML frontmatter containing the post metadata.
    """
    
    priority = 200  # Lower priority than media handlers
    
    def __init__(self):
        self.supported_types = ['text', 'selftext']
        self.config_schema = {
            'include_comments': {
                'type': 'boolean',
                'default': False,
                'description': 'Include post comments in the output'
            },
            'max_title_length': {
                'type': 'integer',
                'default': 50,
                'description': 'Maximum title length for filename'
            },
            'date_format': {
                'type': 'string',
                'default': '%Y-%m-%d_%H-%M-%S',
                'description': 'Date format for filename'
            }
        }
    
    def can_handle(self, content_type: str, post_data: Dict[str, Any]) -> bool:
        """Check if this handler can process text content."""
        if content_type not in self.supported_types:
            return False
        
        # Check if post has selftext content
        selftext = post_data.get('selftext', '').strip()
        if not selftext:
            return False
        
        # Don't handle if it's primarily a link post
        url = post_data.get('url', '')
        if url and not url.startswith(('https://www.reddit.com/', 'https://old.reddit.com/')):
            return False
        
        return True
    
    async def process(self, post_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Process text content and save as Markdown file."""
        result = {
            'success': False,
            'processed_files': [],
            'errors': [],
            'metadata': {},
            'handler': self.__class__.__name__
        }
        
        try:
            # Extract configuration
            max_title_length = config.get('max_title_length', 50)
            date_format = config.get('date_format', '%Y-%m-%d_%H-%M-%S')
            include_comments = config.get('include_comments', False)
            
            # Generate filename
            filename = self._generate_filename(post_data, max_title_length, date_format)
            output_path = Path(config.get('output_dir', 'downloads')) / filename
            
            # Create Markdown content
            content = self._create_markdown_content(post_data, include_comments)
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            result['processed_files'].append(str(output_path))
            result['success'] = True
            result['metadata'] = {
                'filename': filename,
                'content_length': len(content),
                'post_type': 'text',
                'has_comments': include_comments
            }
            
        except Exception as e:
            result['errors'].append(f"Text processing error: {str(e)}")
        
        return result
    
    def _generate_filename(self, post_data: Dict[str, Any], max_title_length: int, 
                          date_format: str) -> str:
        """Generate a safe filename for the text file."""
        post_id = post_data.get('id', 'unknown')
        title = post_data.get('title', 'untitled')
        
        # Sanitize title
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title.replace(' ', '_')
        
        # Truncate if necessary
        if len(safe_title) > max_title_length:
            safe_title = safe_title[:max_title_length]
        
        # Add timestamp if available
        created_utc = post_data.get('created_utc')
        if created_utc:
            date_str = datetime.fromtimestamp(created_utc).strftime(date_format)
            return f"{date_str}_{post_id}_{safe_title}.md"
        else:
            return f"{post_id}_{safe_title}.md"
    
    def _create_markdown_content(self, post_data: Dict[str, Any], include_comments: bool) -> str:
        """Create Markdown content with YAML frontmatter."""
        # Prepare metadata for frontmatter
        metadata = {
            'title': post_data.get('title', ''),
            'author': post_data.get('author', ''),
            'subreddit': post_data.get('subreddit', ''),
            'post_id': post_data.get('id', ''),
            'url': post_data.get('url', ''),
            'score': post_data.get('score', 0),
            'num_comments': post_data.get('num_comments', 0),
            'is_nsfw': post_data.get('is_nsfw', False),
            'created_utc': post_data.get('created_utc', 0),
            'permalink': post_data.get('permalink', ''),
            'export_date': datetime.now().isoformat()
        }
        
        # Remove None values
        metadata = {k: v for k, v in metadata.items() if v is not None}
        
        # Create YAML frontmatter
        frontmatter = yaml.dump(metadata, default_flow_style=False, allow_unicode=True)
        
        # Get selftext content
        selftext = post_data.get('selftext', '')
        
        # Build complete content
        content_parts = [
            '---',
            frontmatter.strip(),
            '---',
            '',
            f"# {post_data.get('title', 'Untitled')}",
            '',
            selftext,
        ]
        
        # Add comments if requested
        if include_comments:
            comments = post_data.get('comments', [])
            if comments:
                content_parts.extend([
                    '',
                    '---',
                    '',
                    '## Comments',
                    ''
                ])
                
                for i, comment in enumerate(comments, 1):
                    content_parts.extend([
                        f"### Comment {i} by {comment.get('author', 'unknown')}",
                        f"Score: {comment.get('score', 0)}",
                        '',
                        comment.get('body', ''),
                        ''
                    ])
        
        return '\n'.join(content_parts)
    
    def get_supported_types(self) -> List[str]:
        """Get list of supported content types."""
        return self.supported_types.copy()


def initialize_plugin():
    """Initialize the simple text handler plugin."""
    print(f"Initializing {__plugin_info__['name']} v{__plugin_info__['version']}")
    
    # Ensure yaml is available
    try:
        import yaml
    except ImportError:
        print("Warning: PyYAML not available, YAML frontmatter may be limited")


def cleanup_plugin():
    """Clean up the simple text handler plugin."""
    print(f"Cleaning up {__plugin_info__['name']}")