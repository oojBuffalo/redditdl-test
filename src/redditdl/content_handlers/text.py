"""
Text Content Handler

Handles Reddit self-posts (text posts) by saving the content as Markdown files
with YAML frontmatter containing metadata. Preserves formatting and includes
comprehensive post information.
"""

import time
import yaml
from pathlib import Path
from typing import Dict, Any, Set, List

from .base import BaseContentHandler, HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.utils import sanitize_filename
from redditdl.core.templates import FilenameTemplateEngine


class TextContentHandler(BaseContentHandler):
    """
    Content handler for Reddit text posts (self-posts).
    
    Saves text content as Markdown files with YAML frontmatter containing
    all available post metadata. Handles Unicode content properly and
    preserves original formatting.
    """
    
    def __init__(self, priority: int = 60):
        super().__init__("text", priority)
        self._template_engine: FilenameTemplateEngine = None
    
    @property
    def supported_content_types(self) -> Set[str]:
        """Text handler supports text content type."""
        return {'text'}
    
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this is a text post with selftext content
        """
        if content_type != 'text':
            return False
        
        # Must be a self post with text content
        return getattr(post, 'is_self', False) and bool(post.selftext)
    
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Save text post content as Markdown with YAML frontmatter.
        
        Args:
            post: PostMetadata object to process
            output_dir: Directory to save content to
            config: Handler configuration options
            
        Returns:
            HandlerResult with processing details
            
        Raises:
            HandlerError: If processing fails
        """
        start_time = time.time()
        result = HandlerResult(
            handler_name=self.name,
            content_type='text'
        )
        
        try:
            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            filename = self._construct_filename(post, config)
            output_path = output_dir / filename
            
            # Create Markdown content with YAML frontmatter
            markdown_content = self._create_markdown_content(post, config)
            
            self.logger.info(f"Saving text post: {post.id}")
            
            # Write the file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            result.success = True
            result.add_file(output_path)
            result.add_operation("text_save")
            
            # Create JSON sidecar if requested
            if config.get('create_sidecars', False):
                sidecar_path = self._create_json_sidecar(post, output_path)
                if sidecar_path:
                    result.sidecar_created = True
                    result.add_file(sidecar_path)
                    result.add_operation("sidecar_creation")
            
            self.logger.info(f"Successfully saved text post: {output_path.name}")
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Error processing text post {post.id}: {e}")
            raise HandlerError(f"Text processing failed: {e}") from e
        
        result.processing_time = time.time() - start_time
        return result
    
    def _construct_filename(self, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Construct a filename for the text post.
        
        Args:
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            Safe filename string with .md extension
        """
        # Check if custom filename template is provided
        filename_template = config.get('filename_template')
        if filename_template:
            return self._apply_template(filename_template, post, config)
        else:
            # Default filename construction
            title_part = post.title[:50] if post.title else "text_post"
            # Replace colons in date for safer filenames
            safe_date = post.date_iso.replace(':', '_')
            base_filename = f"{safe_date}_{post.id}_{title_part}"
            filename = f"{sanitize_filename(base_filename)}.md"
            return sanitize_filename(filename)
    
    def _apply_template(self, template: str, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Apply Jinja2 template rendering for filename generation.
        
        Args:
            template: Jinja2 template string
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            Rendered filename string with .md extension
        """
        # Initialize template engine if needed
        if self._template_engine is None:
            self._template_engine = FilenameTemplateEngine()
        
        # Prepare template variables
        template_vars = {
            'subreddit': post.subreddit,
            'post_id': post.id,
            'title': post.title,
            'author': post.author,
            'date': post.date_iso,
            'ext': 'md',
            'content_type': 'text',
            'url': post.url,
            'is_video': post.is_video,
        }
        
        # Add any additional variables from post data
        post_dict = post.to_dict()
        for key, value in post_dict.items():
            if key not in template_vars:
                template_vars[key] = value
        
        try:
            # Get max filename length from config
            max_length = config.get('max_filename_length', 200)
            
            # Render the template
            filename = self._template_engine.render(template, template_vars, max_length)
            
            # Ensure .md extension
            if not filename.endswith('.md'):
                # If template didn't include extension or used different one
                path_obj = Path(filename)
                if path_obj.suffix:
                    filename = str(path_obj.with_suffix('.md'))
                else:
                    filename += '.md'
            
            return filename
            
        except Exception as e:
            self.logger.warning(f"Template rendering failed: {e}, falling back to default")
            # Fallback to default filename
            title_part = post.title[:50] if post.title else "text_post"
            safe_date = post.date_iso.replace(':', '_')
            base_filename = f"{safe_date}_{post.id}_{title_part}"
            return f"{sanitize_filename(base_filename)}.md"
    
    def _create_markdown_content(self, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Create Markdown content with YAML frontmatter.
        
        Args:
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            Complete Markdown content string
        """
        # Create metadata dictionary for YAML frontmatter
        metadata = {
            'title': post.title,
            'author': post.author,
            'subreddit': post.subreddit,
            'url': post.url,
            'id': post.id,
            'created_utc': post.created_utc if hasattr(post, 'created_utc') else None,
            'date': post.date_iso,
        }
        
        # Add optional fields if available
        if hasattr(post, 'score'):
            metadata['score'] = post.score
        if hasattr(post, 'num_comments'):
            metadata['num_comments'] = post.num_comments
        if hasattr(post, 'is_nsfw'):
            metadata['nsfw'] = post.is_nsfw
        if hasattr(post, 'spoiler'):
            metadata['spoiler'] = post.spoiler
        if hasattr(post, 'locked'):
            metadata['locked'] = post.locked
        if hasattr(post, 'archived'):
            metadata['archived'] = post.archived
        if hasattr(post, 'edited'):
            metadata['edited'] = post.edited
        
        # Include content preview in metadata if requested
        if config.get('include_preview', True):
            selftext = post.selftext or ""
            preview_text = selftext[:200] if selftext else ""
            if len(selftext) > 200:
                preview_text += "..."
            metadata['preview'] = preview_text
        
        # Create YAML frontmatter
        yaml_front = yaml.dump(
            metadata, 
            default_flow_style=False, 
            allow_unicode=True,
            sort_keys=True
        )
        
        # Escape any content that might conflict with YAML delimiters
        content = post.selftext or ""
        
        # Build the complete Markdown content
        markdown_content = f"""---
{yaml_front}---

# {post.title}

{content}
"""
        
        return markdown_content
    
    def _create_json_sidecar(self, post: PostMetadata, md_path: Path) -> Path:
        """
        Create a JSON sidecar file with complete metadata.
        
        Args:
            post: PostMetadata object
            md_path: Path to the markdown file
            
        Returns:
            Path to the created sidecar file
        """
        import json
        
        sidecar_path = md_path.with_suffix('.json')
        
        try:
            with open(sidecar_path, 'w', encoding='utf-8') as f:
                json.dump(post.to_dict(), f, indent=2, ensure_ascii=False)
            
            return sidecar_path
        except Exception as e:
            self.logger.warning(f"Failed to create JSON sidecar: {e}")
            return None
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate text handler configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate create_sidecars flag
        create_sidecars = config.get('create_sidecars')
        if create_sidecars is not None and not isinstance(create_sidecars, bool):
            errors.append("create_sidecars must be a boolean")
        
        # Validate include_preview flag
        include_preview = config.get('include_preview')
        if include_preview is not None and not isinstance(include_preview, bool):
            errors.append("include_preview must be a boolean")
        
        return errors