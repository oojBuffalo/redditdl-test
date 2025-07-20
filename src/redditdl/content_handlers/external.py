"""
External Links Content Handler

Handles Reddit posts that link to external websites by creating
bookmark files, metadata records, and optional link previews.
Tracks external domains and content types.
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, Set, List
from urllib.parse import urlparse

from .base import BaseContentHandler, HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.utils import sanitize_filename
from redditdl.core.templates import FilenameTemplateEngine


class ExternalLinksHandler(BaseContentHandler):
    """
    Content handler for Reddit posts linking to external websites.
    
    Creates bookmark files, extracts domain information, and tracks
    external links with metadata. Optionally attempts to fetch
    link previews and analyze content types.
    """
    
    def __init__(self, priority: int = 80):
        super().__init__("external", priority)
        self._template_engine: FilenameTemplateEngine = None
    
    @property
    def supported_content_types(self) -> Set[str]:
        """External handler supports external content type."""
        return {'external'}
    
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this is an external link post
        """
        if content_type != 'external':
            return False
        
        # Must have a URL that's not a self post
        return bool(post.url) and not getattr(post, 'is_self', False)
    
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Process external link and create bookmark/metadata files.
        
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
            content_type='external'
        )
        
        try:
            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Extract and analyze URL
            url = post.url
            if not url:
                raise HandlerError("No URL found in external link post")
            
            # Analyze the URL
            url_info = self._analyze_url(url)
            
            # Generate filenames
            metadata_filename = self._construct_metadata_filename(post, config)
            metadata_path = output_dir / metadata_filename
            
            # Create external link metadata
            link_data = self._create_link_metadata(post, url_info, config)
            
            self.logger.info(f"Processing external link: {url}")
            
            # Save link metadata
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(link_data, f, indent=2, ensure_ascii=False)
            
            result.success = True
            result.add_file(metadata_path)
            result.add_operation("link_metadata")
            
            # Create bookmark file if requested
            if config.get('create_bookmarks', True):
                bookmark_path = self._create_bookmark_file(link_data, output_dir, config)
                if bookmark_path:
                    result.add_file(bookmark_path)
                    result.add_operation("bookmark_creation")
            
            # Create domain tracking if requested
            if config.get('track_domains', True):
                domain_path = self._update_domain_tracking(url_info, output_dir, config)
                if domain_path:
                    result.add_file(domain_path)
                    result.add_operation("domain_tracking")
            
            # Create summary if requested
            if config.get('create_summary', True):
                summary_path = self._create_link_summary(link_data, metadata_path, config)
                if summary_path:
                    result.add_file(summary_path)
                    result.add_operation("link_summary")
            
            self.logger.info(f"Successfully processed external link: {metadata_path.name}")
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Error processing external link post {post.id}: {e}")
            raise HandlerError(f"External link processing failed: {e}") from e
        
        result.processing_time = time.time() - start_time
        return result
    
    def _construct_metadata_filename(self, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Construct a filename for link metadata.
        
        Args:
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            Safe filename string with .json extension
        """
        # Check if custom filename template is provided
        filename_template = config.get('filename_template')
        if filename_template:
            return self._apply_template(filename_template, post, config)
        else:
            # Default filename construction
            title_part = post.title[:50] if post.title else "external_link"
            base_filename = f"{post.date_iso}_{post.id}_{title_part}_link"
            filename = f"{sanitize_filename(base_filename)}.json"
            return sanitize_filename(filename)
    
    def _apply_template(self, template: str, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Apply Jinja2 template rendering for filename generation.
        
        Args:
            template: Jinja2 template string
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            Rendered filename string with .json extension
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
            'ext': 'json',
            'content_type': 'external',
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
            
            # Ensure .json extension
            if not filename.endswith('.json'):
                path_obj = Path(filename)
                if path_obj.suffix:
                    filename = str(path_obj.with_suffix('.json'))
                else:
                    filename += '.json'
            
            return filename
            
        except Exception as e:
            self.logger.warning(f"Template rendering failed: {e}, falling back to default")
            # Fallback to default filename
            title_part = post.title[:50] if post.title else "external_link"
            base_filename = f"{post.date_iso}_{post.id}_{title_part}_link"
            return f"{sanitize_filename(base_filename)}.json"
    
    def _analyze_url(self, url: str) -> Dict[str, Any]:
        """
        Analyze an external URL and extract information.
        
        Args:
            url: URL to analyze
            
        Returns:
            Dictionary with URL analysis results
        """
        try:
            parsed = urlparse(url)
            
            return {
                'original_url': url,
                'scheme': parsed.scheme,
                'domain': parsed.netloc,
                'path': parsed.path,
                'query': parsed.query,
                'fragment': parsed.fragment,
                'is_secure': parsed.scheme == 'https',
                'top_level_domain': self._extract_tld(parsed.netloc),
                'subdomain': self._extract_subdomain(parsed.netloc),
                'has_query_params': bool(parsed.query),
                'has_fragment': bool(parsed.fragment),
                'url_length': len(url),
                'estimated_content_type': self._guess_content_type(url, parsed)
            }
        except Exception as e:
            self.logger.warning(f"Failed to analyze URL {url}: {e}")
            return {
                'original_url': url,
                'domain': 'unknown',
                'analysis_error': str(e)
            }
    
    def _extract_tld(self, domain: str) -> str:
        """Extract top-level domain from domain name."""
        if not domain:
            return 'unknown'
        parts = domain.split('.')
        return parts[-1] if parts else 'unknown'
    
    def _extract_subdomain(self, domain: str) -> str:
        """Extract subdomain from domain name."""
        if not domain:
            return ''
        parts = domain.split('.')
        if len(parts) > 2:
            return '.'.join(parts[:-2])
        return ''
    
    def _guess_content_type(self, url: str, parsed_url) -> str:
        """
        Guess content type from URL patterns.
        
        Args:
            url: Original URL
            parsed_url: Parsed URL object
            
        Returns:
            Estimated content type
        """
        domain = parsed_url.netloc.lower()
        path = parsed_url.path.lower()
        
        # Common patterns
        if any(video_site in domain for video_site in ['youtube.com', 'youtu.be', 'vimeo.com', 'twitch.tv']):
            return 'video'
        elif any(image_site in domain for image_site in ['instagram.com', 'flickr.com', 'imgur.com']):
            return 'image'
        elif any(social_site in domain for social_site in ['twitter.com', 'facebook.com', 'linkedin.com']):
            return 'social_media'
        elif any(news_site in domain for news_site in ['bbc.com', 'cnn.com', 'reuters.com', 'nytimes.com']):
            return 'news'
        elif 'github.com' in domain:
            return 'code_repository'
        elif 'wikipedia.org' in domain:
            return 'wiki'
        elif any(ext in path for ext in ['.pdf', '.doc', '.docx']):
            return 'document'
        elif any(ext in path for ext in ['.jpg', '.png', '.gif', '.jpeg']):
            return 'image'
        elif any(ext in path for ext in ['.mp4', '.avi', '.mov']):
            return 'video'
        else:
            return 'webpage'
    
    def _create_link_metadata(self, post: PostMetadata, url_info: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create comprehensive link metadata.
        
        Args:
            post: PostMetadata object
            url_info: URL analysis results
            config: Configuration options
            
        Returns:
            Link metadata dictionary
        """
        link_data = {
            'post_metadata': post.to_dict(),
            'url_analysis': url_info,
            'link_info': {
                'extracted_at': time.time(),
                'handler': self.name,
                'post_id': post.id,
                'subreddit': post.subreddit,
                'author': post.author,
                'title': post.title,
                'date': post.date_iso
            }
        }
        
        # Add Reddit-specific metadata if available
        if hasattr(post, 'score'):
            link_data['link_info']['score'] = post.score
        if hasattr(post, 'num_comments'):
            link_data['link_info']['comments'] = post.num_comments
        if hasattr(post, 'domain'):
            link_data['link_info']['reddit_domain'] = post.domain
        
        return link_data
    
    def _create_bookmark_file(self, link_data: Dict[str, Any], output_dir: Path, config: Dict[str, Any]) -> Path:
        """
        Create a browser-compatible bookmark file.
        
        Args:
            link_data: Link metadata
            output_dir: Output directory
            config: Configuration options
            
        Returns:
            Path to the bookmark file
        """
        bookmark_path = output_dir / "bookmarks.html"
        
        try:
            post_data = link_data['post_metadata']
            url_info = link_data['url_analysis']
            
            # Create HTML bookmark entry
            bookmark_entry = f"""<DT><A HREF="{url_info['original_url']}" ADD_DATE="{int(time.time())}">{post_data['title']}</A>
<DD>From r/{post_data['subreddit']} by u/{post_data['author']} - {post_data['date']}
"""
            
            # Check if bookmark file exists and append, or create new
            if bookmark_path.exists():
                # Read existing content and append
                with open(bookmark_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Insert before closing tags
                if '</DL>' in content:
                    content = content.replace('</DL>', bookmark_entry + '</DL>')
                else:
                    content += bookmark_entry
            else:
                # Create new bookmark file
                content = f"""<!DOCTYPE NETSCAPE-Bookmark-file-1>
<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">
<TITLE>Reddit External Links</TITLE>
<H1>Reddit External Links</H1>
<DL><p>
{bookmark_entry}</DL><p>
"""
            
            with open(bookmark_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return bookmark_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create bookmark file: {e}")
            return None
    
    def _update_domain_tracking(self, url_info: Dict[str, Any], output_dir: Path, config: Dict[str, Any]) -> Path:
        """
        Update domain tracking statistics.
        
        Args:
            url_info: URL analysis results
            output_dir: Output directory
            config: Configuration options
            
        Returns:
            Path to the domain tracking file
        """
        tracking_path = output_dir / "domain_stats.json"
        
        try:
            domain = url_info.get('domain', 'unknown')
            content_type = url_info.get('estimated_content_type', 'unknown')
            
            # Load existing tracking data
            tracking_data = {}
            if tracking_path.exists():
                with open(tracking_path, 'r', encoding='utf-8') as f:
                    tracking_data = json.load(f)
            
            # Initialize tracking structure if needed
            if 'domains' not in tracking_data:
                tracking_data['domains'] = {}
            if 'content_types' not in tracking_data:
                tracking_data['content_types'] = {}
            if 'last_updated' not in tracking_data:
                tracking_data['last_updated'] = time.time()
            
            # Update domain stats
            if domain not in tracking_data['domains']:
                tracking_data['domains'][domain] = {
                    'count': 0,
                    'first_seen': time.time(),
                    'content_types': {}
                }
            
            tracking_data['domains'][domain]['count'] += 1
            tracking_data['domains'][domain]['last_seen'] = time.time()
            
            # Update content type stats for domain
            domain_data = tracking_data['domains'][domain]
            if content_type not in domain_data['content_types']:
                domain_data['content_types'][content_type] = 0
            domain_data['content_types'][content_type] += 1
            
            # Update global content type stats
            if content_type not in tracking_data['content_types']:
                tracking_data['content_types'][content_type] = 0
            tracking_data['content_types'][content_type] += 1
            
            # Update timestamps
            tracking_data['last_updated'] = time.time()
            
            # Save updated tracking data
            with open(tracking_path, 'w', encoding='utf-8') as f:
                json.dump(tracking_data, f, indent=2, ensure_ascii=False)
            
            return tracking_path
            
        except Exception as e:
            self.logger.warning(f"Failed to update domain tracking: {e}")
            return None
    
    def _create_link_summary(self, link_data: Dict[str, Any], metadata_path: Path, config: Dict[str, Any]) -> Path:
        """
        Create a Markdown summary of the external link.
        
        Args:
            link_data: Link metadata
            metadata_path: Path to the metadata file
            config: Configuration options
            
        Returns:
            Path to the summary file
        """
        summary_path = metadata_path.with_suffix('.md')
        
        try:
            post_data = link_data['post_metadata']
            url_info = link_data['url_analysis']
            
            # Create Markdown content
            content = f"""# External Link: {post_data['title']}

**URL:** {url_info['original_url']}  
**Domain:** {url_info['domain']}  
**Content Type:** {url_info.get('estimated_content_type', 'unknown')}  
**Subreddit:** r/{post_data['subreddit']}  
**Author:** u/{post_data['author']}  
**Posted:** {post_data['date']}

## URL Analysis

- **Scheme:** {url_info.get('scheme', 'unknown')}
- **Secure:** {url_info.get('is_secure', False)}
- **Top Level Domain:** {url_info.get('top_level_domain', 'unknown')}
- **Has Query Parameters:** {url_info.get('has_query_params', False)}

## Reddit Post Information

- **Post ID:** {post_data['id']}
- **Reddit URL:** {post_data['url']}

---
*External link data extracted from Reddit post {post_data['id']}*
"""
            
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return summary_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create link summary: {e}")
            return None
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate external links handler configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate create_bookmarks flag
        create_bookmarks = config.get('create_bookmarks')
        if create_bookmarks is not None and not isinstance(create_bookmarks, bool):
            errors.append("create_bookmarks must be a boolean")
        
        # Validate track_domains flag
        track_domains = config.get('track_domains')
        if track_domains is not None and not isinstance(track_domains, bool):
            errors.append("track_domains must be a boolean")
        
        # Validate create_summary flag
        create_summary = config.get('create_summary')
        if create_summary is not None and not isinstance(create_summary, bool):
            errors.append("create_summary must be a boolean")
        
        return errors