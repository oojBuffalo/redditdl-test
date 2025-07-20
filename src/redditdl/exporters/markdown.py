"""
Markdown Exporter

Template-based Markdown exporter for human-readable reports and documentation
of Reddit post data with customizable formatting and structure.
"""

import json
import time
import gzip
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime
from textwrap import wrap, dedent

from .base import BaseExporter, ExportResult, FormatInfo


class MarkdownExporter(BaseExporter):
    """
    Template-based Markdown exporter for Reddit post data.
    
    Features:
    - Customizable templates for different report styles
    - Automatic table of contents generation
    - Post categorization and grouping
    - Statistics and summary sections
    - Image gallery support
    - Link validation and formatting
    - Export metadata inclusion
    - Multiple output styles (report, documentation, blog)
    """
    
    def _create_format_info(self) -> FormatInfo:
        """Create format information for Markdown export."""
        return FormatInfo(
            name="markdown",
            extension=".md",
            description="Markdown format with template-based rendering",
            mime_type="text/markdown",
            supports_compression=True,
            supports_streaming=False,
            supports_incremental=False,
            schema_required=False
        )
    
    def _create_config_schema(self) -> Dict[str, Any]:
        """Create configuration schema for Markdown export."""
        return {
            'template': {
                'type': 'string',
                'default': 'report',
                'choices': ['report', 'documentation', 'blog', 'summary', 'custom'],
                'description': 'Output template style'
            },
            'title': {
                'type': 'string',
                'default': 'Reddit Data Export',
                'description': 'Document title'
            },
            'include_toc': {
                'type': 'boolean',
                'default': True,
                'description': 'Include table of contents'
            },
            'include_statistics': {
                'type': 'boolean',
                'default': True,
                'description': 'Include statistics summary'
            },
            'include_metadata': {
                'type': 'boolean',
                'default': True,
                'description': 'Include export metadata'
            },
            'group_by': {
                'type': 'string',
                'default': 'subreddit',
                'choices': ['none', 'subreddit', 'author', 'post_type', 'date'],
                'description': 'Group posts by field'
            },
            'sort_by': {
                'type': 'string',
                'default': 'score',
                'choices': ['score', 'date', 'title', 'comments', 'author'],
                'description': 'Sort posts by field'
            },
            'sort_order': {
                'type': 'string',
                'default': 'desc',
                'choices': ['asc', 'desc'],
                'description': 'Sort order'
            },
            'max_posts': {
                'type': 'integer',
                'default': 0,
                'minimum': 0,
                'description': 'Maximum posts to include (0 = all)'
            },
            'max_content_length': {
                'type': 'integer',
                'default': 500,
                'minimum': 100,
                'maximum': 5000,
                'description': 'Maximum content length per post'
            },
            'include_selftext': {
                'type': 'boolean',
                'default': True,
                'description': 'Include post self-text content'
            },
            'include_images': {
                'type': 'boolean',
                'default': True,
                'description': 'Include image links and galleries'
            },
            'include_links': {
                'type': 'boolean',
                'default': True,
                'description': 'Include external links'
            },
            'link_style': {
                'type': 'string',
                'default': 'inline',
                'choices': ['inline', 'reference', 'footnote'],
                'description': 'Link formatting style'
            },
            'date_format': {
                'type': 'string',
                'default': '%Y-%m-%d %H:%M',
                'description': 'Date format for timestamps'
            },
            'compress': {
                'type': 'boolean',
                'default': False,
                'description': 'Compress output with gzip'
            },
            'custom_template_path': {
                'type': 'string',
                'default': '',
                'description': 'Path to custom template file'
            },
            'encoding': {
                'type': 'string',
                'default': 'utf-8',
                'choices': ['utf-8', 'utf-16', 'latin-1'],
                'description': 'Text encoding for output'
            }
        }
    
    def export(self, data: Dict[str, Any], output_path: str, config: Dict[str, Any]) -> ExportResult:
        """Export data to Markdown format."""
        start_time = time.time()
        result = ExportResult(format_name="markdown")
        
        try:
            # Validate input data
            validation_errors = self.validate_data(data)
            if validation_errors:
                for error in validation_errors:
                    result.add_error(error)
                return result
            
            # Prepare output path
            output_file = self.prepare_output_path(output_path, config)
            
            # Extract and process posts
            posts = data.get('posts', [])
            if not posts:
                result.add_warning("No posts to export")
                # Create empty document
                self._write_empty_document(output_file, config)
                result.output_path = str(output_file)
                return result
            
            # Process posts according to configuration
            processed_posts = self._process_posts(posts, config)
            
            # Generate document sections
            document_sections = self._generate_document(processed_posts, data, config)
            
            # Write markdown file
            self._write_markdown_file(document_sections, output_file, config, result)
            
            result.output_path = str(output_file)
            result.records_exported = len(processed_posts)
            result.execution_time = time.time() - start_time
            
            # Get file size
            if output_file.exists():
                result.file_size = output_file.stat().st_size
            
            self.logger.info(f"Markdown export completed: {result.records_exported} posts to {output_file}")
            
        except Exception as e:
            result.add_error(f"Markdown export failed: {e}")
            self.logger.error(f"Markdown export error: {e}")
        
        return result
    
    def _process_posts(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process and filter posts according to configuration."""
        processed = []
        
        for post in posts:
            processed_post = post.copy()
            
            # Truncate content if needed
            max_length = config.get('max_content_length', 500)
            selftext = processed_post.get('selftext', '')
            if selftext and len(selftext) > max_length:
                processed_post['selftext'] = selftext[:max_length] + "..."
            
            processed.append(processed_post)
        
        # Sort posts
        sort_by = config.get('sort_by', 'score')
        sort_order = config.get('sort_order', 'desc')
        reverse = sort_order == 'desc'
        
        try:
            if sort_by == 'date':
                processed.sort(key=lambda p: p.get('created_utc', 0), reverse=reverse)
            elif sort_by == 'score':
                processed.sort(key=lambda p: p.get('score', 0), reverse=reverse)
            elif sort_by == 'comments':
                processed.sort(key=lambda p: p.get('num_comments', 0), reverse=reverse)
            elif sort_by == 'title':
                processed.sort(key=lambda p: p.get('title', '').lower(), reverse=reverse)
            elif sort_by == 'author':
                processed.sort(key=lambda p: p.get('author', '').lower(), reverse=reverse)
        except Exception as e:
            self.logger.warning(f"Failed to sort posts: {e}")
        
        # Limit posts if requested
        max_posts = config.get('max_posts', 0)
        if max_posts > 0:
            processed = processed[:max_posts]
        
        return processed
    
    def _generate_document(self, posts: List[Dict[str, Any]], 
                          data: Dict[str, Any], config: Dict[str, Any]) -> List[str]:
        """Generate document sections based on template."""
        sections = []
        
        # Document header
        sections.append(self._generate_header(config))
        
        # Table of contents (placeholder - will be updated after content generation)
        if config.get('include_toc', True):
            sections.append("<!-- TOC_PLACEHOLDER -->")
        
        # Statistics section
        if config.get('include_statistics', True):
            sections.append(self._generate_statistics(posts, config))
        
        # Posts content
        group_by = config.get('group_by', 'subreddit')
        if group_by == 'none':
            sections.append(self._generate_posts_section(posts, config))
        else:
            sections.extend(self._generate_grouped_posts(posts, group_by, config))
        
        # Metadata section
        if config.get('include_metadata', True):
            sections.append(self._generate_metadata_section(data, config))
        
        # Generate and insert table of contents
        if config.get('include_toc', True):
            toc = self._generate_toc(sections)
            for i, section in enumerate(sections):
                if "<!-- TOC_PLACEHOLDER -->" in section:
                    sections[i] = toc
                    break
        
        return sections
    
    def _generate_header(self, config: Dict[str, Any]) -> str:
        """Generate document header."""
        title = config.get('title', 'Reddit Data Export')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        header = f"""# {title}
        
*Generated on {timestamp}*

---
"""
        return dedent(header).strip()
    
    def _generate_statistics(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> str:
        """Generate statistics summary."""
        if not posts:
            return "## Statistics\n\nNo posts to analyze.\n"
        
        # Calculate statistics
        total_posts = len(posts)
        total_score = sum(post.get('score', 0) for post in posts)
        total_comments = sum(post.get('num_comments', 0) for post in posts)
        
        subreddits = set(post.get('subreddit', '') for post in posts if post.get('subreddit'))
        authors = set(post.get('author', '') for post in posts if post.get('author'))
        
        post_types = {}
        for post in posts:
            post_type = post.get('post_type', 'unknown')
            post_types[post_type] = post_types.get(post_type, 0) + 1
        
        nsfw_count = sum(1 for post in posts if post.get('is_nsfw', False))
        
        # Format statistics
        stats = f"""## Statistics

| Metric | Value |
|--------|--------|
| Total Posts | {total_posts:,} |
| Total Score | {total_score:,} |
| Total Comments | {total_comments:,} |
| Unique Subreddits | {len(subreddits):,} |
| Unique Authors | {len(authors):,} |
| NSFW Posts | {nsfw_count:,} ({nsfw_count/total_posts*100:.1f}%) |

### Post Types

| Type | Count | Percentage |
|------|-------|-----------|"""
        
        for post_type, count in sorted(post_types.items(), key=lambda x: x[1], reverse=True):
            percentage = count / total_posts * 100
            stats += f"\n| {post_type.title()} | {count:,} | {percentage:.1f}% |"
        
        # Top subreddits
        if len(subreddits) > 1:
            subreddit_counts = {}
            for post in posts:
                sub = post.get('subreddit', '')
                if sub:
                    subreddit_counts[sub] = subreddit_counts.get(sub, 0) + 1
            
            stats += "\n\n### Top Subreddits\n\n| Subreddit | Posts |\n|-----------|-------|\n"
            for sub, count in sorted(subreddit_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                stats += f"| r/{sub} | {count:,} |\n"
        
        stats += "\n"
        return stats
    
    def _generate_grouped_posts(self, posts: List[Dict[str, Any]], 
                               group_by: str, config: Dict[str, Any]) -> List[str]:
        """Generate posts grouped by specified field."""
        groups = {}
        
        for post in posts:
            if group_by == 'subreddit':
                key = post.get('subreddit', 'Unknown')
            elif group_by == 'author':
                key = post.get('author', 'Unknown')
            elif group_by == 'post_type':
                key = post.get('post_type', 'unknown').title()
            elif group_by == 'date':
                created_utc = post.get('created_utc', 0)
                if created_utc:
                    try:
                        dt = datetime.fromtimestamp(created_utc)
                        key = dt.strftime('%Y-%m-%d')
                    except (ValueError, OSError):
                        key = 'Unknown Date'
                else:
                    key = 'Unknown Date'
            else:
                key = 'Unknown'
            
            if key not in groups:
                groups[key] = []
            groups[key].append(post)
        
        sections = []
        
        # Sort groups by size or name
        sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)
        
        for group_name, group_posts in sorted_groups:
            if group_by == 'subreddit':
                section_title = f"## r/{group_name}"
            elif group_by == 'date':
                section_title = f"## {group_name}"
            else:
                section_title = f"## {group_name}"
            
            section_content = [section_title]
            section_content.append(f"\n*{len(group_posts)} posts*\n")
            
            # Generate posts for this group
            for post in group_posts:
                section_content.append(self._format_post(post, config))
            
            sections.append('\n'.join(section_content))
        
        return sections
    
    def _generate_posts_section(self, posts: List[Dict[str, Any]], config: Dict[str, Any]) -> str:
        """Generate posts section without grouping."""
        content = ["## Posts\n"]
        
        for post in posts:
            content.append(self._format_post(post, config))
        
        return '\n'.join(content)
    
    def _format_post(self, post: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Format a single post for Markdown output."""
        title = post.get('title', 'Untitled')
        author = post.get('author', 'Unknown')
        subreddit = post.get('subreddit', 'Unknown')
        score = post.get('score', 0)
        num_comments = post.get('num_comments', 0)
        url = post.get('url', '')
        permalink = post.get('permalink', '')
        selftext = post.get('selftext', '')
        
        # Format date
        date_format = config.get('date_format', '%Y-%m-%d %H:%M')
        created_utc = post.get('created_utc', 0)
        if created_utc:
            try:
                dt = datetime.fromtimestamp(created_utc)
                date_str = dt.strftime(date_format)
            except (ValueError, OSError):
                date_str = 'Unknown'
        else:
            date_str = post.get('date_iso', 'Unknown')
        
        # Start formatting
        formatted = f"\n### {self._escape_markdown(title)}\n\n"
        
        # Post metadata
        metadata_parts = [
            f"**Author:** u/{author}",
            f"**Subreddit:** r/{subreddit}",
            f"**Score:** {score:,}",
            f"**Comments:** {num_comments:,}",
            f"**Date:** {date_str}"
        ]
        
        # Add post type and flags
        post_type = post.get('post_type', 'link')
        if post_type != 'link':
            metadata_parts.append(f"**Type:** {post_type.title()}")
        
        if post.get('is_nsfw', False):
            metadata_parts.append("**NSFW:** Yes")
        
        if post.get('spoiler', False):
            metadata_parts.append("**Spoiler:** Yes")
        
        formatted += " | ".join(metadata_parts) + "\n\n"
        
        # Content
        if config.get('include_selftext', True) and selftext:
            formatted += f"{self._format_text_content(selftext)}\n\n"
        
        # Links
        if config.get('include_links', True):
            link_style = config.get('link_style', 'inline')
            
            if url and url != permalink:
                if link_style == 'inline':
                    formatted += f"**Link:** [{url}]({url})\n\n"
                elif link_style == 'reference':
                    formatted += f"**Link:** [External Link][{post.get('id', '')}]\n\n"
                else:  # footnote
                    formatted += f"**Link:** {url}\n\n"
            
            if permalink:
                reddit_url = f"https://reddit.com{permalink}"
                formatted += f"**Reddit:** [View on Reddit]({reddit_url})\n\n"
        
        # Images and galleries
        if config.get('include_images', True):
            gallery_urls = post.get('gallery_image_urls', [])
            if gallery_urls:
                formatted += "**Gallery:**\n\n"
                for i, img_url in enumerate(gallery_urls[:5], 1):  # Limit to first 5
                    formatted += f"{i}. ![Image {i}]({img_url})\n"
                
                if len(gallery_urls) > 5:
                    formatted += f"\n*... and {len(gallery_urls) - 5} more images*\n"
                
                formatted += "\n"
        
        formatted += "---\n"
        return formatted
    
    def _format_text_content(self, text: str) -> str:
        """Format text content for Markdown."""
        if not text:
            return ""
        
        # Escape and format
        text = self._escape_markdown(text)
        
        # Convert to blockquote for readability
        lines = text.split('\n')
        quoted_lines = [f"> {line}" if line.strip() else ">" for line in lines]
        
        return '\n'.join(quoted_lines)
    
    def _escape_markdown(self, text: str) -> str:
        """Escape special Markdown characters."""
        if not text:
            return ""
        
        # Escape common Markdown special characters
        replacements = {
            '\\': '\\\\',
            '*': '\\*',
            '_': '\\_',
            '`': '\\`',
            '[': '\\[',
            ']': '\\]',
            '(': '\\(',
            ')': '\\)',
            '#': '\\#',
            '+': '\\+',
            '-': '\\-',
            '.': '\\.',
            '!': '\\!',
            '|': '\\|'
        }
        
        for char, escaped in replacements.items():
            text = text.replace(char, escaped)
        
        return text
    
    def _generate_metadata_section(self, data: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Generate export metadata section."""
        export_info = data.get('export_info', {})
        
        metadata = f"""## Export Information

| Field | Value |
|-------|-------|
| Export Format | Markdown |
| Export Date | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |
| Posts Exported | {len(data.get('posts', []))} |
| Template | {config.get('template', 'report')} |
"""
        
        if export_info:
            metadata += f"| Original Timestamp | {export_info.get('timestamp', 'Unknown')} |\n"
            metadata += f"| Schema Version | {export_info.get('schema_version', 'Unknown')} |\n"
        
        metadata += "\n"
        return metadata
    
    def _generate_toc(self, sections: List[str]) -> str:
        """Generate table of contents from sections."""
        toc_lines = ["## Table of Contents\n"]
        
        for section in sections:
            lines = section.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('## '):
                    title = line[3:].strip()
                    anchor = title.lower().replace(' ', '-').replace('/', '').replace('\\', '')
                    toc_lines.append(f"- [{title}](#{anchor})")
                elif line.startswith('### '):
                    title = line[4:].strip()
                    anchor = title.lower().replace(' ', '-').replace('/', '').replace('\\', '')
                    toc_lines.append(f"  - [{title}](#{anchor})")
        
        toc_lines.append("")
        return '\n'.join(toc_lines)
    
    def _write_markdown_file(self, sections: List[str], output_file: Path,
                            config: Dict[str, Any], result: ExportResult) -> None:
        """Write markdown content to file."""
        content = '\n\n'.join(sections)
        encoding = config.get('encoding', 'utf-8')
        
        if config.get('compress', False):
            output_file = output_file.with_suffix(output_file.suffix + '.gz')
            with gzip.open(output_file, 'wt', encoding=encoding) as f:
                f.write(content)
            result.metadata['compressed'] = True
        else:
            with open(output_file, 'w', encoding=encoding) as f:
                f.write(content)
            result.metadata['compressed'] = False
        
        result.metadata['sections'] = len(sections)
        result.metadata['template'] = config.get('template', 'report')
    
    def _write_empty_document(self, output_file: Path, config: Dict[str, Any]) -> None:
        """Write empty markdown document."""
        title = config.get('title', 'Reddit Data Export')
        content = f"""# {title}

*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

---

## No Data

No posts were available for export.
"""
        
        with open(output_file, 'w', encoding=config.get('encoding', 'utf-8')) as f:
            f.write(content)
    
    def _get_size_factor(self) -> float:
        """Get size factor for Markdown format."""
        return 0.8  # Markdown is typically smaller than JSON but larger than CSV