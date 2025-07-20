"""
Poll Content Handler

Handles Reddit poll posts by extracting and saving poll data including
questions, options, vote counts, and results. Creates structured JSON
and optional visualization files.
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, Set, List

from .base import BaseContentHandler, HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.utils import sanitize_filename
from redditdl.core.templates import FilenameTemplateEngine


class PollContentHandler(BaseContentHandler):
    """
    Content handler for Reddit poll posts.
    
    Extracts poll questions, options, vote counts, and percentages.
    Saves poll data as structured JSON with optional result visualization.
    """
    
    def __init__(self, priority: int = 70):
        super().__init__("poll", priority)
        self._template_engine: FilenameTemplateEngine = None
    
    @property
    def supported_content_types(self) -> Set[str]:
        """Poll handler supports poll content type."""
        return {'poll'}
    
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this is a poll post with poll data
        """
        if content_type != 'poll':
            return False
        
        # Must have poll data
        poll_data = getattr(post, 'poll_data', None)
        return bool(poll_data)
    
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Extract and save poll data.
        
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
            content_type='poll'
        )
        
        try:
            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Get poll data
            poll_data = getattr(post, 'poll_data', None)
            if not poll_data:
                raise HandlerError("No poll data found in post")
            
            # Generate filename
            filename = self._construct_filename(post, config)
            output_path = output_dir / filename
            
            # Create structured poll data
            structured_data = self._create_structured_poll_data(post, poll_data, config)
            
            self.logger.info(f"Saving poll data: {post.id}")
            
            # Save poll data as JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(structured_data, f, indent=2, ensure_ascii=False)
            
            result.success = True
            result.add_file(output_path)
            result.add_operation("poll_save")
            
            # Create visualization if requested
            if config.get('create_visualization', False):
                viz_path = self._create_poll_visualization(structured_data, output_path, config)
                if viz_path:
                    result.add_file(viz_path)
                    result.add_operation("poll_visualization")
            
            # Create text summary if requested
            if config.get('create_summary', True):
                summary_path = self._create_poll_summary(structured_data, output_path, config)
                if summary_path:
                    result.add_file(summary_path)
                    result.add_operation("poll_summary")
            
            self.logger.info(f"Successfully saved poll data: {output_path.name}")
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Error processing poll post {post.id}: {e}")
            raise HandlerError(f"Poll processing failed: {e}") from e
        
        result.processing_time = time.time() - start_time
        return result
    
    def _construct_filename(self, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Construct a filename for the poll data.
        
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
            title_part = post.title[:50] if post.title else "poll"
            # Replace colons in date for safer filenames
            safe_date = post.date_iso.replace(':', '_')
            base_filename = f"{safe_date}_{post.id}_{title_part}_poll"
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
            'content_type': 'poll',
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
            title_part = post.title[:50] if post.title else "poll"
            safe_date = post.date_iso.replace(':', '_')
            base_filename = f"{safe_date}_{post.id}_{title_part}_poll"
            return f"{sanitize_filename(base_filename)}.json"
    
    def _create_structured_poll_data(self, post: PostMetadata, poll_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create structured poll data with metadata.
        
        Args:
            post: PostMetadata object
            poll_data: Raw poll data from Reddit
            config: Configuration options
            
        Returns:
            Structured poll data dictionary
        """
        # Extract poll information
        question = poll_data.get('question', post.title)
        options = poll_data.get('options', [])
        total_votes = poll_data.get('total_vote_count', 0)
        voting_end_timestamp = poll_data.get('voting_end_timestamp')
        
        # Process poll options
        processed_options = []
        for i, option in enumerate(options):
            option_data = {
                'id': option.get('id', f'option_{i+1}'),
                'text': option.get('text', f'Option {i+1}'),
                'vote_count': option.get('vote_count', 0),
                'percentage': 0.0
            }
            
            # Calculate percentage
            if total_votes > 0:
                option_data['percentage'] = (option_data['vote_count'] / total_votes) * 100
            
            processed_options.append(option_data)
        
        # Create structured data
        structured_data = {
            'post_metadata': post.to_dict(),
            'poll_data': {
                'question': question,
                'options': processed_options,
                'total_votes': total_votes,
                'voting_end_timestamp': voting_end_timestamp,
                'extracted_at': time.time(),
                'handler': self.name
            },
            'summary': {
                'total_options': len(processed_options),
                'most_voted_option': None,
                'least_voted_option': None,
                'vote_distribution': {}
            }
        }
        
        # Add summary statistics
        if processed_options:
            # Find most and least voted options
            most_voted = max(processed_options, key=lambda x: x['vote_count'])
            least_voted = min(processed_options, key=lambda x: x['vote_count'])
            
            structured_data['summary']['most_voted_option'] = {
                'text': most_voted['text'],
                'votes': most_voted['vote_count'],
                'percentage': most_voted['percentage']
            }
            
            structured_data['summary']['least_voted_option'] = {
                'text': least_voted['text'],
                'votes': least_voted['vote_count'],
                'percentage': least_voted['percentage']
            }
            
            # Create vote distribution
            for option in processed_options:
                structured_data['summary']['vote_distribution'][option['text']] = option['vote_count']
        
        return structured_data
    
    def _create_poll_visualization(self, poll_data: Dict[str, Any], json_path: Path, config: Dict[str, Any]) -> Path:
        """
        Create a simple text-based visualization of poll results.
        
        Args:
            poll_data: Structured poll data
            json_path: Path to the JSON file
            config: Configuration options
            
        Returns:
            Path to the visualization file
        """
        viz_path = json_path.with_suffix('.txt')
        
        try:
            poll_info = poll_data['poll_data']
            question = poll_info['question']
            options = poll_info['options']
            total_votes = poll_info['total_votes']
            
            # Create text visualization
            lines = [
                f"Poll Results: {question}",
                "=" * 60,
                f"Total Votes: {total_votes}",
                "",
            ]
            
            # Add option results with simple bar chart
            for option in options:
                percentage = option['percentage']
                vote_count = option['vote_count']
                text = option['text']
                
                # Create simple bar (each # represents ~2%)
                bar_length = int(percentage // 2)
                bar = "#" * bar_length
                
                lines.append(f"{text[:30]:<30} {vote_count:>6} votes ({percentage:5.1f}%) {bar}")
            
            lines.extend([
                "",
                f"Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"Source: Reddit post {poll_data['post_metadata']['id']}"
            ])
            
            with open(viz_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
            
            return viz_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create poll visualization: {e}")
            return None
    
    def _create_poll_summary(self, poll_data: Dict[str, Any], json_path: Path, config: Dict[str, Any]) -> Path:
        """
        Create a Markdown summary of the poll.
        
        Args:
            poll_data: Structured poll data
            json_path: Path to the JSON file
            config: Configuration options
            
        Returns:
            Path to the summary file
        """
        summary_path = json_path.with_suffix('.md')
        
        try:
            poll_info = poll_data['poll_data']
            summary = poll_data['summary']
            post_data = poll_data['post_metadata']
            
            # Create Markdown content
            content = f"""# Poll: {poll_info['question']}

**Subreddit:** r/{post_data['subreddit']}  
**Author:** u/{post_data['author']}  
**Posted:** {post_data['date']}  
**Total Votes:** {poll_info['total_votes']}

## Results

"""
            
            # Add results table
            for option in poll_info['options']:
                content += f"- **{option['text']}**: {option['vote_count']} votes ({option['percentage']:.1f}%)\n"
            
            content += f"""

## Summary

- **Most voted option:** {summary['most_voted_option']['text'] if summary['most_voted_option'] else 'N/A'}
- **Least voted option:** {summary['least_voted_option']['text'] if summary['least_voted_option'] else 'N/A'}
- **Total options:** {summary['total_options']}

---
*Data extracted from Reddit post {post_data['id']}*
"""
            
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return summary_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create poll summary: {e}")
            return None
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate poll handler configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate create_visualization flag
        create_visualization = config.get('create_visualization')
        if create_visualization is not None and not isinstance(create_visualization, bool):
            errors.append("create_visualization must be a boolean")
        
        # Validate create_summary flag
        create_summary = config.get('create_summary')
        if create_summary is not None and not isinstance(create_summary, bool):
            errors.append("create_summary must be a boolean")
        
        return errors