"""
Crosspost Content Handler

Handles Reddit crossposts by tracking original post relationships,
detecting duplicates, and preserving crosspost metadata chains.
Creates relationship files and prevents infinite loops.
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, Set, Optional, List

from .base import BaseContentHandler, HandlerResult, HandlerError
from redditdl.scrapers import PostMetadata
from redditdl.utils import sanitize_filename
from redditdl.core.templates import FilenameTemplateEngine


class CrosspostContentHandler(BaseContentHandler):
    """
    Content handler for Reddit crossposts.
    
    Tracks original post relationships, handles crosspost chains,
    detects duplicates, and preserves crosspost metadata including
    original sources and relationships.
    """
    
    def __init__(self, priority: int = 30):
        super().__init__("crosspost", priority)
        self._processed_crossposts: Set[str] = set()
        self._template_engine: FilenameTemplateEngine = None
    
    @property
    def supported_content_types(self) -> Set[str]:
        """Crosspost handler supports crosspost content type."""
        return {'crosspost'}
    
    def can_handle(self, post: PostMetadata, content_type: str) -> bool:
        """
        Check if this handler can process the post.
        
        Args:
            post: PostMetadata object to check
            content_type: Detected content type
            
        Returns:
            True if this is a crosspost with parent information
        """
        if content_type != 'crosspost':
            return False
        
        # Must have crosspost parent ID
        crosspost_parent = getattr(post, 'crosspost_parent_id', None)
        return bool(crosspost_parent)
    
    async def process(
        self, 
        post: PostMetadata, 
        output_dir: Path,
        config: Dict[str, Any]
    ) -> HandlerResult:
        """
        Process crosspost and track relationships.
        
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
            content_type='crosspost'
        )
        
        try:
            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Check for duplicate/circular crosspost
            crosspost_parent = getattr(post, 'crosspost_parent_id', None)
            if not crosspost_parent:
                raise HandlerError("No crosspost parent ID found")
            
            # Detect circular references
            if self._is_circular_crosspost(post, config):
                self.logger.warning(f"Circular crosspost detected for {post.id}, skipping")
                result.success = True
                result.add_operation("circular_detection")
                return result
            
            # Generate filenames
            metadata_filename = self._construct_metadata_filename(post, config)
            metadata_path = output_dir / metadata_filename
            
            # Create crosspost metadata
            crosspost_data = self._create_crosspost_metadata(post, config)
            
            self.logger.info(f"Processing crosspost: {post.id} -> {crosspost_parent}")
            
            # Save crosspost metadata
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(crosspost_data, f, indent=2, ensure_ascii=False)
            
            result.success = True
            result.add_file(metadata_path)
            result.add_operation("crosspost_tracking")
            
            # Create relationship file if requested
            if config.get('create_relationships', True):
                rel_path = self._create_relationship_file(crosspost_data, output_dir, config)
                if rel_path:
                    result.add_file(rel_path)
                    result.add_operation("relationship_mapping")
            
            # Create summary if requested
            if config.get('create_summary', True):
                summary_path = self._create_crosspost_summary(crosspost_data, metadata_path, config)
                if summary_path:
                    result.add_file(summary_path)
                    result.add_operation("crosspost_summary")
            
            # Track this crosspost to prevent duplicates
            self._processed_crossposts.add(post.id)
            
            self.logger.info(f"Successfully processed crosspost: {metadata_path.name}")
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Error processing crosspost {post.id}: {e}")
            raise HandlerError(f"Crosspost processing failed: {e}") from e
        
        result.processing_time = time.time() - start_time
        return result
    
    def _construct_metadata_filename(self, post: PostMetadata, config: Dict[str, Any]) -> str:
        """
        Construct a filename for crosspost metadata.
        
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
            title_part = post.title[:50] if post.title else "crosspost"
            # Replace colons in date for safer filenames
            safe_date = post.date_iso.replace(':', '_')
            base_filename = f"{safe_date}_{post.id}_{title_part}_crosspost"
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
            'content_type': 'crosspost',
            'url': post.url,
            'is_video': post.is_video,
        }
        
        # Add crosspost-specific variables
        crosspost_parent = getattr(post, 'crosspost_parent_id', None)
        if crosspost_parent:
            template_vars['crosspost_parent_id'] = crosspost_parent
        
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
            title_part = post.title[:50] if post.title else "crosspost"
            safe_date = post.date_iso.replace(':', '_')
            base_filename = f"{safe_date}_{post.id}_{title_part}_crosspost"
            return f"{sanitize_filename(base_filename)}.json"
    
    def _create_crosspost_metadata(self, post: PostMetadata, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create comprehensive crosspost metadata.
        
        Args:
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            Crosspost metadata dictionary
        """
        crosspost_parent = getattr(post, 'crosspost_parent_id', None)
        
        # Create structured crosspost data
        crosspost_data = {
            'crosspost_metadata': {
                'crosspost_id': post.id,
                'original_post_id': crosspost_parent,
                'crosspost_subreddit': post.subreddit,
                'crosspost_author': post.author,
                'crosspost_title': post.title,
                'crosspost_url': post.url,
                'crosspost_date': post.date_iso,
                'created_at': time.time(),
                'handler': self.name
            },
            'post_data': post.to_dict(),
            'relationships': {
                'parent_id': crosspost_parent,
                'relationship_type': 'crosspost',
                'chain_depth': self._calculate_chain_depth(post),
                'is_duplicate': post.id in self._processed_crossposts
            }
        }
        
        # Add additional metadata if available
        if hasattr(post, 'score'):
            crosspost_data['crosspost_metadata']['score'] = post.score
        if hasattr(post, 'num_comments'):
            crosspost_data['crosspost_metadata']['comments'] = post.num_comments
        
        return crosspost_data
    
    def _calculate_chain_depth(self, post: PostMetadata) -> int:
        """
        Calculate the depth of the crosspost chain.
        
        Args:
            post: PostMetadata object
            
        Returns:
            Chain depth (1 for direct crosspost, higher for chains)
        """
        # Enhanced chain depth calculation with relationship tracking
        depth = 1
        max_depth = 10  # Prevent infinite loops
        seen_posts = set()
        current_parent = getattr(post, 'crosspost_parent_id', None)
        
        # Try to trace the crosspost chain using existing relationship data
        relationship_file = Path.cwd() / "crosspost_relationships.json"
        relationships = {}
        
        if relationship_file.exists():
            try:
                with open(relationship_file, 'r', encoding='utf-8') as f:
                    relationships = json.load(f)
            except (json.JSONDecodeError, IOError):
                # If we can't read relationships, default to depth 1
                return 1
        
        # Trace the chain backwards through parent relationships
        while current_parent and depth < max_depth:
            if current_parent in seen_posts:
                # Circular reference detected, break
                break
            
            seen_posts.add(current_parent)
            
            # Look for the parent in our relationships
            if current_parent in relationships:
                parent_info = relationships[current_parent]
                current_parent = parent_info.get('parent_id')
                if current_parent:
                    depth += 1
                else:
                    break
            else:
                # No more parent information available
                break
        
        return depth
    
    def _is_circular_crosspost(self, post: PostMetadata, config: Dict[str, Any]) -> bool:
        """
        Check if this crosspost creates a circular reference.
        
        Args:
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            True if circular reference detected
        """
        # Simple check: if we've already processed this post ID
        if post.id in self._processed_crossposts:
            return True
        
        # Enhanced circular detection using relationship tracking
        return self._detect_circular_chain(post, config)
    
    def _detect_circular_chain(self, post: PostMetadata, config: Dict[str, Any]) -> bool:
        """
        Detect circular references in crosspost chains.
        
        Args:
            post: PostMetadata object
            config: Configuration options
            
        Returns:
            True if circular chain detected
        """
        max_depth = 20  # Maximum chain depth to check
        seen_posts = set()
        current_post_id = post.id
        current_parent = getattr(post, 'crosspost_parent_id', None)
        
        # Load existing relationships
        relationship_file = Path.cwd() / "crosspost_relationships.json"
        relationships = {}
        
        if relationship_file.exists():
            try:
                with open(relationship_file, 'r', encoding='utf-8') as f:
                    relationships = json.load(f)
            except (json.JSONDecodeError, IOError):
                return False
        
        # Check if adding this post would create a cycle
        depth = 0
        while current_parent and depth < max_depth:
            if current_parent == current_post_id:
                # Direct circular reference: parent points back to current post
                return True
            
            if current_parent in seen_posts:
                # Circular reference detected in chain
                return True
            
            seen_posts.add(current_parent)
            
            # Follow the chain
            if current_parent in relationships:
                parent_info = relationships[current_parent]
                current_parent = parent_info.get('parent_id')
                depth += 1
            else:
                # Chain ends here
                break
        
        return False
    
    def get_relationship_stats(self, output_dir: Path) -> Dict[str, Any]:
        """
        Get statistics about crosspost relationships.
        
        Args:
            output_dir: Directory containing relationship files
            
        Returns:
            Dictionary with relationship statistics
        """
        relationship_file = output_dir / "crosspost_relationships.json"
        
        if not relationship_file.exists():
            return {
                'total_crossposts': 0,
                'max_chain_depth': 0,
                'chains_count': 0,
                'circular_references': 0
            }
        
        try:
            with open(relationship_file, 'r', encoding='utf-8') as f:
                relationships = json.load(f)
        except (json.JSONDecodeError, IOError):
            return {'error': 'Failed to read relationship file'}
        
        # Analyze relationships
        stats = {
            'total_crossposts': len(relationships),
            'max_chain_depth': 0,
            'chains_count': 0,
            'circular_references': 0,
            'chain_depths': {},
            'popular_originals': {}
        }
        
        # Find chain heads (posts that are not children of other posts)
        all_posts = set(relationships.keys())
        child_posts = set()
        
        for post_id, rel_info in relationships.items():
            parent_id = rel_info.get('parent_id')
            if parent_id:
                child_posts.add(post_id)
        
        chain_heads = all_posts - child_posts
        
        # Analyze each chain
        for head_id in chain_heads:
            depth = self._calculate_chain_depth_from_relationships(head_id, relationships)
            stats['max_chain_depth'] = max(stats['max_chain_depth'], depth)
            
            if depth > 1:
                stats['chains_count'] += 1
            
            # Track depth distribution
            if depth not in stats['chain_depths']:
                stats['chain_depths'][depth] = 0
            stats['chain_depths'][depth] += 1
        
        # Find popular original posts (most crossposted)
        original_counts = {}
        for rel_info in relationships.values():
            parent_id = rel_info.get('parent_id')
            if parent_id:
                if parent_id not in original_counts:
                    original_counts[parent_id] = 0
                original_counts[parent_id] += 1
        
        # Get top 5 most crossposted originals
        sorted_originals = sorted(original_counts.items(), key=lambda x: x[1], reverse=True)
        stats['popular_originals'] = dict(sorted_originals[:5])
        
        return stats
    
    def _calculate_chain_depth_from_relationships(self, post_id: str, relationships: Dict[str, Any]) -> int:
        """
        Calculate chain depth starting from a specific post using relationship data.
        
        Args:
            post_id: Starting post ID
            relationships: Dictionary of all relationships
            
        Returns:
            Chain depth starting from this post
        """
        depth = 1
        max_depth = 20
        seen_posts = set()
        
        # Find all children of this post
        children = [pid for pid, rel in relationships.items() if rel.get('parent_id') == post_id]
        
        if not children:
            return depth
        
        # Recursively find the maximum depth among all children
        max_child_depth = 0
        for child_id in children:
            if child_id not in seen_posts and depth < max_depth:
                seen_posts.add(child_id)
                child_depth = self._calculate_chain_depth_from_relationships(child_id, relationships)
                max_child_depth = max(max_child_depth, child_depth)
        
        return depth + max_child_depth
    
    def _create_relationship_file(self, crosspost_data: Dict[str, Any], output_dir: Path, config: Dict[str, Any]) -> Optional[Path]:
        """
        Create a relationship mapping file.
        
        Args:
            crosspost_data: Crosspost metadata
            output_dir: Output directory
            config: Configuration options
            
        Returns:
            Path to the relationship file
        """
        rel_path = output_dir / "crosspost_relationships.json"
        
        try:
            # Load existing relationships if file exists
            relationships = {}
            if rel_path.exists():
                with open(rel_path, 'r', encoding='utf-8') as f:
                    relationships = json.load(f)
            
            # Add new relationship
            crosspost_id = crosspost_data['crosspost_metadata']['crosspost_id']
            parent_id = crosspost_data['relationships']['parent_id']
            
            relationships[crosspost_id] = {
                'parent_id': parent_id,
                'subreddit': crosspost_data['crosspost_metadata']['crosspost_subreddit'],
                'author': crosspost_data['crosspost_metadata']['crosspost_author'],
                'date': crosspost_data['crosspost_metadata']['crosspost_date'],
                'chain_depth': crosspost_data['relationships']['chain_depth']
            }
            
            # Save updated relationships
            with open(rel_path, 'w', encoding='utf-8') as f:
                json.dump(relationships, f, indent=2, ensure_ascii=False)
            
            return rel_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create relationship file: {e}")
            return None
    
    def _create_crosspost_summary(self, crosspost_data: Dict[str, Any], metadata_path: Path, config: Dict[str, Any]) -> Optional[Path]:
        """
        Create a Markdown summary of the crosspost.
        
        Args:
            crosspost_data: Crosspost metadata
            metadata_path: Path to the metadata file
            config: Configuration options
            
        Returns:
            Path to the summary file
        """
        summary_path = metadata_path.with_suffix('.md')
        
        try:
            metadata = crosspost_data['crosspost_metadata']
            relationships = crosspost_data['relationships']
            
            # Create Markdown content
            content = f"""# Crosspost: {metadata['crosspost_title']}

**Crosspost ID:** {metadata['crosspost_id']}  
**Original Post ID:** {metadata['original_post_id']}  
**Subreddit:** r/{metadata['crosspost_subreddit']}  
**Author:** u/{metadata['crosspost_author']}  
**Posted:** {metadata['crosspost_date']}

## Relationship Information

- **Relationship Type:** {relationships['relationship_type']}
- **Chain Depth:** {relationships['chain_depth']}
- **Is Duplicate:** {relationships['is_duplicate']}

## Links

- **Crosspost URL:** {metadata['crosspost_url']}
- **Original Post:** https://reddit.com/comments/{metadata['original_post_id']}

---
*Crosspost data extracted from Reddit post {metadata['crosspost_id']}*
"""
            
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return summary_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create crosspost summary: {e}")
            return None
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate crosspost handler configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate create_relationships flag
        create_relationships = config.get('create_relationships')
        if create_relationships is not None and not isinstance(create_relationships, bool):
            errors.append("create_relationships must be a boolean")
        
        # Validate create_summary flag
        create_summary = config.get('create_summary')
        if create_summary is not None and not isinstance(create_summary, bool):
            errors.append("create_summary must be a boolean")
        
        return errors