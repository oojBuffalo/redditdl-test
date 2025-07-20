"""
Organization Pipeline Stage

Handles the organization and structuring of downloaded content files.
This stage provides file organization capabilities and serves as a placeholder
for more advanced organization features to be implemented in later tasks.
"""

import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from redditdl.core.pipeline.interfaces import PipelineStage, PipelineContext, PipelineResult
from redditdl.scrapers import PostMetadata


class OrganizationStage(PipelineStage):
    """
    Pipeline stage for organizing downloaded content.
    
    This stage handles the organization and structuring of downloaded files
    according to various organizational schemes. It provides a foundation
    for the file organization features specified in the PRD.
    
    This is currently a placeholder implementation that will be enhanced
    in later tasks with features like:
    - Directory structure organization (by subreddit, user, date, etc.)
    - File renaming and restructuring
    - Duplicate detection and handling
    - Archive creation and management
    
    Configuration options:
    - organize_by: Organization scheme ("subreddit", "user", "date", "type")
    - create_structure: Whether to create organized directory structure
    - move_files: Whether to move files into organized structure
    - duplicate_handling: How to handle duplicate files
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("organization", config)
    
    async def process(self, context: PipelineContext) -> PipelineResult:
        """
        Organize downloaded content files.
        
        Args:
            context: Pipeline context containing posts and metadata
            
        Returns:
            PipelineResult: Results of the organization process
        """
        result = PipelineResult(stage_name=self.name)
        start_time = time.time()
        
        try:
            posts_count = len(context.posts)
            self.logger.info(f"Organizing content for {posts_count} posts")
            
            if posts_count == 0:
                result.add_warning("No posts to organize")
                return result
            
            # Get organization configuration
            organize_by = context.get_config("organize_by", self.get_config("organize_by", "none"))
            create_structure = context.get_config("create_structure", self.get_config("create_structure", False))
            
            if organize_by == "none" or not create_structure:
                self.logger.info("Organization disabled or not configured")
                result.processed_count = posts_count
                result.set_data("organization_scheme", "none")
                result.set_data("files_organized", 0)
                return result
            
            # For now, this is a placeholder implementation
            # In future tasks, this will implement actual file organization
            organized_count = 0
            
            if organize_by == "subreddit":
                organized_count = self._organize_by_subreddit(context)
            elif organize_by == "user":
                organized_count = self._organize_by_user(context)
            elif organize_by == "date":
                organized_count = self._organize_by_date(context)
            elif organize_by == "type":
                organized_count = self._organize_by_type(context)
            else:
                result.add_warning(f"Unknown organization scheme: {organize_by}")
            
            result.processed_count = posts_count
            result.set_data("organization_scheme", organize_by)
            result.set_data("files_organized", organized_count)
            result.set_data("total_posts", posts_count)
            
            self.logger.info(
                f"Organization completed: {organized_count} items organized using '{organize_by}' scheme"
            )
            
        except Exception as e:
            self.logger.error(f"Error during organization: {e}")
            result.add_error(f"Organization failed: {e}")
        
        result.execution_time = time.time() - start_time
        return result
    
    def _organize_by_subreddit(self, context: PipelineContext) -> int:
        """
        Organize content by subreddit (placeholder implementation).
        
        Args:
            context: Pipeline context
            
        Returns:
            Number of items organized
        """
        self.logger.debug("Organizing by subreddit (placeholder)")
        
        # Placeholder: In actual implementation, this would:
        # 1. Group posts by subreddit
        # 2. Create directory structure for each subreddit
        # 3. Move or link files to appropriate directories
        # 4. Update file paths in metadata
        
        subreddits = set()
        for post in context.posts:
            if post.subreddit:
                subreddits.add(post.subreddit)
        
        self.logger.debug(f"Found {len(subreddits)} unique subreddits")
        return len(context.posts)  # Placeholder return
    
    def _organize_by_user(self, context: PipelineContext) -> int:
        """
        Organize content by user (placeholder implementation).
        
        Args:
            context: Pipeline context
            
        Returns:
            Number of items organized
        """
        self.logger.debug("Organizing by user (placeholder)")
        
        # Placeholder: Similar to subreddit organization but by author
        users = set()
        for post in context.posts:
            if post.author and post.author != '[deleted]':
                users.add(post.author)
        
        self.logger.debug(f"Found {len(users)} unique users")
        return len(context.posts)  # Placeholder return
    
    def _organize_by_date(self, context: PipelineContext) -> int:
        """
        Organize content by date (placeholder implementation).
        
        Args:
            context: Pipeline context
            
        Returns:
            Number of items organized
        """
        self.logger.debug("Organizing by date (placeholder)")
        
        # Placeholder: Organize files into date-based directory structure
        # YYYY/MM/DD or similar hierarchy
        
        dates = set()
        for post in context.posts:
            if hasattr(post, 'date_iso') and post.date_iso:
                # Extract date part (YYYY-MM-DD)
                date_part = post.date_iso.split('T')[0] if 'T' in post.date_iso else post.date_iso
                dates.add(date_part)
        
        self.logger.debug(f"Found {len(dates)} unique dates")
        return len(context.posts)  # Placeholder return
    
    def _organize_by_type(self, context: PipelineContext) -> int:
        """
        Organize content by media type (placeholder implementation).
        
        Args:
            context: Pipeline context
            
        Returns:
            Number of items organized
        """
        self.logger.debug("Organizing by media type (placeholder)")
        
        # Placeholder: Organize files by type (images, videos, text, etc.)
        types = set()
        for post in context.posts:
            # Basic type detection based on URL or content
            if post.media_url or post.url:
                url = (post.media_url or post.url).lower()
                if any(ext in url for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    types.add('images')
                elif any(ext in url for ext in ['.mp4', '.webm', '.mov', '.avi']):
                    types.add('videos')
                else:
                    types.add('other')
            elif post.selftext:
                types.add('text')
        
        self.logger.debug(f"Found content types: {types}")
        return len(context.posts)  # Placeholder return
    
    def validate_config(self) -> List[str]:
        """
        Validate the organization stage configuration.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate organization scheme
        organize_by = self.get_config("organize_by")
        if organize_by is not None and organize_by not in ["none", "subreddit", "user", "date", "type"]:
            errors.append("organize_by must be one of: none, subreddit, user, date, type")
        
        # Validate boolean options
        create_structure = self.get_config("create_structure")
        if create_structure is not None and not isinstance(create_structure, bool):
            errors.append("create_structure must be a boolean")
        
        move_files = self.get_config("move_files")
        if move_files is not None and not isinstance(move_files, bool):
            errors.append("move_files must be a boolean")
        
        return errors
    
    async def pre_process(self, context: PipelineContext) -> None:
        """Pre-processing setup for organization stage."""
        self.logger.debug("Organization stage pre-processing")
        
        # Log configuration for debugging
        organize_by = context.get_config("organize_by", self.get_config("organize_by", "none"))
        create_structure = context.get_config("create_structure", self.get_config("create_structure", False))
        move_files = context.get_config("move_files", self.get_config("move_files", False))
        
        self.logger.debug(
            f"Organization configuration - Scheme: {organize_by}, "
            f"Create structure: {create_structure}, Move files: {move_files}"
        )
        
        # Validate that we have content to organize
        processing_completed = context.get_metadata("processing_completed", False)
        if not processing_completed:
            self.logger.warning("Processing stage may not have completed successfully")
    
    async def post_process(self, context: PipelineContext, result: PipelineResult) -> None:
        """Post-processing cleanup for organization stage."""
        self.logger.debug("Organization stage post-processing")
        
        # Store organization metadata in context for other stages
        if result.success:
            context.set_metadata("organization_completed", True)
            context.set_metadata("organization_scheme", result.get_data("organization_scheme"))
            context.set_metadata("files_organized", result.get_data("files_organized", 0))
        else:
            context.set_metadata("organization_completed", False)
            
        # Log organization results
        scheme = result.get_data("organization_scheme", "unknown")
        organized_count = result.get_data("files_organized", 0)
        
        if organized_count > 0:
            self.logger.info(f"Organization completed: {organized_count} items organized using '{scheme}' scheme")
        else:
            self.logger.info("No organization performed (disabled or no applicable content)")