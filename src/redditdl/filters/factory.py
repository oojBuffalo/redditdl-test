"""
Filter Factory for creating filter instances from configuration.

Provides a centralized factory for creating and configuring filter instances
based on configuration dictionaries or CLI arguments.
"""

import logging
from typing import Any, Dict, List, Optional, Type, Union
from redditdl.filters.base import Filter, FilterChain, FilterComposition
from redditdl.filters.score import ScoreFilter
from redditdl.filters.date import DateFilter
from redditdl.filters.keyword import KeywordFilter
from redditdl.filters.domain import DomainFilter
from redditdl.filters.media_type import MediaTypeFilter
from redditdl.filters.nsfw import NSFWFilter


class FilterFactory:
    """
    Factory class for creating filter instances from configuration.
    
    Supports creating individual filters and filter chains with composition logic.
    """
    
    # Registry of available filter types
    FILTER_REGISTRY: Dict[str, Type[Filter]] = {
        'score': ScoreFilter,
        'date': DateFilter,
        'keyword': KeywordFilter,
        'domain': DomainFilter,
        'media_type': MediaTypeFilter,
        'nsfw': NSFWFilter,
    }
    
    def __init__(self):
        """Initialize the filter factory."""
        self.logger = logging.getLogger(__name__)
    
    @classmethod
    def create_filter(cls, filter_type: str, config: Optional[Dict[str, Any]] = None) -> Filter:
        """
        Create a single filter instance.
        
        Args:
            filter_type: Type of filter to create
            config: Configuration for the filter
            
        Returns:
            Filter instance
            
        Raises:
            ValueError: If filter type is unknown
        """
        if filter_type not in cls.FILTER_REGISTRY:
            available_types = ', '.join(sorted(cls.FILTER_REGISTRY.keys()))
            raise ValueError(f"Unknown filter type '{filter_type}'. Available types: {available_types}")
        
        filter_class = cls.FILTER_REGISTRY[filter_type]
        return filter_class(config)
    
    @classmethod
    def create_filter_chain(
        cls, 
        filter_configs: List[Dict[str, Any]], 
        composition: Union[str, FilterComposition] = FilterComposition.AND
    ) -> FilterChain:
        """
        Create a filter chain from a list of filter configurations.
        
        Args:
            filter_configs: List of filter configuration dictionaries
            composition: How to combine filters ('and', 'or', or FilterComposition)
            
        Returns:
            FilterChain instance
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Convert string composition to enum
        if isinstance(composition, str):
            composition_map = {
                'and': FilterComposition.AND,
                'or': FilterComposition.OR
            }
            composition = composition_map.get(composition.lower(), FilterComposition.AND)
        
        filters = []
        for i, filter_config in enumerate(filter_configs):
            try:
                if 'type' not in filter_config:
                    raise ValueError(f"Filter configuration {i} missing 'type' field")
                
                filter_type = filter_config['type']
                filter_params = filter_config.get('config', {})
                
                filter_instance = cls.create_filter(filter_type, filter_params)
                filters.append(filter_instance)
                
            except Exception as e:
                raise ValueError(f"Error creating filter {i}: {e}")
        
        return FilterChain(filters, composition)
    
    @classmethod
    def create_from_cli_args(cls, args: Dict[str, Any]) -> Optional[FilterChain]:
        """
        Create a filter chain from CLI arguments.
        
        Args:
            args: Dictionary of CLI arguments
            
        Returns:
            FilterChain instance or None if no filters specified
        """
        filter_configs = []
        
        # Score filter
        if args.get('min_score') is not None or args.get('max_score') is not None:
            score_config = {}
            if args.get('min_score') is not None:
                score_config['min_score'] = args['min_score']
            if args.get('max_score') is not None:
                score_config['max_score'] = args['max_score']
            
            filter_configs.append({
                'type': 'score',
                'config': score_config
            })
        
        # Date filter
        date_fields = ['date_after', 'date_before', 'date_from', 'date_to']
        date_config = {field: args[field] for field in date_fields if args.get(field) is not None}
        if date_config:
            filter_configs.append({
                'type': 'date',
                'config': date_config
            })
        
        # Keyword filter
        keywords_include = args.get('keywords_include', [])
        keywords_exclude = args.get('keywords_exclude', [])
        if keywords_include or keywords_exclude:
            keyword_config = {}
            if keywords_include:
                keyword_config['keywords_include'] = keywords_include
            if keywords_exclude:
                keyword_config['keywords_exclude'] = keywords_exclude
            
            # Add keyword filter options
            for option in ['case_sensitive', 'whole_words_only', 'search_title', 'search_selftext', 'regex_mode']:
                if args.get(option) is not None:
                    keyword_config[option] = args[option]
            
            filter_configs.append({
                'type': 'keyword',
                'config': keyword_config
            })
        
        # Domain filter
        domains_allow = args.get('domains_allow', [])
        domains_block = args.get('domains_block', [])
        if domains_allow or domains_block:
            domain_config = {}
            if domains_allow:
                domain_config['domains_allow'] = domains_allow
            if domains_block:
                domain_config['domains_block'] = domains_block
            
            # Add domain filter options
            for option in ['match_subdomains', 'case_sensitive', 'self_posts_action']:
                if args.get(option) is not None:
                    domain_config[option] = args[option]
            
            filter_configs.append({
                'type': 'domain',
                'config': domain_config
            })
        
        # Media type filter
        media_types = args.get('media_types', [])
        file_extensions = args.get('file_extensions', [])
        exclude_media_types = args.get('exclude_media_types', [])
        exclude_file_extensions = args.get('exclude_file_extensions', [])
        if media_types or file_extensions or exclude_media_types or exclude_file_extensions:
            media_config = {}
            if media_types:
                media_config['media_types'] = media_types
            if file_extensions:
                media_config['file_extensions'] = file_extensions
            if exclude_media_types:
                media_config['exclude_media_types'] = exclude_media_types
            if exclude_file_extensions:
                media_config['exclude_file_extensions'] = exclude_file_extensions
            
            # Add media type filter options
            for option in ['include_self_posts', 'include_link_posts', 'strict_mode']:
                if args.get(option) is not None:
                    media_config[option] = args[option]
            
            filter_configs.append({
                'type': 'media_type',
                'config': media_config
            })
        
        # NSFW filter
        nsfw_mode = args.get('nsfw_filter') or args.get('nsfw_mode')
        if nsfw_mode and nsfw_mode != 'include':
            nsfw_config = {'mode': nsfw_mode}
            if args.get('nsfw_strict_mode') is not None:
                nsfw_config['strict_mode'] = args['nsfw_strict_mode']
            
            filter_configs.append({
                'type': 'nsfw',
                'config': nsfw_config
            })
        
        # Create filter chain if any filters were configured
        if filter_configs:
            composition = args.get('filter_composition', 'and')
            return cls.create_filter_chain(filter_configs, composition)
        
        return None
    
    @classmethod
    def create_from_config(cls, config: Dict[str, Any]) -> Optional[FilterChain]:
        """
        Create a filter chain from a configuration dictionary.
        
        Args:
            config: Configuration dictionary with filter settings
            
        Returns:
            FilterChain instance or None if no filters specified
        """
        # Check if filters are defined in configuration
        filters_config = config.get('filters', {})
        if not filters_config:
            return None
        
        filter_configs = []
        
        # Extract individual filter configurations
        for filter_type, filter_params in filters_config.items():
            if filter_type in cls.FILTER_REGISTRY and filter_params:
                filter_configs.append({
                    'type': filter_type,
                    'config': filter_params
                })
        
        # Create filter chain if any filters were configured
        if filter_configs:
            composition = config.get('filter_composition', 'and')
            return cls.create_filter_chain(filter_configs, composition)
        
        return None
    
    @classmethod
    def get_available_filters(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all available filter types.
        
        Returns:
            Dictionary mapping filter types to their schemas and descriptions
        """
        filter_info = {}
        
        for filter_type, filter_class in cls.FILTER_REGISTRY.items():
            # Create a temporary instance to get schema and description
            try:
                temp_instance = filter_class({})
                filter_info[filter_type] = {
                    'name': temp_instance.name,
                    'description': temp_instance.description,
                    'schema': temp_instance.get_config_schema()
                }
            except Exception as e:
                filter_info[filter_type] = {
                    'name': filter_class.__name__,
                    'description': f"Error getting info: {e}",
                    'schema': {}
                }
        
        return filter_info
    
    @classmethod
    def validate_filter_config(cls, filter_type: str, config: Dict[str, Any]) -> List[str]:
        """
        Validate a filter configuration without creating the filter.
        
        Args:
            filter_type: Type of filter to validate
            config: Configuration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        try:
            filter_instance = cls.create_filter(filter_type, config)
            return filter_instance.validate_config()
        except Exception as e:
            return [f"Error creating filter: {e}"]
    
    @classmethod
    def register_filter(cls, filter_type: str, filter_class: Type[Filter]) -> None:
        """
        Register a new filter type.
        
        Args:
            filter_type: Name of the filter type
            filter_class: Filter class to register
        """
        if not issubclass(filter_class, Filter):
            raise ValueError(f"Filter class must inherit from Filter")
        
        cls.FILTER_REGISTRY[filter_type] = filter_class
    
    @classmethod
    def unregister_filter(cls, filter_type: str) -> None:
        """
        Unregister a filter type.
        
        Args:
            filter_type: Name of the filter type to remove
        """
        if filter_type in cls.FILTER_REGISTRY:
            del cls.FILTER_REGISTRY[filter_type]