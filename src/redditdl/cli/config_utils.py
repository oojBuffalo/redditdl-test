"""
Configuration Utilities for CLI Commands

Shared utilities for handling configuration in CLI commands including
conversion helpers, validation, and display functions.
"""

import os
from typing import Dict, Any, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from redditdl.core.config import ConfigManager, AppConfig

# Import credentials loading function from main module
try:
    from main import load_credentials_from_dotenv
except ImportError:
    # Fallback stub for testing
    def load_credentials_from_dotenv(dotenv_path_str: str = ".env") -> None:
        pass

console = Console()


def load_config_from_cli(
    config_file: Optional[str] = None,
    cli_args: Optional[Dict[str, Any]] = None
) -> AppConfig:
    """
    Load configuration from CLI arguments with proper error handling.
    
    Args:
        config_file: Optional path to configuration file
        cli_args: Dictionary of CLI arguments to override config
        
    Returns:
        Validated AppConfig instance
        
    Raises:
        typer.Exit: If configuration is invalid
    """
    import typer
    
    try:
        # Load environment variables first
        from .utils import load_credentials_from_dotenv
        load_credentials_from_dotenv()
        
        # Initialize configuration manager
        config_manager = ConfigManager(config_file=config_file)
        
        # Load and validate configuration
        app_config = config_manager.load_config(cli_args=cli_args or {})
        
        # Show configuration warnings
        warnings = config_manager.validate_config(app_config)
        if warnings:
            console.print("[yellow]Configuration warnings:[/yellow]")
            for warning in warnings:
                console.print(f"  • {warning}")
            console.print()
        
        return app_config
        
    except Exception as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        raise typer.Exit(1)


def setup_progress_observers(config: AppConfig) -> 'EventEmitter':
    """
    Set up event emitter and observers based on configuration.
    
    Args:
        config: Application configuration
        
    Returns:
        Configured EventEmitter instance with attached observers
    """
    from core.events.emitter import EventEmitter
    from core.events.observers import ConsoleObserver, LoggingObserver, StatisticsObserver
    from cli.observers.progress import CLIProgressObserver, OutputMode, ProgressDisplay
    
    # Create event emitter
    emitter = EventEmitter()
    
    # Extract UI configuration
    ui_config = config.ui_config
    observer_config = config.observer_config
    
    # Determine output mode
    output_mode = OutputMode.NORMAL
    if ui_config.get('quiet_mode') or ui_config.get('output_mode') == 'quiet':
        output_mode = OutputMode.QUIET
    elif config.verbose or ui_config.get('output_mode') == 'verbose':
        output_mode = OutputMode.VERBOSE
    elif ui_config.get('output_mode') == 'json':
        output_mode = OutputMode.JSON
    
    # Determine progress display
    progress_display = None
    if ui_config.get('no_progress') or ui_config.get('progress_display') == 'none':
        progress_display = ProgressDisplay.NONE
    elif ui_config.get('progress_display'):
        progress_display = ProgressDisplay(ui_config['progress_display'])
    
    # Create and attach CLI progress observer
    try:
        cli_progress = CLIProgressObserver(
            name="cli_progress",
            output_mode=output_mode,
            progress_display=progress_display,
            show_individual=ui_config.get('show_individual_progress', True),
            max_individual_bars=ui_config.get('max_individual_bars', 5),
            show_eta=ui_config.get('show_eta', True),
            show_speed=ui_config.get('show_speed', True),
            show_statistics=ui_config.get('show_statistics', True),
            quiet_mode=(output_mode == OutputMode.QUIET),
            json_output=ui_config.get('json_output')
        )
        emitter.subscribe(cli_progress)
        
    except Exception as e:
        console.print(f"[yellow]Warning: Could not set up progress observer: {e}[/yellow]")
        console.print("[yellow]Falling back to basic console output[/yellow]")
        
        # Fallback to basic console observer
        console_observer = ConsoleObserver(
            name="console_fallback",
            verbose=config.verbose,
            use_rich=False,
            show_timestamps=True
        )
        emitter.subscribe(console_observer)
    
    # Add logging observer if configured
    if "logging" in observer_config.get('enabled_observers', []):
        try:
            logging_config = observer_config.get('logging_observer', {})
            log_observer = LoggingObserver(
                name="file_logger",
                log_file=logging_config.get('log_file', 'redditdl.log'),
                log_level=getattr(__import__('logging'), logging_config.get('log_level', 'INFO'))
            )
            emitter.subscribe(log_observer)
        except Exception as e:
            console.print(f"[yellow]Warning: Could not set up logging observer: {e}[/yellow]")
    
    # Add statistics observer if configured
    if "statistics" in observer_config.get('enabled_observers', []):
        try:
            stats_observer = StatisticsObserver(name="session_stats")
            emitter.subscribe(stats_observer)
        except Exception as e:
            console.print(f"[yellow]Warning: Could not set up statistics observer: {e}[/yellow]")
    
    return emitter


def build_cli_args(
    # Core arguments
    config: Optional[str] = None,
    dry_run: Optional[bool] = None,
    verbose: Optional[bool] = None,
    use_pipeline: Optional[bool] = None,
    
    # Scraping arguments
    api: Optional[bool] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    user_agent: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    sleep: Optional[float] = None,
    limit: Optional[int] = None,
    timeout: Optional[int] = None,
    
    # Output arguments
    outdir: Optional[str] = None,
    export_formats: Optional[list] = None,
    organize_by_date: Optional[bool] = None,
    organize_by_author: Optional[bool] = None,
    organize_by_subreddit: Optional[bool] = None,
    filename_template: Optional[str] = None,
    
    # Filter arguments
    min_score: Optional[int] = None,
    max_score: Optional[int] = None,
    include_nsfw: Optional[bool] = None,
    nsfw_only: Optional[bool] = None,
    nsfw_mode: Optional[str] = None,
    include_keywords: Optional[list] = None,
    exclude_keywords: Optional[list] = None,
    keyword_case_sensitive: Optional[bool] = None,
    keyword_regex: Optional[bool] = None,
    keyword_whole_words: Optional[bool] = None,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    allowed_domains: Optional[list] = None,
    blocked_domains: Optional[list] = None,
    media_types: Optional[list] = None,
    exclude_media_types: Optional[list] = None,
    file_extensions: Optional[list] = None,
    exclude_file_extensions: Optional[list] = None,
    filter_composition: Optional[str] = None,
    
    # Processing arguments
    embed_metadata: Optional[bool] = None,
    create_json_sidecars: Optional[bool] = None,
    concurrent_downloads: Optional[int] = None,
    
    # Processing pipeline arguments
    enable_processing: Optional[bool] = None,
    processing_profile: Optional[str] = None,
    
    # Image processing arguments
    image_format: Optional[str] = None,
    image_quality: Optional[int] = None,
    max_image_resolution: Optional[int] = None,
    
    # Video processing arguments
    video_format: Optional[str] = None,
    video_quality: Optional[int] = None,
    max_video_resolution: Optional[str] = None,
    
    # Thumbnail processing arguments
    generate_thumbnails: Optional[bool] = None,
    thumbnail_size: Optional[int] = None,
    thumbnail_timestamp: Optional[str] = None,
    
    # UI & Progress arguments
    quiet: Optional[bool] = None,
    output_mode: Optional[str] = None,
    progress_display: Optional[str] = None,
    no_progress: Optional[bool] = None,
    json_output: Optional[str] = None,
    show_eta: Optional[bool] = None,
    show_speed: Optional[bool] = None,
    show_statistics: Optional[bool] = None,
    
    **kwargs
) -> Dict[str, Any]:
    """
    Build CLI arguments dictionary from function parameters.
    
    This function takes all the common CLI arguments and builds a dictionary
    suitable for passing to ConfigManager.load_config().
    
    Returns:
        Dictionary of non-None CLI arguments
    """
    # Build the arguments dictionary, excluding None values
    args = {}
    
    # Core settings
    if dry_run is not None:
        args['dry_run'] = dry_run
    if verbose is not None:
        args['verbose'] = verbose
    if use_pipeline is not None:
        args['use_pipeline'] = use_pipeline
    
    # Scraping settings
    if api is not None:
        args['api'] = api
    if client_id is not None:
        args['client_id'] = client_id
    if client_secret is not None:
        args['client_secret'] = client_secret
    if user_agent is not None:
        args['user_agent'] = user_agent
    if username is not None:
        args['username'] = username
    if password is not None:
        args['password'] = password
    if sleep is not None:
        args['sleep'] = sleep
    if limit is not None:
        args['limit'] = limit
    if timeout is not None:
        args['timeout'] = timeout
    
    # Output settings
    if outdir is not None:
        args['outdir'] = outdir
    if export_formats is not None:
        args['export_formats'] = export_formats
    if organize_by_date is not None:
        args['organize_by_date'] = organize_by_date
    if organize_by_author is not None:
        args['organize_by_author'] = organize_by_author
    if organize_by_subreddit is not None:
        args['organize_by_subreddit'] = organize_by_subreddit
    if filename_template is not None:
        args['filename_template'] = filename_template
    
    # Filter settings
    if min_score is not None:
        args['min_score'] = min_score
    if max_score is not None:
        args['max_score'] = max_score
    if include_nsfw is not None:
        args['include_nsfw'] = include_nsfw
    if nsfw_only is not None:
        args['nsfw_only'] = nsfw_only
    if nsfw_mode is not None:
        args['nsfw_mode'] = nsfw_mode
    if include_keywords is not None:
        args['include_keywords'] = include_keywords
    if exclude_keywords is not None:
        args['exclude_keywords'] = exclude_keywords
    if keyword_case_sensitive is not None:
        args['keyword_case_sensitive'] = keyword_case_sensitive
    if keyword_regex is not None:
        args['keyword_regex'] = keyword_regex
    if keyword_whole_words is not None:
        args['keyword_whole_words'] = keyword_whole_words
    if after_date is not None:
        args['after_date'] = after_date
    if before_date is not None:
        args['before_date'] = before_date
    if allowed_domains is not None:
        args['allowed_domains'] = allowed_domains
    if blocked_domains is not None:
        args['blocked_domains'] = blocked_domains
    if media_types is not None:
        args['media_types'] = media_types
    if exclude_media_types is not None:
        args['exclude_media_types'] = exclude_media_types
    if file_extensions is not None:
        args['file_extensions'] = file_extensions
    if exclude_file_extensions is not None:
        args['exclude_file_extensions'] = exclude_file_extensions
    if filter_composition is not None:
        args['filter_composition'] = filter_composition
    
    # Processing settings
    if embed_metadata is not None:
        args['embed_metadata'] = embed_metadata
    if create_json_sidecars is not None:
        args['create_json_sidecars'] = create_json_sidecars
    if concurrent_downloads is not None:
        args['concurrent_downloads'] = concurrent_downloads
    
    # Processing pipeline settings
    if enable_processing is not None:
        args['enable_processing'] = enable_processing
    if processing_profile is not None:
        args['processing_profile'] = processing_profile
    
    # Image processing settings
    if image_format is not None:
        args['image_format'] = image_format
    if image_quality is not None:
        args['image_quality'] = image_quality
    if max_image_resolution is not None:
        args['max_image_resolution'] = max_image_resolution
    
    # Video processing settings
    if video_format is not None:
        args['video_format'] = video_format
    if video_quality is not None:
        args['video_quality'] = video_quality
    if max_video_resolution is not None:
        args['max_video_resolution'] = max_video_resolution
    
    # Thumbnail processing settings
    if generate_thumbnails is not None:
        args['generate_thumbnails'] = generate_thumbnails
    if thumbnail_size is not None:
        args['thumbnail_size'] = thumbnail_size
    if thumbnail_timestamp is not None:
        args['thumbnail_timestamp'] = thumbnail_timestamp
    
    # UI & Progress settings
    if quiet is not None:
        args['quiet'] = quiet
    if output_mode is not None:
        args['output_mode'] = output_mode
    if progress_display is not None:
        args['progress_display'] = progress_display
    if no_progress is not None:
        args['no_progress'] = no_progress
    if json_output is not None:
        args['json_output'] = json_output
    if show_eta is not None:
        args['show_eta'] = show_eta
    if show_speed is not None:
        args['show_speed'] = show_speed
    if show_statistics is not None:
        args['show_statistics'] = show_statistics
    
    # Add any additional kwargs
    for key, value in kwargs.items():
        if value is not None:
            args[key] = value
    
    return args


def print_config_summary(config: AppConfig, target: Optional[str] = None) -> None:
    """
    Print a formatted summary of the current configuration.
    
    Args:
        config: AppConfig instance to summarize
        target: Optional target (user/subreddit) being processed
    """
    # Create summary table
    table = Table(title="Configuration Summary", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="dim", width=20)
    table.add_column("Value", style="white", width=40)
    
    # Core settings
    table.add_row("Mode", "API" if config.scraping.api_mode else "Public")
    table.add_row("Pipeline", "✓ Enabled" if config.use_pipeline else "✗ Disabled")
    table.add_row("Dry Run", "✓ Yes" if config.dry_run else "✗ No")
    
    if target:
        table.add_row("Target", target)
    
    # Scraping settings
    table.add_row("Post Limit", str(config.scraping.post_limit))
    table.add_row("Sleep Interval", f"{config.get_effective_sleep_interval():.1f}s")
    
    # Output settings
    table.add_row("Output Dir", str(config.output.output_dir))
    table.add_row("Export Formats", ", ".join(config.output.export_formats))
    
    # Filter summary
    filters = []
    if config.filters.min_score is not None:
        filters.append(f"score ≥ {config.filters.min_score}")
    if config.filters.max_score is not None:
        filters.append(f"score ≤ {config.filters.max_score}")
    if not config.filters.include_nsfw:
        filters.append("no NSFW")
    elif config.filters.nsfw_only:
        filters.append("NSFW only")
    if config.filters.keywords_include:
        filters.append(f"keywords: {', '.join(config.filters.keywords_include[:3])}{'...' if len(config.filters.keywords_include) > 3 else ''}")
    
    if filters:
        table.add_row("Filters", "; ".join(filters))
    else:
        table.add_row("Filters", "None")
    
    console.print(table)
    console.print()


def validate_api_credentials(config: AppConfig) -> None:
    """
    Validate API credentials if API mode is enabled.
    
    Args:
        config: AppConfig to validate
        
    Raises:
        typer.Exit: If API credentials are missing
    """
    import typer
    
    if config.scraping.api_mode:
        if not config.scraping.client_id or not config.scraping.client_secret:
            console.print(Panel(
                "[red]API mode requires both client ID and client secret.[/red]\n\n"
                "You can provide them via:\n"
                "• Command line: [cyan]--client-id ID --client-secret SECRET[/cyan]\n"
                "• Environment variables: [cyan]REDDITDL_CLIENT_ID, REDDITDL_CLIENT_SECRET[/cyan]\n"
                "• Configuration file: [cyan]scraping.client_id, scraping.client_secret[/cyan]",
                title="[red]Missing API Credentials[/red]",
                border_style="red"
            ))
            raise typer.Exit(1)


def convert_config_to_legacy(config: AppConfig, target_user: Optional[str] = None) -> Dict[str, Any]:
    """
    Convert AppConfig to legacy dictionary format for backward compatibility.
    
    This is used to interface with existing code that expects the old config format.
    
    Args:
        config: AppConfig instance to convert
        target_user: Optional target user for legacy compatibility
        
    Returns:
        Dictionary in legacy format
    """
    legacy_config = {
        # Core settings
        "dry_run": config.dry_run,
        "verbose": config.verbose,
        "use_pipeline": config.use_pipeline,
        
        # Scraping settings
        "api_mode": config.scraping.api_mode,
        "client_id": config.scraping.client_id,
        "client_secret": config.scraping.client_secret,
        "user_agent": config.scraping.user_agent,
        "username": config.scraping.username,
        "password": config.scraping.password,
        "sleep_interval": config.get_effective_sleep_interval(),
        "post_limit": config.scraping.post_limit,
        "timeout": config.scraping.timeout,
        "max_retries": config.scraping.max_retries,
        
        # Output settings
        "output_dir": str(config.output.output_dir),
        "export_formats": config.output.export_formats,
        "organize_by_date": config.output.organize_by_date,
        "organize_by_author": config.output.organize_by_author,
        "organize_by_subreddit": config.output.organize_by_subreddit,
        "filename_template": config.output.filename_template,
        
        # Processing settings
        "embed_metadata": config.processing.embed_metadata,
        "create_json_sidecars": config.processing.create_json_sidecars,
        "concurrent_downloads": config.processing.concurrent_downloads,
        "image_quality": config.processing.image_quality,
        
        # Filter settings
        "min_score": config.filters.min_score,
        "max_score": config.filters.max_score,
        "include_nsfw": config.filters.include_nsfw,
        "nsfw_only": config.filters.nsfw_only,
        "include_keywords": config.filters.keywords_include,
        "exclude_keywords": config.filters.keywords_exclude,
        "after_date": config.filters.date_from,
        "before_date": config.filters.date_to,
    }
    
    # Add target user if provided
    if target_user:
        legacy_config["target_user"] = target_user
    
    return legacy_config


def get_config_file_suggestions() -> list:
    """Get list of suggested configuration file locations."""
    from pathlib import Path
    
    suggestions = [
        Path.cwd() / "redditdl.yaml",
        Path.cwd() / ".redditdl.yaml", 
        Path.home() / ".redditdl" / "config.yaml",
        Path.home() / ".config" / "redditdl" / "config.yaml",
    ]
    
    # Add XDG config directory if available
    xdg_config = os.environ.get('XDG_CONFIG_HOME')
    if xdg_config:
        suggestions.append(Path(xdg_config) / "redditdl" / "config.yaml")
    
    return suggestions


def suggest_config_creation() -> None:
    """Suggest creating a configuration file if none exists."""
    suggestions = get_config_file_suggestions()
    existing_configs = [path for path in suggestions if path.exists()]
    
    if not existing_configs:
        console.print("\n[dim]💡 Tip: Create a configuration file to avoid repeating arguments:[/dim]")
        console.print(f"[dim]   cp config-templates/default.yaml redditdl.yaml[/dim]")
        console.print(f"[dim]   # Edit redditdl.yaml with your preferred settings[/dim]")
        console.print()


def create_cli_args_for_targets(
    targets: list = None,
    targets_file: Optional[str] = None,
    concurrent_targets: Optional[int] = None,
    listing_type: Optional[str] = None,
    time_period: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Create CLI arguments dictionary specifically for multi-target operations.
    
    Args:
        targets: List of targets to process
        targets_file: Path to file containing target list
        concurrent_targets: Number of concurrent targets to process
        listing_type: Subreddit listing type (hot, new, top, controversial, rising)
        time_period: Time period for top listings (hour, day, week, month, year, all)
        **kwargs: Additional CLI arguments
        
    Returns:
        Dictionary of CLI arguments including multi-target specific options
    """
    # Start with base CLI args
    args = build_cli_args(**kwargs)
    
    # Add multi-target specific arguments
    if targets is not None:
        args['targets'] = targets
    if targets_file is not None:
        args['targets_file'] = targets_file
    if concurrent_targets is not None:
        args['concurrent_targets'] = concurrent_targets
    if listing_type is not None:
        args['listing_type'] = listing_type
    if time_period is not None:
        args['time_period'] = time_period
    
    return args