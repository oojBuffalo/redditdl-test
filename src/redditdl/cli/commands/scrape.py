"""
Scrape Command

Main command for downloading media from Reddit users, subreddits, and URLs.
Uses the new Pydantic-based configuration system with ConfigManager.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional, List, Annotated
import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel

from redditdl.cli.config_utils import (
    load_config_from_cli,
    build_cli_args,
    print_config_summary,
    validate_api_credentials,
    convert_config_to_legacy,
    suggest_config_creation,
    setup_progress_observers
)

from redditdl.cli.utils import (
    validate_sleep_interval,
    validate_positive_int,
    print_header,
    handle_keyboard_interrupt,
    handle_fatal_error,
    console
)

# Import enhanced error handling
from redditdl.core.exceptions import (
    RedditDLError, NetworkError, ConfigurationError, AuthenticationError,
    ErrorCode, ErrorContext
)
from redditdl.core.error_context import report_error, get_error_analytics

# Configuration system
from redditdl.core.config import AppConfig

# Event system and observers
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.events.observers import ConsoleObserver, LoggingObserver, StatisticsObserver
from redditdl.cli.observers.progress import CLIProgressObserver, OutputMode, ProgressDisplay

# Import existing functionality
from redditdl.scrapers import PrawScraper, YarsScraper, PostMetadata
from redditdl.metadata import MetadataEmbedder
from redditdl.downloader import MediaDownloader
from redditdl.utils import sanitize_filename

# Create the scrape sub-application
app = typer.Typer(
    name="scrape",
    help="Download media from Reddit sources",
    rich_markup_mode="rich",
    no_args_is_help=True,
)


@app.command("user")
def scrape_user(
    username: Annotated[str, typer.Argument(help="Reddit username to scrape")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    dry_run: Annotated[Optional[bool], typer.Option("--dry-run", help="Execute without downloading files")] = None,
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    use_pipeline: Annotated[Optional[bool], typer.Option("--pipeline/--no-pipeline", help="Use modern pipeline architecture")] = None,
    
    # Scraping settings  
    limit: Annotated[Optional[int], typer.Option("--limit", "-l", callback=validate_positive_int, help="Maximum posts to process")] = -1,
    sleep: Annotated[Optional[float], typer.Option("--sleep", "-s", callback=validate_sleep_interval, help="Sleep interval between requests")] = -1.0,
    timeout: Annotated[Optional[int], typer.Option("--timeout", help="Request timeout in seconds")] = -1,
    
    # API settings
    api: Annotated[Optional[bool], typer.Option("--api", help="Use Reddit API mode (requires credentials)")] = None,
    client_id: Annotated[Optional[str], typer.Option("--client-id", help="Reddit API client ID")] = None,
    client_secret: Annotated[Optional[str], typer.Option("--client-secret", help="Reddit API client secret")] = None,
    user_agent: Annotated[Optional[str], typer.Option("--user-agent", help="User agent string")] = None,
    
    # Authentication
    username_login: Annotated[Optional[str], typer.Option("--username", help="Reddit username for login")] = None,
    password: Annotated[Optional[str], typer.Option("--password", help="Reddit password for login")] = None,
    
    # Output settings
    outdir: Annotated[Optional[str], typer.Option("--output", "-o", help="Output directory")] = None,
    export_formats: Annotated[Optional[List[str]], typer.Option("--export", help="Export formats (json, csv, markdown)")] = None,
    organize_by_date: Annotated[Optional[bool], typer.Option("--organize-by-date", help="Organize files by date")] = None,
    organize_by_author: Annotated[Optional[bool], typer.Option("--organize-by-author", help="Organize files by author")] = None,
    organize_by_subreddit: Annotated[Optional[bool], typer.Option("--organize-by-subreddit", help="Organize files by subreddit")] = None,
    filename_template: Annotated[Optional[str], typer.Option("--filename-template", help="Jinja2 filename template")] = None,
    
    # Filter settings
    min_score: Annotated[Optional[int], typer.Option("--min-score", help="Minimum post score")] = None,
    max_score: Annotated[Optional[int], typer.Option("--max-score", help="Maximum post score")] = None,
    
    # Date filters
    after_date: Annotated[Optional[str], typer.Option("--after-date", help="Include posts after this date (ISO format or relative)")] = None,
    before_date: Annotated[Optional[str], typer.Option("--before-date", help="Include posts before this date (ISO format or relative)")] = None,
    
    # Keyword filters
    include_keywords: Annotated[Optional[List[str]], typer.Option("--include-keyword", help="Include posts with these keywords")] = None,
    exclude_keywords: Annotated[Optional[List[str]], typer.Option("--exclude-keyword", help="Exclude posts with these keywords")] = None,
    keyword_case_sensitive: Annotated[Optional[bool], typer.Option("--keyword-case-sensitive", help="Use case-sensitive keyword matching")] = None,
    keyword_regex: Annotated[Optional[bool], typer.Option("--keyword-regex", help="Treat keywords as regular expressions")] = None,
    keyword_whole_words: Annotated[Optional[bool], typer.Option("--keyword-whole-words", help="Match whole words only")] = None,
    
    # Domain filters
    allowed_domains: Annotated[Optional[List[str]], typer.Option("--allow-domain", help="Allow only these domains")] = None,
    blocked_domains: Annotated[Optional[List[str]], typer.Option("--block-domain", help="Block these domains")] = None,
    
    # Media type filters
    media_types: Annotated[Optional[List[str]], typer.Option("--media-type", help="Include only these media types (image, video, gif, gallery, text, link)")] = None,
    exclude_media_types: Annotated[Optional[List[str]], typer.Option("--exclude-media-type", help="Exclude these media types")] = None,
    file_extensions: Annotated[Optional[List[str]], typer.Option("--extension", help="Include only these file extensions")] = None,
    exclude_file_extensions: Annotated[Optional[List[str]], typer.Option("--exclude-extension", help="Exclude these file extensions")] = None,
    
    # NSFW filters
    include_nsfw: Annotated[Optional[bool], typer.Option("--include-nsfw/--exclude-nsfw", help="Include NSFW content")] = None,
    nsfw_only: Annotated[Optional[bool], typer.Option("--nsfw-only", help="Include only NSFW content")] = None,
    nsfw_mode: Annotated[Optional[str], typer.Option("--nsfw-mode", help="NSFW mode: include, exclude, or only")] = None,
    
    # Filter composition
    filter_composition: Annotated[Optional[str], typer.Option("--filter-composition", help="How to combine filters: 'and' or 'or'")] = None,
    
    # Processing settings
    embed_metadata: Annotated[Optional[bool], typer.Option("--embed-metadata/--no-embed-metadata", help="Embed metadata in files")] = None,
    create_json_sidecars: Annotated[Optional[bool], typer.Option("--json-sidecars/--no-json-sidecars", help="Create JSON sidecar files")] = None,
    concurrent_downloads: Annotated[Optional[int], typer.Option("--concurrent", help="Maximum concurrent downloads")] = -1,
    
    # Processing pipeline settings
    enable_processing: Annotated[Optional[bool], typer.Option("--processing/--no-processing", help="Enable post-download processing")] = None,
    processing_profile: Annotated[Optional[str], typer.Option("--processing-profile", help="Processing profile (lossless, high, medium, low, custom)")] = None,
    
    # Image processing
    image_format: Annotated[Optional[str], typer.Option("--image-format", help="Target image format (jpeg, png, webp, bmp, tiff)")] = None,
    image_quality: Annotated[Optional[int], typer.Option("--image-quality", help="Image quality (1-100)")] = -1,
    max_image_resolution: Annotated[Optional[int], typer.Option("--max-image-resolution", help="Maximum image resolution (pixels)")] = -1,
    
    # Video processing
    video_format: Annotated[Optional[str], typer.Option("--video-format", help="Target video format (mp4, avi, mkv, webm, mov)")] = None,
    video_quality: Annotated[Optional[int], typer.Option("--video-quality", help="Video quality CRF (0-51, lower=better)")] = -1,
    max_video_resolution: Annotated[Optional[str], typer.Option("--max-video-resolution", help="Maximum video resolution (e.g., 1920x1080, 720p)")] = None,
    
    # Thumbnail processing
    generate_thumbnails: Annotated[Optional[bool], typer.Option("--thumbnails/--no-thumbnails", help="Generate thumbnails")] = None,
    thumbnail_size: Annotated[Optional[int], typer.Option("--thumbnail-size", help="Thumbnail size in pixels")] = -1,
    thumbnail_timestamp: Annotated[Optional[str], typer.Option("--thumbnail-timestamp", help="Video thumbnail timestamp (HH:MM:SS)")] = None,
    
    # UI & Progress Options
    quiet: Annotated[Optional[bool], typer.Option("--quiet", "-q", help="Minimal output mode")] = None,
    output_mode: Annotated[Optional[str], typer.Option("--output-mode", help="Output mode: normal, quiet, verbose, json")] = None,
    progress_display: Annotated[Optional[str], typer.Option("--progress", help="Progress display: rich, tqdm, simple, none")] = None,
    no_progress: Annotated[Optional[bool], typer.Option("--no-progress", help="Disable progress bars")] = None,
    json_output: Annotated[Optional[str], typer.Option("--json-output", help="JSON output file path")] = None,
    show_eta: Annotated[Optional[bool], typer.Option("--eta/--no-eta", help="Show estimated time of arrival")] = None,
    show_speed: Annotated[Optional[bool], typer.Option("--speed/--no-speed", help="Show download speeds")] = None,
    show_statistics: Annotated[Optional[bool], typer.Option("--stats/--no-stats", help="Show live statistics")] = None,
):
    """
    Download media from a Reddit user's profile.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Basic download: [green]redditdl scrape user johndoe[/green]
    • API mode: [green]redditdl scrape user johndoe --api --client-id ID --client-secret SECRET[/green]
    • Custom output: [green]redditdl scrape user johndoe -o ./my_downloads -l 50[/green]
    • With config file: [green]redditdl scrape user johndoe --config my-config.yaml[/green]
    • Dry run: [green]redditdl scrape user johndoe --dry-run --export json csv[/green]
    """
    try:
        # Build CLI arguments for configuration
        cli_args = build_cli_args(
            config=config,
            dry_run=dry_run,
            verbose=verbose,
            use_pipeline=use_pipeline,
            api=api,
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username_login,
            password=password,
            sleep=sleep,
            limit=limit,
            timeout=timeout,
            outdir=outdir,
            export_formats=export_formats,
            organize_by_date=organize_by_date,
            organize_by_author=organize_by_author,
            organize_by_subreddit=organize_by_subreddit,
            filename_template=filename_template,
            min_score=min_score,
            max_score=max_score,
            include_nsfw=include_nsfw,
            nsfw_only=nsfw_only,
            nsfw_mode=nsfw_mode,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            keyword_case_sensitive=keyword_case_sensitive,
            keyword_regex=keyword_regex,
            keyword_whole_words=keyword_whole_words,
            after_date=after_date,
            before_date=before_date,
            allowed_domains=allowed_domains,
            blocked_domains=blocked_domains,
            media_types=media_types,
            exclude_media_types=exclude_media_types,
            file_extensions=file_extensions,
            exclude_file_extensions=exclude_file_extensions,
            filter_composition=filter_composition,
            embed_metadata=embed_metadata,
            create_json_sidecars=create_json_sidecars,
            concurrent_downloads=concurrent_downloads,
            # Processing pipeline arguments
            enable_processing=enable_processing,
            processing_profile=processing_profile,
            # Image processing arguments
            image_format=image_format,
            image_quality=image_quality,
            max_image_resolution=max_image_resolution,
            # Video processing arguments
            video_format=video_format,
            video_quality=video_quality,
            max_video_resolution=max_video_resolution,
            # Thumbnail processing arguments
            generate_thumbnails=generate_thumbnails,
            thumbnail_size=thumbnail_size,
            thumbnail_timestamp=thumbnail_timestamp,
            # UI & Progress arguments
            quiet=quiet,
            output_mode=output_mode,
            progress_display=progress_display,
            no_progress=no_progress,
            json_output=json_output,
            show_eta=show_eta,
            show_speed=show_speed,
            show_statistics=show_statistics,
        )
        
        # Load configuration
        app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
        
        # Validate API credentials if needed
        validate_api_credentials(app_config)
        
        # Print header
        print_header("RedditDL - Reddit Media Downloader", f"Downloading from user: {username}")
        
        # Print configuration summary
        print_config_summary(app_config, target=f"u/{username}")
        
        # Set up progress observers and event system
        event_emitter = setup_progress_observers(app_config)
        
        # Execute scraping
        if app_config.use_pipeline:
            asyncio.run(_scrape_user_pipeline(app_config, username, event_emitter))
        else:
            _scrape_user_legacy(app_config, username)
        
        console.print("[bold green]✓ Scraping completed successfully![/bold green]")
        
        # Suggest configuration file if none exists
        suggest_config_creation()
        
    except RedditDLError as e:
        # Handle structured RedditDL errors with user-friendly messages
        error_context = ErrorContext(
            operation="cli_scrape_user",
            target=username
        )
        
        # Display user-friendly error message
        console.print(f"\n[red]❌ {e.get_user_message()}[/red]")
        
        # Show error analytics if available
        analytics = get_error_analytics()
        error_stats = analytics.get_error_statistics()
        if error_stats.get('total_errors', 0) > 1:
            console.print(f"\n[yellow]📊 Recent errors: {error_stats['total_errors']} total[/yellow]")
        
        # Log detailed error for debugging
        report_error(e, error_context)
        
        # Exit with error code
        raise typer.Exit(1)
    except Exception as e:
        # Handle unexpected errors with intelligent error categorization
        import traceback
        
        # Print full stack trace for debugging
        console.print("\n[red]FULL STACK TRACE:[/red]")
        traceback.print_exc()
        
        error_context = ErrorContext(
            operation="cli_scrape_user", 
            target=username
        )
        
        # Check if this looks like a known error type
        error_message = str(e)
        if "network" in error_message.lower() or "connection" in error_message.lower():
            console.print(f"\n[red]❌ Network Error: {error_message}[/red]")
            console.print("\n💡 Suggested solutions:")
            console.print("   1. Check your internet connection")
            console.print("   2. Try again in a few minutes")
            console.print("   3. Check if Reddit is experiencing issues")
        elif "authentication" in error_message.lower() or "credential" in error_message.lower():
            console.print(f"\n[red]❌ Authentication Error: {error_message}[/red]")
            console.print("\n💡 Suggested solutions:")
            console.print("   1. Check your Reddit API credentials")
            console.print("   2. Try running without --api flag (public mode)")
            console.print("   3. Verify credentials at https://www.reddit.com/prefs/apps")
        else:
            console.print(f"\n[red]❌ Unexpected Error: {error_message}[/red]")
            console.print("\n💡 Suggested solutions:")
            console.print("   1. Try running with --verbose for more details")
            console.print("   2. Check the configuration file")
            console.print("   3. Report this issue if it persists")
        
        raise e from e


@app.command("subreddit")
def scrape_subreddit(
    subreddit: Annotated[str, typer.Argument(help="Subreddit name to scrape")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    dry_run: Annotated[Optional[bool], typer.Option("--dry-run", help="Execute without downloading files")] = None,
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    use_pipeline: Annotated[Optional[bool], typer.Option("--pipeline/--no-pipeline", help="Use modern pipeline architecture")] = None,
    
    # Scraping settings
    limit: Annotated[Optional[int], typer.Option("--limit", "-l", callback=validate_positive_int, help="Maximum posts to process")] = -1,
    sleep: Annotated[Optional[float], typer.Option("--sleep", "-s", callback=validate_sleep_interval, help="Sleep interval between requests")] = -1.0,
    listing: Annotated[str, typer.Option("--listing", help="Subreddit listing type")] = "hot",
    time_filter: Annotated[str, typer.Option("--time", help="Time filter for top listings")] = "all",
    
    # API settings
    api: Annotated[Optional[bool], typer.Option("--api", help="Use Reddit API mode")] = None,
    client_id: Annotated[Optional[str], typer.Option("--client-id", help="Reddit API client ID")] = None,
    client_secret: Annotated[Optional[str], typer.Option("--client-secret", help="Reddit API client secret")] = None,
    
    # Output settings
    outdir: Annotated[Optional[str], typer.Option("--output", "-o", help="Output directory")] = None,
    export_formats: Annotated[Optional[List[str]], typer.Option("--export", help="Export formats")] = None,
):
    """
    Download media from a subreddit.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Hot posts: [green]redditdl scrape subreddit pics[/green]
    • Top posts: [green]redditdl scrape subreddit earthporn --listing top --time week[/green]
    • API mode: [green]redditdl scrape subreddit funny --api --client-id ID --client-secret SECRET[/green]
    """
    console.print(f"[yellow]Subreddit scraping will be implemented in Task 13[/yellow]")
    console.print(f"Requested: r/{subreddit} with listing '{listing}'")
    
    # Build CLI args and load config for future implementation
    cli_args = build_cli_args(
        config=config,
        dry_run=dry_run,
        verbose=verbose,
        use_pipeline=use_pipeline,
        api=api,
        client_id=client_id,
        client_secret=client_secret,
        sleep=sleep,
        limit=limit,
        outdir=outdir,
        export_formats=export_formats,
    )
    
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    print_config_summary(app_config, target=f"r/{subreddit}")
    
    # TODO: Implement subreddit scraping functionality in Task 13
    raise typer.Exit(0)


@app.command("url")
def scrape_url(
    url: Annotated[str, typer.Argument(help="Reddit URL to scrape")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    dry_run: Annotated[Optional[bool], typer.Option("--dry-run", help="Execute without downloading files")] = None,
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    
    # Output settings
    outdir: Annotated[Optional[str], typer.Option("--output", "-o", help="Output directory")] = None,
):
    """
    Download media from a specific Reddit URL.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Single post: [green]redditdl scrape url https://reddit.com/r/pics/comments/abc123/title[/green]
    • Comment thread: [green]redditdl scrape url https://reddit.com/r/askreddit/comments/def456[/green]
    """
    console.print(f"[yellow]URL scraping will be implemented in Task 13[/yellow]")
    console.print(f"Requested URL: {url}")
    
    # Build CLI args and load config for future implementation
    cli_args = build_cli_args(
        config=config,
        dry_run=dry_run,
        verbose=verbose,
        outdir=outdir,
    )
    
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    print_config_summary(app_config, target=url)
    
    # TODO: Implement URL scraping functionality in Task 13
    raise typer.Exit(0)


async def _scrape_user_pipeline(config: AppConfig, username: str, event_emitter: EventEmitter) -> None:
    """
    Execute user scraping using the pipeline architecture.
    
    Args:
        config: Application configuration
        username: Reddit username to scrape
        event_emitter: Event emitter for progress tracking
    """
    try:
        # Import pipeline components
        from redditdl.core.pipeline.interfaces import PipelineContext
        from redditdl.core.pipeline.executor import PipelineExecutor
        from redditdl.pipeline.stages.acquisition import AcquisitionStage
        from redditdl.pipeline.stages.filter import FilterStage
        from redditdl.pipeline.stages.processing import ProcessingStage
        from redditdl.pipeline.stages.organization import OrganizationStage
        from redditdl.pipeline.stages.export import ExportStage
        
        # Create pipeline context
        context = PipelineContext()
        
        # Initialize event system
        try:
            from redditdl.core.events.emitter import EventEmitter
            from redditdl.core.events.observers import ConsoleObserver, StatisticsObserver
            
            event_emitter = EventEmitter()
            context.events = event_emitter
            
            # Add default observers
            console_observer = ConsoleObserver(
                verbose=config.verbose,
                use_rich=True,
                show_timestamps=False
            )
            stats_observer = StatisticsObserver()
            
            event_emitter.subscribe('*', console_observer, weak=False)
            event_emitter.subscribe('*', stats_observer, weak=False)
            
        except ImportError as e:
            console.print(f"[yellow]Event system not available: {e}[/yellow]")
            context.events = None
        
        # Configure pipeline context from AppConfig
        context.set_config("api_mode", config.scraping.api_mode)
        context.set_config("target_user", username)
        context.set_config("post_limit", config.scraping.post_limit)
        context.set_config("sleep_interval", config.get_effective_sleep_interval())
        context.set_config("output_dir", str(config.output.output_dir))
        context.set_config("export_formats", config.output.export_formats)
        context.set_config("dry_run", config.dry_run)
        
        # API authentication configuration
        if config.scraping.api_mode:
            context.set_config("client_id", config.scraping.client_id)
            context.set_config("client_secret", config.scraping.client_secret)
            context.set_config("user_agent", config.scraping.user_agent)
            if config.scraping.username and config.scraping.password:
                context.set_config("username", config.scraping.username)
                context.set_config("password", config.scraping.password)
        
        # Create pipeline executor
        executor = PipelineExecutor(error_handling="continue")
        
        # Configure pipeline stages
        acquisition_config = {
            "api_mode": config.scraping.api_mode,
            "target_user": username,
            "post_limit": config.scraping.post_limit,
            "sleep_interval": config.get_effective_sleep_interval()
        }
        
        if config.scraping.api_mode:
            acquisition_config.update({
                "client_id": config.scraping.client_id,
                "client_secret": config.scraping.client_secret,
                "user_agent": config.scraping.user_agent
            })
            if config.scraping.username and config.scraping.password:
                acquisition_config.update({
                    "username": config.scraping.username,
                    "password": config.scraping.password
                })
        
        processing_config = {
            "output_dir": str(config.output.output_dir),
            "sleep_interval": config.get_effective_sleep_interval(),
            "dry_run": config.dry_run,
            "embed_metadata": config.processing.embed_metadata,
            "create_json_sidecars": config.processing.create_json_sidecars,
            "concurrent_downloads": config.processing.concurrent_downloads
        }
        
        export_config = {
            "export_formats": config.output.export_formats,
            "export_dir": config.get_export_dir(),
            "include_metadata": True,
            "include_posts": True
        }
        
        # Add pipeline stages
        executor.add_stage(AcquisitionStage(acquisition_config))
        executor.add_stage(FilterStage(config.filters.model_dump()))  # Pass filter config
        
        # Skip processing stage in dry-run mode
        if not config.dry_run:
            executor.add_stage(ProcessingStage(processing_config))
        else:
            console.print("[yellow]Dry-run mode: Skipping content processing/download[/yellow]")
        
        executor.add_stage(OrganizationStage())  # Placeholder for now
        executor.add_stage(ExportStage(export_config))
        
        # Execute pipeline
        console.print("[cyan]Starting pipeline execution...[/cyan]")
        
        metrics = await executor.execute(context)
        
        # Log execution results
        console.print("[bold green]Pipeline execution completed![/bold green]")
        console.print(f"Total execution time: {metrics.total_execution_time:.2f}s")
        console.print(f"Successful stages: {metrics.successful_stages}/{metrics.total_stages}")
        console.print(f"Total posts processed: {len(context.posts)}")
        
        if metrics.failed_stages > 0:
            console.print(f"[yellow]Pipeline completed with {metrics.failed_stages} failed stages[/yellow]")
            for result in executor.get_stage_results():
                if not result.success:
                    console.print(f"[red]Stage '{result.stage_name}' failed: {result.errors}[/red]")
        
    except Exception as e:
        console.print(f"[red]Pipeline execution failed: {e}[/red]")
        raise


@app.command("targets")
def scrape_targets(
    targets: Annotated[Optional[List[str]], typer.Argument(help="Mixed targets (users, subreddits, URLs)")] = None,
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Multi-target settings
    targets_file: Annotated[Optional[str], typer.Option("--targets-file", "-f", help="File containing targets (one per line)")] = None,
    concurrent_targets: Annotated[Optional[int], typer.Option("--concurrent-targets", help="Maximum concurrent target processing")] = None,
    
    # Core settings
    dry_run: Annotated[Optional[bool], typer.Option("--dry-run", help="Execute without downloading files")] = None,
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    use_pipeline: Annotated[Optional[bool], typer.Option("--pipeline/--no-pipeline", help="Use modern pipeline architecture")] = None,
    
    # Scraping settings  
    limit: Annotated[Optional[int], typer.Option("--limit", "-l", callback=validate_positive_int, help="Maximum posts to process per target")] = -1,
    sleep: Annotated[Optional[float], typer.Option("--sleep", "-s", callback=validate_sleep_interval, help="Sleep interval between requests")] = -1.0,
    timeout: Annotated[Optional[int], typer.Option("--timeout", help="Request timeout in seconds")] = -1,
    
    # Subreddit listing options
    listing_type: Annotated[Optional[str], typer.Option("--listing", help="Default subreddit listing type (hot, new, top, controversial, rising)")] = None,
    time_period: Annotated[Optional[str], typer.Option("--time-period", help="Time period for top/controversial listings (hour, day, week, month, year, all)")] = None,
    
    # API settings
    api: Annotated[Optional[bool], typer.Option("--api", help="Use Reddit API mode (requires credentials)")] = None,
    client_id: Annotated[Optional[str], typer.Option("--client-id", help="Reddit API client ID")] = None,
    client_secret: Annotated[Optional[str], typer.Option("--client-secret", help="Reddit API client secret")] = None,
    user_agent: Annotated[Optional[str], typer.Option("--user-agent", help="User agent string")] = None,
    
    # Authentication
    username_login: Annotated[Optional[str], typer.Option("--username", help="Reddit username for login")] = None,
    password: Annotated[Optional[str], typer.Option("--password", help="Reddit password for login")] = None,
    
    # Output settings
    outdir: Annotated[Optional[str], typer.Option("--output", "-o", help="Output directory")] = None,
    export_formats: Annotated[Optional[List[str]], typer.Option("--export", help="Export formats (json, csv, sqlite, markdown)")] = None,
    organize_by_date: Annotated[Optional[bool], typer.Option("--organize-by-date", help="Organize files by date")] = None,
    organize_by_author: Annotated[Optional[bool], typer.Option("--organize-by-author", help="Organize files by author")] = None,
    organize_by_subreddit: Annotated[Optional[bool], typer.Option("--organize-by-subreddit", help="Organize files by subreddit")] = None,
    filename_template: Annotated[Optional[str], typer.Option("--filename-template", help="Jinja2 filename template")] = None,
    
    # Filter settings
    min_score: Annotated[Optional[int], typer.Option("--min-score", help="Minimum post score")] = None,
    max_score: Annotated[Optional[int], typer.Option("--max-score", help="Maximum post score")] = None,
    
    # Date filters
    after_date: Annotated[Optional[str], typer.Option("--after-date", help="Include posts after this date (ISO format or relative)")] = None,
    before_date: Annotated[Optional[str], typer.Option("--before-date", help="Include posts before this date (ISO format or relative)")] = None,
    
    # Keyword filters
    include_keywords: Annotated[Optional[List[str]], typer.Option("--include-keyword", help="Include posts with these keywords")] = None,
    exclude_keywords: Annotated[Optional[List[str]], typer.Option("--exclude-keyword", help="Exclude posts with these keywords")] = None,
    keyword_case_sensitive: Annotated[Optional[bool], typer.Option("--keyword-case-sensitive", help="Use case-sensitive keyword matching")] = None,
    keyword_regex: Annotated[Optional[bool], typer.Option("--keyword-regex", help="Treat keywords as regular expressions")] = None,
    keyword_whole_words: Annotated[Optional[bool], typer.Option("--keyword-whole-words", help="Match whole words only")] = None,
    
    # Domain filters
    allowed_domains: Annotated[Optional[List[str]], typer.Option("--allow-domain", help="Allow only these domains")] = None,
    blocked_domains: Annotated[Optional[List[str]], typer.Option("--block-domain", help="Block these domains")] = None,
    
    # Media type filters
    media_types: Annotated[Optional[List[str]], typer.Option("--media-type", help="Include only these media types (image, video, gif, gallery, text, link)")] = None,
    exclude_media_types: Annotated[Optional[List[str]], typer.Option("--exclude-media-type", help="Exclude these media types")] = None,
    file_extensions: Annotated[Optional[List[str]], typer.Option("--extension", help="Include only these file extensions")] = None,
    exclude_file_extensions: Annotated[Optional[List[str]], typer.Option("--exclude-extension", help="Exclude these file extensions")] = None,
    
    # NSFW filters
    include_nsfw: Annotated[Optional[bool], typer.Option("--include-nsfw/--exclude-nsfw", help="Include NSFW content")] = None,
    nsfw_only: Annotated[Optional[bool], typer.Option("--nsfw-only", help="Include only NSFW content")] = None,
    nsfw_mode: Annotated[Optional[str], typer.Option("--nsfw-mode", help="NSFW mode: include, exclude, or only")] = None,
    
    # Filter composition
    filter_composition: Annotated[Optional[str], typer.Option("--filter-composition", help="How to combine filters: 'and' or 'or'")] = None,
    
    # Processing settings
    embed_metadata: Annotated[Optional[bool], typer.Option("--embed-metadata/--no-embed-metadata", help="Embed metadata in files")] = None,
    create_json_sidecars: Annotated[Optional[bool], typer.Option("--json-sidecars/--no-json-sidecars", help="Create JSON sidecar files")] = None,
    concurrent_downloads: Annotated[Optional[int], typer.Option("--concurrent", help="Maximum concurrent downloads")] = -1,
    image_quality: Annotated[Optional[int], typer.Option("--image-quality", help="Image quality (1-100)")] = -1,
):
    """
    Download media from multiple Reddit targets (users, subreddits, URLs, saved/upvoted posts).
    
    This command supports advanced multi-target processing with concurrent execution,
    file-based target loading, and enhanced subreddit listing options.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Mixed targets: [green]redditdl scrape targets johndoe r/pics saved[/green]
    • From file: [green]redditdl scrape targets --targets-file my-targets.txt[/green]
    • Concurrent processing: [green]redditdl scrape targets user1 user2 r/sub1 r/sub2 --concurrent-targets 4[/green]
    • Subreddit top posts: [green]redditdl scrape targets r/earthporn r/pics --listing top --time-period week[/green]
    • With authentication: [green]redditdl scrape targets saved upvoted --api --client-id ID --client-secret SECRET --username USER --password PASS[/green]
    • Dry run with exports: [green]redditdl scrape targets johndoe r/funny --dry-run --export json csv sqlite[/green]
    """
    try:
        # Validate inputs
        if not targets and not targets_file:
            rprint("[red]Error: You must specify either targets as arguments or --targets-file[/red]")
            raise typer.Exit(1)
        
        # Build CLI arguments for configuration using multi-target helper
        cli_args = create_cli_args_for_targets(
            targets=targets or [],
            targets_file=targets_file,
            concurrent_targets=concurrent_targets,
            listing_type=listing_type,
            time_period=time_period,
            config=config,
            dry_run=dry_run,
            verbose=verbose,
            use_pipeline=True,  # Force pipeline mode for multi-target
            api=api,
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username_login,
            password=password,
            outdir=outdir,
            export_formats=export_formats,
            organize_by_date=organize_by_date,
            organize_by_author=organize_by_author,
            organize_by_subreddit=organize_by_subreddit,
            filename_template=filename_template,
            limit=limit,
            sleep=sleep,
            timeout=timeout,
            min_score=min_score,
            max_score=max_score,
            after_date=after_date,
            before_date=before_date,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            keyword_case_sensitive=keyword_case_sensitive,
            keyword_regex=keyword_regex,
            keyword_whole_words=keyword_whole_words,
            allowed_domains=allowed_domains,
            blocked_domains=blocked_domains,
            media_types=media_types,
            exclude_media_types=exclude_media_types,
            file_extensions=file_extensions,
            exclude_file_extensions=exclude_file_extensions,
            include_nsfw=include_nsfw,
            nsfw_only=nsfw_only,
            nsfw_mode=nsfw_mode,
            filter_composition=filter_composition,
            embed_metadata=embed_metadata,
            create_json_sidecars=create_json_sidecars,
            concurrent_downloads=concurrent_downloads,
            image_quality=image_quality,
        )
        
        # Load and validate configuration
        app_config = load_config_from_cli(config, cli_args)
        
        # Validate API credentials if in API mode
        validate_api_credentials(app_config)
        
        # Print configuration summary
        print_config_summary(app_config)
        
        # Print header
        print_header("Multi-Target Reddit Scraper")
        
        # Ensure pipeline mode is used for multi-target processing
        if not app_config.use_pipeline:
            console.print("[yellow]Forcing pipeline mode for multi-target processing[/yellow]")
            app_config.use_pipeline = True
        
        # Execute multi-target scraping using enhanced pipeline
        asyncio.run(_scrape_targets_pipeline(app_config, targets or [], targets_file))
        
        console.print("[bold green]✓ Multi-target scraping completed successfully![/bold green]")
        
        # Suggest configuration file if none exists
        suggest_config_creation()
        
    except KeyboardInterrupt:
        handle_keyboard_interrupt()
    except Exception as e:
        handle_fatal_error(e)
        raise e from e


def _scrape_user_legacy(config: AppConfig, username: str) -> None:
    """
    Execute user scraping using legacy direct processing.
    
    Args:
        config: Application configuration
        username: Reddit username to scrape
    """
    console.print("[cyan]Using legacy processing mode[/cyan]")
    
    if config.dry_run:
        console.print("[yellow]--dry-run option is only supported in pipeline mode[/yellow]")
    if config.output.export_formats and len(config.output.export_formats) > 1:
        console.print("[yellow]--export-formats option is only supported in pipeline mode[/yellow]")
    
    # Initialize components based on configuration
    embedder = MetadataEmbedder()
    outdir = config.output.output_dir
    downloader = MediaDownloader(outdir, config.get_effective_sleep_interval(), embedder)
    
    # Select and configure scraper based on mode
    if config.scraping.api_mode:
        console.print("[cyan]Using Reddit API mode (PRAW)[/cyan]")
        scraper = PrawScraper(
            client_id=config.scraping.client_id,
            client_secret=config.scraping.client_secret,
            user_agent=config.scraping.user_agent,
            login_username=config.scraping.username,
            login_password=config.scraping.password,
            sleep_interval=config.get_effective_sleep_interval()
        )
    else:
        console.print("[cyan]Using non-API mode (YARS)[/cyan]")
        scraper = YarsScraper(sleep_interval=config.get_effective_sleep_interval())
    
    # Fetch user posts
    console.print(f"[cyan]Fetching posts from u/{username}...[/cyan]")
    posts = scraper.fetch_user_posts(username, config.scraping.post_limit)
    
    if not posts:
        console.print("[yellow]No posts found or accessible for this user[/yellow]")
        return
    
    console.print(f"[green]Found {len(posts)} posts[/green]")
    
    # Process posts and download media
    _process_posts_legacy(posts, downloader)


def _process_posts_legacy(posts: List[PostMetadata], downloader: MediaDownloader) -> None:
    """
    Process a list of posts and download their media (legacy approach).
    
    Args:
        posts: List of PostMetadata objects to process
        downloader: MediaDownloader instance for downloading files
    """
    from urllib.parse import urlparse
    
    successful_downloads = 0
    total_posts = len(posts)
    
    console.print(f"[cyan]Processing {total_posts} posts...[/cyan]")
    
    for i, post in enumerate(posts, 1):
        console.print(f"[{i}/{total_posts}] Processing post {post.id}: {post.title[:50]}...")
        
        # Determine the media URL to use
        media_url = post.media_url or post.url
        
        if not media_url:
            console.print(f"[yellow]Skipping post {post.id} - no media URL found[/yellow]")
            continue
        
        # Skip text posts and external links that aren't media
        if not _is_media_url(media_url):
            console.print(f"[dim]Skipping post {post.id} - not a media URL: {media_url}[/dim]")
            continue
        
        try:
            # Construct filename
            filename = _construct_filename(post, media_url)
            
            # Download the media file
            output_path = downloader.download(media_url, filename, post.to_dict())
            
            if output_path.exists():
                console.print(f"[green]✓ Saved: {output_path.name}[/green]")
                successful_downloads += 1
            else:
                console.print(f"[red]✗ Failed to save: {filename}[/red]")
                
        except Exception as e:
            console.print(f"[red]✗ Error processing post {post.id}: {e}[/red]")
    
    console.print(f"[bold green]Download complete: {successful_downloads}/{total_posts} files saved successfully[/bold green]")


def _construct_filename(post: PostMetadata, media_url: str) -> str:
    """
    Construct a safe filename for the downloaded media.
    
    Args:
        post: PostMetadata object containing post information
        media_url: URL of the media to download
        
    Returns:
        Safe filename string
    """
    from urllib.parse import urlparse
    
    # Parse URL to get potential file extension
    parsed_url = urlparse(media_url)
    url_path = parsed_url.path
    
    # Get extension from URL if available
    extension = ""
    if url_path and '.' in url_path:
        extension = Path(url_path).suffix.lower()
    
    # If no extension, try to guess from common patterns
    if not extension:
        if 'i.redd.it' in media_url or 'imgur.com' in media_url:
            extension = '.jpg'  # Default for image hosts
        elif 'v.redd.it' in media_url:
            extension = '.mp4'  # Default for Reddit videos
    
    # Create base filename using date, post ID, and sanitized title
    title_part = post.title[:50] if post.title else "untitled"
    base_filename = f"{post.date_iso}_{post.id}_{title_part}"
    
    # Sanitize and add extension
    safe_base = sanitize_filename(base_filename)
    return f"{safe_base}{extension}"


def _is_media_url(url: str) -> bool:
    """
    Determine if a URL points to downloadable media.
    
    Args:
        url: URL to check
        
    Returns:
        True if URL appears to be media, False otherwise
    """
    if not url:
        return False
    
    url_lower = url.lower()
    
    # Known media hosts
    media_hosts = [
        'i.redd.it',
        'v.redd.it', 
        'imgur.com',
        'i.imgur.com',
        'gfycat.com',
        'redgifs.com'
    ]
    
    # Check if URL is from a known media host
    for host in media_hosts:
        if host in url_lower:
            return True
    
    # Check for common media file extensions
    media_extensions = [
        '.jpg', '.jpeg', '.png', '.gif', '.webp',
        '.mp4', '.webm', '.mov', '.avi', '.mkv'
    ]
    
    for ext in media_extensions:
        if url_lower.endswith(ext):
            return True
    
    return False


async def _scrape_targets_pipeline(config: AppConfig, targets: List[str], targets_file: Optional[str]) -> None:
    """
    Execute multi-target scraping using the enhanced pipeline architecture.
    
    Args:
        config: Application configuration
        targets: List of targets to scrape (users, subreddits, URLs)
        targets_file: Optional path to file containing target list
    """
    from redditdl.core.pipeline.executor import PipelineExecutor
    from redditdl.core.pipeline.interfaces import PipelineContext
    from redditdl.pipeline.stages.acquisition import AcquisitionStage
    from redditdl.pipeline.stages.filter import FilterStage
    from redditdl.pipeline.stages.processing import ProcessingStage
    from redditdl.pipeline.stages.organization import OrganizationStage
    from redditdl.pipeline.stages.export import ExportStage
    from redditdl.core.events.emitter import EventEmitter
    from redditdl.core.state.manager import StateManager
    from redditdl.cli.config_utils import create_cli_args_for_targets
    
    console.print("[cyan]Initializing multi-target pipeline...[/cyan]")
    
    try:
        # Create event emitter and state manager
        event_emitter = EventEmitter()
        state_manager = StateManager(config.session_file or "session.db")
        
        # Create and configure pipeline executor
        executor = PipelineExecutor(event_emitter)
        
        # Add pipeline stages
        acquisition_stage = AcquisitionStage()
        filter_stage = FilterStage()
        processing_stage = ProcessingStage()
        organization_stage = OrganizationStage()
        export_stage = ExportStage()
        
        executor.add_stage(acquisition_stage)
        executor.add_stage(filter_stage)
        executor.add_stage(processing_stage)
        executor.add_stage(organization_stage)
        executor.add_stage(export_stage)
        
        # Create pipeline context with multi-target configuration
        context = PipelineContext(
            posts=[],  # Will be populated by acquisition stage
            config=config,
            session=state_manager,
            events=event_emitter,
            targets=targets,
            targets_file=targets_file
        )
        
        # Execute the complete pipeline
        console.print("[cyan]Starting multi-target pipeline execution...[/cyan]")
        result = await executor.execute(context)
        
        # Report results
        if result.success:
            console.print(f"[bold green]✓ Pipeline completed successfully![/bold green]")
            console.print(f"[green]Posts processed: {len(result.data.get('processed_posts', []))}[/green]")
            console.print(f"[green]Targets processed: {len(result.data.get('processed_targets', []))}[/green]")
        else:
            console.print(f"[bold red]✗ Pipeline failed: {result.error}[/bold red]")
            
    except Exception as e:
        console.print(f"[bold red]✗ Fatal error in multi-target pipeline: {e}[/bold red]")
        raise