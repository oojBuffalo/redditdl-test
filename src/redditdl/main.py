#!/usr/bin/env python3
"""
RedditDL - Reddit Media Downloader
Main module with CLI argument parsing and orchestration.

This module supports both the modern Typer-based CLI and legacy argparse
for backward compatibility. The new CLI provides enhanced user experience
with rich formatting, multi-command structure, and better error messages.
"""

import argparse
import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

# Legacy imports
from redditdl.scrapers import PrawScraper, YarsScraper, PostMetadata
from redditdl.metadata import MetadataEmbedder
from redditdl.downloader import MediaDownloader
from redditdl.utils import sanitize_filename

# New pipeline imports
from redditdl.core.pipeline.interfaces import PipelineContext
from redditdl.core.pipeline.executor import PipelineExecutor
from redditdl.pipeline.stages.acquisition import AcquisitionStage
from redditdl.pipeline.stages.filter import FilterStage
from redditdl.pipeline.stages.processing import ProcessingStage
from redditdl.pipeline.stages.organization import OrganizationStage
from redditdl.pipeline.stages.export import ExportStage

# New CLI imports
try:
    from redditdl.cli.main import app as typer_app
    TYPER_AVAILABLE = True
except ImportError:
    TYPER_AVAILABLE = False

# Configuration system imports
try:
    from redditdl.core.config import AppConfig
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False


def load_credentials_from_dotenv(dotenv_path_str: str = ".env") -> None:
    """
    Loads key-value pairs from a .env file into os.environ.
    Does not override existing environment variables.
    Logs the process.
    """
    dotenv_path = Path(dotenv_path_str)
    if dotenv_path.is_file(): # More specific check than exists()
        logging.info(f"Attempting to load credentials from {dotenv_path.resolve()}")
        try:
            with open(dotenv_path, 'r') as f:
                lines_read = 0
                vars_loaded = 0
                for line_number, line in enumerate(f, 1):
                    lines_read +=1
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip("'\"") # Strip potential quotes

                    if not key: # Skip if key is empty after strip
                        logging.warning(f"Skipping line {line_number} in {dotenv_path.name}: empty key")
                        continue

                    if key not in os.environ:
                        os.environ[key] = value
                        vars_loaded += 1
                        logging.debug(f"Loaded '{key}' from {dotenv_path.name}")
                    else:
                        logging.debug(f"'{key}' already set in environment, not overridden by {dotenv_path.name}")
            if lines_read > 0:
                logging.info(f"Processed {dotenv_path.name}: {vars_loaded} new variable(s) loaded into environment.")
            else:
                logging.info(f"{dotenv_path.name} is empty.")
        except IOError as e:
            logging.warning(f"Could not read {dotenv_path.name}: {e}")
    else:
        logging.info(f"{dotenv_path.resolve()} not found or is not a file. Relying on existing environment variables or CLI arguments.")


def setup_logging() -> None:
    """Set up logging configuration for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for RedditDL.
    
    Returns:
        argparse.Namespace: Parsed arguments with validation applied.
        
    Raises:
        SystemExit: If arguments are invalid or validation fails.
    """
    parser = argparse.ArgumentParser(
        prog="redditdl",
        description="Download media from Reddit user profiles with metadata embedding",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python main.py --user <username_to_scrape>
  uv run python main.py --user <username_to_scrape> --api --client_id <your_client_id> --client_secret <your_client_secret>
  uv run python main.py --user <username_to_scrape> --login --username <your_login_username> --password <your_login_password>
  uv run python main.py --user <username_to_scrape> [--outdir <directory>] [--limit <number>]
        """
    )

    # Required arguments
    parser.add_argument(
        "--user",
        type=str,
        required=True,
        help="Reddit username to scrape (required)"
    )
    
    # Output and behavior arguments
    parser.add_argument(
        "--outdir",
        type=str,
        default="downloads",
        help="Output directory for downloaded media (default: downloads)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of posts to process (default: 20)"
    )
    
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Sleep interval between requests in seconds (default: 1.0)"
    )
    
    parser.add_argument(
        "--use-pipeline",
        action="store_true",
        help="Use new pipeline architecture (experimental)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute pipeline without downloading files (pipeline mode only)"
    )
    
    parser.add_argument(
        "--export-formats",
        nargs="*",
        default=[],
        help="Export formats to generate (json, csv, markdown)"
    )
    
    # Create mutually exclusive group for authentication modes
    auth_group = parser.add_mutually_exclusive_group()
    auth_group.add_argument(
        "--api",
        action="store_true",
        help="Use Reddit API mode (requires client_id and client_secret)"
    )
    
    auth_group.add_argument(
        "--login",
        action="store_true", 
        help="Use authenticated mode (requires username and password)"
    )
    
    # API credentials group
    api_group = parser.add_argument_group('API credentials', 'Reddit API authentication options')
    api_group.add_argument(
        "--client-id",
        type=str,
        default=os.getenv("REDDIT_CLIENT_ID"),
        help="Reddit API client ID (default: from REDDIT_CLIENT_ID env var)"
    )
    
    api_group.add_argument(
        "--client-secret",
        type=str,
        default=os.getenv("REDDIT_CLIENT_SECRET"),
        help="Reddit API client secret (default: from REDDIT_CLIENT_SECRET env var)"
    )
    
    api_group.add_argument(
        "--user-agent",
        type=str,
        default=os.getenv("REDDIT_USER_AGENT", "RedditDL/2.0 by u/redditdl"),
        help="User agent string for Reddit API (default: RedditDL/2.0 by u/redditdl)"
    )
    
    # Login credentials group
    login_group = parser.add_argument_group('Login credentials', 'Reddit username/password authentication options')
    login_group.add_argument(
        "--username",
        type=str,
        default=os.getenv("REDDIT_USERNAME"),
        help="Reddit username for authentication (default: from REDDIT_USERNAME env var)"
    )
    
    login_group.add_argument(
        "--password",
        type=str,
        default=os.getenv("REDDIT_PASSWORD"),
        help="Reddit password for authentication (default: from REDDIT_PASSWORD env var)"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validation logic
    _validate_arguments(args)
    
    return args


def _validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate parsed arguments for consistency and completeness.
    
    Args:
        args: Parsed arguments from argparse
        
    Raises:
        SystemExit: If validation fails
    """
    # Validate sleep interval
    if args.sleep < 0:
        print("Error: --sleep must be a positive number", file=sys.stderr)
        sys.exit(1)
    
    # Validate API mode credentials
    if args.api:
        client_id = getattr(args, 'client_id', None) or getattr(args, 'client-id', None)
        client_secret = getattr(args, 'client_secret', None) or getattr(args, 'client-secret', None)
        if not client_id or not client_secret:
            print(
                "Error: --api mode requires both --client-id and --client-secret",
                file=sys.stderr
            )
            sys.exit(1)
    
    # Validate login mode credentials  
    if args.login:
        if not args.username or not args.password:
            print(
                "Error: --login mode requires both --username and --password",
                file=sys.stderr
            )
            sys.exit(1)
    
    # Note: Mutual exclusivity of --api and --login is now handled by argparse
    # so we no longer need to check for this manually


def construct_filename(post: PostMetadata, media_url: str) -> str:
    """
    Construct a safe filename for the downloaded media.
    
    Args:
        post: PostMetadata object containing post information
        media_url: URL of the media to download
        
    Returns:
        Safe filename string
    """
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


def process_posts(posts: list[PostMetadata], downloader: MediaDownloader) -> None:
    """
    Process a list of posts and download their media.
    
    Args:
        posts: List of PostMetadata objects to process
        downloader: MediaDownloader instance for downloading files
    """
    successful_downloads = 0
    total_posts = len(posts)
    
    logging.info(f"Processing {total_posts} posts...")
    
    for i, post in enumerate(posts, 1):
        logging.info(f"[{i}/{total_posts}] Processing post {post.id}: {post.title[:50]}...")
        
        # Determine the media URL to use
        media_url = post.media_url or post.url
        
        if not media_url:
            logging.warning(f"Skipping post {post.id} - no media URL found")
            continue
        
        # Skip text posts and external links that aren't media
        if not _is_media_url(media_url):
            logging.info(f"Skipping post {post.id} - not a media URL: {media_url}")
            continue
        
        try:
            # Construct filename
            filename = construct_filename(post, media_url)
            
            # Download the media file
            output_path = downloader.download(media_url, filename, post.to_dict())
            
            if output_path.exists():
                logging.info(f"✓ Saved: {output_path.name}")
                successful_downloads += 1
            else:
                logging.warning(f"✗ Failed to save: {filename}")
                
        except Exception as e:
            logging.error(f"✗ Error processing post {post.id}: {e}")
    
    logging.info(f"Download complete: {successful_downloads}/{total_posts} files saved successfully")


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


async def process_posts_pipeline_config(config: 'AppConfig', target_user: str) -> None:
    """
    Process posts using the pipeline architecture with AppConfig.
    
    This function creates and executes a processing pipeline using the modern
    Pydantic-based configuration system with SQLite state management.
    
    Args:
        config: Application configuration
        target_user: Reddit username to scrape
    """
    try:
        # Create pipeline context with configuration
        context = PipelineContext()
        
        # Initialize state management
        try:
            from redditdl.core.state.manager import StateManager
            from redditdl.core.state.recovery import SessionRecovery
            
            state_manager = StateManager(config.session_dir / "state.db")
            context.state_manager = state_manager
            
            # Create session
            session_id = state_manager.create_session(
                config=config,
                target_type='user',
                target_value=target_user
            )
            context.session_id = session_id
            
            logging.info(f"Created session: {session_id}")
            
            # Check for resumable sessions
            recovery = SessionRecovery(state_manager)
            resumable = recovery.find_resumable_sessions(max_age_days=1)
            if resumable:
                logging.info(f"Found {len(resumable)} resumable sessions")
            
        except ImportError as e:
            logging.warning(f"State management not available: {e}")
            context.state_manager = None
            context.session_id = None
        
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
            
            logging.info("Event system initialized with console and statistics observers")
            
        except ImportError as e:
            logging.warning(f"Event system not available: {e}")
            context.events = None
        
        # Configure pipeline context from AppConfig
        context.set_config("api_mode", config.scraping.api_mode)
        context.set_config("target_user", target_user)
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
            "target_user": target_user,
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
            logging.info("Dry-run mode: Skipping content processing/download")
        
        executor.add_stage(OrganizationStage())  # Placeholder for now
        executor.add_stage(ExportStage(export_config))
        
        # Execute pipeline
        logging.info("Starting pipeline execution...")
        logging.info(f"Pipeline stages: {executor.get_stage_names()}")
        
        metrics = await executor.execute(context)
        
        # Log execution results
        logging.info("Pipeline execution completed!")
        logging.info(f"Total execution time: {metrics.total_execution_time:.2f}s")
        logging.info(f"Successful stages: {metrics.successful_stages}/{metrics.total_stages}")
        logging.info(f"Total posts processed: {len(context.posts)}")
        
        # Log stage execution times
        for stage_name, execution_time in metrics.stage_times.items():
            logging.info(f"  {stage_name}: {execution_time:.2f}s")
        
        # Update session status based on pipeline results
        if context.state_manager and context.session_id:
            try:
                from datetime import datetime
                if metrics.failed_stages > 0:
                    context.state_manager.update_session_status(
                        context.session_id, 'failed', datetime.now()
                    )
                else:
                    context.state_manager.update_session_status(
                        context.session_id, 'completed', datetime.now()
                    )
                
                # Store final statistics
                context.state_manager.set_metadata(
                    context.session_id, 'execution_time', metrics.total_execution_time, 'number'
                )
                context.state_manager.set_metadata(
                    context.session_id, 'posts_processed', len(context.posts), 'number'
                )
                
            except Exception as e:
                logging.warning(f"Failed to update session status: {e}")
        
        if metrics.failed_stages > 0:
            logging.warning(f"Pipeline completed with {metrics.failed_stages} failed stages")
            for result in executor.get_stage_results():
                if not result.success:
                    logging.error(f"Stage '{result.stage_name}' failed: {result.errors}")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        
        # Mark session as failed if state management is available
        if context.state_manager and context.session_id:
            try:
                from datetime import datetime
                context.state_manager.update_session_status(
                    context.session_id, 'failed', datetime.now()
                )
                context.state_manager.set_metadata(
                    context.session_id, 'error_message', str(e), 'string'
                )
            except Exception as state_error:
                logging.warning(f"Failed to mark session as failed: {state_error}")
        
        raise
    
    finally:
        # Clean up state manager connection
        if context.state_manager:
            try:
                context.state_manager.close()
            except Exception as e:
                logging.warning(f"Error closing state manager: {e}")


async def process_posts_pipeline(args: argparse.Namespace) -> None:
    """
    Process posts using the pipeline architecture (legacy argparse version).
    
    This function creates and executes a processing pipeline with configurable
    stages for acquisition, filtering, processing, organization, and export.
    
    Args:
        args: Parsed command line arguments
    """
    try:
        # Create pipeline context with configuration
        context = PipelineContext()
        
        # Initialize event system
        try:
            from redditdl.core.events.emitter import EventEmitter
            from redditdl.core.events.observers import ConsoleObserver, StatisticsObserver
            
            event_emitter = EventEmitter()
            context.events = event_emitter
            
            # Add default observers
            console_observer = ConsoleObserver(
                verbose=not args.dry_run,
                use_rich=True,
                show_timestamps=False
            )
            stats_observer = StatisticsObserver()
            
            event_emitter.subscribe('*', console_observer, weak=False)
            event_emitter.subscribe('*', stats_observer, weak=False)
            
            logging.info("Event system initialized with console and statistics observers")
            
        except ImportError as e:
            logging.warning(f"Event system not available: {e}")
            context.events = None
        
        # Configure pipeline context from command line arguments
        context.set_config("api_mode", args.api)
        context.set_config("target_user", args.user)
        context.set_config("post_limit", args.limit)
        context.set_config("sleep_interval", args.sleep)
        context.set_config("output_dir", args.outdir)
        context.set_config("export_formats", args.export_formats)
        
        # API authentication configuration
        if args.api:
            context.set_config("client_id", args.client_id)
            context.set_config("client_secret", args.client_secret)
            context.set_config("user_agent", args.user_agent)
            if args.login:
                context.set_config("username", args.username)
                context.set_config("password", args.password)
        
        # Create pipeline executor
        executor = PipelineExecutor(error_handling="continue")
        
        # Configure pipeline stages
        acquisition_config = {
            "api_mode": args.api,
            "target_user": args.user,
            "post_limit": args.limit,
            "sleep_interval": args.sleep
        }
        
        if args.api:
            acquisition_config.update({
                "client_id": args.client_id,
                "client_secret": args.client_secret,
                "user_agent": args.user_agent
            })
            if args.login:
                acquisition_config.update({
                    "username": args.username,
                    "password": args.password
                })
        
        processing_config = {
            "output_dir": args.outdir,
            "sleep_interval": args.sleep,
            "dry_run": args.dry_run
        }
        
        export_config = {
            "export_formats": args.export_formats if args.export_formats else ["json"],
            "export_dir": Path(args.outdir) / "exports",
            "include_metadata": True,
            "include_posts": True
        }
        
        # Add pipeline stages
        executor.add_stage(AcquisitionStage(acquisition_config))
        executor.add_stage(FilterStage())  # Basic filtering, will be enhanced in Task 8
        
        # Skip processing stage in dry-run mode
        if not args.dry_run:
            executor.add_stage(ProcessingStage(processing_config))
        else:
            logging.info("Dry-run mode: Skipping content processing/download")
        
        executor.add_stage(OrganizationStage())  # Placeholder for now
        executor.add_stage(ExportStage(export_config))
        
        # Execute pipeline
        logging.info("Starting pipeline execution...")
        logging.info(f"Pipeline stages: {executor.get_stage_names()}")
        
        metrics = await executor.execute(context)
        
        # Log execution results
        logging.info("Pipeline execution completed!")
        logging.info(f"Total execution time: {metrics.total_execution_time:.2f}s")
        logging.info(f"Successful stages: {metrics.successful_stages}/{metrics.total_stages}")
        logging.info(f"Total posts processed: {len(context.posts)}")
        
        # Log stage execution times
        for stage_name, execution_time in metrics.stage_times.items():
            logging.info(f"  {stage_name}: {execution_time:.2f}s")
        
        if metrics.failed_stages > 0:
            logging.warning(f"Pipeline completed with {metrics.failed_stages} failed stages")
            for result in executor.get_stage_results():
                if not result.success:
                    logging.error(f"Stage '{result.stage_name}' failed: {result.errors}")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        raise


def main() -> None:
    """
    Main entry point for RedditDL application.
    
    Detects command structure and routes to appropriate CLI:
    - Modern Typer CLI for multi-command interface (redditdl scrape user ...)
    - Legacy argparse for backward compatibility (direct invocation)
    """
    # Check if we should use the new Typer CLI
    if TYPER_AVAILABLE and _should_use_typer_cli():
        # Use new Typer-based CLI
        typer_app()
        return
    
    # Fall back to legacy CLI for backward compatibility
    _run_legacy_cli()


def _should_use_typer_cli() -> bool:
    """
    Determine if we should use the new Typer CLI based on command structure.
    
    Returns True if:
    - Command looks like multi-command structure (scrape, audit, interactive)
    - No legacy-specific flags are detected
    """
    args = sys.argv[1:]
    
    # If no arguments, show new CLI help
    if not args:
        return True
    
    # If first argument is a known subcommand, use new CLI
    subcommands = ["scrape", "audit", "interactive", "help", "--help", "-h", "--version", "-v"]
    if args[0] in subcommands:
        return True
    
    # If using --user flag (legacy style), check for new-style subcommands
    if "--user" in args:
        # Check if this looks like: redditdl scrape user <username>
        try:
            user_index = args.index("--user")
            if user_index > 0 and args[user_index - 1] == "user":
                return True
        except (ValueError, IndexError):
            pass
    
    # If we detect modern flags like --pipeline or multi-command structure, use new CLI
    modern_flags = ["--pipeline", "--dry-run", "--export"]
    if any(flag in args for flag in modern_flags):
        return True
    
    # Default to legacy CLI for backward compatibility
    return False


def _run_legacy_cli() -> None:
    """
    Run the legacy argparse-based CLI for backward compatibility.
    """
    setup_logging()
    load_credentials_from_dotenv() # Load .env before parsing args
    
    try:
        # Parse command line arguments
        args = parse_args()
        
        logging.info("RedditDL - Reddit Media Downloader (Legacy Mode)")
        logging.info("=" * 40)
        logging.info(f"Target user: {args.user}")
        logging.info(f"Output directory: {args.outdir}")
        logging.info(f"Post limit: {args.limit}")
        logging.info(f"Sleep interval: {args.sleep}s")
        
        # Show suggestion to use new CLI
        if TYPER_AVAILABLE:
            logging.info("💡 Try the new CLI: redditdl scrape user %s", args.user)
        
        # Try to use new configuration system if available
        if CONFIG_AVAILABLE:
            try:
                from redditdl.core.config import ConfigManager
                from redditdl.cli.config_utils import convert_config_to_legacy
                
                # Convert argparse to CLI args format
                cli_args = {
                    'api': args.api,
                    'client_id': getattr(args, 'client_id', None),
                    'client_secret': getattr(args, 'client_secret', None),
                    'user_agent': getattr(args, 'user_agent', None),
                    'username': getattr(args, 'username', None),
                    'password': getattr(args, 'password', None),
                    'sleep': args.sleep,
                    'limit': args.limit,
                    'outdir': args.outdir,
                    'use_pipeline': args.use_pipeline,
                    'dry_run': args.dry_run,
                    'export_formats': getattr(args, 'export_formats', []),
                }
                
                # Load configuration using new system
                config_manager = ConfigManager()
                app_config = config_manager.load_config(cli_args=cli_args)
                
                logging.info("Using new configuration system for legacy CLI")
                
                # Use new pipeline function if pipeline mode
                if args.use_pipeline:
                    logging.info("Using new pipeline architecture")
                    if args.dry_run:
                        logging.info("Dry-run mode enabled - no files will be downloaded")
                    if args.export_formats:
                        logging.info(f"Export formats: {args.export_formats}")
                    
                    # Run new pipeline processing with config
                    asyncio.run(process_posts_pipeline_config(app_config, args.user))
                    return
                    
            except Exception as e:
                logging.warning(f"Failed to use new config system: {e}, falling back to legacy")
        
        # Choose processing mode (legacy approach)
        if args.use_pipeline:
            logging.info("Using pipeline architecture (legacy config)")
            if args.dry_run:
                logging.info("Dry-run mode enabled - no files will be downloaded")
            if args.export_formats:
                logging.info(f"Export formats: {args.export_formats}")
            
            # Run legacy pipeline processing
            asyncio.run(process_posts_pipeline(args))
        else:
            logging.info("Using legacy processing mode")
            
            if args.dry_run:
                logging.warning("--dry-run option is only supported in pipeline mode")
            if args.export_formats:
                logging.warning("--export-formats option is only supported in pipeline mode")
            
            # Initialize components based on configuration
            embedder = MetadataEmbedder()
            outdir = Path(args.outdir)
            downloader = MediaDownloader(outdir, args.sleep, embedder)
            
            # Select and configure scraper based on mode
            if args.api:
                logging.info("Using Reddit API mode (PRAW)")
                scraper = PrawScraper(
                    client_id=args.client_id,
                    client_secret=args.client_secret,
                    user_agent=args.user_agent,
                    login_username=args.username if args.login else None,
                    login_password=args.password if args.login else None,
                    sleep_interval=args.sleep
                )
            else:
                logging.info("Using non-API mode (YARS)")
                scraper = YarsScraper(sleep_interval=args.sleep)
            
            # Fetch user posts
            logging.info(f"Fetching posts from u/{args.user}...")
            posts = scraper.fetch_user_posts(args.user, args.limit)
            
            if not posts:
                logging.warning("No posts found or accessible for this user")
                return
            
            logging.info(f"Found {len(posts)} posts")
            
            # Process posts and download media
            process_posts(posts, downloader)
        
        logging.info("RedditDL completed successfully!")
        
    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 