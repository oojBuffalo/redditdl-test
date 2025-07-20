"""
Interactive Shell Implementation

Core implementation of the REPL (Read-Eval-Print Loop) functionality for
RedditDL interactive mode. Provides command parsing, execution, and integration
with the pipeline system.
"""

import asyncio
import shlex
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import logging

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter, CompleteEvent, Completion
from prompt_toolkit.history import FileHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

# Import core systems
from redditdl.core.config import AppConfig
from redditdl.core.pipeline.executor import PipelineExecutor
from redditdl.core.pipeline.interfaces import PipelineContext
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.events.observers import ConsoleObserver, StatisticsObserver
from redditdl.core.state.manager import StateManager
from redditdl.scrapers import PostMetadata

# Import pipeline stages
from redditdl.pipeline.stages.acquisition import AcquisitionStage
from redditdl.pipeline.stages.filter import FilterStage
from redditdl.pipeline.stages.processing import ProcessingStage
from redditdl.pipeline.stages.export import ExportStage

# Import filters for interactive filtering
from redditdl.filters.factory import FilterFactory

# Import target handlers
from redditdl.targets.resolver import TargetResolver

console = Console()
logger = logging.getLogger(__name__)


class InteractiveShell:
    """
    Interactive shell for RedditDL with command parsing and pipeline integration.
    
    Provides a REPL interface that allows users to explore Reddit content
    interactively, test filters, preview posts, and execute targeted operations
    without running full batch processes.
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize the interactive shell with configuration.
        
        Args:
            config: Application configuration instance
        """
        self.config = config
        self.console = console
        
        # Session state
        self.session_id = f"interactive_{int(datetime.now().timestamp())}"
        self.current_posts: List[PostMetadata] = []
        self.filtered_posts: List[PostMetadata] = []
        self.session_stats = {
            "posts_discovered": 0,
            "posts_filtered": 0,
            "posts_downloaded": 0,
            "total_size": 0,
            "start_time": datetime.now()
        }
        
        # Core systems
        self.event_emitter = EventEmitter()
        self.state_manager = StateManager(self.config.session_dir / f"{self.session_id}.db")
        self.pipeline_executor = PipelineExecutor()
        self.target_resolver = TargetResolver()
        self.filter_factory = FilterFactory()
        
        # Observers for progress tracking
        self.console_observer = ConsoleObserver()
        self.stats_observer = StatisticsObserver()
        self.event_emitter.subscribe(self.console_observer)
        self.event_emitter.subscribe(self.stats_observer)
        
        # Command completions
        self.commands = [
            "explore", "download", "filter", "preview", "stats", 
            "config", "clear", "help", "quit", "exit"
        ]
        self.completer = WordCompleter(self.commands, ignore_case=True)
        
        # Command history
        history_file = self.config.session_dir / "repl_history.txt"
        history_file.parent.mkdir(exist_ok=True)
        self.history = FileHistory(str(history_file))
        
        # Style for prompt
        self.style = Style.from_dict({
            'prompt': '#00aa00 bold',
            'command': '#0000aa bold',
            'error': '#aa0000 bold',
            'success': '#00aa00',
            'info': '#888888',
        })
        
        # Initialize pipeline stages
        self._setup_pipeline()
        
    def _setup_pipeline(self) -> None:
        """Setup pipeline stages for interactive use."""
        # Create pipeline context
        self.context = PipelineContext(
            posts=[],
            config=self.config.model_dump(),
            session_state={},
            events=self.event_emitter,
            metadata={"session_id": self.session_id}
        )
        
        # Add stages to pipeline
        self.acquisition_stage = AcquisitionStage()
        self.filter_stage = FilterStage()
        self.processing_stage = ProcessingStage()
        self.export_stage = ExportStage()
        
        self.pipeline_executor.add_stage(self.acquisition_stage)
        self.pipeline_executor.add_stage(self.filter_stage)
        self.pipeline_executor.add_stage(self.processing_stage)
        self.pipeline_executor.add_stage(self.export_stage)
    
    async def start_repl(self) -> None:
        """
        Start the interactive REPL session.
        
        Main loop that handles user input, parses commands, and executes them.
        Continues until user types 'quit' or 'exit'.
        """
        # Show welcome message
        self._show_welcome()
        
        # Create prompt session
        session = PromptSession(
            history=self.history,
            completer=self.completer,
            style=self.style,
            complete_style='multi-column'
        )
        
        while True:
            try:
                # Get user input
                user_input = await session.prompt_async(
                    HTML('<prompt>redditdl></prompt> '),
                    complete_style='multi-column'
                )
                
                # Skip empty input
                if not user_input.strip():
                    continue
                
                # Parse and execute command
                should_exit = await self._handle_command(user_input.strip())
                if should_exit:
                    break
                    
            except KeyboardInterrupt:
                # Handle Ctrl+C gracefully
                self.console.print("\n[info]Use 'quit' or 'exit' to leave the REPL[/info]")
                continue
            except EOFError:
                # Handle Ctrl+D
                break
            except Exception as e:
                # Handle unexpected errors
                self.console.print(f"[error]Error: {e}[/error]")
                logger.exception("Error in REPL")
        
        # Show goodbye message
        self._show_goodbye()
    
    async def _handle_command(self, command_line: str) -> bool:
        """
        Parse and handle a command from the REPL.
        
        Args:
            command_line: The complete command line input
            
        Returns:
            True if the REPL should exit, False otherwise
        """
        try:
            # Parse command line
            parts = shlex.split(command_line)
            if not parts:
                return False
            
            command = parts[0].lower()
            args = parts[1:]
            
            # Route to appropriate handler
            if command in ['quit', 'exit']:
                return True
            elif command == 'help':
                self._show_help()
            elif command == 'explore':
                await self._handle_explore(args)
            elif command == 'download':
                await self._handle_download(args)
            elif command == 'filter':
                await self._handle_filter(args)
            elif command == 'preview':
                self._handle_preview(args)
            elif command == 'stats':
                self._handle_stats()
            elif command == 'config':
                self._handle_config(args)
            elif command == 'clear':
                self._handle_clear()
            else:
                self.console.print(f"[error]Unknown command: {command}[/error]")
                self.console.print("[info]Type 'help' for available commands[/info]")
            
            return False
            
        except Exception as e:
            self.console.print(f"[error]Command error: {e}[/error]")
            return False
    
    def _show_welcome(self) -> None:
        """Show welcome message and basic information."""
        welcome_text = f"""[bold cyan]RedditDL Interactive Mode[/bold cyan]

[green]Session ID:[/green] {self.session_id}
[green]Configuration:[/green] {'API mode' if self.config.scraping.api_mode else 'Public mode'}
[green]Output Directory:[/green] {self.config.output.output_dir}

Type [cyan]help[/cyan] for available commands, [cyan]quit[/cyan] to exit."""
        
        self.console.print(Panel(welcome_text, title="Welcome", border_style="blue"))
    
    def _show_goodbye(self) -> None:
        """Show goodbye message with session summary."""
        duration = datetime.now() - self.session_stats["start_time"]
        
        goodbye_text = f"""[bold cyan]Session Summary[/bold cyan]

[green]Posts discovered:[/green] {self.session_stats['posts_discovered']}
[green]Posts filtered:[/green] {self.session_stats['posts_filtered']}
[green]Posts downloaded:[/green] {self.session_stats['posts_downloaded']}
[green]Session duration:[/green] {duration}

Thank you for using RedditDL!"""
        
        self.console.print(Panel(goodbye_text, title="Goodbye", border_style="green"))
    
    def _show_help(self) -> None:
        """Show help information with all available commands."""
        help_table = Table(title="Available Commands", show_header=True, header_style="bold cyan")
        help_table.add_column("Command", style="green", width=20)
        help_table.add_column("Description", style="white")
        help_table.add_column("Examples", style="dim")
        
        commands_help = [
            ("explore <target>", "Browse posts from user/subreddit", "explore user:johndoe\nexplore r/pics"),
            ("download <post_id>", "Download specific post by ID", "download abc123"),
            ("download all", "Download all filtered posts", "download all"),
            ("filter <criteria>", "Apply filters to current posts", "filter score:>100\nfilter nsfw:exclude"),
            ("preview", "Show previews of current posts", "preview"),
            ("preview <count>", "Show limited number of previews", "preview 5"),
            ("stats", "Show session statistics", "stats"),
            ("config", "Show current configuration", "config"),
            ("clear", "Clear current session data", "clear"),
            ("help", "Show this help message", "help"),
            ("quit/exit", "Exit the REPL", "quit"),
        ]
        
        for cmd, desc, examples in commands_help:
            help_table.add_row(cmd, desc, examples)
        
        self.console.print(help_table)
    
    async def _handle_explore(self, args: List[str]) -> None:
        """Handle the explore command to browse posts."""
        if not args:
            self.console.print("[error]Usage: explore <target>[/error]")
            self.console.print("[info]Examples: explore user:johndoe, explore r/pics[/info]")
            return
        
        target = args[0]
        
        try:
            # Resolve target
            self.console.print(f"[info]Exploring {target}...[/info]")
            
            # Update context with target
            self.context.metadata["targets"] = [target]
            
            # Use acquisition stage to get posts
            result = await self.acquisition_stage.process(self.context)
            
            if result.success and result.data.get("posts"):
                self.current_posts = result.data["posts"]
                self.filtered_posts = self.current_posts.copy()
                count = len(self.current_posts)
                
                self.session_stats["posts_discovered"] += count
                self.session_stats["posts_filtered"] = len(self.filtered_posts)
                
                self.console.print(f"[success]Found {count} posts[/success]")
                
                # Show a brief preview
                if count > 0:
                    self._show_post_preview(limit=3)
                    if count > 3:
                        self.console.print(f"[info]... and {count - 3} more posts. Use 'preview' to see all.[/info]")
            else:
                self.console.print("[error]Failed to explore target[/error]")
                if result.error:
                    self.console.print(f"[error]Error: {result.error}[/error]")
                    
        except Exception as e:
            self.console.print(f"[error]Exploration failed: {e}[/error]")
            logger.exception("Exploration error")
    
    async def _handle_download(self, args: List[str]) -> None:
        """Handle the download command for specific posts or all filtered posts."""
        if not args:
            self.console.print("[error]Usage: download <post_id> or download all[/error]")
            return
        
        if args[0].lower() == "all":
            await self._download_all_posts()
        else:
            await self._download_specific_post(args[0])
    
    async def _download_specific_post(self, post_id: str) -> None:
        """Download a specific post by ID."""
        # Find post in current list
        target_post = None
        for post in self.filtered_posts:
            if post.post_id == post_id:
                target_post = post
                break
        
        if not target_post:
            self.console.print(f"[error]Post {post_id} not found in current session[/error]")
            return
        
        try:
            # Create context with single post
            download_context = PipelineContext(
                posts=[target_post],
                config=self.config.model_dump(),
                events=self.event_emitter,
                metadata={"interactive": True}
            )
            
            # Run through processing stage only
            result = await self.processing_stage.process(download_context)
            
            if result.success:
                self.session_stats["posts_downloaded"] += 1
                self.console.print(f"[success]✓ Downloaded post {post_id}[/success]")
            else:
                self.console.print(f"[error]Failed to download post {post_id}[/error]")
                
        except Exception as e:
            self.console.print(f"[error]Download failed: {e}[/error]")
    
    async def _download_all_posts(self) -> None:
        """Download all filtered posts."""
        if not self.filtered_posts:
            self.console.print("[error]No posts to download. Use 'explore' first.[/error]")
            return
        
        count = len(self.filtered_posts)
        self.console.print(f"[info]Downloading {count} posts...[/info]")
        
        try:
            # Create context with all filtered posts
            download_context = PipelineContext(
                posts=self.filtered_posts,
                config=self.config.model_dump(),
                events=self.event_emitter,
                metadata={"interactive": True}
            )
            
            # Run through processing stage
            result = await self.processing_stage.process(download_context)
            
            if result.success:
                self.session_stats["posts_downloaded"] += count
                self.console.print(f"[success]✓ Downloaded {count} posts[/success]")
            else:
                self.console.print(f"[error]Some downloads may have failed[/error]")
                
        except Exception as e:
            self.console.print(f"[error]Batch download failed: {e}[/error]")
    
    async def _handle_filter(self, args: List[str]) -> None:
        """Handle the filter command to apply interactive filtering."""
        if not args:
            self.console.print("[error]Usage: filter <criteria>[/error]")
            self.console.print("[info]Examples: filter score:>100, filter nsfw:exclude[/info]")
            return
        
        if not self.current_posts:
            self.console.print("[error]No posts to filter. Use 'explore' first.[/error]")
            return
        
        criteria = " ".join(args)
        
        try:
            # Parse filter criteria and create filter
            filter_obj = self._parse_filter_criteria(criteria)
            if not filter_obj:
                return
            
            # Apply filter to current posts
            original_count = len(self.filtered_posts)
            self.filtered_posts = []
            
            for post in self.current_posts:
                if await filter_obj.apply(post):
                    self.filtered_posts.append(post)
            
            filtered_count = len(self.filtered_posts)
            removed_count = original_count - filtered_count
            
            self.session_stats["posts_filtered"] = filtered_count
            
            self.console.print(f"[success]Filter applied: {filtered_count} posts remaining ({removed_count} filtered out)[/success]")
            
            # Show preview of filtered results
            if filtered_count > 0:
                self._show_post_preview(limit=3)
                
        except Exception as e:
            self.console.print(f"[error]Filter failed: {e}[/error]")
    
    def _parse_filter_criteria(self, criteria: str):
        """Parse filter criteria string and return appropriate filter object."""
        # Simple criteria parsing - can be enhanced
        if "score:" in criteria:
            # Extract score condition
            try:
                score_part = criteria.split("score:")[1].strip()
                if score_part.startswith(">"):
                    min_score = int(score_part[1:])
                    return self.filter_factory.create_score_filter(min_score=min_score)
                elif score_part.startswith("<"):
                    max_score = int(score_part[1:])
                    return self.filter_factory.create_score_filter(max_score=max_score)
                else:
                    exact_score = int(score_part)
                    return self.filter_factory.create_score_filter(min_score=exact_score, max_score=exact_score)
            except ValueError:
                self.console.print("[error]Invalid score criteria. Use format: score:>100 or score:<50[/error]")
                return None
        
        elif "nsfw:" in criteria:
            nsfw_mode = criteria.split("nsfw:")[1].strip().lower()
            if nsfw_mode in ["include", "exclude", "only"]:
                return self.filter_factory.create_nsfw_filter(mode=nsfw_mode)
            else:
                self.console.print("[error]Invalid NSFW mode. Use: include, exclude, or only[/error]")
                return None
        
        else:
            self.console.print(f"[error]Unknown filter criteria: {criteria}[/error]")
            self.console.print("[info]Supported filters: score:>N, score:<N, nsfw:include/exclude/only[/info]")
            return None
    
    def _handle_preview(self, args: List[str]) -> None:
        """Handle the preview command to show post previews."""
        limit = None
        if args:
            try:
                limit = int(args[0])
            except ValueError:
                self.console.print("[error]Invalid limit. Use a number.[/error]")
                return
        
        if not self.filtered_posts:
            self.console.print("[error]No posts to preview. Use 'explore' first.[/error]")
            return
        
        self._show_post_preview(limit=limit)
    
    def _show_post_preview(self, limit: Optional[int] = None) -> None:
        """Show preview of current filtered posts."""
        posts_to_show = self.filtered_posts[:limit] if limit else self.filtered_posts
        
        preview_table = Table(title="Post Previews", show_header=True, header_style="bold cyan")
        preview_table.add_column("#", style="dim", width=3)
        preview_table.add_column("ID", style="yellow", width=8)
        preview_table.add_column("Title", style="white", max_width=40)
        preview_table.add_column("Score", style="green", width=6)
        preview_table.add_column("Comments", style="blue", width=8)
        preview_table.add_column("Type", style="magenta", width=8)
        
        for i, post in enumerate(posts_to_show, 1):
            title = post.title[:40] + "..." if len(post.title) > 40 else post.title
            preview_table.add_row(
                str(i),
                post.post_id[:8],
                title,
                str(post.score),
                str(post.num_comments),
                post.post_type
            )
        
        self.console.print(preview_table)
        
        if limit and len(self.filtered_posts) > limit:
            remaining = len(self.filtered_posts) - limit
            self.console.print(f"[info]... and {remaining} more posts[/info]")
    
    def _handle_stats(self) -> None:
        """Handle the stats command to show session statistics."""
        duration = datetime.now() - self.session_stats["start_time"]
        
        stats_table = Table(title="Session Statistics", show_header=False)
        stats_table.add_column("Metric", style="cyan", width=20)
        stats_table.add_column("Value", style="white")
        
        stats_table.add_row("Session ID", self.session_id)
        stats_table.add_row("Posts discovered", str(self.session_stats["posts_discovered"]))
        stats_table.add_row("Posts filtered", str(self.session_stats["posts_filtered"]))
        stats_table.add_row("Posts downloaded", str(self.session_stats["posts_downloaded"]))
        stats_table.add_row("Session duration", str(duration).split('.')[0])
        stats_table.add_row("Configuration", "API mode" if self.config.scraping.api_mode else "Public mode")
        stats_table.add_row("Output directory", str(self.config.output.output_dir))
        
        self.console.print(stats_table)
    
    def _handle_config(self, args: List[str]) -> None:
        """Handle the config command to show current configuration."""
        if args and args[0] == "full":
            # Show full configuration
            config_dict = self.config.model_dump()
            self.console.print_json(data=config_dict)
        else:
            # Show summary configuration
            config_table = Table(title="Configuration Summary", show_header=False)
            config_table.add_column("Setting", style="cyan", width=20)
            config_table.add_column("Value", style="white")
            
            config_table.add_row("API Mode", str(self.config.scraping.api_mode))
            config_table.add_row("Post Limit", str(self.config.scraping.post_limit))
            config_table.add_row("Sleep Interval", f"{self.config.get_effective_sleep_interval():.1f}s")
            config_table.add_row("Output Directory", str(self.config.output.output_dir))
            config_table.add_row("Export Formats", ", ".join(self.config.output.export_formats))
            config_table.add_row("Dry Run", str(self.config.dry_run))
            config_table.add_row("Verbose", str(self.config.verbose))
            
            self.console.print(config_table)
            self.console.print("[info]Use 'config full' to see complete configuration[/info]")
    
    def _handle_clear(self) -> None:
        """Handle the clear command to clear session data."""
        self.current_posts = []
        self.filtered_posts = []
        self.session_stats.update({
            "posts_discovered": 0,
            "posts_filtered": 0,
            "posts_downloaded": 0,
            "total_size": 0
        })
        
        self.console.print("[success]Session data cleared[/success]")