"""
Interactive Command

Interactive REPL mode for ad-hoc exploration and testing of RedditDL functionality.
"""

import asyncio
from typing import Optional, Annotated
import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel

from redditdl.cli.utils import print_header, console
from redditdl.cli.config_utils import (
    load_config_from_cli,
    build_cli_args,
    print_config_summary
)
from redditdl.cli.interactive import InteractiveShell

# Configuration system
from redditdl.core.config import AppConfig

# Create the interactive sub-application
app = typer.Typer(
    name="interactive",
    help="Launch interactive REPL mode",
    rich_markup_mode="rich",
)


@app.command()
def start(
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file to load")] = None,
    api: Annotated[Optional[bool], typer.Option("--api", help="Start with API mode enabled")] = None,
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
):
    """
    Launch interactive REPL mode for exploring RedditDL functionality.
    
    [bold cyan]Features:[/bold cyan]
    
    • Explore users and subreddits interactively
    • Test filters and configurations
    • Preview posts before downloading
    • Execute commands step by step
    
    [bold cyan]Commands in REPL:[/bold cyan]
    
    • [green]explore user:username[/green] - Browse user posts
    • [green]explore subreddit:name[/green] - Browse subreddit posts
    • [green]download post:id[/green] - Download specific post
    • [green]filter score:>100[/green] - Apply score filter
    • [green]stats[/green] - Show session statistics
    • [green]help[/green] - Show available commands
    • [green]quit[/green] - Exit REPL
    
    [bold cyan]Examples:[/bold cyan]
    
    • Basic REPL: [green]redditdl interactive[/green]
    • With API mode: [green]redditdl interactive --api[/green]
    • Load config: [green]redditdl interactive --config myconfig.yaml[/green]
    """
    # Load configuration
    cli_args = build_cli_args(
        config=config,
        api=api,
        verbose=verbose,
    )
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    
    print_header("RedditDL Interactive Mode", "REPL for ad-hoc exploration")
    
    if app_config.verbose:
        print_config_summary(app_config, target="Interactive REPL")
    
    # Initialize and start interactive shell
    shell = InteractiveShell(app_config)
    
    try:
        # Run the async REPL
        asyncio.run(shell.start_repl())
    except KeyboardInterrupt:
        console.print("\n[dim]Interactive session interrupted[/dim]")
    except Exception as e:
        console.print(f"[red]Interactive session error: {e}[/red]")
        raise typer.Exit(1)

