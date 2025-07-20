#!/usr/bin/env python3
"""
RedditDL CLI Main Application

Modern Typer-based command-line interface providing multi-command structure,
rich formatting, and enhanced user experience.
"""

import typer
from rich import print as rprint
from rich.console import Console
from typing import Optional

from redditdl.cli import __version__
from redditdl.cli.commands import scrape, audit, interactive

console = Console()

# Create main Typer application
app = typer.Typer(
    name="redditdl",
    help="🚀 Reddit Media Downloader with advanced pipeline architecture",
    epilog="Visit https://github.com/example/redditdl for documentation and examples.",
    context_settings={"help_option_names": ["-h", "--help"]},
    rich_markup_mode="rich",
    no_args_is_help=True,
)

# Add subcommands
app.add_typer(scrape.app, name="scrape", help="Download media from Reddit users/subreddits")
app.add_typer(audit.app, name="audit", help="Audit and repair downloaded archives")
app.add_typer(interactive.app, name="interactive", help="Launch interactive REPL mode")

# Add completion support (hidden from main help)
try:
    from redditdl.cli import completion
except ImportError:
    pass  # Completion support is optional


def version_callback(value: bool):
    """Show version information."""
    if value:
        console.print(f"[bold cyan]RedditDL[/bold cyan] version [green]{__version__}[/green]")
        console.print("A modern Reddit media downloader with pipeline architecture")
        raise typer.Exit()


@app.callback()
def app_callback(
    version: Optional[bool] = typer.Option(
        None, 
        "--version", 
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version information and exit"
    ),
):
    """
    🚀 RedditDL - Modern Reddit Media Downloader
    
    Download and organize media from Reddit with advanced features:
    
    • Pipeline architecture for robust processing
    • Plugin system for extensibility  
    • Rich progress tracking and reporting
    • Multiple export formats
    • Archive management and repair tools
    • Interactive exploration mode
    
    [bold]Quick Start:[/bold]
    
    • Download from user: [cyan]redditdl scrape user username[/cyan]
    • Download from subreddit: [cyan]redditdl scrape subreddit pics[/cyan]
    • Audit archive: [cyan]redditdl audit check ./downloads[/cyan]
    • Interactive mode: [cyan]redditdl interactive[/cyan]
    """
    pass


def main():
    """Entry point for the redditdl console script."""
    import sys
    import traceback
    
    # Set up global exception handler to catch everything
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        console.print("\n[bold red]GLOBAL EXCEPTION HANDLER - FULL STACK TRACE:[/bold red]")
        traceback.print_exception(exc_type, exc_value, exc_traceback)
    
    sys.excepthook = handle_exception
    
    try:
        # Also try to catch the exception before Typer processes it
        import typer
        
        # Monkey patch Typer's exception handling
        original_main = app.main
        def debug_main(*args, **kwargs):
            try:
                return original_main(*args, **kwargs)
            except Exception as e:
                console.print("\n[bold red]TYPER APP EXCEPTION - FULL STACK TRACE:[/bold red]")
                traceback.print_exc()
                console.print(f"[bold red]Error:[/bold red] {e}")
                raise
        
        app.main = debug_main
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print("\n[bold red]MAIN FUNCTION EXCEPTION - FULL STACK TRACE:[/bold red]")
        traceback.print_exc()
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()