"""
CLI Utilities

Shared utilities for CLI commands including validation, formatting,
and common functionality.
"""

import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
import typer
from rich import print as rprint
from rich.panel import Panel
from rich.console import Console

console = Console()


def load_credentials_from_dotenv(dotenv_path_str: str = ".env") -> Dict[str, str]:
    """
    Load key-value pairs from a .env file into a dictionary.
    Does not override existing environment variables.
    
    Args:
        dotenv_path_str: Path to the .env file
        
    Returns:
        Dictionary of loaded environment variables
    """
    dotenv_path = Path(dotenv_path_str)
    loaded_vars = {}
    
    if dotenv_path.is_file():
        console.print(f"[dim]Loading credentials from {dotenv_path.resolve()}[/dim]")
        try:
            with open(dotenv_path, 'r') as f:
                lines_read = 0
                vars_loaded = 0
                for line_number, line in enumerate(f, 1):
                    lines_read += 1
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip("'\"")  # Strip potential quotes

                    if not key:  # Skip if key is empty after strip
                        console.print(f"[yellow]Skipping line {line_number} in {dotenv_path.name}: empty key[/yellow]")
                        continue

                    if key not in os.environ:
                        os.environ[key] = value
                        loaded_vars[key] = value
                        vars_loaded += 1
                        console.print(f"[dim]Loaded '{key}' from {dotenv_path.name}[/dim]")
                    else:
                        console.print(f"[dim]'{key}' already set in environment, not overridden[/dim]")
                        
                if lines_read > 0:
                    console.print(f"[green]Processed {dotenv_path.name}: {vars_loaded} new variable(s) loaded[/green]")
                else:
                    console.print(f"[yellow]{dotenv_path.name} is empty[/yellow]")
                    
        except IOError as e:
            console.print(f"[red]Could not read {dotenv_path.name}: {e}[/red]")
    else:
        console.print(f"[dim]{dotenv_path.resolve()} not found. Using existing environment variables or CLI arguments.[/dim]")
    
    return loaded_vars


def validate_sleep_interval(value: float) -> float:
    """Validate sleep interval is positive."""
    if value < 0:
        raise typer.BadParameter("Sleep interval must be a positive number")
    return value


def validate_positive_int(value: int) -> int:
    """Validate integer is positive."""
    if value <= 0:
        raise typer.BadParameter("Value must be a positive integer")
    return value


def validate_api_credentials(client_id: Optional[str], client_secret: Optional[str]) -> None:
    """Validate API credentials are provided when using API mode."""
    if not client_id or not client_secret:
        error_msg = Panel(
            "[red]API mode requires both client ID and client secret.[/red]\n\n"
            "You can provide them via:\n"
            "• Command line: [cyan]--client-id ID --client-secret SECRET[/cyan]\n"
            "• Environment variables: [cyan]REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET[/cyan]\n"
            "• .env file: [cyan]REDDIT_CLIENT_ID=your_id[/cyan]",
            title="[red]Missing API Credentials[/red]",
            border_style="red"
        )
        console.print(error_msg)
        raise typer.Exit(1)


def validate_login_credentials(username: Optional[str], password: Optional[str]) -> None:
    """Validate login credentials are provided when using login mode."""
    if not username or not password:
        error_msg = Panel(
            "[red]Login mode requires both username and password.[/red]\n\n"
            "You can provide them via:\n"
            "• Command line: [cyan]--username USER --password PASS[/cyan]\n"
            "• Environment variables: [cyan]REDDIT_USERNAME, REDDIT_PASSWORD[/cyan]\n"
            "• .env file: [cyan]REDDIT_USERNAME=your_username[/cyan]",
            title="[red]Missing Login Credentials[/red]",
            border_style="red"
        )
        console.print(error_msg)
        raise typer.Exit(1)


def print_header(title: str, subtitle: Optional[str] = None) -> None:
    """Print a formatted header for CLI output."""
    if subtitle:
        header_text = f"[bold cyan]{title}[/bold cyan]\n[dim]{subtitle}[/dim]"
    else:
        header_text = f"[bold cyan]{title}[/bold cyan]"
    
    console.print(Panel(header_text, border_style="cyan"))


def print_config_summary(config: Dict[str, Any]) -> None:
    """Print a summary of the current configuration."""
    config_lines = []
    
    if config.get("target_user"):
        config_lines.append(f"Target user: [cyan]{config['target_user']}[/cyan]")
    if config.get("output_dir"):
        config_lines.append(f"Output directory: [cyan]{config['output_dir']}[/cyan]")
    if config.get("post_limit"):
        config_lines.append(f"Post limit: [cyan]{config['post_limit']}[/cyan]")
    if config.get("sleep_interval"):
        config_lines.append(f"Sleep interval: [cyan]{config['sleep_interval']}s[/cyan]")
    if config.get("api_mode"):
        config_lines.append(f"Mode: [green]Reddit API (PRAW)[/green]")
    else:
        config_lines.append(f"Mode: [yellow]Public scraping (YARS)[/yellow]")
    if config.get("dry_run"):
        config_lines.append(f"Dry run: [yellow]Enabled[/yellow]")
    if config.get("export_formats"):
        config_lines.append(f"Export formats: [cyan]{', '.join(config['export_formats'])}[/cyan]")
    
    if config_lines:
        console.print(Panel(
            "\n".join(config_lines),
            title="[bold]Configuration[/bold]",
            border_style="green"
        ))


def confirm_action(message: str, default: bool = False) -> bool:
    """Ask for user confirmation with rich formatting."""
    return typer.confirm(message, default=default)


def handle_keyboard_interrupt() -> None:
    """Handle keyboard interrupt gracefully."""
    console.print("\n[yellow]Operation cancelled by user[/yellow]")
    raise typer.Exit(1)


def handle_fatal_error(error: Exception) -> None:
    """Handle fatal errors with rich formatting."""
    import traceback
    
    # Print full stack trace for debugging  
    console.print("\n[red]FATAL ERROR STACK TRACE:[/red]")
    traceback.print_exc()
    
    error_msg = Panel(
        f"[red]{str(error)}[/red]",
        title="[red]Fatal Error[/red]",
        border_style="red"
    )
    console.print(error_msg)
    raise typer.Exit(1)