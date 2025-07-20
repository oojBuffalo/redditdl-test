
from redditdl.core.error_context import get_error_analytics
from redditdl.core.exceptions import RedditDLError
import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.padding import Padding

console = Console(stderr=True)

def handle_error(err: RedditDLError):
    """Handles RedditDLError exceptions, formats them, and prints them to the console."""
    reporter = get_error_analytics().get_reporter(err)
    report = reporter.generate_report()

    console.print()
    error_panel = Panel(
        Text(report.description, justify="full"),
        title=f"[bold red]Error: {report.title}[/bold red]",
        border_style="red",
        expand=False
    )
    console.print(error_panel)

    if report.suggestions:
        console.print("\n[bold green]Suggested solutions:[/bold green]")
        for i, suggestion in enumerate(report.suggestions, 1):
            suggestion_text = Text(f"{i}. {suggestion.action}: {suggestion.description}\n")
            if suggestion.command:
                suggestion_text.append(f"   Run: ", style="bold")
                suggestion_text.append(f"{suggestion.command}", style="cyan")
            console.print(Padding(suggestion_text, (0, 1)))

    if report.correlation_id:
        console.print(Padding(f"Trace ID: [yellow]{report.correlation_id}[/yellow]", (1, 0, 0, 0)))

    get_error_analytics().record_error(err)
    raise typer.Exit(code=1) 