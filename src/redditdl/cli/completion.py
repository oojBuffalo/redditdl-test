"""
Command Completion Support

Provides shell completion for the RedditDL CLI using Typer's built-in
completion capabilities.
"""

import typer
from pathlib import Path

from .main import app


def install_completion(
    shell: str = typer.Option("bash", help="Shell type (bash, zsh, fish, powershell)")
):
    """
    Install shell completion for RedditDL.
    
    This will add command completion to your shell configuration.
    """
    try:
        # Get completion script
        completion_script = typer.completion.get_completion(app, shell=shell)
        
        # Determine shell config file
        home = Path.home()
        if shell == "bash":
            config_file = home / ".bashrc"
            if not config_file.exists():
                config_file = home / ".bash_profile"
        elif shell == "zsh":
            config_file = home / ".zshrc"
        elif shell == "fish":
            config_dir = home / ".config" / "fish" / "completions"
            config_dir.mkdir(parents=True, exist_ok=True)
            config_file = config_dir / "redditdl.fish"
        else:
            typer.echo(f"Completion for {shell} not fully supported yet")
            typer.echo("Copy the following to your shell configuration:")
            typer.echo(completion_script)
            return
        
        # Install completion
        if shell == "fish":
            # Fish uses separate completion files
            with open(config_file, "w") as f:
                f.write(completion_script)
            typer.echo(f"✓ Completion installed to {config_file}")
        else:
            # Bash/Zsh append to config file
            completion_line = f"\n# RedditDL completion\n{completion_script}\n"
            
            with open(config_file, "a") as f:
                f.write(completion_line)
            typer.echo(f"✓ Completion added to {config_file}")
        
        typer.echo(f"Restart your {shell} shell or run 'source {config_file}' to enable completion")
        
    except Exception as e:
        typer.echo(f"Error installing completion: {e}", err=True)
        raise typer.Exit(1)


def show_completion(
    shell: str = typer.Option("bash", help="Shell type (bash, zsh, fish, powershell)")
):
    """
    Show completion script for manual installation.
    """
    try:
        completion_script = typer.completion.get_completion(app, shell=shell)
        typer.echo(completion_script)
    except Exception as e:
        typer.echo(f"Error generating completion: {e}", err=True)
        raise typer.Exit(1)


# Add completion commands to main app
completion_app = typer.Typer(name="completion", help="Shell completion management")
completion_app.command("install")(install_completion)
completion_app.command("show")(show_completion)

# This will be imported and added to the main app
app.add_typer(completion_app, name="completion", hidden=True)