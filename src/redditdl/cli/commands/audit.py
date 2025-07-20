"""
Audit Command

Commands for auditing and repairing downloaded archives, checking integrity,
and managing archive organization.
"""

import json
import hashlib
import zipfile
import tarfile
import shutil
import sqlite3
from pathlib import Path
from typing import Optional, List, Annotated, Dict, Any, Set
from datetime import datetime
import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from redditdl.cli.utils import confirm_action, print_header, console
from redditdl.cli.config_utils import (
    load_config_from_cli,
    build_cli_args,
    print_config_summary
)

# Configuration and state management
from redditdl.core.config import AppConfig
from redditdl.core.state.manager import StateManager
from redditdl.exporters.base import ExporterRegistry

class ArchiveAuditor:
    """
    Comprehensive archive auditing and repair system.
    
    Provides integrity checking, repair capabilities, compression, and reporting
    for RedditDL download archives.
    """
    
    def __init__(self, archive_path: Path, config: AppConfig):
        """
        Initialize the archive auditor.
        
        Args:
            archive_path: Path to the archive directory
            config: Application configuration
        """
        self.archive_path = Path(archive_path)
        self.config = config
        self.console = console
        
        # Initialize state manager for database operations
        self.state_manager = None
        self._init_state_manager()
    
    def _init_state_manager(self) -> None:
        """Initialize state manager if SQLite databases exist."""
        try:
            # Look for state databases in the archive
            db_files = list(self.archive_path.rglob("*.db"))
            if db_files:
                # Use the first found database or most recent
                db_path = max(db_files, key=lambda x: x.stat().st_mtime)
                self.state_manager = StateManager(db_path)
        except Exception as e:
            if self.config.verbose:
                self.console.print(f"[dim]No state database found: {e}[/dim]")
    
    def audit_archive(self, check_integrity: bool = False, verbose: bool = False) -> List[Dict[str, Any]]:
        """
        Perform comprehensive archive audit.
        
        Args:
            check_integrity: Perform deep integrity checks
            verbose: Enable verbose output
            
        Returns:
            List of issues found
        """
        issues = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            
            # Phase 1: File system scan
            scan_task = progress.add_task("[cyan]Scanning files...", total=None)
            file_issues = self._check_file_system()
            issues.extend(file_issues)
            progress.update(scan_task, completed=100)
            
            # Phase 2: Metadata consistency
            metadata_task = progress.add_task("[cyan]Checking metadata...", total=None)
            metadata_issues = self._check_metadata_consistency()
            issues.extend(metadata_issues)
            progress.update(metadata_task, completed=100)
            
            # Phase 3: Database integrity (if available)
            if self.state_manager:
                db_task = progress.add_task("[cyan]Checking database...", total=None)
                db_issues = self._check_database_integrity()
                issues.extend(db_issues)
                progress.update(db_task, completed=100)
            
            # Phase 4: Deep integrity checks
            if check_integrity:
                integrity_task = progress.add_task("[cyan]Deep integrity check...", total=None)
                integrity_issues = self._check_deep_integrity()
                issues.extend(integrity_issues)
                progress.update(integrity_task, completed=100)
        
        return issues
    
    def _check_file_system(self) -> List[Dict[str, Any]]:
        """Check file system for basic issues."""
        issues = []
        
        try:
            # Count files and identify types
            all_files = list(self.archive_path.rglob("*"))
            media_files = [f for f in all_files if f.is_file() and f.suffix.lower() in 
                          {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.webm', '.mov'}]
            json_files = [f for f in all_files if f.is_file() and f.suffix.lower() == '.json']
            
            # Check for missing media files referenced in JSON
            missing_media = self._find_missing_media_files(json_files, media_files)
            if missing_media:
                issues.append({
                    "type": "missing_media",
                    "severity": "error",
                    "message": f"Found {len(missing_media)} missing media files",
                    "count": len(missing_media),
                    "details": missing_media[:10]  # Show first 10
                })
            
            # Check for orphaned media files (no corresponding JSON)
            orphaned_media = self._find_orphaned_media_files(json_files, media_files)
            if orphaned_media:
                issues.append({
                    "type": "orphaned_media",
                    "severity": "warning",
                    "message": f"Found {len(orphaned_media)} orphaned media files",
                    "count": len(orphaned_media),
                    "details": orphaned_media[:10]
                })
            
            # Check for empty directories
            empty_dirs = [d for d in all_files if d.is_dir() and not any(d.iterdir())]
            if empty_dirs:
                issues.append({
                    "type": "empty_directories",
                    "severity": "info",
                    "message": f"Found {len(empty_dirs)} empty directories",
                    "count": len(empty_dirs),
                    "details": [str(d) for d in empty_dirs[:10]]
                })
                
        except Exception as e:
            issues.append({
                "type": "filesystem_error",
                "severity": "error",
                "message": f"File system scan error: {str(e)}",
                "count": 1
            })
        
        return issues
    
    def _check_metadata_consistency(self) -> List[Dict[str, Any]]:
        """Check metadata consistency and validity."""
        issues = []
        
        try:
            json_files = list(self.archive_path.rglob("*.json"))
            invalid_json = []
            missing_fields = []
            
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Check for required fields
                    required_fields = ['id', 'title', 'url', 'subreddit', 'author']
                    missing = [field for field in required_fields if field not in data]
                    if missing:
                        missing_fields.append((str(json_file), missing))
                        
                except json.JSONDecodeError:
                    invalid_json.append(str(json_file))
                except Exception:
                    invalid_json.append(str(json_file))
            
            if invalid_json:
                issues.append({
                    "type": "invalid_json",
                    "severity": "error",
                    "message": f"Found {len(invalid_json)} corrupted JSON files",
                    "count": len(invalid_json),
                    "details": invalid_json[:10]
                })
            
            if missing_fields:
                issues.append({
                    "type": "incomplete_metadata",
                    "severity": "warning",
                    "message": f"Found {len(missing_fields)} JSON files with missing fields",
                    "count": len(missing_fields),
                    "details": [f"{file}: {fields}" for file, fields in missing_fields[:5]]
                })
                
        except Exception as e:
            issues.append({
                "type": "metadata_check_error",
                "severity": "error",
                "message": f"Metadata check error: {str(e)}",
                "count": 1
            })
        
        return issues
    
    def _check_database_integrity(self) -> List[Dict[str, Any]]:
        """Check database integrity using StateManager."""
        issues = []
        
        if not self.state_manager:
            return issues
        
        try:
            integrity_report = self.state_manager.check_integrity()
            
            if not integrity_report['database_ok']:
                issues.append({
                    "type": "database_corruption",
                    "severity": "error",
                    "message": "Database integrity check failed",
                    "count": len(integrity_report['issues']),
                    "details": integrity_report['issues']
                })
            
            # Check for abandoned sessions
            sessions = self.state_manager.list_sessions(status='active')
            if sessions:
                issues.append({
                    "type": "abandoned_sessions",
                    "severity": "warning",
                    "message": f"Found {len(sessions)} active sessions that may be abandoned",
                    "count": len(sessions),
                    "details": [s['id'] for s in sessions[:5]]
                })
                
        except Exception as e:
            issues.append({
                "type": "database_check_error",
                "severity": "error",
                "message": f"Database check error: {str(e)}",
                "count": 1
            })
        
        return issues
    
    def _check_deep_integrity(self) -> List[Dict[str, Any]]:
        """Perform deep integrity checks on media files."""
        issues = []
        
        try:
            media_files = list(self.archive_path.rglob("*"))
            media_files = [f for f in media_files if f.is_file() and f.suffix.lower() in 
                          {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.webm', '.mov'}]
            
            corrupted_files = []
            
            for media_file in media_files[:100]:  # Limit to avoid long processing
                try:
                    # Basic file header check
                    if self._check_file_header(media_file):
                        continue
                    corrupted_files.append(str(media_file))
                except Exception:
                    corrupted_files.append(str(media_file))
            
            if corrupted_files:
                issues.append({
                    "type": "corrupted_media",
                    "severity": "error",
                    "message": f"Found {len(corrupted_files)} potentially corrupted media files",
                    "count": len(corrupted_files),
                    "details": corrupted_files[:10]
                })
                
        except Exception as e:
            issues.append({
                "type": "integrity_check_error",
                "severity": "error",
                "message": f"Deep integrity check error: {str(e)}",
                "count": 1
            })
        
        return issues
    
    def _find_missing_media_files(self, json_files: List[Path], media_files: List[Path]) -> List[str]:
        """Find media files referenced in JSON but missing from filesystem."""
        missing = []
        media_paths = {f.name for f in media_files}
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Check if corresponding media file exists
                post_id = data.get('id', '')
                if post_id:
                    # Look for files starting with post_id
                    expected_files = [f for f in media_paths if f.startswith(post_id)]
                    if not expected_files:
                        missing.append(f"Media for {json_file.name}")
                        
            except Exception:
                continue
        
        return missing
    
    def _find_orphaned_media_files(self, json_files: List[Path], media_files: List[Path]) -> List[str]:
        """Find media files without corresponding JSON metadata."""
        orphaned = []
        json_ids = set()
        
        # Extract post IDs from JSON files
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                post_id = data.get('id', '')
                if post_id:
                    json_ids.add(post_id)
            except Exception:
                continue
        
        # Check each media file
        for media_file in media_files:
            # Try to extract post ID from filename
            filename = media_file.name
            post_id = filename.split('-')[0] if '-' in filename else filename.split('.')[0]
            
            if post_id not in json_ids:
                orphaned.append(str(media_file))
        
        return orphaned
    
    def _check_file_header(self, file_path: Path) -> bool:
        """Basic file header validation."""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(16)
            
            ext = file_path.suffix.lower()
            
            # Basic magic number checks
            if ext in ['.jpg', '.jpeg'] and not header.startswith(b'\\xff\\xd8'):
                return False
            elif ext == '.png' and not header.startswith(b'\\x89PNG'):
                return False
            elif ext == '.gif' and not header.startswith(b'GIF'):
                return False
            elif ext == '.webp' and b'WEBP' not in header:
                return False
            
            return True
            
        except Exception:
            return False
    
    def repair_issues(self, issues: List[Dict[str, Any]]) -> int:
        """
        Repair detected issues.
        
        Args:
            issues: List of issues from audit
            
        Returns:
            Number of issues repaired
        """
        repaired_count = 0
        
        for issue in issues:
            issue_type = issue.get('type', '')
            
            try:
                if issue_type == 'empty_directories':
                    repaired_count += self._repair_empty_directories(issue)
                elif issue_type == 'abandoned_sessions':
                    repaired_count += self._repair_abandoned_sessions(issue)
                elif issue_type == 'invalid_json':
                    repaired_count += self._repair_invalid_json(issue)
                elif issue_type == 'orphaned_media':
                    # For orphaned media, we'll just log them for now
                    self.console.print(f"[yellow]Logged {issue['count']} orphaned files for manual review[/yellow]")
                    
            except Exception as e:
                self.console.print(f"[red]Failed to repair {issue_type}: {e}[/red]")
        
        return repaired_count
    
    def _repair_empty_directories(self, issue: Dict[str, Any]) -> int:
        """Remove empty directories."""
        count = 0
        for dir_path in issue.get('details', []):
            try:
                Path(dir_path).rmdir()
                count += 1
            except Exception:
                pass
        return count
    
    def _repair_abandoned_sessions(self, issue: Dict[str, Any]) -> int:
        """Mark abandoned sessions as completed."""
        if not self.state_manager:
            return 0
        
        count = 0
        for session_id in issue.get('details', []):
            try:
                self.state_manager.update_session_status(
                    session_id, 'completed', datetime.now()
                )
                count += 1
            except Exception:
                pass
        return count
    
    def _repair_invalid_json(self, issue: Dict[str, Any]) -> int:
        """Attempt to repair or remove invalid JSON files."""
        count = 0
        for json_path in issue.get('details', []):
            try:
                # For now, just backup the corrupted file
                json_file = Path(json_path)
                backup_path = json_file.with_suffix('.json.corrupted')
                shutil.move(json_file, backup_path)
                self.console.print(f"[yellow]Moved corrupted file: {json_file.name} -> {backup_path.name}[/yellow]")
                count += 1
            except Exception:
                pass
        return count
    
    def create_backup(self) -> Path:
        """Create a backup of the archive."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{self.archive_path.name}_backup_{timestamp}"
        backup_path = self.archive_path.parent / backup_name
        
        shutil.copytree(self.archive_path, backup_path)
        return backup_path
    
    def create_compressed_archive(
        self,
        output_path: Optional[str] = None,
        format: str = "zip",
        compression_level: str = "normal",
        exclude_patterns: List[str] = None
    ) -> str:
        """
        Create compressed archive.
        
        Args:
            output_path: Output file path
            format: Archive format ('zip', 'tar.gz')
            compression_level: Compression level
            exclude_patterns: Patterns to exclude
            
        Returns:
            Path to created archive
        """
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"{self.archive_path.name}_archive_{timestamp}.{format}"
        
        exclude_set = set(exclude_patterns or [])
        
        if format.lower() == "zip":
            return self._create_zip_archive(output_path, compression_level, exclude_set)
        elif format.lower() in ["tar.gz", "tgz"]:
            return self._create_tar_archive(output_path, compression_level, exclude_set)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _create_zip_archive(self, output_path: str, compression_level: str, exclude_set: Set[str]) -> str:
        """Create ZIP archive."""
        compression_map = {
            "none": zipfile.ZIP_STORED,
            "normal": zipfile.ZIP_DEFLATED,
            "max": zipfile.ZIP_BZIP2
        }
        
        compression = compression_map.get(compression_level, zipfile.ZIP_DEFLATED)
        
        with zipfile.ZipFile(output_path, 'w', compression=compression) as zf:
            for file_path in self.archive_path.rglob("*"):
                if file_path.is_file():
                    # Check exclusion patterns
                    if any(file_path.match(pattern) for pattern in exclude_set):
                        continue
                    
                    arcname = file_path.relative_to(self.archive_path)
                    zf.write(file_path, arcname)
        
        return output_path
    
    def _create_tar_archive(self, output_path: str, compression_level: str, exclude_set: Set[str]) -> str:
        """Create TAR.GZ archive."""
        mode = "w:gz"  # Always use gzip compression for tar
        
        with tarfile.open(output_path, mode) as tf:
            for file_path in self.archive_path.rglob("*"):
                if file_path.is_file():
                    # Check exclusion patterns
                    if any(file_path.match(pattern) for pattern in exclude_set):
                        continue
                    
                    arcname = file_path.relative_to(self.archive_path)
                    tf.add(file_path, arcname)
        
        return output_path
    
    def generate_statistics(self, detailed: bool = False) -> Dict[str, Any]:
        """Generate comprehensive archive statistics."""
        try:
            files = list(self.archive_path.rglob("*"))
            media_files = [f for f in files if f.is_file() and f.suffix.lower() in 
                          {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.webm', '.mov'}]
            metadata_files = [f for f in files if f.is_file() and f.suffix.lower() == '.json']
            
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            
            stats = {
                "audit_timestamp": datetime.now().isoformat(),
                "archive_path": str(self.archive_path.resolve()),
                "total_files": len([f for f in files if f.is_file()]),
                "total_directories": len([f for f in files if f.is_dir()]),
                "media_files": len(media_files),
                "metadata_files": len(metadata_files),
                "total_size_bytes": total_size,
                "total_size_formatted": _format_bytes(total_size)
            }
            
            if detailed:
                # File type breakdown
                file_types = {}
                for f in media_files:
                    ext = f.suffix.lower()
                    file_types[ext] = file_types.get(ext, 0) + 1
                
                stats["file_types"] = file_types
                stats["average_file_size"] = total_size / len(media_files) if media_files else 0
                
                # Date range analysis
                if metadata_files:
                    dates = []
                    for json_file in metadata_files[:100]:  # Sample to avoid long processing
                        try:
                            with open(json_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            created_utc = data.get('created_utc')
                            if created_utc:
                                dates.append(created_utc)
                        except Exception:
                            continue
                    
                    if dates:
                        stats["earliest_post"] = min(dates)
                        stats["latest_post"] = max(dates)
                        stats["date_range_days"] = (max(dates) - min(dates)) / 86400  # Convert to days
                
                # Database statistics if available
                if self.state_manager:
                    try:
                        db_report = self.state_manager.check_integrity()
                        stats["database_statistics"] = db_report.get('statistics', {})
                    except Exception:
                        pass
            
            return stats
            
        except Exception as e:
            return {"error": str(e), "audit_timestamp": datetime.now().isoformat()}


# Create the audit sub-application
app = typer.Typer(
    name="audit",
    help="Audit and repair downloaded archives",
    rich_markup_mode="rich",
    no_args_is_help=True,
)


@app.command("check")
def audit_check(
    archive_path: Annotated[str, typer.Argument(help="Path to archive directory to audit")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    fix_issues: Annotated[bool, typer.Option("--fix", help="Automatically fix detected issues")] = False,
    check_integrity: Annotated[bool, typer.Option("--integrity", help="Perform deep integrity checks")] = False,
):
    """
    Check archive integrity and identify issues.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Basic check: [green]redditdl audit check ./downloads[/green]
    • Deep integrity: [green]redditdl audit check ./downloads --integrity[/green]
    • Auto-fix issues: [green]redditdl audit check ./downloads --fix[/green]
    """
    # Load configuration
    cli_args = build_cli_args(
        config=config,
        verbose=verbose,
    )
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    
    print_header("Archive Audit", f"Checking: {archive_path}")
    
    archive_dir = Path(archive_path)
    if not archive_dir.exists():
        console.print(f"[red]Error: Archive directory does not exist: {archive_path}[/red]")
        raise typer.Exit(1)
    
    if not archive_dir.is_dir():
        console.print(f"[red]Error: Path is not a directory: {archive_path}[/red]")
        raise typer.Exit(1)
    
    console.print(f"[cyan]Auditing archive at: {archive_dir.resolve()}[/cyan]")
    
    if app_config.verbose:
        print_config_summary(app_config, target=f"Archive: {archive_path}")
    
    # Create and use ArchiveAuditor for actual audit functionality
    try:
        auditor = ArchiveAuditor(archive_dir, app_config)
        issues_found = auditor.audit_archive(check_integrity=check_integrity, verbose=app_config.verbose)
        
        if issues_found:
            _display_audit_results(issues_found)
            
            if fix_issues:
                console.print("[yellow]Auto-fixing enabled...[/yellow]")
                repair_results = auditor.repair_archive(issues_found, create_backup=True)
                _display_repair_results(repair_results)
            else:
                if confirm_action("Would you like to fix these issues?"):
                    repair_results = auditor.repair_archive(issues_found, create_backup=True)
                    _display_repair_results(repair_results)
        else:
            console.print("[bold green]✓ Archive audit completed - no issues found![/bold green]")
    
    except Exception as e:
        console.print(f"[red]Error during audit: {str(e)}[/red]")
        if app_config.verbose:
            console.print_exception(show_locals=True)
        raise typer.Exit(1)


@app.command("repair")
def audit_repair(
    archive_path: Annotated[str, typer.Argument(help="Path to archive directory to repair")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    backup: Annotated[bool, typer.Option("--backup", help="Create backup before repair")] = True,
    force: Annotated[bool, typer.Option("--force", help="Force repair without confirmation")] = False,
):
    """
    Repair corrupted or missing metadata in archive.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Repair with backup: [green]redditdl audit repair ./downloads[/green]
    • Force repair: [green]redditdl audit repair ./downloads --force --no-backup[/green]
    """
    # Load configuration
    cli_args = build_cli_args(
        config=config,
        verbose=verbose,
    )
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    
    print_header("Archive Repair", f"Repairing: {archive_path}")
    
    archive_dir = Path(archive_path)
    if not archive_dir.exists():
        console.print(f"[red]Error: Archive directory does not exist: {archive_path}[/red]")
        raise typer.Exit(1)
    
    if app_config.verbose:
        print_config_summary(app_config, target=f"Archive: {archive_path}")
    
    if not force:
        warning_text = (
            "[yellow]This will attempt to repair metadata and fix issues in your archive.[/yellow]\n\n"
            f"Archive path: [cyan]{archive_dir.resolve()}[/cyan]\n"
            f"Backup enabled: [{'green' if backup else 'red'}]{backup}[/{'green' if backup else 'red'}]\n\n"
            "[bold]This operation may modify your files. Continue?[/bold]"
        )
        
        if not confirm_action(warning_text):
            console.print("[yellow]Repair cancelled by user[/yellow]")
            raise typer.Exit(0)
    
    # Create and use ArchiveAuditor for repair functionality
    try:
        auditor = ArchiveAuditor(archive_dir, app_config)
        
        # First, run an audit to identify issues
        console.print("[cyan]Scanning archive for issues...[/cyan]")
        issues_found = auditor.audit_archive(check_integrity=True, verbose=app_config.verbose)
        
        if not issues_found:
            console.print("[bold green]✓ No issues found - archive is healthy![/bold green]")
            return
        
        # Display issues found
        console.print(f"[yellow]Found {len(issues_found)} issues to repair:[/yellow]")
        _display_audit_results(issues_found)
        
        # Perform repairs
        console.print("[cyan]Performing repairs...[/cyan]")
        repair_results = auditor.repair_archive(issues_found, create_backup=backup)
        
        # Display repair results
        _display_repair_results(repair_results)
        
        # Run final verification
        console.print("[cyan]Verifying repairs...[/cyan]")
        remaining_issues = auditor.audit_archive(check_integrity=False, verbose=False)
        
        if not remaining_issues:
            console.print("[bold green]✓ All issues successfully repaired![/bold green]")
        else:
            console.print(f"[yellow]Warning: {len(remaining_issues)} issues remain after repair[/yellow]")
            if app_config.verbose:
                _display_audit_results(remaining_issues)
    
    except Exception as e:
        console.print(f"[red]Error during repair: {str(e)}[/red]")
        if app_config.verbose:
            console.print_exception(show_locals=True)
        raise typer.Exit(1)


@app.command("compress")
def audit_compress(
    archive_path: Annotated[str, typer.Argument(help="Path to archive directory to compress")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    output: Annotated[Optional[str], typer.Option("--output", "-o", help="Output archive file path")] = None,
    format: Annotated[str, typer.Option("--format", help="Archive format")] = "zip",
    compression: Annotated[str, typer.Option("--compression", help="Compression level")] = "normal",
    exclude: Annotated[Optional[List[str]], typer.Option("--exclude", help="Patterns to exclude")] = None,
):
    """
    Create compressed archive from downloads.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Create ZIP: [green]redditdl audit compress ./downloads[/green]
    • Custom output: [green]redditdl audit compress ./downloads -o backup.tar.gz --format tar.gz[/green]
    • Exclude patterns: [green]redditdl audit compress ./downloads --exclude "*.tmp" "*.log"[/green]
    """
    # Load configuration
    cli_args = build_cli_args(
        config=config,
        verbose=verbose,
    )
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    
    print_header("Archive Compression", f"Compressing: {archive_path}")
    
    archive_dir = Path(archive_path)
    if not archive_dir.exists():
        console.print(f"[red]Error: Archive directory does not exist: {archive_path}[/red]")
        raise typer.Exit(1)
    
    if app_config.verbose:
        print_config_summary(app_config, target=f"Archive: {archive_path}")
    
    # Generate output filename if not provided
    if not output:
        output = f"{archive_dir.name}_backup.{format}"
    
    console.print(f"[cyan]Input directory: {archive_dir.resolve()}[/cyan]")
    console.print(f"[cyan]Output archive: {output}[/cyan]")
    console.print(f"[cyan]Format: {format}[/cyan]")
    console.print(f"[cyan]Compression: {compression}[/cyan]")
    
    if exclude:
        console.print(f"[cyan]Excluding patterns: {', '.join(exclude)}[/cyan]")
    
    # Create and use ArchiveAuditor for compression functionality
    try:
        auditor = ArchiveAuditor(archive_dir, app_config)
        
        # Validate format
        valid_formats = ["zip", "tar.gz"]
        if format not in valid_formats:
            console.print(f"[red]Error: Unsupported format '{format}'. Valid formats: {', '.join(valid_formats)}[/red]")
            raise typer.Exit(1)
        
        # Validate compression level
        valid_compression = ["none", "fast", "normal", "best"]
        if compression not in valid_compression:
            console.print(f"[red]Error: Invalid compression level '{compression}'. Valid levels: {', '.join(valid_compression)}[/red]")
            raise typer.Exit(1)
        
        # Convert exclude patterns to list if provided
        exclude_patterns = exclude or []
        
        # Compress the archive
        console.print(f"[cyan]Creating {format.upper()} archive...[/cyan]")
        
        result = auditor.compress_archive(
            output_path=Path(output),
            archive_format=format,
            compression_level=compression,
            exclude_patterns=exclude_patterns
        )
        
        if result.get("success", False):
            output_size = result.get("output_size", 0)
            original_size = result.get("original_size", 0)
            files_compressed = result.get("files_compressed", 0)
            compression_ratio = result.get("compression_ratio", 0)
            
            console.print(f"[bold green]✓ Archive created successfully![/bold green]")
            console.print(f"[cyan]Output file: {result.get('output_path', output)}[/cyan]")
            console.print(f"[cyan]Files compressed: {files_compressed:,}[/cyan]")
            console.print(f"[cyan]Original size: {_format_bytes(original_size)}[/cyan]")
            console.print(f"[cyan]Compressed size: {_format_bytes(output_size)}[/cyan]")
            console.print(f"[cyan]Compression ratio: {compression_ratio:.1%}[/cyan]")
            
            if result.get("excluded_files"):
                console.print(f"[yellow]Excluded {len(result['excluded_files'])} files[/yellow]")
                if app_config.verbose:
                    for excluded in result["excluded_files"][:10]:  # Show first 10
                        console.print(f"  [dim]- {excluded}[/dim]")
                    if len(result["excluded_files"]) > 10:
                        console.print(f"  [dim]... and {len(result['excluded_files']) - 10} more[/dim]")
        else:
            console.print(f"[red]✗ Archive creation failed: {result.get('error', 'Unknown error')}[/red]")
            raise typer.Exit(1)
    
    except Exception as e:
        console.print(f"[red]Error during compression: {str(e)}[/red]")
        if app_config.verbose:
            console.print_exception(show_locals=True)
        raise typer.Exit(1)


@app.command("stats")
def audit_stats(
    archive_path: Annotated[str, typer.Argument(help="Path to archive directory to analyze")],
    
    # Configuration
    config: Annotated[Optional[str], typer.Option("--config", "-c", help="Configuration file path")] = None,
    
    # Core settings
    verbose: Annotated[Optional[bool], typer.Option("--verbose", "-v", help="Enable verbose output")] = None,
    detailed: Annotated[bool, typer.Option("--detailed", "-d", help="Show detailed statistics")] = False,
    export: Annotated[Optional[str], typer.Option("--export", help="Export stats to file")] = None,
):
    """
    Display archive statistics and metadata summary.
    
    [bold cyan]Examples:[/bold cyan]
    
    • Basic stats: [green]redditdl audit stats ./downloads[/green]
    • Detailed analysis: [green]redditdl audit stats ./downloads --detailed[/green]
    • Export to file: [green]redditdl audit stats ./downloads --export stats.json[/green]
    """
    # Load configuration
    cli_args = build_cli_args(
        config=config,
        verbose=verbose,
    )
    app_config = load_config_from_cli(config_file=config, cli_args=cli_args)
    
    print_header("Archive Statistics", f"Analyzing: {archive_path}")
    
    archive_dir = Path(archive_path)
    if not archive_dir.exists():
        console.print(f"[red]Error: Archive directory does not exist: {archive_path}[/red]")
        raise typer.Exit(1)
    
    if app_config.verbose:
        print_config_summary(app_config, target=f"Archive: {archive_path}")
    
    # Create and use ArchiveAuditor for statistics generation
    try:
        auditor = ArchiveAuditor(archive_dir, app_config)
        
        # Generate comprehensive statistics
        console.print("[cyan]Generating archive statistics...[/cyan]")
        stats = auditor.generate_statistics(detailed=detailed)
        
        # Display statistics
        _display_archive_stats(stats, detailed)
        
        # Export statistics if requested
        if export:
            console.print(f"[cyan]Exporting statistics to: {export}[/cyan]")
            
            export_path = Path(export)
            export_format = export_path.suffix.lower()
            
            if export_format == '.json':
                # Export as JSON
                with open(export_path, 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, default=str)
                console.print(f"[green]✓ Statistics exported to {export_path}[/green]")
            
            elif export_format == '.csv':
                # Export as CSV using ExporterFactory
                try:
                    from redditdl.exporters.base import ExporterRegistry
                    exporter_registry = ExporterRegistry()
                    csv_exporter = exporter_registry.get_exporter('csv')
                    
                    # Flatten stats for CSV export
                    flattened_stats = _flatten_stats_for_csv(stats)
                    csv_exporter.export([flattened_stats], export_path)
                    console.print(f"[green]✓ Statistics exported to {export_path}[/green]")
                except ImportError:
                    console.print("[yellow]CSV export requires ExporterRegistry - falling back to JSON[/yellow]")
                    json_path = export_path.with_suffix('.json')
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(stats, f, indent=2, default=str)
                    console.print(f"[green]✓ Statistics exported to {json_path}[/green]")
            
            else:
                # Default to JSON for unknown formats
                console.print(f"[yellow]Unknown format '{export_format}', defaulting to JSON[/yellow]")
                json_path = export_path.with_suffix('.json')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(stats, f, indent=2, default=str)
                console.print(f"[green]✓ Statistics exported to {json_path}[/green]")
    
    except Exception as e:
        console.print(f"[red]Error generating statistics: {str(e)}[/red]")
        if app_config.verbose:
            console.print_exception(show_locals=True)
        raise typer.Exit(1)


def _simulate_audit_check(archive_dir: Path, verbose: bool, check_integrity: bool) -> List[dict]:
    """Simulate audit check and return issues found."""
    import random
    
    # Simulate scanning files
    with console.status("[bold green]Scanning archive..."):
        # Count files for simulation
        try:
            files = list(archive_dir.rglob("*"))
            media_files = [f for f in files if f.is_file() and f.suffix.lower() in ['.jpg', '.png', '.mp4', '.gif']]
            metadata_files = [f for f in files if f.is_file() and f.suffix.lower() in ['.json']]
        except Exception:
            files = []
            media_files = []
            metadata_files = []
    
    issues = []
    
    # Simulate finding some issues
    if len(media_files) > 5:
        # Simulate missing metadata
        issues.append({
            "type": "missing_metadata",
            "severity": "warning",
            "message": f"Found {random.randint(1, 3)} media files without metadata",
            "count": random.randint(1, 3)
        })
    
    if len(files) > 10:
        # Simulate corrupted files
        issues.append({
            "type": "corrupted_file",
            "severity": "error", 
            "message": f"Found {random.randint(0, 2)} potentially corrupted files",
            "count": random.randint(0, 2)
        })
    
    if check_integrity and len(media_files) > 0:
        # Simulate integrity issues
        issues.append({
            "type": "integrity_check",
            "severity": "info",
            "message": f"Performed deep integrity check on {len(media_files)} files",
            "count": len(media_files)
        })
    
    return issues


def _display_audit_results(issues: List[dict]) -> None:
    """Display audit results in a formatted table."""
    table = Table(title="Audit Results")
    table.add_column("Type", style="cyan")
    table.add_column("Severity", style="yellow")
    table.add_column("Description", style="white")
    table.add_column("Count", style="green")
    
    for issue in issues:
        severity_color = {
            "error": "red",
            "warning": "yellow", 
            "info": "blue"
        }.get(issue["severity"], "white")
        
        table.add_row(
            issue["type"].replace("_", " ").title(),
            f"[{severity_color}]{issue['severity'].upper()}[/{severity_color}]",
            issue["message"],
            str(issue["count"])
        )
    
    console.print(table)


def _fix_audit_issues(issues: List[dict], archive_dir: Path) -> None:
    """Simulate fixing audit issues."""
    console.print("[bold]Fixing issues...[/bold]")
    
    for issue in issues:
        if issue["type"] == "missing_metadata":
            console.print(f"[yellow]• Regenerating missing metadata files...[/yellow]")
        elif issue["type"] == "corrupted_file":
            console.print(f"[red]• Marking corrupted files for manual review...[/red]")
        elif issue["type"] == "integrity_check":
            console.print(f"[blue]• Integrity check completed[/blue]")
    
    console.print("[bold green]✓ Issues have been addressed![/bold green]")


def _display_repair_results(repair_results: List[Dict[str, Any]]) -> None:
    """Display repair operation results in a formatted table."""
    if not repair_results:
        console.print("[bold green]✓ No repairs needed![/bold green]")
        return
    
    table = Table(title="Repair Results")
    table.add_column("Operation", style="cyan")
    table.add_column("Status", style="yellow")
    table.add_column("Description", style="white")
    table.add_column("Count", style="green")
    
    total_fixed = 0
    total_failed = 0
    
    for result in repair_results:
        status_color = "green" if result.get("success", False) else "red"
        status_text = "SUCCESS" if result.get("success", False) else "FAILED"
        
        if result.get("success", False):
            total_fixed += result.get("count", 0)
        else:
            total_failed += result.get("count", 0)
        
        table.add_row(
            result.get("operation", "Unknown").replace("_", " ").title(),
            f"[{status_color}]{status_text}[/{status_color}]",
            result.get("description", "No description"),
            str(result.get("count", 0))
        )
    
    console.print(table)
    
    # Summary
    if total_fixed > 0:
        console.print(f"[bold green]✓ Successfully repaired {total_fixed} issues[/bold green]")
    if total_failed > 0:
        console.print(f"[bold red]✗ Failed to repair {total_failed} issues[/bold red]")


def _generate_archive_stats(archive_dir: Path, detailed: bool) -> dict:
    """Generate statistics for the archive directory."""
    try:
        files = list(archive_dir.rglob("*"))
        media_files = [f for f in files if f.is_file() and f.suffix.lower() in ['.jpg', '.png', '.mp4', '.gif', '.webp']]
        metadata_files = [f for f in files if f.is_file() and f.suffix.lower() in ['.json']]
        
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        
        stats = {
            "total_files": len([f for f in files if f.is_file()]),
            "total_directories": len([f for f in files if f.is_dir()]),
            "media_files": len(media_files),
            "metadata_files": len(metadata_files),
            "total_size": total_size,
            "archive_path": str(archive_dir.resolve())
        }
        
        if detailed:
            # Calculate additional detailed stats
            file_types = {}
            for f in media_files:
                ext = f.suffix.lower()
                file_types[ext] = file_types.get(ext, 0) + 1
            
            stats["file_types"] = file_types
            stats["average_file_size"] = total_size / len(media_files) if media_files else 0
        
        return stats
        
    except Exception as e:
        console.print(f"[red]Error generating stats: {e}[/red]")
        return {"error": str(e)}


def _display_archive_stats(stats: dict, detailed: bool) -> None:
    """Display archive statistics in a formatted panel."""
    if "error" in stats:
        console.print(f"[red]Failed to generate statistics: {stats['error']}[/red]")
        return
    
    stats_text = f"""[cyan]Archive Path:[/cyan] {stats['archive_path']}
[cyan]Total Files:[/cyan] {stats['total_files']:,}
[cyan]Directories:[/cyan] {stats['total_directories']:,}
[cyan]Media Files:[/cyan] {stats['media_files']:,}
[cyan]Metadata Files:[/cyan] {stats['metadata_files']:,}
[cyan]Total Size:[/cyan] {_format_bytes(stats['total_size'])}"""
    
    if detailed and "file_types" in stats:
        stats_text += "\n\n[bold]File Types:[/bold]"
        for ext, count in stats["file_types"].items():
            stats_text += f"\n[cyan]{ext}:[/cyan] {count:,}"
        
        if stats["average_file_size"] > 0:
            stats_text += f"\n\n[cyan]Average File Size:[/cyan] {_format_bytes(stats['average_file_size'])}"
    
    console.print(Panel(stats_text, title="[bold]Archive Statistics[/bold]", border_style="green"))


def _format_bytes(bytes_count: float) -> str:
    """Format bytes in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.1f} PB"


def _flatten_stats_for_csv(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested statistics dictionary for CSV export."""
    flattened = {}
    
    for key, value in stats.items():
        if isinstance(value, dict):
            # Flatten nested dictionaries
            for subkey, subvalue in value.items():
                flattened[f"{key}_{subkey}"] = subvalue
        elif isinstance(value, list):
            # Convert lists to comma-separated strings
            flattened[key] = ', '.join(str(v) for v in value)
        else:
            flattened[key] = value
    
    return flattened