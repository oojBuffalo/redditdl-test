"""
Migration utilities for converting JSON state to SQLite.

Provides tools to migrate from the old JSON-based session files
to the new SQLite-based state management system.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from .manager import StateManager
from ..config.models import AppConfig


logger = logging.getLogger(__name__)


def find_json_session_files(search_dir: Path = None) -> List[Path]:
    """
    Find existing JSON session files in common locations.
    
    Args:
        search_dir: Directory to search (default: current directory)
        
    Returns:
        List of JSON session file paths
    """
    if search_dir is None:
        search_dir = Path.cwd()
    
    json_files = []
    
    # Common patterns for JSON session files
    patterns = [
        "*.session.json",
        "session_*.json",
        "redditdl_*.json",
        ".redditdl/*.json",
        "downloads/*.json",
        "sessions/*.json"
    ]
    
    for pattern in patterns:
        json_files.extend(search_dir.glob(pattern))
    
    # Filter to only actual session files (basic validation)
    session_files = []
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check if it looks like a session file
            if _is_session_file(data):
                session_files.append(file_path)
                
        except (json.JSONDecodeError, IOError):
            logger.debug(f"Skipping invalid JSON file: {file_path}")
            continue
    
    return session_files


def _is_session_file(data: Dict[str, Any]) -> bool:
    """
    Check if JSON data appears to be a session file.
    
    Args:
        data: Parsed JSON data
        
    Returns:
        True if data looks like a session file
    """
    # Look for common session file indicators
    indicators = [
        'posts', 'downloads', 'session_id', 'target_user',
        'config', 'scraping_config', 'metadata', 'created_at'
    ]
    
    found_indicators = sum(1 for key in indicators if key in data)
    return found_indicators >= 2  # Require at least 2 indicators


def migrate_json_session(
    json_path: Path,
    state_manager: StateManager,
    default_config: Optional[AppConfig] = None
) -> str:
    """
    Migrate a single JSON session file to SQLite.
    
    Args:
        json_path: Path to JSON session file
        state_manager: StateManager instance
        default_config: Default config to use if not found in JSON
        
    Returns:
        Created session ID
        
    Raises:
        ValueError: If JSON file is invalid or migration fails
    """
    logger.info(f"Migrating JSON session: {json_path}")
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        raise ValueError(f"Cannot read JSON file {json_path}: {e}")
    
    # Extract session information
    session_info = _extract_session_info(data, json_path)
    
    # Use provided config or create minimal default
    if default_config is None:
        default_config = AppConfig()
    
    # Create session
    session_id = state_manager.create_session(
        config=default_config,
        target_type=session_info['target_type'],
        target_value=session_info['target_value'],
        session_id=session_info['session_id']
    )
    
    # Migrate posts if present
    posts = data.get('posts', [])
    if isinstance(posts, dict):
        # Handle both list and dict formats
        posts = list(posts.values())
    
    for post_data in posts:
        try:
            # Normalize post data to expected format
            normalized_post = _normalize_post_data(post_data)
            state_manager.save_post(
                session_id=session_id,
                post_data=normalized_post,
                status='processed'  # Assume old posts were processed
            )
        except Exception as e:
            logger.warning(f"Failed to migrate post {post_data.get('id', 'unknown')}: {e}")
    
    # Migrate downloads if present
    downloads = data.get('downloads', [])
    for download_data in downloads:
        try:
            download_id = state_manager.add_download(
                post_id=download_data.get('post_id', 'unknown'),
                session_id=session_id,
                url=download_data.get('url', ''),
                filename=download_data.get('filename', ''),
                local_path=download_data.get('local_path')
            )
            
            # Mark as completed if successful
            if download_data.get('status') == 'completed':
                state_manager.mark_download_completed(
                    download_id=download_id,
                    file_size=download_data.get('file_size'),
                    checksum=download_data.get('checksum')
                )
            elif download_data.get('status') == 'failed':
                state_manager.mark_download_failed(
                    download_id=download_id,
                    error_message=download_data.get('error', 'Migration: status was failed')
                )
                
        except Exception as e:
            logger.warning(f"Failed to migrate download {download_data}: {e}")
    
    # Migrate metadata
    metadata = data.get('metadata', {})
    for key, value in metadata.items():
        try:
            # Determine value type
            if isinstance(value, dict) or isinstance(value, list):
                value_type = 'json'
            elif isinstance(value, bool):
                value_type = 'boolean'
            elif isinstance(value, (int, float)):
                value_type = 'number'
            else:
                value_type = 'string'
            
            state_manager.set_metadata(
                session_id=session_id,
                key=key,
                value=value,
                value_type=value_type
            )
        except Exception as e:
            logger.warning(f"Failed to migrate metadata {key}: {e}")
    
    # Update session status based on original data
    original_status = data.get('status', 'completed')
    if original_status in ['completed', 'failed', 'paused']:
        end_time = None
        if 'end_time' in data:
            try:
                end_time = datetime.fromisoformat(data['end_time'])
            except ValueError:
                pass
        
        state_manager.update_session_status(
            session_id=session_id,
            status=original_status,
            end_time=end_time
        )
    
    logger.info(f"Successfully migrated session {session_id}")
    return session_id


def _extract_session_info(data: Dict[str, Any], json_path: Path) -> Dict[str, str]:
    """
    Extract session information from JSON data.
    
    Args:
        data: Parsed JSON data
        json_path: Path to JSON file (for fallback info)
        
    Returns:
        Dictionary with session_id, target_type, target_value
    """
    # Try to extract session ID
    session_id = data.get('session_id')
    if not session_id:
        # Generate from filename
        session_id = json_path.stem
    
    # Try to extract target information
    target_type = 'user'  # Default assumption
    target_value = 'unknown'
    
    # Look for target indicators
    if 'target_user' in data:
        target_type = 'user'
        target_value = data['target_user']
    elif 'target_subreddit' in data:
        target_type = 'subreddit'
        target_value = data['target_subreddit']
    elif 'target_url' in data:
        target_type = 'url'
        target_value = data['target_url']
    elif 'config' in data:
        config = data['config']
        if 'target_user' in config:
            target_type = 'user'
            target_value = config['target_user']
    
    # Try to infer from filename
    if target_value == 'unknown':
        filename = json_path.stem.lower()
        if 'user_' in filename:
            target_type = 'user'
            target_value = filename.split('user_')[1].split('_')[0]
        elif 'subreddit_' in filename:
            target_type = 'subreddit'
            target_value = filename.split('subreddit_')[1].split('_')[0]
    
    return {
        'session_id': session_id,
        'target_type': target_type,
        'target_value': target_value
    }


def _normalize_post_data(post_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize post data to standard PostMetadata format.
    
    Args:
        post_data: Raw post data from JSON
        
    Returns:
        Normalized post data dictionary
    """
    # Ensure required fields exist
    normalized = {
        'id': post_data.get('id', 'unknown'),
        'title': post_data.get('title', ''),
        'url': post_data.get('url', ''),
        'author': post_data.get('author', ''),
        'subreddit': post_data.get('subreddit', ''),
        'created_utc': post_data.get('created_utc', 0),
        'score': post_data.get('score', 0),
        'num_comments': post_data.get('num_comments', 0),
        'is_nsfw': post_data.get('is_nsfw', False),
        'is_self': post_data.get('is_self', False),
        'selftext': post_data.get('selftext', ''),
        'media_url': post_data.get('media_url'),
        'date_iso': post_data.get('date_iso', ''),
    }
    
    # Add any additional fields that were present
    for key, value in post_data.items():
        if key not in normalized:
            normalized[key] = value
    
    return normalized


def migrate_json_to_sqlite(
    json_files: Optional[List[Path]] = None,
    search_dir: Optional[Path] = None,
    db_path: Optional[Path] = None,
    default_config: Optional[AppConfig] = None,
    backup: bool = True
) -> Dict[str, Any]:
    """
    Migrate JSON session files to SQLite database.
    
    Args:
        json_files: Specific JSON files to migrate (if None, auto-discover)
        search_dir: Directory to search for JSON files
        db_path: SQLite database path
        default_config: Default configuration for sessions
        backup: Whether to backup original JSON files
        
    Returns:
        Migration report dictionary
    """
    logger.info("Starting JSON to SQLite migration")
    
    # Discover JSON files if not provided
    if json_files is None:
        json_files = find_json_session_files(search_dir)
    
    if not json_files:
        logger.info("No JSON session files found to migrate")
        return {
            'status': 'completed',
            'files_found': 0,
            'files_migrated': 0,
            'errors': []
        }
    
    logger.info(f"Found {len(json_files)} JSON session files to migrate")
    
    # Initialize state manager
    state_manager = StateManager(db_path)
    
    # Migration results
    report = {
        'status': 'completed',
        'files_found': len(json_files),
        'files_migrated': 0,
        'errors': [],
        'migrated_sessions': []
    }
    
    for json_path in json_files:
        try:
            session_id = migrate_json_session(
                json_path=json_path,
                state_manager=state_manager,
                default_config=default_config
            )
            
            report['files_migrated'] += 1
            report['migrated_sessions'].append(session_id)
            
            # Backup original file if requested
            if backup:
                backup_path = json_path.with_suffix('.json.backup')
                json_path.rename(backup_path)
                logger.info(f"Backed up original file to: {backup_path}")
            
        except Exception as e:
            error_msg = f"Failed to migrate {json_path}: {str(e)}"
            logger.error(error_msg)
            report['errors'].append(error_msg)
    
    if report['errors']:
        report['status'] = 'partial'
    
    logger.info(f"Migration completed: {report['files_migrated']}/{report['files_found']} files migrated")
    
    return report


def create_migration_script(output_path: Path = None) -> Path:
    """
    Create a standalone migration script for users.
    
    Args:
        output_path: Path for the migration script
        
    Returns:
        Path to created script
    """
    if output_path is None:
        output_path = Path.cwd() / "migrate_to_sqlite.py"
    
    script_content = '''#!/usr/bin/env python3
"""
RedditDL JSON to SQLite Migration Script

This script migrates existing JSON session files to the new SQLite-based
state management system.
"""

import sys
from pathlib import Path

# Add the RedditDL directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from core.state.migrations import migrate_json_to_sqlite

def main():
    print("RedditDL JSON to SQLite Migration")
    print("=" * 40)
    
    # Run migration
    report = migrate_json_to_sqlite(backup=True)
    
    print(f"Files found: {report['files_found']}")
    print(f"Files migrated: {report['files_migrated']}")
    
    if report['errors']:
        print(f"Errors: {len(report['errors'])}")
        for error in report['errors']:
            print(f"  - {error}")
    
    if report['migrated_sessions']:
        print("Migrated sessions:")
        for session_id in report['migrated_sessions']:
            print(f"  - {session_id}")
    
    print(f"Migration status: {report['status']}")

if __name__ == "__main__":
    main()
'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(script_content)
    
    # Make script executable on Unix systems
    try:
        output_path.chmod(0o755)
    except OSError:
        pass  # Windows doesn't support chmod
    
    return output_path