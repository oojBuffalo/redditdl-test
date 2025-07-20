"""
State Manager

Provides SQLite-based state management for RedditDL sessions with
atomic operations, session recovery, and integrity checking.
"""

import json
import sqlite3
import hashlib
import threading
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Union
from contextlib import contextmanager
from dataclasses import asdict
import queue
import time

from ..config.models import AppConfig
from ..monitoring.metrics import get_metrics_collector, time_operation


class ConnectionPool:
    """
    Thread-safe connection pool for SQLite database connections.
    """
    
    def __init__(self, db_path: Path, max_connections: int = 10):
        """
        Initialize connection pool.
        
        Args:
            db_path: Database file path
            max_connections: Maximum number of connections in pool
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = queue.Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()
        
        # Pre-create some connections
        for _ in range(min(3, max_connections)):
            self._pool.put(self._create_connection())
            self._created_connections += 1
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
        return conn
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool."""
        connection = None
        try:
            # Try to get from pool
            try:
                connection = self._pool.get_nowait()
            except queue.Empty:
                # Create new connection if pool is empty and under limit
                with self._lock:
                    if self._created_connections < self.max_connections:
                        connection = self._create_connection()
                        self._created_connections += 1
                    else:
                        # Wait for available connection
                        connection = self._pool.get(timeout=5.0)
            
            yield connection
            
        finally:
            if connection:
                try:
                    # Return connection to pool
                    self._pool.put_nowait(connection)
                except queue.Full:
                    # Pool is full, close the connection
                    connection.close()
                    with self._lock:
                        self._created_connections -= 1
    
    def close_all(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
        
        self._created_connections = 0


class StateManager:
    """
    Manages application state using SQLite for robust persistence.
    
    Provides session management, post tracking, download status,
    and recovery capabilities for interrupted operations.
    
    Enhanced with connection pooling for improved concurrent performance.
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None, 
                 max_connections: int = 10):
        """
        Initialize state manager with database path and connection pooling.
        
        Args:
            db_path: Path to SQLite database file (default: .redditdl/state.db)
            max_connections: Maximum number of database connections
        """
        if db_path is None:
            db_path = Path.cwd() / ".redditdl" / "state.db"
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connection pool for concurrent access
        self._connection_pool = ConnectionPool(self.db_path, max_connections)
        
        # Metrics
        self._metrics = get_metrics_collector()
        if self._metrics:
            self._metrics.counter("state.operations", "State manager operations")
            self._metrics.timer("state.operation_time", "State operation time")
        
        # Initialize database schema
        self._initialize_database()
    
    def close(self):
        """Close all database connections."""
        self._connection_pool.close_all()
    
    @contextmanager
    def _transaction(self):
        """Context manager for atomic database operations with metrics."""
        with time_operation("state.operation_time"):
            with self._connection_pool.get_connection() as conn:
                try:
                    yield conn
                    conn.commit()
                    if self._metrics:
                        self._metrics.increment("state.operations")
                except Exception:
                    conn.rollback()
                    if self._metrics:
                        self._metrics.increment("state.errors")
                    raise
    
    def _initialize_database(self) -> None:
        """Initialize database with schema from schema.sql."""
        schema_path = Path(__file__).parent / "schema.sql"
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        with self._transaction() as conn:
            conn.executescript(schema_sql)
    
    def _generate_config_hash(self, config: AppConfig) -> str:
        """Generate hash of configuration for session identification."""
        # Create a normalized config dict for hashing
        config_dict = config.model_dump(mode='json')
        
        # Remove session-specific fields that shouldn't affect hash
        exclude_fields = {'created', 'session_dir', 'verbose', 'debug'}
        for field in exclude_fields:
            config_dict.pop(field, None)
        
        config_str = json.dumps(config_dict, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:16]
    
    def create_session(
        self,
        config: AppConfig,
        target_type: str,
        target_value: str,
        session_id: Optional[str] = None
    ) -> str:
        """
        Create a new scraping session.
        
        Args:
            config: Application configuration
            target_type: Type of target ('user', 'subreddit', 'url')
            target_value: Target identifier (username, subreddit, URL)
            session_id: Optional custom session ID
            
        Returns:
            Session ID string
            
        Raises:
            ValueError: If session already exists and is active
        """
        if session_id is None:
            # Generate session ID from timestamp and target
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_id = f"{target_type}_{target_value}_{timestamp}"
        
        config_hash = self._generate_config_hash(config)
        
        with self._transaction() as conn:
            # Check if active session exists for this target/config
            cursor = conn.execute("""
                SELECT id, status FROM sessions 
                WHERE config_hash = ? AND target_type = ? AND target_value = ? 
                AND status = 'active'
            """, (config_hash, target_type, target_value))
            
            existing = cursor.fetchone()
            if existing:
                raise ValueError(
                    f"Active session {existing['id']} already exists for "
                    f"{target_type} '{target_value}'"
                )
            
            # Create new session
            conn.execute("""
                INSERT INTO sessions (
                    id, config_hash, target_type, target_value, 
                    status, start_time, metadata
                ) VALUES (?, ?, ?, ?, 'active', ?, ?)
            """, (
                session_id,
                config_hash,
                target_type,
                target_value,
                datetime.now().isoformat(),
                json.dumps({
                    'created_by': 'StateManager',
                    'config_version': config.version
                })
            ))
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session information by ID.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Session data dictionary or None if not found
        """
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM sessions WHERE id = ?
        """, (session_id,))
        
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    
    def save_post(
        self,
        session_id: str,
        post_data: Dict[str, Any],
        status: str = 'pending'
    ) -> None:
        """
        Save post metadata to the session.
        
        Args:
            session_id: Session identifier
            post_data: Post metadata dictionary (from PostMetadata.to_dict())
            status: Post processing status
        """
        post_id = post_data.get('id')
        if not post_id:
            raise ValueError("Post data must include 'id' field")
        
        with self._transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO posts (
                    id, session_id, post_data, status
                ) VALUES (?, ?, ?, ?)
            """, (post_id, session_id, json.dumps(post_data), status))
    
    def get_posts(
        self,
        session_id: str,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get posts for a session, optionally filtered by status.
        
        Args:
            session_id: Session identifier
            status: Optional status filter
            
        Returns:
            List of post dictionaries
        """
        conn = self._get_connection()
        
        if status:
            cursor = conn.execute("""
                SELECT * FROM posts 
                WHERE session_id = ? AND status = ?
                ORDER BY discovered_at
            """, (session_id, status))
        else:
            cursor = conn.execute("""
                SELECT * FROM posts 
                WHERE session_id = ?
                ORDER BY discovered_at
            """, (session_id,))
        
        posts = []
        for row in cursor.fetchall():
            post_dict = dict(row)
            post_dict['post_data'] = json.loads(post_dict['post_data'])
            posts.append(post_dict)
        
        return posts
    
    def mark_post_processed(
        self,
        post_id: str,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """
        Mark a post as processed with given status.
        
        Args:
            post_id: Post identifier
            status: New status ('processed', 'skipped', 'failed')
            error_message: Optional error message for failed posts
        """
        with self._transaction() as conn:
            conn.execute("""
                UPDATE posts 
                SET status = ?, 
                    processing_attempts = processing_attempts + 1,
                    last_attempt_at = CURRENT_TIMESTAMP,
                    error_message = ?
                WHERE id = ?
            """, (status, error_message, post_id))
            
            # Update session processed count
            cursor = conn.execute("""
                SELECT session_id FROM posts WHERE id = ?
            """, (post_id,))
            row = cursor.fetchone()
            if row:
                conn.execute("""
                    UPDATE sessions 
                    SET processed_posts = (
                        SELECT COUNT(*) FROM posts 
                        WHERE session_id = ? AND status IN ('processed', 'skipped')
                    )
                    WHERE id = ?
                """, (row['session_id'], row['session_id']))
    
    def add_download(
        self,
        post_id: str,
        session_id: str,
        url: str,
        filename: str,
        local_path: Optional[str] = None
    ) -> int:
        """
        Add a download record.
        
        Args:
            post_id: Post identifier
            session_id: Session identifier
            url: Download URL
            filename: Target filename
            local_path: Local file path (if different from filename)
            
        Returns:
            Download ID
        """
        with self._transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO downloads (
                    post_id, session_id, url, filename, local_path, 
                    status, started_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
            """, (post_id, session_id, url, filename, local_path))
            
            return cursor.lastrowid
    
    def mark_download_started(self, download_id: int) -> None:
        """Mark a download as started."""
        with self._transaction() as conn:
            conn.execute("""
                UPDATE downloads 
                SET status = 'downloading', started_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (download_id,))
    
    def mark_download_completed(
        self,
        download_id: int,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None
    ) -> None:
        """
        Mark a download as completed.
        
        Args:
            download_id: Download identifier
            file_size: Size of downloaded file in bytes
            checksum: File checksum for integrity verification
        """
        with self._transaction() as conn:
            conn.execute("""
                UPDATE downloads 
                SET status = 'completed', 
                    completed_at = CURRENT_TIMESTAMP,
                    file_size = ?,
                    checksum = ?
                WHERE id = ?
            """, (file_size, checksum, download_id))
    
    def mark_download_failed(
        self,
        download_id: int,
        error_message: str
    ) -> None:
        """
        Mark a download as failed.
        
        Args:
            download_id: Download identifier
            error_message: Error description
        """
        with self._transaction() as conn:
            conn.execute("""
                UPDATE downloads 
                SET status = 'failed',
                    download_attempts = download_attempts + 1,
                    error_message = ?
                WHERE id = ?
            """, (error_message, download_id))
    
    def get_downloads(
        self,
        session_id: str,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get downloads for a session.
        
        Args:
            session_id: Session identifier
            status: Optional status filter
            
        Returns:
            List of download dictionaries
        """
        conn = self._get_connection()
        
        if status:
            cursor = conn.execute("""
                SELECT * FROM downloads 
                WHERE session_id = ? AND status = ?
                ORDER BY started_at
            """, (session_id, status))
        else:
            cursor = conn.execute("""
                SELECT * FROM downloads 
                WHERE session_id = ?
                ORDER BY started_at
            """, (session_id,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_resume_state(self, session_id: str) -> Dict[str, Any]:
        """
        Get state information for resuming an interrupted session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Resume state dictionary with posts and downloads info
        """
        conn = self._get_connection()
        
        # Get session info
        session = self.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        
        # Get pending posts
        pending_posts = self.get_posts(session_id, status='pending')
        
        # Get failed downloads that can be retried
        failed_downloads = self.get_downloads(session_id, status='failed')
        
        # Get overall statistics
        cursor = conn.execute("""
            SELECT 
                COUNT(*) as total_posts,
                COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed_posts,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_posts
            FROM posts WHERE session_id = ?
        """, (session_id,))
        
        stats = dict(cursor.fetchone())
        
        return {
            'session': session,
            'pending_posts': pending_posts,
            'failed_downloads': failed_downloads,
            'statistics': stats,
            'can_resume': len(pending_posts) > 0 or len(failed_downloads) > 0
        }
    
    def update_session_status(
        self,
        session_id: str,
        status: str,
        end_time: Optional[datetime] = None
    ) -> None:
        """
        Update session status.
        
        Args:
            session_id: Session identifier
            status: New status ('active', 'completed', 'failed', 'paused')
            end_time: Optional end time for completed/failed sessions
        """
        with self._transaction() as conn:
            if end_time:
                conn.execute("""
                    UPDATE sessions 
                    SET status = ?, end_time = ?
                    WHERE id = ?
                """, (status, end_time.isoformat(), session_id))
            else:
                conn.execute("""
                    UPDATE sessions 
                    SET status = ?
                    WHERE id = ?
                """, (status, session_id))
    
    def set_metadata(
        self,
        session_id: str,
        key: str,
        value: Any,
        value_type: str = 'string'
    ) -> None:
        """
        Store metadata for a session.
        
        Args:
            session_id: Session identifier
            key: Metadata key
            value: Metadata value
            value_type: Value type hint ('string', 'json', 'number', 'boolean')
        """
        if value_type == 'json':
            value_str = json.dumps(value)
        else:
            value_str = str(value)
        
        with self._transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO metadata (
                    session_id, key, value, type
                ) VALUES (?, ?, ?, ?)
            """, (session_id, key, value_str, value_type))
    
    def get_metadata(
        self,
        session_id: str,
        key: Optional[str] = None
    ) -> Union[Any, Dict[str, Any]]:
        """
        Get metadata for a session.
        
        Args:
            session_id: Session identifier
            key: Optional specific key to retrieve
            
        Returns:
            Metadata value (if key specified) or dict of all metadata
        """
        conn = self._get_connection()
        
        if key:
            cursor = conn.execute("""
                SELECT value, type FROM metadata 
                WHERE session_id = ? AND key = ?
            """, (session_id, key))
            row = cursor.fetchone()
            
            if row:
                value, value_type = row['value'], row['type']
                if value_type == 'json':
                    return json.loads(value)
                elif value_type == 'number':
                    return float(value) if '.' in value else int(value)
                elif value_type == 'boolean':
                    return value.lower() in ('true', '1', 'yes')
                else:
                    return value
            return None
        else:
            cursor = conn.execute("""
                SELECT key, value, type FROM metadata 
                WHERE session_id = ?
            """, (session_id,))
            
            metadata = {}
            for row in cursor.fetchall():
                key, value, value_type = row['key'], row['value'], row['type']
                if value_type == 'json':
                    metadata[key] = json.loads(value)
                elif value_type == 'number':
                    metadata[key] = float(value) if '.' in value else int(value)
                elif value_type == 'boolean':
                    metadata[key] = value.lower() in ('true', '1', 'yes')
                else:
                    metadata[key] = value
            
            return metadata
    
    def list_sessions(
        self,
        status: Optional[str] = None,
        target_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List sessions with optional filtering.
        
        Args:
            status: Optional status filter
            target_type: Optional target type filter
            limit: Maximum number of sessions to return
            
        Returns:
            List of session dictionaries
        """
        conn = self._get_connection()
        
        query = "SELECT * FROM sessions"
        params = []
        conditions = []
        
        if status:
            conditions.append("status = ?")
            params.append(status)
        
        if target_type:
            conditions.append("target_type = ?")
            params.append(target_type)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def cleanup_old_sessions(self, days_old: int = 30) -> int:
        """
        Clean up old completed sessions.
        
        Args:
            days_old: Age threshold in days
            
        Returns:
            Number of sessions deleted
        """
        with self._transaction() as conn:
            cursor = conn.execute("""
                DELETE FROM sessions 
                WHERE status IN ('completed', 'failed') 
                AND created_at < datetime('now', '-{} days')
            """.format(days_old))
            
            return cursor.rowcount
    
    def check_integrity(self) -> Dict[str, Any]:
        """
        Check database integrity and return report.
        
        Returns:
            Integrity check report
        """
        conn = self._get_connection()
        report = {
            'database_ok': True,
            'issues': [],
            'statistics': {}
        }
        
        try:
            # Check database integrity
            cursor = conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            if result != 'ok':
                report['database_ok'] = False
                report['issues'].append(f"Database integrity check failed: {result}")
            
            # Check foreign key consistency
            cursor = conn.execute("PRAGMA foreign_key_check")
            fk_issues = cursor.fetchall()
            if fk_issues:
                report['issues'].extend([
                    f"Foreign key violation in {row[0]}" for row in fk_issues
                ])
            
            # Gather statistics
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_sessions,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_sessions,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_sessions
                FROM sessions
            """)
            report['statistics']['sessions'] = dict(cursor.fetchone())
            
            cursor = conn.execute("SELECT COUNT(*) as total_posts FROM posts")
            report['statistics']['posts'] = dict(cursor.fetchone())
            
            cursor = conn.execute("SELECT COUNT(*) as total_downloads FROM downloads")
            report['statistics']['downloads'] = dict(cursor.fetchone())
            
        except Exception as e:
            report['database_ok'] = False
            report['issues'].append(f"Integrity check error: {str(e)}")
        
        return report
    
    def close(self) -> None:
        """Close database connections."""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            delattr(self._local, 'connection')


class StateManagerError(Exception):
    """Exception raised for state management errors."""
    pass