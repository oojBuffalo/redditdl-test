"""
Session Recovery and Repair Tools

Provides utilities for recovering from interrupted sessions,
repairing corrupted state, and performing maintenance operations.
"""

import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

from .manager import StateManager
from ..config.models import AppConfig


logger = logging.getLogger(__name__)


class SessionRecovery:
    """
    Handles session recovery and repair operations.
    """
    
    def __init__(self, state_manager: StateManager):
        """
        Initialize recovery handler.
        
        Args:
            state_manager: StateManager instance
        """
        self.state_manager = state_manager
    
    def find_resumable_sessions(self, max_age_days: int = 7) -> List[Dict[str, Any]]:
        """
        Find sessions that can be resumed.
        
        Args:
            max_age_days: Maximum age of sessions to consider
            
        Returns:
            List of resumable session info
        """
        # Get recent active or paused sessions
        sessions = self.state_manager.list_sessions(status='active', limit=100)
        sessions.extend(self.state_manager.list_sessions(status='paused', limit=100))
        
        resumable = []
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        for session in sessions:
            try:
                created_at = datetime.fromisoformat(session['created_at'])
                if created_at < cutoff_date:
                    continue
                
                # Check if session has unfinished work
                resume_state = self.state_manager.get_resume_state(session['id'])
                if resume_state['can_resume']:
                    session_info = {
                        'session': session,
                        'resume_state': resume_state,
                        'age_hours': (datetime.now() - created_at).total_seconds() / 3600
                    }
                    resumable.append(session_info)
                    
            except Exception as e:
                logger.warning(f"Error checking session {session['id']}: {e}")
        
        # Sort by most recent first
        resumable.sort(key=lambda x: x['session']['created_at'], reverse=True)
        return resumable
    
    def resume_session(self, session_id: str) -> Dict[str, Any]:
        """
        Resume an interrupted session.
        
        Args:
            session_id: Session to resume
            
        Returns:
            Resume operation report
        """
        logger.info(f"Resuming session: {session_id}")
        
        try:
            resume_state = self.state_manager.get_resume_state(session_id)
            
            if not resume_state['can_resume']:
                return {
                    'success': False,
                    'error': 'Session has no pending work to resume'
                }
            
            # Update session status to active if paused
            session = resume_state['session']
            if session['status'] == 'paused':
                self.state_manager.update_session_status(session_id, 'active')
            
            # Prepare resume report
            report = {
                'success': True,
                'session_id': session_id,
                'pending_posts': len(resume_state['pending_posts']),
                'failed_downloads': len(resume_state['failed_downloads']),
                'statistics': resume_state['statistics']
            }
            
            logger.info(f"Session {session_id} ready for resume: "
                       f"{report['pending_posts']} pending posts, "
                       f"{report['failed_downloads']} failed downloads")
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to resume session {session_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def repair_session(self, session_id: str) -> Dict[str, Any]:
        """
        Repair a corrupted session.
        
        Args:
            session_id: Session to repair
            
        Returns:
            Repair operation report
        """
        logger.info(f"Repairing session: {session_id}")
        
        report = {
            'success': True,
            'session_id': session_id,
            'repairs_performed': [],
            'issues_found': [],
            'errors': []
        }
        
        try:
            session = self.state_manager.get_session(session_id)
            if not session:
                return {
                    'success': False,
                    'error': f'Session {session_id} not found'
                }
            
            # Check for orphaned posts
            orphaned_posts = self._find_orphaned_posts(session_id)
            if orphaned_posts:
                report['issues_found'].append(f"Found {len(orphaned_posts)} orphaned posts")
                # Note: Currently we don't auto-delete orphaned posts as they might be recoverable
            
            # Check for posts without downloads
            posts_without_downloads = self._find_posts_without_downloads(session_id)
            if posts_without_downloads:
                report['issues_found'].append(
                    f"Found {len(posts_without_downloads)} posts without download records"
                )
            
            # Check for inconsistent download counts
            actual_counts = self._calculate_actual_download_counts(session_id)
            if (actual_counts['successful'] != session['successful_downloads'] or
                actual_counts['failed'] != session['failed_downloads']):
                
                # Fix download counts
                self.state_manager._get_connection().execute("""
                    UPDATE sessions 
                    SET successful_downloads = ?,
                        failed_downloads = ?
                    WHERE id = ?
                """, (actual_counts['successful'], actual_counts['failed'], session_id))
                
                report['repairs_performed'].append("Fixed download count discrepancies")
            
            # Check for downloads without posts
            downloads_without_posts = self._find_downloads_without_posts(session_id)
            if downloads_without_posts:
                report['issues_found'].append(
                    f"Found {len(downloads_without_posts)} downloads without post records"
                )
            
            # Verify file existence for completed downloads
            missing_files = self._verify_downloaded_files(session_id)
            if missing_files:
                report['issues_found'].append(f"Found {len(missing_files)} missing download files")
                
                # Option to mark missing files as failed
                for download_id, file_path in missing_files:
                    self.state_manager.mark_download_failed(
                        download_id, f"File missing during repair: {file_path}"
                    )
                report['repairs_performed'].append(f"Marked {len(missing_files)} missing files as failed")
            
            # Update session timestamp
            self.state_manager._get_connection().execute("""
                UPDATE sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = ?
            """, (session_id,))
            
            if not report['issues_found']:
                report['repairs_performed'].append("No issues found - session is healthy")
            
        except Exception as e:
            logger.error(f"Error during session repair: {e}")
            report['success'] = False
            report['errors'].append(str(e))
        
        return report
    
    def _find_orphaned_posts(self, session_id: str) -> List[str]:
        """Find posts that reference non-existent sessions."""
        conn = self.state_manager._get_connection()
        cursor = conn.execute("""
            SELECT p.id FROM posts p
            LEFT JOIN sessions s ON p.session_id = s.id
            WHERE p.session_id = ? AND s.id IS NULL
        """, (session_id,))
        return [row[0] for row in cursor.fetchall()]
    
    def _find_posts_without_downloads(self, session_id: str) -> List[str]:
        """Find processed posts that have no download records."""
        conn = self.state_manager._get_connection()
        cursor = conn.execute("""
            SELECT p.id FROM posts p
            LEFT JOIN downloads d ON p.id = d.post_id
            WHERE p.session_id = ? AND p.status = 'processed' AND d.id IS NULL
        """, (session_id,))
        return [row[0] for row in cursor.fetchall()]
    
    def _find_downloads_without_posts(self, session_id: str) -> List[int]:
        """Find downloads that reference non-existent posts."""
        conn = self.state_manager._get_connection()
        cursor = conn.execute("""
            SELECT d.id FROM downloads d
            LEFT JOIN posts p ON d.post_id = p.id
            WHERE d.session_id = ? AND p.id IS NULL
        """, (session_id,))
        return [row[0] for row in cursor.fetchall()]
    
    def _calculate_actual_download_counts(self, session_id: str) -> Dict[str, int]:
        """Calculate actual download counts from database."""
        conn = self.state_manager._get_connection()
        cursor = conn.execute("""
            SELECT 
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
            FROM downloads WHERE session_id = ?
        """, (session_id,))
        row = cursor.fetchone()
        return {'successful': row[0], 'failed': row[1]}
    
    def _verify_downloaded_files(self, session_id: str) -> List[Tuple[int, str]]:
        """Verify that completed downloads actually exist on disk."""
        conn = self.state_manager._get_connection()
        cursor = conn.execute("""
            SELECT id, local_path, filename FROM downloads 
            WHERE session_id = ? AND status = 'completed'
            AND (local_path IS NOT NULL OR filename IS NOT NULL)
        """, (session_id,))
        
        missing_files = []
        for row in cursor.fetchall():
            download_id, local_path, filename = row
            file_path = local_path or filename
            
            if file_path and not Path(file_path).exists():
                missing_files.append((download_id, file_path))
        
        return missing_files
    
    def cleanup_abandoned_sessions(self, max_age_days: int = 30) -> Dict[str, Any]:
        """
        Clean up old abandoned sessions.
        
        Args:
            max_age_days: Age threshold for cleanup
            
        Returns:
            Cleanup report
        """
        logger.info(f"Cleaning up sessions older than {max_age_days} days")
        
        report = {
            'sessions_removed': 0,
            'files_cleaned': 0,
            'errors': []
        }
        
        try:
            # Find old sessions
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            conn = self.state_manager._get_connection()
            
            cursor = conn.execute("""
                SELECT id FROM sessions 
                WHERE status IN ('completed', 'failed') 
                AND created_at < ?
            """, (cutoff_date.isoformat(),))
            
            old_sessions = [row[0] for row in cursor.fetchall()]
            
            for session_id in old_sessions:
                try:
                    # Get associated files before deletion
                    downloads = self.state_manager.get_downloads(session_id)
                    
                    # Delete session (cascades to posts and downloads)
                    conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
                    report['sessions_removed'] += 1
                    
                    # Optionally clean up associated files
                    # Note: This is commented out for safety - files might be valuable
                    # for file_info in downloads:
                    #     if file_info.get('local_path'):
                    #         file_path = Path(file_info['local_path'])
                    #         if file_path.exists():
                    #             file_path.unlink()
                    #             report['files_cleaned'] += 1
                    
                except Exception as e:
                    logger.error(f"Error cleaning up session {session_id}: {e}")
                    report['errors'].append(f"Session {session_id}: {str(e)}")
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Cleanup operation failed: {e}")
            report['errors'].append(str(e))
        
        return report
    
    def validate_file_integrity(self, session_id: str) -> Dict[str, Any]:
        """
        Validate integrity of downloaded files.
        
        Args:
            session_id: Session to validate
            
        Returns:
            Validation report
        """
        logger.info(f"Validating file integrity for session: {session_id}")
        
        report = {
            'files_checked': 0,
            'files_valid': 0,
            'files_missing': 0,
            'files_corrupted': 0,
            'issues': []
        }
        
        downloads = self.state_manager.get_downloads(session_id, status='completed')
        
        for download in downloads:
            download_id = download['id']
            local_path = download.get('local_path') or download.get('filename')
            stored_checksum = download.get('checksum')
            
            if not local_path:
                continue
            
            report['files_checked'] += 1
            file_path = Path(local_path)
            
            if not file_path.exists():
                report['files_missing'] += 1
                report['issues'].append(f"Missing file: {local_path}")
                
                # Mark download as failed
                self.state_manager.mark_download_failed(
                    download_id, "File missing during integrity check"
                )
                continue
            
            # Verify checksum if available
            if stored_checksum:
                try:
                    actual_checksum = self._calculate_file_checksum(file_path)
                    if actual_checksum != stored_checksum:
                        report['files_corrupted'] += 1
                        report['issues'].append(
                            f"Checksum mismatch: {local_path} "
                            f"(expected: {stored_checksum}, got: {actual_checksum})"
                        )
                    else:
                        report['files_valid'] += 1
                except Exception as e:
                    report['issues'].append(f"Error checking {local_path}: {e}")
            else:
                # No checksum to verify, assume valid if file exists
                report['files_valid'] += 1
        
        return report
    
    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def export_session_data(self, session_id: str, export_path: Path) -> Dict[str, Any]:
        """
        Export session data for backup or analysis.
        
        Args:
            session_id: Session to export
            export_path: Path for export file
            
        Returns:
            Export operation report
        """
        logger.info(f"Exporting session data: {session_id} to {export_path}")
        
        try:
            session = self.state_manager.get_session(session_id)
            if not session:
                return {
                    'success': False,
                    'error': f'Session {session_id} not found'
                }
            
            posts = self.state_manager.get_posts(session_id)
            downloads = self.state_manager.get_downloads(session_id)
            metadata = self.state_manager.get_metadata(session_id)
            
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'export_version': '1.0',
                'session': dict(session),
                'posts': posts,
                'downloads': downloads,
                'metadata': metadata
            }
            
            import json
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            return {
                'success': True,
                'export_path': str(export_path),
                'posts_exported': len(posts),
                'downloads_exported': len(downloads)
            }
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }