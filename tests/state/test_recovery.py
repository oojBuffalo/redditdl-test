"""
Tests for SessionRecovery

Tests session recovery, repair, and maintenance functionality.
"""

import pytest
import tempfile
import json
from pathlib import Path
from datetime import datetime, timedelta
from redditdl.core.state.manager import StateManager
from redditdl.core.state.recovery import SessionRecovery
from redditdl.core.config.models import AppConfig


class TestSessionRecovery:
    """Test SessionRecovery functionality."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = Path(f.name)
        
        yield db_path
        
        # Cleanup
        if db_path.exists():
            db_path.unlink()
    
    @pytest.fixture
    def state_manager(self, temp_db):
        """Create StateManager instance with temporary database."""
        return StateManager(temp_db)
    
    @pytest.fixture
    def recovery(self, state_manager):
        """Create SessionRecovery instance."""
        return SessionRecovery(state_manager)
    
    @pytest.fixture
    def sample_config(self):
        """Create sample configuration for testing."""
        return AppConfig(
            scraping={'api_mode': False, 'post_limit': 20},
            output={'output_dir': 'test_downloads'}
        )
    
    def test_find_resumable_sessions(self, recovery, state_manager, sample_config):
        """Test finding sessions that can be resumed."""
        # Create an active session with pending posts
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Add a pending post
        post_data = {
            'id': 'pending_post_123',
            'title': 'Pending Post',
            'author': 'test_user',
            'subreddit': 'test_subreddit',
            'url': 'https://example.com/pending'
        }
        state_manager.save_post(session_id, post_data, status='pending')
        
        # Find resumable sessions
        resumable = recovery.find_resumable_sessions()
        
        assert len(resumable) == 1
        assert resumable[0]['session']['id'] == session_id
        assert resumable[0]['resume_state']['can_resume'] is True
        assert len(resumable[0]['resume_state']['pending_posts']) == 1
    
    def test_resume_session(self, recovery, state_manager, sample_config):
        """Test resuming an interrupted session."""
        # Create paused session
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.update_session_status(session_id, 'paused')
        
        # Add pending work
        post_data = {
            'id': 'pending_post_123',
            'title': 'Pending Post',
            'author': 'test_user',
            'subreddit': 'test_subreddit',
            'url': 'https://example.com/pending'
        }
        state_manager.save_post(session_id, post_data, status='pending')
        
        # Resume session
        result = recovery.resume_session(session_id)
        
        assert result['success'] is True
        assert result['session_id'] == session_id
        assert result['pending_posts'] == 1
        
        # Verify session status updated
        session = state_manager.get_session(session_id)
        assert session['status'] == 'active'
    
    def test_resume_completed_session(self, recovery, state_manager, sample_config):
        """Test attempting to resume completed session."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.update_session_status(session_id, 'completed', datetime.now())
        
        result = recovery.resume_session(session_id)
        
        assert result['success'] is False
        assert 'no pending work' in result['error'].lower()
    
    def test_repair_session_healthy(self, recovery, state_manager, sample_config):
        """Test repairing a healthy session."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Add a post and download
        post_data = {
            'id': 'test_post_123',
            'title': 'Test Post',
            'author': 'test_user',
            'subreddit': 'test_subreddit',
            'url': 'https://example.com/test'
        }
        state_manager.save_post(session_id, post_data, status='processed')
        
        download_id = state_manager.add_download(
            post_id='test_post_123',
            session_id=session_id,
            url='https://example.com/image.jpg',
            filename='test.jpg'
        )
        state_manager.mark_download_completed(download_id)
        
        # Repair session
        report = recovery.repair_session(session_id)
        
        assert report['success'] is True
        assert 'No issues found' in report['repairs_performed']
    
    def test_repair_session_missing_files(self, recovery, state_manager, sample_config, tmp_path):
        """Test repairing session with missing download files."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Add download with non-existent file
        download_id = state_manager.add_download(
            post_id='test_post',
            session_id=session_id,
            url='https://example.com/image.jpg',
            filename=str(tmp_path / 'nonexistent.jpg')
        )
        state_manager.mark_download_completed(download_id)
        
        # Repair session
        report = recovery.repair_session(session_id)
        
        assert report['success'] is True
        assert any('missing files' in issue for issue in report['issues_found'])
        assert any('marked' in repair and 'missing files' in repair 
                  for repair in report['repairs_performed'])
        
        # Verify download marked as failed
        downloads = state_manager.get_downloads(session_id, status='failed')
        assert len(downloads) == 1
    
    def test_cleanup_abandoned_sessions(self, recovery, state_manager, sample_config):
        """Test cleaning up abandoned sessions."""
        # Create and complete an old session
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.update_session_status(session_id, 'completed', datetime.now())
        
        # Manually update created_at to be old
        conn = state_manager._get_connection()
        old_date = (datetime.now() - timedelta(days=35)).isoformat()
        conn.execute("UPDATE sessions SET created_at = ? WHERE id = ?", 
                    (old_date, session_id))
        conn.commit()
        
        # Cleanup old sessions
        report = recovery.cleanup_abandoned_sessions(max_age_days=30)
        
        assert report['sessions_removed'] == 1
        
        # Verify session was deleted
        session = state_manager.get_session(session_id)
        assert session is None
    
    def test_validate_file_integrity(self, recovery, state_manager, sample_config, tmp_path):
        """Test validating downloaded file integrity."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Create a test file
        test_file = tmp_path / 'test_image.jpg'
        test_content = b'fake image content'
        test_file.write_bytes(test_content)
        
        # Calculate checksum
        import hashlib
        expected_checksum = hashlib.sha256(test_content).hexdigest()
        
        # Add download with checksum
        download_id = state_manager.add_download(
            post_id='test_post',
            session_id=session_id,
            url='https://example.com/image.jpg',
            filename=str(test_file)
        )
        state_manager.mark_download_completed(
            download_id,
            file_size=len(test_content),
            checksum=expected_checksum
        )
        
        # Validate integrity
        report = recovery.validate_file_integrity(session_id)
        
        assert report['files_checked'] == 1
        assert report['files_valid'] == 1
        assert report['files_missing'] == 0
        assert report['files_corrupted'] == 0
    
    def test_validate_file_integrity_corrupted(self, recovery, state_manager, sample_config, tmp_path):
        """Test detecting corrupted files."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Create a test file
        test_file = tmp_path / 'test_image.jpg'
        test_file.write_bytes(b'fake image content')
        
        # Add download with wrong checksum
        download_id = state_manager.add_download(
            post_id='test_post',
            session_id=session_id,
            url='https://example.com/image.jpg',
            filename=str(test_file)
        )
        state_manager.mark_download_completed(
            download_id,
            checksum='wrong_checksum'
        )
        
        # Validate integrity
        report = recovery.validate_file_integrity(session_id)
        
        assert report['files_checked'] == 1
        assert report['files_valid'] == 0
        assert report['files_corrupted'] == 1
        assert any('checksum mismatch' in issue.lower() for issue in report['issues'])
    
    def test_export_session_data(self, recovery, state_manager, sample_config, tmp_path):
        """Test exporting session data for backup."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Add some data
        post_data = {
            'id': 'test_post_123',
            'title': 'Test Post',
            'author': 'test_user',
            'subreddit': 'test_subreddit',
            'url': 'https://example.com/test'
        }
        state_manager.save_post(session_id, post_data)
        state_manager.set_metadata(session_id, 'test_key', 'test_value')
        
        export_path = tmp_path / 'session_export.json'
        
        # Export session data
        result = recovery.export_session_data(session_id, export_path)
        
        assert result['success'] is True
        assert result['posts_exported'] == 1
        assert export_path.exists()
        
        # Verify export content
        with open(export_path) as f:
            export_data = json.load(f)
        
        assert 'session' in export_data
        assert 'posts' in export_data
        assert 'metadata' in export_data
        assert export_data['session']['id'] == session_id
        assert len(export_data['posts']) == 1
        assert 'test_key' in export_data['metadata']
    
    def test_export_nonexistent_session(self, recovery, tmp_path):
        """Test exporting non-existent session."""
        export_path = tmp_path / 'export.json'
        
        result = recovery.export_session_data('nonexistent', export_path)
        
        assert result['success'] is False
        assert 'not found' in result['error'].lower()
    
    def test_find_resumable_sessions_age_filter(self, recovery, state_manager, sample_config):
        """Test age filtering in resumable session search."""
        # Create an old active session
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Add pending post
        post_data = {
            'id': 'pending_post_123',
            'title': 'Pending Post',
            'author': 'test_user',
            'subreddit': 'test_subreddit',
            'url': 'https://example.com/pending'
        }
        state_manager.save_post(session_id, post_data, status='pending')
        
        # Manually update created_at to be old
        conn = state_manager._get_connection()
        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        conn.execute("UPDATE sessions SET created_at = ? WHERE id = ?", 
                    (old_date, session_id))
        conn.commit()
        
        # Search with age filter
        resumable = recovery.find_resumable_sessions(max_age_days=7)
        assert len(resumable) == 0
        
        resumable = recovery.find_resumable_sessions(max_age_days=14)
        assert len(resumable) == 1