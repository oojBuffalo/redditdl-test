"""
Tests for StateManager

Tests the SQLite-based state management system including session
creation, post tracking, download management, and recovery.
"""

import pytest
import tempfile
import json
from pathlib import Path
from datetime import datetime
from redditdl.core.state.manager import StateManager
from redditdl.core.config.models import AppConfig


class TestStateManager:
    """Test StateManager functionality."""
    
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
    def sample_config(self):
        """Create sample configuration for testing."""
        return AppConfig(
            scraping={
                'api_mode': False,
                'post_limit': 20
            },
            output={
                'output_dir': 'test_downloads'
            }
        )
    
    @pytest.fixture
    def sample_post_data(self):
        """Create sample post data for testing."""
        return {
            'id': 'test_post_123',
            'title': 'Test Post Title',
            'author': 'test_user',
            'subreddit': 'test_subreddit',
            'url': 'https://example.com/post',
            'score': 100,
            'created_utc': 1640995200,
            'is_nsfw': False
        }
    
    def test_database_initialization(self, temp_db):
        """Test database is properly initialized with schema."""
        manager = StateManager(temp_db)
        
        # Check that database file exists
        assert temp_db.exists()
        
        # Check that tables exist
        conn = manager._get_connection()
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['sessions', 'posts', 'downloads', 'metadata']
        for table in expected_tables:
            assert table in tables
    
    def test_create_session(self, state_manager, sample_config):
        """Test session creation."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        assert session_id is not None
        assert isinstance(session_id, str)
        
        # Verify session exists in database
        session = state_manager.get_session(session_id)
        assert session is not None
        assert session['target_type'] == 'user'
        assert session['target_value'] == 'test_user'
        assert session['status'] == 'active'
    
    def test_duplicate_active_session_fails(self, state_manager, sample_config):
        """Test that creating duplicate active session fails."""
        # Create first session
        session_id1 = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Try to create duplicate session
        with pytest.raises(ValueError, match="Active session .* already exists"):
            state_manager.create_session(
                config=sample_config,
                target_type='user',
                target_value='test_user'
            )
    
    def test_save_and_get_posts(self, state_manager, sample_config, sample_post_data):
        """Test saving and retrieving posts."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Save post
        state_manager.save_post(session_id, sample_post_data)
        
        # Retrieve posts
        posts = state_manager.get_posts(session_id)
        assert len(posts) == 1
        assert posts[0]['id'] == sample_post_data['id']
        assert posts[0]['post_data']['title'] == sample_post_data['title']
        assert posts[0]['status'] == 'pending'
    
    def test_mark_post_processed(self, state_manager, sample_config, sample_post_data):
        """Test marking posts as processed."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.save_post(session_id, sample_post_data)
        
        # Mark as processed
        state_manager.mark_post_processed(sample_post_data['id'], 'processed')
        
        # Verify status updated
        posts = state_manager.get_posts(session_id, status='processed')
        assert len(posts) == 1
        assert posts[0]['status'] == 'processed'
    
    def test_download_management(self, state_manager, sample_config, sample_post_data):
        """Test download tracking functionality."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.save_post(session_id, sample_post_data)
        
        # Add download
        download_id = state_manager.add_download(
            post_id=sample_post_data['id'],
            session_id=session_id,
            url='https://example.com/image.jpg',
            filename='test_image.jpg'
        )
        
        assert download_id is not None
        
        # Mark download as started
        state_manager.mark_download_started(download_id)
        
        # Mark download as completed
        state_manager.mark_download_completed(
            download_id,
            file_size=1024,
            checksum='abc123'
        )
        
        # Verify download status
        downloads = state_manager.get_downloads(session_id, status='completed')
        assert len(downloads) == 1
        assert downloads[0]['status'] == 'completed'
        assert downloads[0]['file_size'] == 1024
        assert downloads[0]['checksum'] == 'abc123'
    
    def test_download_failure(self, state_manager, sample_config, sample_post_data):
        """Test handling download failures."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.save_post(session_id, sample_post_data)
        
        download_id = state_manager.add_download(
            post_id=sample_post_data['id'],
            session_id=session_id,
            url='https://example.com/image.jpg',
            filename='test_image.jpg'
        )
        
        # Mark download as failed
        error_message = "Network timeout"
        state_manager.mark_download_failed(download_id, error_message)
        
        downloads = state_manager.get_downloads(session_id, status='failed')
        assert len(downloads) == 1
        assert downloads[0]['status'] == 'failed'
        assert downloads[0]['error_message'] == error_message
    
    def test_metadata_storage(self, state_manager, sample_config):
        """Test session metadata storage and retrieval."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Store different types of metadata
        state_manager.set_metadata(session_id, 'string_key', 'string_value', 'string')
        state_manager.set_metadata(session_id, 'number_key', 42, 'number')
        state_manager.set_metadata(session_id, 'boolean_key', True, 'boolean')
        state_manager.set_metadata(session_id, 'json_key', {'nested': 'object'}, 'json')
        
        # Retrieve individual metadata
        assert state_manager.get_metadata(session_id, 'string_key') == 'string_value'
        assert state_manager.get_metadata(session_id, 'number_key') == 42
        assert state_manager.get_metadata(session_id, 'boolean_key') is True
        assert state_manager.get_metadata(session_id, 'json_key') == {'nested': 'object'}
        
        # Retrieve all metadata
        all_metadata = state_manager.get_metadata(session_id)
        assert len(all_metadata) == 4
        assert all_metadata['string_key'] == 'string_value'
    
    def test_session_status_updates(self, state_manager, sample_config):
        """Test updating session status."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Update to completed
        end_time = datetime.now()
        state_manager.update_session_status(session_id, 'completed', end_time)
        
        session = state_manager.get_session(session_id)
        assert session['status'] == 'completed'
        assert session['end_time'] is not None
    
    def test_resume_state(self, state_manager, sample_config, sample_post_data):
        """Test getting resume state for interrupted sessions."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Add some posts
        state_manager.save_post(session_id, sample_post_data)
        
        # Add another post and mark as processed
        processed_post = sample_post_data.copy()
        processed_post['id'] = 'processed_post_456'
        state_manager.save_post(session_id, processed_post)
        state_manager.mark_post_processed('processed_post_456', 'processed')
        
        # Get resume state
        resume_state = state_manager.get_resume_state(session_id)
        
        assert resume_state['can_resume'] is True
        assert len(resume_state['pending_posts']) == 1
        assert resume_state['statistics']['total_posts'] == 2
        assert resume_state['statistics']['processed_posts'] == 1
    
    def test_list_sessions(self, state_manager, sample_config):
        """Test listing sessions with filters."""
        # Create multiple sessions
        session1 = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='user1'
        )
        
        session2 = state_manager.create_session(
            config=sample_config,
            target_type='subreddit',
            target_value='subreddit1'
        )
        
        # Complete one session
        state_manager.update_session_status(session2, 'completed', datetime.now())
        
        # List all sessions
        all_sessions = state_manager.list_sessions()
        assert len(all_sessions) == 2
        
        # List only active sessions
        active_sessions = state_manager.list_sessions(status='active')
        assert len(active_sessions) == 1
        assert active_sessions[0]['id'] == session1
        
        # List by target type
        user_sessions = state_manager.list_sessions(target_type='user')
        assert len(user_sessions) == 1
        assert user_sessions[0]['target_value'] == 'user1'
    
    def test_integrity_check(self, state_manager, sample_config):
        """Test database integrity checking."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        report = state_manager.check_integrity()
        
        assert report['database_ok'] is True
        assert 'sessions' in report['statistics']
        assert 'posts' in report['statistics']
        assert 'downloads' in report['statistics']
        assert report['statistics']['sessions']['total_sessions'] == 1
    
    def test_cleanup_old_sessions(self, state_manager, sample_config):
        """Test cleaning up old sessions."""
        # Create and complete a session
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        state_manager.update_session_status(session_id, 'completed', datetime.now())
        
        # Clean up should not remove recent sessions
        deleted_count = state_manager.cleanup_old_sessions(days_old=30)
        assert deleted_count == 0
        
        # Verify session still exists
        session = state_manager.get_session(session_id)
        assert session is not None
    
    def test_config_hash_generation(self, state_manager):
        """Test configuration hash generation for session deduplication."""
        config1 = AppConfig(
            scraping={'api_mode': True, 'post_limit': 20},
            output={'output_dir': 'downloads'}
        )
        
        config2 = AppConfig(
            scraping={'api_mode': True, 'post_limit': 20},
            output={'output_dir': 'downloads'}
        )
        
        config3 = AppConfig(
            scraping={'api_mode': False, 'post_limit': 20},
            output={'output_dir': 'downloads'}
        )
        
        hash1 = state_manager._generate_config_hash(config1)
        hash2 = state_manager._generate_config_hash(config2)
        hash3 = state_manager._generate_config_hash(config3)
        
        # Same configurations should produce same hash
        assert hash1 == hash2
        
        # Different configurations should produce different hash
        assert hash1 != hash3
    
    def test_database_connection_management(self, state_manager):
        """Test database connection management and cleanup."""
        # Test connection creation
        conn1 = state_manager._get_connection()
        conn2 = state_manager._get_connection()
        
        # Should return same connection (thread-local)
        assert conn1 is conn2
        
        # Test connection cleanup
        state_manager.close()
        
        # New connection should be created after close
        conn3 = state_manager._get_connection()
        assert conn3 is not conn1
    
    def test_transaction_rollback(self, state_manager, sample_config):
        """Test transaction rollback on errors."""
        session_id = state_manager.create_session(
            config=sample_config,
            target_type='user',
            target_value='test_user'
        )
        
        # Test that errors in transactions are rolled back
        try:
            with state_manager._transaction() as conn:
                conn.execute("INSERT INTO posts (id, session_id, post_data, status) VALUES (?, ?, ?, ?)",
                           ('test_post', session_id, '{"id": "test"}', 'pending'))
                # Cause an error
                conn.execute("INSERT INTO posts (id, session_id, post_data, status) VALUES (?, ?, ?, ?)",
                           ('test_post', session_id, '{"id": "test"}', 'pending'))  # Duplicate ID
        except Exception:
            pass
        
        # Verify transaction was rolled back
        posts = state_manager.get_posts(session_id)
        assert len(posts) == 0