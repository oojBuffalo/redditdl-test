"""
Tests for migration utilities

Tests JSON to SQLite migration functionality.
"""

import pytest
import tempfile
import json
from pathlib import Path
from redditdl.core.state.migrations import (
    find_json_session_files,
    migrate_json_session,
    migrate_json_to_sqlite,
    _is_session_file,
    _extract_session_info,
    _normalize_post_data
)
from redditdl.core.state.manager import StateManager
from redditdl.core.config.models import AppConfig


class TestMigrations:
    """Test migration functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
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
            scraping={'api_mode': False, 'post_limit': 20},
            output={'output_dir': 'test_downloads'}
        )
    
    @pytest.fixture
    def sample_json_session(self, temp_dir):
        """Create sample JSON session file."""
        session_data = {
            'session_id': 'test_session_123',
            'target_user': 'test_user',
            'status': 'completed',
            'created_at': '2023-01-01T10:00:00',
            'end_time': '2023-01-01T11:00:00',
            'posts': [
                {
                    'id': 'post_123',
                    'title': 'Test Post',
                    'author': 'test_user',
                    'subreddit': 'test_subreddit',
                    'url': 'https://example.com/post',
                    'score': 100,
                    'created_utc': 1672574400,
                    'is_nsfw': False
                },
                {
                    'id': 'post_456',
                    'title': 'Another Post',
                    'author': 'test_user',
                    'subreddit': 'test_subreddit',
                    'url': 'https://example.com/post2',
                    'score': 50,
                    'created_utc': 1672578000,
                    'is_nsfw': True
                }
            ],
            'downloads': [
                {
                    'post_id': 'post_123',
                    'url': 'https://example.com/image.jpg',
                    'filename': 'image.jpg',
                    'local_path': '/downloads/image.jpg',
                    'status': 'completed',
                    'file_size': 1024
                },
                {
                    'post_id': 'post_456',
                    'url': 'https://example.com/video.mp4',
                    'filename': 'video.mp4',
                    'status': 'failed',
                    'error': 'Network timeout'
                }
            ],
            'metadata': {
                'scraper_type': 'YARS',
                'total_runtime': 300.5,
                'user_agent': 'RedditDL/1.0'
            }
        }
        
        json_file = temp_dir / 'test_session.json'
        with open(json_file, 'w') as f:
            json.dump(session_data, f, indent=2)
        
        return json_file
    
    def test_is_session_file(self):
        """Test session file detection."""
        # Valid session file
        valid_session = {
            'posts': [],
            'session_id': 'test_123',
            'target_user': 'test_user'
        }
        assert _is_session_file(valid_session) is True
        
        # Another valid pattern
        valid_session2 = {
            'downloads': [],
            'config': {'target_user': 'test'},
            'metadata': {}
        }
        assert _is_session_file(valid_session2) is True
        
        # Invalid file
        invalid_file = {
            'random_data': 'value',
            'not_a_session': True
        }
        assert _is_session_file(invalid_file) is False
    
    def test_extract_session_info(self, temp_dir):
        """Test extracting session info from JSON data."""
        # Test with explicit session info
        data = {
            'session_id': 'explicit_session',
            'target_user': 'test_user'
        }
        json_path = temp_dir / 'test.json'
        
        info = _extract_session_info(data, json_path)
        assert info['session_id'] == 'explicit_session'
        assert info['target_type'] == 'user'
        assert info['target_value'] == 'test_user'
        
        # Test with config-based info
        data2 = {
            'config': {
                'target_user': 'config_user'
            }
        }
        
        info2 = _extract_session_info(data2, json_path)
        assert info2['target_type'] == 'user'
        assert info2['target_value'] == 'config_user'
        
        # Test filename fallback
        filename_path = temp_dir / 'user_testuser_session.json'
        data3 = {}
        
        info3 = _extract_session_info(data3, filename_path)
        assert info3['target_type'] == 'user'
        assert info3['target_value'] == 'testuser'
    
    def test_normalize_post_data(self):
        """Test post data normalization."""
        raw_post = {
            'id': 'test_post',
            'title': 'Test Title',
            'author': 'test_author',
            'score': 100,
            'custom_field': 'custom_value'
        }
        
        normalized = _normalize_post_data(raw_post)
        
        # Check required fields are present
        assert normalized['id'] == 'test_post'
        assert normalized['title'] == 'Test Title'
        assert normalized['author'] == 'test_author'
        assert normalized['score'] == 100
        
        # Check defaults for missing fields
        assert normalized['subreddit'] == ''
        assert normalized['is_nsfw'] is False
        assert normalized['num_comments'] == 0
        
        # Check custom fields are preserved
        assert normalized['custom_field'] == 'custom_value'
    
    def test_find_json_session_files(self, temp_dir):
        """Test finding JSON session files."""
        # Create various files
        session_file1 = temp_dir / 'session_123.json'
        session_file2 = temp_dir / 'user_test.session.json'
        random_json = temp_dir / 'random.json'
        not_json = temp_dir / 'file.txt'
        
        # Create session files
        session_data = {'posts': [], 'session_id': 'test'}
        with open(session_file1, 'w') as f:
            json.dump(session_data, f)
        with open(session_file2, 'w') as f:
            json.dump(session_data, f)
        
        # Create non-session JSON
        with open(random_json, 'w') as f:
            json.dump({'not': 'a session'}, f)
        
        # Create non-JSON file
        with open(not_json, 'w') as f:
            f.write('not json')
        
        found_files = find_json_session_files(temp_dir)
        
        assert len(found_files) == 2
        assert session_file1 in found_files
        assert session_file2 in found_files
        assert random_json not in found_files
        assert not_json not in found_files
    
    def test_migrate_json_session(self, sample_json_session, state_manager, sample_config):
        """Test migrating a single JSON session file."""
        session_id = migrate_json_session(
            json_path=sample_json_session,
            state_manager=state_manager,
            default_config=sample_config
        )
        
        assert session_id is not None
        
        # Verify session was created
        session = state_manager.get_session(session_id)
        assert session is not None
        assert session['target_type'] == 'user'
        assert session['target_value'] == 'test_user'
        assert session['status'] == 'completed'
        
        # Verify posts were migrated
        posts = state_manager.get_posts(session_id)
        assert len(posts) == 2
        
        post_ids = [post['id'] for post in posts]
        assert 'post_123' in post_ids
        assert 'post_456' in post_ids
        
        # Verify downloads were migrated
        downloads = state_manager.get_downloads(session_id)
        assert len(downloads) == 2
        
        completed_downloads = state_manager.get_downloads(session_id, status='completed')
        failed_downloads = state_manager.get_downloads(session_id, status='failed')
        
        assert len(completed_downloads) == 1
        assert len(failed_downloads) == 1
        
        # Verify metadata was migrated
        metadata = state_manager.get_metadata(session_id)
        assert 'scraper_type' in metadata
        assert metadata['scraper_type'] == 'YARS'
        assert metadata['total_runtime'] == 300.5
    
    def test_migrate_json_to_sqlite(self, temp_dir, temp_db, sample_config):
        """Test full JSON to SQLite migration."""
        # Create multiple session files
        session_files = []
        for i in range(3):
            session_data = {
                'session_id': f'session_{i}',
                'target_user': f'user_{i}',
                'posts': [
                    {
                        'id': f'post_{i}_1',
                        'title': f'Post {i}.1',
                        'author': f'user_{i}',
                        'subreddit': 'test',
                        'url': f'https://example.com/post_{i}_1'
                    }
                ],
                'downloads': []
            }
            
            json_file = temp_dir / f'session_{i}.json'
            with open(json_file, 'w') as f:
                json.dump(session_data, f)
            session_files.append(json_file)
        
        # Create invalid JSON file
        invalid_file = temp_dir / 'invalid.json'
        with open(invalid_file, 'w') as f:
            f.write('invalid json content')
        
        # Run migration
        report = migrate_json_to_sqlite(
            json_files=session_files + [invalid_file],
            db_path=temp_db,
            default_config=sample_config,
            backup=False
        )
        
        assert report['files_found'] == 4
        assert report['files_migrated'] == 3  # 3 valid files
        assert len(report['errors']) == 1  # 1 invalid file
        assert len(report['migrated_sessions']) == 3
        
        # Verify sessions were created
        state_manager = StateManager(temp_db)
        sessions = state_manager.list_sessions()
        assert len(sessions) == 3
    
    def test_migrate_with_backup(self, temp_dir, temp_db, sample_config):
        """Test migration with file backup."""
        # Create session file
        session_data = {
            'session_id': 'backup_test',
            'target_user': 'test_user',
            'posts': []
        }
        
        json_file = temp_dir / 'backup_session.json'
        with open(json_file, 'w') as f:
            json.dump(session_data, f)
        
        # Run migration with backup
        report = migrate_json_to_sqlite(
            json_files=[json_file],
            db_path=temp_db,
            default_config=sample_config,
            backup=True
        )
        
        assert report['files_migrated'] == 1
        
        # Verify original file was backed up
        backup_file = json_file.with_suffix('.json.backup')
        assert backup_file.exists()
        assert not json_file.exists()  # Original should be moved
        
        # Verify backup content
        with open(backup_file) as f:
            backup_data = json.load(f)
        assert backup_data['session_id'] == 'backup_test'
    
    def test_migrate_auto_discovery(self, temp_dir, temp_db, sample_config):
        """Test automatic JSON file discovery."""
        # Create session files with different naming patterns
        patterns = [
            'session_auto.json',
            'user_discovery.session.json',
            'redditdl_test.json'
        ]
        
        for pattern in patterns:
            session_data = {
                'posts': [],
                'session_id': pattern.replace('.json', '').replace('.session', ''),
                'target_user': 'auto_user'
            }
            
            json_file = temp_dir / pattern
            with open(json_file, 'w') as f:
                json.dump(session_data, f)
        
        # Run migration with auto-discovery
        report = migrate_json_to_sqlite(
            search_dir=temp_dir,
            db_path=temp_db,
            default_config=sample_config,
            backup=False
        )
        
        assert report['files_found'] == 3
        assert report['files_migrated'] == 3
    
    def test_migrate_invalid_json(self, temp_dir, temp_db, sample_config):
        """Test handling invalid JSON files."""
        # Create invalid JSON file
        invalid_file = temp_dir / 'invalid.json'
        with open(invalid_file, 'w') as f:
            f.write('{ invalid json content')
        
        # Try to migrate
        with pytest.raises(ValueError, match="Cannot read JSON file"):
            migrate_json_session(
                json_path=invalid_file,
                state_manager=StateManager(temp_db),
                default_config=sample_config
            )
    
    def test_migrate_missing_file(self, temp_db, sample_config):
        """Test handling missing JSON files."""
        missing_file = Path('/nonexistent/file.json')
        
        with pytest.raises(ValueError, match="Cannot read JSON file"):
            migrate_json_session(
                json_path=missing_file,
                state_manager=StateManager(temp_db),
                default_config=sample_config
            )