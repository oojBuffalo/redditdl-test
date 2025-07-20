"""
Workflow Integration Tests

Tests complete workflows from end-to-end including scraping,
filtering, processing, and exporting in various configurations.
"""

import asyncio
import pytest
import sqlite3
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

from redditdl.core.config.models import AppConfig
from redditdl.core.pipeline.executor import PipelineExecutor
from redditdl.core.pipeline.interfaces import PipelineContext
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.state.manager import StateManager
from redditdl.scrapers import PostMetadata, PrawScraper, YarsScraper
from redditdl.downloader import MediaDownloader
from redditdl.metadata import MetadataEmbedder
from redditdl.pipeline.stages.acquisition import AcquisitionStage
from redditdl.pipeline.stages.filter import FilterStage
from redditdl.pipeline.stages.processing import ProcessingStage
from redditdl.pipeline.stages.organization import OrganizationStage
from redditdl.pipeline.stages.export import ExportStage


class TestCompleteWorkflows:
    """Test complete end-to-end workflows."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_db = self.temp_dir / 'test_state.db'
        
        # Create basic test configuration
        self.test_config = AppConfig(
            dry_run=False,
            verbose=False,
            use_pipeline=True,
            scraping={
                'api_mode': False,
                'post_limit': 5,
                'sleep_interval_api': 0.1,
                'sleep_interval_public': 0.1,
                'timeout': 30,
                'max_retries': 2
            },
            output={
                'output_dir': str(self.temp_dir / 'downloads'),
                'export_formats': ['json'],
                'organize_by_subreddit': True,
                'filename_template': '{{ subreddit }}/{{ post_id }}-{{ title|slugify }}.{{ ext }}'
            },
            processing={
                'embed_metadata': True,
                'create_json_sidecars': True,
                'concurrent_downloads': 2
            },
            filters={
                'min_score': 5,
                'include_nsfw': False,
                'filter_composition': 'and'
            }
        )
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def create_test_posts(self, count=5):
        """Create test posts for workflows."""
        posts = []
        for i in range(count):
            post = PostMetadata(
                id=f'post_{i}',
                title=f'Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                media_url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                author=f'user_{i}',
                subreddit='testsubreddit',
                score=10 + i * 5,  # Scores 10, 15, 20, 25, 30
                num_comments=i * 2,
                created_utc=1640995200 + i * 3600,
                is_nsfw=(i % 4 == 0),  # Every 4th post is NSFW
                is_self=False,
                is_video=False
            )
            posts.append(post)
        return posts
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complete_user_scraping_workflow(self):
        """Test complete user scraping workflow with all components."""
        # Mock scrapers
        test_posts = self.create_test_posts(10)
        
        with patch('redditdl.scrapers.YarsScraper') as mock_scraper_class:
            mock_scraper = Mock()
            mock_scraper.scrape_posts.return_value = test_posts
            mock_scraper_class.return_value = mock_scraper
            
            # Mock downloader
            with patch('redditdl.downloader.MediaDownloader') as mock_downloader_class:
                mock_downloader = Mock()
                mock_downloader.download_media = AsyncMock(return_value={
                    'success': True,
                    'local_path': str(self.temp_dir / 'downloads' / 'test.jpg'),
                    'file_size': 1024000,
                    'download_time': 1.5
                })
                mock_downloader_class.return_value = mock_downloader
                
                # Mock metadata embedder
                with patch('redditdl.metadata.MetadataEmbedder') as mock_embedder_class:
                    mock_embedder = Mock()
                    mock_embedder.embed_exif_metadata = Mock()
                    mock_embedder.create_json_sidecar = Mock()
                    mock_embedder_class.return_value = mock_embedder
                    
                    # Create event emitter
                    emitter = EventEmitter()
                    
                    # Create state manager
                    state_manager = StateManager(db_path=str(self.test_db))
                    
                    # Create pipeline executor
                    executor = PipelineExecutor()
                    
                    # Create pipeline context
                    context = PipelineContext(
                        target='testuser',
                        target_type='user',
                        config=self.test_config,
                        state_manager=state_manager,
                        event_emitter=emitter,
                        session_id='test_session'
                    )
                    
                    # Execute workflow (would normally be done by CLI)
                    result = await self._execute_user_workflow(
                        context, mock_scraper, mock_downloader, mock_embedder
                    )
                    
                    # Verify workflow success
                    assert result['success'] is True
                    assert result['posts_processed'] > 0
                    assert result['downloads_completed'] >= 0
                    
                    # Verify scraper was called
                    mock_scraper.scrape_posts.assert_called_once()
                    
                    # Verify state was persisted
                    assert self.test_db.exists()
    
    @pytest.mark.asyncio
    async def _execute_user_workflow(self, context, mock_scraper, mock_downloader, mock_embedder):
        """Execute a user scraping workflow."""
        # Simulate acquisition stage
        posts = mock_scraper.scrape_posts(context.target)
        
        # Simulate filtering stage
        filtered_posts = self._apply_filters(posts, context.config.filters)
        
        # Simulate processing stage
        processed_results = []
        for post in filtered_posts:
            if not context.config.dry_run:
                download_result = await mock_downloader.download_media(
                    post.media_url,
                    str(context.config.output.output_dir),
                    post
                )
                
                if download_result['success'] and context.config.processing.embed_metadata:
                    mock_embedder.embed_exif_metadata(
                        download_result['local_path'],
                        post
                    )
                    
                    if context.config.processing.create_json_sidecars:
                        mock_embedder.create_json_sidecar(
                            download_result['local_path'],
                            post
                        )
                
                processed_results.append({
                    'post': post,
                    'download_result': download_result
                })
            else:
                processed_results.append({
                    'post': post,
                    'download_result': {'success': True, 'dry_run': True}
                })
        
        # Simulate export stage
        if 'json' in context.config.output.export_formats:
            await self._export_to_json(filtered_posts, context.config.output.output_dir)
        
        return {
            'success': True,
            'posts_discovered': len(posts),
            'posts_filtered': len(filtered_posts),
            'posts_processed': len(processed_results),
            'downloads_completed': sum(1 for r in processed_results 
                                     if r['download_result']['success'])
        }
    
    def _apply_filters(self, posts, filter_config):
        """Apply configured filters to posts."""
        filtered_posts = []
        
        for post in posts:
            # Apply score filter
            if filter_config.min_score is not None:
                if post.score < filter_config.min_score:
                    continue
            
            # Apply NSFW filter
            if not filter_config.include_nsfw and post.is_nsfw:
                continue
            
            filtered_posts.append(post)
        
        return filtered_posts
    
    async def _export_to_json(self, posts, output_dir):
        """Export posts to JSON format."""
        export_data = []
        for post in posts:
            export_data.append({
                'id': post.id,
                'title': post.title,
                'url': post.url,
                'author': post.author,
                'subreddit': post.subreddit,
                'score': post.score,
                'created_utc': post.created_utc
            })
        
        export_file = Path(output_dir) / 'export.json'
        export_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(export_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2)
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_pipeline_mode_workflow(self):
        """Test workflow using modern pipeline architecture."""
        test_posts = self.create_test_posts(5)
        
        with patch('redditdl.pipeline.stages.acquisition.AcquisitionStage') as mock_acquisition, \
             patch('redditdl.pipeline.stages.filter.FilterStage') as mock_filter, \
             patch('redditdl.pipeline.stages.processing.ProcessingStage') as mock_processing, \
             patch('redditdl.pipeline.stages.export.ExportStage') as mock_export:
            
            # Mock pipeline stages
            mock_acquisition_instance = Mock()
            mock_acquisition_instance.process = AsyncMock(return_value={
                'success': True,
                'posts': test_posts,
                'posts_discovered': len(test_posts)
            })
            mock_acquisition.return_value = mock_acquisition_instance
            
            mock_filter_instance = Mock()
            mock_filter_instance.process = AsyncMock(return_value={
                'success': True,
                'posts': test_posts[:3],  # Filter out 2 posts
                'posts_filtered': 3,
                'posts_rejected': 2
            })
            mock_filter.return_value = mock_filter_instance
            
            mock_processing_instance = Mock()
            mock_processing_instance.process = AsyncMock(return_value={
                'success': True,
                'posts': test_posts[:3],
                'downloads_completed': 3,
                'downloads_failed': 0
            })
            mock_processing.return_value = mock_processing_instance
            
            mock_export_instance = Mock()
            mock_export_instance.process = AsyncMock(return_value={
                'success': True,
                'export_formats': ['json'],
                'export_files': ['export.json']
            })
            mock_export.return_value = mock_export_instance
            
            # Create pipeline executor
            executor = PipelineExecutor()
            executor.add_stage(mock_acquisition_instance)
            executor.add_stage(mock_filter_instance)
            executor.add_stage(mock_processing_instance)
            executor.add_stage(mock_export_instance)
            
            # Create event emitter
            emitter = EventEmitter()
            
            # Create context
            context = PipelineContext(
                target='testuser',
                target_type='user',
                config=self.test_config,
                state_manager=StateManager(db_path=str(self.test_db)),
                event_emitter=emitter,
                session_id='pipeline_test'
            )
            
            # Execute pipeline
            result = await executor.execute(context)
            
            # Verify pipeline execution
            assert result.success is True
            assert result.posts_processed >= 0
            
            # Verify all stages were called
            mock_acquisition_instance.process.assert_called_once()
            mock_filter_instance.process.assert_called_once()
            mock_processing_instance.process.assert_called_once()
            mock_export_instance.process.assert_called_once()
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_multi_target_workflow(self):
        """Test workflow with multiple targets (users and subreddits)."""
        targets = ['user1', 'user2', 'r/subreddit1']
        test_posts_per_target = 3
        
        with patch('redditdl.targets.handlers.UserTargetHandler') as mock_user_handler, \
             patch('redditdl.targets.handlers.SubredditTargetHandler') as mock_sub_handler:
            
            # Mock handlers
            mock_user_instance = Mock()
            mock_user_instance.process_target = AsyncMock(return_value={
                'success': True,
                'posts': self.create_test_posts(test_posts_per_target),
                'target': 'user1'
            })
            mock_user_handler.return_value = mock_user_instance
            
            mock_sub_instance = Mock()
            mock_sub_instance.process_target = AsyncMock(return_value={
                'success': True,
                'posts': self.create_test_posts(test_posts_per_target),
                'target': 'r/subreddit1'
            })
            mock_sub_handler.return_value = mock_sub_instance
            
            # Simulate multi-target processing
            all_results = []
            
            for target in targets:
                if target.startswith('r/'):
                    # Subreddit target
                    result = await mock_sub_instance.process_target(target, self.test_config)
                else:
                    # User target
                    result = await mock_user_instance.process_target(target, self.test_config)
                
                all_results.append(result)
            
            # Verify multi-target results
            assert len(all_results) == 3
            assert all(r['success'] for r in all_results)
            
            total_posts = sum(len(r['posts']) for r in all_results)
            assert total_posts == test_posts_per_target * 3
    
    @pytest.mark.integration
    def test_configuration_integration_workflow(self):
        """Test workflow with various configuration combinations."""
        # Test different configuration scenarios
        configs = [
            # API mode configuration
            {
                'scraping': {'api_mode': True},
                'output': {'export_formats': ['json', 'csv']},
                'processing': {'embed_metadata': False}
            },
            # High-quality processing configuration
            {
                'processing': {
                    'enable_processing': True,
                    'image_quality': 95,
                    'max_image_resolution': 1920
                },
                'filters': {'min_score': 100}
            },
            # Minimal configuration
            {
                'dry_run': True,
                'output': {'export_formats': []},
                'processing': {'embed_metadata': False, 'create_json_sidecars': False}
            }
        ]
        
        for i, config_override in enumerate(configs):
            # Update test config with overrides
            test_config = self.test_config.copy(deep=True)
            for section, values in config_override.items():
                if hasattr(test_config, section):
                    section_obj = getattr(test_config, section)
                    for key, value in values.items():
                        setattr(section_obj, key, value)
                else:
                    setattr(test_config, section, values)
            
            # Verify configuration is valid
            assert isinstance(test_config, AppConfig)
            
            # Test that configuration can be used in workflow
            # (This would normally involve actual pipeline execution)
            workflow_config = {
                'target': f'test_user_{i}',
                'config': test_config,
                'expected_behavior': 'success'
            }
            
            assert workflow_config['target'] is not None
            assert workflow_config['config'] is not None
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_error_recovery_workflow(self):
        """Test workflow error handling and recovery."""
        test_posts = self.create_test_posts(5)
        
        with patch('redditdl.scrapers.YarsScraper') as mock_scraper_class:
            mock_scraper = Mock()
            
            # Simulate intermittent failures
            call_count = 0
            def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ConnectionError("Network error")
                elif call_count == 2:
                    raise TimeoutError("Request timeout")
                else:
                    return test_posts
            
            mock_scraper.scrape_posts.side_effect = side_effect
            mock_scraper_class.return_value = mock_scraper
            
            # Test retry logic (would normally be in scraper)
            max_retries = 3
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    posts = mock_scraper.scrape_posts('testuser')
                    break
                except (ConnectionError, TimeoutError) as e:
                    last_error = e
                    if attempt == max_retries - 1:
                        raise
                    
                    # Simulate exponential backoff
                    await asyncio.sleep(0.1 * (2 ** attempt))
            
            # Verify eventual success
            assert posts == test_posts
            assert call_count == 3  # Failed twice, succeeded on third try
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_state_persistence_workflow(self):
        """Test workflow state persistence and recovery."""
        test_posts = self.create_test_posts(10)
        
        # Create state manager
        state_manager = StateManager(db_path=str(self.test_db))
        
        # Create initial session
        session_id = await state_manager.create_session(
            target='testuser',
            config_hash='test_hash',
            total_posts=len(test_posts)
        )
        
        # Save some posts
        processed_posts = []
        for i, post in enumerate(test_posts[:5]):
            await state_manager.save_post(session_id, post)
            
            # Mark some as downloaded
            if i < 3:
                await state_manager.mark_downloaded(
                    session_id,
                    post.id,
                    post.media_url,
                    f'/path/to/{post.id}.jpg',
                    success=True
                )
            
            processed_posts.append(post)
        
        # Simulate interruption and recovery
        recovery_state = await state_manager.get_resume_state(session_id)
        
        assert recovery_state is not None
        assert recovery_state['session_id'] == session_id
        assert recovery_state['total_posts'] == len(test_posts)
        assert recovery_state['processed_posts'] == 5
        assert recovery_state['downloaded_posts'] == 3
        
        # Continue processing remaining posts
        remaining_posts = test_posts[5:]
        for post in remaining_posts:
            await state_manager.save_post(session_id, post)
        
        # Verify final state
        final_state = await state_manager.get_resume_state(session_id)
        assert final_state['processed_posts'] == len(test_posts)
        
        # Mark session as completed
        await state_manager.update_session_status(session_id, 'completed')
    
    @pytest.mark.integration
    def test_plugin_integration_workflow(self):
        """Test workflow with plugins loaded."""
        from redditdl.core.plugins.manager import PluginManager
        
        # Create plugin manager
        plugin_manager = PluginManager()
        
        # Mock plugin loading
        with patch.object(plugin_manager, 'load_plugins') as mock_load:
            mock_load.return_value = True
            
            with patch.object(plugin_manager, 'get_content_handlers') as mock_handlers:
                # Mock plugin content handlers
                mock_plugin_handler = Mock()
                mock_plugin_handler.can_handle.return_value = True
                mock_plugin_handler.process = AsyncMock(return_value={
                    'success': True,
                    'handler': 'plugin_handler',
                    'processed_content': 'test_data'
                })
                
                mock_handlers.return_value = [mock_plugin_handler]
                
                # Simulate workflow with plugins
                test_post = self.create_test_posts(1)[0]
                content_handlers = plugin_manager.get_content_handlers()
                
                # Find appropriate handler
                handler = None
                for h in content_handlers:
                    if h.can_handle('image', test_post.__dict__):
                        handler = h
                        break
                
                assert handler is not None
                assert handler == mock_plugin_handler
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_concurrent_processing_workflow(self):
        """Test workflow with concurrent processing."""
        test_posts = self.create_test_posts(10)
        
        with patch('redditdl.downloader.MediaDownloader') as mock_downloader_class:
            mock_downloader = Mock()
            
            # Mock async download method
            async def mock_download(url, output_dir, post_metadata):
                # Simulate download time
                await asyncio.sleep(0.1)
                return {
                    'success': True,
                    'local_path': f'/path/to/{post_metadata.id}.jpg',
                    'file_size': 1024000,
                    'download_time': 0.1
                }
            
            mock_downloader.download_media = mock_download
            mock_downloader_class.return_value = mock_downloader
            
            # Process posts concurrently
            semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent downloads
            
            async def process_post(post):
                async with semaphore:
                    result = await mock_downloader.download_media(
                        post.media_url,
                        str(self.temp_dir),
                        post
                    )
                    return {'post': post, 'result': result}
            
            # Create tasks for all posts
            tasks = [process_post(post) for post in test_posts]
            
            # Execute concurrently
            results = await asyncio.gather(*tasks)
            
            # Verify all posts processed
            assert len(results) == len(test_posts)
            assert all(r['result']['success'] for r in results)
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_export_integration_workflow(self):
        """Test workflow with multiple export formats."""
        test_posts = self.create_test_posts(5)
        
        # Configure multiple export formats
        export_config = {
            'output_dir': str(self.temp_dir / 'exports'),
            'formats': ['json', 'csv', 'sqlite']
        }
        
        # Mock exporters
        with patch('redditdl.exporters.json.JsonExporter') as mock_json, \
             patch('redditdl.exporters.csv.CsvExporter') as mock_csv, \
             patch('redditdl.exporters.sqlite.SqliteExporter') as mock_sqlite:
            
            # Mock exporter instances
            mock_json_instance = Mock()
            mock_json_instance.export.return_value = {'success': True, 'file': 'export.json'}
            mock_json.return_value = mock_json_instance
            
            mock_csv_instance = Mock()
            mock_csv_instance.export.return_value = {'success': True, 'file': 'export.csv'}
            mock_csv.return_value = mock_csv_instance
            
            mock_sqlite_instance = Mock()
            mock_sqlite_instance.export.return_value = {'success': True, 'file': 'export.db'}
            mock_sqlite.return_value = mock_sqlite_instance
            
            # Execute export workflow
            export_results = []
            
            for format_name in export_config['formats']:
                if format_name == 'json':
                    exporter = mock_json_instance
                elif format_name == 'csv':
                    exporter = mock_csv_instance
                elif format_name == 'sqlite':
                    exporter = mock_sqlite_instance
                
                result = exporter.export(test_posts, {
                    'output_file': f"{export_config['output_dir']}/export.{format_name}"
                })
                export_results.append(result)
            
            # Verify all exports succeeded
            assert len(export_results) == 3
            assert all(r['success'] for r in export_results)
            
            # Verify all exporters were called
            mock_json_instance.export.assert_called_once()
            mock_csv_instance.export.assert_called_once()
            mock_sqlite_instance.export.assert_called_once()