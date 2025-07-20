"""
Performance and Regression Testing

Tests for performance benchmarks, memory usage validation,
and regression detection across all system components.
"""

import asyncio
import gc
import time
import psutil
import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from memory_profiler import profile
import tracemalloc

from redditdl.core.pipeline.executor import PipelineExecutor
from redditdl.core.pipeline.interfaces import PipelineContext
from redditdl.core.events.emitter import EventEmitter
from redditdl.core.state.manager import StateManager
from redditdl.core.config.models import AppConfig
from redditdl.core.concurrency.processor import ConcurrentProcessor, BatchProcessor, BatchConfig
from redditdl.core.concurrency.pools import WorkerPoolManager, PoolType
from redditdl.core.concurrency.limiters import MultiLimiter, LimiterType
from redditdl.core.monitoring.metrics import MetricsCollector
from redditdl.core.monitoring.profiler import ResourceProfiler
from redditdl.core.cache.manager import CacheManager
from redditdl.scrapers import PostMetadata
from redditdl.downloader import MediaDownloader
from redditdl.metadata import MetadataEmbedder


class TestPerformanceBenchmarks:
    """Performance benchmark tests for core components."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_db = self.temp_dir / 'test_state.db'
        
        # Start memory tracking
        tracemalloc.start()
        self.start_memory = psutil.Process().memory_info().rss
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        
        # Stop memory tracking
        tracemalloc.stop()
        gc.collect()
    
    def get_memory_usage(self):
        """Get current memory usage in MB."""
        return psutil.Process().memory_info().rss / 1024 / 1024
    
    def create_test_posts(self, count=1000):
        """Create test posts for performance testing."""
        posts = []
        for i in range(count):
            post = PostMetadata(
                id=f'perf_test_{i:06d}',
                title=f'Performance Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                media_url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                author=f'user_{i % 100}',  # 100 different users
                subreddit=f'sub_{i % 20}',  # 20 different subreddits
                score=i * 5,
                num_comments=i % 50,
                created_utc=1640995200 + i * 60,
                is_nsfw=(i % 10 == 0),
                is_self=False,
                is_video=(i % 20 == 0)
            )
            posts.append(post)
        return posts
    
    @pytest.mark.performance
    def test_large_dataset_processing_performance(self):
        """Test processing performance with large datasets."""
        post_count = 10000
        posts = self.create_test_posts(post_count)
        
        start_time = time.time()
        start_memory = self.get_memory_usage()
        
        # Simulate processing pipeline
        processed_posts = []
        for batch_start in range(0, len(posts), 1000):
            batch = posts[batch_start:batch_start + 1000]
            
            # Simulate filter stage
            filtered_batch = [
                post for post in batch 
                if post.score >= 5 and not post.is_nsfw
            ]
            
            # Simulate processing stage
            for post in filtered_batch:
                # Simulate metadata processing
                processed_post = {
                    'id': post.id,
                    'title': post.title,
                    'processed': True,
                    'timestamp': time.time()
                }
                processed_posts.append(processed_post)
        
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        processing_time = end_time - start_time
        memory_increase = end_memory - start_memory
        
        # Performance assertions
        assert processing_time < 10.0, f"Processing took too long: {processing_time:.2f}s"
        assert memory_increase < 100, f"Memory usage increased too much: {memory_increase:.2f}MB"
        assert len(processed_posts) > 0, "No posts were processed"
        
        # Throughput should be reasonable
        throughput = len(processed_posts) / processing_time
        assert throughput > 500, f"Throughput too low: {throughput:.1f} posts/sec"
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_concurrent_download_performance(self):
        """Test concurrent download performance."""
        posts = self.create_test_posts(100)
        
        with patch('downloader.MediaDownloader') as mock_downloader_class:
            mock_downloader = Mock()
            
            # Mock async download method with realistic timing
            async def mock_download(url, output_dir, post_metadata):
                await asyncio.sleep(0.01)  # Simulate 10ms download
                return {
                    'success': True,
                    'local_path': f'/path/to/{post_metadata.id}.jpg',
                    'file_size': 1024000,
                    'download_time': 0.01
                }
            
            mock_downloader.download_media = mock_download
            mock_downloader_class.return_value = mock_downloader
            
            start_time = time.time()
            
            # Process posts concurrently
            semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent downloads
            
            async def process_post(post):
                async with semaphore:
                    result = await mock_downloader.download_media(
                        post.media_url,
                        str(self.temp_dir),
                        post
                    )
                    return {'post': post, 'result': result}
            
            # Execute concurrent processing
            tasks = [process_post(post) for post in posts]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Performance assertions
            assert processing_time < 5.0, f"Concurrent processing took too long: {processing_time:.2f}s"
            assert len(results) == len(posts), "Not all posts were processed"
            assert all(r['result']['success'] for r in results), "Some downloads failed"
            
            # Concurrent processing should be faster than sequential
            sequential_time = len(posts) * 0.01  # Estimated sequential time
            speedup = sequential_time / processing_time
            assert speedup > 5, f"Insufficient speedup from concurrency: {speedup:.1f}x"
    
    @pytest.mark.performance
    def test_state_manager_performance(self):
        """Test state manager performance with large datasets."""
        state_manager = StateManager(db_path=str(self.test_db))
        posts = self.create_test_posts(5000)
        
        start_time = time.time()
        start_memory = self.get_memory_usage()
        
        # Create session
        session_id = asyncio.run(state_manager.create_session(
            target='performance_test',
            config_hash='test_hash',
            total_posts=len(posts)
        ))
        
        # Batch insert posts
        batch_size = 500
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            for post in batch:
                asyncio.run(state_manager.save_post(session_id, post))
        
        # Mark some as downloaded
        for i, post in enumerate(posts[:1000]):
            if i % 10 == 0:  # Every 10th post
                asyncio.run(state_manager.mark_downloaded(
                    session_id,
                    post.id,
                    post.media_url,
                    f'/path/to/{post.id}.jpg',
                    success=True
                ))
        
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        processing_time = end_time - start_time
        memory_increase = end_memory - start_memory
        
        # Performance assertions
        assert processing_time < 15.0, f"State management took too long: {processing_time:.2f}s"
        assert memory_increase < 50, f"Memory usage increased too much: {memory_increase:.2f}MB"
        
        # Verify data integrity
        recovery_state = asyncio.run(state_manager.get_resume_state(session_id))
        assert recovery_state['total_posts'] == len(posts)
        assert recovery_state['processed_posts'] == len(posts)
    
    @pytest.mark.performance
    def test_filter_pipeline_performance(self):
        """Test filter pipeline performance with large datasets."""
        from filters.score import ScoreFilter
        from filters.domain import DomainFilter
        from filters.nsfw import NSFWFilter
        
        posts = self.create_test_posts(50000)  # Large dataset
        
        # Create filter chain
        filters = [
            ScoreFilter(min_score=10, max_score=1000),
            DomainFilter(allowed_domains=['example.com', 'imgur.com']),
            NSFWFilter(mode='exclude')
        ]
        
        start_time = time.time()
        start_memory = self.get_memory_usage()
        
        # Apply filters in sequence
        filtered_posts = posts.copy()
        for filter_instance in filters:
            filtered_posts = [
                post for post in filtered_posts 
                if filter_instance.apply(post)
            ]
        
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        processing_time = end_time - start_time
        memory_increase = end_memory - start_memory
        
        # Performance assertions
        assert processing_time < 5.0, f"Filter pipeline took too long: {processing_time:.2f}s"
        assert memory_increase < 20, f"Memory usage increased too much: {memory_increase:.2f}MB"
        
        # Should filter out some posts
        assert len(filtered_posts) < len(posts), "Filters didn't remove any posts"
        assert len(filtered_posts) > 0, "Filters removed all posts"
        
        # Throughput should be high
        throughput = len(posts) / processing_time
        assert throughput > 10000, f"Filter throughput too low: {throughput:.1f} posts/sec"
    
    @pytest.mark.performance
    def test_event_system_performance(self):
        """Test event system performance under load."""
        emitter = EventEmitter()
        
        # Create multiple observers
        observers = []
        for i in range(10):
            observer = Mock()
            observer.handle_event = Mock()
            observers.append(observer)
            emitter.subscribe('test_event', observer.handle_event)
        
        event_count = 10000
        start_time = time.time()
        start_memory = self.get_memory_usage()
        
        # Emit many events
        for i in range(event_count):
            event_data = {
                'event_id': i,
                'timestamp': time.time(),
                'data': f'test_data_{i}'
            }
            emitter.emit('test_event', event_data)
        
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        processing_time = end_time - start_time
        memory_increase = end_memory - start_memory
        
        # Performance assertions
        assert processing_time < 2.0, f"Event processing took too long: {processing_time:.2f}s"
        assert memory_increase < 10, f"Memory usage increased too much: {memory_increase:.2f}MB"
        
        # Verify all observers received all events
        for observer in observers:
            assert observer.handle_event.call_count == event_count
        
        # Event throughput should be high
        throughput = event_count / processing_time
        assert throughput > 5000, f"Event throughput too low: {throughput:.1f} events/sec"


class TestMemoryUsageValidation:
    """Test memory usage patterns and detect memory leaks."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        tracemalloc.start()
        gc.collect()
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        tracemalloc.stop()
        gc.collect()
    
    @pytest.mark.memory
    def test_memory_leak_detection(self):
        """Test for memory leaks in core components."""
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Simulate multiple processing cycles
        for cycle in range(10):
            posts = []
            for i in range(1000):
                post = PostMetadata(
                    id=f'leak_test_{cycle}_{i}',
                    title=f'Leak Test Post {i}',
                    url=f'https://example.com/image_{i}.jpg',
                    domain='example.com'
                )
                posts.append(post)
            
            # Process posts
            processed = []
            for post in posts:
                processed_data = {
                    'id': post.id,
                    'title': post.title,
                    'processed': True
                }
                processed.append(processed_data)
            
            # Clear references
            del posts
            del processed
            gc.collect()
            
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory increase should be bounded
            assert memory_increase < 50, f"Memory leak detected: {memory_increase:.2f}MB increase"
    
    @pytest.mark.memory
    def test_large_dataset_memory_efficiency(self):
        """Test memory efficiency with large datasets."""
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Process in batches to test memory efficiency
        total_posts = 50000
        batch_size = 5000
        
        for batch_start in range(0, total_posts, batch_size):
            batch_posts = []
            for i in range(batch_start, min(batch_start + batch_size, total_posts)):
                post = PostMetadata(
                    id=f'memory_test_{i}',
                    title=f'Memory Test Post {i}',
                    url=f'https://example.com/image_{i}.jpg',
                    domain='example.com',
                    author=f'user_{i % 100}',
                    subreddit=f'sub_{i % 20}'
                )
                batch_posts.append(post)
            
            # Process batch
            processed_batch = []
            for post in batch_posts:
                processed_data = {
                    'id': post.id,
                    'title': post.title,
                    'metadata': {
                        'author': post.author,
                        'subreddit': post.subreddit
                    }
                }
                processed_batch.append(processed_data)
            
            # Clear batch
            del batch_posts
            del processed_batch
            gc.collect()
            
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory should not grow significantly during batch processing
            assert memory_increase < 30, f"Memory usage grew too much: {memory_increase:.2f}MB"
    
    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_async_processing_memory_usage(self):
        """Test memory usage in async processing scenarios."""
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        async def process_posts_async(posts):
            """Simulate async post processing."""
            processed = []
            for post in posts:
                await asyncio.sleep(0.001)  # Simulate async work
                processed_data = {
                    'id': post.id,
                    'title': post.title,
                    'processed_at': time.time()
                }
                processed.append(processed_data)
            return processed
        
        # Run multiple async processing cycles
        for cycle in range(5):
            posts = []
            for i in range(2000):
                post = PostMetadata(
                    id=f'async_test_{cycle}_{i}',
                    title=f'Async Test Post {i}',
                    url=f'https://example.com/image_{i}.jpg',
                    domain='example.com'
                )
                posts.append(post)
            
            # Process asynchronously
            tasks = []
            for batch_start in range(0, len(posts), 500):
                batch = posts[batch_start:batch_start + 500]
                task = process_posts_async(batch)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            
            # Clear references
            del posts
            del results
            del tasks
            gc.collect()
            
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory should remain stable across async processing cycles
            assert memory_increase < 25, f"Async processing memory leak: {memory_increase:.2f}MB"


class TestRegressionDetection:
    """Test for performance regressions across versions."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.benchmark_file = self.temp_dir / 'benchmarks.json'
        
        # Load or create benchmark baseline
        if self.benchmark_file.exists():
            with open(self.benchmark_file, 'r') as f:
                self.baselines = json.load(f)
        else:
            self.baselines = {}
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def save_benchmark(self, test_name, timing, memory_usage):
        """Save benchmark results."""
        self.baselines[test_name] = {
            'timing': timing,
            'memory_usage': memory_usage,
            'timestamp': time.time()
        }
        
        with open(self.benchmark_file, 'w') as f:
            json.dump(self.baselines, f, indent=2)
    
    def check_regression(self, test_name, timing, memory_usage, tolerance=0.2):
        """Check for performance regression."""
        if test_name not in self.baselines:
            # First run, save as baseline
            self.save_benchmark(test_name, timing, memory_usage)
            return
        
        baseline = self.baselines[test_name]
        
        # Check timing regression
        timing_increase = (timing - baseline['timing']) / baseline['timing']
        assert timing_increase < tolerance, \
            f"Performance regression in {test_name}: {timing_increase:.1%} slower"
        
        # Check memory regression
        memory_increase = (memory_usage - baseline['memory_usage']) / baseline['memory_usage']
        assert memory_increase < tolerance, \
            f"Memory regression in {test_name}: {memory_increase:.1%} more memory"
    
    @pytest.mark.regression
    def test_post_processing_regression(self):
        """Test for regressions in post processing performance."""
        posts = []
        for i in range(5000):
            post = PostMetadata(
                id=f'regression_test_{i}',
                title=f'Regression Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                score=i % 100
            )
            posts.append(post)
        
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        start_time = time.time()
        
        # Process posts
        processed = []
        for post in posts:
            # Simulate various processing steps
            processed_data = {
                'id': post.id,
                'title': post.title.upper(),
                'score_category': 'high' if post.score > 50 else 'low',
                'processed': True
            }
            processed.append(processed_data)
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        timing = end_time - start_time
        memory_usage = end_memory - start_memory
        
        self.check_regression('post_processing', timing, memory_usage)
    
    @pytest.mark.regression
    def test_state_management_regression(self):
        """Test for regressions in state management performance."""
        db_path = self.temp_dir / 'regression_test.db'
        state_manager = StateManager(db_path=str(db_path))
        
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        start_time = time.time()
        
        # Create session and save posts
        session_id = asyncio.run(state_manager.create_session(
            target='regression_test',
            config_hash='test_hash',
            total_posts=1000
        ))
        
        for i in range(1000):
            post = PostMetadata(
                id=f'state_regression_{i}',
                title=f'State Regression Test {i}',
                url=f'https://example.com/image_{i}.jpg',
                domain='example.com'
            )
            asyncio.run(state_manager.save_post(session_id, post))
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        timing = end_time - start_time
        memory_usage = end_memory - start_memory
        
        self.check_regression('state_management', timing, memory_usage)
    
    @pytest.mark.regression
    def test_filter_processing_regression(self):
        """Test for regressions in filter processing performance."""
        from filters.score import ScoreFilter
        from filters.nsfw import NSFWFilter
        
        posts = []
        for i in range(10000):
            post = PostMetadata(
                id=f'filter_regression_{i}',
                title=f'Filter Regression Test {i}',
                url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                score=i % 200,
                is_nsfw=(i % 20 == 0)
            )
            posts.append(post)
        
        filters = [
            ScoreFilter(min_score=25),
            NSFWFilter(mode='exclude')
        ]
        
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        start_time = time.time()
        
        # Apply filters
        filtered_posts = posts.copy()
        for filter_instance in filters:
            filtered_posts = [
                post for post in filtered_posts 
                if filter_instance.apply(post)
            ]
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        timing = end_time - start_time
        memory_usage = end_memory - start_memory
        
        self.check_regression('filter_processing', timing, memory_usage)


@pytest.mark.scalability
class TestScalabilityLimits:
    """Test system behavior at scale limits."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.slow
    def test_maximum_post_count_handling(self):
        """Test handling of maximum reasonable post counts."""
        max_posts = 100000  # 100K posts
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Generate posts in batches to avoid memory issues
        processed_count = 0
        batch_size = 10000
        
        for batch_start in range(0, max_posts, batch_size):
            batch_posts = []
            batch_end = min(batch_start + batch_size, max_posts)
            
            for i in range(batch_start, batch_end):
                post = PostMetadata(
                    id=f'scale_test_{i}',
                    title=f'Scale Test Post {i}',
                    url=f'https://example.com/image_{i}.jpg',
                    domain='example.com'
                )
                batch_posts.append(post)
            
            # Process batch
            for post in batch_posts:
                processed_count += 1
            
            # Clear batch to manage memory
            del batch_posts
            gc.collect()
            
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_usage = current_memory - start_memory
            
            # Memory should remain bounded
            assert memory_usage < 100, f"Memory usage too high: {memory_usage:.2f}MB"
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should complete in reasonable time
        assert processing_time < 60, f"Processing took too long: {processing_time:.2f}s"
        assert processed_count == max_posts, f"Not all posts processed: {processed_count}/{max_posts}"
    
    @pytest.mark.slow
    def test_concurrent_operation_limits(self):
        """Test limits of concurrent operations."""
        import threading
        import queue
        
        # Test with increasing concurrency levels
        for concurrency in [10, 50, 100]:
            start_time = time.time()
            result_queue = queue.Queue()
            
            def worker(worker_id):
                """Worker function for concurrent testing."""
                posts = []
                for i in range(100):
                    post = PostMetadata(
                        id=f'concurrent_{worker_id}_{i}',
                        title=f'Concurrent Test {i}',
                        url=f'https://example.com/image_{i}.jpg',
                        domain='example.com'
                    )
                    posts.append(post)
                
                # Simulate processing
                processed = len(posts)
                result_queue.put(processed)
            
            # Start workers
            threads = []
            for worker_id in range(concurrency):
                thread = threading.Thread(target=worker, args=(worker_id,))
                thread.start()
                threads.append(thread)
            
            # Wait for completion
            for thread in threads:
                thread.join()
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Collect results
            total_processed = 0
            while not result_queue.empty():
                total_processed += result_queue.get()
            
            # Verify results
            expected_total = concurrency * 100
            assert total_processed == expected_total, \
                f"Concurrency {concurrency}: Expected {expected_total}, got {total_processed}"
            
            # Performance should scale reasonably
            assert processing_time < 10, \
                f"Concurrency {concurrency} took too long: {processing_time:.2f}s"
    
    @pytest.mark.slow
    def test_database_scale_limits(self):
        """Test database performance at scale."""
        db_path = self.temp_dir / 'scale_test.db'
        state_manager = StateManager(db_path=str(db_path))
        
        # Test with large number of sessions and posts
        num_sessions = 100
        posts_per_session = 1000
        
        start_time = time.time()
        
        for session_num in range(num_sessions):
            session_id = asyncio.run(state_manager.create_session(
                target=f'scale_test_user_{session_num}',
                config_hash=f'hash_{session_num}',
                total_posts=posts_per_session
            ))
            
            # Add posts in batches
            for batch_start in range(0, posts_per_session, 100):
                batch_posts = []
                for i in range(batch_start, min(batch_start + 100, posts_per_session)):
                    post = PostMetadata(
                        id=f'scale_post_{session_num}_{i}',
                        title=f'Scale Post {i}',
                        url=f'https://example.com/image_{i}.jpg',
                        domain='example.com'
                    )
                    batch_posts.append(post)
                
                # Save batch
                for post in batch_posts:
                    asyncio.run(state_manager.save_post(session_id, post))
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should handle large scale efficiently
        total_posts = num_sessions * posts_per_session
        assert processing_time < 120, f"Database operations took too long: {processing_time:.2f}s"
        
        # Verify database size is reasonable
        db_size_mb = db_path.stat().st_size / 1024 / 1024
        assert db_size_mb < 500, f"Database too large: {db_size_mb:.2f}MB"


class TestConcurrencyPerformance:
    """Performance tests for new concurrency features."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        tracemalloc.start()
        self.start_memory = psutil.Process().memory_info().rss
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        tracemalloc.stop()
        gc.collect()
    
    def create_test_posts(self, count=1000):
        """Create test posts for performance testing."""
        posts = []
        for i in range(count):
            post = PostMetadata(
                id=f'concurrency_test_{i:06d}',
                title=f'Concurrency Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                media_url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                author=f'user_{i % 100}',
                subreddit=f'sub_{i % 20}',
                score=i * 5,
                num_comments=i % 50,
                created_utc=1640995200 + i * 60,
                is_nsfw=(i % 10 == 0),
                is_self=False,
                is_video=(i % 20 == 0)
            )
            posts.append(post)
        return posts
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_concurrent_processor_performance(self):
        """Test concurrent processor with large datasets."""
        posts = self.create_test_posts(5000)
        
        # Mock processing function
        def process_post(post):
            # Simulate processing work
            time.sleep(0.001)  # 1ms processing per post
            return {
                'id': post.id,
                'processed': True,
                'processing_time': 0.001
            }
        
        processor = ConcurrentProcessor()
        await processor.start()
        
        try:
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # Process posts concurrently
            config = BatchConfig(
                batch_size=100,
                max_concurrent_batches=5,
                memory_limit_mb=256
            )
            
            results = await processor.process_posts(
                posts, process_post, config
            )
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            processing_time = end_time - start_time
            memory_increase = end_memory - start_memory
            
            # Performance assertions
            assert len(results) == len(posts), "Not all posts were processed"
            assert processing_time < 15.0, f"Concurrent processing too slow: {processing_time:.2f}s"
            assert memory_increase < 100, f"Memory usage too high: {memory_increase:.2f}MB"
            
            # Throughput should be better than sequential
            throughput = len(posts) / processing_time
            assert throughput > 500, f"Throughput too low: {throughput:.1f} posts/sec"
        
        finally:
            await processor.stop()
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_worker_pool_scaling(self):
        """Test worker pool automatic scaling performance."""
        pool_manager = WorkerPoolManager()
        await pool_manager.start()
        
        try:
            # Submit increasing numbers of tasks
            task_counts = [10, 50, 100, 200]
            
            for task_count in task_counts:
                async def test_task():
                    await asyncio.sleep(0.01)  # 10ms task
                    return "completed"
                
                start_time = time.time()
                
                # Submit tasks
                tasks = [
                    pool_manager.submit_async(test_task(), PoolType.ASYNC)
                    for _ in range(task_count)
                ]
                
                # Wait for completion
                results = await asyncio.gather(*tasks)
                
                end_time = time.time()
                processing_time = end_time - start_time
                
                # Verify all tasks completed
                assert len(results) == task_count
                assert all(r == "completed" for r in results)
                
                # Check scaling efficiency
                if task_count >= 100:
                    # For 100+ tasks, should complete faster than sequential
                    sequential_time = task_count * 0.01
                    speedup = sequential_time / processing_time
                    assert speedup > 3, f"Insufficient speedup for {task_count} tasks: {speedup:.1f}x"
        
        finally:
            await pool_manager.stop()
    
    @pytest.mark.performance
    def test_rate_limiter_performance(self):
        """Test rate limiter performance under load."""
        limiter = MultiLimiter()
        
        start_time = time.time()
        
        # Submit many requests
        async def rate_limited_requests():
            tasks = []
            for _ in range(100):
                async def request():
                    await limiter.acquire(LimiterType.API)
                    return "success"
                
                tasks.append(request())
            
            return await asyncio.gather(*tasks)
        
        # Run rate limited requests
        results = asyncio.run(rate_limited_requests())
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify all requests completed
        assert len(results) == 100
        assert all(r == "success" for r in results)
        
        # Check rate limiting is working (should take some time)
        assert processing_time > 1.0, "Rate limiting not working"
        assert processing_time < 30.0, f"Rate limiting too slow: {processing_time:.2f}s"
        
        # Check limiter statistics
        stats = limiter.get_all_stats()
        assert "api" in stats
        assert stats["api"]["total_requests"] == 100
    
    @pytest.mark.performance
    def test_database_connection_pool_performance(self):
        """Test database connection pool performance."""
        db_path = self.temp_dir / 'pool_test.db'
        state_manager = StateManager(db_path=str(db_path), max_connections=5)
        
        try:
            start_time = time.time()
            
            # Simulate concurrent database operations
            def database_operation(operation_id):
                # Create some test data
                posts = self.create_test_posts(10)
                
                for post in posts:
                    # Simulate database writes
                    with state_manager._transaction() as conn:
                        conn.execute(
                            "INSERT OR REPLACE INTO posts (id, title, url, domain) VALUES (?, ?, ?, ?)",
                            (f"{operation_id}_{post.id}", post.title, post.url, post.domain)
                        )
                
                return len(posts)
            
            # Run operations concurrently using thread pool
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(database_operation, i)
                    for i in range(20)  # 20 concurrent operations
                ]
                
                results = [future.result() for future in futures]
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Verify all operations completed
            assert len(results) == 20
            assert sum(results) == 200  # 20 operations × 10 posts each
            
            # Should complete efficiently with connection pooling
            assert processing_time < 10.0, f"Database operations too slow: {processing_time:.2f}s"
        
        finally:
            state_manager.close()


class TestCachePerformance:
    """Performance tests for caching system."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_manager = CacheManager()
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        self.cache_manager.clear()
    
    @pytest.mark.performance
    def test_cache_hit_performance(self):
        """Test cache hit performance with large datasets."""
        # Pre-populate cache with test data
        test_data = {}
        for i in range(1000):
            key = f"test_key_{i}"
            value = {"data": f"test_value_{i}", "number": i, "metadata": {"type": "test"}}
            self.cache_manager.set(key, value)
            test_data[key] = value
        
        # Test cache hit performance
        start_time = time.time()
        
        hit_count = 0
        for _ in range(5000):  # 5000 cache lookups
            key = f"test_key_{_ % 1000}"  # Cycle through existing keys
            cached_value = self.cache_manager.get(key)
            if cached_value is not None:
                hit_count += 1
        
        end_time = time.time()
        lookup_time = end_time - start_time
        
        # Performance assertions
        assert hit_count == 5000, "Not all cache hits succeeded"
        assert lookup_time < 1.0, f"Cache lookups too slow: {lookup_time:.3f}s"
        
        # Check hit rate
        cache_info = self.cache_manager.get_cache_info()
        hit_rate = cache_info["stats"]["hit_rate"]
        assert hit_rate > 0.95, f"Hit rate too low: {hit_rate:.2f}"
        
        # Throughput should be high
        throughput = 5000 / lookup_time
        assert throughput > 10000, f"Cache throughput too low: {throughput:.0f} ops/sec"
    
    @pytest.mark.performance
    def test_cache_memory_efficiency(self):
        """Test cache memory efficiency with large datasets."""
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Add large amount of data to cache
        large_data_count = 10000
        for i in range(large_data_count):
            key = f"large_key_{i}"
            # Create moderately sized value (1KB each)
            value = {"data": "x" * 1000, "index": i}
            self.cache_manager.set(key, value)
        
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_increase = current_memory - start_memory
        
        # Should not use excessive memory
        expected_memory = (large_data_count * 1024) / 1024 / 1024  # Rough estimate
        assert memory_increase < expected_memory * 2, f"Memory usage too high: {memory_increase:.1f}MB"
        
        # Test cache eviction by adding more data
        for i in range(large_data_count, large_data_count + 1000):
            key = f"eviction_key_{i}"
            value = {"data": "y" * 1000, "index": i}
            self.cache_manager.set(key, value)
        
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        final_increase = final_memory - start_memory
        
        # Memory should not continue growing indefinitely
        assert final_increase < memory_increase * 1.5, "Cache not evicting properly"


class TestMonitoringPerformance:
    """Performance tests for monitoring and metrics systems."""
    
    def setup_method(self):
        """Set up test environment."""
        self.metrics_collector = MetricsCollector()
        self.profiler = ResourceProfiler()
    
    def teardown_method(self):
        """Clean up test environment."""
        self.metrics_collector.clear_all()
        self.profiler.clear_profiles()
    
    @pytest.mark.performance
    def test_metrics_collection_performance(self):
        """Test metrics collection performance under load."""
        start_time = time.time()
        
        # Generate many metric events
        metric_count = 10000
        for i in range(metric_count):
            # Different types of metrics
            self.metrics_collector.increment("test.counter", 1)
            self.metrics_collector.set_gauge("test.gauge", i % 100)
            self.metrics_collector.record("test.histogram", i % 50)
        
        end_time = time.time()
        collection_time = end_time - start_time
        
        # Should handle high-frequency metrics efficiently
        assert collection_time < 2.0, f"Metrics collection too slow: {collection_time:.3f}s"
        
        # Throughput should be high
        throughput = metric_count / collection_time
        assert throughput > 5000, f"Metrics throughput too low: {throughput:.0f} metrics/sec"
        
        # Verify metrics were recorded
        summaries = self.metrics_collector.get_all_summaries()
        assert "test.counter" in summaries
        assert "test.gauge" in summaries
        assert "test.histogram" in summaries
        
        # Check counter value
        counter_summary = summaries["test.counter"]
        assert counter_summary.count > 0
    
    @pytest.mark.performance
    def test_profiler_overhead(self):
        """Test profiler overhead on performance."""
        # Baseline: function without profiling
        def test_function_baseline():
            result = 0
            for i in range(1000):
                result += i * i
            return result
        
        start_time = time.time()
        for _ in range(100):
            test_function_baseline()
        baseline_time = time.time() - start_time
        
        # With profiling
        @self.profiler.profile_function("test_function")
        def test_function_profiled():
            result = 0
            for i in range(1000):
                result += i * i
            return result
        
        start_time = time.time()
        for _ in range(100):
            test_function_profiled()
        profiled_time = time.time() - start_time
        
        # Profiling overhead should be minimal
        overhead = profiled_time - baseline_time
        overhead_percent = (overhead / baseline_time) * 100
        
        assert overhead_percent < 50, f"Profiler overhead too high: {overhead_percent:.1f}%"
        
        # Verify profile results were collected
        profiles = self.profiler.get_profile_results("test_function")
        assert len(profiles) == 100, "Not all profile results collected"


class TestIntegratedPerformance:
    """Integrated performance tests combining all optimization features."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Initialize all performance components
        self.concurrent_processor = ConcurrentProcessor()
        self.cache_manager = CacheManager()
        self.metrics_collector = MetricsCollector()
        self.profiler = ResourceProfiler()
        
        tracemalloc.start()
        self.start_memory = psutil.Process().memory_info().rss
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        
        # Clean up components
        asyncio.run(self.concurrent_processor.stop())
        self.cache_manager.clear()
        self.metrics_collector.clear_all()
        self.profiler.clear_profiles()
        
        tracemalloc.stop()
        gc.collect()
    
    @pytest.mark.performance
    @pytest.mark.asyncio
    async def test_full_pipeline_performance(self):
        """Test full pipeline performance with all optimizations enabled."""
        # Create large test dataset
        post_count = 2000
        posts = []
        for i in range(post_count):
            post = PostMetadata(
                id=f'integrated_test_{i:06d}',
                title=f'Integrated Test Post {i}',
                url=f'https://example.com/image_{i}.jpg',
                media_url=f'https://example.com/image_{i}.jpg',
                domain='example.com',
                author=f'user_{i % 100}',
                subreddit=f'sub_{i % 20}',
                score=i * 5,
                num_comments=i % 50,
                created_utc=1640995200 + i * 60,
                is_nsfw=(i % 10 == 0),
                is_self=False,
                is_video=(i % 20 == 0)
            )
            posts.append(post)
        
        # Start all systems
        await self.concurrent_processor.start()
        await self.metrics_collector.start_monitoring()
        self.profiler.start_memory_tracking()
        
        try:
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # Define processing function with caching and metrics
            @self.cache_manager.cached(ttl=300)  # 5 minute cache
            def process_post_with_optimizations(post):
                # Record metrics
                self.metrics_collector.increment("posts.processed")
                
                with self.metrics_collector.time_operation("post.processing_time"):
                    # Simulate processing with some computation
                    result = {
                        'id': post.id,
                        'title': post.title,
                        'processed': True,
                        'score_category': 'high' if post.score > 1000 else 'medium' if post.score > 500 else 'low',
                        'processing_timestamp': time.time()
                    }
                    
                    # Simulate some work
                    time.sleep(0.0005)  # 0.5ms processing
                    
                    return result
            
            # Process with concurrent processor
            config = BatchConfig(
                batch_size=50,
                max_concurrent_batches=8,
                memory_limit_mb=512,
                gc_interval=50
            )
            
            with self.profiler.profile_context("full_pipeline"):
                results = await self.concurrent_processor.process_posts(
                    posts, process_post_with_optimizations, config
                )
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            processing_time = end_time - start_time
            memory_increase = end_memory - start_memory
            
            # Performance assertions
            assert len(results) >= post_count * 0.95, f"Too many failed posts: {len(results)}/{post_count}"
            assert processing_time < 20.0, f"Integrated pipeline too slow: {processing_time:.2f}s"
            assert memory_increase < 200, f"Memory usage too high: {memory_increase:.2f}MB"
            
            # Throughput should be excellent with all optimizations
            throughput = len(results) / processing_time
            assert throughput > 200, f"Integrated throughput too low: {throughput:.1f} posts/sec"
            
            # Verify all systems collected data
            metrics_summaries = self.metrics_collector.get_all_summaries()
            assert "posts.processed" in metrics_summaries
            assert "post.processing_time" in metrics_summaries
            
            profile_results = self.profiler.get_profile_results("full_pipeline")
            assert len(profile_results) > 0, "No profile results collected"
            
            cache_info = self.cache_manager.get_cache_info()
            cache_hit_rate = cache_info["stats"]["hit_rate"]
            
            # Log performance summary
            print(f"\n=== Integrated Performance Results ===")
            print(f"Posts processed: {len(results)}/{post_count}")
            print(f"Processing time: {processing_time:.2f}s")
            print(f"Throughput: {throughput:.1f} posts/sec")
            print(f"Memory increase: {memory_increase:.1f}MB")
            print(f"Cache hit rate: {cache_hit_rate:.2%}")
            print(f"Profile duration: {profile_results[0].duration:.3f}s")
        
        finally:
            await self.metrics_collector.stop_monitoring()
            self.profiler.stop_memory_tracking()