"""
Concurrent Processor

High-performance concurrent processing engine for RedditDL with
batch processing, memory optimization, and intelligent scheduling.
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Callable, TypeVar, Generic, Iterator
from dataclasses import dataclass, field
from concurrent.futures import as_completed
import gc
import psutil

from redditdl.core.concurrency.pools import WorkerPoolManager, PoolType, submit_async_task
from redditdl.core.concurrency.limiters import MultiLimiter, LimiterType, rate_limit
from redditdl.scrapers import PostMetadata


T = TypeVar('T')
R = TypeVar('R')
logger = logging.getLogger(__name__)


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 50
    max_concurrent_batches: int = 5
    memory_limit_mb: int = 512
    gc_interval: int = 100
    progress_interval: float = 1.0


@dataclass
class ProcessingStats:
    """Statistics for concurrent processing."""
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    batches_completed: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    memory_peak_mb: float = 0.0
    throughput_items_per_sec: float = 0.0


class BatchProcessor(Generic[T, R]):
    """
    Generic batch processor for concurrent operations.
    
    Features:
    - Configurable batch sizes and concurrency limits
    - Memory monitoring and garbage collection
    - Rate limiting integration
    - Progress tracking and statistics
    - Error handling and recovery
    """
    
    def __init__(self, 
                 processor_func: Callable[[T], R],
                 config: Optional[BatchConfig] = None,
                 limiter_type: Optional[LimiterType] = None):
        """
        Initialize batch processor.
        
        Args:
            processor_func: Function to process individual items
            config: Batch processing configuration
            limiter_type: Rate limiter type to use
        """
        self.processor_func = processor_func
        self.config = config or BatchConfig()
        self.limiter_type = limiter_type
        self.stats = ProcessingStats()
        
        # Internal state
        self._pool_manager = WorkerPoolManager()
        self._limiter = MultiLimiter()
        self._last_gc = time.time()
        self._last_progress = time.time()
    
    async def process_batch(self, items: List[T]) -> List[R]:
        """
        Process a batch of items concurrently.
        
        Args:
            items: List of items to process
            
        Returns:
            List of processed results
        """
        if not items:
            return []
        
        # Apply rate limiting if configured
        if self.limiter_type:
            await rate_limit(self.limiter_type)
        
        # Create tasks for batch processing
        tasks = []
        for item in items:
            # Wrap processor function for async execution
            async def process_item(item_to_process=item):
                try:
                    # Use thread pool for CPU-intensive tasks
                    if self._is_cpu_intensive():
                        result = await self._pool_manager.submit_thread(
                            self.processor_func, item_to_process
                        )
                    else:
                        # Use async pool for I/O-bound tasks
                        result = await submit_async_task(
                            self._async_wrapper(item_to_process),
                            PoolType.ASYNC
                        )
                    
                    self.stats.processed_items += 1
                    return result
                
                except Exception as e:
                    self.stats.failed_items += 1
                    logger.warning(f"Failed to process item: {e}")
                    return None
            
            tasks.append(process_item())
        
        # Execute batch concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and None results
        processed_results = [
            result for result in results 
            if result is not None and not isinstance(result, Exception)
        ]
        
        self.stats.batches_completed += 1
        return processed_results
    
    async def process_stream(self, items: Iterator[T]) -> List[R]:
        """
        Process a stream of items with memory-efficient batching.
        
        Args:
            items: Iterator of items to process
            
        Returns:
            List of all processed results
        """
        self.stats.start_time = time.time()
        all_results = []
        
        # Convert iterator to list for counting
        item_list = list(items)
        self.stats.total_items = len(item_list)
        
        # Process in batches
        batch_semaphore = asyncio.Semaphore(self.config.max_concurrent_batches)
        
        async def process_batch_with_semaphore(batch):
            async with batch_semaphore:
                return await self.process_batch(batch)
        
        # Create batch tasks
        batch_tasks = []
        for i in range(0, len(item_list), self.config.batch_size):
            batch = item_list[i:i + self.config.batch_size]
            task = process_batch_with_semaphore(batch)
            batch_tasks.append(task)
        
        # Process batches with progress tracking
        for task in asyncio.as_completed(batch_tasks):
            batch_results = await task
            all_results.extend(batch_results)
            
            # Update statistics
            await self._update_stats()
            
            # Memory management
            await self._manage_memory()
            
            # Progress reporting
            await self._report_progress()
        
        self.stats.end_time = time.time()
        self._calculate_final_stats()
        
        return all_results
    
    async def _async_wrapper(self, item: T) -> R:
        """Wrap synchronous processor function for async execution."""
        return self.processor_func(item)
    
    def _is_cpu_intensive(self) -> bool:
        """Determine if processor function is CPU-intensive."""
        # Heuristic: assume CPU-intensive if processor involves image/video processing
        func_name = getattr(self.processor_func, '__name__', '').lower()
        cpu_keywords = ['process', 'convert', 'resize', 'compress', 'encode']
        return any(keyword in func_name for keyword in cpu_keywords)
    
    async def _update_stats(self) -> None:
        """Update processing statistics."""
        # Update memory usage
        try:
            process = psutil.Process()
            current_memory = process.memory_info().rss / 1024 / 1024
            self.stats.memory_peak_mb = max(self.stats.memory_peak_mb, current_memory)
        except Exception:
            pass
    
    async def _manage_memory(self) -> None:
        """Manage memory usage with garbage collection."""
        now = time.time()
        
        # Check if we should run garbage collection
        if now - self._last_gc > self.config.gc_interval:
            # Check memory usage
            try:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                
                if memory_mb > self.config.memory_limit_mb:
                    # Force garbage collection
                    gc.collect()
                    logger.debug(f"Garbage collection triggered at {memory_mb:.1f}MB")
            
            except Exception:
                pass
            
            self._last_gc = now
    
    async def _report_progress(self) -> None:
        """Report processing progress."""
        now = time.time()
        
        if now - self._last_progress > self.config.progress_interval:
            progress_percent = (
                (self.stats.processed_items / self.stats.total_items) * 100
                if self.stats.total_items > 0 else 0
            )
            
            elapsed = now - self.stats.start_time
            rate = self.stats.processed_items / elapsed if elapsed > 0 else 0
            
            logger.info(
                f"Progress: {progress_percent:.1f}% "
                f"({self.stats.processed_items}/{self.stats.total_items}) "
                f"Rate: {rate:.1f} items/sec"
            )
            
            self._last_progress = now
    
    def _calculate_final_stats(self) -> None:
        """Calculate final processing statistics."""
        if self.stats.end_time and self.stats.start_time:
            elapsed = self.stats.end_time - self.stats.start_time
            self.stats.throughput_items_per_sec = (
                self.stats.processed_items / elapsed if elapsed > 0 else 0
            )
    
    def get_stats(self) -> ProcessingStats:
        """Get current processing statistics."""
        return self.stats


class ConcurrentProcessor:
    """
    Main concurrent processing engine for RedditDL.
    
    Provides high-level interface for concurrent processing of Reddit posts
    with automatic optimization and resource management.
    """
    
    def __init__(self):
        """Initialize concurrent processor."""
        self._pool_manager = WorkerPoolManager()
        self._started = False
    
    async def start(self) -> None:
        """Start the concurrent processor."""
        if self._started:
            return
        
        await self._pool_manager.start()
        self._started = True
        logger.info("Concurrent processor started")
    
    async def stop(self) -> None:
        """Stop the concurrent processor."""
        if not self._started:
            return
        
        await self._pool_manager.stop()
        self._started = False
        logger.info("Concurrent processor stopped")
    
    async def process_posts(self, 
                           posts: List[PostMetadata],
                           processor_func: Callable[[PostMetadata], Any],
                           config: Optional[BatchConfig] = None) -> List[Any]:
        """
        Process a list of Reddit posts concurrently.
        
        Args:
            posts: List of posts to process
            processor_func: Function to process individual posts
            config: Batch processing configuration
            
        Returns:
            List of processed results
        """
        if not self._started:
            await self.start()
        
        # Create batch processor
        batch_processor = BatchProcessor(
            processor_func=processor_func,
            config=config,
            limiter_type=LimiterType.API  # Default to API rate limiting
        )
        
        # Process posts
        results = await batch_processor.process_stream(iter(posts))
        
        # Log final statistics
        stats = batch_processor.get_stats()
        logger.info(
            f"Processed {stats.processed_items}/{stats.total_items} posts "
            f"({stats.failed_items} failed) "
            f"in {stats.batches_completed} batches. "
            f"Throughput: {stats.throughput_items_per_sec:.1f} posts/sec"
        )
        
        return results
    
    async def process_downloads(self,
                               download_tasks: List[Dict[str, Any]],
                               downloader_func: Callable[[Dict[str, Any]], Any],
                               config: Optional[BatchConfig] = None) -> List[Any]:
        """
        Process download tasks concurrently.
        
        Args:
            download_tasks: List of download task specifications
            downloader_func: Function to process downloads
            config: Batch processing configuration
            
        Returns:
            List of download results
        """
        if not self._started:
            await self.start()
        
        # Create batch processor for downloads
        batch_processor = BatchProcessor(
            processor_func=downloader_func,
            config=config or BatchConfig(
                batch_size=20,  # Smaller batches for downloads
                max_concurrent_batches=3,
                memory_limit_mb=256
            ),
            limiter_type=LimiterType.DOWNLOADS
        )
        
        # Process downloads
        results = await batch_processor.process_stream(iter(download_tasks))
        
        # Log final statistics
        stats = batch_processor.get_stats()
        logger.info(
            f"Processed {stats.processed_items}/{stats.total_items} downloads "
            f"({stats.failed_items} failed). "
            f"Throughput: {stats.throughput_items_per_sec:.1f} downloads/sec"
        )
        
        return results
    
    def get_pool_metrics(self) -> Dict[str, Any]:
        """Get metrics for all worker pools."""
        return self._pool_manager.get_all_metrics()


# Global concurrent processor instance
_global_processor = ConcurrentProcessor()


async def process_concurrently(items: List[T], 
                              processor_func: Callable[[T], R],
                              config: Optional[BatchConfig] = None) -> List[R]:
    """
    Convenience function for concurrent processing.
    
    Args:
        items: List of items to process
        processor_func: Function to process individual items
        config: Batch processing configuration
        
    Returns:
        List of processed results
    """
    batch_processor = BatchProcessor(
        processor_func=processor_func,
        config=config
    )
    
    return await batch_processor.process_stream(iter(items))


def get_concurrent_processor() -> ConcurrentProcessor:
    """Get global concurrent processor instance."""
    return _global_processor