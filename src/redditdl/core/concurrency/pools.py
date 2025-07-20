"""
Worker Pool Management

Advanced worker pool management for concurrent processing with
adaptive scaling, resource monitoring, and load balancing.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Callable, Any, TypeVar, Coroutine
from dataclasses import dataclass, field
from enum import Enum
import psutil
from concurrent.futures import ThreadPoolExecutor
import threading
from redditdl.core.monitoring.metrics import get_metrics_collector, time_operation


T = TypeVar('T')
logger = logging.getLogger(__name__)


class PoolType(Enum):
    """Types of worker pools."""
    ASYNC = "async"         # Async task processing
    THREAD = "thread"       # Thread-based processing
    DOWNLOAD = "download"   # Media download operations
    PROCESSING = "processing"  # CPU-intensive tasks


@dataclass
class PoolConfig:
    """Configuration for worker pools."""
    min_workers: int = 2
    max_workers: int = 10
    target_cpu_percent: float = 70.0
    target_memory_percent: float = 80.0
    scale_up_threshold: float = 80.0
    scale_down_threshold: float = 30.0
    scale_interval: float = 30.0
    queue_size_limit: int = 1000


@dataclass
class PoolMetrics:
    """Metrics for pool performance tracking."""
    active_workers: int = 0
    queued_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    average_task_time: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    last_scale_time: float = field(default_factory=time.time)


class AsyncWorkerPool:
    """
    Adaptive async worker pool with automatic scaling.
    
    Features:
    - Automatic scaling based on CPU/memory usage
    - Task queue management with backpressure
    - Performance metrics collection
    - Resource monitoring and optimization
    """
    
    def __init__(self, config: Optional[PoolConfig] = None):
        """
        Initialize async worker pool.
        
        Args:
            config: Pool configuration
        """
        self.config = config or PoolConfig()
        self.metrics = PoolMetrics()
        
        # Task management
        self._task_queue: asyncio.Queue = asyncio.Queue(
            maxsize=self.config.queue_size_limit
        )
        self._workers: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        
        # Performance tracking
        self._task_times: List[float] = []
        self._lock = asyncio.Lock()
        
        # Start monitoring task
        self._monitor_task: Optional[asyncio.Task] = None
        self._started = False
    
    async def start(self) -> None:
        """Start the worker pool."""
        if self._started:
            return
        
        self._started = True
        
        # Start initial workers
        for _ in range(self.config.min_workers):
            await self._add_worker()
        
        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        logger.info(f"Started async worker pool with {len(self._workers)} workers")
    
    async def stop(self) -> None:
        """Stop the worker pool and wait for completion."""
        if not self._started:
            return
        
        logger.info("Stopping async worker pool...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Cancel monitoring
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # Wait for all workers to complete
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        
        self._started = False
        logger.info("Async worker pool stopped")
    
    async def submit(self, coro: Coroutine[Any, Any, T]) -> T:
        """
        Submit a coroutine for execution.
        
        Args:
            coro: Coroutine to execute
            
        Returns:
            Result from coroutine execution
        """
        if not self._started:
            await self.start()
        
        # Create future for result
        future = asyncio.Future()
        
        # Package task with future
        task_item = (coro, future, time.time())
        
        # Add to queue (may block if queue is full)
        try:
            await asyncio.wait_for(
                self._task_queue.put(task_item),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            raise RuntimeError("Task queue is full and timed out")
        
        # Wait for result
        return await future
    
    async def _add_worker(self) -> None:
        """Add a new worker to the pool."""
        if len(self._workers) >= self.config.max_workers:
            return
        
        worker_id = len(self._workers)
        worker_task = asyncio.create_task(self._worker_loop(worker_id))
        self._workers.append(worker_task)
        
        async with self._lock:
            self.metrics.active_workers = len(self._workers)
    
    async def _remove_worker(self) -> None:
        """Remove a worker from the pool."""
        if len(self._workers) <= self.config.min_workers:
            return
        
        if self._workers:
            worker = self._workers.pop()
            worker.cancel()
            
            async with self._lock:
                self.metrics.active_workers = len(self._workers)
    
    async def _worker_loop(self, worker_id: int) -> None:
        """Main worker loop."""
        logger.debug(f"Worker {worker_id} started")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Get task from queue with timeout
                    coro, future, submit_time = await asyncio.wait_for(
                        self._task_queue.get(),
                        timeout=1.0
                    )
                    
                    # Execute task
                    start_time = time.time()
                    try:
                        result = await coro
                        future.set_result(result)
                        
                        # Track successful completion
                        async with self._lock:
                            self.metrics.completed_tasks += 1
                        
                    except Exception as e:
                        future.set_exception(e)
                        
                        # Track failed completion
                        async with self._lock:
                            self.metrics.failed_tasks += 1
                        
                        logger.warning(f"Task failed in worker {worker_id}: {e}")
                    
                    # Update performance metrics
                    task_time = time.time() - start_time
                    self._task_times.append(task_time)
                    
                    # Keep only recent task times (last 100)
                    if len(self._task_times) > 100:
                        self._task_times = self._task_times[-100:]
                    
                    self._task_queue.task_done()
                    
                except asyncio.TimeoutError:
                    # Timeout waiting for tasks - check if we should scale down
                    continue
                
        except asyncio.CancelledError:
            logger.debug(f"Worker {worker_id} cancelled")
        
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _monitor_loop(self) -> None:
        """Monitor pool performance and adjust worker count."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.scale_interval)
                await self._update_metrics()
                await self._scale_pool()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in pool monitoring: {e}")
    
    async def _update_metrics(self) -> None:
        """Update pool metrics."""
        async with self._lock:
            # Update queue size
            self.metrics.queued_tasks = self._task_queue.qsize()
            
            # Update average task time
            if self._task_times:
                self.metrics.average_task_time = sum(self._task_times) / len(self._task_times)
            
            # Update resource usage
            try:
                process = psutil.Process()
                self.metrics.cpu_usage = process.cpu_percent()
                self.metrics.memory_usage = process.memory_percent()
            except Exception:
                # Fallback if psutil fails
                self.metrics.cpu_usage = 0.0
                self.metrics.memory_usage = 0.0
    
    async def _scale_pool(self) -> None:
        """Scale pool based on current metrics."""
        now = time.time()
        
        # Don't scale too frequently
        if now - self.metrics.last_scale_time < self.config.scale_interval:
            return
        
        queue_size = self.metrics.queued_tasks
        cpu_usage = self.metrics.cpu_usage
        memory_usage = self.metrics.memory_usage
        
        # Calculate queue utilization
        queue_utilization = (queue_size / self.config.queue_size_limit) * 100
        
        # Scale up conditions
        should_scale_up = (
            queue_utilization > self.config.scale_up_threshold or
            (queue_size > 0 and cpu_usage < self.config.target_cpu_percent)
        )
        
        # Scale down conditions
        should_scale_down = (
            queue_utilization < self.config.scale_down_threshold and
            queue_size == 0 and
            len(self._workers) > self.config.min_workers
        )
        
        # Prevent scaling if resource usage is too high
        if memory_usage > self.config.target_memory_percent:
            should_scale_up = False
        
        if should_scale_up:
            await self._add_worker()
            self.metrics.last_scale_time = now
            logger.info(f"Scaled up to {len(self._workers)} workers "
                       f"(queue: {queue_size}, cpu: {cpu_usage:.1f}%)")
        
        elif should_scale_down:
            await self._remove_worker()
            self.metrics.last_scale_time = now
            logger.info(f"Scaled down to {len(self._workers)} workers "
                       f"(queue: {queue_size}, cpu: {cpu_usage:.1f}%)")
    
    def get_metrics(self) -> PoolMetrics:
        """Get current pool metrics."""
        return self.metrics


class WorkerPoolManager:
    """
    Manages multiple worker pools for different operation types.
    
    Provides centralized management of async and thread pools
    with appropriate configurations for different workloads.
    """
    
    def __init__(self):
        """Initialize worker pool manager."""
        self._pools: Dict[PoolType, Any] = {}
        self._thread_executor: Optional[ThreadPoolExecutor] = None
        self._started = False
    
    async def start(self) -> None:
        """Start all worker pools."""
        if self._started:
            return
        
        # Initialize async pools
        self._pools[PoolType.ASYNC] = AsyncWorkerPool(
            PoolConfig(
                min_workers=3,
                max_workers=15,
                target_cpu_percent=70.0,
                scale_up_threshold=70.0,
                scale_down_threshold=20.0
            )
        )
        
        self._pools[PoolType.DOWNLOAD] = AsyncWorkerPool(
            PoolConfig(
                min_workers=5,
                max_workers=20,
                target_cpu_percent=60.0,
                scale_up_threshold=80.0,
                scale_down_threshold=30.0
            )
        )
        
        self._pools[PoolType.PROCESSING] = AsyncWorkerPool(
            PoolConfig(
                min_workers=2,
                max_workers=8,
                target_cpu_percent=85.0,
                scale_up_threshold=90.0,
                scale_down_threshold=40.0
            )
        )
        
        # Initialize thread pool
        max_threads = min(32, (psutil.cpu_count() or 4) * 4)
        self._thread_executor = ThreadPoolExecutor(
            max_workers=max_threads,
            thread_name_prefix="redditdl-thread"
        )
        self._pools[PoolType.THREAD] = self._thread_executor
        
        # Start async pools
        for pool_type, pool in self._pools.items():
            if isinstance(pool, AsyncWorkerPool):
                await pool.start()
        
        self._started = True
        logger.info("Worker pool manager started")
    
    async def stop(self) -> None:
        """Stop all worker pools."""
        if not self._started:
            return
        
        logger.info("Stopping worker pool manager...")
        
        # Stop async pools
        for pool_type, pool in self._pools.items():
            if isinstance(pool, AsyncWorkerPool):
                await pool.stop()
        
        # Stop thread pool
        if self._thread_executor:
            self._thread_executor.shutdown(wait=True)
        
        self._started = False
        logger.info("Worker pool manager stopped")
    
    async def submit_async(self, coro: Coroutine[Any, Any, T], 
                          pool_type: PoolType = PoolType.ASYNC) -> T:
        """
        Submit async task to specified pool.
        
        Args:
            coro: Coroutine to execute
            pool_type: Type of pool to use
            
        Returns:
            Result from coroutine execution
        """
        if not self._started:
            await self.start()
        
        if pool_type not in self._pools:
            raise ValueError(f"Unknown pool type: {pool_type}")
        
        pool = self._pools[pool_type]
        if not isinstance(pool, AsyncWorkerPool):
            raise ValueError(f"Pool type {pool_type} is not async")
        
        return await pool.submit(coro)
    
    async def submit_thread(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Submit function to thread pool.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Result from function execution
        """
        if not self._started:
            await self.start()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._thread_executor, func, *args, **kwargs)
    
    def get_pool_metrics(self, pool_type: PoolType) -> Optional[PoolMetrics]:
        """Get metrics for specific pool type."""
        if pool_type not in self._pools:
            return None
        
        pool = self._pools[pool_type]
        if isinstance(pool, AsyncWorkerPool):
            return pool.get_metrics()
        
        return None
    
    def get_all_metrics(self) -> Dict[str, PoolMetrics]:
        """Get metrics for all async pools."""
        metrics = {}
        for pool_type, pool in self._pools.items():
            if isinstance(pool, AsyncWorkerPool):
                metrics[pool_type.value] = pool.get_metrics()
        return metrics


# Global pool manager instance
_global_pool_manager = WorkerPoolManager()


async def submit_async_task(coro: Coroutine[Any, Any, T], 
                           pool_type: PoolType = PoolType.ASYNC) -> T:
    """
    Convenience function for submitting async tasks.
    
    Args:
        coro: Coroutine to execute
        pool_type: Type of pool to use
        
    Returns:
        Result from coroutine execution
    """
    return await _global_pool_manager.submit_async(coro, pool_type)


async def submit_thread_task(func: Callable[..., T], *args, **kwargs) -> T:
    """
    Convenience function for submitting thread tasks.
    
    Args:
        func: Function to execute
        *args: Function arguments
        **kwargs: Function keyword arguments
        
    Returns:
        Result from function execution
    """
    return await _global_pool_manager.submit_thread(func, *args, **kwargs)


def get_pool_manager() -> WorkerPoolManager:
    """Get global worker pool manager."""
    return _global_pool_manager