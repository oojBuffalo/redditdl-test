"""
Event Emitter for RedditDL Pipeline

Provides thread-safe event broadcasting with async support, event queuing,
and observer management for the RedditDL pipeline system.
"""

import asyncio
import logging
import threading
import time
import weakref
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Union

from redditdl.core.events.types import BaseEvent, EventType


logger = logging.getLogger(__name__)


class EventEmitter:
    """
    Thread-safe event emitter with async support and observer management.
    
    Features:
    - Thread-safe event emission and observer management
    - Async and sync observer support
    - Event queuing for performance
    - Wildcard event subscriptions
    - Event history for replay capability
    - Observer error isolation
    - Weak references to prevent memory leaks
    """
    
    def __init__(self, 
                 max_history: int = 1000,
                 max_queue_size: int = 10000,
                 enable_history: bool = True):
        """
        Initialize the event emitter.
        
        Args:
            max_history: Maximum number of events to keep in history
            max_queue_size: Maximum size of event queue before blocking
            enable_history: Whether to store event history
        """
        self.max_history = max_history
        self.max_queue_size = max_queue_size
        self.enable_history = enable_history
        
        # Thread synchronization
        self._lock = threading.RLock()
        self._async_lock = asyncio.Lock()
        
        # Observer storage: event_type -> set of observers
        self._observers: Dict[str, Set[Any]] = defaultdict(set)
        self._wildcard_observers: Set[Any] = set()
        
        # Event history and queuing
        self._event_history: deque = deque(maxlen=max_history if enable_history else 0)
        self._event_queue: asyncio.Queue = None  # Created when needed
        
        # Statistics and monitoring
        self._stats = {
            'events_emitted': 0,
            'events_processed': 0,
            'observers_notified': 0,
            'observer_errors': 0,
            'queue_overflows': 0
        }
        
        # Async processing
        self._processing_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._thread_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="event-")
    
    def subscribe(self, 
                  event_type: Union[str, type], 
                  observer: Union[Callable, Coroutine],
                  weak: bool = True) -> bool:
        """
        Subscribe an observer to events of a specific type.
        
        Args:
            event_type: Event type to subscribe to (class or string name)
            observer: Callable or coroutine to handle events
            weak: Use weak references to prevent memory leaks
            
        Returns:
            True if subscription was successful
        """
        try:
            with self._lock:
                # Convert event type to string
                if isinstance(event_type, type):
                    event_type_str = event_type.__name__
                else:
                    event_type_str = str(event_type)
                
                # Handle wildcard subscriptions
                if event_type_str == '*' or event_type_str == 'all':
                    if weak:
                        self._wildcard_observers.add(weakref.ref(observer))
                    else:
                        self._wildcard_observers.add(observer)
                else:
                    if weak:
                        self._observers[event_type_str].add(weakref.ref(observer))
                    else:
                        self._observers[event_type_str].add(observer)
                
                logger.debug(f"Subscribed observer to {event_type_str} events")
                return True
                
        except Exception as e:
            logger.error(f"Failed to subscribe observer: {e}")
            return False
    
    def unsubscribe(self, 
                   event_type: Union[str, type], 
                   observer: Union[Callable, Coroutine]) -> bool:
        """
        Unsubscribe an observer from events.
        
        Args:
            event_type: Event type to unsubscribe from
            observer: Observer to remove
            
        Returns:
            True if unsubscription was successful
        """
        try:
            with self._lock:
                # Convert event type to string
                if isinstance(event_type, type):
                    event_type_str = event_type.__name__
                else:
                    event_type_str = str(event_type)
                
                # Handle wildcard unsubscriptions
                if event_type_str == '*' or event_type_str == 'all':
                    # Remove both direct and weak references
                    to_remove = set()
                    for obs in self._wildcard_observers:
                        if isinstance(obs, weakref.ref):
                            if obs() is observer:
                                to_remove.add(obs)
                        elif obs is observer:
                            to_remove.add(obs)
                    self._wildcard_observers -= to_remove
                else:
                    # Remove from specific event type
                    to_remove = set()
                    for obs in self._observers[event_type_str]:
                        if isinstance(obs, weakref.ref):
                            if obs() is observer:
                                to_remove.add(obs)
                        elif obs is observer:
                            to_remove.add(obs)
                    self._observers[event_type_str] -= to_remove
                
                logger.debug(f"Unsubscribed observer from {event_type_str} events")
                return True
                
        except Exception as e:
            logger.error(f"Failed to unsubscribe observer: {e}")
            return False
    
    def emit(self, event: EventType) -> bool:
        """
        Emit an event to all subscribed observers.
        
        Args:
            event: Event instance to emit
            
        Returns:
            True if event was successfully queued for processing
        """
        try:
            # Update statistics
            self._stats['events_emitted'] += 1
            
            # Store in history if enabled
            if self.enable_history:
                self._event_history.append(event)
            
            # Queue event for async processing
            if asyncio.get_running_loop():
                # We're in an async context
                asyncio.create_task(self._process_event_async(event))
            else:
                # We're in a sync context, use thread pool
                self._thread_pool.submit(self._process_event_sync, event)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit event {event.event_type}: {e}")
            return False
    
    async def emit_async(self, event: EventType) -> bool:
        """
        Emit an event asynchronously.
        
        Args:
            event: Event instance to emit
            
        Returns:
            True if event was successfully processed
        """
        try:
            # Update statistics
            self._stats['events_emitted'] += 1
            
            # Store in history if enabled
            if self.enable_history:
                self._event_history.append(event)
            
            # Process event immediately
            await self._process_event_async(event)
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit event {event.event_type}: {e}")
            return False
    
    async def _process_event_async(self, event: EventType) -> None:
        """Process an event asynchronously, notifying all observers."""
        try:
            event_type = event.event_type
            observers_to_notify = []
            
            # Collect observers
            async with self._async_lock:
                # Get specific observers
                if event_type in self._observers:
                    observers_to_notify.extend(self._observers[event_type])
                
                # Get wildcard observers
                observers_to_notify.extend(self._wildcard_observers)
            
            # Notify observers
            await self._notify_observers_async(event, observers_to_notify)
            
            self._stats['events_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing event {event.event_type}: {e}")
    
    def _process_event_sync(self, event: EventType) -> None:
        """Process an event synchronously, notifying all observers."""
        try:
            event_type = event.event_type
            observers_to_notify = []
            
            # Collect observers
            with self._lock:
                # Get specific observers
                if event_type in self._observers:
                    observers_to_notify.extend(self._observers[event_type])
                
                # Get wildcard observers
                observers_to_notify.extend(self._wildcard_observers)
            
            # Notify observers synchronously
            self._notify_observers_sync(event, observers_to_notify)
            
            self._stats['events_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing event {event.event_type}: {e}")
    
    async def _notify_observers_async(self, event: EventType, observers: List[Any]) -> None:
        """Notify observers asynchronously with error isolation."""
        tasks = []
        
        for observer in observers:
            # Resolve weak references
            if isinstance(observer, weakref.ref):
                observer_func = observer()
                if observer_func is None:
                    continue  # Weak reference expired
            else:
                observer_func = observer
            
            # Create notification task
            task = asyncio.create_task(
                self._safe_notify_observer_async(observer_func, event)
            )
            tasks.append(task)
        
        # Wait for all notifications to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def _notify_observers_sync(self, event: EventType, observers: List[Any]) -> None:
        """Notify observers synchronously with error isolation."""
        for observer in observers:
            # Resolve weak references
            if isinstance(observer, weakref.ref):
                observer_func = observer()
                if observer_func is None:
                    continue  # Weak reference expired
            else:
                observer_func = observer
            
            # Notify observer safely
            self._safe_notify_observer_sync(observer_func, event)
    
    async def _safe_notify_observer_async(self, observer: Callable, event: EventType) -> None:
        """Safely notify an observer asynchronously with error handling."""
        try:
            if asyncio.iscoroutinefunction(observer):
                await observer(event)
            else:
                # Run sync observer in thread pool
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(self._thread_pool, observer, event)
            
            self._stats['observers_notified'] += 1
            
        except Exception as e:
            self._stats['observer_errors'] += 1
            logger.warning(f"Observer error for {event.event_type}: {e}")
    
    def _safe_notify_observer_sync(self, observer: Callable, event: EventType) -> None:
        """Safely notify an observer synchronously with error handling."""
        try:
            observer(event)
            self._stats['observers_notified'] += 1
            
        except Exception as e:
            self._stats['observer_errors'] += 1
            logger.warning(f"Observer error for {event.event_type}: {e}")
    
    def get_observers(self, event_type: Optional[str] = None) -> Dict[str, int]:
        """
        Get count of observers by event type.
        
        Args:
            event_type: Specific event type to check, or None for all
            
        Returns:
            Dictionary mapping event types to observer counts
        """
        with self._lock:
            if event_type:
                return {
                    event_type: len(self._observers.get(event_type, set())),
                    'wildcard': len(self._wildcard_observers)
                }
            else:
                result = {}
                for et, obs in self._observers.items():
                    result[et] = len(obs)
                result['wildcard'] = len(self._wildcard_observers)
                return result
    
    def get_event_history(self, 
                         event_type: Optional[str] = None,
                         limit: Optional[int] = None) -> List[EventType]:
        """
        Get event history, optionally filtered by type.
        
        Args:
            event_type: Filter by specific event type
            limit: Maximum number of events to return
            
        Returns:
            List of events from history
        """
        if not self.enable_history:
            return []
        
        events = list(self._event_history)
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if limit:
            events = events[-limit:]
        
        return events
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get event system statistics."""
        return {
            **self._stats,
            'total_observers': sum(len(obs) for obs in self._observers.values()),
            'wildcard_observers': len(self._wildcard_observers),
            'event_types': list(self._observers.keys()),
            'history_size': len(self._event_history) if self.enable_history else 0,
            'history_enabled': self.enable_history
        }
    
    def clear_history(self) -> None:
        """Clear event history."""
        if self.enable_history:
            self._event_history.clear()
    
    def clear_observers(self) -> None:
        """Remove all observers."""
        with self._lock:
            self._observers.clear()
            self._wildcard_observers.clear()
    
    def shutdown(self) -> None:
        """Shutdown the event emitter and cleanup resources."""
        try:
            self._shutdown_event.set()
            if self._processing_task:
                self._processing_task.cancel()
            self._thread_pool.shutdown(wait=True)
            self.clear_observers()
            self.clear_history()
            logger.info("Event emitter shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during event emitter shutdown: {e}")
    
    def __del__(self):
        """Cleanup when emitter is garbage collected."""
        try:
            self.shutdown()
        except:
            pass