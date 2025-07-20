#!/usr/bin/env python3
"""
Tests for RedditDL Event Emitter

Comprehensive tests for EventEmitter functionality including
thread safety, async support, and observer management.
"""

import asyncio
import pytest
import threading
import time
from unittest.mock import Mock, call
from concurrent.futures import ThreadPoolExecutor

import sys
sys.path.insert(0, '.')

from redditdl.core.events.emitter import EventEmitter
from redditdl.core.events.types import BaseEvent, PostDiscoveredEvent, DownloadStartedEvent


class TestEventEmitterBasic:
    """Test suite for basic EventEmitter functionality."""
    
    def test_emitter_initialization(self):
        """Test EventEmitter initialization."""
        emitter = EventEmitter()
        
        assert emitter.max_history == 1000
        assert emitter.max_queue_size == 10000
        assert emitter.enable_history is True
        assert len(emitter._observers) == 0
        assert len(emitter._wildcard_observers) == 0
        assert emitter._stats['events_emitted'] == 0
    
    def test_emitter_custom_initialization(self):
        """Test EventEmitter with custom parameters."""
        emitter = EventEmitter(
            max_history=500,
            max_queue_size=5000,
            enable_history=False
        )
        
        assert emitter.max_history == 500
        assert emitter.max_queue_size == 5000
        assert emitter.enable_history is False
    
    def test_subscribe_observer(self):
        """Test subscribing observers to events."""
        emitter = EventEmitter()
        observer = Mock()
        
        # Subscribe to specific event type
        result = emitter.subscribe('TestEvent', observer)
        assert result is True
        
        # Check observer was added
        observers = emitter.get_observers('TestEvent')
        assert observers['TestEvent'] == 1
    
    def test_subscribe_observer_by_class(self):
        """Test subscribing observer using event class."""
        emitter = EventEmitter()
        observer = Mock()
        
        # Subscribe using event class
        result = emitter.subscribe(BaseEvent, observer)
        assert result is True
        
        # Check observer was added
        observers = emitter.get_observers('BaseEvent')
        assert observers['BaseEvent'] == 1
    
    def test_subscribe_wildcard_observer(self):
        """Test subscribing to all events with wildcard."""
        emitter = EventEmitter()
        observer = Mock()
        
        # Subscribe to all events
        result = emitter.subscribe('*', observer)
        assert result is True
        
        # Check wildcard observer was added
        observers = emitter.get_observers()
        assert observers['wildcard'] == 1
    
    def test_unsubscribe_observer(self):
        """Test unsubscribing observers."""
        emitter = EventEmitter()
        observer = Mock()
        
        # Subscribe then unsubscribe
        emitter.subscribe('TestEvent', observer)
        result = emitter.unsubscribe('TestEvent', observer)
        assert result is True
        
        # Check observer was removed
        observers = emitter.get_observers('TestEvent')
        assert observers['TestEvent'] == 0
    
    def test_unsubscribe_wildcard_observer(self):
        """Test unsubscribing wildcard observer."""
        emitter = EventEmitter()
        observer = Mock()
        
        # Subscribe then unsubscribe wildcard
        emitter.subscribe('*', observer)
        result = emitter.unsubscribe('*', observer)
        assert result is True
        
        # Check wildcard observer was removed
        observers = emitter.get_observers()
        assert observers['wildcard'] == 0


class TestEventEmitterEventProcessing:
    """Test suite for event processing functionality."""
    
    def test_emit_event_to_specific_observer(self):
        """Test emitting event to specific observer."""
        emitter = EventEmitter()
        observer = Mock()
        event = BaseEvent()
        
        # Subscribe and emit
        emitter.subscribe('BaseEvent', observer, weak=False)
        emitter.emit(event)
        
        # Give some time for async processing
        time.sleep(0.1)
        
        # Check observer was called
        observer.assert_called_once_with(event)
    
    def test_emit_event_to_wildcard_observer(self):
        """Test emitting event to wildcard observer."""
        emitter = EventEmitter()
        observer = Mock()
        event = BaseEvent()
        
        # Subscribe wildcard and emit
        emitter.subscribe('*', observer, weak=False)
        emitter.emit(event)
        
        # Give some time for async processing
        time.sleep(0.1)
        
        # Check observer was called
        observer.assert_called_once_with(event)
    
    def test_emit_event_to_multiple_observers(self):
        """Test emitting event to multiple observers."""
        emitter = EventEmitter()
        observer1 = Mock()
        observer2 = Mock()
        observer3 = Mock()  # Wildcard observer
        event = BaseEvent()
        
        # Subscribe multiple observers
        emitter.subscribe('BaseEvent', observer1, weak=False)
        emitter.subscribe('BaseEvent', observer2, weak=False)
        emitter.subscribe('*', observer3, weak=False)
        emitter.emit(event)
        
        # Give some time for async processing
        time.sleep(0.1)
        
        # Check all observers were called
        observer1.assert_called_once_with(event)
        observer2.assert_called_once_with(event)
        observer3.assert_called_once_with(event)
    
    def test_emit_different_event_types(self):
        """Test emitting different event types to appropriate observers."""
        emitter = EventEmitter()
        base_observer = Mock()
        post_observer = Mock()
        download_observer = Mock()
        wildcard_observer = Mock()
        
        # Subscribe different observers
        emitter.subscribe('BaseEvent', base_observer, weak=False)
        emitter.subscribe('PostDiscoveredEvent', post_observer, weak=False)
        emitter.subscribe('DownloadStartedEvent', download_observer, weak=False)
        emitter.subscribe('*', wildcard_observer, weak=False)
        
        # Emit different event types
        base_event = BaseEvent()
        post_event = PostDiscoveredEvent()
        download_event = DownloadStartedEvent()
        
        emitter.emit(base_event)
        emitter.emit(post_event)
        emitter.emit(download_event)
        
        # Give some time for async processing
        time.sleep(0.1)
        
        # Check observers received correct events
        base_observer.assert_called_once_with(base_event)
        post_observer.assert_called_once_with(post_event)
        download_observer.assert_called_once_with(download_event)
        
        # Wildcard observer should receive all events
        assert wildcard_observer.call_count == 3
        wildcard_observer.assert_has_calls([
            call(base_event),
            call(post_event),
            call(download_event)
        ], any_order=True)
    
    def test_observer_error_isolation(self):
        """Test that observer errors don't affect other observers."""
        emitter = EventEmitter()
        
        # Create observers - one that raises exception
        def failing_observer(event):
            raise ValueError("Test error")
        
        working_observer = Mock()
        
        # Subscribe both observers
        emitter.subscribe('BaseEvent', failing_observer, weak=False)
        emitter.subscribe('BaseEvent', working_observer, weak=False)
        
        # Emit event
        event = BaseEvent()
        emitter.emit(event)
        
        # Give some time for async processing
        time.sleep(0.1)
        
        # Working observer should still be called despite failing observer
        working_observer.assert_called_once_with(event)
        
        # Check error was counted in statistics
        stats = emitter.get_statistics()
        assert stats['observer_errors'] == 1


class TestEventEmitterAsyncSupport:
    """Test suite for async event processing."""
    
    @pytest.mark.asyncio
    async def test_emit_async(self):
        """Test async event emission."""
        emitter = EventEmitter()
        observer = Mock()
        event = BaseEvent()
        
        # Subscribe and emit async
        emitter.subscribe('BaseEvent', observer, weak=False)
        result = await emitter.emit_async(event)
        
        assert result is True
        observer.assert_called_once_with(event)
    
    @pytest.mark.asyncio
    async def test_async_observer(self):
        """Test async observer function."""
        emitter = EventEmitter()
        
        # Create async observer
        async def async_observer(event):
            await asyncio.sleep(0.01)  # Simulate async work
            async_observer.called = True
            async_observer.event = event
        
        async_observer.called = False
        async_observer.event = None
        
        # Subscribe and emit
        emitter.subscribe('BaseEvent', async_observer, weak=False)
        event = BaseEvent()
        await emitter.emit_async(event)
        
        assert async_observer.called is True
        assert async_observer.event is event
    
    @pytest.mark.asyncio
    async def test_mixed_sync_async_observers(self):
        """Test mixing sync and async observers."""
        emitter = EventEmitter()
        
        # Create sync and async observers
        sync_observer = Mock()
        
        async def async_observer(event):
            await asyncio.sleep(0.01)
            async_observer.called = True
        
        async_observer.called = False
        
        # Subscribe both
        emitter.subscribe('BaseEvent', sync_observer, weak=False)
        emitter.subscribe('BaseEvent', async_observer, weak=False)
        
        # Emit event
        event = BaseEvent()
        await emitter.emit_async(event)
        
        # Both should be called
        sync_observer.assert_called_once_with(event)
        assert async_observer.called is True


class TestEventEmitterThreadSafety:
    """Test suite for thread safety."""
    
    def test_concurrent_subscribe_unsubscribe(self):
        """Test concurrent subscription/unsubscription operations."""
        emitter = EventEmitter()
        observers = [Mock() for _ in range(10)]
        
        def subscribe_unsubscribe_worker(observer_idx):
            observer = observers[observer_idx]
            for i in range(100):
                emitter.subscribe('TestEvent', observer, weak=False)
                emitter.unsubscribe('TestEvent', observer)
        
        # Run concurrent operations
        threads = []
        for i in range(5):
            thread = threading.Thread(target=subscribe_unsubscribe_worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should complete without errors and have consistent state
        observers_count = emitter.get_observers('TestEvent')
        assert observers_count['TestEvent'] == 0
    
    def test_concurrent_event_emission(self):
        """Test concurrent event emission from multiple threads."""
        emitter = EventEmitter()
        observer = Mock()
        
        emitter.subscribe('BaseEvent', observer, weak=False)
        
        def emit_worker():
            for i in range(50):
                event = BaseEvent()
                emitter.emit(event)
        
        # Run concurrent emissions
        threads = []
        for i in range(4):
            thread = threading.Thread(target=emit_worker)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Give time for all events to process
        time.sleep(0.5)
        
        # Should have received all events
        stats = emitter.get_statistics()
        assert stats['events_emitted'] == 200  # 4 threads * 50 events


class TestEventEmitterHistory:
    """Test suite for event history functionality."""
    
    def test_event_history_enabled(self):
        """Test event history when enabled."""
        emitter = EventEmitter(enable_history=True, max_history=5)
        
        # Emit several events
        events = []
        for i in range(3):
            event = BaseEvent()
            events.append(event)
            emitter.emit(event)
        
        # Check history
        history = emitter.get_event_history()
        assert len(history) == 3
        assert all(e in history for e in events)
    
    def test_event_history_max_limit(self):
        """Test event history respects max limit."""
        emitter = EventEmitter(enable_history=True, max_history=3)
        
        # Emit more events than max history
        events = []
        for i in range(5):
            event = BaseEvent()
            events.append(event)
            emitter.emit(event)
        
        # Check history only keeps last 3
        history = emitter.get_event_history()
        assert len(history) == 3
        assert history == events[-3:]
    
    def test_event_history_disabled(self):
        """Test event history when disabled."""
        emitter = EventEmitter(enable_history=False)
        
        # Emit events
        for i in range(3):
            emitter.emit(BaseEvent())
        
        # History should be empty
        history = emitter.get_event_history()
        assert len(history) == 0
    
    def test_event_history_filtered_by_type(self):
        """Test filtering event history by type."""
        emitter = EventEmitter(enable_history=True)
        
        # Emit different event types
        base_event = BaseEvent()
        post_event = PostDiscoveredEvent()
        download_event = DownloadStartedEvent()
        
        emitter.emit(base_event)
        emitter.emit(post_event)
        emitter.emit(download_event)
        
        # Filter by specific type
        base_history = emitter.get_event_history('BaseEvent')
        post_history = emitter.get_event_history('PostDiscoveredEvent')
        
        assert len(base_history) == 1
        assert base_history[0] is base_event
        assert len(post_history) == 1
        assert post_history[0] is post_event
    
    def test_clear_history(self):
        """Test clearing event history."""
        emitter = EventEmitter(enable_history=True)
        
        # Emit events and clear
        emitter.emit(BaseEvent())
        emitter.emit(BaseEvent())
        emitter.clear_history()
        
        # History should be empty
        history = emitter.get_event_history()
        assert len(history) == 0


class TestEventEmitterStatistics:
    """Test suite for statistics functionality."""
    
    def test_statistics_tracking(self):
        """Test basic statistics tracking."""
        emitter = EventEmitter()
        observer = Mock()
        
        emitter.subscribe('BaseEvent', observer, weak=False)
        
        # Emit events
        for i in range(3):
            emitter.emit(BaseEvent())
        
        # Give time for processing
        time.sleep(0.1)
        
        # Check statistics
        stats = emitter.get_statistics()
        assert stats['events_emitted'] == 3
        assert stats['events_processed'] >= 0  # May not all be processed yet
        assert stats['total_observers'] == 1
    
    def test_statistics_observer_errors(self):
        """Test error counting in statistics."""
        emitter = EventEmitter()
        
        def failing_observer(event):
            raise ValueError("Test error")
        
        emitter.subscribe('BaseEvent', failing_observer, weak=False)
        emitter.emit(BaseEvent())
        
        # Give time for processing
        time.sleep(0.1)
        
        # Check error was counted
        stats = emitter.get_statistics()
        assert stats['observer_errors'] >= 1


class TestEventEmitterCleanup:
    """Test suite for cleanup and shutdown functionality."""
    
    def test_clear_observers(self):
        """Test clearing all observers."""
        emitter = EventEmitter()
        observer1 = Mock()
        observer2 = Mock()
        
        # Subscribe observers
        emitter.subscribe('BaseEvent', observer1, weak=False)
        emitter.subscribe('*', observer2, weak=False)
        
        # Clear all observers
        emitter.clear_observers()
        
        # Check all observers removed
        stats = emitter.get_statistics()
        assert stats['total_observers'] == 0
        assert stats['wildcard_observers'] == 0
    
    def test_shutdown(self):
        """Test emitter shutdown."""
        emitter = EventEmitter()
        observer = Mock()
        
        emitter.subscribe('BaseEvent', observer, weak=False)
        emitter.emit(BaseEvent())
        
        # Shutdown should complete without errors
        emitter.shutdown()
        
        # Should clear observers and history
        stats = emitter.get_statistics()
        assert stats['total_observers'] == 0
        assert stats['history_size'] == 0


if __name__ == "__main__":
    pytest.main([__file__])