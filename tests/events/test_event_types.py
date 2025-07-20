#!/usr/bin/env python3
"""
Tests for RedditDL Event Types

Comprehensive tests for all event type classes and their functionality.
"""

import pytest
import time
from datetime import datetime

import sys
# sys.path.insert(0, '.') # This line is no longer needed with src/ layout

from redditdl.core.events.types import (
    BaseEvent, PostDiscoveredEvent, DownloadStartedEvent,
    DownloadProgressEvent, DownloadCompletedEvent, PostProcessedEvent,
    FilterAppliedEvent, PipelineStageEvent, ErrorEvent, StatisticsEvent,
    create_event_from_dict, EVENT_TYPES
)


class TestBaseEvent:
    """Test suite for BaseEvent class."""
    
    def test_base_event_initialization(self):
        """Test BaseEvent initialization with defaults."""
        event = BaseEvent()
        
        assert event.timestamp > 0
        assert isinstance(event.session_id, str)
        assert len(event.session_id) == 8
        assert isinstance(event.event_id, str)
        assert len(event.event_id) == 12
        assert event.event_type == "BaseEvent"
    
    def test_base_event_custom_values(self):
        """Test BaseEvent with custom values."""
        timestamp = time.time()
        session_id = "test1234"
        event_id = "testevent123"
        
        event = BaseEvent(
            timestamp=timestamp,
            session_id=session_id,
            event_id=event_id
        )
        
        assert event.timestamp == timestamp
        assert event.session_id == session_id
        assert event.event_id == event_id
    
    def test_datetime_property(self):
        """Test datetime property conversion."""
        event = BaseEvent()
        dt = event.datetime
        
        assert isinstance(dt, datetime)
        # Allow for small floating point precision differences
        assert abs(dt.timestamp() - event.timestamp) < 0.001
    
    def test_to_dict(self):
        """Test event serialization to dictionary."""
        event = BaseEvent()
        event_dict = event.to_dict()
        
        required_keys = {'event_type', 'timestamp', 'session_id', 'event_id', 'datetime'}
        assert all(key in event_dict for key in required_keys)
        assert event_dict['event_type'] == 'BaseEvent'
        assert event_dict['timestamp'] == event.timestamp
        assert event_dict['session_id'] == event.session_id
        assert event_dict['event_id'] == event.event_id


class TestPostDiscoveredEvent:
    """Test suite for PostDiscoveredEvent."""
    
    def test_initialization_defaults(self):
        """Test PostDiscoveredEvent with default values."""
        event = PostDiscoveredEvent()
        
        assert event.event_type == "PostDiscoveredEvent"
        assert event.post_count == 0
        assert event.source == ""
        assert event.target == ""
        assert event.source_type == ""
        assert event.posts_preview == []
    
    def test_initialization_custom(self):
        """Test PostDiscoveredEvent with custom values."""
        posts_preview = [{"id": "test1", "title": "Test Post"}]
        
        event = PostDiscoveredEvent(
            post_count=25,
            source="reddit_api",
            target="testuser",
            source_type="user",
            posts_preview=posts_preview
        )
        
        assert event.post_count == 25
        assert event.source == "reddit_api"
        assert event.target == "testuser"
        assert event.source_type == "user"
        assert event.posts_preview == posts_preview
    
    def test_to_dict_includes_custom_fields(self):
        """Test that to_dict includes all custom fields."""
        event = PostDiscoveredEvent(post_count=10, source="test")
        event_dict = event.to_dict()
        
        assert event_dict['post_count'] == 10
        assert event_dict['source'] == "test"


class TestDownloadProgressEvent:
    """Test suite for DownloadProgressEvent."""
    
    def test_progress_percentage_calculation(self):
        """Test progress percentage calculation."""
        event = DownloadProgressEvent(
            bytes_downloaded=500,
            total_bytes=1000
        )
        
        assert event.progress_percentage == 50.0
    
    def test_progress_percentage_no_total(self):
        """Test progress percentage when total is unknown."""
        event = DownloadProgressEvent(
            bytes_downloaded=500,
            total_bytes=None
        )
        
        assert event.progress_percentage is None
    
    def test_progress_ratio_calculation(self):
        """Test progress ratio calculation."""
        event = DownloadProgressEvent(
            bytes_downloaded=250,
            total_bytes=1000
        )
        
        assert event.progress_ratio == 0.25
    
    def test_progress_ratio_no_total(self):
        """Test progress ratio when total is unknown."""
        event = DownloadProgressEvent(
            bytes_downloaded=250,
            total_bytes=None
        )
        
        assert event.progress_ratio is None


class TestFilterAppliedEvent:
    """Test suite for FilterAppliedEvent."""
    
    def test_filter_percentage_calculation(self):
        """Test filter percentage calculation."""
        event = FilterAppliedEvent(
            posts_before=100,
            posts_after=75,
            posts_filtered=25
        )
        
        assert event.filter_percentage == 25.0
    
    def test_filter_percentage_no_posts(self):
        """Test filter percentage when no posts to start."""
        event = FilterAppliedEvent(
            posts_before=0,
            posts_after=0,
            posts_filtered=0
        )
        
        assert event.filter_percentage == 0.0


class TestStatisticsEvent:
    """Test suite for StatisticsEvent."""
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        event = StatisticsEvent(
            posts_processed=100,
            posts_successful=85,
            posts_failed=15
        )
        
        assert event.success_rate == 85.0
    
    def test_completion_percentage_calculation(self):
        """Test completion percentage calculation."""
        event = StatisticsEvent(
            total_posts=200,
            posts_processed=50
        )
        
        assert event.completion_percentage == 25.0
    
    def test_zero_division_handling(self):
        """Test that zero division is handled gracefully."""
        event = StatisticsEvent(
            total_posts=0,
            posts_processed=0
        )
        
        assert event.success_rate == 0.0
        assert event.completion_percentage == 0.0


class TestEventSerialization:
    """Test suite for event serialization/deserialization."""
    
    def test_create_event_from_dict_base_event(self):
        """Test creating BaseEvent from dictionary."""
        event_data = {
            'event_type': 'BaseEvent',
            'timestamp': time.time(),
            'session_id': 'test1234',
            'event_id': 'testevent123'
        }
        
        event = create_event_from_dict(event_data)
        
        assert isinstance(event, BaseEvent)
        assert event.timestamp == event_data['timestamp']
        assert event.session_id == event_data['session_id']
        assert event.event_id == event_data['event_id']
    
    def test_create_event_from_dict_specific_event(self):
        """Test creating specific event type from dictionary."""
        event_data = {
            'event_type': 'PostDiscoveredEvent',
            'timestamp': time.time(),
            'session_id': 'test1234',
            'event_id': 'testevent123',
            'post_count': 50,
            'source': 'reddit_api',
            'target': 'testuser',
            'source_type': 'user'
        }
        
        event = create_event_from_dict(event_data)
        
        assert isinstance(event, PostDiscoveredEvent)
        assert event.post_count == 50
        assert event.source == 'reddit_api'
        assert event.target == 'testuser'
        assert event.source_type == 'user'
    
    def test_create_event_from_dict_missing_type(self):
        """Test error handling for missing event type."""
        event_data = {
            'timestamp': time.time(),
            'session_id': 'test1234'
        }
        
        with pytest.raises(ValueError, match="Event data must contain 'event_type' field"):
            create_event_from_dict(event_data)
    
    def test_create_event_from_dict_unknown_type(self):
        """Test error handling for unknown event type."""
        event_data = {
            'event_type': 'UnknownEvent',
            'timestamp': time.time()
        }
        
        with pytest.raises(ValueError, match="Unknown event type: UnknownEvent"):
            create_event_from_dict(event_data)
    
    def test_create_event_from_dict_invalid_data(self):
        """Test error handling for invalid event data."""
        event_data = {
            'event_type': 'PostDiscoveredEvent',
            'invalid_field': 'invalid_value'
        }
        
        with pytest.raises(ValueError, match="Invalid event data"):
            create_event_from_dict(event_data)
    
    def test_round_trip_serialization(self):
        """Test that events can be serialized and deserialized."""
        original_event = DownloadProgressEvent(
            post_id="test123",
            url="https://example.com/test.jpg",
            filename="test.jpg",
            bytes_downloaded=1024,
            total_bytes=2048,
            download_speed=1024.0,
            eta_seconds=1.0
        )
        
        # Serialize to dict
        event_dict = original_event.to_dict()
        
        # Deserialize back to event
        recreated_event = create_event_from_dict(event_dict)
        
        # Compare all important fields
        assert isinstance(recreated_event, DownloadProgressEvent)
        assert recreated_event.post_id == original_event.post_id
        assert recreated_event.url == original_event.url
        assert recreated_event.filename == original_event.filename
        assert recreated_event.bytes_downloaded == original_event.bytes_downloaded
        assert recreated_event.total_bytes == original_event.total_bytes
        assert recreated_event.download_speed == original_event.download_speed
        assert recreated_event.eta_seconds == original_event.eta_seconds


class TestEventTypeRegistry:
    """Test suite for event type registry."""
    
    def test_event_types_registry_complete(self):
        """Test that all event types are in the registry."""
        expected_types = {
            'BaseEvent', 'PostDiscoveredEvent', 'DownloadStartedEvent',
            'DownloadProgressEvent', 'DownloadCompletedEvent', 'PostProcessedEvent',
            'FilterAppliedEvent', 'PipelineStageEvent', 'ErrorEvent', 'StatisticsEvent'
        }
        
        assert set(EVENT_TYPES.keys()) == expected_types
    
    def test_event_types_registry_classes(self):
        """Test that registry contains correct classes."""
        assert EVENT_TYPES['BaseEvent'] is BaseEvent
        assert EVENT_TYPES['PostDiscoveredEvent'] is PostDiscoveredEvent
        assert EVENT_TYPES['DownloadStartedEvent'] is DownloadStartedEvent
        assert EVENT_TYPES['DownloadProgressEvent'] is DownloadProgressEvent
        assert EVENT_TYPES['DownloadCompletedEvent'] is DownloadCompletedEvent
        assert EVENT_TYPES['PostProcessedEvent'] is PostProcessedEvent
        assert EVENT_TYPES['FilterAppliedEvent'] is FilterAppliedEvent
        assert EVENT_TYPES['PipelineStageEvent'] is PipelineStageEvent
        assert EVENT_TYPES['ErrorEvent'] is ErrorEvent
        assert EVENT_TYPES['StatisticsEvent'] is StatisticsEvent


if __name__ == "__main__":
    pytest.main([__file__])