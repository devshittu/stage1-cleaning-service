"""
tests/unit/events/test_cloud_event.py

Unit tests for CloudEvent model.

Tests cover:
- CloudEvents v1.0 specification compliance
- Required and optional fields
- Event type constants
- Serialization methods
- HTTP header generation
"""

import json
import pytest
from datetime import datetime

from src.events.cloud_event import CloudEvent, EventTypes, EVENT_SOURCE


class TestEventTypes:
    """Test EventTypes constants."""

    @pytest.mark.unit
    def test_all_event_types_defined(self):
        """Test all expected event types are defined."""
        expected_types = [
            "JOB_STARTED",
            "JOB_PROGRESS",
            "JOB_PAUSED",
            "JOB_RESUMED",
            "JOB_COMPLETED",
            "JOB_FAILED",
            "JOB_CANCELLED"
        ]

        for event_type in expected_types:
            assert hasattr(EventTypes, event_type)

    @pytest.mark.unit
    def test_event_types_follow_naming_convention(self):
        """Test event types follow reverse-DNS naming convention."""
        all_types = [
            EventTypes.JOB_STARTED,
            EventTypes.JOB_PROGRESS,
            EventTypes.JOB_COMPLETED,
            EventTypes.JOB_FAILED
        ]

        for event_type in all_types:
            assert event_type.startswith("com.storytelling.cleaning.")
            assert event_type.count(".") >= 3  # At least 4 segments


class TestCloudEventCreation:
    """Test CloudEvent creation and validation."""

    @pytest.mark.unit
    def test_minimal_cloud_event(self):
        """Test creation with required fields only."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE
        )

        assert event.specversion == "1.0"
        assert event.type == EventTypes.JOB_STARTED
        assert event.source == EVENT_SOURCE
        assert event.id is not None  # Auto-generated
        assert event.time is not None  # Auto-generated

    @pytest.mark.unit
    def test_auto_generated_id(self):
        """Test ID is auto-generated as UUID."""
        event1 = CloudEvent(type=EventTypes.JOB_STARTED, source=EVENT_SOURCE)
        event2 = CloudEvent(type=EventTypes.JOB_STARTED, source=EVENT_SOURCE)

        assert event1.id != event2.id
        assert len(event1.id) == 36  # UUID format

    @pytest.mark.unit
    def test_auto_generated_timestamp(self):
        """Test timestamp is auto-generated in ISO 8601 format."""
        event = CloudEvent(type=EventTypes.JOB_STARTED, source=EVENT_SOURCE)

        assert event.time is not None
        assert "T" in event.time  # ISO 8601 format
        assert event.time.endswith("Z")  # UTC indicator

    @pytest.mark.unit
    def test_custom_id_accepted(self):
        """Test custom ID can be provided."""
        custom_id = "custom-event-id-123"
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            id=custom_id
        )

        assert event.id == custom_id

    @pytest.mark.unit
    def test_custom_timestamp_accepted(self):
        """Test custom timestamp can be provided."""
        custom_time = "2024-01-15T10:30:00Z"
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            time=custom_time
        )

        assert event.time == custom_time

    @pytest.mark.unit
    def test_with_subject(self):
        """Test event with subject field."""
        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            subject="job/test-job-123"
        )

        assert event.subject == "job/test-job-123"

    @pytest.mark.unit
    def test_with_data_payload(self):
        """Test event with data payload."""
        data = {
            "job_id": "test-job-123",
            "status": "completed",
            "documents_processed": 100
        }

        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            data=data
        )

        assert event.data == data
        assert event.data["job_id"] == "test-job-123"

    @pytest.mark.unit
    def test_default_datacontenttype(self):
        """Test default datacontenttype is application/json."""
        event = CloudEvent(type=EventTypes.JOB_STARTED, source=EVENT_SOURCE)

        assert event.datacontenttype == "application/json"

    @pytest.mark.unit
    def test_custom_datacontenttype(self):
        """Test custom datacontenttype can be set."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            datacontenttype="text/plain"
        )

        assert event.datacontenttype == "text/plain"


class TestCloudEventSerialization:
    """Test CloudEvent serialization methods."""

    @pytest.mark.unit
    def test_to_dict(self):
        """Test conversion to dictionary."""
        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            subject="job/test-123",
            data={"job_id": "test-123", "status": "completed"}
        )

        event_dict = event.to_dict()

        assert event_dict["type"] == EventTypes.JOB_COMPLETED
        assert event_dict["source"] == EVENT_SOURCE
        assert event_dict["subject"] == "job/test-123"
        assert event_dict["data"]["job_id"] == "test-123"

    @pytest.mark.unit
    def test_to_dict_excludes_none(self):
        """Test to_dict excludes None values."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            subject=None,  # Explicitly None
            data=None
        )

        event_dict = event.to_dict()

        assert "subject" not in event_dict
        assert "data" not in event_dict

    @pytest.mark.unit
    def test_to_json(self):
        """Test conversion to JSON string."""
        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            data={"job_id": "test-123"}
        )

        json_str = event.to_json()

        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["type"] == EventTypes.JOB_COMPLETED

    @pytest.mark.unit
    def test_json_parseable(self):
        """Test JSON output is parseable."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            data={"test": "data"}
        )

        json_str = event.to_json()
        parsed = json.loads(json_str)

        assert parsed["specversion"] == "1.0"
        assert parsed["data"]["test"] == "data"


class TestCloudEventHttpHeaders:
    """Test CloudEvents HTTP header generation."""

    @pytest.mark.unit
    def test_get_http_headers_required_fields(self):
        """Test HTTP headers include all required CloudEvents fields."""
        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            id="test-id-123"
        )

        headers = event.get_http_headers()

        assert headers["ce-specversion"] == "1.0"
        assert headers["ce-type"] == EventTypes.JOB_COMPLETED
        assert headers["ce-source"] == EVENT_SOURCE
        assert headers["ce-id"] == "test-id-123"

    @pytest.mark.unit
    def test_get_http_headers_with_time(self):
        """Test HTTP headers include time if present."""
        custom_time = "2024-01-15T10:30:00Z"
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            time=custom_time
        )

        headers = event.get_http_headers()

        assert headers["ce-time"] == custom_time

    @pytest.mark.unit
    def test_get_http_headers_with_subject(self):
        """Test HTTP headers include subject if present."""
        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            subject="job/test-123"
        )

        headers = event.get_http_headers()

        assert headers["ce-subject"] == "job/test-123"

    @pytest.mark.unit
    def test_get_http_headers_content_type(self):
        """Test HTTP headers include Content-Type from datacontenttype."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            datacontenttype="application/json"
        )

        headers = event.get_http_headers()

        assert headers["Content-Type"] == "application/json"

    @pytest.mark.unit
    def test_get_http_headers_without_optional_fields(self):
        """Test HTTP headers work without optional fields."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            subject=None,
            time=None
        )

        headers = event.get_http_headers()

        assert "ce-specversion" in headers
        assert "ce-type" in headers
        assert "ce-source" in headers
        assert "ce-id" in headers


class TestCloudEventCompliance:
    """Test CloudEvents v1.0 specification compliance."""

    @pytest.mark.unit
    def test_spec_version_is_1_0(self):
        """Test specversion is always 1.0."""
        event = CloudEvent(type=EventTypes.JOB_STARTED, source=EVENT_SOURCE)

        assert event.specversion == "1.0"

    @pytest.mark.unit
    def test_type_is_required(self):
        """Test type field is required."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            CloudEvent(source=EVENT_SOURCE)  # Missing type

    @pytest.mark.unit
    def test_source_is_required(self):
        """Test source field is required."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            CloudEvent(type=EventTypes.JOB_STARTED)  # Missing source

    @pytest.mark.unit
    def test_complete_lifecycle_events(self):
        """Test all job lifecycle event types work."""
        lifecycle_events = [
            EventTypes.JOB_STARTED,
            EventTypes.JOB_PROGRESS,
            EventTypes.JOB_PAUSED,
            EventTypes.JOB_RESUMED,
            EventTypes.JOB_COMPLETED,
            EventTypes.JOB_FAILED,
            EventTypes.JOB_CANCELLED
        ]

        for event_type in lifecycle_events:
            event = CloudEvent(type=event_type, source=EVENT_SOURCE)
            assert event.type == event_type


class TestCloudEventEdgeCases:
    """Test edge cases and special scenarios."""

    @pytest.mark.unit
    def test_empty_data_payload(self):
        """Test event with empty data payload."""
        event = CloudEvent(
            type=EventTypes.JOB_STARTED,
            source=EVENT_SOURCE,
            data={}
        )

        assert event.data == {}

    @pytest.mark.unit
    def test_large_data_payload(self):
        """Test event with large data payload."""
        large_data = {
            f"key_{i}": f"value_{i}" for i in range(1000)
        }

        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            data=large_data
        )

        assert len(event.data) == 1000

    @pytest.mark.unit
    def test_nested_data_structure(self):
        """Test event with deeply nested data."""
        nested_data = {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep"
                    }
                }
            }
        }

        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            data=nested_data
        )

        assert event.data["level1"]["level2"]["level3"]["value"] == "deep"

    @pytest.mark.unit
    def test_special_characters_in_subject(self):
        """Test subject can contain special characters."""
        event = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            subject="job/test-123_with-special.chars@domain"
        )

        assert "@" in event.subject

    @pytest.mark.unit
    def test_roundtrip_serialization(self):
        """Test event can be serialized and deserialized."""
        original = CloudEvent(
            type=EventTypes.JOB_COMPLETED,
            source=EVENT_SOURCE,
            subject="job/test-123",
            data={"job_id": "test-123", "status": "completed"}
        )

        # Serialize to JSON
        json_str = original.to_json()

        # Deserialize back
        parsed = CloudEvent.model_validate_json(json_str)

        assert parsed.type == original.type
        assert parsed.source == original.source
        assert parsed.subject == original.subject
        assert parsed.data == original.data
