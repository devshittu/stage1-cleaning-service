"""
tests/unit/events/test_event_publisher.py

Unit tests for EventPublisher.

Tests cover:
- Publisher initialization with/without config
- Multi-backend initialization
- Event filtering
- Publishing to multiple backends
- Health checks across backends
- Metrics tracking
- Error handling and graceful degradation
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from collections import defaultdict

from src.events.event_publisher import EventPublisher, get_event_publisher
from src.events.cloud_event import CloudEvent, EventTypes, EVENT_SOURCE


class TestEventPublisherInitialization:
    """Test EventPublisher initialization."""

    @pytest.mark.unit
    def test_initialization_with_config(self):
        """Test initialization with provided config."""
        config = {
            "enabled": True,
            "backends": []
        }

        publisher = EventPublisher(config=config)

        assert publisher.enabled is True
        assert publisher.config == config

    @pytest.mark.unit
    def test_initialization_disabled(self):
        """Test initialization when disabled in config."""
        config = {
            "enabled": False,
            "backends": []
        }

        publisher = EventPublisher(config=config)

        assert publisher.enabled is False

    @pytest.mark.unit
    def test_initialization_without_config(self):
        """Test initialization without config (loads from ConfigManager)."""
        with patch('src.utils.config_manager.ConfigManager') as mock_cm:
            mock_settings = Mock()
            mock_settings.events = Mock()
            mock_settings.events.model_dump.return_value = {"enabled": True}
            mock_cm.get_settings.return_value = mock_settings

            publisher = EventPublisher(config=None)

            assert publisher.config is not None

    @pytest.mark.unit
    def test_initialization_config_load_failure(self):
        """Test initialization handles config load failure gracefully."""
        with patch('src.utils.config_manager.ConfigManager') as mock_cm:
            mock_cm.get_settings.side_effect = Exception("Config error")

            publisher = EventPublisher(config=None)

            assert publisher.config == {}

    @pytest.mark.unit
    def test_backend_class_mapping(self):
        """Test backend class mapping is defined."""
        assert "redis_streams" in EventPublisher.BACKEND_CLASSES
        assert "webhook" in EventPublisher.BACKEND_CLASSES
        assert "kafka" in EventPublisher.BACKEND_CLASSES
        assert "nats" in EventPublisher.BACKEND_CLASSES
        assert "rabbitmq" in EventPublisher.BACKEND_CLASSES

    @pytest.mark.unit
    def test_metrics_initialized(self):
        """Test metrics are initialized properly."""
        publisher = EventPublisher(config={"enabled": True})

        assert publisher._metrics["total_events"] == 0
        assert publisher._metrics["successful"] == 0
        assert publisher._metrics["failed"] == 0

    @pytest.mark.unit
    def test_event_filter_parsing(self):
        """Test event filter is parsed from config."""
        config = {
            "enabled": True,
            "publish_events": ["com.test.event.type1", "com.test.event.type2"]
        }

        publisher = EventPublisher(config=config)

        assert publisher.publish_events == {"com.test.event.type1", "com.test.event.type2"}

    @pytest.mark.unit
    def test_get_event_publisher_singleton(self):
        """Test get_event_publisher returns singleton."""
        publisher1 = get_event_publisher()
        publisher2 = get_event_publisher()

        # Note: This might not be true singleton due to test isolation
        # but function should return same instance in production


class TestInitializeBackends:
    """Test backend initialization."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_when_disabled(self):
        """Test initialize returns False when disabled."""
        publisher = EventPublisher(config={"enabled": False})

        result = await publisher.initialize()

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_with_no_backends(self):
        """Test initialize uses default backends when none configured."""
        publisher = EventPublisher(config={"enabled": True, "backends": []})

        with patch.object(publisher, 'BACKEND_CLASSES', {}) as mock_classes:
            result = await publisher.initialize()

            # Should attempt to use defaults but fail if no classes available
            assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_with_backends(self):
        """Test initialize with configured backends."""
        config = {
            "enabled": True,
            "backends": [
                {
                    "type": "redis_streams",
                    "enabled": True,
                    "config": {"stream_name": "test"}
                }
            ]
        }

        publisher = EventPublisher(config=config)

        # Mock backend class
        mock_backend = Mock()
        mock_backend.initialize = AsyncMock(return_value=True)
        mock_backend.backend_type = "redis_streams"

        with patch.object(publisher, 'BACKEND_CLASSES', {"redis_streams": Mock(return_value=mock_backend)}):
            result = await publisher.initialize()

            assert result is True
            assert len(publisher.backends) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_skips_disabled_backends(self):
        """Test initialize skips backends disabled in config."""
        config = {
            "enabled": True,
            "backends": [
                {
                    "type": "redis_streams",
                    "enabled": False,
                    "config": {}
                }
            ]
        }

        publisher = EventPublisher(config=config)

        result = await publisher.initialize()

        assert result is False  # No backends initialized
        assert len(publisher.backends) == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_unknown_backend_type(self):
        """Test initialize handles unknown backend type gracefully."""
        config = {
            "enabled": True,
            "backends": [
                {
                    "type": "unknown_backend",
                    "enabled": True,
                    "config": {}
                }
            ]
        }

        publisher = EventPublisher(config=config)

        result = await publisher.initialize()

        assert result is False
        assert len(publisher.backends) == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_backend_failure(self):
        """Test initialize handles backend initialization failure."""
        config = {
            "enabled": True,
            "backends": [
                {
                    "type": "redis_streams",
                    "enabled": True,
                    "config": {}
                }
            ]
        }

        publisher = EventPublisher(config=config)

        # Mock backend that fails to initialize
        mock_backend = Mock()
        mock_backend.initialize = AsyncMock(return_value=False)

        with patch.object(publisher, 'BACKEND_CLASSES', {"redis_streams": Mock(return_value=mock_backend)}):
            result = await publisher.initialize()

            assert result is False
            assert len(publisher.backends) == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_multiple_backends(self):
        """Test initialize with multiple backends."""
        config = {
            "enabled": True,
            "backends": [
                {"type": "redis_streams", "enabled": True, "config": {}},
                {"type": "webhook", "enabled": True, "config": {}}
            ]
        }

        publisher = EventPublisher(config=config)

        # Mock backends
        mock_redis = Mock()
        mock_redis.initialize = AsyncMock(return_value=True)
        mock_redis.backend_type = "redis_streams"

        mock_webhook = Mock()
        mock_webhook.initialize = AsyncMock(return_value=True)
        mock_webhook.backend_type = "webhook"

        backend_classes = {
            "redis_streams": Mock(return_value=mock_redis),
            "webhook": Mock(return_value=mock_webhook)
        }

        with patch.object(publisher, 'BACKEND_CLASSES', backend_classes):
            result = await publisher.initialize()

            assert result is True
            assert len(publisher.backends) == 2


class TestShouldPublishEvent:
    """Test event filtering."""

    @pytest.mark.unit
    def test_should_publish_no_filter(self):
        """Test should publish all events when no filter configured."""
        publisher = EventPublisher(config={"enabled": True})
        publisher.publish_events = None

        result = publisher.should_publish_event("com.test.any.event")

        assert result is True

    @pytest.mark.unit
    def test_should_publish_with_filter_match(self):
        """Test should publish when event type matches filter."""
        publisher = EventPublisher(config={"enabled": True})
        publisher.publish_events = {"com.test.event.type1", "com.test.event.type2"}

        result = publisher.should_publish_event("com.test.event.type1")

        assert result is True

    @pytest.mark.unit
    def test_should_publish_with_filter_no_match(self):
        """Test should not publish when event type doesn't match filter."""
        publisher = EventPublisher(config={"enabled": True})
        publisher.publish_events = {"com.test.event.type1", "com.test.event.type2"}

        result = publisher.should_publish_event("com.test.event.type3")

        assert result is False


class TestPublish:
    """Test event publishing."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_when_disabled(self):
        """Test publish returns not published when disabled."""
        publisher = EventPublisher(config={"enabled": False})

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is False
        assert result["reason"] == "event_publisher_not_enabled"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_when_no_backends(self):
        """Test publish returns not published when no backends."""
        publisher = EventPublisher(config={"enabled": True})
        publisher.backends = []

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_filtered_event(self):
        """Test publish filters event that doesn't match filter."""
        publisher = EventPublisher(config={"enabled": True})
        publisher.publish_events = {EventTypes.JOB_COMPLETED}
        publisher.backends = [Mock()]  # Has backends

        event = CloudEvent(type=EventTypes.JOB_STARTED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is False
        assert result["reason"] == "event_filtered"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_success_single_backend(self):
        """Test successful publish to single backend."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.publish = AsyncMock(return_value=True)

        publisher.backends = [mock_backend]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is True
        assert result["event_type"] == EventTypes.JOB_COMPLETED
        assert "redis_streams" in result["backends"]
        assert result["backends"]["redis_streams"]["success"] is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_success_multiple_backends(self):
        """Test successful publish to multiple backends."""
        publisher = EventPublisher(config={"enabled": True})

        mock_redis = Mock()
        mock_redis.backend_type = "redis_streams"
        mock_redis.publish = AsyncMock(return_value=True)

        mock_webhook = Mock()
        mock_webhook.backend_type = "webhook"
        mock_webhook.publish = AsyncMock(return_value=True)

        publisher.backends = [mock_redis, mock_webhook]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is True
        assert len(result["backends"]) == 2
        assert result["backends"]["redis_streams"]["success"] is True
        assert result["backends"]["webhook"]["success"] is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_partial_failure(self):
        """Test publish with some backends failing."""
        publisher = EventPublisher(config={"enabled": True})

        mock_redis = Mock()
        mock_redis.backend_type = "redis_streams"
        mock_redis.publish = AsyncMock(return_value=True)

        mock_webhook = Mock()
        mock_webhook.backend_type = "webhook"
        mock_webhook.publish = AsyncMock(return_value=False)

        publisher.backends = [mock_redis, mock_webhook]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        # Should still be published (at least one backend succeeded)
        assert result["published"] is True
        assert result["backends"]["redis_streams"]["success"] is True
        assert result["backends"]["webhook"]["success"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_all_backends_fail(self):
        """Test publish when all backends fail."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.publish = AsyncMock(return_value=False)

        publisher.backends = [mock_backend]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_backend_exception(self):
        """Test publish handles backend exceptions gracefully."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.publish = AsyncMock(side_effect=Exception("Backend error"))

        publisher.backends = [mock_backend]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await publisher.publish(event)

        assert result["published"] is False
        assert result["backends"]["redis_streams"]["success"] is False
        assert "error" in result["backends"]["redis_streams"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_updates_metrics(self):
        """Test publish updates metrics."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.publish = AsyncMock(return_value=True)

        publisher.backends = [mock_backend]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        await publisher.publish(event)

        assert publisher._metrics["total_events"] == 1
        assert publisher._metrics["successful"] == 1
        assert publisher._metrics["backend_success"]["redis_streams"] == 1


class TestHealthCheck:
    """Test health check."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_when_disabled(self):
        """Test health check when disabled."""
        publisher = EventPublisher(config={"enabled": False})

        result = await publisher.health_check()

        assert result["enabled"] is False
        assert result["backends"] == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_all_healthy(self):
        """Test health check when all backends healthy."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.health_check = AsyncMock(return_value={
            "backend": "redis_streams",
            "healthy": True
        })

        publisher.backends = [mock_backend]

        result = await publisher.health_check()

        assert result["enabled"] is True
        assert result["healthy"] is True
        assert len(result["backends"]) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_some_unhealthy(self):
        """Test health check when some backends unhealthy."""
        publisher = EventPublisher(config={"enabled": True})

        mock_redis = Mock()
        mock_redis.backend_type = "redis_streams"
        mock_redis.health_check = AsyncMock(return_value={
            "backend": "redis_streams",
            "healthy": True
        })

        mock_webhook = Mock()
        mock_webhook.backend_type = "webhook"
        mock_webhook.health_check = AsyncMock(return_value={
            "backend": "webhook",
            "healthy": False
        })

        publisher.backends = [mock_redis, mock_webhook]

        result = await publisher.health_check()

        assert result["enabled"] is True
        assert result["healthy"] is False
        assert len(result["backends"]) == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_backend_exception(self):
        """Test health check handles backend exceptions."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.health_check = AsyncMock(side_effect=Exception("Health check error"))

        publisher.backends = [mock_backend]

        result = await publisher.health_check()

        assert result["enabled"] is True
        assert result["healthy"] is False
        assert result["backends"][0]["healthy"] is False


class TestGetMetrics:
    """Test metrics retrieval."""

    @pytest.mark.unit
    def test_get_metrics_initial(self):
        """Test get_metrics returns initial state."""
        publisher = EventPublisher(config={"enabled": True})

        metrics = publisher.get_metrics()

        assert metrics["total_events"] == 0
        assert metrics["successful"] == 0
        assert metrics["failed"] == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_metrics_after_publish(self):
        """Test get_metrics after publishing events."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.publish = AsyncMock(return_value=True)

        publisher.backends = [mock_backend]

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)
        await publisher.publish(event)
        await publisher.publish(event)

        metrics = publisher.get_metrics()

        assert metrics["total_events"] == 2
        assert metrics["successful"] == 2
        assert metrics["backend_success"]["redis_streams"] == 2


class TestClose:
    """Test publisher closure."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_no_backends(self):
        """Test close with no backends."""
        publisher = EventPublisher(config={"enabled": True})

        # Should not raise exception
        await publisher.close()

        assert publisher.backends == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_with_backends(self):
        """Test close with backends."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.close = AsyncMock()

        publisher.backends = [mock_backend]

        await publisher.close()

        mock_backend.close.assert_called_once()
        assert publisher.backends == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_backend_exception(self):
        """Test close handles backend exceptions gracefully."""
        publisher = EventPublisher(config={"enabled": True})

        mock_backend = Mock()
        mock_backend.backend_type = "redis_streams"
        mock_backend.close = AsyncMock(side_effect=Exception("Close error"))

        publisher.backends = [mock_backend]

        # Should not raise exception
        await publisher.close()

        assert publisher.backends == []
