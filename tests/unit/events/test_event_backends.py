"""
tests/unit/events/test_event_backends.py

Unit tests for event backends (Redis Streams and Webhook).

Tests cover:
- Backend initialization
- Event publishing (success and failure scenarios)
- Health checks
- Error handling and graceful degradation
- Retry logic (webhook)
- Metrics tracking
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
import json

from src.events.cloud_event import CloudEvent, EventTypes, EVENT_SOURCE


class TestRedisStreamsBackend:
    """Test Redis Streams backend."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test backend initialization with config."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        config = {
            "stream_name": "test:stream",
            "max_len": 5000,
            "ttl_seconds": 3600
        }

        backend = RedisStreamsBackend(config)

        assert backend.stream_name == "test:stream"
        assert backend.max_len == 5000
        assert backend.ttl_seconds == 3600

    @pytest.mark.unit
    def test_default_config(self):
        """Test backend uses default config values."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})

        assert backend.stream_name == "stage1:cleaning:events"
        assert backend.max_len == 10000
        assert backend.ttl_seconds == 86400

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_without_redis_package(self):
        """Test initialize handles missing redis package gracefully."""
        from src.events.backends import redis_streams

        with patch.object(redis_streams, 'REDIS_AVAILABLE', False):
            backend = redis_streams.RedisStreamsBackend({})

            result = await backend.initialize()

            assert result is False
            assert backend.enabled is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_success(self):
        """Test successful backend initialization."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})

        with patch('src.events.backends.redis_streams.aioredis') as mock_aioredis:
            mock_client = Mock()
            mock_client.ping = AsyncMock()
            mock_aioredis.from_url = AsyncMock(return_value=mock_client)

            result = await backend.initialize()

            assert result is True
            assert backend.redis_client is not None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_connection_failure(self):
        """Test initialize handles connection failure."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})

        with patch('src.events.backends.redis_streams.aioredis') as mock_aioredis:
            mock_aioredis.from_url = AsyncMock(side_effect=Exception("Connection failed"))

            result = await backend.initialize()

            assert result is False
            assert backend.enabled is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_when_disabled(self):
        """Test publish returns False when backend disabled."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({"fail_silently": True})
        backend.enabled = False

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await backend.publish(event)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_success(self):
        """Test successful event publish."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})
        backend.enabled = True

        mock_client = Mock()
        mock_client.xadd = AsyncMock(return_value="1234567890-0")
        mock_client.ttl = AsyncMock(return_value=-1)
        mock_client.expire = AsyncMock()

        backend.redis_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE, data={"job_id": "test-123"})

        result = await backend.publish(event)

        assert result is True
        mock_client.xadd.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_sets_ttl(self):
        """Test publish sets TTL on stream if not already set."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({"ttl_seconds": 7200})
        backend.enabled = True

        mock_client = Mock()
        mock_client.xadd = AsyncMock(return_value="1234567890-0")
        mock_client.ttl = AsyncMock(return_value=-1)  # No TTL set
        mock_client.expire = AsyncMock()

        backend.redis_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        await backend.publish(event)

        mock_client.expire.assert_called_once_with(backend.stream_name, 7200)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_skips_ttl_if_set(self):
        """Test publish skips setting TTL if already set."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})
        backend.enabled = True

        mock_client = Mock()
        mock_client.xadd = AsyncMock(return_value="1234567890-0")
        mock_client.ttl = AsyncMock(return_value=3600)  # TTL already set

        backend.redis_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        await backend.publish(event)

        # expire should not be called
        assert not hasattr(mock_client, 'expire') or mock_client.expire.call_count == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_handles_error_silently(self):
        """Test publish handles errors gracefully in fail_silently mode."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({"fail_silently": True})
        backend.enabled = True

        mock_client = Mock()
        mock_client.xadd = AsyncMock(side_effect=Exception("Publish error"))

        backend.redis_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await backend.publish(event)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_raises_without_fail_silently(self):
        """Test publish raises exception when fail_silently is False."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({"fail_silently": False})
        backend.enabled = True

        mock_client = Mock()
        mock_client.xadd = AsyncMock(side_effect=Exception("Publish error"))

        backend.redis_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        with pytest.raises(Exception):
            await backend.publish(event)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_when_disabled(self):
        """Test health check when backend disabled."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})
        backend.enabled = False

        result = await backend.health_check()

        assert result["backend"] == "redis_streams"
        assert result["healthy"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_success(self):
        """Test successful health check."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({"stream_name": "test:stream"})
        backend.enabled = True

        mock_client = Mock()
        mock_client.ping = AsyncMock()
        mock_client.xlen = AsyncMock(return_value=150)

        backend.redis_client = mock_client

        result = await backend.health_check()

        assert result["backend"] == "redis_streams"
        assert result["healthy"] is True
        assert result["stream_length"] == 150

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_failure(self):
        """Test health check handles Redis errors."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})
        backend.enabled = True

        mock_client = Mock()
        mock_client.ping = AsyncMock(side_effect=Exception("Connection lost"))

        backend.redis_client = mock_client

        result = await backend.health_check()

        assert result["backend"] == "redis_streams"
        assert result["healthy"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close(self):
        """Test backend cleanup."""
        from src.events.backends.redis_streams import RedisStreamsBackend

        backend = RedisStreamsBackend({})

        mock_client = Mock()
        mock_client.close = AsyncMock()

        backend.redis_client = mock_client

        await backend.close()

        mock_client.close.assert_called_once()
        assert backend.redis_client is None


class TestWebhookBackend:
    """Test Webhook backend."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test backend initialization with config."""
        from src.events.backends.webhook import WebhookBackend

        config = {
            "urls": ["http://example.com/webhook"],
            "headers": {"X-API-Key": "secret"},
            "timeout_seconds": 15,
            "retry_attempts": 5
        }

        backend = WebhookBackend(config)

        assert backend.urls == ["http://example.com/webhook"]
        assert backend.headers == {"X-API-Key": "secret"}
        assert backend.timeout_seconds == 15
        assert backend.retry_attempts == 5

    @pytest.mark.unit
    def test_urls_string_to_list(self):
        """Test single URL string is converted to list."""
        from src.events.backends.webhook import WebhookBackend

        config = {"urls": "http://example.com/webhook"}

        backend = WebhookBackend(config)

        assert backend.urls == ["http://example.com/webhook"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_without_httpx_package(self):
        """Test initialize handles missing httpx package gracefully."""
        from src.events.backends import webhook

        with patch.object(webhook, 'HTTPX_AVAILABLE', False):
            backend = webhook.WebhookBackend({"urls": ["http://example.com"]})

            result = await backend.initialize()

            assert result is False
            assert backend.enabled is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_without_urls(self):
        """Test initialize fails when no URLs configured."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": []})

        with patch('src.events.backends.webhook.httpx'):
            result = await backend.initialize()

            assert result is False
            assert backend.enabled is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_success(self):
        """Test successful backend initialization."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com/webhook"]})

        with patch('src.events.backends.webhook.httpx') as mock_httpx:
            mock_httpx.AsyncClient.return_value = Mock()
            mock_httpx.Timeout.return_value = Mock()

            result = await backend.initialize()

            assert result is True
            assert backend.http_client is not None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_when_disabled(self):
        """Test publish returns False when backend disabled."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com"], "fail_silently": True})
        backend.enabled = False

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await backend.publish(event)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_success_single_url(self):
        """Test successful publish to single URL."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com/webhook"]})
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 200

        mock_client = Mock()
        mock_client.post = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE, data={"job_id": "test-123"})

        result = await backend.publish(event)

        assert result is True
        mock_client.post.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_uses_cloudevents_headers(self):
        """Test publish uses CloudEvents HTTP headers."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com/webhook"]})
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 200

        mock_client = Mock()
        mock_client.post = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        await backend.publish(event)

        # Check headers include CloudEvents headers
        call_args = mock_client.post.call_args
        headers = call_args.kwargs['headers']

        assert 'ce-specversion' in headers
        assert 'ce-type' in headers
        assert 'ce-source' in headers

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_merges_custom_headers(self):
        """Test publish merges custom headers with CloudEvents headers."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({
            "urls": ["http://example.com/webhook"],
            "headers": {"X-API-Key": "secret123"}
        })
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 200

        mock_client = Mock()
        mock_client.post = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        await backend.publish(event)

        call_args = mock_client.post.call_args
        headers = call_args.kwargs['headers']

        assert headers["X-API-Key"] == "secret123"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_retry_on_failure(self):
        """Test publish retries on failure."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({
            "urls": ["http://example.com/webhook"],
            "retry_attempts": 3
        })
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 500  # Server error

        mock_client = Mock()
        mock_client.post = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        with patch('asyncio.sleep', new=AsyncMock()):
            result = await backend.publish(event)

            assert result is False
            assert mock_client.post.call_count == 3  # All retries exhausted

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_no_retry_on_4xx(self):
        """Test publish doesn't retry on 4xx client errors."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({
            "urls": ["http://example.com/webhook"],
            "retry_attempts": 3
        })
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 400  # Client error
        mock_response.text = "Bad Request"  # Required for logging

        mock_client = Mock()
        mock_client.post = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        # Patch logger to avoid logging errors in test environment
        with patch('src.events.backends.webhook.logger'):
            result = await backend.publish(event)

            assert result is False
            assert mock_client.post.call_count == 1  # No retries

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_publish_success_multiple_urls(self):
        """Test publish to multiple URLs."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({
            "urls": ["http://url1.com/webhook", "http://url2.com/webhook"]
        })
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 200

        mock_client = Mock()
        mock_client.post = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        event = CloudEvent(type=EventTypes.JOB_COMPLETED, source=EVENT_SOURCE)

        result = await backend.publish(event)

        assert result is True
        assert mock_client.post.call_count == 2  # Published to both URLs

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_when_disabled(self):
        """Test health check when backend disabled."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com"]})
        backend.enabled = False

        result = await backend.health_check()

        assert result["backend"] == "webhook"
        assert result["healthy"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_success(self):
        """Test successful health check."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com/webhook"]})
        backend.enabled = True

        mock_response = Mock()
        mock_response.status_code = 200

        mock_client = Mock()
        mock_client.head = AsyncMock(return_value=mock_response)

        backend.http_client = mock_client

        result = await backend.health_check()

        assert result["backend"] == "webhook"
        assert result["healthy"] is True
        assert len(result["urls"]) == 1
        assert result["urls"][0]["reachable"] is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_health_check_url_unreachable(self):
        """Test health check when URL unreachable."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com/webhook"]})
        backend.enabled = True

        mock_client = Mock()
        mock_client.head = AsyncMock(side_effect=Exception("Connection failed"))

        backend.http_client = mock_client

        result = await backend.health_check()

        assert result["backend"] == "webhook"
        assert result["healthy"] is False
        assert result["urls"][0]["reachable"] is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close(self):
        """Test backend cleanup."""
        from src.events.backends.webhook import WebhookBackend

        backend = WebhookBackend({"urls": ["http://example.com"]})

        mock_client = Mock()
        mock_client.aclose = AsyncMock()

        backend.http_client = mock_client

        await backend.close()

        mock_client.aclose.assert_called_once()
        assert backend.http_client is None
