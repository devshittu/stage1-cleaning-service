"""
src/events/backends/redis_streams.py

Redis Streams event publishing backend.

Primary backend for low-latency inter-stage event distribution.

Features:
- XADD for stream publishing
- Configurable max length
- TTL support via EXPIRE
- Consumer group ready

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if Redis unavailable
- All operations wrapped in try-catch
- Fail-silently mode available
"""

import logging
import os
from typing import Dict, Any, Optional

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    aioredis = None

from src.events.event_backend import EventBackend
from src.events.cloud_event import CloudEvent

logger = logging.getLogger("ingestion_service")


class RedisStreamsBackend(EventBackend):
    """
    Redis Streams backend for CloudEvents publishing.

    Configuration:
    - url: Redis connection URL
    - stream_name: Stream name (e.g., "stage1:cleaning:events")
    - max_len: Maximum stream length (trim old events)
    - ttl_seconds: Stream TTL in seconds
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize Redis Streams backend."""
        super().__init__(config)

        self.redis_client: Optional[Any] = None
        self.stream_name = config.get("stream_name", "stage1:cleaning:events")
        self.max_len = config.get("max_len", 10000)
        self.ttl_seconds = config.get("ttl_seconds", 86400)  # 24 hours

        # Get Redis URL from config or environment
        self.redis_url = config.get("url") or os.getenv(
            "REDIS_CACHE_URL",
            "redis://redis-cache:6379/1"
        )

        self.fail_silently = config.get("fail_silently", True)

    async def initialize(self) -> bool:
        """Initialize Redis client."""
        if not REDIS_AVAILABLE:
            logger.warning("redis_streams_backend_unavailable_package_not_installed")
            self.enabled = False
            return False

        try:
            self.redis_client = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )

            # Test connection
            await self.redis_client.ping()

            logger.info(
                "redis_streams_backend_initialized",
                stream_name=self.stream_name,
                max_len=self.max_len
            )

            return True

        except Exception as e:
            logger.error(f"failed_to_initialize_redis_streams_backend: {e}")
            self.enabled = False
            return False

    async def publish(self, event: CloudEvent) -> bool:
        """
        Publish CloudEvent to Redis Stream.

        Args:
            event: CloudEvent to publish

        Returns:
            True if publish succeeded
        """
        if not self.enabled or not self.redis_client:
            if not self.fail_silently:
                raise RuntimeError("Redis Streams backend not available")
            return False

        try:
            # Convert CloudEvent to Redis Stream fields
            event_data = event.to_dict()

            # Flatten nested 'data' field for Redis
            fields = {
                "specversion": event_data["specversion"],
                "type": event_data["type"],
                "source": event_data["source"],
                "id": event_data["id"],
                "time": event_data.get("time", ""),
                "subject": event_data.get("subject", ""),
                "datacontenttype": event_data.get("datacontenttype", "application/json"),
            }

            # Add data payload as JSON string
            if "data" in event_data and event_data["data"]:
                import json
                fields["data"] = json.dumps(event_data["data"])

            # Publish to stream with max length limit
            message_id = await self.redis_client.xadd(
                self.stream_name,
                fields,
                maxlen=self.max_len,
                approximate=True  # Faster, allows slight over-limit
            )

            # Set TTL on stream (only if not already set)
            ttl = await self.redis_client.ttl(self.stream_name)
            if ttl == -1:  # No TTL set
                await self.redis_client.expire(self.stream_name, self.ttl_seconds)

            logger.info(
                "event_published_to_redis_streams",
                stream_name=self.stream_name,
                message_id=message_id,
                event_type=event.type,
                event_id=event.id
            )

            self._record_success()
            return True

        except Exception as e:
            error_msg = f"failed_to_publish_to_redis_streams: {e}"
            logger.error(error_msg)
            self._record_failure(str(e))

            if not self.fail_silently:
                raise

            return False

    async def health_check(self) -> Dict[str, Any]:
        """Check Redis Streams backend health."""
        if not self.enabled or not self.redis_client:
            return {
                "backend": "redis_streams",
                "healthy": False,
                "reason": "not_initialized"
            }

        try:
            await self.redis_client.ping()

            # Get stream length
            stream_len = await self.redis_client.xlen(self.stream_name)

            return {
                "backend": "redis_streams",
                "healthy": True,
                "stream_name": self.stream_name,
                "stream_length": stream_len,
                "max_len": self.max_len
            }

        except Exception as e:
            return {
                "backend": "redis_streams",
                "healthy": False,
                "reason": str(e)
            }

    async def close(self):
        """Close Redis client."""
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
            logger.info("redis_streams_backend_closed")
