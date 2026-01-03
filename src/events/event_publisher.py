"""
src/events/event_publisher.py

Multi-backend event publisher orchestrator.

Responsibilities:
- Load backend configuration from settings
- Initialize multiple backends
- Publish events to all enabled backends
- Track metrics per backend
- Provide health checks

DESIGN PATTERN: Strategy pattern with multi-backend support
- Default backends: redis_streams + webhook
- Optional backends: kafka, nats, rabbitmq
- Graceful degradation for unavailable backends
- Fail-silently mode (errors don't break job processing)
"""

import logging
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict

from src.events.cloud_event import CloudEvent, EventTypes
from src.events.event_backend import EventBackend
from src.events.backends import (
    RedisStreamsBackend,
    WebhookBackend,
    KafkaBackend,
    NATSBackend,
    RabbitMQBackend
)

logger = logging.getLogger("ingestion_service")


class EventPublisher:
    """
    Multi-backend event publisher.

    Features:
    - Publish to multiple backends simultaneously
    - Event filtering (publish only specific event types)
    - Per-backend metrics tracking
    - Health monitoring
    - Fail-silently mode
    """

    # Backend class mapping
    BACKEND_CLASSES = {
        "redis_streams": RedisStreamsBackend,
        "webhook": WebhookBackend,
        "kafka": KafkaBackend,
        "nats": NATSBackend,
        "rabbitmq": RabbitMQBackend,
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize event publisher.

        Args:
            config: Event configuration dictionary (from settings.yaml)
                    If None, loads from ConfigManager
        """
        self.config = config
        self.backends: List[EventBackend] = []
        self.enabled = False
        self.publish_events: Optional[Set[str]] = None

        # Metrics
        self._metrics = {
            "total_events": 0,
            "successful": 0,
            "failed": 0,
            "backend_success": defaultdict(int),
            "backend_failures": defaultdict(int),
        }

        # Load configuration if not provided
        if self.config is None:
            try:
                from src.utils.config_manager import ConfigManager
                settings = ConfigManager.get_settings()
                self.config = settings.get("events", {})
            except Exception as e:
                logger.warning(f"failed_to_load_event_config: {e}")
                self.config = {}

        self.enabled = self.config.get("enabled", False)

        if not self.enabled:
            logger.info("event_publisher_disabled_in_config")

        # Parse event filter (if specified)
        publish_events_config = self.config.get("publish_events")
        if publish_events_config:
            self.publish_events = set(publish_events_config)

    async def initialize(self) -> bool:
        """
        Initialize all configured backends.

        Returns:
            True if at least one backend initialized successfully
        """
        if not self.enabled:
            return False

        # Get backend configurations
        backend_configs = self.config.get("backends", [])

        # Default to redis_streams + webhook if no backends specified
        if not backend_configs:
            logger.info("no_backends_configured_using_defaults")
            backend_configs = [
                {
                    "type": "redis_streams",
                    "enabled": True,
                    "config": {
                        "stream_name": "stage1:cleaning:events",
                        "max_len": 10000,
                        "ttl_seconds": 86400,
                    }
                },
                {
                    "type": "webhook",
                    "enabled": False,  # Disabled by default (requires URL)
                    "config": {
                        "urls": [],
                        "timeout_seconds": 30,
                        "retry_attempts": 3,
                    }
                }
            ]

        # Initialize each backend
        for backend_config in backend_configs:
            backend_type = backend_config.get("type")
            backend_enabled = backend_config.get("enabled", True)
            backend_settings = backend_config.get("config", {})

            if not backend_enabled:
                logger.info(f"{backend_type}_backend_disabled_in_config")
                continue

            if backend_type not in self.BACKEND_CLASSES:
                logger.warning(f"unknown_backend_type: {backend_type}")
                continue

            # Create backend instance
            backend_class = self.BACKEND_CLASSES[backend_type]
            backend = backend_class(backend_settings)

            # Initialize backend
            success = await backend.initialize()

            if success:
                self.backends.append(backend)
                logger.info(f"{backend_type}_backend_initialized")
            else:
                logger.warning(f"{backend_type}_backend_initialization_failed")

        if not self.backends:
            logger.warning("no_backends_initialized_event_publishing_disabled")
            self.enabled = False
            return False

        backends_list = [b.backend_type for b in self.backends]
        event_filter = list(self.publish_events) if self.publish_events else "all"
        logger.info(
            f"event_publisher_initialized: backends={backends_list}, event_filter={event_filter}"
        )

        return True

    def should_publish_event(self, event_type: str) -> bool:
        """
        Check if event type should be published (event filtering).

        Args:
            event_type: CloudEvent type

        Returns:
            True if event should be published
        """
        if not self.publish_events:
            return True  # No filter, publish all events

        return event_type in self.publish_events

    async def publish(self, event: CloudEvent) -> Dict[str, Any]:
        """
        Publish CloudEvent to all enabled backends.

        Args:
            event: CloudEvent to publish

        Returns:
            Dictionary with publish results per backend
        """
        if not self.enabled or not self.backends:
            return {
                "published": False,
                "reason": "event_publisher_not_enabled",
                "backends": []
            }

        # Check event filter
        if not self.should_publish_event(event.type):
            logger.debug(
                f"event_filtered_not_published: event_type={event.type}, event_id={event.id}"
            )
            return {
                "published": False,
                "reason": "event_filtered",
                "event_type": event.type,
                "backends": []
            }

        # Publish to all backends
        results = {}
        any_success = False

        for backend in self.backends:
            try:
                success = await backend.publish(event)
                results[backend.backend_type] = {
                    "success": success,
                    "error": None
                }

                if success:
                    any_success = True
                    self._metrics["backend_success"][backend.backend_type] += 1
                else:
                    self._metrics["backend_failures"][backend.backend_type] += 1

            except Exception as e:
                logger.error(
                    f"backend_publish_exception: {backend.backend_type}: {e}",
                    exc_info=True
                )
                results[backend.backend_type] = {
                    "success": False,
                    "error": str(e)
                }
                self._metrics["backend_failures"][backend.backend_type] += 1

        # Update global metrics
        self._metrics["total_events"] += 1
        if any_success:
            self._metrics["successful"] += 1
        else:
            self._metrics["failed"] += 1

        backends_list = list(results.keys())
        logger.info(
            f"event_published: event_type={event.type}, event_id={event.id}, "
            f"backends={backends_list}, any_success={any_success}"
        )

        return {
            "published": any_success,
            "event_type": event.type,
            "event_id": event.id,
            "backends": results
        }

    async def health_check(self) -> Dict[str, Any]:
        """
        Check health of all backends.

        Returns:
            Dictionary with health status per backend
        """
        if not self.enabled:
            return {
                "enabled": False,
                "backends": []
            }

        backend_health = []

        for backend in self.backends:
            try:
                health = await backend.health_check()
                backend_health.append(health)
            except Exception as e:
                backend_health.append({
                    "backend": backend.backend_type,
                    "healthy": False,
                    "error": str(e)
                })

        all_healthy = all(b.get("healthy", False) for b in backend_health)

        return {
            "enabled": True,
            "healthy": all_healthy,
            "backends": backend_health,
            "metrics": self.get_metrics()
        }

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get event publisher metrics.

        Returns:
            Dictionary with metrics
        """
        return {
            "total_events": self._metrics["total_events"],
            "successful": self._metrics["successful"],
            "failed": self._metrics["failed"],
            "backend_success": dict(self._metrics["backend_success"]),
            "backend_failures": dict(self._metrics["backend_failures"]),
        }

    async def close(self):
        """Close all backends."""
        for backend in self.backends:
            try:
                await backend.close()
            except Exception as e:
                logger.error(f"failed_to_close_backend_{backend.backend_type}: {e}")

        self.backends = []
        logger.info("event_publisher_closed")


# Singleton instance
_event_publisher_instance: Optional[EventPublisher] = None


def get_event_publisher() -> EventPublisher:
    """Get singleton instance of event publisher."""
    global _event_publisher_instance

    if _event_publisher_instance is None:
        _event_publisher_instance = EventPublisher()

    return _event_publisher_instance
