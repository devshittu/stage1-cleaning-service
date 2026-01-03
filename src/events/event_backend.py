"""
src/events/event_backend.py

Abstract base class for event publishing backends.

Provides interface for pluggable event backends:
- Redis Streams
- Webhooks
- Kafka
- NATS
- RabbitMQ

DESIGN PATTERN: Strategy pattern
- Each backend implements publish() method
- EventPublisher orchestrates multi-backend publishing
- Graceful degradation for unavailable backends
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging

from src.events.cloud_event import CloudEvent

logger = logging.getLogger("ingestion_service")


class EventBackend(ABC):
    """
    Abstract base class for event publishing backends.

    All backends must implement:
    - initialize(): Setup connections/clients
    - publish(): Publish a CloudEvent
    - health_check(): Verify backend availability
    - close(): Cleanup resources
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize backend with configuration.

        Args:
            config: Backend-specific configuration dictionary
        """
        self.config = config
        self.enabled = config.get("enabled", True)
        self.backend_type = self.__class__.__name__.replace("Backend", "").lower()
        self._metrics = {
            "total_published": 0,
            "total_failed": 0,
            "last_publish_time": None,
            "last_error": None
        }

    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize backend (create clients, establish connections).

        Returns:
            True if initialization succeeded
        """
        pass

    @abstractmethod
    async def publish(self, event: CloudEvent) -> bool:
        """
        Publish a CloudEvent to this backend.

        Args:
            event: CloudEvent to publish

        Returns:
            True if publish succeeded
        """
        pass

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Check backend health.

        Returns:
            Dictionary with health status
        """
        pass

    @abstractmethod
    async def close(self):
        """Close connections and cleanup resources."""
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get backend metrics.

        Returns:
            Dictionary with metrics
        """
        return {
            "backend": self.backend_type,
            "enabled": self.enabled,
            **self._metrics
        }

    def _record_success(self):
        """Record successful publish."""
        from datetime import datetime
        self._metrics["total_published"] += 1
        self._metrics["last_publish_time"] = datetime.utcnow().isoformat()

    def _record_failure(self, error: str):
        """Record failed publish."""
        self._metrics["total_failed"] += 1
        self._metrics["last_error"] = error

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(enabled={self.enabled})"
