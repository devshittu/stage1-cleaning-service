"""
src/events/backends/nats.py

NATS event publishing backend (optional).

For cloud-native event messaging.

Features:
- NATS JetStream support
- Subject-based routing
- Persistent streams

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if nats-py unavailable
- Optional backend (disabled if package missing)
- Fail-silently mode available
"""

import logging
from typing import Dict, Any, Optional

try:
    from nats.aio.client import Client as NATS
    from nats.js import JetStreamContext
    NATS_AVAILABLE = True
except ImportError:
    NATS_AVAILABLE = False
    NATS = None
    JetStreamContext = None

from src.events.event_backend import EventBackend
from src.events.cloud_event import CloudEvent

logger = logging.getLogger("ingestion_service")


class NATSBackend(EventBackend):
    """
    NATS backend for CloudEvents publishing (optional).

    Configuration:
    - servers: List of NATS server URLs
    - subject: NATS subject for publishing
    - use_jetstream: Use JetStream for persistence
    - fail_silently: Continue on errors
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize NATS backend."""
        super().__init__(config)

        self.servers = config.get("servers", ["nats://localhost:4222"])
        self.subject = config.get("subject", "stage1.cleaning.events")
        self.use_jetstream = config.get("use_jetstream", True)
        self.fail_silently = config.get("fail_silently", True)

        self.nc: Optional[Any] = None
        self.js: Optional[Any] = None

    async def initialize(self) -> bool:
        """Initialize NATS client."""
        if not NATS_AVAILABLE:
            logger.info("nats_backend_unavailable_package_not_installed")
            self.enabled = False
            return False

        try:
            self.nc = NATS()
            await self.nc.connect(servers=self.servers)

            if self.use_jetstream:
                self.js = self.nc.jetstream()

            logger.info(
                "nats_backend_initialized",
                servers=self.servers,
                subject=self.subject,
                use_jetstream=self.use_jetstream
            )

            return True

        except Exception as e:
            logger.warning(f"failed_to_initialize_nats_backend: {e}")
            self.enabled = False
            return False

    async def publish(self, event: CloudEvent) -> bool:
        """
        Publish CloudEvent to NATS subject.

        Args:
            event: CloudEvent to publish

        Returns:
            True if publish succeeded
        """
        if not self.enabled or not self.nc:
            if not self.fail_silently:
                raise RuntimeError("NATS backend not available")
            return False

        try:
            # Serialize CloudEvent as JSON
            event_json = event.to_json()
            event_bytes = event_json.encode('utf-8')

            # Publish via JetStream or core NATS
            if self.use_jetstream and self.js:
                ack = await self.js.publish(self.subject, event_bytes)
                logger.info(
                    "event_published_to_nats_jetstream",
                    subject=self.subject,
                    event_type=event.type,
                    event_id=event.id,
                    stream=ack.stream,
                    seq=ack.seq
                )
            else:
                await self.nc.publish(self.subject, event_bytes)
                logger.info(
                    "event_published_to_nats",
                    subject=self.subject,
                    event_type=event.type,
                    event_id=event.id
                )

            self._record_success()
            return True

        except Exception as e:
            error_msg = f"failed_to_publish_to_nats: {e}"
            logger.error(error_msg)
            self._record_failure(str(e))

            if not self.fail_silently:
                raise

            return False

    async def health_check(self) -> Dict[str, Any]:
        """Check NATS backend health."""
        if not self.enabled or not self.nc:
            return {
                "backend": "nats",
                "healthy": False,
                "reason": "not_initialized"
            }

        try:
            is_connected = self.nc.is_connected

            return {
                "backend": "nats",
                "healthy": is_connected,
                "subject": self.subject,
                "servers": self.servers,
                "use_jetstream": self.use_jetstream
            }

        except Exception as e:
            return {
                "backend": "nats",
                "healthy": False,
                "reason": str(e)
            }

    async def close(self):
        """Close NATS client."""
        if self.nc:
            await self.nc.close()
            self.nc = None
            self.js = None
            logger.info("nats_backend_closed")
