"""
src/events/backends/rabbitmq.py

RabbitMQ event publishing backend (optional).

For flexible message routing and queuing.

Features:
- AMQP protocol support
- Exchange and routing key configuration
- Persistent messaging

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if aio-pika unavailable
- Optional backend (disabled if package missing)
- Fail-silently mode available
"""

import logging
from typing import Dict, Any, Optional

try:
    import aio_pika
    from aio_pika import DeliveryMode
    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False
    aio_pika = None
    DeliveryMode = None

from src.events.event_backend import EventBackend
from src.events.cloud_event import CloudEvent

logger = logging.getLogger("ingestion_service")


class RabbitMQBackend(EventBackend):
    """
    RabbitMQ backend for CloudEvents publishing (optional).

    Configuration:
    - url: RabbitMQ connection URL (amqp://user:pass@host:port/)
    - exchange: Exchange name
    - routing_key: Routing key for messages
    - exchange_type: Exchange type (direct, topic, fanout, headers)
    - fail_silently: Continue on errors
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize RabbitMQ backend."""
        super().__init__(config)

        self.url = config.get("url", "amqp://guest:guest@localhost:5672/")
        self.exchange_name = config.get("exchange", "stage1.cleaning")
        self.routing_key = config.get("routing_key", "events")
        self.exchange_type = config.get("exchange_type", "topic")
        self.fail_silently = config.get("fail_silently", True)

        self.connection: Optional[Any] = None
        self.channel: Optional[Any] = None
        self.exchange: Optional[Any] = None

    async def initialize(self) -> bool:
        """Initialize RabbitMQ connection."""
        if not RABBITMQ_AVAILABLE:
            logger.info("rabbitmq_backend_unavailable_package_not_installed")
            self.enabled = False
            return False

        try:
            self.connection = await aio_pika.connect_robust(self.url)
            self.channel = await self.connection.channel()

            # Declare exchange
            self.exchange = await self.channel.declare_exchange(
                self.exchange_name,
                type=getattr(aio_pika.ExchangeType, self.exchange_type.upper()),
                durable=True
            )

            logger.info(
                "rabbitmq_backend_initialized",
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                exchange_type=self.exchange_type
            )

            return True

        except Exception as e:
            logger.warning(f"failed_to_initialize_rabbitmq_backend: {e}")
            self.enabled = False
            return False

    async def publish(self, event: CloudEvent) -> bool:
        """
        Publish CloudEvent to RabbitMQ exchange.

        Args:
            event: CloudEvent to publish

        Returns:
            True if publish succeeded
        """
        if not self.enabled or not self.exchange:
            if not self.fail_silently:
                raise RuntimeError("RabbitMQ backend not available")
            return False

        try:
            # Serialize CloudEvent as JSON
            event_json = event.to_json()

            # Create message
            message = aio_pika.Message(
                body=event_json.encode('utf-8'),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                headers={
                    "ce-specversion": event.specversion,
                    "ce-type": event.type,
                    "ce-source": event.source,
                    "ce-id": event.id,
                }
            )

            # Publish to exchange
            await self.exchange.publish(
                message,
                routing_key=self.routing_key
            )

            logger.info(
                "event_published_to_rabbitmq",
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                event_type=event.type,
                event_id=event.id
            )

            self._record_success()
            return True

        except Exception as e:
            error_msg = f"failed_to_publish_to_rabbitmq: {e}"
            logger.error(error_msg)
            self._record_failure(str(e))

            if not self.fail_silently:
                raise

            return False

    async def health_check(self) -> Dict[str, Any]:
        """Check RabbitMQ backend health."""
        if not self.enabled or not self.connection:
            return {
                "backend": "rabbitmq",
                "healthy": False,
                "reason": "not_initialized"
            }

        try:
            is_closed = self.connection.is_closed

            return {
                "backend": "rabbitmq",
                "healthy": not is_closed,
                "exchange": self.exchange_name,
                "routing_key": self.routing_key,
                "exchange_type": self.exchange_type
            }

        except Exception as e:
            return {
                "backend": "rabbitmq",
                "healthy": False,
                "reason": str(e)
            }

    async def close(self):
        """Close RabbitMQ connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            self.exchange = None
            logger.info("rabbitmq_backend_closed")
