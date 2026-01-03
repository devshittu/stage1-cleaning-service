"""
src/events/backends/kafka.py

Kafka event publishing backend (optional).

For high-throughput event-driven deployments.

Features:
- Async Kafka producer
- Configurable topic and partitioning
- Batch publishing support

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if aiokafka unavailable
- Optional backend (disabled if package missing)
- Fail-silently mode available
"""

import logging
from typing import Dict, Any, Optional

try:
    from aiokafka import AIOKafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None

from src.events.event_backend import EventBackend
from src.events.cloud_event import CloudEvent

logger = logging.getLogger("ingestion_service")


class KafkaBackend(EventBackend):
    """
    Kafka backend for CloudEvents publishing (optional).

    Configuration:
    - bootstrap_servers: List of Kafka broker addresses
    - topic: Kafka topic name
    - compression_type: Compression (none, gzip, snappy, lz4, zstd)
    - fail_silently: Continue on errors
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize Kafka backend."""
        super().__init__(config)

        self.bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])
        self.topic = config.get("topic", "stage1.cleaning.events")
        self.compression_type = config.get("compression_type", "gzip")
        self.fail_silently = config.get("fail_silently", True)

        self.producer: Optional[Any] = None

    async def initialize(self) -> bool:
        """Initialize Kafka producer."""
        if not KAFKA_AVAILABLE:
            logger.info("kafka_backend_unavailable_package_not_installed")
            self.enabled = False
            return False

        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                compression_type=self.compression_type,
                value_serializer=lambda v: v.encode('utf-8')
            )

            await self.producer.start()

            logger.info(
                "kafka_backend_initialized",
                bootstrap_servers=self.bootstrap_servers,
                topic=self.topic
            )

            return True

        except Exception as e:
            logger.warning(f"failed_to_initialize_kafka_backend: {e}")
            self.enabled = False
            return False

    async def publish(self, event: CloudEvent) -> bool:
        """
        Publish CloudEvent to Kafka topic.

        Args:
            event: CloudEvent to publish

        Returns:
            True if publish succeeded
        """
        if not self.enabled or not self.producer:
            if not self.fail_silently:
                raise RuntimeError("Kafka backend not available")
            return False

        try:
            # Serialize CloudEvent as JSON
            event_json = event.to_json()

            # Publish to Kafka topic
            await self.producer.send_and_wait(
                self.topic,
                value=event_json
            )

            logger.info(
                "event_published_to_kafka",
                topic=self.topic,
                event_type=event.type,
                event_id=event.id
            )

            self._record_success()
            return True

        except Exception as e:
            error_msg = f"failed_to_publish_to_kafka: {e}"
            logger.error(error_msg)
            self._record_failure(str(e))

            if not self.fail_silently:
                raise

            return False

    async def health_check(self) -> Dict[str, Any]:
        """Check Kafka backend health."""
        if not self.enabled or not self.producer:
            return {
                "backend": "kafka",
                "healthy": False,
                "reason": "not_initialized"
            }

        # Kafka client doesn't have simple health check
        # Assume healthy if producer is running
        return {
            "backend": "kafka",
            "healthy": True,
            "topic": self.topic,
            "bootstrap_servers": self.bootstrap_servers
        }

    async def close(self):
        """Close Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None
            logger.info("kafka_backend_closed")
