"""
src/events/__init__.py

CloudEvents v1.0 event publishing module.

Provides multi-backend event publishing for inter-stage communication.

Usage:
    from src.events import EventPublisher, CloudEvent, EventTypes

    # Initialize publisher
    publisher = EventPublisher()
    await publisher.initialize()

    # Create and publish event
    event = CloudEvent(
        type=EventTypes.JOB_COMPLETED,
        source="stage1-cleaning-pipeline",
        subject=f"job/{job_id}",
        data={
            "job_id": job_id,
            "documents_processed": 150,
            "processing_time_ms": 45000
        }
    )

    result = await publisher.publish(event)

Backends:
    - redis_streams: Redis Streams (default, low-latency)
    - webhook: HTTP webhooks (default, direct notifications)
    - kafka: Kafka (optional, high-throughput)
    - nats: NATS (optional, cloud-native)
    - rabbitmq: RabbitMQ (optional, flexible routing)
"""

from src.events.cloud_event import CloudEvent, EventTypes, EVENT_SOURCE
from src.events.event_publisher import EventPublisher, get_event_publisher
from src.events.event_backend import EventBackend

__all__ = [
    "CloudEvent",
    "EventTypes",
    "EVENT_SOURCE",
    "EventPublisher",
    "get_event_publisher",
    "EventBackend",
]
