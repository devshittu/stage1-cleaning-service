"""
src/events/backends/__init__.py

Event backend implementations.

Available backends:
- redis_streams: Redis Streams (primary, low-latency)
- webhook: HTTP webhooks (secondary, direct notifications)
- kafka: Kafka (optional, high-throughput)
- nats: NATS (optional, cloud-native)
- rabbitmq: RabbitMQ (optional, flexible routing)
"""

from src.events.backends.redis_streams import RedisStreamsBackend
from src.events.backends.webhook import WebhookBackend
from src.events.backends.kafka import KafkaBackend
from src.events.backends.nats import NATSBackend
from src.events.backends.rabbitmq import RabbitMQBackend

__all__ = [
    "RedisStreamsBackend",
    "WebhookBackend",
    "KafkaBackend",
    "NATSBackend",
    "RabbitMQBackend",
]
