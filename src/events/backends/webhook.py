"""
src/events/backends/webhook.py

Webhook (HTTP POST) event publishing backend.

Secondary backend for direct HTTP notifications to downstream services.

Features:
- CloudEvents binary content mode (HTTP headers)
- Configurable URLs and headers
- Retry with exponential backoff
- Timeout handling

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if httpx unavailable
- All operations wrapped in try-catch
- Fail-silently mode available
"""

import logging
from typing import Dict, Any, Optional, List

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None

from src.events.event_backend import EventBackend
from src.events.cloud_event import CloudEvent

logger = logging.getLogger("ingestion_service")


class WebhookBackend(EventBackend):
    """
    Webhook (HTTP) backend for CloudEvents publishing.

    Configuration:
    - urls: List of webhook URLs to POST to
    - headers: Additional HTTP headers (e.g., API keys)
    - timeout_seconds: Request timeout
    - retry_attempts: Number of retry attempts
    - fail_silently: Continue on errors
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize Webhook backend."""
        super().__init__(config)

        self.urls = config.get("urls", [])
        if isinstance(self.urls, str):
            self.urls = [self.urls]

        self.headers = config.get("headers", {})
        self.timeout_seconds = config.get("timeout_seconds", 30)
        self.retry_attempts = config.get("retry_attempts", 3)
        self.fail_silently = config.get("fail_silently", True)

        self.http_client: Optional[Any] = None

    async def initialize(self) -> bool:
        """Initialize HTTP client."""
        if not HTTPX_AVAILABLE:
            logger.warning("webhook_backend_unavailable_package_not_installed")
            self.enabled = False
            return False

        if not self.urls:
            logger.warning("webhook_backend_disabled_no_urls_configured")
            self.enabled = False
            return False

        try:
            self.http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds),
                follow_redirects=True
            )

            logger.info(
                "webhook_backend_initialized",
                urls=self.urls,
                timeout_seconds=self.timeout_seconds
            )

            return True

        except Exception as e:
            logger.error(f"failed_to_initialize_webhook_backend: {e}")
            self.enabled = False
            return False

    async def publish(self, event: CloudEvent) -> bool:
        """
        Publish CloudEvent via webhook.

        Uses CloudEvents binary content mode:
        - Event attributes sent as HTTP headers (ce-*)
        - Event data sent as request body

        Args:
            event: CloudEvent to publish

        Returns:
            True if publish succeeded to at least one URL
        """
        if not self.enabled or not self.http_client:
            if not self.fail_silently:
                raise RuntimeError("Webhook backend not available")
            return False

        # Build CloudEvents HTTP headers
        ce_headers = event.get_http_headers()

        # Merge with custom headers
        headers = {**ce_headers, **self.headers}

        # Event data as JSON body
        import json
        body = json.dumps(event.data) if event.data else "{}"

        # Track success for at least one URL
        any_success = False

        for url in self.urls:
            success = await self._publish_to_url(url, headers, body, event)
            if success:
                any_success = True

        if any_success:
            self._record_success()
        else:
            self._record_failure("all_webhooks_failed")

        return any_success

    async def _publish_to_url(
        self,
        url: str,
        headers: Dict[str, str],
        body: str,
        event: CloudEvent
    ) -> bool:
        """
        Publish to a single webhook URL with retries.

        Args:
            url: Webhook URL
            headers: HTTP headers
            body: Request body
            event: CloudEvent (for logging)

        Returns:
            True if publish succeeded
        """
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = await self.http_client.post(
                    url,
                    headers=headers,
                    content=body
                )

                # Consider 2xx successful
                if 200 <= response.status_code < 300:
                    logger.info(
                        "event_published_to_webhook",
                        url=url,
                        status_code=response.status_code,
                        event_type=event.type,
                        event_id=event.id,
                        attempt=attempt
                    )
                    return True

                # Log non-2xx responses
                logger.warning(
                    "webhook_publish_failed_non_2xx",
                    url=url,
                    status_code=response.status_code,
                    response_text=response.text[:200],  # First 200 chars
                    attempt=attempt
                )

                # Don't retry on 4xx client errors (except 429 rate limit)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    break

            except httpx.TimeoutException:
                logger.warning(
                    "webhook_publish_timeout",
                    url=url,
                    timeout_seconds=self.timeout_seconds,
                    attempt=attempt
                )

            except Exception as e:
                logger.error(
                    f"webhook_publish_error: {e}",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "event_type": event.type
                    }
                )

            # Exponential backoff before retry (if not last attempt)
            if attempt < self.retry_attempts:
                import asyncio
                backoff = 2 ** attempt  # 2, 4, 8 seconds
                await asyncio.sleep(backoff)

        # All attempts failed
        return False

    async def health_check(self) -> Dict[str, Any]:
        """Check Webhook backend health."""
        if not self.enabled or not self.http_client:
            return {
                "backend": "webhook",
                "healthy": False,
                "reason": "not_initialized"
            }

        # Test connectivity to each URL (HEAD request)
        url_health = []

        for url in self.urls:
            try:
                response = await self.http_client.head(url, timeout=5.0)
                url_health.append({
                    "url": url,
                    "reachable": True,
                    "status_code": response.status_code
                })
            except Exception as e:
                url_health.append({
                    "url": url,
                    "reachable": False,
                    "error": str(e)
                })

        all_healthy = all(u["reachable"] for u in url_health)

        return {
            "backend": "webhook",
            "healthy": all_healthy,
            "urls": url_health,
            "retry_attempts": self.retry_attempts,
            "timeout_seconds": self.timeout_seconds
        }

    async def close(self):
        """Close HTTP client."""
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None
            logger.info("webhook_backend_closed")
