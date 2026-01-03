"""
src/events/cloud_event.py

CloudEvents v1.0 specification wrapper.

Provides a Pydantic model for CloudEvents standard format
used in inter-stage communication.

Spec: https://github.com/cloudevents/spec/blob/v1.0/spec.md

DESIGN PATTERN: Zero-regression approach
- Pure data model, no side effects
- Strict validation via Pydantic
- Clear separation from event publishing logic
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class EventTypes:
    """CloudEvents type constants for Stage 1 cleaning pipeline."""

    # Job lifecycle events
    JOB_STARTED = "com.storytelling.cleaning.job.started"
    JOB_PROGRESS = "com.storytelling.cleaning.job.progress"
    JOB_PAUSED = "com.storytelling.cleaning.job.paused"
    JOB_RESUMED = "com.storytelling.cleaning.job.resumed"
    JOB_COMPLETED = "com.storytelling.cleaning.job.completed"
    JOB_FAILED = "com.storytelling.cleaning.job.failed"
    JOB_CANCELLED = "com.storytelling.cleaning.job.cancelled"


# Event source identifier
EVENT_SOURCE = "stage1-cleaning-pipeline"


class CloudEvent(BaseModel):
    """
    CloudEvents v1.0 specification model.

    Required attributes:
    - specversion: CloudEvents spec version (always "1.0")
    - type: Event type (reverse-DNS format)
    - source: Event source identifier
    - id: Unique event ID

    Optional attributes:
    - time: Event timestamp (ISO 8601)
    - subject: Subject of the event
    - datacontenttype: Content type of data
    - data: Event payload
    """

    specversion: str = Field(default="1.0", description="CloudEvents spec version")
    type: str = Field(..., description="Event type in reverse-DNS format")
    source: str = Field(..., description="Event source identifier")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique event ID")

    time: Optional[str] = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z",
        description="Event timestamp (ISO 8601)"
    )
    subject: Optional[str] = Field(None, description="Subject of the event")
    datacontenttype: str = Field(default="application/json", description="Content type of data")
    data: Optional[Dict[str, Any]] = Field(None, description="Event payload data")

    class Config:
        json_schema_extra = {
            "example": {
                "specversion": "1.0",
                "type": "com.storytelling.cleaning.job.completed",
                "source": "stage1-cleaning-pipeline",
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "time": "2026-01-02T00:30:00Z",
                "subject": "job/abc-123",
                "datacontenttype": "application/json",
                "data": {
                    "job_id": "abc-123",
                    "batch_id": "batch_2026-01-02",
                    "documents_processed": 150,
                    "documents_total": 150,
                    "processing_time_ms": 45000,
                    "output_files": ["/data/output/cleaned_2026-01-02.jsonl"]
                }
            }
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (for serialization)."""
        return self.model_dump(exclude_none=True)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return self.model_dump_json(exclude_none=True)

    def get_http_headers(self) -> Dict[str, str]:
        """
        Get CloudEvents HTTP headers for binary content mode.

        Used by webhook backend to send CloudEvents via HTTP.
        Spec: https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md
        """
        headers = {
            "ce-specversion": self.specversion,
            "ce-type": self.type,
            "ce-source": self.source,
            "ce-id": self.id,
        }

        if self.time:
            headers["ce-time"] = self.time

        if self.subject:
            headers["ce-subject"] = self.subject

        if self.datacontenttype:
            headers["Content-Type"] = self.datacontenttype

        return headers
