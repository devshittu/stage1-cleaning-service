"""
src/schemas/job_models.py

Defines job lifecycle models for batch processing management.

Provides:
- JobStatus enum for state machine tracking
- Job state models for PostgreSQL persistence
- Checkpoint models for Redis persistence
- Job lifecycle request/response schemas

DESIGN PATTERN: Zero-regression approach
- All models are new additions
- Existing data_models.py remains unchanged
- Old API endpoints continue to work
"""

from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """
    Job lifecycle states.

    State transitions:
    - QUEUED → RUNNING → COMPLETED
    - QUEUED → RUNNING → PAUSED → QUEUED (resume)
    - QUEUED → CANCELLED
    - RUNNING → PAUSED
    - RUNNING → CANCELLED
    - RUNNING → FAILED
    - PAUSED → CANCELLED
    """
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class JobCreate(BaseModel):
    """Request model for creating a new batch job."""
    batch_id: Optional[str] = Field(None, description="Optional batch identifier for correlation")
    total_documents: int = Field(..., gt=0, description="Total number of documents in batch")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional job metadata")


class JobCheckpoint(BaseModel):
    """Checkpoint data structure stored in Redis."""
    job_id: str
    processed_count: int = 0
    total_count: int = 0
    last_processed_doc_id: Optional[str] = None
    progress_percent: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    statistics: Dict[str, Any] = Field(default_factory=dict)


class JobState(BaseModel):
    """Complete job state model (PostgreSQL persistence)."""
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    batch_id: Optional[str] = None
    status: JobStatus = JobStatus.QUEUED
    celery_task_id: Optional[str] = None

    # Progress tracking
    total_documents: int = 0
    processed_documents: int = 0
    failed_documents: int = 0
    progress_percent: float = 0.0

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    paused_at: Optional[datetime] = None
    resumed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Metadata and results
    metadata: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    statistics: Optional[Dict[str, Any]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
            UUID: lambda v: str(v) if v else None
        }


class JobStatusResponse(BaseModel):
    """Response model for job status queries."""
    job_id: str
    batch_id: Optional[str] = None
    status: JobStatus
    progress_percent: float = 0.0
    total_documents: int = 0
    processed_documents: int = 0
    failed_documents: int = 0
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    statistics: Optional[Dict[str, Any]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class JobListResponse(BaseModel):
    """Response model for listing jobs."""
    jobs: List[JobStatusResponse]
    total_count: int
    page: int = 1
    page_size: int = 50


class BatchSubmitRequest(BaseModel):
    """Request model for submitting a batch job (new API)."""
    batch_id: Optional[str] = Field(None, description="Optional batch identifier")
    documents: List[Dict[str, Any]] = Field(..., min_length=1, max_length=10000,
                                            description="List of documents to process")
    checkpoint_interval: Optional[int] = Field(None, ge=1, le=1000,
                                              description="Save checkpoint every N documents")
    persist_to_backends: Optional[List[str]] = Field(None,
                                                     description="Storage backends to use")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Job metadata")


class BatchSubmitResponse(BaseModel):
    """Response model for batch job submission."""
    status: str = "accepted"
    job_id: str
    batch_id: Optional[str] = None
    total_documents: int
    message: str
    estimated_duration_minutes: Optional[int] = None


class JobPauseResponse(BaseModel):
    """Response model for pause request."""
    status: str
    job_id: str
    message: str
    checkpoint_saved: bool = False


class JobResumeResponse(BaseModel):
    """Response model for resume request."""
    status: str
    job_id: str
    message: str
    resume_from_checkpoint: bool = False
    remaining_documents: Optional[int] = None


class JobCancelResponse(BaseModel):
    """Response model for cancel request."""
    status: str
    job_id: str
    message: str
    documents_processed: int = 0


class ResourceUsage(BaseModel):
    """Resource usage metrics."""
    cpu_percent: float
    memory_percent: float
    memory_used_gb: float
    memory_total_gb: float
    gpu_available: bool = False
    gpu_memory_used_mb: Optional[float] = None
    gpu_memory_total_mb: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
