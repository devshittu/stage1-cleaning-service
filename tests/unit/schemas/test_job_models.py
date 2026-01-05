"""
tests/unit/schemas/test_job_models.py

Unit tests for job lifecycle Pydantic models.

Tests cover:
- JobStatus enum and state transitions
- Job creation and validation
- Checkpoint models
- Job state management
- Request/response models
- Resource usage metrics
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.job_models import (
    JobStatus,
    JobCreate,
    JobCheckpoint,
    JobState,
    JobStatusResponse,
    JobListResponse,
    BatchSubmitRequest,
    BatchSubmitResponse,
    JobPauseResponse,
    JobResumeResponse,
    JobCancelResponse,
    ResourceUsage
)


class TestJobStatus:
    """Test JobStatus enum."""

    @pytest.mark.unit
    def test_all_statuses_defined(self):
        """Test all job statuses are defined."""
        expected_statuses = ["QUEUED", "RUNNING", "PAUSED", "COMPLETED", "CANCELLED", "FAILED"]

        for status in expected_statuses:
            assert hasattr(JobStatus, status)

    @pytest.mark.unit
    def test_status_values(self):
        """Test status enum values match expected lowercase strings."""
        assert JobStatus.QUEUED.value == "queued"
        assert JobStatus.RUNNING.value == "running"
        assert JobStatus.PAUSED.value == "paused"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.CANCELLED.value == "cancelled"
        assert JobStatus.FAILED.value == "failed"

    @pytest.mark.unit
    def test_status_comparison(self):
        """Test status enum comparison."""
        assert JobStatus.QUEUED == JobStatus.QUEUED
        assert JobStatus.QUEUED != JobStatus.RUNNING
        assert JobStatus.QUEUED.value == "queued"


class TestJobCreate:
    """Test JobCreate request model."""

    @pytest.mark.unit
    def test_minimal_job_create(self):
        """Test minimal job creation with required fields only."""
        job = JobCreate(total_documents=100)

        assert job.total_documents == 100
        assert job.batch_id is None
        assert job.metadata is None

    @pytest.mark.unit
    def test_full_job_create(self):
        """Test job creation with all optional fields."""
        job = JobCreate(
            batch_id="batch-123",
            total_documents=500,
            metadata={"source": "test", "priority": "high"}
        )

        assert job.batch_id == "batch-123"
        assert job.total_documents == 500
        assert job.metadata["source"] == "test"

    @pytest.mark.unit
    def test_zero_documents_raises_error(self):
        """Test zero documents raises validation error."""
        with pytest.raises(ValidationError):
            JobCreate(total_documents=0)

    @pytest.mark.unit
    def test_negative_documents_raises_error(self):
        """Test negative documents raises validation error."""
        with pytest.raises(ValidationError):
            JobCreate(total_documents=-10)

    @pytest.mark.unit
    def test_missing_total_documents_raises_error(self):
        """Test missing total_documents raises validation error."""
        with pytest.raises(ValidationError):
            JobCreate(batch_id="test")


class TestJobCheckpoint:
    """Test JobCheckpoint model."""

    @pytest.mark.unit
    def test_minimal_checkpoint(self):
        """Test minimal checkpoint creation."""
        checkpoint = JobCheckpoint(job_id="job-123")

        assert checkpoint.job_id == "job-123"
        assert checkpoint.processed_count == 0
        assert checkpoint.total_count == 0
        assert checkpoint.last_processed_doc_id is None
        assert checkpoint.progress_percent == 0.0
        assert checkpoint.statistics == {}

    @pytest.mark.unit
    def test_checkpoint_with_progress(self):
        """Test checkpoint with progress data."""
        checkpoint = JobCheckpoint(
            job_id="job-123",
            processed_count=250,
            total_count=1000,
            last_processed_doc_id="doc-250",
            progress_percent=25.0,
            statistics={"errors": 5, "warnings": 10}
        )

        assert checkpoint.processed_count == 250
        assert checkpoint.total_count == 1000
        assert checkpoint.progress_percent == 25.0
        assert checkpoint.statistics["errors"] == 5

    @pytest.mark.unit
    def test_checkpoint_timestamp_auto_generated(self):
        """Test checkpoint timestamp is auto-generated."""
        checkpoint = JobCheckpoint(job_id="job-123")

        assert checkpoint.timestamp is not None
        assert isinstance(checkpoint.timestamp, datetime)


class TestJobState:
    """Test JobState model."""

    @pytest.mark.unit
    def test_minimal_job_state(self):
        """Test minimal job state creation."""
        job = JobState()

        assert job.job_id is not None  # Auto-generated UUID
        assert job.status == JobStatus.QUEUED
        assert job.total_documents == 0
        assert job.processed_documents == 0
        assert job.failed_documents == 0
        assert job.progress_percent == 0.0

    @pytest.mark.unit
    def test_job_id_auto_generated(self):
        """Test job_id is auto-generated as UUID."""
        job1 = JobState()
        job2 = JobState()

        assert job1.job_id != job2.job_id
        assert len(job1.job_id) == 36  # UUID format

    @pytest.mark.unit
    def test_job_state_with_all_fields(self):
        """Test job state with all fields populated."""
        now = datetime.utcnow()

        job = JobState(
            job_id="custom-job-123",
            batch_id="batch-456",
            status=JobStatus.RUNNING,
            celery_task_id="celery-task-789",
            total_documents=1000,
            processed_documents=500,
            failed_documents=10,
            progress_percent=50.0,
            started_at=now,
            metadata={"source": "test"},
            statistics={"avg_time_ms": 150}
        )

        assert job.job_id == "custom-job-123"
        assert job.batch_id == "batch-456"
        assert job.status == JobStatus.RUNNING
        assert job.celery_task_id == "celery-task-789"
        assert job.processed_documents == 500
        assert job.failed_documents == 10

    @pytest.mark.unit
    def test_timestamps_default_to_utc_now(self):
        """Test timestamps are auto-generated."""
        job = JobState()

        assert job.created_at is not None
        assert job.updated_at is not None
        assert isinstance(job.created_at, datetime)

    @pytest.mark.unit
    def test_optional_timestamps_are_none(self):
        """Test optional timestamps default to None."""
        job = JobState()

        assert job.started_at is None
        assert job.paused_at is None
        assert job.resumed_at is None
        assert job.completed_at is None

    @pytest.mark.unit
    def test_job_state_json_serialization(self):
        """Test job state can be serialized to JSON."""
        job = JobState(
            total_documents=100,
            metadata={"key": "value"}
        )

        json_str = job.model_dump_json()

        assert "job_id" in json_str
        assert "queued" in json_str  # status value
        assert '"metadata":{"key":"value"}' in json_str


class TestJobStatusResponse:
    """Test JobStatusResponse model."""

    @pytest.mark.unit
    def test_status_response_creation(self):
        """Test status response model creation."""
        now = datetime.utcnow()

        response = JobStatusResponse(
            job_id="job-123",
            batch_id="batch-456",
            status=JobStatus.COMPLETED,
            progress_percent=100.0,
            total_documents=1000,
            processed_documents=990,
            failed_documents=10,
            created_at=now,
            completed_at=now
        )

        assert response.job_id == "job-123"
        assert response.status == JobStatus.COMPLETED
        assert response.progress_percent == 100.0

    @pytest.mark.unit
    def test_status_response_optional_fields(self):
        """Test status response with optional fields as None."""
        response = JobStatusResponse(
            job_id="job-123",
            status=JobStatus.QUEUED,
            created_at=datetime.utcnow()
        )

        assert response.batch_id is None
        assert response.started_at is None
        assert response.completed_at is None
        assert response.error_message is None


class TestJobListResponse:
    """Test JobListResponse model."""

    @pytest.mark.unit
    def test_job_list_response(self):
        """Test job list response with multiple jobs."""
        now = datetime.utcnow()

        jobs = [
            JobStatusResponse(
                job_id=f"job-{i}",
                status=JobStatus.COMPLETED,
                created_at=now
            )
            for i in range(5)
        ]

        response = JobListResponse(
            jobs=jobs,
            total_count=50,
            page=1,
            page_size=5
        )

        assert len(response.jobs) == 5
        assert response.total_count == 50
        assert response.page == 1
        assert response.page_size == 5

    @pytest.mark.unit
    def test_empty_job_list(self):
        """Test job list response with no jobs."""
        response = JobListResponse(
            jobs=[],
            total_count=0
        )

        assert response.jobs == []
        assert response.total_count == 0


class TestBatchSubmitRequest:
    """Test BatchSubmitRequest model."""

    @pytest.mark.unit
    def test_minimal_batch_submit(self):
        """Test minimal batch submit request."""
        request = BatchSubmitRequest(
            documents=[{"id": "1", "text": "content"}]
        )

        assert len(request.documents) == 1
        assert request.batch_id is None
        assert request.checkpoint_interval is None

    @pytest.mark.unit
    def test_batch_submit_with_all_fields(self):
        """Test batch submit with all optional fields."""
        request = BatchSubmitRequest(
            batch_id="batch-123",
            documents=[{"id": str(i)} for i in range(100)],
            checkpoint_interval=10,
            persist_to_backends=["jsonl", "postgresql"],
            metadata={"source": "test"}
        )

        assert request.batch_id == "batch-123"
        assert len(request.documents) == 100
        assert request.checkpoint_interval == 10
        assert "jsonl" in request.persist_to_backends

    @pytest.mark.unit
    def test_empty_documents_raises_error(self):
        """Test empty documents list raises validation error."""
        with pytest.raises(ValidationError):
            BatchSubmitRequest(documents=[])

    @pytest.mark.unit
    def test_too_many_documents_raises_error(self):
        """Test exceeding max documents raises validation error."""
        with pytest.raises(ValidationError):
            BatchSubmitRequest(
                documents=[{"id": str(i)} for i in range(10001)]
            )

    @pytest.mark.unit
    def test_invalid_checkpoint_interval_raises_error(self):
        """Test invalid checkpoint interval raises validation error."""
        with pytest.raises(ValidationError):
            BatchSubmitRequest(
                documents=[{"id": "1"}],
                checkpoint_interval=0  # Must be >= 1
            )

    @pytest.mark.unit
    def test_checkpoint_interval_too_high_raises_error(self):
        """Test checkpoint interval exceeding max raises error."""
        with pytest.raises(ValidationError):
            BatchSubmitRequest(
                documents=[{"id": "1"}],
                checkpoint_interval=1001  # Must be <= 1000
            )


class TestBatchSubmitResponse:
    """Test BatchSubmitResponse model."""

    @pytest.mark.unit
    def test_batch_submit_response(self):
        """Test batch submit response creation."""
        response = BatchSubmitResponse(
            job_id="job-123",
            batch_id="batch-456",
            total_documents=1000,
            message="Job submitted successfully",
            estimated_duration_minutes=30
        )

        assert response.status == "accepted"
        assert response.job_id == "job-123"
        assert response.total_documents == 1000
        assert response.estimated_duration_minutes == 30

    @pytest.mark.unit
    def test_response_default_status(self):
        """Test response has default status of 'accepted'."""
        response = BatchSubmitResponse(
            job_id="job-123",
            total_documents=100,
            message="Submitted"
        )

        assert response.status == "accepted"


class TestJobPauseResponse:
    """Test JobPauseResponse model."""

    @pytest.mark.unit
    def test_pause_response(self):
        """Test pause response creation."""
        response = JobPauseResponse(
            status="success",
            job_id="job-123",
            message="Job paused successfully",
            checkpoint_saved=True
        )

        assert response.status == "success"
        assert response.checkpoint_saved is True

    @pytest.mark.unit
    def test_pause_response_no_checkpoint(self):
        """Test pause response without checkpoint saved."""
        response = JobPauseResponse(
            status="success",
            job_id="job-123",
            message="Job paused"
        )

        assert response.checkpoint_saved is False


class TestJobResumeResponse:
    """Test JobResumeResponse model."""

    @pytest.mark.unit
    def test_resume_response(self):
        """Test resume response creation."""
        response = JobResumeResponse(
            status="accepted",
            job_id="job-123",
            message="Job resumed from checkpoint",
            resume_from_checkpoint=True,
            remaining_documents=500
        )

        assert response.status == "accepted"
        assert response.resume_from_checkpoint is True
        assert response.remaining_documents == 500

    @pytest.mark.unit
    def test_resume_response_from_beginning(self):
        """Test resume response without checkpoint."""
        response = JobResumeResponse(
            status="accepted",
            job_id="job-123",
            message="Job started from beginning"
        )

        assert response.resume_from_checkpoint is False
        assert response.remaining_documents is None


class TestJobCancelResponse:
    """Test JobCancelResponse model."""

    @pytest.mark.unit
    def test_cancel_response(self):
        """Test cancel response creation."""
        response = JobCancelResponse(
            status="success",
            job_id="job-123",
            message="Job cancelled",
            documents_processed=250
        )

        assert response.status == "success"
        assert response.documents_processed == 250

    @pytest.mark.unit
    def test_cancel_response_no_progress(self):
        """Test cancel response with no documents processed."""
        response = JobCancelResponse(
            status="success",
            job_id="job-123",
            message="Job cancelled before processing"
        )

        assert response.documents_processed == 0


class TestResourceUsage:
    """Test ResourceUsage model."""

    @pytest.mark.unit
    def test_minimal_resource_usage(self):
        """Test resource usage without GPU."""
        usage = ResourceUsage(
            cpu_percent=45.2,
            memory_percent=60.5,
            memory_used_gb=24.5,
            memory_total_gb=64.0
        )

        assert usage.cpu_percent == 45.2
        assert usage.memory_percent == 60.5
        assert usage.gpu_available is False

    @pytest.mark.unit
    def test_resource_usage_with_gpu(self):
        """Test resource usage with GPU metrics."""
        usage = ResourceUsage(
            cpu_percent=30.0,
            memory_percent=50.0,
            memory_used_gb=16.0,
            memory_total_gb=32.0,
            gpu_available=True,
            gpu_memory_used_mb=8192.0,
            gpu_memory_total_mb=11264.0
        )

        assert usage.gpu_available is True
        assert usage.gpu_memory_used_mb == 8192.0
        assert usage.gpu_memory_total_mb == 11264.0

    @pytest.mark.unit
    def test_timestamp_auto_generated(self):
        """Test timestamp is auto-generated."""
        usage = ResourceUsage(
            cpu_percent=10.0,
            memory_percent=20.0,
            memory_used_gb=5.0,
            memory_total_gb=16.0
        )

        assert usage.timestamp is not None
        assert isinstance(usage.timestamp, datetime)


class TestModelSerialization:
    """Test model serialization and deserialization."""

    @pytest.mark.unit
    def test_job_state_to_dict(self):
        """Test job state can be converted to dict."""
        job = JobState(
            total_documents=100,
            processed_documents=50
        )

        data = job.model_dump()

        assert data["total_documents"] == 100
        assert data["processed_documents"] == 50
        assert data["status"] == "queued"

    @pytest.mark.unit
    def test_batch_request_from_dict(self):
        """Test batch request can be created from dict."""
        data = {
            "batch_id": "batch-123",
            "documents": [{"id": "1", "text": "content"}],
            "checkpoint_interval": 10
        }

        request = BatchSubmitRequest(**data)

        assert request.batch_id == "batch-123"
        assert request.checkpoint_interval == 10

    @pytest.mark.unit
    def test_resource_usage_json_serialization(self):
        """Test resource usage can be serialized to JSON."""
        usage = ResourceUsage(
            cpu_percent=25.5,
            memory_percent=50.0,
            memory_used_gb=8.0,
            memory_total_gb=16.0
        )

        json_str = usage.model_dump_json()

        assert "25.5" in json_str
        assert "cpu_percent" in json_str
