"""
tests/integration/api/test_batch_endpoints.py

Integration tests for batch processing API endpoints.

Tests cover:
- Batch job submission
- Job status checking
- Job lifecycle operations (pause/resume/cancel)
- Job listing
"""

import pytest
import json
from unittest.mock import patch, AsyncMock, Mock, MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def app_client():
    """Create test client with mocked dependencies."""
    # Mock database and Redis before importing app
    with patch('src.utils.job_manager.asyncpg') as mock_asyncpg, \
         patch('redis.asyncio') as mock_redis:

        # Setup mock pool
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value=None)

        mock_acquire = MagicMock()
        mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire.__aexit__ = AsyncMock()
        mock_pool.acquire.return_value = mock_acquire

        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Import app after mocking
        from src.api.app import app
        client = TestClient(app)

        yield client


class TestBatchSubmission:
    """Test batch job submission endpoint."""

    @pytest.mark.integration
    def test_batch_submit_minimal(self, app_client, sample_documents):
        """Test minimal batch submission."""
        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="test-task-123")

            response = app_client.post(
                "/v1/documents/batch",
                json={"documents": sample_documents}
            )

            assert response.status_code == 202
            data = response.json()
            assert "job_id" in data
            assert data["status"] == "accepted"

    @pytest.mark.integration
    def test_batch_submit_with_batch_id(self, app_client, sample_documents):
        """Test batch submission with custom batch ID."""
        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="test-task-123")

            response = app_client.post(
                "/v1/documents/batch",
                json={
                    "documents": sample_documents,
                    "batch_id": "custom-batch-123"
                }
            )

            assert response.status_code == 202
            data = response.json()
            assert data["batch_id"] == "custom-batch-123"

    @pytest.mark.integration
    def test_batch_submit_empty_documents(self, app_client):
        """Test batch submission with empty documents returns error."""
        response = app_client.post(
            "/v1/documents/batch",
            json={"documents": []}
        )

        assert response.status_code == 422  # FastAPI validation error

    @pytest.mark.integration
    def test_batch_submit_invalid_json(self, app_client):
        """Test batch submission with invalid JSON."""
        response = app_client.post(
            "/v1/documents/batch",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422


class TestJobStatus:
    """Test job status endpoint."""

    @pytest.mark.integration
    def test_get_job_status_found(self, app_client):
        """Test getting status of existing job."""
        from datetime import datetime
        from src.schemas.job_models import JobStatus

        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()
            mock_job = MagicMock()
            mock_job.job_id = "test-job-123"
            mock_job.batch_id = "test-batch-123"
            mock_job.status = JobStatus.COMPLETED
            mock_job.progress_percent = 100.0
            mock_job.total_documents = 100
            mock_job.processed_documents = 100
            mock_job.failed_documents = 0
            mock_job.created_at = datetime.utcnow()
            mock_job.started_at = datetime.utcnow()
            mock_job.completed_at = datetime.utcnow()
            mock_job.error_message = None
            mock_job.statistics = {}

            mock_manager.get_job = AsyncMock(return_value=mock_job)
            mock_get_manager.return_value = mock_manager

            response = app_client.get("/v1/jobs/test-job-123")

            assert response.status_code == 200
            data = response.json()
            assert data["job_id"] == "test-job-123"

    @pytest.mark.integration
    def test_get_job_status_not_found(self, app_client):
        """Test getting status of non-existent job."""
        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()
            mock_manager.get_job = AsyncMock(return_value=None)
            mock_get_manager.return_value = mock_manager

            response = app_client.get("/v1/jobs/nonexistent-job")

            assert response.status_code == 404


class TestJobLifecycle:
    """Test job lifecycle operations."""

    @pytest.mark.integration
    def test_pause_job_success(self, app_client):
        """Test pausing a running job."""
        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()
            mock_job = MagicMock()
            mock_job.status.value = "running"
            mock_job.status.name = "RUNNING"

            from src.schemas.job_models import JobStatus
            mock_job.status = JobStatus.RUNNING

            mock_manager.get_job = AsyncMock(return_value=mock_job)
            mock_manager.update_job_status = AsyncMock(return_value=True)
            mock_get_manager.return_value = mock_manager

            response = app_client.patch("/v1/jobs/test-job-123/pause")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "success"

    @pytest.mark.integration
    def test_pause_already_paused_job(self, app_client):
        """Test pausing an already paused job returns error."""
        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()
            mock_job = MagicMock()

            from src.schemas.job_models import JobStatus
            mock_job.status = JobStatus.PAUSED

            mock_manager.get_job = AsyncMock(return_value=mock_job)
            mock_get_manager.return_value = mock_manager

            response = app_client.patch("/v1/jobs/test-job-123/pause")

            assert response.status_code == 400

    @pytest.mark.integration
    def test_cancel_job_success(self, app_client):
        """Test cancelling a job."""
        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()
            mock_job = MagicMock()

            from src.schemas.job_models import JobStatus
            mock_job.status = JobStatus.RUNNING
            mock_job.celery_task_id = "celery-task-123"

            mock_manager.get_job = AsyncMock(return_value=mock_job)
            mock_manager.update_job_status = AsyncMock(return_value=True)
            mock_get_manager.return_value = mock_manager

            with patch('src.api.app.celery_app') as mock_celery:
                mock_celery.control.revoke = Mock()

                response = app_client.delete("/v1/jobs/test-job-123")

                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "success"


class TestJobListing:
    """Test job listing endpoint."""

    @pytest.mark.integration
    def test_list_jobs_no_filter(self, app_client):
        """Test listing all jobs without filters."""
        from datetime import datetime
        from src.schemas.job_models import JobStatus

        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()

            # Create properly mocked jobs with all required attributes
            mock_jobs = []
            for i in range(5):
                job = MagicMock()
                job.job_id = f"job-{i}"
                job.batch_id = f"batch-{i}"
                job.status = JobStatus.COMPLETED
                job.progress_percent = 100.0
                job.total_documents = 100
                job.processed_documents = 100
                job.failed_documents = 0
                job.created_at = datetime.utcnow()
                job.started_at = datetime.utcnow()
                job.completed_at = datetime.utcnow()
                job.error_message = None
                job.statistics = {}
                mock_jobs.append(job)

            mock_manager.list_jobs = AsyncMock(return_value=mock_jobs)
            mock_get_manager.return_value = mock_manager

            response = app_client.get("/v1/jobs")

            assert response.status_code == 200
            data = response.json()
            assert "jobs" in data
            assert len(data["jobs"]) == 5

    @pytest.mark.integration
    def test_list_jobs_with_status_filter(self, app_client):
        """Test listing jobs with status filter."""
        from datetime import datetime
        from src.schemas.job_models import JobStatus

        with patch('src.api.app.get_job_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.initialize_pool = AsyncMock()

            # Create properly mocked job with all required attributes
            job = MagicMock()
            job.job_id = "job-1"
            job.batch_id = "batch-1"
            job.status = JobStatus.RUNNING
            job.progress_percent = 50.0
            job.total_documents = 100
            job.processed_documents = 50
            job.failed_documents = 0
            job.created_at = datetime.utcnow()
            job.started_at = datetime.utcnow()
            job.completed_at = None
            job.error_message = None
            job.statistics = {}

            mock_jobs = [job]

            mock_manager.list_jobs = AsyncMock(return_value=mock_jobs)
            mock_get_manager.return_value = mock_manager

            response = app_client.get("/v1/jobs?status=RUNNING")

            assert response.status_code == 200
            data = response.json()
            assert len(data["jobs"]) == 1


class TestHealthEndpoint:
    """Test health check endpoint."""

    @pytest.mark.integration
    def test_health_check(self, app_client):
        """Test health check returns OK."""
        response = app_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
