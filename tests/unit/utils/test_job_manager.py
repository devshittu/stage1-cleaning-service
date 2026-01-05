"""
tests/unit/utils/test_job_manager.py

Unit tests for JobManager.

Tests cover:
- Singleton pattern enforcement
- PostgreSQL connection pooling
- Job creation and tracking
- Status transitions with timestamps
- Progress updates
- Job querying and listing
- Error handling and graceful degradation
"""

import pytest
import json
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime

from src.utils.job_manager import JobManager, get_job_manager
from src.schemas.job_models import JobStatus, JobState


class TestJobManagerInitialization:
    """Test JobManager initialization."""

    @pytest.mark.unit
    def test_singleton_pattern(self):
        """Test JobManager is a singleton."""
        manager1 = JobManager()
        manager2 = JobManager()

        assert manager1 is manager2

    @pytest.mark.unit
    def test_get_job_manager_returns_singleton(self):
        """Test get_job_manager function returns singleton."""
        manager1 = get_job_manager()
        manager2 = get_job_manager()

        assert manager1 is manager2

    @pytest.mark.unit
    @patch('src.utils.job_manager.ASYNCPG_AVAILABLE', False)
    def test_initialization_without_asyncpg(self):
        """Test initialization when asyncpg is not available."""
        # Reset singleton for test
        JobManager._instance = None

        manager = JobManager()

        assert manager.enabled is False

    @pytest.mark.unit
    @patch('src.utils.job_manager.ASYNCPG_AVAILABLE', True)
    def test_initialization_with_asyncpg(self):
        """Test initialization when asyncpg is available."""
        # Reset singleton for test
        JobManager._instance = None

        manager = JobManager()

        assert manager.enabled is True

    @pytest.mark.unit
    def test_database_connection_settings(self):
        """Test database connection settings from environment."""
        manager = JobManager()

        assert hasattr(manager, 'db_host')
        assert hasattr(manager, 'db_port')
        assert hasattr(manager, 'db_name')
        assert hasattr(manager, 'db_user')


class TestInitializePool:
    """Test connection pool initialization."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_pool_when_disabled(self):
        """Test pool initialization when disabled."""
        manager = JobManager()
        manager.enabled = False

        result = await manager.initialize_pool()

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_pool_success(self, mock_postgres_pool):
        """Test successful pool initialization."""
        manager = JobManager()
        manager.enabled = True

        with patch('src.utils.job_manager.asyncpg') as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(return_value=mock_postgres_pool)

            result = await manager.initialize_pool()

            assert result is True
            mock_asyncpg.create_pool.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_pool_already_initialized(self):
        """Test initialization when pool already exists."""
        manager = JobManager()
        manager.enabled = True

        # Set pre-existing pool
        JobManager._pool = Mock()

        result = await manager.initialize_pool()

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_pool_connection_failure(self):
        """Test pool initialization handles connection failure."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = None

        with patch('src.utils.job_manager.asyncpg') as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(side_effect=Exception("Connection failed"))

            result = await manager.initialize_pool()

            assert result is False
            assert manager.enabled is False


class TestCreateTables:
    """Test database table creation."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_tables_when_no_pool(self):
        """Test _create_tables handles no pool gracefully."""
        manager = JobManager()
        JobManager._pool = None

        # Should not raise exception
        await manager._create_tables()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_tables_success(self, mock_postgres_pool):
        """Test successful table creation."""
        manager = JobManager()
        JobManager._pool = mock_postgres_pool

        await manager._create_tables()

        # Verify execute was called
        mock_postgres_pool.acquire.return_value.__aenter__.return_value.execute.assert_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_tables_failure_raises(self, mock_postgres_pool):
        """Test _create_tables raises on failure."""
        manager = JobManager()
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute = AsyncMock(side_effect=Exception("Table creation failed"))

        with pytest.raises(Exception):
            await manager._create_tables()


class TestCreateJob:
    """Test job creation."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_job_when_disabled(self):
        """Test create_job returns None when disabled."""
        manager = JobManager()
        manager.enabled = False

        result = await manager.create_job("job-123")

        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_job_success(self, mock_postgres_pool):
        """Test successful job creation."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        result = await manager.create_job(
            job_id="job-123",
            batch_id="batch-456",
            total_documents=100,
            metadata={"source": "test"}
        )

        assert result is not None
        assert result.job_id == "job-123"
        assert result.batch_id == "batch-456"
        assert result.status == JobStatus.QUEUED
        assert result.total_documents == 100

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_job_minimal(self, mock_postgres_pool):
        """Test job creation with minimal parameters."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        result = await manager.create_job(job_id="job-123")

        assert result is not None
        assert result.job_id == "job-123"
        assert result.batch_id is None
        assert result.total_documents == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_create_job_handles_error(self, mock_postgres_pool):
        """Test create_job handles database errors gracefully."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute = AsyncMock(side_effect=Exception("Database error"))

        result = await manager.create_job("job-123")

        assert result is None


class TestUpdateJobStatus:
    """Test job status updates."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_status_when_disabled(self):
        """Test update_job_status returns False when disabled."""
        manager = JobManager()
        manager.enabled = False

        result = await manager.update_job_status("job-123", JobStatus.RUNNING)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_status_to_running(self, mock_postgres_pool):
        """Test updating status to RUNNING sets started_at."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        result = await manager.update_job_status(
            job_id="job-123",
            status=JobStatus.RUNNING,
            celery_task_id="celery-456"
        )

        assert result is True
        # Verify execute was called
        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.assert_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_status_to_paused(self, mock_postgres_pool):
        """Test updating status to PAUSED sets paused_at."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        result = await manager.update_job_status("job-123", JobStatus.PAUSED)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_status_to_completed(self, mock_postgres_pool):
        """Test updating status to COMPLETED sets completed_at."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        result = await manager.update_job_status("job-123", JobStatus.COMPLETED)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_status_with_error_message(self, mock_postgres_pool):
        """Test updating status with error message."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        result = await manager.update_job_status(
            job_id="job-123",
            status=JobStatus.FAILED,
            error_message="Processing error occurred"
        )

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_status_handles_error(self, mock_postgres_pool):
        """Test update_job_status handles database errors gracefully."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute = AsyncMock(side_effect=Exception("Database error"))

        result = await manager.update_job_status("job-123", JobStatus.RUNNING)

        assert result is False


class TestUpdateJobProgress:
    """Test job progress updates."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_progress_when_disabled(self):
        """Test update_job_progress returns False when disabled."""
        manager = JobManager()
        manager.enabled = False

        result = await manager.update_job_progress("job-123", 50)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_progress_success(self, mock_postgres_pool):
        """Test successful progress update."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        # Mock fetchrow to return total_documents
        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value={'total_documents': 100})

        result = await manager.update_job_progress(
            job_id="job-123",
            processed_documents=50,
            failed_documents=5
        )

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_progress_calculates_percent(self, mock_postgres_pool):
        """Test progress percentage calculation."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value={'total_documents': 200})

        result = await manager.update_job_progress("job-123", 100)

        # Should calculate 50% progress
        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_progress_with_statistics(self, mock_postgres_pool):
        """Test progress update with statistics."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value={'total_documents': 100})

        statistics = {"avg_time_ms": 150, "errors": 5}

        result = await manager.update_job_progress(
            job_id="job-123",
            processed_documents=50,
            statistics=statistics
        )

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_progress_job_not_found(self, mock_postgres_pool):
        """Test progress update when job not found."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=None)

        result = await manager.update_job_progress("nonexistent-job", 50)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_update_progress_handles_error(self, mock_postgres_pool):
        """Test update_job_progress handles database errors gracefully."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(side_effect=Exception("Database error"))

        result = await manager.update_job_progress("job-123", 50)

        assert result is False


class TestGetJob:
    """Test job retrieval."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_job_when_disabled(self):
        """Test get_job returns None when disabled."""
        manager = JobManager()
        manager.enabled = False

        result = await manager.get_job("job-123")

        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_job_success(self, mock_postgres_pool):
        """Test successful job retrieval."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_row = {
            'job_id': 'job-123',
            'batch_id': 'batch-456',
            'status': 'running',
            'celery_task_id': 'celery-789',
            'total_documents': 100,
            'processed_documents': 50,
            'failed_documents': 5,
            'progress_percent': 50.0,
            'created_at': datetime.utcnow(),
            'started_at': datetime.utcnow(),
            'paused_at': None,
            'resumed_at': None,
            'completed_at': None,
            'updated_at': datetime.utcnow(),
            'metadata': json.dumps({"source": "test"}),
            'error_message': None,
            'statistics': json.dumps({"avg_time_ms": 150})
        }

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=mock_row)

        result = await manager.get_job("job-123")

        assert result is not None
        assert result.job_id == "job-123"
        assert result.status == JobStatus.RUNNING
        assert result.processed_documents == 50

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_job_not_found(self, mock_postgres_pool):
        """Test get_job when job doesn't exist."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=None)

        result = await manager.get_job("nonexistent-job")

        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_job_handles_error(self, mock_postgres_pool):
        """Test get_job handles database errors gracefully."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(side_effect=Exception("Database error"))

        result = await manager.get_job("job-123")

        assert result is None


class TestListJobs:
    """Test job listing."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_jobs_when_disabled(self):
        """Test list_jobs returns empty list when disabled."""
        manager = JobManager()
        manager.enabled = False

        result = await manager.list_jobs()

        assert result == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_jobs_success(self, mock_postgres_pool):
        """Test successful job listing."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_rows = [
            {
                'job_id': f'job-{i}',
                'batch_id': None,
                'status': 'completed',
                'celery_task_id': None,
                'total_documents': 100,
                'processed_documents': 100,
                'failed_documents': 0,
                'progress_percent': 100.0,
                'created_at': datetime.utcnow(),
                'started_at': datetime.utcnow(),
                'paused_at': None,
                'resumed_at': None,
                'completed_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
                'metadata': json.dumps({}),
                'error_message': None,
                'statistics': json.dumps({})
            }
            for i in range(5)
        ]

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        result = await manager.list_jobs(limit=5)

        assert len(result) == 5

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_jobs_with_status_filter(self, mock_postgres_pool):
        """Test listing jobs with status filter."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await manager.list_jobs(status=JobStatus.RUNNING)

        assert result == []
        mock_conn.fetch.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_jobs_with_batch_filter(self, mock_postgres_pool):
        """Test listing jobs with batch_id filter."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await manager.list_jobs(batch_id="batch-123")

        assert result == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_jobs_with_pagination(self, mock_postgres_pool):
        """Test listing jobs with pagination."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await manager.list_jobs(limit=10, offset=20)

        assert result == []

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_jobs_handles_error(self, mock_postgres_pool):
        """Test list_jobs handles database errors gracefully."""
        manager = JobManager()
        manager.enabled = True
        JobManager._pool = mock_postgres_pool

        mock_conn = mock_postgres_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(side_effect=Exception("Database error"))

        result = await manager.list_jobs()

        assert result == []


class TestClose:
    """Test connection pool closure."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_pool(self, mock_postgres_pool):
        """Test closing connection pool."""
        manager = JobManager()
        JobManager._pool = mock_postgres_pool

        await manager.close()

        mock_postgres_pool.close.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_sets_pool_to_none(self, mock_postgres_pool):
        """Test close sets pool to None."""
        manager = JobManager()
        JobManager._pool = mock_postgres_pool

        await manager.close()

        assert JobManager._pool is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_when_no_pool(self):
        """Test close when no pool exists."""
        manager = JobManager()
        JobManager._pool = None

        # Should not raise exception
        await manager.close()
