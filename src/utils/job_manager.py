"""
src/utils/job_manager.py

Job lifecycle manager for batch processing.

Responsibilities:
- Create and track job records in PostgreSQL
- Manage job state transitions
- Update job progress and statistics
- Query job status and history
- Support pause/resume/cancel operations

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if database unavailable
- All operations wrapped in try-catch
- Fail-safe mode continues without tracking
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    asyncpg = None

from src.schemas.job_models import JobStatus, JobState, JobStatusResponse

logger = logging.getLogger("ingestion_service")


class JobManager:
    """
    Manages job lifecycle in PostgreSQL database.

    Singleton pattern with connection pooling.
    """

    _instance: Optional['JobManager'] = None
    _pool: Optional[Any] = None

    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize job manager (singleton)."""
        if not hasattr(self, '_initialized'):
            self._initialized = False
            self.enabled = ASYNCPG_AVAILABLE

            if not self.enabled:
                logger.warning("asyncpg not available - job tracking disabled")
                return

            # Database connection settings
            self.db_host = os.getenv("POSTGRES_HOST", "postgres")
            self.db_port = int(os.getenv("POSTGRES_PORT", "5432"))
            self.db_name = os.getenv("POSTGRES_DB", "stage1_cleaning")
            self.db_user = os.getenv("POSTGRES_USER", "stage1_user")
            self.db_password = os.getenv("POSTGRES_PASSWORD", "")

            self._initialized = True

    async def initialize_pool(self):
        """Initialize connection pool (call once at startup)."""
        if not self.enabled:
            return False

        if JobManager._pool is not None:
            return True

        try:
            JobManager._pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                min_size=2,
                max_size=10,
                command_timeout=60
            )

            # Create job_registry table if not exists
            await self._create_tables()

            logger.info(
                f"Job manager pool initialized: host={self.db_host}, database={self.db_name}"
            )
            return True

        except Exception as e:
            logger.error(f"failed_to_initialize_job_manager_pool: {e}")
            self.enabled = False
            return False

    async def _create_tables(self):
        """Create job_registry table if not exists."""
        if not JobManager._pool:
            return

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS job_registry (
            job_id VARCHAR(255) PRIMARY KEY,
            batch_id VARCHAR(255),
            status VARCHAR(50) NOT NULL,
            celery_task_id VARCHAR(255),

            -- Progress tracking
            total_documents INTEGER DEFAULT 0,
            processed_documents INTEGER DEFAULT 0,
            failed_documents INTEGER DEFAULT 0,
            progress_percent FLOAT DEFAULT 0.0,

            -- Timestamps
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            started_at TIMESTAMP,
            paused_at TIMESTAMP,
            resumed_at TIMESTAMP,
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

            -- Metadata
            metadata JSONB,
            error_message TEXT,
            statistics JSONB
        );

        CREATE INDEX IF NOT EXISTS idx_job_status ON job_registry(status);
        CREATE INDEX IF NOT EXISTS idx_job_batch_id ON job_registry(batch_id);
        CREATE INDEX IF NOT EXISTS idx_job_created_at ON job_registry(created_at DESC);
        """

        try:
            async with JobManager._pool.acquire() as conn:
                await conn.execute(create_table_sql)
            logger.info("job_registry_table_created_or_verified")
        except Exception as e:
            logger.error(f"failed_to_create_job_registry_table: {e}")
            raise

    async def create_job(
        self,
        job_id: str,
        batch_id: Optional[str] = None,
        total_documents: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[JobState]:
        """
        Create a new job record.

        Args:
            job_id: Unique job identifier
            batch_id: Optional batch identifier
            total_documents: Total documents in batch
            metadata: Optional job metadata

        Returns:
            JobState object if successful, None otherwise
        """
        if not self.enabled or not JobManager._pool:
            logger.warning("job_manager_disabled_skipping_create_job")
            return None

        try:
            async with JobManager._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO job_registry (
                        job_id, batch_id, status, total_documents, metadata,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
                    """,
                    job_id,
                    batch_id,
                    JobStatus.QUEUED.value,
                    total_documents,
                    json.dumps(metadata if metadata else {}),
                    datetime.utcnow(),
                    datetime.utcnow()
                )

            logger.info(
                f"Job created: job_id={job_id}, batch_id={batch_id}, total_documents={total_documents}"
            )

            return JobState(
                job_id=job_id,
                batch_id=batch_id,
                status=JobStatus.QUEUED,
                total_documents=total_documents,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"failed_to_create_job: {e}", exc_info=True)
            return None

    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        celery_task_id: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update job status.

        Args:
            job_id: Job identifier
            status: New status
            celery_task_id: Optional Celery task ID
            error_message: Optional error message

        Returns:
            True if successful
        """
        if not self.enabled or not JobManager._pool:
            return False

        try:
            timestamp_field = None
            timestamp_value = datetime.utcnow()

            if status == JobStatus.RUNNING:
                timestamp_field = "started_at"
            elif status == JobStatus.PAUSED:
                timestamp_field = "paused_at"
            elif status == JobStatus.COMPLETED:
                timestamp_field = "completed_at"

            async with JobManager._pool.acquire() as conn:
                if timestamp_field:
                    await conn.execute(
                        f"""
                        UPDATE job_registry
                        SET status = $1, {timestamp_field} = $2,
                            celery_task_id = $3, error_message = $4,
                            updated_at = $5
                        WHERE job_id = $6
                        """,
                        status.value,
                        timestamp_value,
                        celery_task_id,
                        error_message,
                        timestamp_value,
                        job_id
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE job_registry
                        SET status = $1, celery_task_id = $2,
                            error_message = $3, updated_at = $4
                        WHERE job_id = $5
                        """,
                        status.value,
                        celery_task_id,
                        error_message,
                        timestamp_value,
                        job_id
                    )

            logger.info(
                f"Job status updated: job_id={job_id}, status={status.value}"
            )
            return True

        except Exception as e:
            logger.error(f"failed_to_update_job_status: {e}")
            return False

    async def update_job_progress(
        self,
        job_id: str,
        processed_documents: int,
        failed_documents: int = 0,
        statistics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update job progress.

        Args:
            job_id: Job identifier
            processed_documents: Number of processed documents
            failed_documents: Number of failed documents
            statistics: Optional statistics dictionary

        Returns:
            True if successful
        """
        if not self.enabled or not JobManager._pool:
            return False

        try:
            async with JobManager._pool.acquire() as conn:
                # Calculate progress percent
                row = await conn.fetchrow(
                    "SELECT total_documents FROM job_registry WHERE job_id = $1",
                    job_id
                )

                if not row:
                    return False

                total = row['total_documents']
                progress_percent = (processed_documents / total * 100.0) if total > 0 else 0.0

                await conn.execute(
                    """
                    UPDATE job_registry
                    SET processed_documents = $1,
                        failed_documents = $2,
                        progress_percent = $3,
                        statistics = $4,
                        updated_at = $5
                    WHERE job_id = $6
                    """,
                    processed_documents,
                    failed_documents,
                    progress_percent,
                    statistics if statistics else {},
                    datetime.utcnow(),
                    job_id
                )

            return True

        except Exception as e:
            logger.error(f"failed_to_update_job_progress: {e}")
            return False

    async def get_job(self, job_id: str) -> Optional[JobState]:
        """
        Get job by ID.

        Args:
            job_id: Job identifier

        Returns:
            JobState object if found, None otherwise
        """
        if not self.enabled or not JobManager._pool:
            return None

        try:
            async with JobManager._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM job_registry WHERE job_id = $1",
                    job_id
                )

                if not row:
                    return None

                return JobState(
                    job_id=row['job_id'],
                    batch_id=row['batch_id'],
                    status=JobStatus(row['status']),
                    celery_task_id=row['celery_task_id'],
                    total_documents=row['total_documents'],
                    processed_documents=row['processed_documents'],
                    failed_documents=row['failed_documents'],
                    progress_percent=row['progress_percent'],
                    created_at=row['created_at'],
                    started_at=row['started_at'],
                    paused_at=row['paused_at'],
                    resumed_at=row['resumed_at'],
                    completed_at=row['completed_at'],
                    updated_at=row['updated_at'],
                    metadata=row['metadata'],
                    error_message=row['error_message'],
                    statistics=row['statistics']
                )

        except Exception as e:
            logger.error(f"failed_to_get_job: {e}")
            return None

    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        batch_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[JobState]:
        """
        List jobs with optional filtering.

        Args:
            status: Filter by status
            batch_id: Filter by batch_id
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of JobState objects
        """
        if not self.enabled or not JobManager._pool:
            return []

        try:
            async with JobManager._pool.acquire() as conn:
                query = "SELECT * FROM job_registry WHERE 1=1"
                params = []

                if status:
                    params.append(status.value)
                    query += f" AND status = ${len(params)}"

                if batch_id:
                    params.append(batch_id)
                    query += f" AND batch_id = ${len(params)}"

                query += " ORDER BY created_at DESC"

                params.append(limit)
                query += f" LIMIT ${len(params)}"

                params.append(offset)
                query += f" OFFSET ${len(params)}"

                rows = await conn.fetch(query, *params)

                return [
                    JobState(
                        job_id=row['job_id'],
                        batch_id=row['batch_id'],
                        status=JobStatus(row['status']),
                        celery_task_id=row['celery_task_id'],
                        total_documents=row['total_documents'],
                        processed_documents=row['processed_documents'],
                        failed_documents=row['failed_documents'],
                        progress_percent=row['progress_percent'],
                        created_at=row['created_at'],
                        started_at=row['started_at'],
                        paused_at=row['paused_at'],
                        resumed_at=row['resumed_at'],
                        completed_at=row['completed_at'],
                        updated_at=row['updated_at'],
                        metadata=row['metadata'],
                        error_message=row['error_message'],
                        statistics=row['statistics']
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"failed_to_list_jobs: {e}")
            return []

    async def close(self):
        """Close connection pool."""
        if JobManager._pool:
            await JobManager._pool.close()
            JobManager._pool = None
            logger.info("job_manager_pool_closed")


# Singleton instance
_job_manager_instance: Optional[JobManager] = None


def get_job_manager() -> JobManager:
    """Get singleton instance of job manager."""
    global _job_manager_instance

    if _job_manager_instance is None:
        _job_manager_instance = JobManager()

    return _job_manager_instance
