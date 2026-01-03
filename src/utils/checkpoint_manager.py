"""
src/utils/checkpoint_manager.py

Checkpoint manager for progressive persistence and pause/resume support.

Responsibilities:
- Save job progress checkpoints to Redis
- Track processed document IDs in Redis sets
- Load checkpoints for resume operations
- Clear checkpoints on job completion
- Support TTL-based checkpoint expiry

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if Redis unavailable
- All operations wrapped in try-catch
- Fail-safe mode continues without checkpointing
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Set, Any

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    aioredis = None

from src.schemas.job_models import JobCheckpoint

logger = logging.getLogger("ingestion_service")


class CheckpointManager:
    """
    Manages job checkpoints in Redis for progressive persistence.

    Features:
    - Save checkpoint every N documents
    - Track processed document IDs in Redis sets
    - Resume from checkpoint after pause
    - Automatic TTL expiry (24 hours default)
    """

    _instance: Optional['CheckpointManager'] = None
    _redis_client: Optional[Any] = None

    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize checkpoint manager (singleton)."""
        if not hasattr(self, '_initialized'):
            self._initialized = False
            self.enabled = REDIS_AVAILABLE

            if not self.enabled:
                logger.warning("redis not available - checkpointing disabled")
                return

            # Redis connection settings
            self.redis_host = os.getenv("REDIS_CACHE_HOST", "redis-cache")
            self.redis_port = int(os.getenv("REDIS_CACHE_PORT", "6379"))
            self.redis_db = int(os.getenv("REDIS_CACHE_DB", "1"))
            self.redis_url = os.getenv(
                "REDIS_CACHE_URL",
                f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
            )

            # Checkpoint TTL (24 hours default)
            self.checkpoint_ttl = int(os.getenv("CHECKPOINT_TTL_SECONDS", "86400"))

            self._initialized = True

    async def initialize_client(self):
        """Initialize Redis client (call once at startup)."""
        if not self.enabled:
            return False

        if CheckpointManager._redis_client is not None:
            return True

        try:
            CheckpointManager._redis_client = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )

            # Test connection
            await CheckpointManager._redis_client.ping()

            logger.info(
                f"checkpoint_manager_initialized: redis_url={self.redis_url}, "
                f"ttl_seconds={self.checkpoint_ttl}"
            )
            return True

        except Exception as e:
            logger.error(f"failed_to_initialize_checkpoint_manager: {e}")
            self.enabled = False
            return False

    def _checkpoint_key(self, job_id: str) -> str:
        """Generate Redis key for checkpoint data."""
        return f"stage1:job:{job_id}:checkpoint"

    def _processed_docs_key(self, job_id: str) -> str:
        """Generate Redis key for processed documents set."""
        return f"stage1:job:{job_id}:processed"

    def _stats_key(self, job_id: str) -> str:
        """Generate Redis key for job statistics."""
        return f"stage1:job:{job_id}:stats"

    async def save_checkpoint(
        self,
        job_id: str,
        processed_count: int,
        total_count: int,
        last_processed_doc_id: Optional[str] = None,
        statistics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Save job checkpoint to Redis.

        Args:
            job_id: Job identifier
            processed_count: Number of documents processed so far
            total_count: Total documents in job
            last_processed_doc_id: Last processed document ID
            statistics: Optional statistics dictionary

        Returns:
            True if successful
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return False

        try:
            progress_percent = (processed_count / total_count * 100.0) if total_count > 0 else 0.0

            checkpoint = JobCheckpoint(
                job_id=job_id,
                processed_count=processed_count,
                total_count=total_count,
                last_processed_doc_id=last_processed_doc_id,
                progress_percent=progress_percent,
                timestamp=datetime.utcnow(),
                statistics=statistics or {}
            )

            checkpoint_data = checkpoint.model_dump_json()

            # Save checkpoint with TTL
            await CheckpointManager._redis_client.setex(
                self._checkpoint_key(job_id),
                self.checkpoint_ttl,
                checkpoint_data
            )

            # Also save statistics separately for quick access
            if statistics:
                await CheckpointManager._redis_client.setex(
                    self._stats_key(job_id),
                    self.checkpoint_ttl,
                    json.dumps(statistics)
                )

            logger.info(
                f"checkpoint_saved: job_id={job_id}, processed_count={processed_count}, "
                f"total_count={total_count}, progress_percent={progress_percent:.1f}%"
            )

            return True

        except Exception as e:
            logger.error(f"failed_to_save_checkpoint: {e}")
            return False

    async def load_checkpoint(self, job_id: str) -> Optional[JobCheckpoint]:
        """
        Load job checkpoint from Redis.

        Args:
            job_id: Job identifier

        Returns:
            JobCheckpoint object if found, None otherwise
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return None

        try:
            checkpoint_data = await CheckpointManager._redis_client.get(
                self._checkpoint_key(job_id)
            )

            if not checkpoint_data:
                logger.info(f"no_checkpoint_found for job_id={job_id}")
                return None

            checkpoint = JobCheckpoint.model_validate_json(checkpoint_data)

            logger.info(
                f"checkpoint_loaded: job_id={job_id}, processed_count={checkpoint.processed_count}, "
                f"total_count={checkpoint.total_count}, progress_percent={checkpoint.progress_percent:.1f}%"
            )

            return checkpoint

        except Exception as e:
            logger.error(f"failed_to_load_checkpoint: {e}")
            return None

    async def mark_document_processed(
        self,
        job_id: str,
        document_id: str
    ) -> bool:
        """
        Mark a document as processed (add to Redis set).

        Args:
            job_id: Job identifier
            document_id: Document identifier

        Returns:
            True if successful
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return False

        try:
            await CheckpointManager._redis_client.sadd(
                self._processed_docs_key(job_id),
                document_id
            )

            # Set TTL on the set
            await CheckpointManager._redis_client.expire(
                self._processed_docs_key(job_id),
                self.checkpoint_ttl
            )

            return True

        except Exception as e:
            logger.error(f"failed_to_mark_document_processed: {e}")
            return False

    async def get_processed_documents(self, job_id: str) -> Set[str]:
        """
        Get set of processed document IDs.

        Args:
            job_id: Job identifier

        Returns:
            Set of processed document IDs
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return set()

        try:
            processed_docs = await CheckpointManager._redis_client.smembers(
                self._processed_docs_key(job_id)
            )

            return set(processed_docs) if processed_docs else set()

        except Exception as e:
            logger.error(f"failed_to_get_processed_documents: {e}")
            return set()

    async def is_document_processed(
        self,
        job_id: str,
        document_id: str
    ) -> bool:
        """
        Check if a document has been processed.

        Args:
            job_id: Job identifier
            document_id: Document identifier

        Returns:
            True if document was processed
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return False

        try:
            is_member = await CheckpointManager._redis_client.sismember(
                self._processed_docs_key(job_id),
                document_id
            )

            return bool(is_member)

        except Exception as e:
            logger.error(f"failed_to_check_document_processed: {e}")
            return False

    async def get_processed_count(self, job_id: str) -> int:
        """
        Get count of processed documents.

        Args:
            job_id: Job identifier

        Returns:
            Number of processed documents
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return 0

        try:
            count = await CheckpointManager._redis_client.scard(
                self._processed_docs_key(job_id)
            )

            return count if count else 0

        except Exception as e:
            logger.error(f"failed_to_get_processed_count: {e}")
            return 0

    async def clear_checkpoint(self, job_id: str) -> bool:
        """
        Clear job checkpoint and processed documents set.

        Call on job completion or cancellation.

        Args:
            job_id: Job identifier

        Returns:
            True if successful
        """
        if not self.enabled or not CheckpointManager._redis_client:
            return False

        try:
            # Delete all keys related to this job
            await CheckpointManager._redis_client.delete(
                self._checkpoint_key(job_id),
                self._processed_docs_key(job_id),
                self._stats_key(job_id)
            )

            logger.info(
                f"checkpoint_cleared: job_id={job_id}"
            )

            return True

        except Exception as e:
            logger.error(f"failed_to_clear_checkpoint: {e}")
            return False

    async def close(self):
        """Close Redis client."""
        if CheckpointManager._redis_client:
            await CheckpointManager._redis_client.close()
            CheckpointManager._redis_client = None
            logger.info("checkpoint_manager_client_closed")


# Singleton instance
_checkpoint_manager_instance: Optional[CheckpointManager] = None


def get_checkpoint_manager() -> CheckpointManager:
    """Get singleton instance of checkpoint manager."""
    global _checkpoint_manager_instance

    if _checkpoint_manager_instance is None:
        _checkpoint_manager_instance = CheckpointManager()

    return _checkpoint_manager_instance
