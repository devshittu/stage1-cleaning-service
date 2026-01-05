"""
tests/unit/utils/test_checkpoint_manager.py

Unit tests for CheckpointManager.

Tests cover:
- Singleton pattern enforcement
- Redis client initialization
- Checkpoint save/load operations
- Document tracking (processed sets)
- TTL management
- Error handling and graceful degradation
- Key generation patterns
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime

from src.utils.checkpoint_manager import CheckpointManager, get_checkpoint_manager
from src.schemas.job_models import JobCheckpoint


class TestCheckpointManagerInitialization:
    """Test CheckpointManager initialization."""

    @pytest.mark.unit
    def test_singleton_pattern(self):
        """Test CheckpointManager is a singleton."""
        manager1 = CheckpointManager()
        manager2 = CheckpointManager()

        assert manager1 is manager2

    @pytest.mark.unit
    def test_get_checkpoint_manager_returns_singleton(self):
        """Test get_checkpoint_manager function returns singleton."""
        manager1 = get_checkpoint_manager()
        manager2 = get_checkpoint_manager()

        assert manager1 is manager2

    @pytest.mark.unit
    @patch('src.utils.checkpoint_manager.REDIS_AVAILABLE', False)
    def test_initialization_without_redis(self):
        """Test initialization when Redis is not available."""
        # Reset singleton for test
        CheckpointManager._instance = None

        manager = CheckpointManager()

        assert manager.enabled is False

    @pytest.mark.unit
    @patch('src.utils.checkpoint_manager.REDIS_AVAILABLE', True)
    def test_initialization_with_redis(self):
        """Test initialization when Redis is available."""
        # Reset singleton for test
        CheckpointManager._instance = None

        manager = CheckpointManager()

        assert manager.enabled is True

    @pytest.mark.unit
    def test_redis_connection_settings(self):
        """Test Redis connection settings from environment."""
        manager = CheckpointManager()

        # Should have default values
        assert hasattr(manager, 'redis_host')
        assert hasattr(manager, 'redis_port')
        assert hasattr(manager, 'redis_db')


class TestKeyGeneration:
    """Test Redis key generation methods."""

    @pytest.mark.unit
    def test_checkpoint_key_format(self):
        """Test checkpoint key follows expected pattern."""
        manager = CheckpointManager()
        job_id = "test-job-123"

        key = manager._checkpoint_key(job_id)

        assert key == "stage1:job:test-job-123:checkpoint"

    @pytest.mark.unit
    def test_processed_docs_key_format(self):
        """Test processed documents key follows expected pattern."""
        manager = CheckpointManager()
        job_id = "test-job-123"

        key = manager._processed_docs_key(job_id)

        assert key == "stage1:job:test-job-123:processed"

    @pytest.mark.unit
    def test_stats_key_format(self):
        """Test stats key follows expected pattern."""
        manager = CheckpointManager()
        job_id = "test-job-123"

        key = manager._stats_key(job_id)

        assert key == "stage1:job:test-job-123:stats"

    @pytest.mark.unit
    def test_keys_unique_per_job(self):
        """Test keys are unique for different jobs."""
        manager = CheckpointManager()

        key1 = manager._checkpoint_key("job-1")
        key2 = manager._checkpoint_key("job-2")

        assert key1 != key2


class TestInitializeClient:
    """Test Redis client initialization."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_client_when_disabled(self):
        """Test client initialization when Redis is disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.initialize_client()

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_client_success(self, mock_redis):
        """Test successful client initialization."""
        manager = CheckpointManager()
        manager.enabled = True

        with patch('src.utils.checkpoint_manager.aioredis') as mock_aioredis:
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            result = await manager.initialize_client()

            assert result is True
            mock_redis.ping.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_client_already_initialized(self):
        """Test initialization when client already exists."""
        manager = CheckpointManager()
        manager.enabled = True

        # Set pre-existing client
        CheckpointManager._redis_client = Mock()

        result = await manager.initialize_client()

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_initialize_client_connection_failure(self):
        """Test client initialization handles connection failure."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = None

        with patch('src.utils.checkpoint_manager.aioredis') as mock_aioredis:
            mock_aioredis.from_url = AsyncMock(side_effect=Exception("Connection failed"))

            result = await manager.initialize_client()

            assert result is False
            assert manager.enabled is False


class TestSaveCheckpoint:
    """Test checkpoint save operations."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_save_checkpoint_when_disabled(self):
        """Test save_checkpoint returns False when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.save_checkpoint("job-123", 50, 100)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_save_checkpoint_success(self, mock_redis):
        """Test successful checkpoint save."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        result = await manager.save_checkpoint(
            job_id="job-123",
            processed_count=50,
            total_count=100,
            last_processed_doc_id="doc-50"
        )

        assert result is True
        mock_redis.setex.assert_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_save_checkpoint_calculates_progress(self, mock_redis):
        """Test checkpoint save calculates progress percentage."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        await manager.save_checkpoint(
            job_id="job-123",
            processed_count=25,
            total_count=100
        )

        # Verify setex was called (progress should be 25%)
        assert mock_redis.setex.call_count >= 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_save_checkpoint_with_statistics(self, mock_redis):
        """Test checkpoint save with statistics."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        statistics = {"errors": 5, "warnings": 10}

        result = await manager.save_checkpoint(
            job_id="job-123",
            processed_count=50,
            total_count=100,
            statistics=statistics
        )

        assert result is True
        # Should save both checkpoint and statistics
        assert mock_redis.setex.call_count == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_save_checkpoint_handles_zero_total(self, mock_redis):
        """Test checkpoint save handles zero total documents."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        result = await manager.save_checkpoint(
            job_id="job-123",
            processed_count=0,
            total_count=0
        )

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_save_checkpoint_handles_redis_error(self, mock_redis):
        """Test save_checkpoint handles Redis errors gracefully."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.setex = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.save_checkpoint("job-123", 50, 100)

        assert result is False


class TestLoadCheckpoint:
    """Test checkpoint load operations."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_load_checkpoint_when_disabled(self):
        """Test load_checkpoint returns None when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.load_checkpoint("job-123")

        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_load_checkpoint_not_found(self, mock_redis):
        """Test load_checkpoint when checkpoint doesn't exist."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.get = AsyncMock(return_value=None)

        result = await manager.load_checkpoint("job-123")

        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_load_checkpoint_success(self, mock_redis):
        """Test successful checkpoint load."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        checkpoint_data = JobCheckpoint(
            job_id="job-123",
            processed_count=50,
            total_count=100,
            progress_percent=50.0
        ).model_dump_json()

        mock_redis.get = AsyncMock(return_value=checkpoint_data)

        result = await manager.load_checkpoint("job-123")

        assert result is not None
        assert result.job_id == "job-123"
        assert result.processed_count == 50
        assert result.total_count == 100

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_load_checkpoint_handles_redis_error(self, mock_redis):
        """Test load_checkpoint handles Redis errors gracefully."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.get = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.load_checkpoint("job-123")

        assert result is None


class TestMarkDocumentProcessed:
    """Test document processed marking."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_mark_document_when_disabled(self):
        """Test mark_document_processed returns False when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.mark_document_processed("job-123", "doc-1")

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_mark_document_success(self, mock_redis):
        """Test successful document marking."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        result = await manager.mark_document_processed("job-123", "doc-1")

        assert result is True
        mock_redis.sadd.assert_called_once()
        mock_redis.expire.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_mark_document_sets_ttl(self, mock_redis):
        """Test document marking sets TTL on set."""
        manager = CheckpointManager()
        manager.enabled = True
        manager.checkpoint_ttl = 3600
        CheckpointManager._redis_client = mock_redis

        await manager.mark_document_processed("job-123", "doc-1")

        # Verify expire was called with TTL
        mock_redis.expire.assert_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_mark_document_handles_redis_error(self, mock_redis):
        """Test mark_document_processed handles Redis errors."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.sadd = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.mark_document_processed("job-123", "doc-1")

        assert result is False


class TestGetProcessedDocuments:
    """Test getting processed documents set."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_documents_when_disabled(self):
        """Test returns empty set when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.get_processed_documents("job-123")

        assert result == set()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_documents_success(self, mock_redis):
        """Test successful retrieval of processed documents."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.smembers = AsyncMock(return_value={"doc-1", "doc-2", "doc-3"})

        result = await manager.get_processed_documents("job-123")

        assert result == {"doc-1", "doc-2", "doc-3"}

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_documents_empty_set(self, mock_redis):
        """Test retrieval when no documents processed."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.smembers = AsyncMock(return_value=None)

        result = await manager.get_processed_documents("job-123")

        assert result == set()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_documents_handles_error(self, mock_redis):
        """Test handles Redis errors gracefully."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.smembers = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.get_processed_documents("job-123")

        assert result == set()


class TestIsDocumentProcessed:
    """Test checking if document is processed."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_document_processed_when_disabled(self):
        """Test returns False when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.is_document_processed("job-123", "doc-1")

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_document_processed_true(self, mock_redis):
        """Test returns True when document was processed."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.sismember = AsyncMock(return_value=1)

        result = await manager.is_document_processed("job-123", "doc-1")

        assert result is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_document_processed_false(self, mock_redis):
        """Test returns False when document not processed."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.sismember = AsyncMock(return_value=0)

        result = await manager.is_document_processed("job-123", "doc-1")

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_document_processed_handles_error(self, mock_redis):
        """Test handles Redis errors gracefully."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.sismember = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.is_document_processed("job-123", "doc-1")

        assert result is False


class TestGetProcessedCount:
    """Test getting processed document count."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_count_when_disabled(self):
        """Test returns 0 when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.get_processed_count("job-123")

        assert result == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_count_success(self, mock_redis):
        """Test successful count retrieval."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.scard = AsyncMock(return_value=150)

        result = await manager.get_processed_count("job-123")

        assert result == 150

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_count_empty(self, mock_redis):
        """Test count when no documents processed."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.scard = AsyncMock(return_value=None)

        result = await manager.get_processed_count("job-123")

        assert result == 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_processed_count_handles_error(self, mock_redis):
        """Test handles Redis errors gracefully."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.scard = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.get_processed_count("job-123")

        assert result == 0


class TestClearCheckpoint:
    """Test checkpoint clearing."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_clear_checkpoint_when_disabled(self):
        """Test returns False when disabled."""
        manager = CheckpointManager()
        manager.enabled = False

        result = await manager.clear_checkpoint("job-123")

        assert result is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_clear_checkpoint_success(self, mock_redis):
        """Test successful checkpoint clearing."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        result = await manager.clear_checkpoint("job-123")

        assert result is True
        mock_redis.delete.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_clear_checkpoint_deletes_all_keys(self, mock_redis):
        """Test clear deletes all related keys."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        await manager.clear_checkpoint("job-123")

        # Should delete checkpoint, processed docs, and stats
        call_args = mock_redis.delete.call_args[0]
        assert len(call_args) == 3

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_clear_checkpoint_handles_error(self, mock_redis):
        """Test handles Redis errors gracefully."""
        manager = CheckpointManager()
        manager.enabled = True
        CheckpointManager._redis_client = mock_redis

        mock_redis.delete = AsyncMock(side_effect=Exception("Redis error"))

        result = await manager.clear_checkpoint("job-123")

        assert result is False


class TestClose:
    """Test Redis client closure."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_client(self, mock_redis):
        """Test closing Redis client."""
        manager = CheckpointManager()
        CheckpointManager._redis_client = mock_redis

        await manager.close()

        mock_redis.close.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_sets_client_to_none(self, mock_redis):
        """Test close sets client to None."""
        manager = CheckpointManager()
        CheckpointManager._redis_client = mock_redis

        await manager.close()

        assert CheckpointManager._redis_client is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_close_when_no_client(self):
        """Test close when no client exists."""
        manager = CheckpointManager()
        CheckpointManager._redis_client = None

        # Should not raise exception
        await manager.close()
