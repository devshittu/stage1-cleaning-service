"""
tests/conftest.py

Pytest configuration and shared fixtures for all tests.

This file provides common test fixtures, mocks, and utilities used across
unit, integration, and E2E tests. Fixtures are organized by scope and purpose.
"""

import asyncio
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from unittest.mock import Mock, MagicMock, AsyncMock

import pytest
from pydantic import BaseModel


# ============================================================================
# Test Configuration
# ============================================================================

@pytest.fixture(scope="session")
def test_data_dir():
    """Return path to test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def temp_dir():
    """Create temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ============================================================================
# Async Event Loop Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_document():
    """Sample document for testing."""
    return {
        "title": "Test Article",
        "body": "This is a test article body with some content.",
        "date": "2024-01-15",
        "domain": "test",
        "url": "https://example.com/test",
        "authors": ["Test Author"]
    }


@pytest.fixture
def sample_documents():
    """List of sample documents."""
    return [
        {
            "title": "Article 1",
            "body": "First article content.",
            "date": "2024-01-01"
        },
        {
            "title": "Article 2",
            "body": "Second article content.",
            "date": "2024-01-02"
        },
        {
            "title": "Article 3",
            "body": "Third article content.",
            "date": "2024-01-03"
        }
    ]


@pytest.fixture
def sample_jsonl_content():
    """Sample JSONL file content."""
    return '\n'.join([
        json.dumps({"title": "Article 1", "body": "Content 1", "date": "2024-01-01"}),
        json.dumps({"title": "Article 2", "body": "Content 2", "date": "2024-01-02"}),
        json.dumps({"title": "Article 3", "body": "Content 3", "date": "2024-01-03"})
    ])


@pytest.fixture
def sample_malformed_jsonl():
    """Sample malformed JSONL content for error testing."""
    return '\n'.join([
        '{"title": "Good Article", "body": "This is fine"}',
        '{"title": "Bad Article with "unescaped quotes", "body": "Will fail"}',
        '{"title": "Another Good", "body": "This works"}'
    ])


# ============================================================================
# Job and Batch Fixtures
# ============================================================================

@pytest.fixture
def sample_job_id():
    """Sample job ID."""
    return "test-job-123"


@pytest.fixture
def sample_batch_id():
    """Sample batch ID."""
    return "test-batch-456"


@pytest.fixture
def sample_job_metadata():
    """Sample job metadata."""
    return {
        "batch_id": "test-batch",
        "created_by": "pytest",
        "environment": "test",
        "_resume_data": {
            "documents_json": json.dumps([{"title": "Test", "body": "Content"}]),
            "checkpoint_interval": 10,
            "persist_to_backends": ["jsonl"]
        }
    }


@pytest.fixture
def sample_checkpoint_data():
    """Sample checkpoint data."""
    return {
        "job_id": "test-job-123",
        "progress_percent": 45.5,
        "documents_processed": 455,
        "documents_failed": 5,
        "documents_total": 1000,
        "last_checkpoint_time": datetime.utcnow().isoformat()
    }


# ============================================================================
# CloudEvents Fixtures
# ============================================================================

@pytest.fixture
def sample_cloudevent_data():
    """Sample CloudEvent data."""
    return {
        "specversion": "1.0",
        "type": "com.storytelling.cleaning.job.completed",
        "source": "stage1-cleaning-pipeline",
        "id": "test-event-123",
        "time": datetime.utcnow().isoformat(),
        "subject": "job/test-job-123",
        "datacontenttype": "application/json",
        "data": {
            "job_id": "test-job-123",
            "status": "completed",
            "documents_processed": 100
        }
    }


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)
    mock.get = AsyncMock(return_value=None)
    mock.set = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    mock.xadd = AsyncMock(return_value="1234567890-0")
    mock.xlen = AsyncMock(return_value=10)
    mock.ttl = AsyncMock(return_value=-1)
    mock.expire = AsyncMock(return_value=True)
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_postgres_pool():
    """Mock PostgreSQL connection pool."""
    mock_pool = MagicMock()

    # Mock connection context manager
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_conn.fetchval = AsyncMock(return_value=None)

    # Make acquire() return async context manager
    mock_acquire = MagicMock()
    mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire.__aexit__ = AsyncMock(return_value=None)
    mock_pool.acquire.return_value = mock_acquire

    # Make close() async
    mock_pool.close = AsyncMock()

    return mock_pool


@pytest.fixture
def mock_spacy_model():
    """Mock spaCy NLP model."""
    mock_nlp = Mock()
    mock_doc = Mock()
    mock_doc.ents = []
    mock_doc.text = "processed text"
    mock_nlp.return_value = mock_doc
    return mock_nlp


@pytest.fixture
def mock_celery_task():
    """Mock Celery task."""
    mock_task = Mock()
    mock_task.delay = Mock(return_value=Mock(id="task-123"))
    mock_task.apply_async = Mock(return_value=Mock(id="task-123"))
    return mock_task


# ============================================================================
# HTTP Client Fixtures
# ============================================================================

@pytest.fixture
def mock_httpx_client():
    """Mock HTTPX async client for webhook testing."""
    mock_client = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.text = "OK"
    mock_response.json = AsyncMock(return_value={"status": "success"})
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.close = AsyncMock()
    return mock_client


# ============================================================================
# File System Fixtures
# ============================================================================

@pytest.fixture
def temp_jsonl_file(tmp_path, sample_jsonl_content):
    """Create temporary JSONL file."""
    file_path = tmp_path / "test_input.jsonl"
    file_path.write_text(sample_jsonl_content)
    return file_path


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def test_config():
    """Sample test configuration."""
    return {
        "general": {
            "stage_name": "stage1-cleaning",
            "stage_number": 1,
            "environment": "test"
        },
        "celery": {
            "broker_url": "redis://localhost:6379/0",
            "result_backend": "redis://localhost:6379/1",
            "task_acks_late": True
        },
        "storage": {
            "backends": ["jsonl"],
            "base_output_dir": "/tmp/test_output"
        },
        "events": {
            "enabled": True,
            "backends": [
                {
                    "type": "redis_streams",
                    "enabled": True,
                    "config": {
                        "stream_name": "test:stream",
                        "max_len": 1000
                    }
                }
            ]
        },
        "resource_management": {
            "idle_timeout_seconds": 300,
            "cleanup_on_idle": True
        }
    }


# ============================================================================
# Database Setup/Teardown Fixtures
# ============================================================================

@pytest.fixture
async def mock_job_manager(mock_postgres_pool):
    """Mock JobManager with database pool."""
    from unittest.mock import patch

    with patch('src.utils.job_manager.JobManager._pool', mock_postgres_pool):
        from src.utils.job_manager import JobManager
        manager = JobManager()
        manager.enabled = True
        yield manager


# ============================================================================
# Cleanup Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup after each test."""
    yield
    # Cleanup code runs after test
    # Reset any global state if needed


# ============================================================================
# Parametrize Helpers
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers",
        "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers",
        "e2e: mark test as an end-to-end test"
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers",
        "requires_redis: mark test as requiring Redis connection"
    )
    config.addinivalue_line(
        "markers",
        "requires_postgres: mark test as requiring PostgreSQL connection"
    )
