"""
tests/integration/celery/test_celery_tasks.py

Integration tests for Celery tasks.

Tests cover:
- Single article processing task
- Batch processing task
- Retry logic and error handling
- Integration with JobManager/CheckpointManager
- CloudEvents publishing
- Progressive persistence
"""

import pytest
import json
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import date

from src.celery_app import preprocess_article_task, process_batch_task
from src.schemas.data_models import ArticleInput
from src.schemas.job_models import JobStatus


@pytest.fixture
def sample_article_json():
    """Sample article data as JSON string."""
    article = {
        "document_id": "test-doc-123",
        "text": "This is a test article with some content.",
        "title": "Test Article",
        "publication_date": "2024-01-15"
    }
    return json.dumps(article)


@pytest.fixture
def sample_documents_json():
    """Sample documents batch as JSON string."""
    documents = [
        {
            "document_id": f"doc-{i}",
            "text": f"Article {i} content",
            "title": f"Article {i}"
        }
        for i in range(5)
    ]
    return json.dumps(documents)


class TestPreprocessArticleTask:
    """Test single article preprocessing task."""

    @pytest.mark.integration
    def test_preprocess_article_success(self, sample_article_json):
        """Test successful article preprocessing."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor:
            mock_result = Mock()
            mock_result.document_id = "test-doc-123"
            mock_result.cleaned_text = "This is cleaned text"
            mock_result.statistics = {"words": 10}

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            # Create mock request for bound task
            with patch.object(preprocess_article_task, 'request') as mock_request:
                mock_request.id = "task-123"
                mock_request.retries = 0

                result = preprocess_article_task.run(sample_article_json)

                assert result is not None
                mock_preprocessor.preprocess.assert_called_once()

    @pytest.mark.integration
    def test_preprocess_with_custom_config(self, sample_article_json):
        """Test preprocessing with custom cleaning config."""
        custom_config = json.dumps({"remove_html_tags": False})

        with patch('src.celery_app.preprocessor') as mock_preprocessor:
            mock_result = Mock()
            mock_result.document_id = "test-doc-123"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            with patch.object(preprocess_article_task, 'request') as mock_request:
                mock_request.id = "task-123"
                mock_request.retries = 0


            result = preprocess_article_task(
                sample_article_json,
                custom_cleaning_config_json=custom_config
            )

            assert result is not None

    @pytest.mark.integration
    def test_preprocess_invalid_json(self):
        """Test preprocessing handles invalid JSON gracefully."""
        invalid_json = "not valid json"


        with pytest.raises(Exception):
            preprocess_article_task.run( invalid_json)

    @pytest.mark.integration
    def test_preprocess_preprocessor_error(self, sample_article_json):
        """Test preprocessing handles preprocessor errors."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor:
            mock_preprocessor.preprocess = Mock(side_effect=Exception("Processing error"))

            mock_self.retry = Mock(side_effect=Exception("Retry failed"))

            with pytest.raises(Exception):
                preprocess_article_task.run( sample_article_json)


class TestProcessBatchTask:
    """Test batch processing task."""

    @pytest.mark.integration
    def test_process_batch_basic(self, sample_documents_json):
        """Test basic batch processing."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.MANAGERS_AVAILABLE', False):

            mock_result = Mock()
            mock_result.document_id = "doc-1"
            mock_result.cleaned_text = "Cleaned"
            mock_result.statistics = {}

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            result = process_batch_task.run(
                                    job_id="job-123",
                batch_id="batch-456",
                documents_json=sample_documents_json
            )

            assert result is not None
            assert "status" in result or "documents_processed" in result

    @pytest.mark.integration
    def test_process_batch_invalid_json(self):
        """Test batch processing handles invalid JSON."""
            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


        result = process_batch_task.run(
                                job_id="job-123",
            batch_id=None,
            documents_json="invalid json"
        )

        assert result["status"] == "failed"
        assert "error" in result

    @pytest.mark.integration
    def test_process_batch_with_checkpoint_interval(self, sample_documents_json):
        """Test batch processing with custom checkpoint interval."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.MANAGERS_AVAILABLE', False):

            mock_result = Mock()
            mock_result.document_id = "doc-1"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            result = process_batch_task.run(
                                    job_id="job-123",
                batch_id=None,
                documents_json=sample_documents_json,
                checkpoint_interval=2  # Checkpoint every 2 documents
            )

            assert result is not None

    @pytest.mark.integration
    def test_process_batch_with_job_manager(self, sample_documents_json):
        """Test batch processing with JobManager integration."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.MANAGERS_AVAILABLE', True), \
             patch('src.celery_app.get_job_manager') as mock_get_manager:

            mock_result = Mock()
            mock_result.document_id = "doc-1"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            # Mock job manager
            mock_manager = Mock()
            mock_job = Mock()
            mock_job.status = JobStatus.RUNNING

            mock_manager.get_job = AsyncMock(return_value=mock_job)
            mock_manager.update_job_status = AsyncMock(return_value=True)
            mock_manager.update_job_progress = AsyncMock(return_value=True)

            mock_get_manager.return_value = mock_manager

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            with patch('src.celery_app.run_async_safe') as mock_async:
                # Make run_async_safe execute synchronously for test
                mock_async.side_effect = lambda coro: None

                result = process_batch_task.run(
                                        job_id="job-123",
                    batch_id=None,
                    documents_json=sample_documents_json
                )

                assert result is not None

    @pytest.mark.integration
    def test_process_batch_with_persist_backends(self, sample_documents_json):
        """Test batch processing with storage backend persistence."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.MANAGERS_AVAILABLE', False), \
             patch('src.celery_app.StorageBackendFactory') as mock_factory:

            mock_result = Mock()
            mock_result.document_id = "doc-1"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            # Mock storage backend
            mock_backend = Mock()
            mock_backend.save = Mock()
            mock_backend.close = Mock()

            mock_factory.create_backends.return_value = [mock_backend]

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            result = process_batch_task.run(
                                    job_id="job-123",
                batch_id=None,
                documents_json=sample_documents_json,
                persist_to_backends=["jsonl"]
            )

            assert result is not None

    @pytest.mark.integration
    def test_process_batch_empty_documents(self):
        """Test batch processing with empty documents list."""
            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


        result = process_batch_task.run(
                                job_id="job-123",
            batch_id=None,
            documents_json="[]"
        )

        # Should handle empty list gracefully
        assert result is not None


class TestTaskRetryLogic:
    """Test task retry logic and error handling."""

    @pytest.mark.integration
    def test_preprocess_retry_on_transient_error(self, sample_article_json):
        """Test task retries on transient errors."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor:
            # Simulate transient error
            mock_preprocessor.preprocess = Mock(side_effect=Exception("Transient error"))

            mock_self.retry = Mock(side_effect=Exception("Retrying"))

            with pytest.raises(Exception):
                preprocess_article_task.run( sample_article_json)

            # Should attempt retry
            mock_self.retry.assert_called_once()

    @pytest.mark.integration
    def test_preprocess_max_retries_exceeded(self, sample_article_json):
        """Test task behavior when max retries exceeded."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor:
            mock_preprocessor.preprocess = Mock(side_effect=Exception("Permanent error"))

            mock_self.retry = Mock(side_effect=Exception("No more retries"))

            with pytest.raises(Exception):
                preprocess_article_task.run( sample_article_json)


class TestTaskIntegration:
    """Test task integration with infrastructure components."""

    @pytest.mark.integration
    def test_batch_with_checkpoint_manager(self, sample_documents_json):
        """Test batch processing with CheckpointManager integration."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.MANAGERS_AVAILABLE', True), \
             patch('src.celery_app.get_checkpoint_manager') as mock_get_checkpoint:

            mock_result = Mock()
            mock_result.document_id = "doc-1"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            # Mock checkpoint manager
            mock_checkpoint = Mock()
            mock_checkpoint.load_checkpoint = AsyncMock(return_value=None)
            mock_checkpoint.save_checkpoint = AsyncMock(return_value=True)
            mock_checkpoint.get_processed_documents = AsyncMock(return_value=set())

            mock_get_checkpoint.return_value = mock_checkpoint

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            with patch('src.celery_app.run_async_safe') as mock_async:
                mock_async.side_effect = lambda coro: None

                result = process_batch_task.run(
                                        job_id="job-123",
                    batch_id=None,
                    documents_json=sample_documents_json
                )

                assert result is not None

    @pytest.mark.integration
    def test_batch_with_resource_manager(self, sample_documents_json):
        """Test batch processing with ResourceManager integration."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.MANAGERS_AVAILABLE', True), \
             patch('src.celery_app.get_resource_manager') as mock_get_resource:

            mock_result = Mock()
            mock_result.document_id = "doc-1"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            # Mock resource manager
            mock_resource = Mock()
            mock_resource.record_activity = AsyncMock()
            mock_resource.get_resource_usage = Mock(return_value=Mock(
                cpu_percent=50.0,
                memory_percent=60.0
            ))

            mock_get_resource.return_value = mock_resource

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            with patch('src.celery_app.run_async_safe') as mock_async:
                mock_async.side_effect = lambda coro: None

                result = process_batch_task.run(
                                        job_id="job-123",
                    batch_id=None,
                    documents_json=sample_documents_json
                )

                assert result is not None

    @pytest.mark.integration
    def test_batch_with_event_publisher(self, sample_documents_json):
        """Test batch processing with CloudEvents publishing."""
        with patch('src.celery_app.preprocessor') as mock_preprocessor, \
             patch('src.celery_app.EVENTS_AVAILABLE', True), \
             patch('src.celery_app.get_event_publisher') as mock_get_publisher:

            mock_result = Mock()
            mock_result.document_id = "doc-1"

            mock_preprocessor.preprocess = Mock(return_value=mock_result)

            # Mock event publisher
            mock_publisher = Mock()
            mock_publisher.publish = AsyncMock(return_value={"published": True})

            mock_get_publisher.return_value = mock_publisher

            with patch.object(process_batch_task, 'request') as mock_request:
                mock_request.id = "task-123"


            with patch('src.celery_app.run_async_safe') as mock_async:
                mock_async.side_effect = lambda coro: None

                result = process_batch_task.run(
                                        job_id="job-123",
                    batch_id=None,
                    documents_json=sample_documents_json
                )

                assert result is not None
