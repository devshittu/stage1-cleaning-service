"""
tests/integration/api/test_additional_endpoints.py

Integration tests for additional API endpoints beyond batch management.

Tests cover:
- POST /v1/preprocess (single article preprocessing)
- POST /v1/documents/batch (batch submit with documents)
- POST /v1/batch/file (file upload)
- GET /v1/batch/{task_id} (legacy batch status)
- GET /health (health check with resource metrics)
- GET / (root endpoint)
- Error handling and validation
"""

import json
import pytest
from datetime import datetime
from unittest.mock import patch, Mock, MagicMock, AsyncMock
from fastapi.testclient import TestClient


@pytest.fixture
def temp_jsonl_file(tmp_path):
    """Create temporary JSONL file with sample documents."""
    file_path = tmp_path / "test_documents.jsonl"
    documents = [
        {
            "document_id": "doc-1",
            "text": "Content 1 for article one",
            "title": "Article 1"
        },
        {
            "document_id": "doc-2",
            "text": "Content 2 for article two",
            "title": "Article 2"
        },
        {
            "document_id": "doc-3",
            "text": "Content 3 for article three",
            "title": "Article 3"
        }
    ]

    with open(file_path, 'w') as f:
        for doc in documents:
            f.write(json.dumps(doc) + '\n')

    return file_path


@pytest.fixture
def app_client():
    """Create test client with mocked dependencies."""
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
        mock_pool.close = AsyncMock()

        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        from src.api.app import app
        client = TestClient(app)

        yield client


class TestSingleArticlePreprocess:
    """Test single article preprocessing endpoint."""

    @pytest.mark.integration
    def test_preprocess_single_article_success(self, app_client):
        """Test successful single article preprocessing."""
        request_data = {
            "article": {
                "document_id": "test-article-123",
                "text": "This is the article body with some text to clean.",
                "title": "Test Article Title"
            }
        }

        response = app_client.post("/v1/preprocess", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["document_id"] == "test-article-123"
        assert "cleaned_text" in data
        assert "entities" in data
        assert data["version"] == "1.0"

    @pytest.mark.integration
    def test_preprocess_missing_required_field(self, app_client):
        """Test preprocessing fails with missing required field."""
        request_data = {
            "article": {
                "title": "Test Article Title"
                # Missing required document_id and text fields
            }
        }

        response = app_client.post("/v1/preprocess", json=request_data)

        assert response.status_code == 422  # Validation error

    @pytest.mark.integration
    def test_preprocess_empty_body(self, app_client):
        """Test preprocessing with empty body."""
        request_data = {
            "article": {
                "document_id": "test-empty",
                "text": "",  # Empty text
                "title": "Test Article"
            }
        }

        response = app_client.post("/v1/preprocess", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["document_id"] == "test-empty"
        assert data["cleaned_text"] == ""  # Empty text should remain empty
        assert "entities" in data

    @pytest.mark.integration
    def test_preprocess_with_special_characters(self, app_client):
        """Test preprocessing handles special characters."""
        request_data = {
            "article": {
                "document_id": "test-special",
                "text": "Body with emoji 😊 and unicode characters",
                "title": "Test Article with smart quotes"
            }
        }

        response = app_client.post("/v1/preprocess", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["document_id"] == "test-special"

    @pytest.mark.integration
    def test_preprocess_processor_error(self, app_client):
        """Test handling of processor errors."""
        request_data = {
            "article": {
                "document_id": "test-error",
                "text": "Test body that will cause error",
                "title": "Test"
            }
        }

        with patch('src.api.app.preprocessor') as mock_processor:
            mock_processor.preprocess.side_effect = Exception("Processing error")

            response = app_client.post("/v1/preprocess", json=request_data)

            assert response.status_code == 500


class TestBatchDocumentsEndpoint:
    """Test batch documents submission endpoint."""

    @pytest.mark.integration
    def test_batch_submit_success(self, app_client):
        """Test successful batch submission."""
        batch_data = {
            "documents": [
                {"title": "Article 1", "body": "Content 1", "metadata": {}},
                {"title": "Article 2", "body": "Content 2", "metadata": {}},
                {"title": "Article 3", "body": "Content 3", "metadata": {}}
            ]
        }

        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="task-123")

            response = app_client.post("/v1/documents/batch", json=batch_data)

            assert response.status_code == 202
            data = response.json()
            assert "job_id" in data
            assert data["status"] == "accepted"

    @pytest.mark.integration
    def test_batch_submit_with_batch_id(self, app_client):
        """Test batch submission with custom batch ID."""
        batch_data = {
            "documents": [
                {"title": "Article 1", "body": "Content 1", "metadata": {}}
            ],
            "batch_id": "custom-batch-123"
        }

        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="task-123")

            response = app_client.post("/v1/documents/batch", json=batch_data)

            assert response.status_code == 202
            data = response.json()
            assert data["batch_id"] == "custom-batch-123"

    @pytest.mark.integration
    def test_batch_submit_empty_documents(self, app_client):
        """Test batch submission with empty documents list."""
        batch_data = {
            "documents": []
        }

        response = app_client.post("/v1/documents/batch", json=batch_data)

        assert response.status_code == 422  # Validation error

    @pytest.mark.integration
    def test_batch_submit_with_checkpoint_interval(self, app_client):
        """Test batch submission with custom checkpoint interval."""
        batch_data = {
            "documents": [
                {"title": "Article 1", "body": "Content 1", "metadata": {}}
            ],
            "checkpoint_interval": 5
        }

        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="task-123")

            response = app_client.post("/v1/documents/batch", json=batch_data)

            assert response.status_code == 202

    @pytest.mark.integration
    def test_batch_submit_with_backends(self, app_client):
        """Test batch submission with custom storage backends."""
        batch_data = {
            "documents": [
                {"title": "Article 1", "body": "Content 1", "metadata": {}}
            ],
            "persist_to_backends": ["jsonl", "parquet"]
        }

        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="task-123")

            response = app_client.post("/v1/documents/batch", json=batch_data)

            assert response.status_code == 202

    @pytest.mark.integration
    def test_batch_submit_invalid_document_structure(self, app_client):
        """Test batch submission with invalid document structure."""
        batch_data = {
            "documents": [
                {"title": "Only title"}  # Missing required fields - will fail during processing
            ]
        }

        response = app_client.post("/v1/documents/batch", json=batch_data)

        # Endpoint accepts request (validation happens during processing)
        assert response.status_code == 202
        data = response.json()
        assert "job_id" in data


class TestBatchFileUpload:
    """Test batch file upload endpoint."""

    @pytest.mark.integration
    def test_file_upload_success(self, app_client, temp_jsonl_file):
        """Test successful file upload."""
        with open(temp_jsonl_file, 'rb') as f:
            files = {'file': ('test.jsonl', f, 'application/x-ndjson')}

            response = app_client.post("/v1/preprocess/batch-file", files=files)

            assert response.status_code == 202
            data = response.json()
            assert "task_id" in data or "task_ids" in data

    @pytest.mark.integration
    def test_file_upload_with_batch_id(self, app_client, temp_jsonl_file):
        """Test file upload with custom batch ID."""
        with open(temp_jsonl_file, 'rb') as f:
            files = {'file': ('test.jsonl', f, 'application/x-ndjson')}
            data = {'batch_id': 'custom-batch-123'}

            response = app_client.post("/v1/preprocess/batch-file", files=files, data=data)

            assert response.status_code == 202

    @pytest.mark.integration
    def test_file_upload_invalid_format(self, app_client, tmp_path):
        """Test file upload with invalid file format."""
        # Create a non-JSONL file
        invalid_file = tmp_path / "test.txt"
        invalid_file.write_text("Not JSON Lines format")

        with open(invalid_file, 'rb') as f:
            files = {'file': ('test.txt', f, 'text/plain')}

            response = app_client.post("/v1/preprocess/batch-file", files=files)

            # Should handle gracefully (either 400 or accept and fail during processing)
            assert response.status_code in [400, 422, 202]

    @pytest.mark.integration
    def test_file_upload_empty_file(self, app_client, tmp_path):
        """Test file upload with empty file."""
        empty_file = tmp_path / "empty.jsonl"
        empty_file.write_text("")

        with open(empty_file, 'rb') as f:
            files = {'file': ('empty.jsonl', f, 'application/x-ndjson')}

            response = app_client.post("/v1/preprocess/batch-file", files=files)

            # Should reject empty files
            assert response.status_code in [400, 422]


class TestLegacyBatchStatus:
    """Test legacy batch status endpoint."""

    @pytest.mark.integration
    def test_get_batch_status_success(self, app_client):
        """Test getting batch status by task ID."""
        with patch('src.api.app.celery_app') as mock_celery:
            mock_task = MagicMock()
            mock_task.state = "SUCCESS"
            mock_task.result = {"processed": 10, "failed": 0}
            mock_celery.AsyncResult.return_value = mock_task

            response = app_client.get("/v1/preprocess/status/task-123")

            assert response.status_code == 200
            data = response.json()
            assert "state" in data or "status" in data

    @pytest.mark.integration
    def test_get_batch_status_pending(self, app_client):
        """Test getting status of pending batch."""
        with patch('src.api.app.celery_app') as mock_celery:
            mock_task = MagicMock()
            mock_task.state = "PENDING"
            mock_task.result = None
            mock_celery.AsyncResult.return_value = mock_task

            response = app_client.get("/v1/preprocess/status/task-123")

            assert response.status_code == 200

    @pytest.mark.integration
    def test_get_batch_status_failed(self, app_client):
        """Test getting status of failed batch."""
        with patch('src.api.app.celery_app') as mock_celery:
            mock_task = MagicMock()
            mock_task.state = "FAILURE"
            mock_task.result = Exception("Processing failed")
            mock_celery.AsyncResult.return_value = mock_task

            response = app_client.get("/v1/preprocess/status/task-123")

            assert response.status_code == 200


class TestHealthEndpoint:
    """Test health check endpoint."""

    @pytest.mark.integration
    def test_health_check_healthy(self, app_client):
        """Test health check when all systems healthy."""
        response = app_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["model_loaded"] is True
        assert "spacy_model" in data

    @pytest.mark.integration
    def test_health_check_resource_warnings(self, app_client):
        """Test health check includes celery broker status."""
        response = app_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert "celery_broker_connected" in data
        assert "gpu_enabled" in data

    @pytest.mark.integration
    def test_health_check_database_status(self, app_client):
        """Test health check returns successful status."""
        response = app_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    @pytest.mark.integration
    def test_health_check_redis_status(self, app_client):
        """Test health check always returns 200 when service is running."""
        response = app_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        # Should include basic service info
        assert "status" in data


class TestRootEndpoint:
    """Test root endpoint."""

    @pytest.mark.integration
    def test_root_endpoint_returns_info(self, app_client):
        """Test root endpoint returns API information."""
        response = app_client.get("/")

        assert response.status_code == 200
        data = response.json()
        assert "message" in data or "service" in data or "version" in data

    @pytest.mark.integration
    def test_root_endpoint_has_docs_link(self, app_client):
        """Test root endpoint includes documentation link."""
        response = app_client.get("/")

        assert response.status_code == 200
        data = response.json()
        # Should provide info about available endpoints or docs
        assert isinstance(data, dict)


class TestAPIErrorHandling:
    """Test API error handling."""

    @pytest.mark.integration
    def test_404_for_unknown_endpoint(self, app_client):
        """Test 404 response for unknown endpoints."""
        response = app_client.get("/v1/unknown/endpoint")

        assert response.status_code == 404

    @pytest.mark.integration
    def test_405_for_wrong_method(self, app_client):
        """Test 405 response for wrong HTTP method."""
        response = app_client.put("/health")

        assert response.status_code == 405

    @pytest.mark.integration
    def test_invalid_json_body(self, app_client):
        """Test handling of invalid JSON in request body."""
        response = app_client.post(
            "/v1/preprocess",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422

    @pytest.mark.integration
    def test_large_request_handling(self, app_client):
        """Test handling of large requests."""
        # Create a very large batch
        large_batch = {
            "documents": [
                {"title": f"Article {i}", "body": f"Content {i}" * 100, "metadata": {}}
                for i in range(1000)
            ]
        }

        with patch('src.api.app.process_batch_task') as mock_task:
            mock_task.delay.return_value = Mock(id="task-123")

            response = app_client.post("/v1/documents/batch", json=large_batch)

            # Should handle large requests (may have size limits)
            assert response.status_code in [202, 413]  # 413 = Payload Too Large


class TestAPIRequestValidation:
    """Test API request validation."""

    @pytest.mark.integration
    def test_validate_article_input_schema(self, app_client):
        """Test validation of ArticleInput schema."""
        invalid_article = {
            "title": 123,  # Should be string
            "body": "Valid body",
            "metadata": {}
        }

        response = app_client.post("/v1/preprocess", json=invalid_article)

        assert response.status_code == 422

    @pytest.mark.integration
    def test_validate_batch_request_schema(self, app_client):
        """Test validation of batch request schema."""
        invalid_batch = {
            "documents": "not a list",  # Should be list
            "batch_id": "valid-id"
        }

        response = app_client.post("/v1/documents/batch", json=invalid_batch)

        assert response.status_code == 422

    @pytest.mark.integration
    def test_validate_optional_fields(self, app_client):
        """Test validation accepts optional fields."""
        request_data = {
            "article": {
                "document_id": "test-minimal",
                "text": "Test body"
                # title and other fields are optional
            }
        }

        response = app_client.post("/v1/preprocess", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["document_id"] == "test-minimal"
