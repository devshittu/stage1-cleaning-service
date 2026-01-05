"""
tests/e2e/cli/test_batch_commands.py

End-to-end tests for CLI batch management commands.

Tests cover:
- batch submit command with file input
- batch status command with real-time updates
- batch pause/resume/cancel commands
- batch list command with filters
- batch watch command for progress monitoring
- Error handling and validation
"""

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
from click.testing import CliRunner

from src.cli.batch_commands import (
    submit_batch,
    get_status,
    pause_job,
    resume_job,
    cancel_job,
    list_jobs,
    watch_job
)


@pytest.fixture
def cli_runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_jsonl_file(tmp_path):
    """Create temporary JSONL file with sample documents."""
    file_path = tmp_path / "test_documents.jsonl"
    documents = [
        {"title": "Article 1", "body": "Content 1", "date": "2024-01-01"},
        {"title": "Article 2", "body": "Content 2", "date": "2024-01-02"},
        {"title": "Article 3", "body": "Content 3", "date": "2024-01-03"}
    ]

    with open(file_path, 'w') as f:
        for doc in documents:
            f.write(json.dumps(doc) + '\n')

    return str(file_path)


@pytest.fixture
def mock_successful_response():
    """Mock successful API response."""
    mock_response = Mock()
    mock_response.status_code = 202
    mock_response.json.return_value = {
        "status": "accepted",
        "job_id": "test-job-123",
        "batch_id": "test-batch-123",
        "message": "Batch job submitted successfully"
    }
    return mock_response


@pytest.fixture
def mock_job_status_response():
    """Mock job status API response."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "job_id": "test-job-123",
        "batch_id": "test-batch-123",
        "status": "running",
        "progress_percent": 45.5,
        "total_documents": 100,
        "processed_documents": 45,
        "failed_documents": 0,
        "created_at": "2024-01-01T10:00:00",
        "started_at": "2024-01-01T10:00:05"
    }
    return mock_response


class TestBatchSubmitCommand:
    """Test batch submit CLI command."""

    @pytest.mark.e2e
    def test_submit_batch_success(self, cli_runner, temp_jsonl_file, mock_successful_response):
        """Test successful batch submission."""
        with patch('httpx.post', return_value=mock_successful_response):
            result = cli_runner.invoke(submit_batch, ['--file', temp_jsonl_file])

            assert result.exit_code == 0
            assert "test-job-123" in result.output
            assert "submitted successfully" in result.output.lower()

    @pytest.mark.e2e
    def test_submit_batch_with_batch_id(self, cli_runner, temp_jsonl_file, mock_successful_response):
        """Test batch submission with custom batch ID."""
        with patch('httpx.post', return_value=mock_successful_response):
            result = cli_runner.invoke(
                submit_batch,
                ['--file', temp_jsonl_file, '--batch-id', 'custom-batch-123']
            )

            assert result.exit_code == 0
            assert "test-job-123" in result.output

    @pytest.mark.e2e
    def test_submit_batch_with_checkpoint_interval(self, cli_runner, temp_jsonl_file, mock_successful_response):
        """Test batch submission with custom checkpoint interval."""
        with patch('httpx.post', return_value=mock_successful_response):
            result = cli_runner.invoke(
                submit_batch,
                ['--file', temp_jsonl_file, '--checkpoint-interval', '5']
            )

            assert result.exit_code == 0

    @pytest.mark.e2e
    def test_submit_batch_with_multiple_backends(self, cli_runner, temp_jsonl_file, mock_successful_response):
        """Test batch submission with multiple storage backends."""
        with patch('httpx.post', return_value=mock_successful_response):
            result = cli_runner.invoke(
                submit_batch,
                ['--file', temp_jsonl_file, '--backends', 'jsonl', '--backends', 'parquet']
            )

            assert result.exit_code == 0

    @pytest.mark.e2e
    def test_submit_batch_file_not_found(self, cli_runner):
        """Test batch submission with non-existent file."""
        result = cli_runner.invoke(submit_batch, ['--file', '/nonexistent/file.jsonl'])

        assert result.exit_code != 0
        assert "does not exist" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.e2e
    def test_submit_batch_empty_file(self, cli_runner, tmp_path):
        """Test batch submission with empty JSONL file."""
        empty_file = tmp_path / "empty.jsonl"
        empty_file.write_text("")

        with patch('httpx.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 422
            mock_response.json.return_value = {"detail": "No documents provided"}
            mock_post.return_value = mock_response

            result = cli_runner.invoke(submit_batch, ['--file', str(empty_file)])

            # Should handle error gracefully
            assert "error" in result.output.lower() or "no documents" in result.output.lower()

    @pytest.mark.e2e
    def test_submit_batch_api_error(self, cli_runner, temp_jsonl_file):
        """Test batch submission when API returns error."""
        import httpx

        mock_error_response = Mock()
        mock_error_response.status_code = 500
        mock_error_response.text = "Internal server error"
        mock_error_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error",
            request=Mock(),
            response=mock_error_response
        )

        with patch('httpx.post', return_value=mock_error_response):
            result = cli_runner.invoke(submit_batch, ['--file', str(temp_jsonl_file)])

            assert result.exit_code != 0
            assert "error" in result.output.lower()


class TestBatchStatusCommand:
    """Test batch status CLI command."""

    @pytest.mark.e2e
    def test_get_status_success(self, cli_runner, mock_job_status_response):
        """Test getting job status successfully."""
        with patch('httpx.get', return_value=mock_job_status_response):
            result = cli_runner.invoke(get_status, ['--job-id', 'test-job-123'])

            assert result.exit_code == 0
            assert "test-job-123" in result.output
            assert "45.5" in result.output or "45" in result.output  # Progress

    @pytest.mark.e2e
    def test_get_status_verbose(self, cli_runner, mock_job_status_response):
        """Test getting job status with verbose output."""
        with patch('httpx.get', return_value=mock_job_status_response):
            result = cli_runner.invoke(get_status, ['--job-id', 'test-job-123', '--verbose'])

            assert result.exit_code == 0
            assert "test-job-123" in result.output

    @pytest.mark.e2e
    def test_get_status_job_not_found(self, cli_runner):
        """Test getting status of non-existent job."""
        import httpx

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"detail": "Job not found"}
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found",
            request=Mock(),
            response=mock_response
        )

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(get_status, ['--job-id', 'nonexistent-job'])

            assert result.exit_code != 0
            assert "not found" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.e2e
    def test_get_status_completed_job(self, cli_runner):
        """Test getting status of completed job."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "job_id": "test-job-123",
            "status": "completed",
            "progress_percent": 100.0,
            "total_documents": 100,
            "processed_documents": 100,
            "failed_documents": 0
        }

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(get_status, ['--job-id', 'test-job-123'])

            assert result.exit_code == 0
            assert "completed" in result.output.lower()
            assert "100" in result.output


class TestBatchPauseCommand:
    """Test batch pause CLI command."""

    @pytest.mark.e2e
    def test_pause_job_success(self, cli_runner):
        """Test pausing a running job."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "message": "Job paused successfully"
        }

        with patch('httpx.patch', return_value=mock_response):
            result = cli_runner.invoke(pause_job, ['--job-id', 'test-job-123'])

            assert result.exit_code == 0
            assert "paused" in result.output.lower() or "success" in result.output.lower()

    @pytest.mark.e2e
    def test_pause_already_paused_job(self, cli_runner):
        """Test pausing an already paused job."""
        import httpx

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "detail": "Job is already paused"
        }
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "400 Bad Request",
            request=Mock(),
            response=mock_response
        )

        with patch('httpx.patch', return_value=mock_response):
            result = cli_runner.invoke(pause_job, ['--job-id', 'test-job-123'])

            assert result.exit_code != 0
            assert "already paused" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.e2e
    def test_pause_completed_job(self, cli_runner):
        """Test pausing a completed job (should fail)."""
        import httpx

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "detail": "Cannot pause completed job"
        }
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "400 Bad Request",
            request=Mock(),
            response=mock_response
        )

        with patch('httpx.patch', return_value=mock_response):
            result = cli_runner.invoke(pause_job, ['--job-id', 'test-job-123'])

            assert result.exit_code != 0
            assert "error" in result.output.lower() or "cannot" in result.output.lower()


class TestBatchResumeCommand:
    """Test batch resume CLI command."""

    @pytest.mark.e2e
    def test_resume_job_success(self, cli_runner):
        """Test resuming a paused job."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "message": "Job resumed successfully"
        }

        with patch('httpx.patch', return_value=mock_response):
            result = cli_runner.invoke(resume_job, ['--job-id', 'test-job-123'])

            assert result.exit_code == 0
            assert "resumed" in result.output.lower() or "success" in result.output.lower()

    @pytest.mark.e2e
    def test_resume_running_job(self, cli_runner):
        """Test resuming an already running job."""
        import httpx

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "detail": "Job is already running"
        }
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "400 Bad Request",
            request=Mock(),
            response=mock_response
        )

        with patch('httpx.patch', return_value=mock_response):
            result = cli_runner.invoke(resume_job, ['--job-id', 'test-job-123'])

            assert result.exit_code != 0
            assert "error" in result.output.lower()


class TestBatchCancelCommand:
    """Test batch cancel CLI command."""

    @pytest.mark.e2e
    def test_cancel_job_success(self, cli_runner):
        """Test cancelling a job."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "message": "Job cancelled successfully"
        }
        mock_response.raise_for_status = Mock()  # Don't raise for success

        with patch('httpx.delete', return_value=mock_response):
            result = cli_runner.invoke(cancel_job, ['--job-id', 'test-job-123'], input='y\n')

            assert result.exit_code == 0
            assert "cancel" in result.output.lower()

    @pytest.mark.e2e
    def test_cancel_completed_job(self, cli_runner):
        """Test cancelling a completed job (should fail)."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "detail": "Cannot cancel completed job"
        }

        with patch('httpx.delete', return_value=mock_response):
            result = cli_runner.invoke(cancel_job, ['--job-id', 'test-job-123'])

            assert result.exit_code != 0


class TestBatchListCommand:
    """Test batch list CLI command."""

    @pytest.mark.e2e
    def test_list_jobs_no_filter(self, cli_runner):
        """Test listing all jobs without filters."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobs": [
                {"job_id": f"job-{i}", "status": "completed", "progress_percent": 100.0}
                for i in range(5)
            ],
            "total_count": 5
        }

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(list_jobs, [])

            assert result.exit_code == 0
            assert "job-0" in result.output
            assert "job-4" in result.output

    @pytest.mark.e2e
    def test_list_jobs_with_status_filter(self, cli_runner):
        """Test listing jobs filtered by status."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobs": [
                {"job_id": "job-1", "status": "running", "progress_percent": 50.0}
            ],
            "total_count": 1
        }

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(list_jobs, ['--status', 'running'])

            assert result.exit_code == 0
            assert "job-1" in result.output

    @pytest.mark.e2e
    def test_list_jobs_with_batch_id_filter(self, cli_runner):
        """Test listing jobs filtered by batch ID."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobs": [
                {"job_id": "job-1", "batch_id": "batch-123", "status": "completed"}
            ],
            "total_count": 1
        }

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(list_jobs, ['--batch-id', 'batch-123'])

            assert result.exit_code == 0
            assert "job-1" in result.output

    @pytest.mark.e2e
    def test_list_jobs_with_limit(self, cli_runner):
        """Test listing jobs with limit."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobs": [
                {"job_id": f"job-{i}", "status": "completed"}
                for i in range(10)
            ],
            "total_count": 10
        }

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(list_jobs, ['--limit', '10'])

            assert result.exit_code == 0

    @pytest.mark.e2e
    def test_list_jobs_empty_result(self, cli_runner):
        """Test listing jobs when no jobs exist."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "jobs": [],
            "total_count": 0
        }

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(list_jobs, [])

            assert result.exit_code == 0
            assert "no jobs" in result.output.lower() or "0" in result.output


class TestBatchWatchCommand:
    """Test batch watch CLI command."""

    @pytest.mark.e2e
    def test_watch_job_until_completion(self, cli_runner):
        """Test watching a job until it completes."""
        # Simulate progression: running -> running -> completed
        # Create mock responses - need to return completed status indefinitely
        call_count = [0]

        def mock_get_response(*args, **kwargs):
            call_count[0] += 1
            mock = Mock(status_code=200)
            mock.raise_for_status = Mock()

            if call_count[0] == 1:
                mock.json.return_value = {
                    "job_id": "test-job-123",
                    "status": "running",
                    "progress_percent": 33.0,
                    "processed_documents": 33,
                    "total_documents": 100
                }
            elif call_count[0] == 2:
                mock.json.return_value = {
                    "job_id": "test-job-123",
                    "status": "running",
                    "progress_percent": 66.0,
                    "processed_documents": 66,
                    "total_documents": 100
                }
            else:
                # Return completed for all subsequent calls
                mock.json.return_value = {
                    "job_id": "test-job-123",
                    "status": "COMPLETED",
                    "progress_percent": 100.0,
                    "processed_documents": 100,
                    "total_documents": 100
                }
            return mock

        with patch('httpx.get', side_effect=mock_get_response):
            with patch('time.sleep'):  # Speed up test
                result = cli_runner.invoke(watch_job, ['--job-id', 'test-job-123', '--interval', '1'])

                assert result.exit_code == 0
                assert "completed" in result.output.lower()

    @pytest.mark.e2e
    def test_watch_job_custom_interval(self, cli_runner):
        """Test watching a job with custom interval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "job_id": "test-job-123",
            "status": "completed",
            "progress_percent": 100.0
        }

        with patch('httpx.get', return_value=mock_response):
            with patch('time.sleep'):
                result = cli_runner.invoke(watch_job, ['--job-id', 'test-job-123', '--interval', '5'])

                assert result.exit_code == 0

    @pytest.mark.e2e
    def test_watch_job_failed(self, cli_runner):
        """Test watching a job that fails."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "job_id": "test-job-123",
            "status": "failed",
            "progress_percent": 45.0,
            "error_message": "Processing error occurred"
        }

        with patch('httpx.get', return_value=mock_response):
            with patch('time.sleep'):
                result = cli_runner.invoke(watch_job, ['--job-id', 'test-job-123'])

                assert result.exit_code == 0
                assert "failed" in result.output.lower()


class TestCLIErrorHandling:
    """Test CLI error handling and edge cases."""

    @pytest.mark.e2e
    def test_network_error_handling(self, cli_runner, temp_jsonl_file):
        """Test handling of network errors."""
        with patch('httpx.post', side_effect=ConnectionError("Network unreachable")):
            result = cli_runner.invoke(submit_batch, ['--file', temp_jsonl_file])

            assert result.exit_code != 0
            assert "error" in result.output.lower() or "network" in result.output.lower()

    @pytest.mark.e2e
    def test_timeout_handling(self, cli_runner):
        """Test handling of request timeouts."""
        import httpx

        with patch('httpx.get', side_effect=httpx.TimeoutException("Request timeout")):
            result = cli_runner.invoke(get_status, ['--job-id', 'test-job-123'])

            assert result.exit_code != 0
            assert "timeout" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.e2e
    def test_invalid_json_response(self, cli_runner):
        """Test handling of invalid JSON responses from API."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.text = "Invalid response"

        with patch('httpx.get', return_value=mock_response):
            result = cli_runner.invoke(get_status, ['--job-id', 'test-job-123'])

            assert result.exit_code != 0
