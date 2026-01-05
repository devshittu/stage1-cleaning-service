"""
tests/unit/utils/test_resource_manager.py

Unit tests for ResourceManager.

Tests cover:
- Singleton pattern enforcement
- Resource usage monitoring (CPU/RAM/GPU)
- Threshold warnings
- Activity tracking and idle detection
- Resource cleanup hooks
- Job resource tracking context manager
- Graceful degradation when psutil unavailable
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.utils.resource_manager import ResourceManager, get_resource_manager
from src.schemas.job_models import ResourceUsage


class TestResourceManagerInitialization:
    """Test ResourceManager initialization."""

    @pytest.mark.unit
    def test_singleton_pattern(self):
        """Test ResourceManager is a singleton."""
        manager1 = ResourceManager()
        manager2 = ResourceManager()

        assert manager1 is manager2

    @pytest.mark.unit
    def test_get_resource_manager_returns_singleton(self):
        """Test get_resource_manager function returns singleton."""
        manager1 = get_resource_manager()
        manager2 = get_resource_manager()

        assert manager1 is manager2

    @pytest.mark.unit
    @patch('src.utils.resource_manager.PSUTIL_AVAILABLE', False)
    def test_initialization_without_psutil(self):
        """Test initialization when psutil is not available."""
        # Reset singleton for test
        ResourceManager._instance = None

        manager = ResourceManager()

        assert manager.enabled is False

    @pytest.mark.unit
    @patch('src.utils.resource_manager.PSUTIL_AVAILABLE', True)
    def test_initialization_with_psutil(self):
        """Test initialization when psutil is available."""
        # Reset singleton for test
        ResourceManager._instance = None

        manager = ResourceManager()

        assert manager.enabled is True

    @pytest.mark.unit
    def test_configuration_settings(self):
        """Test configuration settings from environment."""
        manager = ResourceManager()

        assert hasattr(manager, 'idle_timeout_seconds')
        assert hasattr(manager, 'cpu_threshold_percent')
        assert hasattr(manager, 'memory_threshold_percent')
        assert hasattr(manager, 'cleanup_on_idle')


class TestGetResourceUsage:
    """Test resource usage monitoring."""

    @pytest.mark.unit
    def test_get_resource_usage_when_disabled(self):
        """Test returns default ResourceUsage when disabled."""
        manager = ResourceManager()
        manager.enabled = False

        usage = manager.get_resource_usage()

        assert usage.cpu_percent == 0.0
        assert usage.memory_percent == 0.0
        assert usage.gpu_available is False

    @pytest.mark.unit
    @patch('src.utils.resource_manager.psutil')
    def test_get_resource_usage_success(self, mock_psutil):
        """Test successful resource usage retrieval."""
        manager = ResourceManager()
        manager.enabled = True

        # Mock CPU and memory
        mock_psutil.cpu_percent.return_value = 45.2
        mock_psutil.virtual_memory.return_value = Mock(
            percent=60.5,
            used=25 * (1024 ** 3),  # 25 GB
            total=64 * (1024 ** 3)  # 64 GB
        )

        usage = manager.get_resource_usage()

        assert usage.cpu_percent == 45.2
        assert usage.memory_percent == 60.5
        assert usage.memory_used_gb == pytest.approx(25.0, abs=0.1)
        assert usage.memory_total_gb == pytest.approx(64.0, abs=0.1)

    @pytest.mark.unit
    @patch('src.utils.resource_manager.psutil')
    @patch('src.utils.resource_manager.subprocess')
    def test_get_resource_usage_with_gpu(self, mock_subprocess, mock_psutil):
        """Test resource usage with GPU metrics."""
        manager = ResourceManager()
        manager.enabled = True

        mock_psutil.cpu_percent.return_value = 30.0
        mock_psutil.virtual_memory.return_value = Mock(
            percent=50.0,
            used=16 * (1024 ** 3),
            total=32 * (1024 ** 3)
        )

        # Mock nvidia-smi output
        mock_result = Mock(
            returncode=0,
            stdout="8192, 11264\n"
        )
        mock_subprocess.run.return_value = mock_result

        usage = manager.get_resource_usage()

        assert usage.gpu_available is True
        assert usage.gpu_memory_used_mb == 8192.0
        assert usage.gpu_memory_total_mb == 11264.0

    @pytest.mark.unit
    @patch('src.utils.resource_manager.psutil')
    @patch('src.utils.resource_manager.subprocess')
    def test_get_resource_usage_no_gpu(self, mock_subprocess, mock_psutil):
        """Test resource usage when nvidia-smi not available."""
        manager = ResourceManager()
        manager.enabled = True

        mock_psutil.cpu_percent.return_value = 30.0
        mock_psutil.virtual_memory.return_value = Mock(
            percent=50.0,
            used=16 * (1024 ** 3),
            total=32 * (1024 ** 3)
        )

        # Mock nvidia-smi failure
        mock_subprocess.run.side_effect = FileNotFoundError()

        usage = manager.get_resource_usage()

        assert usage.gpu_available is False
        assert usage.gpu_memory_used_mb is None

    @pytest.mark.unit
    @patch('src.utils.resource_manager.psutil')
    def test_get_resource_usage_handles_error(self, mock_psutil):
        """Test get_resource_usage handles errors gracefully."""
        manager = ResourceManager()
        manager.enabled = True

        mock_psutil.cpu_percent.side_effect = Exception("psutil error")

        usage = manager.get_resource_usage()

        # Should return default values on error
        assert usage.cpu_percent == 0.0
        assert usage.memory_percent == 0.0


class TestCheckResourceWarnings:
    """Test resource warning detection."""

    @pytest.mark.unit
    def test_check_warnings_below_threshold(self):
        """Test warnings when usage below thresholds."""
        manager = ResourceManager()
        manager.enabled = True
        manager.cpu_threshold_percent = 95.0
        manager.memory_threshold_percent = 90.0

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=50.0,
                memory_percent=60.0,
                memory_used_gb=16.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            warnings = manager.check_resource_warnings()

            assert warnings["memory_high"] is False
            assert warnings["cpu_high"] is False

    @pytest.mark.unit
    def test_check_warnings_memory_high(self):
        """Test warnings when memory exceeds threshold."""
        manager = ResourceManager()
        manager.enabled = True
        manager.memory_threshold_percent = 90.0

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=50.0,
                memory_percent=92.5,
                memory_used_gb=29.6,
                memory_total_gb=32.0,
                gpu_available=False
            )

            warnings = manager.check_resource_warnings()

            assert warnings["memory_high"] is True
            assert warnings["memory_percent"] == 92.5

    @pytest.mark.unit
    def test_check_warnings_cpu_high(self):
        """Test warnings when CPU exceeds threshold."""
        manager = ResourceManager()
        manager.enabled = True
        manager.cpu_threshold_percent = 95.0

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=97.5,
                memory_percent=60.0,
                memory_used_gb=16.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            warnings = manager.check_resource_warnings()

            assert warnings["cpu_high"] is True
            assert warnings["cpu_percent"] == 97.5

    @pytest.mark.unit
    def test_check_warnings_both_high(self):
        """Test warnings when both CPU and memory exceed thresholds."""
        manager = ResourceManager()
        manager.enabled = True
        manager.cpu_threshold_percent = 95.0
        manager.memory_threshold_percent = 90.0

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=96.0,
                memory_percent=91.0,
                memory_used_gb=29.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            warnings = manager.check_resource_warnings()

            assert warnings["cpu_high"] is True
            assert warnings["memory_high"] is True


class TestActivityTracking:
    """Test activity tracking."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_record_activity(self):
        """Test recording activity updates last activity time."""
        manager = ResourceManager()

        await manager.record_activity("job-123", "processing")

        assert manager._last_activity_time is not None
        assert "job-123" in manager._active_jobs

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_record_multiple_activities(self):
        """Test recording multiple activities."""
        manager = ResourceManager()

        await manager.record_activity("job-1", "start")
        await manager.record_activity("job-2", "processing")

        assert "job-1" in manager._active_jobs
        assert "job-2" in manager._active_jobs

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_record_activity_updates_timestamp(self):
        """Test recording activity updates timestamp."""
        manager = ResourceManager()

        first_time = datetime.utcnow()
        await manager.record_activity("job-123", "start")
        first_activity_time = manager._last_activity_time

        # Wait briefly and record again
        await asyncio.sleep(0.01)
        await manager.record_activity("job-123", "progress")
        second_activity_time = manager._last_activity_time

        assert second_activity_time >= first_activity_time


class TestIdleDetection:
    """Test idle detection."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_idle_no_activity(self):
        """Test is_idle returns True when no activity recorded."""
        manager = ResourceManager()
        manager._last_activity_time = None

        is_idle = await manager.is_idle()

        assert is_idle is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_idle_recent_activity(self):
        """Test is_idle returns False for recent activity."""
        manager = ResourceManager()
        manager.idle_timeout_seconds = 300

        # Record recent activity
        manager._last_activity_time = datetime.utcnow()

        is_idle = await manager.is_idle()

        assert is_idle is False

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_idle_old_activity(self):
        """Test is_idle returns True when activity is old."""
        manager = ResourceManager()
        manager.idle_timeout_seconds = 300

        # Record old activity (10 minutes ago)
        manager._last_activity_time = datetime.utcnow() - timedelta(seconds=600)

        is_idle = await manager.is_idle()

        assert is_idle is True

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_is_idle_exact_timeout(self):
        """Test is_idle at exact timeout boundary."""
        manager = ResourceManager()
        manager.idle_timeout_seconds = 300

        # Record activity exactly at timeout
        manager._last_activity_time = datetime.utcnow() - timedelta(seconds=300)

        is_idle = await manager.is_idle()

        assert is_idle is True


class TestCleanupIdleResources:
    """Test idle resource cleanup."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_cleanup_when_not_idle(self):
        """Test cleanup skipped when not idle."""
        manager = ResourceManager()
        manager._last_activity_time = datetime.utcnow()

        await manager.cleanup_idle_resources()

        # Should not perform cleanup

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_cleanup_when_disabled(self):
        """Test cleanup skipped when cleanup_on_idle is False."""
        manager = ResourceManager()
        manager.cleanup_on_idle = False
        manager._last_activity_time = datetime.utcnow() - timedelta(seconds=600)

        await manager.cleanup_idle_resources()

        # Should not perform cleanup

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_cleanup_clears_old_jobs(self):
        """Test cleanup removes old jobs from tracking."""
        manager = ResourceManager()
        manager.cleanup_on_idle = True
        manager.idle_timeout_seconds = 300

        # Add old and new jobs
        old_time = datetime.utcnow() - timedelta(seconds=600)
        new_time = datetime.utcnow()

        manager._active_jobs = {
            "old-job-1": old_time,
            "old-job-2": old_time,
            "new-job": new_time
        }

        manager._last_activity_time = old_time

        await manager.cleanup_idle_resources()

        # Old jobs should be removed, new job retained
        assert "old-job-1" not in manager._active_jobs
        assert "old-job-2" not in manager._active_jobs
        assert "new-job" in manager._active_jobs

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_cleanup_with_torch_available(self):
        """Test cleanup with PyTorch GPU cache clearing."""
        manager = ResourceManager()
        manager.cleanup_on_idle = True
        manager._last_activity_time = datetime.utcnow() - timedelta(seconds=600)

        # Mock torch module
        mock_torch = Mock()
        mock_torch.cuda.is_available.return_value = True
        mock_torch.cuda.empty_cache = Mock()
        
        with patch.dict('sys.modules', {'torch': mock_torch}):

            await manager.cleanup_idle_resources()

            mock_torch.cuda.empty_cache.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_cleanup_without_torch(self):
        """Test cleanup when torch not available."""
        manager = ResourceManager()
        manager.cleanup_on_idle = True
        manager._last_activity_time = datetime.utcnow() - timedelta(seconds=600)

        # Should not raise ImportError
        await manager.cleanup_idle_resources()


class TestReleaseResources:
    """Test explicit resource release."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_release_resources(self):
        """Test basic resource release."""
        manager = ResourceManager()

        # Should not raise exception
        await manager.release_resources()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_release_with_torch_available(self):
        """Test release with PyTorch GPU cache clearing."""
        manager = ResourceManager()

        # Mock torch module
        mock_torch = Mock()
        mock_torch.cuda.is_available.return_value = True
        mock_torch.cuda.empty_cache = Mock()
        
        with patch.dict('sys.modules', {'torch': mock_torch}):

            await manager.release_resources()

            mock_torch.cuda.empty_cache.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_release_without_torch(self):
        """Test release when torch not available."""
        manager = ResourceManager()

        # Should not raise ImportError
        await manager.release_resources()


class TestTrackJobResources:
    """Test job resource tracking context manager."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_track_job_resources_records_activity(self):
        """Test context manager records activity on entry."""
        manager = ResourceManager()

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=50.0,
                memory_percent=60.0,
                memory_used_gb=16.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            async with manager.track_job_resources("job-123"):
                assert "job-123" in manager._active_jobs

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_track_job_resources_releases_on_exit(self):
        """Test context manager releases resources on exit."""
        manager = ResourceManager()

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=50.0,
                memory_percent=60.0,
                memory_used_gb=16.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            with patch.object(manager, 'release_resources') as mock_release:
                async with manager.track_job_resources("job-123"):
                    pass

                mock_release.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_track_job_resources_removes_job_on_exit(self):
        """Test context manager removes job from active tracking on exit."""
        manager = ResourceManager()

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=50.0,
                memory_percent=60.0,
                memory_used_gb=16.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            async with manager.track_job_resources("job-123"):
                assert "job-123" in manager._active_jobs

            # After context exit, job should be removed
            assert "job-123" not in manager._active_jobs

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_track_job_resources_logs_delta(self):
        """Test context manager logs resource delta."""
        manager = ResourceManager()

        start_usage = ResourceUsage(
            cpu_percent=40.0,
            memory_percent=50.0,
            memory_used_gb=16.0,
            memory_total_gb=32.0,
            gpu_available=False
        )

        end_usage = ResourceUsage(
            cpu_percent=60.0,
            memory_percent=70.0,
            memory_used_gb=22.4,
            memory_total_gb=32.0,
            gpu_available=False
        )

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.side_effect = [start_usage, end_usage]

            async with manager.track_job_resources("job-123"):
                pass

            # Should have called get_resource_usage twice
            assert mock_get.call_count == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_track_job_resources_exception_handling(self):
        """Test context manager handles exceptions properly."""
        manager = ResourceManager()

        with patch.object(manager, 'get_resource_usage') as mock_get:
            mock_get.return_value = ResourceUsage(
                cpu_percent=50.0,
                memory_percent=60.0,
                memory_used_gb=16.0,
                memory_total_gb=32.0,
                gpu_available=False
            )

            with pytest.raises(ValueError):
                async with manager.track_job_resources("job-123"):
                    raise ValueError("Test error")

            # Job should still be removed even after exception
            assert "job-123" not in manager._active_jobs
