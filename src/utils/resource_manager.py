"""
src/utils/resource_manager.py

Resource lifecycle manager for monitoring and cleanup.

Responsibilities:
- Monitor CPU/RAM/GPU usage
- Detect idle periods
- Provide cleanup hooks for heavy models
- Support low-resource mode
- Track resource usage per job

DESIGN PATTERN: Zero-regression approach
- Graceful degradation if psutil unavailable
- All operations wrapped in try-catch
- Fail-safe mode continues without monitoring
"""

import asyncio
import logging
import os
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

from src.schemas.job_models import ResourceUsage

logger = logging.getLogger("ingestion_service")


class ResourceManager:
    """
    Manages system resource monitoring and cleanup.

    Features:
    - CPU/RAM/GPU monitoring
    - Idle detection
    - Resource cleanup hooks
    - Per-job resource tracking
    """

    _instance: Optional['ResourceManager'] = None

    def __new__(cls):
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize resource manager (singleton)."""
        if not hasattr(self, '_initialized'):
            self._initialized = False
            self.enabled = PSUTIL_AVAILABLE

            if not self.enabled:
                logger.warning("psutil not available - resource monitoring disabled")
                return

            # Configuration
            self.idle_timeout_seconds = int(os.getenv("IDLE_TIMEOUT_SECONDS", "300"))  # 5 min
            self.cpu_threshold_percent = float(os.getenv("CPU_THRESHOLD_PERCENT", "95"))
            self.memory_threshold_percent = float(os.getenv("MEMORY_THRESHOLD_PERCENT", "90"))
            self.cleanup_on_idle = os.getenv("CLEANUP_ON_IDLE", "true").lower() == "true"

            # State tracking
            self._last_activity_time: Optional[datetime] = None
            self._active_jobs: Dict[str, datetime] = {}

            self._initialized = True
            logger.info(
                f"resource_manager_initialized: idle_timeout_seconds={self.idle_timeout_seconds}, "
                f"cleanup_on_idle={self.cleanup_on_idle}"
            )

    def get_resource_usage(self) -> ResourceUsage:
        """
        Get current resource usage.

        Returns:
            ResourceUsage object with current metrics
        """
        if not self.enabled:
            return ResourceUsage(
                cpu_percent=0.0,
                memory_percent=0.0,
                memory_used_gb=0.0,
                memory_total_gb=0.0,
                gpu_available=False
            )

        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)

            # Memory usage
            mem = psutil.virtual_memory()
            memory_percent = mem.percent
            memory_used_gb = mem.used / (1024 ** 3)  # Convert to GB
            memory_total_gb = mem.total / (1024 ** 3)

            # GPU usage (try nvidia-smi)
            gpu_available = False
            gpu_memory_used_mb = None
            gpu_memory_total_mb = None

            try:
                result = subprocess.run(
                    ["nvidia-smi", "--query-gpu=memory.used,memory.total",
                     "--format=csv,noheader,nounits"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )

                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    if lines and lines[0]:
                        parts = lines[0].split(',')
                        if len(parts) == 2:
                            gpu_memory_used_mb = float(parts[0].strip())
                            gpu_memory_total_mb = float(parts[1].strip())
                            gpu_available = True

            except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
                pass  # nvidia-smi not available or failed

            return ResourceUsage(
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_gb=memory_used_gb,
                memory_total_gb=memory_total_gb,
                gpu_available=gpu_available,
                gpu_memory_used_mb=gpu_memory_used_mb,
                gpu_memory_total_mb=gpu_memory_total_mb,
                timestamp=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"failed_to_get_resource_usage: {e}")
            return ResourceUsage(
                cpu_percent=0.0,
                memory_percent=0.0,
                memory_used_gb=0.0,
                memory_total_gb=0.0,
                gpu_available=False
            )

    def check_resource_warnings(self) -> Dict[str, Any]:
        """
        Check if resource usage exceeds thresholds.

        Returns:
            Dictionary with warning flags
        """
        usage = self.get_resource_usage()

        warnings = {
            "memory_high": usage.memory_percent >= self.memory_threshold_percent,
            "cpu_high": usage.cpu_percent >= self.cpu_threshold_percent,
            "memory_percent": usage.memory_percent,
            "cpu_percent": usage.cpu_percent
        }

        if warnings["memory_high"]:
            logger.warning(
                f"high_memory_usage: memory_percent={usage.memory_percent}, "
                f"threshold={self.memory_threshold_percent}"
            )

        if warnings["cpu_high"]:
            logger.warning(
                f"high_cpu_usage: cpu_percent={usage.cpu_percent}, "
                f"threshold={self.cpu_threshold_percent}"
            )

        return warnings

    async def record_activity(self, job_id: str, activity: str):
        """
        Record job activity to update idle detection.

        Args:
            job_id: Job identifier
            activity: Activity description
        """
        self._last_activity_time = datetime.utcnow()
        self._active_jobs[job_id] = self._last_activity_time

        logger.debug(
            f"activity_recorded: job_id={job_id}, activity={activity}"
        )

    async def is_idle(self) -> bool:
        """
        Check if system is idle (no activity for idle_timeout_seconds).

        Returns:
            True if idle
        """
        if not self._last_activity_time:
            return True

        idle_duration = (datetime.utcnow() - self._last_activity_time).total_seconds()
        return idle_duration >= self.idle_timeout_seconds

    async def cleanup_idle_resources(self):
        """
        Cleanup resources during idle period.

        This is a hook that can be called by the main application
        when idle is detected.
        """
        if not await self.is_idle():
            return

        if not self.cleanup_on_idle:
            logger.info("cleanup_on_idle_disabled_skipping")
            return

        logger.info("cleaning_up_idle_resources")

        try:
            # Clear GPU cache if available
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    logger.info("gpu_cache_cleared")
            except ImportError:
                pass

            # Clear active jobs tracking (older than idle timeout)
            cutoff_time = datetime.utcnow() - timedelta(seconds=self.idle_timeout_seconds)
            jobs_to_remove = [
                job_id for job_id, last_time in self._active_jobs.items()
                if last_time < cutoff_time
            ]

            for job_id in jobs_to_remove:
                del self._active_jobs[job_id]

            if jobs_to_remove:
                logger.info(
                    f"cleared_idle_job_tracking: jobs_cleared={len(jobs_to_remove)}"
                )

        except Exception as e:
            logger.error(f"failed_to_cleanup_idle_resources: {e}")

    async def release_resources(self):
        """
        Explicit resource release (call on job completion/pause).
        """
        try:
            # Clear GPU cache if available
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    logger.info("gpu_cache_cleared_on_release")
            except ImportError:
                pass

            logger.info("resources_released")

        except Exception as e:
            logger.error(f"failed_to_release_resources: {e}")

    @asynccontextmanager
    async def track_job_resources(self, job_id: str):
        """
        Context manager to track resources for a job.

        Usage:
            async with resource_manager.track_job_resources(job_id):
                # Process job
                pass

        Args:
            job_id: Job identifier
        """
        # Record start
        start_usage = self.get_resource_usage()
        await self.record_activity(job_id, "job_start")

        logger.info(
            f"job_resource_tracking_started: job_id={job_id}, "
            f"cpu_percent={start_usage.cpu_percent}, memory_percent={start_usage.memory_percent}"
        )

        try:
            yield self

        finally:
            # Record end and cleanup
            end_usage = self.get_resource_usage()
            await self.release_resources()

            # Calculate delta
            cpu_delta = end_usage.cpu_percent - start_usage.cpu_percent
            memory_delta = end_usage.memory_percent - start_usage.memory_percent

            logger.info(
                f"job_resource_tracking_completed: job_id={job_id}, "
                f"cpu_delta={cpu_delta}, memory_delta={memory_delta}, "
                f"final_memory_percent={end_usage.memory_percent}"
            )

            # Remove from active jobs
            if job_id in self._active_jobs:
                del self._active_jobs[job_id]


# Singleton instance
_resource_manager_instance: Optional[ResourceManager] = None


def get_resource_manager() -> ResourceManager:
    """Get singleton instance of resource manager."""
    global _resource_manager_instance

    if _resource_manager_instance is None:
        _resource_manager_instance = ResourceManager()

    return _resource_manager_instance
