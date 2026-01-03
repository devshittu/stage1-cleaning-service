"""
src/storage/metadata_writer.py

Stage 1 Metadata Registry Writer.

Writes cleaning job and document metadata to shared registry
for consumption by downstream stages.

Responsibilities:
- Register cleaning jobs in job_registry table
- Write cleaned document metadata to document_metadata table
- Support batch_id correlation for cross-stage tracking
- Graceful degradation if registry unavailable

DESIGN PATTERN: Zero-regression approach
- All operations wrapped in try-catch
- Fail-safe mode continues without registry
- Optional dependency (graceful degradation)
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import UUID

logger = logging.getLogger("ingestion_service")

# Try to import shared metadata registry (graceful degradation if unavailable)
try:
    from shared_metadata_registry import MetadataRegistry
    from shared_metadata_registry.models import JobRegistration
    REGISTRY_AVAILABLE = True
except ImportError:
    REGISTRY_AVAILABLE = False
    logger.info("shared_metadata_registry_not_available")


class Stage1MetadataWriter:
    """
    Writes Stage 1 cleaning metadata to shared registry.

    Enables downstream stages (Stage 2+) to query:
    - Cleaning job info
    - Cleaned document metadata
    - Processing statistics
    """

    def __init__(self):
        """Initialize metadata writer."""
        self.registry = None
        self.enabled = False

        if REGISTRY_AVAILABLE:
            try:
                self.registry = MetadataRegistry()
                self.enabled = os.getenv("METADATA_REGISTRY_ENABLED", "true").lower() == "true"
                logger.info(
                    "stage1_metadata_writer_initialized",
                    enabled=self.enabled
                )
            except Exception as e:
                logger.warning(f"failed_to_initialize_metadata_registry: {e}")
                self.registry = None
                self.enabled = False
        else:
            logger.info("metadata_registry_integration_disabled")

    async def register_job(
        self,
        job_id: UUID,
        batch_id: Optional[str] = None,
        total_documents: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Register cleaning job in shared registry.

        Args:
            job_id: Stage 1 job UUID
            batch_id: Batch identifier (for correlation across stages)
            total_documents: Total documents in batch
            metadata: Optional job metadata

        Returns:
            True if registration succeeded
        """
        if not self.enabled or not self.registry:
            return False

        try:
            job_registration = JobRegistration(
                job_id=job_id,
                batch_id=batch_id,
                stage=1,
                stage_name="cleaning",
                metadata={
                    "total_documents": total_documents,
                    "service": "stage1-cleaning-pipeline",
                    **(metadata or {})
                }
            )

            await self.registry.register_job(job_registration)

            logger.info(
                "job_registered_in_metadata_registry",
                job_id=str(job_id),
                batch_id=batch_id,
                total_documents=total_documents
            )

            return True

        except Exception as e:
            logger.error(
                "failed_to_register_job_in_metadata_registry",
                job_id=str(job_id),
                error=str(e)
            )
            return False

    async def write_document_metadata(
        self,
        job_id: UUID,
        batch_id: Optional[str],
        document_id: str,
        cleaned_data: Dict[str, Any]
    ) -> bool:
        """
        Write cleaned document metadata to registry.

        Args:
            job_id: Stage 1 job UUID
            batch_id: Batch identifier
            document_id: Document identifier
            cleaned_data: Cleaned document data

        Returns:
            True if write succeeded
        """
        if not self.enabled or not self.registry:
            return False

        try:
            # Write to document_metadata table
            await self.registry.backend.execute(
                """
                INSERT INTO document_metadata (
                    document_id, job_id, batch_id, stage, stage_name,
                    title, author, publication_date, source_url,
                    full_data, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (document_id, job_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    author = EXCLUDED.author,
                    publication_date = EXCLUDED.publication_date,
                    source_url = EXCLUDED.source_url,
                    full_data = EXCLUDED.full_data,
                    updated_at = NOW()
                """,
                document_id,
                job_id,
                batch_id,
                1,  # Stage 1
                "cleaning",
                cleaned_data.get("cleaned_title"),
                cleaned_data.get("cleaned_author"),
                cleaned_data.get("cleaned_publication_date"),
                str(cleaned_data.get("cleaned_source_url")) if cleaned_data.get("cleaned_source_url") else None,
                cleaned_data,  # Full cleaned data as JSONB
                datetime.utcnow()
            )

            return True

        except Exception as e:
            logger.error(
                "failed_to_write_document_metadata",
                document_id=document_id,
                error=str(e)
            )
            return False

    async def update_job_status(
        self,
        job_id: UUID,
        status: str,
        error_message: Optional[str] = None,
        statistics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update job status in registry.

        Args:
            job_id: Stage 1 job UUID
            status: Job status (completed, failed, etc.)
            error_message: Optional error message if failed
            statistics: Optional statistics dictionary

        Returns:
            True if update succeeded
        """
        if not self.enabled or not self.registry:
            return False

        try:
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow(),
            }

            if status == "completed":
                update_data["completed_at"] = datetime.utcnow()

            metadata_update = {}
            if error_message:
                metadata_update["error"] = error_message

            if statistics:
                metadata_update["statistics"] = statistics

            await self.registry.backend.execute(
                """
                UPDATE job_registry
                SET status = $1,
                    updated_at = $2,
                    completed_at = $3,
                    metadata = COALESCE(metadata, '{}'::jsonb) || $4::jsonb
                WHERE job_id = $5
                """,
                status,
                update_data["updated_at"],
                update_data.get("completed_at"),
                metadata_update,
                job_id
            )

            logger.info(
                "job_status_updated_in_metadata_registry",
                job_id=str(job_id),
                status=status
            )

            return True

        except Exception as e:
            logger.error(
                "failed_to_update_job_status_in_metadata_registry",
                job_id=str(job_id),
                error=str(e)
            )
            return False


# Singleton instance
_metadata_writer_instance: Optional[Stage1MetadataWriter] = None


def get_stage1_metadata_writer() -> Stage1MetadataWriter:
    """Get singleton instance of Stage 1 metadata writer."""
    global _metadata_writer_instance

    if _metadata_writer_instance is None:
        _metadata_writer_instance = Stage1MetadataWriter()

    return _metadata_writer_instance


# Sync wrappers for Celery compatibility

def sync_register_job(
    job_id: UUID,
    batch_id: Optional[str] = None,
    total_documents: int = 0,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Sync wrapper for register_job().

    For use in Celery tasks and other synchronous contexts.
    """
    writer = get_stage1_metadata_writer()

    if not writer.enabled:
        return False

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(
        writer.register_job(
            job_id=job_id,
            batch_id=batch_id,
            total_documents=total_documents,
            metadata=metadata
        )
    )


def sync_write_document_metadata(
    job_id: UUID,
    batch_id: Optional[str],
    document_id: str,
    cleaned_data: Dict[str, Any]
) -> bool:
    """
    Sync wrapper for write_document_metadata().

    For use in Celery tasks.
    """
    writer = get_stage1_metadata_writer()

    if not writer.enabled:
        return False

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(
        writer.write_document_metadata(
            job_id=job_id,
            batch_id=batch_id,
            document_id=document_id,
            cleaned_data=cleaned_data
        )
    )


def sync_update_job_status(
    job_id: UUID,
    status: str,
    error_message: Optional[str] = None,
    statistics: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Sync wrapper for update_job_status().

    For use in Celery tasks.
    """
    writer = get_stage1_metadata_writer()

    if not writer.enabled:
        return False

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(
        writer.update_job_status(
            job_id=job_id,
            status=status,
            error_message=error_message,
            statistics=statistics
        )
    )
