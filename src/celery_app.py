"""
src/celery_app.py

Defines the Celery application and tasks for asynchronous processing.

FIXES APPLIED:
- Fix #8: Enhanced retry logic with exponential backoff and jitter
- FIXED: Added custom_cleaning_config parameter support

ENHANCEMENTS:
- Added batch lifecycle management (process_batch_task)
- Integrated JobManager, CheckpointManager, ResourceManager
- CloudEvents publishing for inter-stage automation
- Metadata registry integration
- Progressive persistence with pause/resume support
"""

import asyncio
import json
import logging
import time
from celery import Celery
from celery import signals
from datetime import datetime
from uuid import UUID, uuid4
from src.core.processor import TextPreprocessor
from src.schemas.data_models import ArticleInput, PreprocessSingleResponse
from src.schemas.job_models import JobStatus
from src.utils.config_manager import ConfigManager
from src.storage.backends import StorageBackendFactory
from typing import Dict, Any, Optional, List

# Import infrastructure managers (with graceful degradation)
try:
    from src.utils.job_manager import get_job_manager
    from src.utils.checkpoint_manager import get_checkpoint_manager
    from src.utils.resource_manager import get_resource_manager
    MANAGERS_AVAILABLE = True
except ImportError:
    MANAGERS_AVAILABLE = False
    logger.info("infrastructure_managers_not_available")

# Import CloudEvents publisher (with graceful degradation)
try:
    from src.events import get_event_publisher, CloudEvent, EventTypes, EVENT_SOURCE
    EVENTS_AVAILABLE = True
except ImportError:
    EVENTS_AVAILABLE = False
    logger.info("cloudevents_publisher_not_available")

# Import metadata writer (with graceful degradation)
try:
    from src.storage.metadata_writer import (
        sync_register_job,
        sync_write_document_metadata,
        sync_update_job_status
    )
    METADATA_WRITER_AVAILABLE = True
except ImportError:
    METADATA_WRITER_AVAILABLE = False
    logger.info("metadata_writer_not_available")

settings = ConfigManager.get_settings()
logger = logging.getLogger("ingestion_service")


def run_async_safe(coro):
    """
    Safely run async coroutine in synchronous Celery context.

    Celery tasks run in synchronous context, but we need to call async functions
    from managers (JobManager, CheckpointManager, etc.). This uses the persistent
    event loop created during worker_process_init to avoid connection pool issues.

    Important: The event loop is reused across all async calls in a worker process,
    ensuring that connection pools (PostgreSQL, Redis) remain valid.

    Args:
        coro: Async coroutine to execute

    Returns:
        Result of the coroutine

    Raises:
        Exception: If coroutine execution fails
    """
    global _worker_event_loop

    # If no persistent loop exists (shouldn't happen), create one as fallback
    if _worker_event_loop is None or _worker_event_loop.is_closed():
        logger.warning("Persistent event loop not found. Creating new loop.")
        _worker_event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_event_loop)

    # Use the persistent event loop
    return _worker_event_loop.run_until_complete(coro)

# Initialize the Celery app with a specific name and broker/backend from settings.
celery_app = Celery(
    "ingestion_service",
    broker=settings.celery.broker_url,
    backend=settings.celery.result_backend
)

# Apply Celery configurations from settings.
celery_app.conf.update(
    task_acks_late=settings.celery.task_acks_late,
    worker_prefetch_multiplier=settings.celery.worker_prefetch_multiplier,
    worker_concurrency=settings.celery.worker_concurrency,
    task_annotations=settings.celery.task_annotations,
    # Additional configurations for better reliability
    task_reject_on_worker_lost=True,
    task_acks_on_failure_or_timeout=True,
    broker_connection_retry_on_startup=True,
)

# A global variable to hold the TextPreprocessor instance per worker process.
# It will be set by the signal handler below.
preprocessor = None

# Global event loop for worker process (reused for all async calls)
_worker_event_loop = None


@signals.worker_process_init.connect
def initialize_preprocessor(**kwargs):
    """
    This signal handler runs when each Celery worker process is initialized.
    It's the perfect place to load the heavy spaCy model to ensure a clean
    GPU context for each worker. Also sets up a persistent event loop for
    async operations to avoid connection pool issues.
    """
    global preprocessor, _worker_event_loop
    logger.info(
        "Celery worker process initializing. Loading TextPreprocessor instance.")
    preprocessor = TextPreprocessor()
    logger.info(
        "TextPreprocessor initialized successfully in Celery worker.")

    # Create a persistent event loop for this worker process
    _worker_event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_worker_event_loop)
    logger.info("Persistent event loop created for worker process.")


@signals.worker_process_shutdown.connect
def cleanup_preprocessor(**kwargs):
    """
    Signal handler for worker process shutdown.
    Properly closes TextPreprocessor resources and event loop.
    """
    global preprocessor, _worker_event_loop
    if preprocessor:
        logger.info(
            "Celery worker shutting down. Cleaning up TextPreprocessor.")
        preprocessor.close()
        preprocessor = None

    # Close the persistent event loop
    if _worker_event_loop and not _worker_event_loop.is_closed():
        logger.info("Closing persistent event loop.")
        try:
            # Cancel all pending tasks
            pending = asyncio.all_tasks(_worker_event_loop)
            for task in pending:
                task.cancel()
            # Run loop once more to handle cancellations
            if pending:
                _worker_event_loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
        except Exception as e:
            logger.warning(f"Error during event loop cleanup: {e}")
        finally:
            _worker_event_loop.close()
            _worker_event_loop = None


@celery_app.task(
    name="preprocess_article",
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 1 minute initial delay
    autoretry_for=(Exception,),  # Retry on any exception
    retry_backoff=True,  # Enable exponential backoff
    retry_backoff_max=600,  # Max 10 minutes between retries
    retry_jitter=True,  # Add randomness to prevent thundering herd
    acks_late=True,  # Acknowledge task only after completion
    reject_on_worker_lost=True  # Reject task if worker dies
)
def preprocess_article_task(
    self,
    article_data_json: str,
    custom_cleaning_config_json: Optional[str] = None
) -> Dict[str, Any]:
    """
    Celery task to preprocess a single article.
    It receives the article data as a JSON string to ensure proper serialization.
    Optionally accepts custom cleaning configuration as JSON string.
    
    Args:
        article_data_json: JSON string of article data
        custom_cleaning_config_json: Optional JSON string of custom cleaning config
        
    Returns:
        Dictionary with processed article data
    
    IMPROVEMENTS:
    - Fix #8: Exponential backoff with jitter for retries
    - Better error handling and logging
    - Automatic retry on transient failures
    - Support for custom cleaning configuration
    """
    global preprocessor
    if preprocessor is None:
        # This is a fallback in case the signal handler failed, though it
        # should not be needed with the worker_process_init signal.
        logger.warning(
            "Preprocessor not initialized in worker_process_init. Initializing within task.")
        preprocessor = TextPreprocessor()

    document_id = "unknown"  # Default for logging in case of early failure
    try:
        # Parse custom cleaning config if provided
        custom_cleaning_config = None
        if custom_cleaning_config_json:
            try:
                custom_cleaning_config = json.loads(
                    custom_cleaning_config_json)
                logger.debug(
                    f"Using custom cleaning config: {custom_cleaning_config}",
                    extra={"task_id": self.request.id}
                )
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Failed to parse custom_cleaning_config_json: {e}. Using default config.",
                    extra={"task_id": self.request.id}
                )

        # Pydantic's model_validate_json deserializes the JSON string back into a Pydantic model.
        article_input = ArticleInput.model_validate_json(article_data_json)
        document_id = article_input.document_id  # Update document_id for logging

        logger.info(
            f"Celery task {self.request.id} processing document_id={document_id}.",
            extra={
                "document_id": document_id,
                "task_id": self.request.id,
                "retry_count": self.request.retries
            }
        )

        processed_data = preprocessor.preprocess(
            document_id=article_input.document_id,
            text=article_input.text,
            title=article_input.title,
            excerpt=article_input.excerpt,
            author=article_input.author,
            publication_date=article_input.publication_date,
            revision_date=article_input.revision_date,
            source_url=article_input.source_url,
            categories=article_input.categories,
            tags=article_input.tags,
            media_asset_urls=article_input.media_asset_urls,
            geographical_data=article_input.geographical_data,
            embargo_date=article_input.embargo_date,
            sentiment=article_input.sentiment,
            word_count=article_input.word_count,
            publisher=article_input.publisher,
            additional_metadata=article_input.additional_metadata,
            custom_cleaning_config=custom_cleaning_config  # Pass custom config
        )

        response = PreprocessSingleResponse(
            document_id=document_id,
            version="1.0",
            original_text=processed_data.get("original_text", ""),
            cleaned_text=processed_data.get("cleaned_text", ""),
            cleaned_title=processed_data.get("cleaned_title"),
            cleaned_excerpt=processed_data.get("cleaned_excerpt"),
            cleaned_author=processed_data.get("cleaned_author"),
            cleaned_publication_date=processed_data.get(
                "cleaned_publication_date"),
            cleaned_revision_date=processed_data.get("cleaned_revision_date"),
            cleaned_source_url=processed_data.get("cleaned_source_url"),
            cleaned_categories=processed_data.get("cleaned_categories"),
            cleaned_tags=processed_data.get("cleaned_tags"),
            cleaned_media_asset_urls=processed_data.get(
                "cleaned_media_asset_urls"),
            cleaned_geographical_data=processed_data.get(
                "cleaned_geographical_data"),
            cleaned_embargo_date=processed_data.get("cleaned_embargo_date"),
            cleaned_sentiment=processed_data.get("cleaned_sentiment"),
            cleaned_word_count=processed_data.get("cleaned_word_count"),
            cleaned_publisher=processed_data.get("cleaned_publisher"),
            temporal_metadata=processed_data.get("temporal_metadata"),
            entities=processed_data.get("entities", []),
            cleaned_additional_metadata=processed_data.get(
                "cleaned_additional_metadata")
        )

        # Persist to storage backends (use default backends for Celery tasks)
        try:
            backends = StorageBackendFactory.get_backends()
            for backend in backends:
                backend.save(response)
        except Exception as storage_error:
            # Log storage error but don't fail the entire task
            logger.error(
                f"Failed to persist document_id={document_id} to storage backends: {storage_error}",
                exc_info=True,
                extra={
                    "document_id": document_id,
                    "task_id": self.request.id
                }
            )
            # Optionally, you can decide whether to retry on storage failures
            # For now, we log and continue

        logger.info(
            f"Celery task {self.request.id} successfully processed document_id={document_id}.",
            extra={
                "document_id": document_id,
                "task_id": self.request.id
            }
        )

        # Ensure the result is a dictionary with no Pydantic Url objects,
        # as Celery's serializer cannot handle them.
        response_dict = response.model_dump()
        if response_dict.get('cleaned_source_url') is not None:
            response_dict['cleaned_source_url'] = str(
                response_dict['cleaned_source_url'])
        if response_dict.get('cleaned_media_asset_urls') is not None:
            response_dict['cleaned_media_asset_urls'] = [
                str(url) for url in response_dict['cleaned_media_asset_urls']]

        return response_dict

    except Exception as e:
        logger.error(
            f"Celery task {self.request.id} failed for document_id={document_id}: {e}",
            exc_info=True,
            extra={
                "document_id": document_id,
                "task_id": self.request.id,
                "retry_count": self.request.retries
            }
        )

        # Check if we should retry
        if self.request.retries < self.max_retries:
            logger.info(
                f"Retrying task {self.request.id} for document_id={document_id} "
                f"(attempt {self.request.retries + 1}/{self.max_retries})",
                extra={
                    "document_id": document_id,
                    "task_id": self.request.id
                }
            )

        # Reraise the exception for Celery to handle retry logic
        raise


def _check_job_should_stop(job_id: str) -> tuple[bool, Optional[str]]:
    """
    Check if job should stop (paused or cancelled).

    Args:
        job_id: Job identifier

    Returns:
        Tuple of (should_stop, reason)
    """
    if not MANAGERS_AVAILABLE:
        return False, None

    try:
        job_manager = get_job_manager()
        job = run_async_safe(job_manager.get_job(job_id))

        if job and job.status == JobStatus.PAUSED:
            return True, "paused"
        elif job and job.status == JobStatus.CANCELLED:
            return True, "cancelled"

        return False, None

    except Exception as e:
        logger.error(f"failed_to_check_job_status: {e}")
        return False, None


@celery_app.task(
    name="process_batch",
    bind=True,
    max_retries=1,
    acks_late=True,
    reject_on_worker_lost=True
)
def process_batch_task(
    self,
    job_id: str,
    batch_id: Optional[str],
    documents_json: str,
    checkpoint_interval: Optional[int] = None,
    persist_to_backends: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Process an entire batch of documents with lifecycle management.

    Features:
    - Progressive persistence (save after each document)
    - Checkpoint support (pause/resume)
    - Resource tracking
    - CloudEvents publishing
    - Metadata registry integration

    Args:
        job_id: Job identifier (UUID string)
        batch_id: Optional batch identifier
        documents_json: JSON array of documents
        checkpoint_interval: Save checkpoint every N documents (default: 10)
        persist_to_backends: Optional list of backend names

    Returns:
        Dictionary with processing results
    """
    global preprocessor

    if preprocessor is None:
        logger.warning("Preprocessor not initialized. Initializing within task.")
        preprocessor = TextPreprocessor()

    # Parse documents
    try:
        documents_data = json.loads(documents_json)
    except json.JSONDecodeError as e:
        logger.error(f"failed_to_parse_documents_json: {e}")
        return {
            "status": "failed",
            "error": f"Invalid JSON: {e}",
            "documents_processed": 0
        }

    total_documents = len(documents_data)
    checkpoint_interval = checkpoint_interval or 10

    # Initialize managers
    job_manager = get_job_manager() if MANAGERS_AVAILABLE else None
    checkpoint_manager = get_checkpoint_manager() if MANAGERS_AVAILABLE else None
    resource_manager = get_resource_manager() if MANAGERS_AVAILABLE else None
    event_publisher = get_event_publisher() if EVENTS_AVAILABLE else None

    # Initialize managers (async)
    if job_manager:
        run_async_safe(job_manager.initialize_pool())
    if checkpoint_manager:
        run_async_safe(checkpoint_manager.initialize_client())
    if event_publisher:
        run_async_safe(event_publisher.initialize())

    start_time = time.time()
    processed_count = 0
    failed_count = 0
    processed_doc_ids = set()

    try:
        # Update job status to RUNNING
        if job_manager:
            run_async_safe(job_manager.update_job_status(
                job_id=job_id,
                status=JobStatus.RUNNING,
                celery_task_id=self.request.id
            ))

        # Register job in metadata registry
        if METADATA_WRITER_AVAILABLE:
            try:
                sync_register_job(
                    job_id=UUID(job_id),
                    batch_id=batch_id,
                    total_documents=total_documents
                )
            except Exception as e:
                logger.warning(f"failed_to_register_job_in_metadata_registry: {e}")

        # Publish job started event
        if event_publisher:
            try:
                event = CloudEvent(
                    type=EventTypes.JOB_STARTED,
                    source=EVENT_SOURCE,
                    subject=f"job/{job_id}",
                    data={
                        "job_id": job_id,
                        "batch_id": batch_id,
                        "total_documents": total_documents,
                        "checkpoint_interval": checkpoint_interval
                    }
                )
                run_async_safe(event_publisher.publish(event))
            except Exception as e:
                logger.warning(f"failed_to_publish_job_started_event: {e}")

        # Check for existing checkpoint (resume support)
        resume_from_checkpoint = False
        if checkpoint_manager:
            checkpoint = run_async_safe(checkpoint_manager.load_checkpoint(job_id))
            if checkpoint:
                processed_doc_ids = run_async_safe(
                    checkpoint_manager.get_processed_documents(job_id)
                )
                processed_count = len(processed_doc_ids)
                resume_from_checkpoint = True
                logger.info(
                    f"resuming_from_checkpoint: job_id={job_id}, processed_count={processed_count}, total_count={total_documents}"
                )

        # Get storage backends
        backends = StorageBackendFactory.get_backends(persist_to_backends)

        # Process documents with resource tracking
        if resource_manager:
            run_async_safe(resource_manager.record_activity(job_id, "batch_start"))

        for idx, doc_data in enumerate(documents_data):
            # Check if job should stop (pause/cancel)
            should_stop, reason = _check_job_should_stop(job_id)
            if should_stop:
                logger.info(
                    f"job_{reason}_detected",
                    extra={
                        "job_id": job_id,
                        "processed_count": processed_count,
                        "total_count": total_documents
                    }
                )

                # Save checkpoint
                if checkpoint_manager:
                    run_async_safe(checkpoint_manager.save_checkpoint(
                        job_id=job_id,
                        processed_count=processed_count,
                        total_count=total_documents,
                        statistics={
                            "failed_count": failed_count,
                            "checkpoint_saved_at": datetime.utcnow().isoformat()
                        }
                    ))

                # Update job status
                if job_manager:
                    run_async_safe(job_manager.update_job_status(
                        job_id=job_id,
                        status=JobStatus.PAUSED if reason == "paused" else JobStatus.CANCELLED
                    ))

                # Publish event
                if event_publisher:
                    try:
                        event_type = EventTypes.JOB_PAUSED if reason == "paused" else EventTypes.JOB_CANCELLED
                        event = CloudEvent(
                            type=event_type,
                            source=EVENT_SOURCE,
                            subject=f"job/{job_id}",
                            data={
                                "job_id": job_id,
                                "batch_id": batch_id,
                                "documents_processed": processed_count,
                                "documents_total": total_documents,
                                "checkpoint_saved": True
                            }
                        )
                        run_async_safe(event_publisher.publish(event))
                    except Exception as e:
                        logger.warning(f"failed_to_publish_{reason}_event: {e}")

                return {
                    "status": reason,
                    "job_id": job_id,
                    "documents_processed": processed_count,
                    "documents_failed": failed_count,
                    "documents_total": total_documents,
                    "checkpoint_saved": True
                }

            try:
                # Parse document
                article_input = ArticleInput.model_validate(doc_data)
                document_id = article_input.document_id

                # Skip if already processed (resume case)
                if document_id in processed_doc_ids:
                    continue

                # Process document
                processed_data = preprocessor.preprocess(
                    document_id=article_input.document_id,
                    text=article_input.text,
                    title=article_input.title,
                    excerpt=article_input.excerpt,
                    author=article_input.author,
                    publication_date=article_input.publication_date,
                    revision_date=article_input.revision_date,
                    source_url=article_input.source_url,
                    categories=article_input.categories,
                    tags=article_input.tags,
                    media_asset_urls=article_input.media_asset_urls,
                    geographical_data=article_input.geographical_data,
                    embargo_date=article_input.embargo_date,
                    sentiment=article_input.sentiment,
                    word_count=article_input.word_count,
                    publisher=article_input.publisher,
                    additional_metadata=article_input.additional_metadata
                )

                # Create response
                response = PreprocessSingleResponse(
                    document_id=document_id,
                    version="1.0",
                    original_text=processed_data.get("original_text", ""),
                    cleaned_text=processed_data.get("cleaned_text", ""),
                    cleaned_title=processed_data.get("cleaned_title"),
                    cleaned_excerpt=processed_data.get("cleaned_excerpt"),
                    cleaned_author=processed_data.get("cleaned_author"),
                    cleaned_publication_date=processed_data.get("cleaned_publication_date"),
                    cleaned_revision_date=processed_data.get("cleaned_revision_date"),
                    cleaned_source_url=processed_data.get("cleaned_source_url"),
                    cleaned_categories=processed_data.get("cleaned_categories"),
                    cleaned_tags=processed_data.get("cleaned_tags"),
                    cleaned_media_asset_urls=processed_data.get("cleaned_media_asset_urls"),
                    cleaned_geographical_data=processed_data.get("cleaned_geographical_data"),
                    cleaned_embargo_date=processed_data.get("cleaned_embargo_date"),
                    cleaned_sentiment=processed_data.get("cleaned_sentiment"),
                    cleaned_word_count=processed_data.get("cleaned_word_count"),
                    cleaned_publisher=processed_data.get("cleaned_publisher"),
                    temporal_metadata=processed_data.get("temporal_metadata"),
                    entities=processed_data.get("entities", []),
                    cleaned_additional_metadata=processed_data.get("cleaned_additional_metadata")
                )

                # Save to storage backends (progressive persistence)
                for backend in backends:
                    backend.save(response)

                # Write to metadata registry
                if METADATA_WRITER_AVAILABLE:
                    try:
                        response_dict = response.model_dump()
                        sync_write_document_metadata(
                            job_id=UUID(job_id),
                            batch_id=batch_id,
                            document_id=document_id,
                            cleaned_data=response_dict
                        )
                    except Exception as e:
                        logger.warning(f"failed_to_write_document_metadata: {e}")

                # Mark document as processed
                processed_doc_ids.add(document_id)
                processed_count += 1

                if checkpoint_manager:
                    run_async_safe(checkpoint_manager.mark_document_processed(job_id, document_id))

                # Save checkpoint periodically
                if checkpoint_manager and processed_count % checkpoint_interval == 0:
                    run_async_safe(checkpoint_manager.save_checkpoint(
                        job_id=job_id,
                        processed_count=processed_count,
                        total_count=total_documents,
                        last_processed_doc_id=document_id,
                        statistics={
                            "failed_count": failed_count
                        }
                    ))

                    # Update job progress
                    if job_manager:
                        run_async_safe(job_manager.update_job_progress(
                            job_id=job_id,
                            processed_documents=processed_count,
                            failed_documents=failed_count
                        ))

                    # Publish progress event
                    if event_publisher:
                        try:
                            event = CloudEvent(
                                type=EventTypes.JOB_PROGRESS,
                                source=EVENT_SOURCE,
                                subject=f"job/{job_id}",
                                data={
                                    "job_id": job_id,
                                    "batch_id": batch_id,
                                    "documents_processed": processed_count,
                                    "documents_total": total_documents,
                                    "progress_percent": (processed_count / total_documents * 100.0)
                                }
                            )
                            run_async_safe(event_publisher.publish(event))
                        except Exception as e:
                            logger.warning(f"failed_to_publish_progress_event: {e}")

            except Exception as e:
                failed_count += 1
                logger.error(
                    f"failed_to_process_document",
                    exc_info=True,
                    extra={
                        "document_id": doc_data.get("document_id", "unknown"),
                        "job_id": job_id,
                        "error": str(e)
                    }
                )

        # Job completed successfully
        processing_time_ms = (time.time() - start_time) * 1000

        statistics = {
            "documents_processed": processed_count,
            "documents_failed": failed_count,
            "documents_total": total_documents,
            "processing_time_ms": processing_time_ms,
            "resumed_from_checkpoint": resume_from_checkpoint
        }

        # Update job status to COMPLETED
        if job_manager:
            run_async_safe(job_manager.update_job_status(
                job_id=job_id,
                status=JobStatus.COMPLETED
            ))
            run_async_safe(job_manager.update_job_progress(
                job_id=job_id,
                processed_documents=processed_count,
                failed_documents=failed_count,
                statistics=statistics
            ))

        # Update metadata registry
        if METADATA_WRITER_AVAILABLE:
            try:
                sync_update_job_status(
                    job_id=UUID(job_id),
                    status="completed",
                    statistics=statistics
                )
            except Exception as e:
                logger.warning(f"failed_to_update_job_status_in_metadata_registry: {e}")

        # Clear checkpoint
        if checkpoint_manager:
            run_async_safe(checkpoint_manager.clear_checkpoint(job_id))

        # Release resources
        if resource_manager:
            run_async_safe(resource_manager.release_resources())

        # Publish job completed event
        if event_publisher:
            try:
                event = CloudEvent(
                    type=EventTypes.JOB_COMPLETED,
                    source=EVENT_SOURCE,
                    subject=f"job/{job_id}",
                    data={
                        "job_id": job_id,
                        "batch_id": batch_id,
                        "documents_processed": processed_count,
                        "documents_failed": failed_count,
                        "documents_total": total_documents,
                        "processing_time_ms": processing_time_ms
                    }
                )
                run_async_safe(event_publisher.publish(event))
            except Exception as e:
                logger.warning(f"failed_to_publish_job_completed_event: {e}")

        logger.info(
            f"batch_processing_completed: job_id={job_id}, processed_count={processed_count}, "
            f"failed_count={failed_count}, total_count={total_documents}, processing_time_ms={processing_time_ms}"
        )

        return {
            "status": "completed",
            "job_id": job_id,
            "batch_id": batch_id,
            "documents_processed": processed_count,
            "documents_failed": failed_count,
            "documents_total": total_documents,
            "processing_time_ms": processing_time_ms
        }

    except Exception as e:
        # Job failed
        error_message = str(e)
        logger.error(
            f"batch_processing_failed: {e}",
            exc_info=True,
            extra={"job_id": job_id}
        )

        # Update job status to FAILED
        if job_manager:
            run_async_safe(job_manager.update_job_status(
                job_id=job_id,
                status=JobStatus.FAILED,
                error_message=error_message
            ))

        # Update metadata registry
        if METADATA_WRITER_AVAILABLE:
            try:
                sync_update_job_status(
                    job_id=UUID(job_id),
                    status="failed",
                    error_message=error_message
                )
            except Exception as registry_error:
                logger.warning(f"failed_to_update_job_failure_in_metadata_registry: {registry_error}")

        # Publish job failed event
        if event_publisher:
            try:
                event = CloudEvent(
                    type=EventTypes.JOB_FAILED,
                    source=EVENT_SOURCE,
                    subject=f"job/{job_id}",
                    data={
                        "job_id": job_id,
                        "batch_id": batch_id,
                        "documents_processed": processed_count,
                        "documents_failed": failed_count,
                        "documents_total": total_documents,
                        "error_message": error_message
                    }
                )
                run_async_safe(event_publisher.publish(event))
            except Exception as event_error:
                logger.warning(f"failed_to_publish_job_failed_event: {event_error}")

        return {
            "status": "failed",
            "job_id": job_id,
            "error": error_message,
            "documents_processed": processed_count,
            "documents_failed": failed_count,
            "documents_total": total_documents
        }


# src/celery_app.py
