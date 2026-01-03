# src/utils/config_manager.py
"""
utils/config_manager.py

Handles loading application settings from a YAML configuration file
using Pydantic for validation and type-hinting.
"""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
import os
import sys


class GeneralSettings(BaseModel):
    """General application settings."""
    log_level: str = Field(
        "INFO", description="Set to INFO for production readiness, DEBUG for development.")
    gpu_enabled: bool = Field(
        False, description="Set to True to leverage GPU (e.g., RTX A4000).")


class TypoCorrectionSettings(BaseModel):
    """Settings for typo correction behavior."""
    min_word_length: int = Field(
        3, description="Minimum word length to check for typos.")
    max_word_length: int = Field(
        15, description="Maximum word length to check (longer words likely proper nouns).")
    skip_capitalized_words: bool = Field(
        True, description="Skip words that start with a capital letter.")
    skip_mixed_case: bool = Field(
        True, description="Skip words with mixed case like 'iPhone'.")
    use_ner_entities: bool = Field(
        True, description="Use NER to skip entity words (CRITICAL for proper nouns).")
    confidence_threshold: float = Field(
        0.7, description="Spell checker confidence threshold (0.0-1.0).")


class CleaningPipelineSettings(BaseModel):
    """Settings for text cleaning pipeline steps."""
    remove_html_tags: bool = Field(True, description="Remove HTML tags.")
    normalize_whitespace: bool = Field(True, description="Normalize whitespace.")
    fix_encoding: bool = Field(True, description="Fix encoding issues with ftfy.")
    normalize_punctuation: bool = Field(True, description="Normalize punctuation.")
    normalize_unicode_dashes: bool = Field(True, description="Convert unicode dashes to ASCII.")
    normalize_smart_quotes: bool = Field(True, description="Convert smart quotes to straight quotes.")
    remove_excessive_punctuation: bool = Field(True, description="Remove repeated punctuation.")
    add_space_after_punctuation: bool = Field(True, description="Ensure space after punctuation.")
    standardize_units: bool = Field(True, description="Standardize unit representations.")
    standardize_currency: bool = Field(True, description="Standardize currency representations.")
    enable_typo_correction: bool = Field(True, description="Enable typo correction.")
    typo_correction: TypoCorrectionSettings = Field(
        default_factory=TypoCorrectionSettings,
        description="Typo correction specific settings.")


class EntityRecognitionSettings(BaseModel):
    """Settings for named entity recognition."""
    enabled: bool = Field(True, description="Enable entity recognition.")
    entity_types_to_extract: List[str] = Field(
        ["PERSON", "ORG", "GPE", "LOC", "DATE", "TIME", "MONEY", "PERCENT"],
        description="Entity types to extract from text.")


class IngestionServiceSettings(BaseModel):
    """Settings for the Ingestion Microservice."""
    port: int = Field(8000, description="Port for the Ingestion service API.")
    model_name: str = Field(
        "en_core_web_trf", description="The spaCy model to use for NER.")
    model_cache_dir: str = Field(
        "/app/.cache/spacy", description="Path for spaCy to cache models.")
    dateparser_languages: List[str] = Field(
        ["en"], description="Languages for dateparser to consider.")
    batch_processing_threads: int = Field(
        4, description="Number of threads for CLI batch processing.")
    langdetect_confidence_threshold: float = Field(
        0.9, description="Minimum confidence for language detection.")
    
    # New nested settings
    cleaning_pipeline: CleaningPipelineSettings = Field(
        default_factory=CleaningPipelineSettings,
        description="Text cleaning pipeline configuration.")
    entity_recognition: EntityRecognitionSettings = Field(
        default_factory=EntityRecognitionSettings,
        description="Entity recognition configuration.")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class CelerySettings(BaseModel):
    """Settings for Celery task queue."""
    broker_url: str = Field("redis://redis:6379/0",
                            description="Redis as broker URL.")
    result_backend: str = Field(
        "redis://redis:6379/0", description="Redis as result backend URL.")
    task_acks_late: bool = Field(
        True, description="Acknowledge task only after it's done.")
    worker_prefetch_multiplier: int = Field(
        1, description="Only fetch one task at a time per worker process.")
    worker_concurrency: int = Field(
        4, description="Number of worker processes. Adjust based on CPU cores.")
    task_annotations: Dict[str, Dict[str, Any]] = Field(
        {'*': {'rate_limit': '300/m'}}, description="Task-specific annotations for Celery.")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class JsonlStorageConfig(BaseModel):
    """Configuration for JSONL file storage."""
    output_path: str = Field("/app/data/processed_articles.jsonl",
                             description="Default output path for JSONL.")


class ElasticsearchStorageConfig(BaseModel):
    """Configuration for Elasticsearch storage."""
    host: str = Field(
        "elasticsearch", description="Elasticsearch host (Docker service name or IP).")
    port: int = Field(9200, description="Elasticsearch port.")
    scheme: str = Field(
        "http", description="Connection scheme (http or https).")
    index_name: str = Field(
        "news_articles", description="Name of the Elasticsearch index.")
    api_key: Optional[str] = Field(
        None, description="Elasticsearch API key for authentication.")


class PostgreSQLStorageConfig(BaseModel):
    """Configuration for PostgreSQL storage."""
    host: str = Field(
        "postgres", description="PostgreSQL host (Docker service name or IP).")
    port: int = Field(5432, description="PostgreSQL port.")
    dbname: str = Field("newsdb", description="PostgreSQL database name.")
    user: str = Field("user", description="PostgreSQL username.")
    password: str = Field("password", description="PostgreSQL password.")
    table_name: str = Field("processed_articles",
                            description="Table name for storing articles.")


class StorageSettings(BaseModel):
    """Overall settings for data storage backends."""
    enabled_backends: List[str] = Field(
        ["jsonl"], description="List of storage backend names.")
    jsonl: Optional[JsonlStorageConfig] = Field(
        None, description="JSONL storage specific configuration.")
    elasticsearch: Optional[ElasticsearchStorageConfig] = Field(
        None, description="Elasticsearch storage specific configuration.")
    postgresql: Optional[PostgreSQLStorageConfig] = Field(
        None, description="PostgreSQL storage specific configuration.")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class FormatterConfig(BaseModel):
    """Logging formatter configuration."""
    class_: str = Field(..., alias="class",
                        description="The class path for the formatter.")
    format: str = Field(..., description="The log format string.")

    model_config = SettingsConfigDict(
        extra='allow',
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class HandlerConfig(BaseModel):
    """Logging handler configuration."""
    class_: str = Field(..., alias="class",
                        description="The class path for the handler.")
    formatter: Optional[str] = Field(
        None, description="The formatter name to use for this handler.")
    stream: Optional[str] = Field(
        None, description="The stream for StreamHandler.")
    filename: Optional[str] = Field(
        None, description="The log file path for file-based handlers.")
    maxBytes: Optional[int] = Field(
        None, description="Maximum file size for RotatingFileHandler.")
    backupCount: Optional[int] = Field(
        None, description="Number of backup files for RotatingFileHandler.")

    model_config = SettingsConfigDict(
        extra='allow',
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""
    version: int
    disable_existing_loggers: bool
    formatters: Dict[str, FormatterConfig]
    handlers: Dict[str, HandlerConfig]
    root: Dict[str, Any]
    loggers: Dict[str, Dict[str, Any]]

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class BatchProcessingSettings(BaseModel):
    """Settings for batch processing and checkpointing."""
    checkpoint_enabled: bool = Field(True, description="Enable checkpoint system")
    checkpoint_interval: int = Field(10, description="Save checkpoint every N documents")
    checkpoint_ttl_seconds: int = Field(86400, description="Checkpoint TTL in seconds")
    default_batch_size: int = Field(100, description="Default batch size")
    max_batch_size: int = Field(10000, description="Maximum batch size")
    save_intermediate_results: bool = Field(True, description="Save intermediate results")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class ResourceManagementSettings(BaseModel):
    """Settings for resource monitoring and management."""
    idle_timeout_seconds: int = Field(300, description="Idle timeout in seconds")
    cleanup_on_idle: bool = Field(True, description="Cleanup resources on idle")
    cpu_threshold_percent: int = Field(95, description="CPU threshold percentage")
    memory_threshold_percent: int = Field(90, description="Memory threshold percentage")
    enable_low_resource_mode: bool = Field(False, description="Enable low resource mode")
    gpu_memory_fraction: float = Field(0.8, description="GPU memory fraction")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class EventBackendConfig(BaseModel):
    """Configuration for an event backend."""
    type: str = Field(..., description="Backend type")
    enabled: bool = Field(False, description="Enable this backend")
    config: Dict[str, Any] = Field(default_factory=dict, description="Backend-specific config")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=(),
        extra='allow'
    )


class EventsSettings(BaseModel):
    """Settings for CloudEvents multi-backend publishing."""
    enabled: bool = Field(True, description="Enable event publishing")
    publish_events: Optional[List[str]] = Field(None, description="Event types to publish")
    backends: List[EventBackendConfig] = Field(default_factory=list, description="Event backends")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=(),
        extra='allow'
    )


class MetadataRegistrySettings(BaseModel):
    """Settings for metadata registry integration."""
    enabled: bool = Field(True, description="Enable metadata registry")
    primary_backend: str = Field("postgresql", description="Primary backend")
    enable_redis_cache: bool = Field(True, description="Enable Redis cache")
    pool_min_size: int = Field(2, description="Connection pool min size")
    pool_max_size: int = Field(10, description="Connection pool max size")
    max_retries: int = Field(3, description="Max retry attempts")
    retry_delay_seconds: int = Field(2, description="Retry delay in seconds")

    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class Settings(BaseSettings):
    """Main settings model, loaded from a YAML file."""
    general: GeneralSettings
    ingestion_service: IngestionServiceSettings
    celery: CelerySettings
    storage: StorageSettings
    logging: LoggingConfig
    batch_processing: Optional[BatchProcessingSettings] = Field(None, description="Batch processing settings")
    resource_management: Optional[ResourceManagementSettings] = Field(None, description="Resource management settings")
    events: Optional[EventsSettings] = Field(None, description="Event publishing settings")
    metadata_registry: Optional[MetadataRegistrySettings] = Field(None, description="Metadata registry settings")

    model_config = SettingsConfigDict(
        protected_namespaces=()
    )


class ConfigManager:
    """
    Singleton class to manage and load application settings.
    """
    _settings: Optional[Settings] = None

    @staticmethod
    def get_settings() -> Settings:
        """
        Loads and returns the application settings. This is a singleton
        method that ensures the config is loaded only once.
        """
        if ConfigManager._settings is None:
            config_path = os.path.join(os.path.dirname(
                __file__), '../../config/settings.yaml')
            if not os.path.exists(config_path):
                raise FileNotFoundError(
                    f"Configuration file not found at {config_path}")

            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)

            try:
                ConfigManager._settings = Settings.model_validate(config_data)
            except Exception as e:
                print(f"CRITICAL ERROR: Failed to validate settings from {config_path}. "
                      f"Please check your settings.yaml file against the schema. Error: {e}", file=sys.stderr)
                raise RuntimeError(
                    "Failed to load and validate application settings.") from e

        return ConfigManager._settings


if __name__ == '__main__':
    try:
        settings = ConfigManager.get_settings()
        print("--- Loaded Settings ---")
        print(f"Log Level: {settings.general.log_level}")
        print(f"GPU Enabled: {settings.general.gpu_enabled}")
        print(f"Typo Correction: {settings.ingestion_service.cleaning_pipeline.enable_typo_correction}")
        print(f"Use NER for Typos: {settings.ingestion_service.cleaning_pipeline.typo_correction.use_ner_entities}")
    except Exception as e:
        print(f"Test failed: {e}", file=sys.stderr)


# src/utils/config_manager.py
