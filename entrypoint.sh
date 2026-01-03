#!/bin/bash
set -e

# =============================================================================
# STAGE 1 CLEANING PIPELINE - ENTRYPOINT SCRIPT
# =============================================================================
# Enhanced entrypoint with infrastructure integration support
# =============================================================================

echo "=== Stage 1 Cleaning Pipeline Entrypoint ==="

# Activate the virtual environment
source /opt/venv/bin/activate

# Create log and cache directories if they don't exist
mkdir -p /app/logs /app/.cache/spacy

# -----------------------------------------------------------------------------
# STEP 1: Install shared metadata registry (if available)
# -----------------------------------------------------------------------------
echo ">>> Checking for shared_metadata_registry package..."

if [ -d "/shared/shared-metadata-registry" ]; then
    echo "Found shared_metadata_registry at /shared/shared-metadata-registry"
    echo "Installing shared_metadata_registry package..."

    cd /shared/shared-metadata-registry
    pip install -e . --no-cache-dir 2>/dev/null || {
        echo "WARNING: Failed to install shared_metadata_registry"
        echo "Metadata registry integration will be disabled"
    }
    cd /app

    echo "✓ shared_metadata_registry installed successfully"
else
    echo "INFO: shared_metadata_registry not found (graceful degradation mode)"
fi

# -----------------------------------------------------------------------------
# STEP 2: Wait for infrastructure services (if in infrastructure mode)
# -----------------------------------------------------------------------------
if [ "${MODE}" = "infrastructure" ] || [ -n "${POSTGRES_HOST}" ]; then
    echo ">>> Waiting for infrastructure services..."

    # Function to wait for a service
    wait_for_service() {
        local host=$1
        local port=$2
        local service_name=$3
        local max_attempts=30
        local attempt=1

        echo "Waiting for ${service_name} at ${host}:${port}..."

        while ! nc -z "${host}" "${port}" > /dev/null 2>&1; do
            if [ ${attempt} -ge ${max_attempts} ]; then
                echo "WARNING: ${service_name} not available after ${max_attempts} attempts"
                return 1
            fi

            echo "  Attempt ${attempt}/${max_attempts}: ${service_name} not ready, waiting..."
            sleep 2
            attempt=$((attempt + 1))
        done

        echo "✓ ${service_name} is ready"
        return 0
    }

    # Wait for PostgreSQL (if enabled)
    if [ "${METADATA_REGISTRY_ENABLED:-true}" = "true" ] || [ "${BATCH_LIFECYCLE_ENABLED:-true}" = "true" ]; then
        wait_for_service "${POSTGRES_HOST:-postgres}" "${POSTGRES_PORT:-5432}" "PostgreSQL" || {
            echo "WARNING: PostgreSQL not available, batch lifecycle features may not work"
        }
    fi

    # Wait for Redis Broker
    wait_for_service "${REDIS_HOST:-redis-broker}" "${REDIS_PORT:-6379}" "Redis Broker" || {
        echo "WARNING: Redis Broker not available, Celery may not work"
    }

    # Wait for Redis Cache (for checkpoints)
    if [ "${CHECKPOINT_ENABLED:-true}" = "true" ]; then
        # Extract cache host from REDIS_CACHE_URL if set
        CACHE_HOST=$(echo "${REDIS_CACHE_URL:-redis://redis-cache:6379/1}" | sed -n 's|.*://\([^:]*\).*|\1|p')
        wait_for_service "${CACHE_HOST:-redis-cache}" "${REDIS_PORT:-6379}" "Redis Cache" || {
            echo "WARNING: Redis Cache not available, checkpoint features may not work"
        }
    fi

    echo "✓ Infrastructure services check complete"
fi

# -----------------------------------------------------------------------------
# STEP 3: Initialize PostgreSQL database schema (if in infrastructure mode)
# -----------------------------------------------------------------------------
if [ "${BATCH_LIFECYCLE_ENABLED:-true}" = "true" ] && [ "${MODE}" = "infrastructure" ]; then
    echo ">>> Initializing PostgreSQL job_registry schema..."

    python3 <<'EOF' 2>&1 | tee -a /app/logs/db_init.log
import asyncio
import os
import sys

async def init_db():
    """Initialize job_registry database schema."""
    try:
        from src.utils.job_manager import JobManager

        job_manager = JobManager()

        # Initialize connection pool
        pool = await job_manager.initialize_pool()

        if pool:
            print("✓ PostgreSQL job_registry schema initialized successfully")
            print(f"  Database: {os.getenv('POSTGRES_DB', 'stage1_cleaning')}")
            print(f"  Host: {os.getenv('POSTGRES_HOST', 'postgres')}")

            # Close pool
            await pool.close()
        else:
            print("WARNING: JobManager initialization failed (graceful degradation)")

    except Exception as e:
        print(f"WARNING: Database initialization failed: {e}")
        print("Job tracking will be disabled (graceful degradation)")

# Run initialization
asyncio.run(init_db())
EOF

    if [ $? -ne 0 ]; then
        echo "WARNING: Failed to initialize job_registry schema"
        echo "Job tracking may not work properly"
    fi

    echo "✓ Database initialization complete"
fi

# -----------------------------------------------------------------------------
# STEP 4: Load configuration and ensure logging is set up
# -----------------------------------------------------------------------------
echo ">>> Loading configuration..."

python -c "from src.utils.logger import setup_logging; setup_logging(); from src.utils.config_manager import ConfigManager; ConfigManager.get_settings()"

echo "✓ Configuration loaded"

# -----------------------------------------------------------------------------
# STEP 5: Check spaCy model
# -----------------------------------------------------------------------------
echo ">>> Checking spaCy model..."

# Get model name from config
MODEL_NAME=$(python -c "from src.utils.config_manager import ConfigManager; print(ConfigManager.get_settings().ingestion_service.model_name)" 2>/dev/null || echo "en_core_web_trf")

echo "Checking for spaCy model: ${MODEL_NAME}"

# Check if the spaCy model is loadable
if python -c "import spacy; spacy.load('${MODEL_NAME}')" 2>/dev/null; then
    echo "✓ spaCy model ${MODEL_NAME} found and loadable"
else
    echo "WARNING: spaCy model ${MODEL_NAME} not found or not loadable"
    echo "Attempting to download..."

    python -m spacy download "${MODEL_NAME}" 2>&1 || {
        echo "ERROR: Failed to download spaCy model ${MODEL_NAME}"
        echo "NER features may not work properly"
    }
fi

# -----------------------------------------------------------------------------
# STEP 6: Start the service
# -----------------------------------------------------------------------------
echo ">>> Starting service..."
echo "Command: $@"

# Execute the main command passed to the script (e.g., `uvicorn ...`)
exec "$@"
# entrypoint.sh