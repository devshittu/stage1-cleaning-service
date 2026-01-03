# Dockerfile
# Stage 1: Build stage - Install dependencies and download models
FROM python:3.11-slim-bookworm AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    SPACY_DATA=/app/.cache/spacy \
    PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Stage 2: Build stage - Install Python dependencies and spaCy model
FROM base AS build

WORKDIR /app

# Copy only requirements to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check --upgrade pip && \
    pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

# Pre-install the spaCy model to avoid runtime downloads
RUN python -m spacy download en_core_web_trf

# Stage 3: Final stage - Copy application code and runtime dependencies
FROM base AS final

WORKDIR /app

# Copy virtual environment (with model) from build stage
COPY --from=build ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# Copy application code and assets AFTER dependencies are installed
COPY src ./src
COPY config ./config
COPY entrypoint.sh .
# Copy the new CLI entrypoint script (main_cli.py)
COPY src/main_cli.py ./src/main_cli.py 
# Explicitly copy the new file
COPY run-cli.sh .

# Create cache, log, and data directories and ensure permissions
RUN mkdir -p /app/.cache/spacy /app/logs /app/data /app/monitoring && \
    chmod -R 777 /app/.cache /app/logs /app/data

# Make the entrypoint and CLI scripts executable
RUN chmod +x entrypoint.sh run-cli.sh

# Expose the port the service will run on
EXPOSE 8000

# Re-assert PATH for runtime environment to ensure virtual environment's bin is primary
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Set the entrypoint
ENTRYPOINT ["./entrypoint.sh"]

# Default command to run the FastAPI application
CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]


# Dockerfile