# Stage 1 Deployment Guide (Verified)

**Service**: Data Cleaning & Preprocessing Service
**Version**: 1.0.0
**Status**: Production Ready ✅
**Last Verified**: January 5, 2026

---

## Quick Start (5 Minutes)

### Prerequisites Check

```bash
# Verify infrastructure is running
docker ps --filter "name=postgres" --filter "name=redis" --filter "name=traefik" --format "{{.Names}}: {{.Status}}"

# Expected output:
# storytelling-postgres: Up X hours (healthy)
# redis-broker: Up X hours (healthy)
# redis-cache: Up X hours (healthy)
# traefik: Up X hours (healthy)
```

If infrastructure is NOT running:
```bash
cd ../infrastructure
docker compose up -d
cd ../stage1-cleaning-service
```

### Deployment Steps

**1. Start Services (Infrastructure Mode)**

```bash
# OPTION A: Using helper script (recommended)
./run-with-infrastructure.sh

# OPTION B: Manual start
docker compose -f docker-compose.infrastructure.yml up -d --build
```

**2. Verify Health**

```bash
# Wait 30 seconds for services to initialize
sleep 30

# Check health endpoint
curl -s http://localhost:8000/health | jq '.'
```

Expected output:
```json
{
  "status": "healthy",
  "service": "stage1-cleaning-orchestrator",
  "version": "1.0.0",
  "spacy_model": "en_core_web_trf",
  "postgres_connected": true,
  "redis_connected": true,
  "timestamp": "2026-01-05T..."
}
```

**3. Verify Services**

```bash
# Check all containers are running
docker compose -f docker-compose.infrastructure.yml ps

# Expected output:
# NAME                      STATUS
# cleaning-orchestrator     Up 30 seconds (healthy)
# cleaning-celery-worker    Up 30 seconds
```

**4. Run E2E Validation**

```bash
# Process 100 real articles
timeout 180 ./test_e2e_pipeline.sh
```

Expected output:
```
========================================
Stage 1 E2E Pipeline Validation
========================================

[1/10] Checking prerequisites...
[2/10] Testing health endpoint...
✓ Service is healthy (status: healthy)
[3/10] Submitting batch job (100 articles)...
✓ Batch job submitted: <job_id>
[4/10] Monitoring job progress...
  Progress: 100/100 documents | Status: completed
✓ Job completed successfully
[5/10] Verifying JSONL output...
  Found 100 processed articles in JSONL
✓ JSONL output verified
[6/10] Validating output schema...
✓ Output schema validated
[7/10] Verifying entity extraction...
  Extracted 12 entities in sample article
✓ Entity extraction working
[8/10] Verifying Redis checkpoints...
✓ Checkpoint found in Redis
[9/10] Verifying event publishing...
  Found 100 events in Redis Stream
✓ Event publishing verified
[10/10] Calculating performance metrics...
  Total processing time: 112s
  Throughput: 0.89 articles/second
  Average latency: 1120ms per article
✓ Performance metrics calculated

========================================
E2E Validation Summary
========================================
✓ All tests passed!

Job ID: <job_id>
Articles processed: 100 / 100
Output file: /app/data/processed_articles_2026-01-05.jsonl
Throughput: 0.89 articles/s

Pipeline is production-ready!
```

---

## Deployment Modes

### Mode 1: Standalone (Development)

**Use Case**: Local development, testing individual components

```bash
# Start with local infrastructure
docker compose up -d

# Access directly
curl http://localhost:8000/health
```

**Characteristics**:
- ❌ No centralized infrastructure integration
- ❌ No Traefik routing
- ❌ Ports exposed directly (8000)
- ✅ Fast iteration
- ✅ Isolated testing

### Mode 2: Infrastructure Integration (Production)

**Use Case**: Production deployment, multi-stage pipeline

```bash
# Start with centralized infrastructure
docker compose -f docker-compose.infrastructure.yml up -d

# Access via Traefik
curl http://localhost/api/v1/cleaning/health
```

**Characteristics**:
- ✅ Centralized PostgreSQL (`stage1_cleaning` database)
- ✅ Centralized Redis (DB 0 for Celery, DB 1 for cache)
- ✅ Traefik routing (`/api/v1/cleaning/*`)
- ✅ Prometheus metrics collection
- ✅ Loki log aggregation
- ✅ Infrastructure network (`storytelling`)
- ❌ Requires infrastructure services running

---

## Configuration

### Environment Variables

**Required** (in `.env` file):
```bash
# PostgreSQL credentials
STAGE1_POSTGRES_PASSWORD=stage1_secure_password

# Optional: Elasticsearch (if using elasticsearch backend)
ELASTICSEARCH_API_KEY=your_elasticsearch_api_key
```

### Service Configuration

**File**: `config/settings.yaml`

**Production Defaults** (pre-configured):
```yaml
general:
  log_level: INFO
  gpu_enabled: True  # Disable if no GPU available

ingestion_service:
  model_name: "en_core_web_trf"
  batch_processing_threads: 4

  cleaning_pipeline:
    enable_html_removal: true
    enable_encoding_correction: true
    enable_typo_correction: true  # 30% of processing time

    typo_correction:
      use_ner_entities: true  # CRITICAL - protects proper nouns
      language: "en"

storage:
  enabled_backends: ["jsonl"]  # Add "postgresql", "elasticsearch" for production

  jsonl:
    output_path: "/app/data/output/processed.jsonl"

  postgresql:
    host: "postgres"
    port: 5432
    database: "stage1_cleaning"
    user: "stage1_user"
    table_name: "processed_articles"
```

**Performance Tuning**:

```yaml
# For high throughput (scale workers):
# docker compose -f docker-compose.infrastructure.yml up -d --scale cleaning-celery-worker=8

# For low memory environments:
ingestion_service:
  batch_processing_threads: 2  # Reduce from 4
  cleaning_pipeline:
    enable_typo_correction: false  # Save 30% processing time

general:
  gpu_enabled: False  # Use CPU if GPU unavailable
```

---

## API Access

### Via Traefik (Production)

**Base URL**: `http://localhost/api/v1/cleaning`

```bash
# Health check
curl http://localhost/api/v1/cleaning/health

# Process single article
curl -X POST "http://localhost/api/v1/cleaning/v1/preprocess" \
  -H "Content-Type: application/json" \
  -d '{
    "document_id": "news-001",
    "text": "Apple Inc. announced new products yesterday."
  }'

# Submit batch job (CLI method - recommended)
docker exec cleaning-orchestrator python -m src.main_cli batch submit \
  -f /app/data/input.jsonl \
  -b "batch_$(date +%Y%m%d_%H%M%S)" \
  -c 10
```

### Direct Access (Development)

**Base URL**: `http://localhost:8000`

```bash
# Health check
curl http://localhost:8000/health

# Interactive API docs
open http://localhost:8000/docs
```

---

## CLI Usage

### Batch Job Workflow

**1. Submit Job**

```bash
docker exec cleaning-orchestrator python -m src.main_cli batch submit \
  -f /app/data/input.jsonl \
  -b "my_batch_$(date +%Y%m%d)" \
  -c 10

# Output:
# Job ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
# Batch ID: my_batch_20260105
# Total documents: 1000
# Chunk size: 10
# Status: queued
```

**2. Monitor Progress**

```bash
JOB_ID="a1b2c3d4-e5f6-7890-abcd-ef1234567890"

docker exec cleaning-orchestrator python -m src.main_cli batch status \
  -j "$JOB_ID"

# Output (updates in real-time):
# Job ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
# Status: running
# Progress: 450/1000 (45.0%)
# Processed: 450
# Failed: 2
# Last checkpoint: 450
```

**3. List Jobs**

```bash
# All jobs
docker exec cleaning-orchestrator python -m src.main_cli batch list

# Filter by batch ID
docker exec cleaning-orchestrator python -m src.main_cli batch list \
  -b "my_batch_20260105"

# Output:
# ┌──────────────────────────────────┬─────────────────┬──────────┬─────────┐
# │ Job ID                           │ Batch ID        │ Status   │ Progress│
# ├──────────────────────────────────┼─────────────────┼──────────┼─────────┤
# │ a1b2c3d4-e5f6-7890-abcd-ef12... │ my_batch_202... │ running  │ 45.0%   │
# └──────────────────────────────────┴─────────────────┴──────────┴─────────┘
```

**4. Pause/Resume (Optional)**

```bash
# Pause job
docker exec cleaning-orchestrator python -m src.main_cli batch pause \
  -j "$JOB_ID"

# Resume job
docker exec cleaning-orchestrator python -m src.main_cli batch resume \
  -j "$JOB_ID"
```

**5. Cancel Job (Optional)**

```bash
docker exec cleaning-orchestrator python -m src.main_cli batch cancel \
  -j "$JOB_ID"
```

---

## Monitoring & Observability

### Health Check

```bash
# Full health status
curl -s http://localhost:8000/health | jq '.'

# Check specific components
curl -s http://localhost:8000/health | jq '.postgres_connected'  # true/false
curl -s http://localhost:8000/health | jq '.redis_connected'     # true/false
```

### Prometheus Metrics

```bash
# Metrics endpoint
curl http://localhost:8000/metrics

# Key metrics:
# - documents_processed_total{stage="1",service="cleaning",status="success"}
# - processing_duration_seconds{stage="1",service="cleaning"}
# - celery_tasks_total{stage="1",status="success"}
```

**Access Prometheus UI**:
```bash
# Open Prometheus dashboard
open http://localhost:9090

# Query: documents_processed_total
# Query: rate(processing_duration_seconds_sum[5m])
```

### Logs

```bash
# Orchestrator logs
docker compose -f docker-compose.infrastructure.yml logs -f cleaning-orchestrator

# Celery worker logs
docker compose -f docker-compose.infrastructure.yml logs -f cleaning-celery-worker

# Filter for errors
docker compose -f docker-compose.infrastructure.yml logs cleaning-orchestrator | grep ERROR

# Structured logs (JSON format)
docker compose -f docker-compose.infrastructure.yml logs cleaning-orchestrator --tail 100 | jq '.'
```

**Access Grafana Loki**:
```bash
# Open Grafana dashboard
open http://localhost:3000

# Login: admin / admin
# Navigate to: Explore > Loki
# Query: {container_name="cleaning-orchestrator"}
```

### Job Registry (PostgreSQL)

```bash
# Query job history
docker exec storytelling-postgres psql -U admin -d stage1_cleaning -c "
SELECT
  job_id,
  batch_id,
  status,
  processed_documents,
  total_documents,
  progress_percent,
  created_at,
  completed_at
FROM job_registry
ORDER BY created_at DESC
LIMIT 10;
"
```

### Event Stream (Redis)

```bash
# View published events
docker exec redis-cache redis-cli -n 1 XLEN "stage1:cleaning:events"

# Read latest events
docker exec redis-cache redis-cli -n 1 XREVRANGE "stage1:cleaning:events" + - COUNT 10

# Output:
# 1) "1736088123456-0"
# 2) 1) "id"
#    2) "evt_abc123"
#    3) "type"
#    4) "com.storytelling.stage1.document.cleaned"
#    5) "source"
#    6) "/stage1/cleaning"
#    7) "datacontenttype"
#    8) "application/json"
#    9) "data"
#    10) "{\"document_id\":\"news-001\",\"cleaned_text\":\"...\"}"
```

---

## Verification Checklist

### Deployment Verification

- [ ] Infrastructure services running (PostgreSQL, Redis, Traefik)
- [ ] Stage 1 containers healthy (`cleaning-orchestrator`, `cleaning-celery-worker`)
- [ ] Health endpoint returns `200 OK`
- [ ] PostgreSQL database `stage1_cleaning` exists with `job_registry` table
- [ ] Redis DB 0 (Celery broker) reachable
- [ ] Redis DB 1 (cache) reachable
- [ ] Traefik routing working (`/api/v1/cleaning/*`)

**Verification Command**:
```bash
# Run all checks
docker exec cleaning-orchestrator python -c "
import requests
import psycopg2
import redis

# Health check
health = requests.get('http://localhost:8000/health').json()
assert health['status'] in ['healthy', 'ok'], 'Service unhealthy'
assert health['postgres_connected'], 'PostgreSQL not connected'
assert health['redis_connected'], 'Redis not connected'

# PostgreSQL check
conn = psycopg2.connect(
    host='postgres',
    port=5432,
    database='stage1_cleaning',
    user='stage1_user',
    password='stage1_secure_password'
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM job_registry')
print(f'Job registry rows: {cur.fetchone()[0]}')
conn.close()

# Redis check
r = redis.Redis(host='redis-cache', port=6379, db=1)
print(f'Redis ping: {r.ping()}')

print('✅ All checks passed!')
"
```

### E2E Pipeline Verification

- [ ] E2E test script runs without errors (`./test_e2e_pipeline.sh`)
- [ ] 100 articles processed successfully
- [ ] JSONL output file created with 100 entries
- [ ] PostgreSQL job record created with `completed` status
- [ ] Redis checkpoints saved (10, 20, 30...100)
- [ ] Redis event stream contains 100 events
- [ ] Processing time < 180 seconds
- [ ] Throughput > 0.5 articles/second

**Run Verification**:
```bash
timeout 180 ./test_e2e_pipeline.sh
# Should complete with "✅ All tests passed!"
```

### Functional Verification

- [ ] Single article processing works (API endpoint)
- [ ] Batch processing works (CLI)
- [ ] Job status tracking works
- [ ] Progress updates in real-time
- [ ] Checkpoint/resume functionality works
- [ ] Entity extraction produces valid entities
- [ ] Output schema matches contract (PreprocessSingleResponse)
- [ ] Multi-backend storage works (JSONL, PostgreSQL if enabled)

**Test Single Article**:
```bash
curl -X POST "http://localhost:8000/v1/preprocess" \
  -H "Content-Type: application/json" \
  -d '{
    "document_id": "test-001",
    "text": "Apple Inc. CEO Tim Cook announced new AI features in San Francisco yesterday."
  }' | jq '.'

# Verify output contains:
# - document_id: "test-001"
# - cleaned_text: "..."
# - entities: [{"text": "Apple Inc.", "type": "ORG"}, ...]
```

---

## Troubleshooting

### Issue: Service Won't Start

**Symptoms**:
```bash
docker compose -f docker-compose.infrastructure.yml ps
# cleaning-orchestrator: Exited (1)
```

**Debug Steps**:

1. Check logs:
```bash
docker compose -f docker-compose.infrastructure.yml logs cleaning-orchestrator
```

2. Verify infrastructure is running:
```bash
docker ps --filter "name=postgres" --filter "name=redis"
```

3. Rebuild containers:
```bash
docker compose -f docker-compose.infrastructure.yml down
docker compose -f docker-compose.infrastructure.yml up -d --build
```

### Issue: Health Check Fails

**Symptoms**:
```bash
curl http://localhost:8000/health
# {"status": "unhealthy", "postgres_connected": false}
```

**Solutions**:

1. Verify PostgreSQL database exists:
```bash
docker exec storytelling-postgres psql -U admin -d postgres -c "\l" | grep stage1_cleaning
```

2. Create database if missing:
```bash
docker exec storytelling-postgres psql -U admin -d postgres -c "
CREATE DATABASE stage1_cleaning;
GRANT ALL ON DATABASE stage1_cleaning TO stage1_user;
"
```

3. Verify credentials in `.env`:
```bash
grep STAGE1_POSTGRES_PASSWORD .env
# STAGE1_POSTGRES_PASSWORD=stage1_secure_password
```

### Issue: Celery Worker Not Processing

**Symptoms**:
```bash
# Job stuck in "queued" status
docker exec cleaning-orchestrator python -m src.main_cli batch status -j <job_id>
# Status: queued (never changes to running)
```

**Solutions**:

1. Check worker status:
```bash
docker compose -f docker-compose.infrastructure.yml logs cleaning-celery-worker

# Look for:
# celery@<hostname> ready.
```

2. Verify Celery broker connection:
```bash
docker exec cleaning-celery-worker celery -A src.celery_app inspect ping

# Expected: pong from worker
```

3. Restart worker:
```bash
docker compose -f docker-compose.infrastructure.yml restart cleaning-celery-worker
```

### Issue: Progress Counter Not Updating

**Symptoms**:
```bash
# Progress stuck at 0% despite processing
docker exec cleaning-orchestrator python -m src.main_cli batch status -j <job_id>
# Progress: 0/100 (0.0%)
```

**Solution**:

This was **FIXED** in `src/utils/job_manager.py:332`:
```python
# Statistics field must be JSON string for PostgreSQL JSONB column
json.dumps(statistics) if statistics else json.dumps({})
```

**Verification**:
```bash
# Check for error in logs
docker compose -f docker-compose.infrastructure.yml logs cleaning-celery-worker | grep "failed_to_update_job_progress"

# Should NOT see: "invalid input for query argument $4: {} (expected str, got dict)"
```

### Issue: Out of Memory

**Symptoms**:
```bash
docker compose -f docker-compose.infrastructure.yml logs cleaning-celery-worker
# Killed (exit code 137)
```

**Solutions**:

1. Reduce batch processing threads:
```yaml
# config/settings.yaml
ingestion_service:
  batch_processing_threads: 2  # Reduce from 4
```

2. Disable typo correction (saves 30% processing time):
```yaml
cleaning_pipeline:
  enable_typo_correction: false
```

3. Scale down Celery workers:
```bash
docker compose -f docker-compose.infrastructure.yml up -d --scale cleaning-celery-worker=2
```

### Issue: GPU Not Detected

**Symptoms**:
```bash
docker exec cleaning-orchestrator python -c "
import spacy
spacy.prefer_gpu()
print(spacy.require_gpu())
"
# False
```

**Solutions**:

1. Verify GPU available on host:
```bash
nvidia-smi
```

2. Verify Docker GPU support:
```bash
docker run --rm --gpus all nvidia/cuda:11.8.0-base-ubuntu22.04 nvidia-smi
```

3. Rebuild container with GPU support:
```bash
docker compose -f docker-compose.infrastructure.yml down
docker compose -f docker-compose.infrastructure.yml up -d --build
```

4. Fallback to CPU (slower but works):
```yaml
# config/settings.yaml
general:
  gpu_enabled: False
```

---

## Performance Optimization

### Current Performance (Verified)

**E2E Test Results** (100 articles, AMD Threadripper 48-core, 160GB RAM):
- **Throughput**: 0.89 articles/second
- **Latency**: ~1120ms per article
- **Processing Time**: 112 seconds total

### Optimization Strategies

#### 1. Scale Celery Workers (Linear Speedup)

```bash
# Scale to 8 workers (8x throughput)
docker compose -f docker-compose.infrastructure.yml up -d --scale cleaning-celery-worker=8

# Expected: ~7 articles/second (8x improvement)
```

#### 2. Enable GPU Acceleration (10x NER Speedup)

```yaml
# config/settings.yaml
general:
  gpu_enabled: True
  model_name: "en_core_web_trf"  # GPU-accelerated transformer
```

**Expected Impact**:
- NER processing: 500ms → 50ms per article (10x faster)
- Overall: ~1120ms → ~670ms per article (~40% faster)

#### 3. Disable Expensive Features

```yaml
cleaning_pipeline:
  enable_typo_correction: false  # Saves 30% processing time
```

**Expected Impact**:
- Processing time: ~1120ms → ~780ms per article (~30% faster)
- Tradeoff: No spell-checking

#### 4. Increase Batch Chunk Size

```bash
# Submit larger chunks (reduces overhead)
docker exec cleaning-orchestrator python -m src.main_cli batch submit \
  -f /app/data/input.jsonl \
  -c 50  # Increase from 10 to 50

# Expected: Lower latency overhead, higher throughput
```

#### 5. Optimize spaCy Pipeline

```yaml
# config/settings.yaml
ingestion_service:
  model_name: "en_core_web_sm"  # Smaller model (faster, less accurate)
```

**Expected Impact**:
- NER processing: 500ms → 100ms per article (5x faster)
- Overall: ~1120ms → ~720ms per article (~35% faster)
- Tradeoff: Lower NER accuracy

### Performance Targets

| Configuration | Throughput | Latency | Use Case |
|---------------|------------|---------|----------|
| **1 worker, CPU** | 0.89 art/s | 1120ms | Development |
| **4 workers, CPU** | 3.6 art/s | 280ms | Small batches |
| **8 workers, CPU** | 7.1 art/s | 140ms | Medium batches |
| **8 workers, GPU** | 14.3 art/s | 70ms | Production |
| **8 workers, GPU + optimized** | 20+ art/s | <50ms | High throughput |

---

## Production Deployment Checklist

### Pre-Deployment

- [ ] Infrastructure services running and healthy
- [ ] `.env` file configured with secure passwords
- [ ] `config/settings.yaml` reviewed and optimized for production
- [ ] Resource limits configured (`deploy.resources` in docker-compose)
- [ ] Observability stack configured (Prometheus, Grafana, Loki)
- [ ] Traefik routing configured (`/api/v1/cleaning/*`)
- [ ] Storage backends enabled (PostgreSQL, Elasticsearch)
- [ ] Backup strategy defined for PostgreSQL

### Deployment

- [ ] Build containers: `docker compose -f docker-compose.infrastructure.yml build`
- [ ] Start services: `docker compose -f docker-compose.infrastructure.yml up -d`
- [ ] Verify health: `curl http://localhost:8000/health`
- [ ] Run E2E test: `./test_e2e_pipeline.sh`
- [ ] Scale workers: `docker compose up -d --scale cleaning-celery-worker=8`
- [ ] Verify Traefik routing: `curl http://localhost/api/v1/cleaning/health`

### Post-Deployment

- [ ] Monitor logs for errors (first 30 minutes)
- [ ] Verify metrics in Prometheus
- [ ] Verify logs in Grafana Loki
- [ ] Test single article processing via API
- [ ] Test batch processing via CLI
- [ ] Submit production batch job
- [ ] Monitor job progress in real-time
- [ ] Verify output files in storage backends
- [ ] Check PostgreSQL job registry for completion
- [ ] Verify CloudEvents in Redis Stream

### Ongoing Monitoring

- [ ] Set up Prometheus alerts (processing failures, high latency)
- [ ] Configure Grafana dashboards (throughput, error rate, latency)
- [ ] Schedule daily health checks
- [ ] Monitor disk usage (JSONL output grows daily)
- [ ] Monitor PostgreSQL connection pool saturation
- [ ] Monitor Redis memory usage
- [ ] Review error logs weekly

---

## Support & Resources

### Documentation

- **Architecture**: `.claude/CLAUDE.md`
- **Testing**: `.claude/rules/testing.md`
- **Infrastructure Integration**: `.claude/rules/infrastructure-integration.md`
- **Production Assessment**: `PRODUCTION_READINESS_VERIFIED.md`

### API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Observability

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **Traefik Dashboard**: http://localhost:8080

### CLI Help

```bash
# General help
docker exec cleaning-orchestrator python -m src.main_cli --help

# Batch command help
docker exec cleaning-orchestrator python -m src.main_cli batch --help

# Specific subcommand help
docker exec cleaning-orchestrator python -m src.main_cli batch submit --help
```

---

## Appendix: Output Schema

### PreprocessSingleResponse

**File**: `src/schemas/data_models.py`

```python
class PreprocessSingleResponse(BaseModel):
    document_id: str
    version: str = "1.0.0"

    # Original data
    original_text: str

    # Cleaned data (prefixed with cleaned_*)
    cleaned_text: str
    cleaned_title: Optional[str]
    cleaned_author: Optional[str]
    cleaned_publication_date: Optional[date]
    cleaned_source_url: Optional[HttpUrl]

    # NLP outputs
    entities: List[Entity]  # [{text, type, start_char, end_char}]

    # Metadata
    cleaned_additional_metadata: Optional[Dict[str, Any]]
    temporal_metadata: Optional[str]
```

### CloudEvent (Redis Stream)

**Format**: CloudEvents v1.0

```json
{
  "id": "evt_abc123",
  "type": "com.storytelling.stage1.document.cleaned",
  "source": "/stage1/cleaning",
  "specversion": "1.0",
  "datacontenttype": "application/json",
  "time": "2026-01-05T12:34:56Z",
  "data": {
    "document_id": "news-001",
    "cleaned_text": "Apple Inc. announced...",
    "entities": [
      {"text": "Apple Inc.", "type": "ORG", "start_char": 0, "end_char": 10}
    ],
    "version": "1.0.0"
  }
}
```

---

**Document Version**: 1.0.0
**Last Updated**: January 5, 2026
**Verified By**: E2E Pipeline Test (100 articles)
**Status**: Production Ready ✅
