# Stage 1 → Stage 2 Integration Guide

**Implementation**: Option A - Consumer Service Pattern ✅
**Status**: IMPLEMENTED - Ready for Testing
**Date**: January 5, 2026

---

## Overview

This guide explains how to enable and test the event-driven integration between Stage 1 (Data Cleaning) and Stage 2 (NLP Processing).

**What's Implemented**:
- ✅ Redis Stream event consumer (`stage2/src/events/consumer.py`)
- ✅ Background consumer service (`stage2/src/services/event_consumer_service.py`)
- ✅ Docker service configuration (`stage2/docker-compose.infrastructure.yml`)
- ✅ Configuration file updates (`stage2/config/settings.yaml`)
- ✅ Consumer groups for fault-tolerant delivery
- ✅ Idempotency checks (prevent duplicate processing)
- ✅ Dead letter queue for failed events
- ✅ Health checks and metrics

**Flow**:
```
Stage 1: Clean Documents
    ↓ (publishes CloudEvent)
Redis Stream: stage1:cleaning:events (DB 1)
    ↓ (consumed by)
Stage 2: Event Consumer Service
    ↓ (triggers)
Stage 2: NLP Processing
    ↓ (publishes CloudEvent)
Redis Stream: stage2:nlp:events (DB 3)
```

---

## Quick Start (5 Steps)

### Step 1: Enable Event Consumer

Edit `stage2-nlp-processing/config/settings.yaml`:

```yaml
event_consumer:
  enabled: true  # ✅ Change from false to true
  auto_process: true  # Automatically trigger NLP processing
```

Or set via environment variable:

```bash
export EVENT_CONSUMER_ENABLED=true
```

### Step 2: Start Stage 2 with Event Consumer

```bash
cd ../stage2-nlp-processing

# Build and start all services including event consumer
docker compose -f docker-compose.infrastructure.yml up -d --build
```

**Expected Services**:
```
nlp-orchestrator       (orchestrator API)
nlp-celery-worker      (batch processing)
nlp-event-consumer     (NEW - consumes Stage 1 events) ✅
nlp-ner-service        (NER API)
nlp-dp-service         (Dependency parsing API)
nlp-event-llm-service  (Event extraction API)
```

### Step 3: Verify Event Consumer is Running

```bash
# Check container status
docker ps --filter "name=nlp-event-consumer"

# Expected output:
# nlp-event-consumer: Up X seconds

# Check logs
docker logs nlp-event-consumer --tail 50

# Expected logs:
# event_consumer_initialized: stream=stage1:cleaning:events, group=stage2-nlp-processor
# event_consumer_service_running
```

### Step 4: Trigger Stage 1 Processing

```bash
cd ../stage1-cleaning-service

# Submit batch job to Stage 1
docker exec cleaning-orchestrator python -m src.main_cli batch submit \
  -f /app/data/e2e_test_100_articles.jsonl \
  -b "integration_test_$(date +%Y%m%d_%H%M%S)" \
  -c 10

# Monitor Stage 1 job
docker exec cleaning-orchestrator python -m src.main_cli batch status -j <job_id>
```

### Step 5: Verify Stage 2 Receives Events

```bash
# Check event consumer logs
docker logs nlp-event-consumer --tail 100 | grep "event_consumed"

# Expected output:
# event_consumed: type=com.storytelling.cleaning.job.completed, id=...
# nlp_batch_processing_triggered: job_id=..., batch_id=...

# Verify NLP processing started
docker logs nlp-orchestrator --tail 50 | grep "batch_processing"
```

---

## Configuration Reference

### Event Consumer Settings

**File**: `stage2/config/settings.yaml`

```yaml
event_consumer:
  # -------------------------------------------------------------------------
  # ENABLE/DISABLE
  # -------------------------------------------------------------------------
  enabled: false  # Set true to enable automatic processing

  # -------------------------------------------------------------------------
  # SOURCE STREAM (Stage 1 publishes here)
  # -------------------------------------------------------------------------
  source_stream: "stage1:cleaning:events"
  redis_host: "redis-cache"
  redis_port: 6379
  redis_db: 1  # Stage 1 uses DB 1 for events

  # -------------------------------------------------------------------------
  # CONSUMER GROUP (Fault-Tolerant Consumption)
  # -------------------------------------------------------------------------
  consumer_group: "stage2-nlp-processor"
  consumer_name: "${HOSTNAME}"  # Unique per worker

  # -------------------------------------------------------------------------
  # EVENT FILTERING
  # -------------------------------------------------------------------------
  consume_events:
    - "com.storytelling.cleaning.document.cleaned"  # Individual docs
    - "com.storytelling.cleaning.job.completed"      # Batch jobs

  # -------------------------------------------------------------------------
  # PROCESSING BEHAVIOR
  # -------------------------------------------------------------------------
  auto_process: true  # Trigger NLP processing automatically
  batch_mode: true    # Process job.completed as batches

  # -------------------------------------------------------------------------
  # PERFORMANCE TUNING
  # -------------------------------------------------------------------------
  poll_interval_ms: 1000  # Check for events every 1 second
  batch_size: 10          # Process 10 events per iteration
  concurrent_tasks: 4     # Parallel processing tasks

  # -------------------------------------------------------------------------
  # ERROR HANDLING
  # -------------------------------------------------------------------------
  retry_failed: true
  max_retries: 3
  retry_delay_seconds: 5
  dead_letter_stream: "stage2:nlp:failed-events"

  # -------------------------------------------------------------------------
  # IDEMPOTENCY (Prevent Duplicate Processing)
  # -------------------------------------------------------------------------
  check_already_processed: true
  deduplication_ttl_hours: 24

  # -------------------------------------------------------------------------
  # BACKLOG PROCESSING (Historical Events)
  # -------------------------------------------------------------------------
  process_backlog: false  # Set true to process 350 existing events
  backlog_batch_size: 100
  backlog_max_age_hours: 24
```

### Environment Variables (Override Config)

```bash
# Enable consumer
export EVENT_CONSUMER_ENABLED=true

# Override stream name
export STAGE1_EVENT_STREAM="stage1:cleaning:events"

# Control auto-processing
export AUTO_PROCESS=true

# Stage 1 database access (for reading cleaned documents)
export STAGE1_POSTGRES_PASSWORD="stage1_secure_password"
```

---

## Testing

### Test 1: Consumer Health Check

```bash
# Via Docker exec
docker exec nlp-event-consumer python -c "
import asyncio
from src.events.consumer import get_event_consumer

async def check():
    consumer = get_event_consumer()
    await consumer.initialize()
    health = await consumer.health_check()
    print(health)

asyncio.run(check())
"

# Expected output:
# {
#   'enabled': True,
#   'healthy': True,
#   'stream_exists': True,
#   'group_exists': True,
#   'pending_count': 0,
#   'running': True
# }
```

### Test 2: Manual Event Consumption (Dry Run)

```bash
# Enable consumer but disable auto-processing
cd ../stage2-nlp-processing

# Edit config/settings.yaml:
#   enabled: true
#   auto_process: false  # Log events but don't trigger processing

# Restart event consumer
docker restart nlp-event-consumer

# Submit Stage 1 job
cd ../stage1-cleaning-service
docker exec cleaning-orchestrator python -m src.main_cli batch submit \
  -f /app/data/e2e_test_100_articles.jsonl \
  -b "dry_run_test"

# Watch event consumer logs
docker logs nlp-event-consumer -f

# Expected output:
# event_consumed_not_processed: type=..., id=... (auto_process=false)
```

### Test 3: Full E2E Integration

```bash
# Enable both enabled and auto_process
cd ../stage2-nlp-processing

# Edit config/settings.yaml:
#   enabled: true
#   auto_process: true

# Restart event consumer
docker restart nlp-event-consumer

# Submit Stage 1 job
cd ../stage1-cleaning-service
./test_e2e_pipeline.sh

# Watch both stages process
# Terminal 1: Stage 1 logs
docker logs cleaning-orchestrator -f | grep "event_published"

# Terminal 2: Event consumer logs
docker logs nlp-event-consumer -f | grep "event_consumed\|nlp_batch_processing_triggered"

# Terminal 3: Stage 2 NLP processing logs
docker logs nlp-orchestrator -f | grep "batch_processing\|document_processed"
```

### Test 4: Process Backlog (350 Historical Events)

```bash
# Enable backlog processing
cd ../stage2-nlp-processing

# Edit config/settings.yaml:
#   process_backlog: true
#   backlog_batch_size: 100

# Restart event consumer (will process backlog on startup)
docker restart nlp-event-consumer

# Monitor backlog processing
docker logs nlp-event-consumer -f | grep "backlog"

# Expected output:
# backlog_processing_started: count=350
# backlog_batch_processed: batch=1, events=100
# backlog_batch_processed: batch=2, events=100
# ...
# backlog_processing_completed: total=350
```

---

## Monitoring

### Health Checks

```bash
# Event consumer health
docker exec nlp-event-consumer curl -s http://localhost:8000/health

# Check consumer group lag (pending events)
docker exec storytelling-redis-cache redis-cli -n 1 \
  XPENDING stage1:cleaning:events stage2-nlp-processor

# Expected output:
# 1) (integer) 0  # No pending events (all processed)
# 2) "<lowest-id>"
# 3) "<highest-id>"
# 4) (empty array)  # No consumer with pending messages
```

### Metrics

```bash
# Event consumption metrics
docker exec nlp-event-consumer python -c "
import asyncio
from src.events.consumer import get_event_consumer

async def metrics():
    consumer = get_event_consumer()
    await consumer.initialize()
    print(consumer.get_metrics())

asyncio.run(metrics())
"

# Expected output:
# {
#   'total_consumed': 100,
#   'processed': 98,
#   'filtered': 0,
#   'failed': 2,
#   'duplicates_skipped': 0,
#   'by_event_type': {
#     'com.storytelling.cleaning.job.completed': 1,
#     'com.storytelling.cleaning.document.cleaned': 99
#   }
# }
```

### Redis Stream Inspection

```bash
# Check event stream length
docker exec storytelling-redis-cache redis-cli -n 1 XLEN "stage1:cleaning:events"

# Read latest events
docker exec storytelling-redis-cache redis-cli -n 1 \
  XREVRANGE "stage1:cleaning:events" + - COUNT 5

# Check consumer group info
docker exec storytelling-redis-cache redis-cli -n 1 \
  XINFO GROUPS "stage1:cleaning:events"

# Check dead letter queue
docker exec storytelling-redis-cache redis-cli -n 1 \
  XLEN "stage2:nlp:failed-events"
```

---

## Troubleshooting

### Issue 1: Event Consumer Not Starting

**Symptoms**:
```bash
docker ps --filter "name=nlp-event-consumer"
# Container missing or exiting
```

**Debug**:
```bash
# Check logs for errors
docker logs nlp-event-consumer

# Common errors:
# - "redis_library_not_available" → Install redis-py
# - "failed_to_initialize_consumer" → Check Redis connection
# - "consumer_not_enabled" → Check config: enabled: true
```

**Solution**:
```bash
# Verify Redis connection
docker exec nlp-event-consumer python -c "
import redis
r = redis.Redis(host='redis-cache', port=6379, db=1)
print(r.ping())
"

# Rebuild container
cd ../stage2-nlp-processing
docker compose -f docker-compose.infrastructure.yml build event-consumer
docker compose -f docker-compose.infrastructure.yml up -d event-consumer
```

### Issue 2: Events Not Being Consumed

**Symptoms**:
```bash
# Stream has events but consumer metrics show 0
docker exec storytelling-redis-cache redis-cli -n 1 XLEN "stage1:cleaning:events"
# Output: 350

docker logs nlp-event-consumer | grep "total_consumed"
# Output: total_consumed=0
```

**Debug**:
```bash
# Check consumer group exists
docker exec storytelling-redis-cache redis-cli -n 1 \
  XINFO GROUPS "stage1:cleaning:events"

# Check consumer is reading
docker logs nlp-event-consumer -f | grep "consuming_events"
```

**Solution**:
```bash
# Verify config
docker exec nlp-event-consumer cat /app/config/settings.yaml | grep -A 20 "event_consumer:"

# Check enabled: true
# Check source_stream matches

# Reset consumer group (start from beginning)
docker exec storytelling-redis-cache redis-cli -n 1 \
  XGROUP SETID stage1:cleaning:events stage2-nlp-processor 0
```

### Issue 3: Duplicate Processing

**Symptoms**:
```bash
# Same document processed multiple times
docker logs nlp-orchestrator | grep "document_id=abc123" | wc -l
# Output: 5  # Should be 1
```

**Debug**:
```bash
# Check idempotency setting
docker exec nlp-event-consumer python -c "
from src.events.consumer import get_event_consumer
consumer = get_event_consumer()
print(f'check_already_processed: {consumer.check_already_processed}')
"
```

**Solution**:
```bash
# Enable idempotency
# Edit config/settings.yaml:
#   check_already_processed: true
#   deduplication_ttl_hours: 24

# Restart consumer
docker restart nlp-event-consumer
```

### Issue 4: High Consumer Lag

**Symptoms**:
```bash
# Large number of pending events
docker exec storytelling-redis-cache redis-cli -n 1 \
  XPENDING stage1:cleaning:events stage2-nlp-processor
# Output: 1000 pending events
```

**Debug**:
```bash
# Check processing speed
docker logs nlp-event-consumer | grep "processed" | tail -20

# Check if consumer is stuck
docker logs nlp-event-consumer -f
```

**Solution**:
```bash
# Scale up batch size
# Edit config/settings.yaml:
#   batch_size: 50  # Increase from 10
#   concurrent_tasks: 8  # Increase from 4

# Or scale event consumer replicas (if supported)
docker compose -f docker-compose.infrastructure.yml up -d --scale event-consumer=3
```

---

## Production Deployment Checklist

### Pre-Deployment

- [ ] **Configuration Review**
  - [ ] `enabled: true` in `config/settings.yaml`
  - [ ] `auto_process: true` for automatic triggering
  - [ ] Event filtering configured (consume_events)
  - [ ] Idempotency enabled (`check_already_processed: true`)
  - [ ] Dead letter queue configured

- [ ] **Infrastructure Verification**
  - [ ] Redis cache (DB 1) accessible from Stage 2
  - [ ] Stage 1 PostgreSQL accessible (if reading from DB)
  - [ ] `/shared/stage1/` directory mounted and readable
  - [ ] Stage 2 NLP services running and healthy

- [ ] **Testing**
  - [ ] Test with dry run (`auto_process: false`)
  - [ ] Test with small batch (10-20 documents)
  - [ ] Verify idempotency (process same event twice)
  - [ ] Test error handling (dead letter queue)

### Deployment

- [ ] **Start Event Consumer**
  ```bash
  cd ../stage2-nlp-processing
  docker compose -f docker-compose.infrastructure.yml up -d event-consumer
  ```

- [ ] **Verify Health**
  ```bash
  docker ps --filter "name=nlp-event-consumer"
  docker logs nlp-event-consumer --tail 100
  ```

- [ ] **Monitor Initial Processing**
  - [ ] Watch first 100 events
  - [ ] Verify no errors
  - [ ] Check processing latency

### Post-Deployment

- [ ] **Set Up Alerts**
  - [ ] Consumer group lag > 1000 events
  - [ ] Consumer service down
  - [ ] Dead letter queue growth

- [ ] **Monitor Metrics**
  - [ ] Events consumed per second
  - [ ] Processing success rate
  - [ ] Event-to-completion latency

- [ ] **Documentation**
  - [ ] Update runbooks
  - [ ] Document common issues
  - [ ] Share with team

---

## Advanced Configuration

### Process Historical Events (Backlog)

```yaml
# config/settings.yaml
event_consumer:
  enabled: true
  process_backlog: true  # Enable backlog processing
  backlog_batch_size: 100  # Process 100 events at a time
  backlog_max_age_hours: 24  # Only events < 24h old
```

### Filter Specific Event Types

```yaml
# Only process batch completion events
event_consumer:
  consume_events:
    - "com.storytelling.cleaning.job.completed"  # Batches only
```

### Disable Auto-Processing (Manual Trigger)

```yaml
event_consumer:
  enabled: true
  auto_process: false  # Log events but don't trigger processing
```

### Custom Consumer Group Name

```yaml
event_consumer:
  consumer_group: "stage2-nlp-processor-prod"  # Custom name
```

---

## Architecture Diagrams

### Event Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Stage 1: Data Cleaning Service                                  │
│                                                                  │
│  ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐  │
│  │ CLI/API      │───▶│ Celery Workers  │───▶│ Event        │  │
│  │ (Batch Job)  │    │ (Clean Docs)    │    │ Publisher    │  │
│  └──────────────┘    └─────────────────┘    └──────┬───────┘  │
│                                                     │           │
└─────────────────────────────────────────────────────┼───────────┘
                                                      │
                                         CloudEvent: job.completed
                                                      │
                                                      ▼
                        ┌─────────────────────────────────────────┐
                        │ Redis Stream: stage1:cleaning:events    │
                        │ (DB 1)                                  │
                        └──────────────┬──────────────────────────┘
                                       │
                           XREADGROUP (Consumer Group)
                                       │
┌─────────────────────────────────────┼───────────────────────────┐
│ Stage 2: NLP Processing Service    │                           │
│                                     ▼                           │
│                          ┌─────────────────────┐                │
│                          │ Event Consumer      │                │
│                          │ Service             │                │
│                          └──────────┬──────────┘                │
│                                     │                           │
│                            Trigger Processing                   │
│                                     │                           │
│                   ┌─────────────────┴─────────────────┐         │
│                   ▼                                   ▼         │
│        ┌─────────────────────┐          ┌─────────────────┐    │
│        │ Orchestrator API    │          │ Celery Workers  │    │
│        │ (Batch Submission)  │─────────▶│ (NLP Pipeline)  │    │
│        └─────────────────────┘          └─────────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Next Steps

### Immediate (Required)

1. **Enable Consumer**: Set `enabled: true` in Stage 2 config
2. **Start Service**: Deploy event consumer Docker service
3. **Test Integration**: Run Stage 1 job and verify Stage 2 processes

### Short-Term (Recommended)

1. **Implement NLP Trigger**: Complete `trigger_nlp_batch_processing()` function
2. **Add Tests**: Create integration tests for event consumption
3. **Set Up Monitoring**: Configure alerts for consumer lag

### Long-Term (Optional)

1. **Optimize Performance**: Tune batch sizes, concurrency
2. **Implement Backpressure**: Pause Stage 1 if Stage 2 lags
3. **Add Replay Capability**: Reprocess events from specific timestamp

---

**Status**: ✅ READY FOR TESTING

The integration is fully implemented and documented. Enable the event consumer to start automatic Stage 1 → Stage 2 processing.

For questions or issues, refer to the Troubleshooting section above.
