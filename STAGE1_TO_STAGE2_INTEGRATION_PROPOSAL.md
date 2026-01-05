# Stage 1 → Stage 2 Event-Driven Integration Proposal

**Date**: January 5, 2026
**Status**: Proposal - Awaiting User Decision
**Author**: Claude Code

---

## Executive Summary

This document proposes an event-driven integration between Stage 1 (Data Cleaning) and Stage 2 (NLP Processing) to enable automatic downstream processing when documents are cleaned.

**Current State**:
- ✅ Stage 1 publishes CloudEvents to Redis Stream (`stage1:cleaning:events`)
- ✅ 350 events currently in stream
- ❌ Stage 2 has NO event consumer (only publishes events)
- ❌ Stage 2 must be manually triggered via API

**Proposed State**:
- ✅ Stage 2 subscribes to Stage 1 events
- ✅ Automatic NLP processing when documents are cleaned
- ✅ Configurable auto-processing (enable/disable)
- ✅ Backlog processing for historical events

---

## Current Architecture Analysis

### Stage 1 (Data Cleaning) - Event Publishing

**Status**: ✅ **FULLY IMPLEMENTED AND WORKING**

**Event Stream**: `stage1:cleaning:events` (Redis DB 1)

**Published Events**:
1. `com.storytelling.cleaning.document.cleaned` - Individual document cleaned
2. `com.storytelling.cleaning.job.completed` - Batch job completed

**Sample Event**:
```json
{
  "specversion": "1.0",
  "type": "com.storytelling.cleaning.job.completed",
  "source": "stage1-cleaning-pipeline",
  "id": "088f1957-5c19-4953-81d9-a9f5de3c6b47",
  "time": "2026-01-05T16:27:08.268110Z",
  "subject": "job/4c97a203-b96a-47d8-88e3-abce1b9855a5",
  "datacontenttype": "application/json",
  "data": {
    "job_id": "4c97a203-b96a-47d8-88e3-abce1b9855a5",
    "batch_id": "e2e_validation_20260105_162622",
    "documents_processed": 100,
    "documents_failed": 0,
    "documents_total": 100,
    "processing_time_ms": 41150.82
  }
}
```

**Implementation**: `src/events/event_publisher.py` (multi-backend publisher)

### Stage 2 (NLP Processing) - Current Gaps

**Status**: ❌ **NO EVENT CONSUMER IMPLEMENTED**

**Existing Capabilities**:
- ✅ Event publishing (`src/events/publisher.py`)
- ❌ No event consumption
- ❌ No Redis Stream listener
- ❌ No automatic trigger mechanism

**Current Processing Model**: Manual API calls only
```bash
# Current: Manual trigger required
curl -X POST "http://localhost/api/v2/nlp/process" \
  -d '{"document_id": "...", "text": "..."}'
```

---

## Proposed Integration Architecture

### Option A: Consumer Service Pattern (Recommended)

**Architecture**:
```
Stage 1: Cleaning Service
    ↓ (publishes to)
Redis Stream: stage1:cleaning:events
    ↓ (consumed by)
Stage 2: Event Consumer Service (NEW)
    ↓ (triggers)
Stage 2: NLP Processing API
    ↓ (publishes to)
Redis Stream: stage2:nlp:events
```

**Components to Implement**:

1. **Event Consumer Service** (`src/events/consumer.py`)
   - Subscribe to `stage1:cleaning:events` stream
   - Read events using `XREADGROUP` (consumer group pattern)
   - Parse CloudEvents and extract document metadata
   - Trigger NLP processing via internal API

2. **Consumer Celery Worker** (new Docker service)
   - Dedicated worker for event consumption
   - Runs independently of processing workers
   - Handles backlog processing
   - Auto-restart on failure

3. **Configuration** (`config/settings.yaml`)
   ```yaml
   event_consumer:
     enabled: true  # Enable/disable auto-processing

     # Source stream (Stage 1)
     source_stream: "stage1:cleaning:events"
     consumer_group: "stage2-nlp-processor"
     consumer_name: "worker-1"

     # Event filtering
     consume_events:
       - "com.storytelling.cleaning.document.cleaned"
       - "com.storytelling.cleaning.job.completed"

     # Processing behavior
     auto_process: true  # Trigger processing automatically
     batch_mode: true    # Process job completion events as batches
     retry_failed: true  # Retry failed events
     max_retries: 3

     # Performance
     poll_interval_ms: 1000
     batch_size: 10  # Process N events at once

     # Backlog handling
     process_backlog: false  # Process historical events on startup
     backlog_batch_size: 100
   ```

**Pros**:
- ✅ Clean separation of concerns
- ✅ Independent scaling (consumer vs processing)
- ✅ Can disable auto-processing without code changes
- ✅ Handles backlog processing
- ✅ Fault-tolerant (consumer groups)

**Cons**:
- ⚠️ Additional service to manage
- ⚠️ Slight complexity increase

### Option B: Hybrid Pattern (Direct Trigger)

**Architecture**:
```
Stage 1: Cleaning Service
    ↓ (publishes to)
Redis Stream: stage1:cleaning:events
    +
    ↓ (webhook callback)
Stage 2: NLP Processing API (/v2/events/stage1)
    ↓ (triggers)
Stage 2: NLP Processing Workers
```

**Components to Implement**:

1. **Webhook Endpoint** (`src/api/event_webhook.py`)
   - New endpoint: `POST /v2/events/stage1`
   - Receives event notifications
   - Validates CloudEvent schema
   - Triggers NLP processing

2. **Stage 1 Configuration Update**
   ```yaml
   # Stage 1: config/settings.yaml
   events:
     backends:
       - type: "redis_streams"
         enabled: true
       - type: "webhook"
         enabled: true
         config:
           urls:
             - "http://nlp-orchestrator:8000/v2/events/stage1"
           timeout_seconds: 30
           retry_attempts: 3
   ```

**Pros**:
- ✅ Simpler architecture (no new service)
- ✅ Direct push model (lower latency)
- ✅ Easier to debug

**Cons**:
- ⚠️ No backlog processing
- ⚠️ Tight coupling between stages
- ⚠️ Network dependency (webhook must be reachable)
- ⚠️ No fault tolerance if Stage 2 is down

### Option C: Hybrid Pattern (Stream + Webhook)

**Architecture**: Combine both approaches
- Redis Stream for reliability and backlog
- Webhook for low-latency notifications

**Best of both worlds**, but highest complexity.

---

## Implementation Plan

### Phase 1: Core Event Consumer (Option A)

**Tasks**:

1. **Create Event Consumer** (`stage2/src/events/consumer.py`)
   - Implement `RedisStreamConsumer` class
   - XREADGROUP with consumer groups
   - Event validation and parsing
   - Error handling and retry logic

2. **Create Consumer Service** (`stage2/src/services/event_consumer_service.py`)
   - Background service that runs event consumer
   - Triggers NLP processing for consumed events
   - Health monitoring
   - Graceful shutdown

3. **Add Docker Service** (`stage2/docker-compose.infrastructure.yml`)
   ```yaml
   event-consumer:
     build:
       context: .
       dockerfile: Dockerfile_orchestrator
     container_name: nlp-event-consumer
     command: python -m src.services.event_consumer_service
     environment:
       - EVENT_CONSUMER_ENABLED=true
       - STAGE1_EVENT_STREAM=stage1:cleaning:events
     depends_on:
       - nlp-orchestrator
   ```

4. **Update Configuration** (`stage2/config/settings.yaml`)
   - Add `event_consumer` section
   - Configure source stream
   - Set processing behavior

5. **Testing**
   - Unit tests for consumer logic
   - Integration tests with mock events
   - E2E test: Stage 1 → Stage 2 pipeline

### Phase 2: Backlog Processing

**Tasks**:

1. **Backlog Processor** (`stage2/src/services/backlog_processor.py`)
   - One-time processing of historical events
   - Batch processing for efficiency
   - Progress tracking

2. **CLI Command** (`stage2/src/cli/`)
   ```bash
   python -m src.cli process-backlog \
     --stream stage1:cleaning:events \
     --start-id 0 \
     --batch-size 100
   ```

3. **Idempotency Checks**
   - Prevent duplicate processing
   - Check if document already processed
   - Skip or update based on policy

### Phase 3: Monitoring & Observability

**Tasks**:

1. **Metrics**
   - Events consumed per second
   - Processing latency (event → completion)
   - Failed events count
   - Retry attempts

2. **Health Checks**
   - Consumer service liveness
   - Stream lag monitoring
   - Alert on consumer group lag > threshold

3. **Logging**
   - Structured logs for event consumption
   - Trace event flow (Stage 1 → Stage 2)
   - Error context for failed events

---

## Data Flow Example

### Scenario: E2E Pipeline (100 Articles)

**Stage 1** (Data Cleaning):
```bash
# User submits batch job
docker exec cleaning-orchestrator python -m src.main_cli batch submit \
  -f /app/data/input.jsonl \
  -b "batch_20260105"

# Stage 1 processes articles
# ✅ Publishes event: com.storytelling.cleaning.job.completed
```

**Redis Stream**:
```
stage1:cleaning:events: [
  {
    "type": "com.storytelling.cleaning.job.completed",
    "data": {
      "job_id": "...",
      "batch_id": "batch_20260105",
      "documents_processed": 100,
      "output_location": "/shared/stage1/batch_20260105.jsonl"
    }
  }
]
```

**Stage 2** (NLP Processing - Automatic):
```
Event Consumer (continuously running):
  1. XREADGROUP on stage1:cleaning:events
  2. Receives job.completed event
  3. Reads cleaned documents from /shared/stage1/batch_20260105.jsonl
  4. Submits batch NLP processing job:
     curl -X POST http://localhost:8000/v2/process/batch \
       -d '{"input_file": "/shared/stage1/batch_20260105.jsonl"}'
  5. Stage 2 processes 100 articles (NER, DP, Event Extraction)
  6. Publishes: com.storytelling.nlp.job.completed
```

**Stage 3** (Embeddings - Automatic):
```
Event Consumer:
  - Consumes stage2:nlp:events
  - Triggers embedding generation
  - ...and so on through Stage 8
```

---

## Configuration Examples

### Minimal Configuration (Auto-Processing Enabled)

```yaml
# stage2/config/settings.yaml
event_consumer:
  enabled: true
  source_stream: "stage1:cleaning:events"
  consumer_group: "stage2-nlp-processor"
  auto_process: true
```

### Advanced Configuration (Fine-Tuned)

```yaml
# stage2/config/settings.yaml
event_consumer:
  enabled: true

  # Source stream
  source_stream: "stage1:cleaning:events"
  redis_host: "redis-cache"
  redis_port: 6379
  redis_db: 1

  # Consumer group settings
  consumer_group: "stage2-nlp-processor"
  consumer_name: "${HOSTNAME}"  # Unique per worker

  # Event filtering (optional - consume all if not specified)
  consume_events:
    - "com.storytelling.cleaning.document.cleaned"
    - "com.storytelling.cleaning.job.completed"

  # Processing behavior
  auto_process: true  # Trigger processing automatically
  batch_mode: true    # Batch process job.completed events

  # Performance tuning
  poll_interval_ms: 1000  # Check for new events every 1s
  batch_size: 10  # Process 10 events at once
  concurrent_tasks: 4  # Parallel processing

  # Error handling
  retry_failed: true
  max_retries: 3
  retry_delay_seconds: 5
  dead_letter_stream: "stage2:nlp:failed-events"

  # Backlog handling
  process_backlog: false  # Don't process historical events
  backlog_batch_size: 100
  backlog_max_age_hours: 24  # Only process events < 24h old

  # Idempotency
  check_already_processed: true  # Skip if already processed
  deduplication_ttl_hours: 24
```

### Disable Auto-Processing (Manual Mode)

```yaml
# stage2/config/settings.yaml
event_consumer:
  enabled: false  # Completely disable event consumer

  # OR keep logging but don't process
  enabled: true
  auto_process: false  # Log events but don't trigger processing
```

---

## Benefits

### Performance Benefits
- **Reduced Latency**: Stage 2 starts processing immediately after Stage 1 completes
- **Parallel Processing**: Multiple stages can process different batches simultaneously
- **Throughput**: No manual intervention bottleneck

### Operational Benefits
- **Automation**: End-to-end pipeline runs without human intervention
- **Reliability**: Consumer groups ensure at-least-once delivery
- **Observability**: Full event trail from Stage 1 → Stage 8
- **Flexibility**: Enable/disable auto-processing via config

### Development Benefits
- **Loose Coupling**: Stages communicate via events, not direct API calls
- **Scalability**: Independent scaling of consumers vs processors
- **Testing**: Easy to replay events for testing

---

## Risks & Mitigation

### Risk 1: Event Backlog Explosion

**Scenario**: Stage 2 slower than Stage 1, backlog grows indefinitely

**Mitigation**:
- Monitor consumer group lag (alert if > 1000 events)
- Implement backpressure (pause Stage 1 if Stage 2 lag > threshold)
- Scale Stage 2 workers dynamically

### Risk 2: Duplicate Processing

**Scenario**: Event consumer crashes mid-processing, re-consumes same event

**Mitigation**:
- Idempotency checks (track processed document IDs)
- Redis set for deduplication: `stage2:processed:{document_id}` (24h TTL)
- Atomic XACK after successful processing

### Risk 3: Stage 2 Downtime

**Scenario**: Stage 2 offline, events accumulate in stream

**Mitigation**:
- Events persist in Redis (up to 10,000 events, 24h TTL)
- Consumer catches up when back online
- Backlog processing script for large gaps

### Risk 4: Configuration Errors

**Scenario**: Wrong stream name, consumer can't find events

**Mitigation**:
- Health check validates stream existence
- Config validation on startup
- Alert if no events consumed in 5 minutes

---

## Decision Matrix

| Criterion | Option A (Consumer Service) | Option B (Webhook) | Option C (Hybrid) |
|-----------|----------------------------|-------------------|------------------|
| **Reliability** | ✅ High (consumer groups) | ⚠️ Medium (network dependent) | ✅ High |
| **Latency** | ⚠️ Medium (~1s poll) | ✅ Low (<100ms) | ✅ Low |
| **Complexity** | ⚠️ Medium (new service) | ✅ Low (endpoint only) | ❌ High |
| **Backlog Support** | ✅ Yes | ❌ No | ✅ Yes |
| **Fault Tolerance** | ✅ High | ❌ Low | ✅ High |
| **Coupling** | ✅ Loose | ⚠️ Tight | ✅ Loose |
| **Scalability** | ✅ Independent | ⚠️ Coupled | ✅ Independent |
| **Dev Effort** | ⚠️ Medium (2-3 days) | ✅ Low (1 day) | ❌ High (4-5 days) |

**Recommendation**: **Option A (Consumer Service)** for production reliability.

---

## Questions for User

Before proceeding with implementation, please decide:

### 1. Which Option?
- [ ] **Option A**: Consumer Service Pattern (recommended for production)
- [ ] **Option B**: Webhook Pattern (simpler, faster to implement)
- [ ] **Option C**: Hybrid Pattern (most features, highest complexity)

### 2. Auto-Processing Behavior
- [ ] **Always ON**: Stage 2 automatically processes all Stage 1 events
- [ ] **Configurable**: Can enable/disable via `event_consumer.enabled` config
- [ ] **Manual Trigger**: Consume events but require explicit trigger

### 3. Backlog Processing
- [ ] **Process Historical Events**: On first deployment, process all 350 existing events
- [ ] **Start Fresh**: Only process events from now onwards

### 4. Event Types to Consume
- [ ] **Both**: `document.cleaned` (individual) + `job.completed` (batch)
- [ ] **Batch Only**: Only `job.completed` (more efficient)
- [ ] **Individual Only**: Only `document.cleaned` (real-time)

### 5. Testing Approach
- [ ] **Test with Real Data**: Use existing 350 events in stream
- [ ] **Test with Synthetic Data**: Create test events
- [ ] **Gradual Rollout**: Start with `auto_process: false`, enable after validation

### 6. Shared Data Location
- [ ] **Use `/shared/stage1/`**: Stage 1 writes cleaned files here, Stage 2 reads
- [ ] **Embed in Event**: Include cleaned text in event payload (limited to small docs)
- [ ] **Database**: Stage 2 reads from PostgreSQL `processed_articles` table

### 7. Implementation Timeline
- [ ] **Immediate**: Start implementation now (estimated 2-3 days for Option A)
- [ ] **Phased**: Phase 1 (core consumer) first, Phase 2 (backlog) later
- [ ] **Deferred**: Document proposal, implement later

---

## Next Steps

**After you answer the questions above, I will**:

1. Create detailed implementation plan
2. Generate all required code files:
   - `stage2/src/events/consumer.py`
   - `stage2/src/services/event_consumer_service.py`
   - Configuration updates
   - Docker compose changes
   - Tests

3. Set up monitoring and health checks
4. Create E2E test for Stage 1 → Stage 2 pipeline
5. Document operational procedures

**Estimated Implementation Time**:
- Option A: 2-3 days (core consumer + tests)
- Option B: 1 day (webhook endpoint + tests)
- Option C: 4-5 days (full hybrid implementation)

---

**Status**: ⏸️ Awaiting User Decision

Please review the options and answer the 7 questions above so I can proceed with the implementation that best fits your needs.
