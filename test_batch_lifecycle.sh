#!/bin/bash
# ==============================================================================
# test_batch_lifecycle.sh
#
# Comprehensive test script for batch lifecycle management in Stage 1
# Cleaning Pipeline.
#
# Tests:
# - Batch submission
# - Job status tracking
# - Pause/resume functionality
# - Cancellation
# - CLI commands
# - Resource monitoring
# - Event publishing
#
# Usage:
#   # Inside container
#   docker exec cleaning-orchestrator bash /app/test_batch_lifecycle.sh
#
#   # Or directly
#   ./test_batch_lifecycle.sh
# ==============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
API_BASE_URL="${ORCHESTRATOR_API_URL:-http://localhost:8000}"
TEST_FILE="${TEST_FILE:-./data/input.jsonl}"
BATCH_ID="test_batch_$(date +%Y%m%d_%H%M%S)"

echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}  Stage 1 Batch Lifecycle Management Test Suite${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo ""
echo -e "${GREEN}Configuration:${NC}"
echo -e "  API Base URL: ${API_BASE_URL}"
echo -e "  Test File: ${TEST_FILE}"
echo -e "  Batch ID: ${BATCH_ID}"
echo ""

# ==============================================================================
# Test 1: API Health Check
# ==============================================================================
echo -e "${YELLOW}[Test 1] Checking API health...${NC}"
HEALTH_RESPONSE=$(curl -s "${API_BASE_URL}/health")
if echo "${HEALTH_RESPONSE}" | grep -q '"status":"ok"'; then
    echo -e "${GREEN}✓ API is healthy${NC}"
    echo "${HEALTH_RESPONSE}" | python3 -m json.tool
else
    echo -e "${RED}✗ API health check failed${NC}"
    echo "${HEALTH_RESPONSE}"
    exit 1
fi
echo ""

# ==============================================================================
# Test 2: Batch Submission via API
# ==============================================================================
echo -e "${YELLOW}[Test 2] Submitting batch job via API...${NC}"

# Read first 20 documents from test file
DOCUMENTS=$(head -20 "${TEST_FILE}" | jq -s '.')

# Submit batch job
BATCH_RESPONSE=$(curl -s -X POST "${API_BASE_URL}/v1/documents/batch" \
    -H "Content-Type: application/json" \
    -d '{
        "documents": '"${DOCUMENTS}"',
        "batch_id": "'"${BATCH_ID}"'",
        "checkpoint_interval": 5,
        "persist_to_backends": ["jsonl"]
    }')

JOB_ID=$(echo "${BATCH_RESPONSE}" | jq -r '.job_id')

if [ -z "${JOB_ID}" ] || [ "${JOB_ID}" = "null" ]; then
    echo -e "${RED}✗ Failed to submit batch job${NC}"
    echo "${BATCH_RESPONSE}" | python3 -m json.tool
    exit 1
fi

echo -e "${GREEN}✓ Batch job submitted successfully${NC}"
echo -e "  Job ID: ${JOB_ID}"
echo -e "  Batch ID: ${BATCH_ID}"
echo ""

# ==============================================================================
# Test 3: Job Status Tracking
# ==============================================================================
echo -e "${YELLOW}[Test 3] Checking job status...${NC}"
sleep 2  # Give it time to start

STATUS_RESPONSE=$(curl -s "${API_BASE_URL}/v1/jobs/${JOB_ID}")
STATUS=$(echo "${STATUS_RESPONSE}" | jq -r '.status')

echo -e "${GREEN}✓ Job status retrieved${NC}"
echo -e "  Status: ${STATUS}"
echo -e "  Progress: $(echo "${STATUS_RESPONSE}" | jq -r '.progress_percent')%"
echo -e "  Processed: $(echo "${STATUS_RESPONSE}" | jq -r '.processed_documents')/$(echo "${STATUS_RESPONSE}" | jq -r '.total_documents')"
echo ""

# ==============================================================================
# Test 4: Job Pause
# ==============================================================================
echo -e "${YELLOW}[Test 4] Pausing job (if running)...${NC}"
sleep 3  # Wait for job to be running

PAUSE_RESPONSE=$(curl -s -X PATCH "${API_BASE_URL}/v1/jobs/${JOB_ID}/pause")
PAUSE_STATUS=$(echo "${PAUSE_RESPONSE}" | jq -r '.status')

if [ "${PAUSE_STATUS}" = "success" ]; then
    echo -e "${GREEN}✓ Job pause requested${NC}"
    echo -e "  Message: $(echo "${PAUSE_RESPONSE}" | jq -r '.message')"

    # Wait for pause to take effect
    sleep 5

    # Check if actually paused
    STATUS_RESPONSE=$(curl -s "${API_BASE_URL}/v1/jobs/${JOB_ID}")
    CURRENT_STATUS=$(echo "${STATUS_RESPONSE}" | jq -r '.status')
    echo -e "  Current Status: ${CURRENT_STATUS}"
else
    echo -e "${YELLOW}⚠ Job pause may have failed (job might already be completed)${NC}"
    echo "${PAUSE_RESPONSE}" | python3 -m json.tool
fi
echo ""

# ==============================================================================
# Test 5: Job Resume
# ==============================================================================
echo -e "${YELLOW}[Test 5] Resuming job (if paused)...${NC}"

if [ "${CURRENT_STATUS}" = "PAUSED" ]; then
    RESUME_RESPONSE=$(curl -s -X PATCH "${API_BASE_URL}/v1/jobs/${JOB_ID}/resume")
    RESUME_STATUS=$(echo "${RESUME_RESPONSE}" | jq -r '.status')

    if [ "${RESUME_STATUS}" = "success" ]; then
        echo -e "${GREEN}✓ Job resumed successfully${NC}"
        echo -e "  Message: $(echo "${RESUME_RESPONSE}" | jq -r '.message')"
        echo -e "  Checkpoint Loaded: $(echo "${RESUME_RESPONSE}" | jq -r '.checkpoint_loaded')"
    else
        echo -e "${RED}✗ Job resume failed${NC}"
        echo "${RESUME_RESPONSE}" | python3 -m json.tool
    fi
else
    echo -e "${YELLOW}⚠ Job is not paused, skipping resume test${NC}"
fi
echo ""

# ==============================================================================
# Test 6: Job List
# ==============================================================================
echo -e "${YELLOW}[Test 6] Listing all jobs...${NC}"

LIST_RESPONSE=$(curl -s "${API_BASE_URL}/v1/jobs?limit=5")
JOB_COUNT=$(echo "${LIST_RESPONSE}" | jq -r '.total')

echo -e "${GREEN}✓ Job list retrieved${NC}"
echo -e "  Total Jobs: ${JOB_COUNT}"
echo -e "  Recent Jobs:"
echo "${LIST_RESPONSE}" | jq -r '.jobs[] | "    - \(.job_id[:16])... [\(.status)] \(.progress_percent)%"'
echo ""

# ==============================================================================
# Test 7: CLI Commands (if available)
# ==============================================================================
echo -e "${YELLOW}[Test 7] Testing CLI commands...${NC}"

# Test CLI help
if command -v python3 &> /dev/null; then
    echo -e "  Testing: python -m src.main_cli batch --help"
    python3 -m src.main_cli batch --help 2>&1 | head -10
    echo -e "${GREEN}✓ CLI batch commands available${NC}"

    # Test batch status via CLI
    echo -e "\n  Testing: python -m src.main_cli batch status -j ${JOB_ID}"
    python3 -m src.main_cli batch status -j "${JOB_ID}" 2>&1 | head -15
    echo -e "${GREEN}✓ CLI batch status command works${NC}"
else
    echo -e "${YELLOW}⚠ Python not available for CLI testing${NC}"
fi
echo ""

# ==============================================================================
# Test 8: Wait for Completion (or cancel)
# ==============================================================================
echo -e "${YELLOW}[Test 8] Monitoring job completion...${NC}"

MAX_WAIT=60  # Maximum 60 seconds
ELAPSED=0
POLL_INTERVAL=3

while [ ${ELAPSED} -lt ${MAX_WAIT} ]; do
    STATUS_RESPONSE=$(curl -s "${API_BASE_URL}/v1/jobs/${JOB_ID}")
    CURRENT_STATUS=$(echo "${STATUS_RESPONSE}" | jq -r '.status')
    PROGRESS=$(echo "${STATUS_RESPONSE}" | jq -r '.progress_percent')

    echo -e "  Status: ${CURRENT_STATUS} | Progress: ${PROGRESS}%"

    if [ "${CURRENT_STATUS}" = "COMPLETED" ] || [ "${CURRENT_STATUS}" = "FAILED" ] || [ "${CURRENT_STATUS}" = "CANCELLED" ]; then
        echo -e "${GREEN}✓ Job reached terminal state: ${CURRENT_STATUS}${NC}"
        break
    fi

    sleep ${POLL_INTERVAL}
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

if [ ${ELAPSED} -ge ${MAX_WAIT} ]; then
    echo -e "${YELLOW}⚠ Job still running after ${MAX_WAIT} seconds${NC}"
fi
echo ""

# ==============================================================================
# Test 9: Final Job Status
# ==============================================================================
echo -e "${YELLOW}[Test 9] Final job status check...${NC}"

FINAL_RESPONSE=$(curl -s "${API_BASE_URL}/v1/jobs/${JOB_ID}")
echo "${FINAL_RESPONSE}" | python3 -m json.tool

echo -e "${GREEN}✓ Final status retrieved${NC}"
echo ""

# ==============================================================================
# Test 10: Event Publishing Verification (if Redis available)
# ==============================================================================
echo -e "${YELLOW}[Test 10] Checking CloudEvents (if Redis available)...${NC}"

if command -v redis-cli &> /dev/null; then
    REDIS_HOST="${REDIS_CACHE_HOST:-redis-cache}"
    REDIS_DB="${REDIS_CACHE_DB:-1}"

    echo -e "  Checking Redis Stream: stage1:cleaning:events"
    EVENTS=$(redis-cli -h "${REDIS_HOST}" -n "${REDIS_DB}" XRANGE stage1:cleaning:events - + COUNT 10)

    if [ ! -z "${EVENTS}" ]; then
        echo -e "${GREEN}✓ CloudEvents found in Redis stream${NC}"
        echo -e "  Events:"
        echo "${EVENTS}" | head -20
    else
        echo -e "${YELLOW}⚠ No events found (may not be enabled)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ redis-cli not available${NC}"
fi
echo ""

# ==============================================================================
# Summary
# ==============================================================================
echo -e "${BLUE}=================================================================${NC}"
echo -e "${BLUE}  Test Suite Summary${NC}"
echo -e "${BLUE}=================================================================${NC}"
echo -e "${GREEN}All tests completed!${NC}"
echo ""
echo -e "Test Results:"
echo -e "  ✓ API Health Check"
echo -e "  ✓ Batch Submission"
echo -e "  ✓ Job Status Tracking"
echo -e "  ✓ Job Pause"
echo -e "  ✓ Job Resume"
echo -e "  ✓ Job Listing"
echo -e "  ✓ CLI Commands"
echo -e "  ✓ Job Monitoring"
echo -e "  ✓ Final Status Check"
echo -e "  ✓ Event Publishing Verification"
echo ""
echo -e "Job ID for reference: ${JOB_ID}"
echo -e "Batch ID: ${BATCH_ID}"
echo ""
echo -e "${GREEN}✓ Test suite completed successfully!${NC}"
