#!/bin/bash

BASE_URL="http://localhost:8080"
# Array to store all created Job IDs
JOB_IDS=()

# Polling and Timeout Configuration
MAX_WAIT_TIME=120  # Max wait time in seconds (2 minutes)
POLL_INTERVAL=2    # Time to wait between status checks (2 seconds)

# Function to extract a value from a JSON string using grep and sed
# Arguments: $1 = JSON String, $2 = JSON Key (e.g., "id")
extract_json_value() {
  local JSON_STRING=$1
  local KEY=$2
  # Uses sed -E (Extended Regex) to capture the text specifically between quotes after the key.
  echo "$JSON_STRING" | grep "\"$KEY\":" | sed -E 's/.*"'"$KEY"'"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/'
}

# Function to create a job and store its ID
create_job_and_store_id() {
  local PAYLOAD=$1
  local RESPONSE=$(curl -s -X POST "$BASE_URL/jobs" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")

  # Extract Job ID using grep/sed
  local JOB_ID=$(extract_json_value "$RESPONSE" "id")

  if [ -z "$JOB_ID" ]; then
    echo "FATAL ERROR: Could not create job or parse ID from response."
    echo "Response: $RESPONSE"
    exit 1
  fi

  JOB_IDS+=("$JOB_ID")
  echo "Created job ID: $JOB_ID"
}

# NEW FUNCTION: Waits for all jobs to complete or until timeout
wait_for_jobs_to_complete() {
  local start_time=$SECONDS
  local total_jobs=${#JOB_IDS[@]}

  echo "Starting wait loop (Max: $MAX_WAIT_TIME sec, Poll: $POLL_INTERVAL sec)..."

  # Infinite loop that only exits via return
  while :; do
    local elapsed=$((SECONDS - start_time))
    local completed_jobs=0

    # 1. Check for timeout
    if [ "$elapsed" -ge "$MAX_WAIT_TIME" ]; then
      echo "--- ⚠️ TIMEOUT REACHED ---"
      echo "Exceeded $MAX_WAIT_TIME seconds limit. Proceeding to final verification."
      return 1 # Signal timeout
    fi

    # 2. Poll all job statuses
    for JOB_ID in "${JOB_IDS[@]}"; do
      STATUS_RESPONSE=$(curl -s -X GET "$BASE_URL/jobs/$JOB_ID")
      STATUS=$(extract_json_value "$STATUS_RESPONSE" "status")

      # COMPLETED and FAILED statuses count as "finished"
      if [ "$STATUS" == "COMPLETED" ] || [ "$STATUS" == "FAILED" ]; then
        completed_jobs=$((completed_jobs + 1))
      fi
    done

    # 3. Check for total completion
    if [ "$completed_jobs" -eq "$total_jobs" ]; then
      echo "--- ✅ ALL JOBS FINISHED ---"
      echo "All $total_jobs jobs completed/failed successfully after $elapsed seconds."
      return 0 # Signal success
    fi

    echo "Status: $completed_jobs/$total_jobs jobs finished. Elapsed: $elapsed sec. Waiting $POLL_INTERVAL sec..."
    sleep $POLL_INTERVAL
  done
}

echo "=== Distributed Task Scheduler Test ==="
echo ""

echo "1. Creating a high-priority sleep job (5s duration)..."
create_job_and_store_id '{"type":"sleep","payload":{"duration":5},"priority":7}'
echo ""

echo "2. Creating 3 priority jobs (2s duration)..."
for i in {1..300}; do
  PRIORITY=$((1 + RANDOM % 10))
  create_job_and_store_id "{\"type\":\"sleep\",\"payload\":{\"duration\":2},\"priority\":$PRIORITY}"
done
echo ""

# --- Polling Check Replaces Fixed Sleep ---
wait_for_jobs_to_complete
# Exit code from wait_for_jobs_to_complete is ignored here, as the final summary handles success/failure
echo ""

echo "=== Verifying Job Statuses ==="
SUCCESS_COUNT=0
TOTAL_COUNT=${#JOB_IDS[@]}

for JOB_ID in "${JOB_IDS[@]}"; do
  # Poll the specific job status endpoint for final state logging
  STATUS_RESPONSE=$(curl -s -X GET "$BASE_URL/jobs/$JOB_ID")

  STATUS=$(extract_json_value "$STATUS_RESPONSE" "status")

  if [ "$STATUS" == "COMPLETED" ]; then
    echo "✅ Job $JOB_ID: COMPLETED successfully."
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  elif [ "$STATUS" == "FAILED" ]; then
    ERROR_MSG=$(extract_json_value "$STATUS_RESPONSE" "error")
    echo "❌ Job $JOB_ID: FAILED. Error: $ERROR_MSG"
  elif [ "$STATUS" == "PENDING" ] || [ "$STATUS" == "RUNNING" ]; then
    echo "⚠️ Job $JOB_ID: Status is $STATUS. Did not complete within the $MAX_WAIT_TIME second limit."
  else
    echo "❓ Job $JOB_ID: Could not retrieve status. Raw value extracted: '$STATUS'"
  fi
done

echo ""
echo "--- Summary ---"
if [ "$SUCCESS_COUNT" -eq "$TOTAL_COUNT" ]; then
  echo "✅ ALL $TOTAL_COUNT jobs completed successfully! Test Passed."
  exit 0
else
  echo "❌ $SUCCESS_COUNT out of $TOTAL_COUNT jobs completed successfully. Test Failed."
  exit 1
fi