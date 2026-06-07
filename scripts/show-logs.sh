#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
AWS_REGION="${AWS_REGION:-eu-north-1}"
PROJECT_NAME="${PROJECT_NAME:-telegram-rutracker-bot}"
SINCE="${SINCE:-1h}"
LIMIT="${LIMIT:-200}"
FOLLOW="${FOLLOW:-0}"
POLL_SECONDS="${POLL_SECONDS:-5}"
LOG_GROUP="/aws/lambda/${PROJECT_NAME}"

if aws logs tail help >/dev/null 2>&1; then
  args=(logs tail "$LOG_GROUP" --region "$AWS_REGION" --since "$SINCE")
  if [[ "$FOLLOW" == "1" ]]; then
    args+=(--follow)
  fi
  exec aws "${args[@]}"
fi

since_to_start_time_ms() {
  local since="$1"

  python3 "$SCRIPT_DIR/since-to-start-time-ms.py" "$since"
}

print_events_since() {
  local start_time="$1"

  aws logs filter-log-events \
    --log-group-name "$LOG_GROUP" \
    --region "$AWS_REGION" \
    --start-time "$start_time" \
    --limit "$LIMIT" \
    --query 'events[].message' \
    --output text
}

start_time="$(since_to_start_time_ms "$SINCE")"

if [[ "$FOLLOW" == "1" ]]; then
  while true; do
    print_events_since "$start_time"
    start_time="$(since_to_start_time_ms 0s)"
    sleep "$POLL_SECONDS"
  done
fi

print_events_since "$start_time"
