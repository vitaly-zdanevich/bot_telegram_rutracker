#!/usr/bin/env bash
set -euo pipefail

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
  python3 - "$1" <<'PY'
import re
import sys
import time

value = sys.argv[1].strip()
match = re.fullmatch(r"(\d+)([smhd]?)", value)
if not match:
    raise SystemExit("SINCE must look like 30m, 1h, 2d, or raw seconds")

amount = int(match.group(1))
unit = match.group(2) or "s"
multiplier = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
}[unit]
print(int((time.time() - amount * multiplier) * 1000))
PY
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
    start_time="$(python3 -c 'import time; print(int(time.time() * 1000))')"
    sleep "$POLL_SECONDS"
  done
fi

print_events_since "$start_time"
