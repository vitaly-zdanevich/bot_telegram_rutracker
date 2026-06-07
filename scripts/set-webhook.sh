#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

read_tfvar() {
  local name="$1"
  local file="$ROOT_DIR/infra/terraform.tfvars"

  [[ -f "$file" ]] || return 0

  python3 - "$name" "$file" <<'PY'
import ast
import re
import sys

name, path = sys.argv[1:]
text = open(path, encoding="utf-8").read()
pattern = re.compile(
    r"^\s*"
    + re.escape(name)
    + r'\s*=\s*("((?:\\.|[^"\\])*)"|(.+?))\s*(?:#.*)?$',
    re.MULTILINE,
)
match = pattern.search(text)
if not match:
    sys.exit(0)

if match.group(2) is not None:
    print(ast.literal_eval(match.group(1)))
else:
    print(match.group(3).strip())
PY
}

TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-${TF_VAR_telegram_bot_token:-$(read_tfvar telegram_bot_token)}}"
TELEGRAM_WEBHOOK_SECRET="${TELEGRAM_WEBHOOK_SECRET:-${TF_VAR_telegram_webhook_secret:-$(read_tfvar telegram_webhook_secret)}}"

: "${TELEGRAM_BOT_TOKEN:?TELEGRAM_BOT_TOKEN is required}"
: "${TELEGRAM_WEBHOOK_SECRET:?TELEGRAM_WEBHOOK_SECRET is required}"

FUNCTION_URL="${FUNCTION_URL:-$(terraform -chdir=infra output -raw function_url)}"

curl -fsS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -F "url=${FUNCTION_URL}" \
  -F "secret_token=${TELEGRAM_WEBHOOK_SECRET}" \
  -F "allowed_updates=[\"message\",\"callback_query\",\"inline_query\"]"
