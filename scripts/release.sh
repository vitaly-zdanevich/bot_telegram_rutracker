#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--middle] \"commit message\"" >&2
}

increment_middle=0
if [[ "${1:-}" == "--middle" ]]; then
  increment_middle=1
  shift
fi

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

message="$1"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "release.sh must be run inside a git repository" >&2
  exit 1
fi

new_version="$(python3 - "$increment_middle" <<'PY'
import re
import sys
from pathlib import Path

middle = sys.argv[1] == "1"
path = Path("Cargo.toml")
text = path.read_text()
match = re.search(r'(?m)^version\s*=\s*"(\d+)\.(\d+)\.(\d+)"', text)
if not match:
    raise SystemExit("Cargo.toml version not found")
major, minor, patch = map(int, match.groups())
if middle:
    minor += 1
    patch = 0
else:
    patch += 1
version = f"{major}.{minor}.{patch}"
text = text[:match.start()] + f'version = "{version}"' + text[match.end():]
path.write_text(text)
print(version)
PY
)"

cargo generate-lockfile

git add Cargo.toml Cargo.lock
git commit -m "$message"
git tag "${new_version}"
git push
git push origin "${new_version}"

echo "Released ${new_version}"
