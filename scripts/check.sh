#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
terraform fmt -check -recursive infra
