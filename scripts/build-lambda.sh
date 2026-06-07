#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEBHOOK_BIN_NAME="telegram-rutracker-bot"
WORKER_BIN_NAME="telegram-rutracker-worker"
TOOLS_DIR="$ROOT_DIR/.tools"
TARGET="aarch64-unknown-linux-gnu"
RUST_TARGET_CPU="${RUST_TARGET_CPU:-neoverse-n1}"
RUN_USER="${USER:-$(id -un 2>/dev/null || echo local)}"
SAFE_RUN_USER="${RUN_USER//[^a-zA-Z0-9_.-]/_}"
BUILD_CACHE_DIR="$ROOT_DIR/build/cache-$SAFE_RUN_USER"
CARGO_TARGET_DIR="$ROOT_DIR/build/target-$SAFE_RUN_USER"
LAMBDA_DIR="$ROOT_DIR/build/lambda-$SAFE_RUN_USER"
WEBHOOK_OUTPUT_ZIP="$ROOT_DIR/build/lambda.zip"
WORKER_OUTPUT_ZIP="$ROOT_DIR/build/worker.zip"

mkdir -p "$BUILD_CACHE_DIR" "$CARGO_TARGET_DIR" "$LAMBDA_DIR" "$ROOT_DIR/build"
export XDG_CACHE_HOME="$BUILD_CACHE_DIR"
export CARGO_TARGET_DIR

install_local_rustup() {
  local arch
  local rustup_arch
  local tmp_dir

  arch="$(uname -m)"
  case "$arch" in
    x86_64 | amd64)
      rustup_arch="x86_64"
      ;;
    aarch64 | arm64)
      rustup_arch="aarch64"
      ;;
    *)
      echo "Unsupported host architecture for automatic rustup install: $arch" >&2
      exit 1
      ;;
  esac

  export RUSTUP_HOME="$TOOLS_DIR/rustup"
  export CARGO_HOME="$TOOLS_DIR/cargo"
  export PATH="$CARGO_HOME/bin:$PATH"

  if [[ -x "$CARGO_HOME/bin/rustup" ]]; then
    return
  fi

  echo "rustup not found; installing a project-local Rust toolchain into $TOOLS_DIR"
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN

  curl -fsSL \
    -o "$tmp_dir/rustup-init" \
    "https://static.rust-lang.org/rustup/dist/${rustup_arch}-unknown-linux-gnu/rustup-init"
  chmod +x "$tmp_dir/rustup-init"

  "$tmp_dir/rustup-init" \
    -y \
    --no-modify-path \
    --profile minimal \
    --default-toolchain stable \
    --target "$TARGET"
}

if command -v rustup >/dev/null 2>&1; then
  if ! rustup target list --installed | grep -qx "$TARGET"; then
    echo "Rust target $TARGET not found; installing it with rustup"
    rustup target add "$TARGET"
  fi
else
  install_local_rustup
  if ! rustup target list --installed | grep -qx "$TARGET"; then
    rustup target add "$TARGET"
  fi
fi

export PATH="$TOOLS_DIR/bin:$PATH"

if [[ -x "$TOOLS_DIR/bin/cargo-lambda" ]]; then
  CARGO_LAMBDA="$TOOLS_DIR/bin/cargo-lambda"
elif command -v cargo-lambda >/dev/null 2>&1; then
  CARGO_LAMBDA="$(command -v cargo-lambda)"
else
  echo "cargo-lambda not found; installing it into $TOOLS_DIR"
  cargo install cargo-lambda --root "$TOOLS_DIR"
  CARGO_LAMBDA="$TOOLS_DIR/bin/cargo-lambda"
fi

CURRENT_TARGET_RUSTFLAGS="${CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS:-}"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="${CURRENT_TARGET_RUSTFLAGS:+$CURRENT_TARGET_RUSTFLAGS }-C target-cpu=${RUST_TARGET_CPU}"

echo "Building Lambda binaries for $TARGET with target-cpu=$RUST_TARGET_CPU"

if cargo lambda build --help >/dev/null 2>&1; then
  CARGO_LAMBDA_BUILD=(cargo lambda build)
elif "$CARGO_LAMBDA" build --help >/dev/null 2>&1; then
  CARGO_LAMBDA_BUILD=("$CARGO_LAMBDA" build)
elif "$CARGO_LAMBDA" lambda build --help >/dev/null 2>&1; then
  CARGO_LAMBDA_BUILD=("$CARGO_LAMBDA" lambda build)
else
  echo "cargo-lambda build command is not available" >&2
  exit 1
fi

build_lambda_bin() {
  local bin_name="$1"
  local output_zip="$2"
  local bin_lambda_dir="$LAMBDA_DIR/$bin_name"
  local zip_candidate

  rm -rf "$bin_lambda_dir"
  mkdir -p "$bin_lambda_dir"

  "${CARGO_LAMBDA_BUILD[@]}" \
    --manifest-path "$ROOT_DIR/Cargo.toml" \
    --release \
    --arm64 \
    --lambda-dir "$bin_lambda_dir" \
    --output-format zip \
    --bin "$bin_name"

  rm -f "$output_zip"
  zip_candidate="$bin_lambda_dir/$bin_name/bootstrap.zip"
  if [[ ! -f "$zip_candidate" ]]; then
    zip_candidate="$(find "$bin_lambda_dir" -maxdepth 4 -type f \( -name bootstrap.zip -o -name '*.zip' \) | sort | head -n 1)"
  fi
  if [[ -z "$zip_candidate" || ! -f "$zip_candidate" ]]; then
    echo "cargo-lambda did not produce a Lambda zip under $bin_lambda_dir" >&2
    exit 1
  fi
  cp "$zip_candidate" "$output_zip"

  echo "Wrote $output_zip"
  printf '%s zip size: %.1f MB\n' "$bin_name" "$(awk "BEGIN { print $(wc -c < "$output_zip") / 1024 / 1024 }")"
}

build_lambda_bin "$WEBHOOK_BIN_NAME" "$WEBHOOK_OUTPUT_ZIP"
build_lambda_bin "$WORKER_BIN_NAME" "$WORKER_OUTPUT_ZIP"
