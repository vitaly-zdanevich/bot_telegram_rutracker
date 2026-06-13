#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${TARGET:-aarch64-unknown-linux-gnu}"
RUST_TARGET_CPU="${RUST_TARGET_CPU:-neoverse-n1}"
RUN_USER="${USER:-$(id -un 2>/dev/null || echo local)}"
SAFE_RUN_USER="${RUN_USER//[^a-zA-Z0-9_.-]/_}"
CARGO_TARGET_DIR="$ROOT_DIR/build/target-oracle-$SAFE_RUN_USER"
OUTPUT_DIR="$ROOT_DIR/build/oracle"
TOOLS_DIR="$ROOT_DIR/.tools"

mkdir -p "$CARGO_TARGET_DIR" "$OUTPUT_DIR"
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

if [[ "$TARGET" == "aarch64-unknown-linux-gnu" ]] \
  && [[ "$(uname -m)" != "aarch64" && "$(uname -m)" != "arm64" ]] \
  && ! command -v aarch64-linux-gnu-gcc >/dev/null 2>&1; then
  echo "Cross compiler aarch64-linux-gnu-gcc is required to build Oracle ARM binaries on this host." >&2
  echo "Install gcc-aarch64-linux-gnu or run this script on the Oracle ARM VM for a native build." >&2
  exit 1
fi

CURRENT_TARGET_RUSTFLAGS="${CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS:-}"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="${CURRENT_TARGET_RUSTFLAGS:+$CURRENT_TARGET_RUSTFLAGS }-C target-cpu=${RUST_TARGET_CPU}"

echo "Building Oracle VM binary for $TARGET with target-cpu=$RUST_TARGET_CPU"
cargo build \
  --manifest-path "$ROOT_DIR/Cargo.toml" \
  --release \
  --target "$TARGET" \
  --bin telegram-rutracker-vm-worker \
  --bin telegram-rutracker-poller

for bin_name in telegram-rutracker-vm-worker telegram-rutracker-poller; do
  cp "$CARGO_TARGET_DIR/$TARGET/release/$bin_name" "$OUTPUT_DIR/$bin_name"
  strip "$OUTPUT_DIR/$bin_name" 2>/dev/null || true
  echo "Wrote $OUTPUT_DIR/$bin_name"
done
