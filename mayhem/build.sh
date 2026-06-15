#!/usr/bin/env bash
#
# aquatic/mayhem/build.sh — build aquatic's reconstructed cargo-fuzz targets as sanitized
# libFuzzer binaries, replicating OSS-Fuzz's Rust path (a `cargo fuzz build -O`).
#
# aquatic is a high-performance Rust BitTorrent tracker; the fuzzed code is its UDP protocol parser
# (aquatic_udp_protocol) and its UDP swarm request handlers (aquatic_udp::swarm). Current upstream
# ships NO fuzz/ directory, so we build our OWN additive cargo-fuzz crate (mayhem/fuzz/) — purely
# additive, no upstream file is modified — with the modern libfuzzer-sys cargo-fuzz 0.12 expects.
# cargo-fuzz targets it via `--fuzz-dir mayhem/fuzz`.
#
# Targets reconstructed (the 2 that still exist upstream):
#   udp_roundtrip   — aquatic_udp_protocol::Request::{parse_bytes, write_bytes} round-trip
#   handle_requests — aquatic_udp::swarm::TorrentMaps::{announce, scrape}
# Dropped: pending_scrape_response_slab (its callee test_pending_scrape_response_slab is gone upstream).
#
# ASan is enabled the Rust way via RUSTFLAGS `-Zsanitizer=address` (NOT clang's $SANITIZER_FLAGS,
# which doesn't apply to rustc), matching OSS-Fuzz's compile for FUZZING_LANGUAGE=rust. nightly is
# required for `-Zsanitizer`.
set -euo pipefail

# clang rejects SOURCE_DATE_EPOCH='' — must be unset or a valid integer.
[ -n "${SOURCE_DATE_EPOCH:-}" ] || unset SOURCE_DATE_EPOCH

: "${MAYHEM_JOBS:=$(nproc)}"
export MAYHEM_JOBS
# cargo-fuzz has no --jobs flag; cargo reads parallelism from CARGO_BUILD_JOBS.
export CARGO_BUILD_JOBS="$MAYHEM_JOBS"

cd "$SRC"

# Our additive cargo-fuzz crate. Discover every target from its fuzz_targets/ dir.
FUZZ_DIR="mayhem/fuzz"
FUZZ_TARGETS=()
for f in "$FUZZ_DIR"/fuzz_targets/*.rs; do
  FUZZ_TARGETS+=("$(basename "${f%.*}")")
done
[ "${#FUZZ_TARGETS[@]}" -gt 0 ] || { echo "ERROR: no fuzz targets under $FUZZ_DIR/fuzz_targets/" >&2; exit 1; }
TRIPLE="x86_64-unknown-linux-gnu"

# Replicate OSS-Fuzz `compile` RUSTFLAGS for a libFuzzer+ASan Rust build.
export RUSTFLAGS="${RUSTFLAGS:-} --cfg fuzzing -Zsanitizer=address -Cdebuginfo=1 -Cforce-frame-pointers"

echo "=== cargo fuzz build (image-default nightly toolchain, ASan via RUSTFLAGS) ==="
echo "RUSTFLAGS=$RUSTFLAGS"
echo "targets: ${FUZZ_TARGETS[*]}"

# The mayhem/fuzz crate is an isolated sub-workspace, so it resolves its OWN lockfile rather than
# the repo's Cargo.lock. Left alone, the resolver picks newer transitive deps that demand a newer
# rustc than the pinned fuzzing nightly (e.g. constant_time_eq 0.5.0 requires rustc 1.95). Pin the
# known-good versions the repo's own Cargo.lock already builds with so the build stays on the pinned
# toolchain. `|| true` keeps this best-effort: if a pin's not in the graph it's simply a no-op.
cargo generate-lockfile --manifest-path "$FUZZ_DIR/Cargo.toml" 2>/dev/null || true
cargo update --manifest-path "$FUZZ_DIR/Cargo.toml" -p constant_time_eq --precise 0.4.2 2>/dev/null || true

# `-O` (release w/ opt) + `--debug-assertions` mirrors OSS-Fuzz's Rust build. Use the image's
# DEFAULT toolchain (the Dockerfile pins it to the required nightly); a `+toolchain` override would
# make rustup try to install a channel into the read-only shared /opt/rust. Build per-target so a
# single bad target doesn't mask the others.
for t in "${FUZZ_TARGETS[@]}"; do
  echo "--- building fuzz target: $t ---"
  cargo fuzz build --fuzz-dir "$FUZZ_DIR" -O --debug-assertions "$t"
  bin="$SRC/$FUZZ_DIR/target/$TRIPLE/release/$t"
  if [ ! -x "$bin" ]; then
    echo "ERROR: expected fuzz binary not found at $bin" >&2
    exit 1
  fi
  cp "$bin" "/mayhem/$t"
  echo "built /mayhem/$t"
done

echo "build.sh complete:"
ls -la "/mayhem/${FUZZ_TARGETS[@]}" 2>&1 || true
