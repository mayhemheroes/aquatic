#!/usr/bin/env bash
#
# aquatic/mayhem/test.sh — RUN aquatic's own functional test suite for the FUZZED crates and emit a
# CTRF summary. exit 0 iff no test failed.
#
# PATCH-grade oracle. The two reconstructed fuzz targets drive:
#   udp_roundtrip   -> aquatic_udp_protocol::Request::{parse_bytes, write_bytes}
#   handle_requests -> aquatic_udp::swarm::TorrentMaps::{announce, scrape}
#
# We assert real behavior of exactly those code paths via upstream's own tests:
#   * aquatic_udp_protocol's #[cfg(test)] suite (crates/udp_protocol/src/{request,response}.rs)
#     contains the round-trip oracle `same_after_conversion` (serialize -> parse -> assert equal)
#     plus byte/value-exact parse assertions (test_announce, test_various_input_lengths, ...). A
#     no-op / output-altering patch to the parser or serializer CANNOT pass these.
#   * aquatic_udp's `cleaning` integration test (crates/udp/tests/cleaning.rs) drives
#     TorrentMaps::announce/scrape and asserts EXACT peer counts and cleaning behavior — the same
#     swarm logic handle_requests fuzzes. (It does not bind a socket; the socket-based tests are
#     excluded so the oracle is deterministic in-container.)
#
# This script only RUNS the suites via `cargo test`; it never builds the fuzz target. We test
# aquatic_udp with --no-default-features — the SAME feature set the fuzz crate links (no mimalloc /
# prometheus), keeping the oracle aligned with the fuzzed code and free of cmake/system deps.
set -uo pipefail
[ -n "${SOURCE_DATE_EPOCH:-}" ] || unset SOURCE_DATE_EPOCH

: "${MAYHEM_JOBS:=$(nproc)}"
cd "$SRC"

# emit_ctrf <tool> <passed> <failed> [skipped] [pending] [other]
emit_ctrf() {
  local tool="$1" passed="$2" failed="$3" skipped="${4:-0}" pending="${5:-0}" other="${6:-0}"
  local tests=$(( passed + failed + skipped + pending + other ))
  cat > "${CTRF_REPORT:-$SRC/ctrf-report.json}" <<JSON
{
  "results": {
    "tool": { "name": "$tool" },
    "summary": {
      "tests": $tests,
      "passed": $passed,
      "failed": $failed,
      "pending": $pending,
      "skipped": $skipped,
      "other": $other
    }
  }
}
JSON
  printf 'CTRF {"results":{"tool":{"name":"%s"},"summary":{"tests":%d,"passed":%d,"failed":%d,"pending":%d,"skipped":%d,"other":%d}}}\n' \
    "$tool" "$tests" "$passed" "$failed" "$pending" "$skipped" "$other"
  [ "$failed" -eq 0 ]
}

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not available — cannot run the test suite" >&2
  emit_ctrf "cargo-test" 0 1 0; exit 2
fi

# Sum 'test result:' lines across binaries from a cargo test invocation.
PASSED=0; FAILED=0; IGNORED=0
run_suite() {
  echo "=== $* ==="
  local out rc
  out="$(RUSTFLAGS="" "$@" --no-fail-fast --jobs "$MAYHEM_JOBS" 2>&1)"; rc=$?
  echo "$out"
  local got=0
  while read -r p f i; do
    PASSED=$(( PASSED + p )); FAILED=$(( FAILED + f )); IGNORED=$(( IGNORED + i )); got=1
  done < <(printf '%s\n' "$out" \
    | sed -n 's/^test result:.* \([0-9][0-9]*\) passed; \([0-9][0-9]*\) failed; \([0-9][0-9]*\) ignored.*/\1 \2 \3/p')
  # If a suite produced no parseable result lines but cargo failed, count it as a failure.
  if [ "$got" -eq 0 ] && [ "$rc" -ne 0 ]; then
    echo "suite produced no 'test result:' lines and cargo exited $rc — counting as failure" >&2
    FAILED=$(( FAILED + 1 ))
  fi
}

# Oracle for udp_roundtrip: the protocol crate's own round-trip + parse assertions.
run_suite cargo test -p aquatic_udp_protocol
# Oracle for handle_requests: the swarm announce/scrape/cleaning assertions (no sockets).
run_suite cargo test -p aquatic_udp --no-default-features --lib
run_suite cargo test -p aquatic_udp --no-default-features --test cleaning

if [ "$(( PASSED + FAILED + IGNORED ))" -eq 0 ]; then
  echo "could not parse any 'test result:' lines from any suite" >&2
  emit_ctrf "cargo-test" 0 1 0; exit 1
fi

emit_ctrf "cargo-test" "$PASSED" "$FAILED" "$IGNORED"
