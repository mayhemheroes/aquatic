# syntax=docker/dockerfile:1.7
#
# Commit-image Dockerfile (SMALL form) — builds ON TOP of the org base image
# (ghcr.io/mayhemheroes/base). aquatic is a high-performance Rust BitTorrent tracker; the fuzzed
# code is its UDP protocol parser (aquatic_udp_protocol) and UDP swarm request handlers
# (aquatic_udp::swarm). The base ships NO Rust toolchain, so this image installs rustup (a pinned
# nightly — cargo-fuzz needs `-Z` flags) + cargo-fuzz as root into a SHARED location (/opt/rust)
# usable by the non-root `mayhem` user, then runs build.sh (cargo fuzz build) + test.sh (aquatic's
# own cargo test suites for the fuzzed crates).
#
# Build (from the repo root):
#   DOCKER_BUILDKIT=1 docker build -f mayhem/Dockerfile \
#       --build-arg COMMIT=$(git rev-parse HEAD) \
#       -t aquatic-commit:test .

FROM ghcr.io/mayhemheroes/base:2026-06

ARG COMMIT
ARG REPO_URL
ARG SOURCE_DATE_EPOCH

# Sanitizer override: the base exports SANITIZER_FLAGS (ASan+UBSan) as the default, but cargo-fuzz
# drives Rust instrumentation via RUSTFLAGS (-Zsanitizer=address), NOT $SANITIZER_FLAGS. Declared
# here for parity; build.sh wires ASan into RUSTFLAGS the cargo-fuzz way.
ARG SANITIZER_FLAGS
ARG MAYHEM_JOBS=

# Pin the nightly toolchain (cargo-fuzz needs nightly for `-Z` flags / -Zsanitizer=address).
ARG RUST_TOOLCHAIN=nightly-2026-01-20
ARG CARGO_FUZZ_VERSION=0.12.0

# Shared Rust install: CARGO_HOME/RUSTUP_HOME under /opt/rust, world-readable, so the non-root
# `mayhem` user can invoke cargo/rustc/cargo-fuzz without owning the toolchain.
ENV RUSTUP_HOME=/opt/rust/rustup \
    CARGO_HOME=/opt/rust/cargo \
    PATH=/opt/rust/cargo/bin:$PATH

USER root
RUN set -eux; \
    mkdir -p "$RUSTUP_HOME" "$CARGO_HOME"; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --no-modify-path --profile minimal --default-toolchain "$RUST_TOOLCHAIN"; \
    rustup component add rust-src --toolchain "$RUST_TOOLCHAIN"; \
    cargo install cargo-fuzz --version "$CARGO_FUZZ_VERSION" --locked; \
    chmod -R a+rX /opt/rust; \
    mkdir -p "$CARGO_HOME/registry" "$CARGO_HOME/git" && chmod -R a+rwX "$CARGO_HOME/registry" "$CARGO_HOME/git"; \
    rustc --version; cargo --version; cargo fuzz --version
USER mayhem

# Source enters from the build CONTEXT (the CI checkout): the FULL repo (incl. .git history) is
# copied to /mayhem as `mayhem`; build.sh builds in place.
WORKDIR /mayhem
COPY --chown=2000:2000 . /mayhem

RUN bash mayhem/build.sh

# Run aquatic's real test suites for the fuzzed crates at build time; a failing suite fails the build.
RUN if [ -f mayhem/test.sh ]; then bash mayhem/test.sh; else echo "no mayhem/test.sh"; fi

LABEL org.opencontainers.image.source="${REPO_URL}" \
      org.opencontainers.image.revision="${COMMIT}" \
      rlenv.tier="commit" \
      rlenv.source_date_epoch="${SOURCE_DATE_EPOCH}" \
      rlenv.user="mayhem"

USER mayhem
WORKDIR /mayhem
