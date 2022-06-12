# Build Stage
FROM ghcr.io/evanrichter/cargo-fuzz:latest as builder

## Add source code to the build stage.
ADD . /src
WORKDIR /src

RUN echo building instrumented harnesses && \
    bash -c "pushd aquatic_udp_protocol/fuzz && cargo +nightly -Z sparse-registry fuzz build && popd" && \
    bash -c "pushd aquatic_udp/fuzz && cargo +nightly -Z sparse-registry fuzz build && popd" && \
    mv aquatic_udp_protocol/fuzz/target/x86_64-unknown-linux-gnu/release/udp_roundtrip /udp_roundtrip && \
    mv aquatic_udp/fuzz/target/x86_64-unknown-linux-gnu/release/pending_scrape_response_slab /pending_scrape_response_slab && \
    mv aquatic_udp/fuzz/target/x86_64-unknown-linux-gnu/release/handle_requests /handle_requests && \
    echo done

RUN echo building non-instrumented harnesses && \
    export RUSTFLAGS="--cfg fuzzing -Clink-dead-code -Cdebug-assertions -C codegen-units=1" && \
    bash -c "pushd aquatic_udp_protocol/fuzz && cargo +nightly -Z sparse-registry build --release && popd" && \
    bash -c "pushd aquatic_udp/fuzz && cargo +nightly -Z sparse-registry build --release && popd" && \
    mv aquatic_udp_protocol/fuzz/target/release/udp_roundtrip /udp_roundtrip_no_inst && \
    mv aquatic_udp/fuzz/target/release/pending_scrape_response_slab /pending_scrape_response_slab_no_inst && \
    mv aquatic_udp/fuzz/target/release/handle_requests /handle_requests_no_inst && \
    echo done

# Package Stage
FROM rustlang/rust:nightly

COPY --from=builder /udp_roundtrip /udp_roundtrip_no_inst /
COPY --from=builder /pending_scrape_response_slab /pending_scrape_response_slab_no_inst /
COPY --from=builder /handle_requests /handle_requests_no_inst /
