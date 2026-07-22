FROM rust:1-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --release -p maqistor-benchmark-noop-worker

FROM debian:bookworm-slim
COPY --from=builder /workspace/target/release/maqistor-benchmark-noop-worker /usr/local/bin/noop-worker
COPY benchmark/certs/ /certs/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/noop-worker"]
