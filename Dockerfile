FROM rust:1.88-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p maqistor

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 maqistor \
    && useradd --uid 10001 --gid maqistor --create-home maqistor \
    && mkdir /data \
    && chown maqistor:maqistor /data

WORKDIR /app
COPY --from=builder /workspace/target/release/maqistor /usr/local/bin/maqistor

USER maqistor
ENTRYPOINT ["maqistor"]
