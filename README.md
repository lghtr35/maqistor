# Maqistor

[![CI](https://github.com/lghtr35/maqistor/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lghtr35/maqistor/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/lghtr35/maqistor?display_name=tag&sort=semver)](https://github.com/lghtr35/maqistor/releases/latest)
[![Container image](https://img.shields.io/badge/GHCR-lghtr35%2Fmaqistor-2496ED?logo=github)](https://github.com/lghtr35/maqistor/pkgs/container/maqistor)
[![License](https://img.shields.io/github/license/lghtr35/maqistor)](LICENSE)

Maqistor is a durable asynchronous job scheduler for a single machine. It
accepts work, preserves each job's lifecycle, and routes queued tasks to
available long-lived workers.

Its model is deliberately small: queues organize work, workers perform it, and
the scheduler coordinates delivery, retries, and results. Workers can be run
independently or kept warm under Maqistor's management. Maqistor focuses on
reliable local execution rather than distributed coordination.

## Install

Build the server from source with a current Rust toolchain:

```powershell
cargo build -p maqistor --release
```

The binary is `target/release/maqistor` (`maqistor.exe` on Windows). Docker is
required only when at least one queue uses `managed_config`. External-worker-only
deployments never connect to a Docker daemon.

## Releases and containers

Release tags (`vX.Y.Z`) create a [GitHub Release](https://github.com/lghtr35/maqistor/releases)
with archives for Linux (x86_64 and ARM64), macOS (Intel and Apple Silicon), and
Windows (x86_64). Each archive includes the `maqistor` executable and its SHA-256
checksum is published with the release.

The same tag publishes a multi-platform image to GitHub Container Registry:

```sh
docker pull ghcr.io/lghtr35/maqistor:vX.Y.Z
```

For a container deployment, mount the configuration and certificates read-only
and persist the SQLite files in `/data`. Set the database paths in the mounted
configuration to `/data/maqistor-ingest.db` and `/data/maqistor-results.db`.

```sh
docker run --rm --name maqistor \
  -p 7828:7828 -p 7829:7829 \
  -v maqistor-data:/data \
  -v "$(pwd)/maqistor.toml:/config/maqistor.toml:ro" \
  -v "$(pwd)/certs:/config/certs:ro" \
  ghcr.io/lghtr35/maqistor:vX.Y.Z --config /config/maqistor.toml
```

## Deployment topologies

Workers are always mTLS TCP peers of `worker_listen`. Docker only supervises
optional managed replicas. Layouts can be combined on the same queues:

- **External-only:** no `[docker]`, no `managed_config`. Workers dial a
  reachable `worker_listen` address.
- **Host + local Docker:** omit `[docker]` or set a local `unix://` /
  `npipe://` endpoint. Managed siblings and independently started workers may
  attach to the same queue names.
- **Maqistor-in-container:** mount the host Docker socket and/or set
  `[docker].endpoint` (host Docker or one remote daemon). Bind
  `worker_listen` to `0.0.0.0` and publish the port and/or join a Docker
  network. Sibling managed workers dial via network service name, published
  host port, or `host.docker.internal`. Remote/external workers dial the same
  published/routable listener over mTLS.
- **Remote single daemon:** set `[docker].endpoint` to `tcp://...` for an
  unsecured lab daemon, or `https://...` with explicit CA, client certificate,
  and client key paths for Docker API mTLS. Containers are managed on that
  daemon; every worker still needs a routable path to maqistor's listen port.

## Set up

1. Copy [maqistor.example.toml](maqistor.example.toml) to a runtime-specific
   configuration file.
2. Provide the three PEM files named by `[worker_tls]`: a CA certificate, a
   server certificate valid for the worker's configured server name, and the
   server private key. Each worker needs a client certificate and private key
   signed by that CA.
3. Configure one or more `[[queues]]`. A queue accepts externally run workers;
   add `[queues.managed_config]` only when Maqistor should maintain tagged
   Docker worker containers for it.
4. Start the binary from the directory used by the relative paths in the
   configuration:

   ```powershell
   .\target\release\maqistor.exe --config .\maqistor.toml
   ```

5. Verify the server, then submit a job for a configured queue:

   ```powershell
   Invoke-WebRequest http://127.0.0.1:7828/health
   Invoke-RestMethod http://127.0.0.1:7828/jobs -Method Post `
     -ContentType 'application/json' `
     -Body '{"name":"example","payload":{"message":"hello"}}'
   ```

The HTTP endpoint returns only job identity and status. A connected worker is
required for execution.

## Reading graph

Start with the executable and follow the links for the layer you are changing:

```text
maqistor binary
  -> HTTP API -> Engine <- SQLite persistence
                   |
                   -> Dispatcher <-> Worker protocol <- Rust worker SDK
```

| Concern | Resource |
| --- | --- |
| Binary, configuration, startup, and operational boundaries | [crates/maqistor](crates/maqistor/README.md) |
| HTTP contract | [crates/api](crates/api/README.md) |
| Scheduling, lifecycle, retries, and ports | [crates/engine](crates/engine/README.md) |
| SQLite schema, durability, and batching | [crates/persistence](crates/persistence/README.md) |
| Worker registry, mTLS listener, and Docker supervision | [crates/dispatcher](crates/dispatcher/README.md) |
| Language-neutral worker wire contract | [crates/worker-protocol](crates/worker-protocol/README.md) |
| Rust worker implementation | [crates/worker-sdk](crates/worker-sdk/README.md) |
| Repeatable capacity benchmarks | [benchmark](benchmark/README.md) |

## Checks

```powershell
cargo check --workspace --all-targets
cargo test --workspace
```

For benchmark prerequisites and lifecycle, see [benchmark/README.md](benchmark/README.md).
