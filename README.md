# Maqistor

Maqistor is a local, durable asynchronous job scheduler. A single binary accepts
JSON jobs over HTTP, stores scheduler state in paired SQLite databases, and
dispatches work to long-lived workers over mutually authenticated TLS. Queues
can use workers started independently or warm containers supervised by Docker.

It is deliberately a single-host system: it does not provide multi-node
coordination, HTTP authentication, or an HTTP TLS endpoint.

## Install

Build the server from source with a current Rust toolchain:

```powershell
cargo build -p maqistor --release
```

The binary is `target/release/maqistor` (`maqistor.exe` on Windows). Docker is
required only when at least one queue uses `managed_config`. External-worker-only
deployments never connect to a Docker daemon.

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
