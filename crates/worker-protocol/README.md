# maqistor-worker-protocol

The versioned, language-neutral worker wire contract. Implement a worker SDK
from the canonical [CDDL schema](worker-protocol-v1.cddl) and the behavioral
rules in [PROTOCOL.md](PROTOCOL.md); both are required.

Protocol version 1 uses mutual TLS and a four-byte unsigned big-endian frame
length followed by a CBOR body. Bodies larger than 1 MiB and unknown protocol
versions are rejected.

Each connection registers one UUID worker instance for one queue. The worker
then receives `job_dispatch` frames and sends exactly one matching `job_result`
for every dispatch. Result capacity values are complete snapshots, not deltas.
Workers also send periodic heartbeats. A shutting-down worker sends `drain` and
waits for `draining` before completing its already accepted jobs. `dispatch_id`
is an opaque fence and must be returned unchanged with the result.

## Reading graph

- [Top-level README](https://github.com/lghtr35/maqistor/blob/main/README.md) - server setup and mTLS requirements.
- [dispatcher](https://github.com/lghtr35/maqistor/blob/main/crates/dispatcher/README.md) - server-side protocol consumer.
- [worker-sdk](https://github.com/lghtr35/maqistor/blob/main/crates/worker-sdk/README.md) - Rust reference implementation.
- [PROTOCOL.md](PROTOCOL.md) - session behavior and compatibility requirements.
