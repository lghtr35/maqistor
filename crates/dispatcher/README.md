# maqistor-dispatcher

The worker-side implementation of the [Engine](../engine/README.md)
`WorkerDispatcher` port. It accepts worker connections over mutual TLS, tracks
their queue capacity, reserves slots, and writes dispatch frames through a
per-worker outbound channel.

## Worker registry

One connection represents one worker instance for one queue. Registration
requires a unique UUID and configured queue name. Worker result messages replace
the registry's reservation estimate with the worker's complete
`running_jobs`/`free_slots` snapshot. Disconnects remove the worker. TLS,
protocol, unknown-queue, and duplicate-instance failures are rejected.

The exact framing and message contract lives in
[worker-protocol](../worker-protocol/README.md). The dispatcher does not own
job lifecycle or persistence.

## Managed workers

`DockerWorkerSupervisor` reconciles every queue with `managed_config` every
five seconds. It starts matching existing containers, creates missing ones, and
replaces a managed container when its configured image changes. Managed
containers are labelled `io.maqistor.managed=true` and use Docker's
`unless-stopped` restart policy.

The binary supplies managed-worker configuration; see
[maqistor](../maqistor/README.md). Independently started workers use the same
protocol and can connect to any configured queue.

## Reading graph

- [Top-level README](../../README.md) - deployment modes.
- [engine](../engine/README.md) - reservation and dispatch contract.
- [worker-protocol](../worker-protocol/README.md) - wire contract.
- [worker-sdk](../worker-sdk/README.md) - Rust worker implementation.
