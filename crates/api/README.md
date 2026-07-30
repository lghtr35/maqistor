# maqistor-api

The HTTP adapter for Maqistor. It validates JSON, translates requests to
[Engine](../engine/README.md) operations, and maps engine errors to HTTP
responses. It has no authentication or authorization layer.

| Route | Success | Notes |
| --- | --- | --- |
| `GET /health` | `204 No Content` | Process health only; it does not report worker capacity |
| `POST /jobs` | `201 Created` with `{ id, name, status }` | Body: `{ "name": string, "payload": any }` |
| `GET /jobs/{id}` | `200 OK` with `{ id, name, status }` | Does not expose payloads, attempts, leases, or result bodies |

Unknown queues and payload-serialization failures return `400`; missing jobs
return `404`; storage failures return `500`. Axum request tracing is installed
on the router.

## Reading graph

- [Top-level README](../../README.md) - endpoint setup and security boundary.
- [engine](../engine/README.md) - commands and errors used by this adapter.
- [maqistor](../maqistor/README.md) - binary wiring and listener configuration.
