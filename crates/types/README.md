# maqistor-types

Shared wire and domain types used by Maqistor adapters and clients. Keep this
crate free of transport and server dependencies so HTTP (and later NATS,
Kafka, and others) can share one shape for jobs and errors.

## HTTP shapes

| Type | Role |
| --- | --- |
| `JobRequest` | Submit body: `{ name, payload }` |
| `JobResponse` | Success body: `{ id, name, status }` |
| `ErrorBody` | Error body: `{ error }` |

```rust
use maqistor_types::{JobRequest, JobResponse};
use serde_json::json;

let request = JobRequest {
    name: "example".into(),
    payload: json!({"message": "hello"}),
};
let _ = request;
let _ = JobResponse {
    id: 1,
    name: "example".into(),
    status: "queued".into(),
};
```

## Reading graph

- [api](../api/README.md) - HTTP adapter that serializes these types
- [client-sdk](../client-sdk/README.md) - clients that speak them over transports
