# maqistor-client-sdk

Thin Rust client for submitting and querying Maqistor jobs. Shared wire
types live in [`maqistor-types`](../types); this crate only speaks transports.

Today that is HTTP via `MaqistorHttpClient`, which implements the
`MaqistorClient` trait (`health`, `enqueue`, `get_job`). Other transports can
implement the same trait later without changing application call sites that are
generic over `MaqistorClient`.

## Install

```toml
[dependencies]
maqistor-client-sdk = { path = "../client-sdk" }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

When the crate is published, replace the path dependency with a version:

```toml
maqistor-client-sdk = "0.1.0"
```

## Example

```rust,no_run
use maqistor_client_sdk::{JobRequest, MaqistorClient, MaqistorHttpClient};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = MaqistorHttpClient::new("http://127.0.0.1:7828");

    client.health().await?;

    let job = client
        .enqueue(JobRequest {
            name: "example".into(),
            payload: json!({"message": "hello"}),
        })
        .await?;

    let same = client.get_job(job.id).await?;
    assert_eq!(same.id, job.id);
    Ok(())
}
```

`enqueue` maps to `POST /jobs` (`201` + `{ id, name, status }`).
`get_job` maps to `GET /jobs/{id}`. API failures become `HttpError::Api` with
the status and the server `error` message.

The HTTP surface returns only job identity and status. A connected worker is
still required for execution.

## Reading graph

- [Top-level README](../../README.md) - server install and endpoint setup
- [api](../api/README.md) - HTTP routes and status codes
- [types](../types/README.md) - shared request/response shapes
- [worker-sdk](../worker-sdk/README.md) - executing queued work
