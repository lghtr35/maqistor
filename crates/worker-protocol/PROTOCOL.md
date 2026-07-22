# Worker Protocol v1

The canonical schema is [worker-protocol-v1.cddl](worker-protocol-v1.cddl). This document defines the transport and message behavior needed to implement an SDK in any language.

## Transport

Workers connect to the configured worker listener over mutually authenticated TLS. One connection serves one queue and one worker instance.

Each frame is:

1. A four-byte unsigned big-endian length.
2. Exactly that many bytes of CBOR data matching `worker-frame` in the CDDL definition.

The CBOR body must not exceed 1,048,576 bytes. Peers must reject frames whose declared length is over that limit, whose actual length differs from the prefix, or whose `protocol_version` is not `1`.

## Session

The worker sends `register` as its first frame. `instance_id` is a UUID string unique among connected workers. `queue_name` must be a configured queue. `running_jobs` starts at zero and `free_slots` is the worker's initial available capacity.

The server replies with `registered` for the same queue. A worker may then receive `job_dispatch` frames. The server may send `error` at any time; the worker must stop using that connection after receiving one.

Workers should send `heartbeat` periodically while connected. Maqistor's SDK uses a five-second interval.

## Job dispatch and completion

Each `job_dispatch` contains the durable `job_id`, the opaque `dispatch_id` fencing token, an `execution_count`, and raw JSON bytes in `payload`.

For every dispatch, the worker sends one `job_result` with the same `job_id` and `dispatch_id`. A successful result carries raw result bytes; a failed result carries a message. `running_jobs` and `free_slots` in each result are the worker's current complete capacity snapshot, not a delta.

Results with an unknown or stale `dispatch_id` may be ignored by the server. Workers must not reuse a result from one dispatch for another dispatch.

## Compatibility

The version is part of every frame. Any incompatible change requires a new CDDL file and a new `protocol_version`; version 1 implementations must continue to reject unknown versions.
