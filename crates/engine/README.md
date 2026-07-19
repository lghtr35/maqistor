# maqistor-engine

The transport- and backend-independent coordinator for Maqistor jobs.

## Contains

- Job and queue domain types, lifecycle commands, and recovery coordination.
- Durable-store and worker-dispatch ports.

## Does not contain

- HTTP, Docker, SQLite, configuration loading, or executable startup code.

## Internal dependencies

None.
