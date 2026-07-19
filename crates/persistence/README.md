# maqistor-persistence

The durable-storage adapter for Maqistor, currently implemented with SQLite.

## Contains

- SQLite schema/migrations, execution ledger, lease recovery, and write batching.
- An implementation of Engine's durable-store port.

## Does not contain

- Job-domain policy, scheduling, ingress, Docker, or process startup.

## Internal dependencies

`maqistor-engine`.
