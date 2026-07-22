# maqistor-persistence

The durable-storage adapter for Maqistor, currently implemented with SQLite.

## Contains

- Split SQLite **ingest** + **results** files (schema v1 each), lease recovery,
  and independently adaptive enqueue / completion write batching. Pre-v1 or
  single-file prototype DBs are not upgraded — delete them and restart.
- An implementation of Engine's durable-store port.

## Does not contain

- Job-domain policy, scheduling, ingress, Docker, or process startup.

## Internal dependencies

`maqistor-engine`.
