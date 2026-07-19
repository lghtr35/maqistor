# maqistor-dispatcher

The worker-side dispatch adapter for Maqistor, with Docker as its first backend.

## Contains

- Engine dispatch-port implementations and future worker capacity/connection management.
- Docker worker lifecycle and protocol adaptation.

## Does not contain

- HTTP ingress, configuration loading, global scheduling policy, or persistence.

## Internal dependencies

`maqistor-engine`.
