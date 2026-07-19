# maqistor-api

The transport-facing ingress gateway for Maqistor.

## Contains

- Request validation, HTTP route adaptation, and response/error mapping.
- Translation from external requests to Engine commands.

## Does not contain

- Job-domain policy, persistence access, worker dispatch, or process startup.

## Internal dependencies

`maqistor-engine`.
