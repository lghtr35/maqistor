# maqistor

The executable composition root for the Maqistor application.

## Contains

- CLI, configuration loading, logging, and startup wiring.
- Construction of the API, Engine, Persistence, and Dispatcher adapters.

## Does not contain

- Job lifecycle, scheduling, transport, Docker, or storage policy.

## Internal dependencies

`maqistor-api`, `maqistor-engine`, `maqistor-persistence`, and `maqistor-dispatcher`.
