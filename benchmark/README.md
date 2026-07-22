# Benchmark suite

External load tests against a **standing** maqistor process. The runner does
**not** start the server.

Driver: **[oha](https://github.com/hatoo/oha)** (orchestrated by
`benchmark/run.py`). Helpers live in `benchmark/oha_util.py`.

| Mode | oha shape | Question |
|------|-----------|----------|
| `closed` | `-c` concurrency sweep | Where does durable ingest throughput peak? |
| `open` | `-q` offered QPS sweep | Can we absorb a target rate? |
| `both` | closed then open | Combined capacity sweep (default) |
| `full` | open QPS + post-step drain | Ingest **and** create→complete cycle delay |

All points wait for in-flight requests at the duration deadline, so the last
durable batch is counted rather than aborted by the load generator.

## Prerequisites

```bash
cargo install oha
# ensure ~/.cargo/bin (or %USERPROFILE%\.cargo\bin) is on PATH
```

### Managed no-op worker

The benchmark `bench` queue runs as a managed Docker worker. Generate local,
short-lived mTLS certificates and build the image from the workspace root
before starting Maqistor:

```bash
sh benchmark/generate-certs.sh
docker build -f benchmark/noop-worker/Dockerfile -t maqistor-benchmark-noop-worker:0.1.3 .
```

The worker connects to `host.docker.internal:17829` (Docker Desktop) and
discards each JSON payload before returning an empty successful result. The
certificate directory is ignored by Git.

`--mode full` requires this worker so jobs drain and complete.

Prefer a **release** server for meaningful numbers:

```bash
cargo build -p maqistor --release
```

## Start maqistor

From the workspace root (`maqistor/`):

```bash
./target/release/maqistor --config benchmark/maqistor.toml
```

Windows:

```powershell
.\target\release\maqistor.exe --config benchmark\maqistor.toml
```

Run the binary from the workspace root so config paths resolve.
Avoid `cargo run -p maqistor-dispatcher -- --config ...` (wrong cwd).

Listens on `http://127.0.0.1:18081`, job name `bench`.
DBs under `benchmark/data/` (gitignored), set in `[persistence]`:
`maqistor-ingest.db` + `maqistor-results.db`. Delete both after a schema cut
(current schema is **v1** on each file; older prototype files will refuse to open).
Enqueue and completion use **separate SQLite writers** so completes do not share
the ingest commit pipe. Each side still self-tunes batch size/wait from request
rate, SQL commit rate, commit duration, and batch fill.
`benchmark/maqistor.toml` sets persistence writer batching under
`[persistence.enqueue]` / `[persistence.completion]`, and dispatch ceilings
under `[dispatch]` (`batch_size_max`, `max_in_flight`). Claim size follows
free worker slots from `reserve`, capped by those ceilings.

## Runner (`run.py`)

```powershell
# Six closed and six open points, 30s each, 5s settle between points.
python benchmark\run.py

# Closed-loop ceiling only.
python benchmark\run.py --mode closed

# Open-loop region with generous client concurrency.
python benchmark\run.py --mode open --open-connections 1000 `
  --open-qps 8000,9000,10000,11000,12000,14000

# Full cycle: offer QPS, drain the bench queue, report cycle delay from DB.
python benchmark\run.py --mode full --open-qps 1000,2000 --duration 10 `
  --settle-seconds 5
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--mode` | `both` | `closed` / `open` / `both` / `full` |
| `--duration` | `30` | Seconds per point (`-z`) |
| `--closed-connections` | `50,100,200,400,800,1200` | Closed-loop `-c` values |
| `--open-qps` | `4000,6000,8000,10000,12000,16000` | Open/full `-q` values |
| `--open-connections` | `1000` | `-c` for every open/full point |
| `--max-p99-ms` | `100` | Stability p99 guardrail |
| `--settle-seconds` | `5` | Pause after each point before the next |
| `--drain-timeout-seconds` | `120` | Full only: max wait for queue drain |
| `--drain-poll-seconds` | `0.5` | Full only: drain poll interval |
| `--db` | `benchmark/data/maqistor-ingest.db` | Full only: ingest SQLite path (results = `maqistor-results.db`) |

An open/full point is marked **stable** only when it has zero errors, achieves
at least 98% of its offered QPS, and stays below the HTTP p99 guardrail. Full
mode also requires a successful drain. This label does not enforce a full-cycle
latency target or require zero backlog at the end of the offer window; judge
full-cycle capacity from backlog, drain time, and cycle percentiles too. The
closed-loop peak is an observed ceiling for this machine and local oha client,
not a universal SQLite limit.

Raw oha JSON lands under `benchmark/results/raw/`. Summaries are
`summary-capacity-*.json` or `summary-capacity-full-*.json`.

### Full-cycle metrics

After each full point, the runner:

1. Snapshots ingest `MAX(id)` before oha (job watermark).
2. Runs the open-loop ingest offer.
3. Records backlog (ingest `pending` + results `running` above the watermark).
4. Polls both SQLite files until those drain (or timeout).
5. Computes create→complete cycle as results `updated_at` − ingest `created_at`
   (unix **milliseconds**) for completed attempts in the window.
6. Reports `done/s` = `completed / (offer_duration + drain_seconds)` next to
   `queued/s` (oha achieved ingest rate).

Cycle percentiles are millisecond-granularity wall-clock delay from durable
enqueue stamp to durable completion stamp.

## Manual oha (same methodology)

```powershell
oha -c 100 -z 90s --latency-correction http://127.0.0.1:18081/health

oha -c 100 -z 90s -m POST -H "Content-Type: application/json" `
  -D benchmark\oha-job.json --latency-correction http://127.0.0.1:18081/jobs
```

On Windows, prefer `-D` file body — inline `-d` JSON is often mangled by PowerShell.

## Notes

- `benchmark/artillery/` is leftover reference only; the runner no longer calls Artillery.
- Python only orchestrates oha, optional drain polling, and the result table.
