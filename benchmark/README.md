# Benchmark suite

Standalone load tests. The runner starts the release `maqistor` binary against
fresh databases under `benchmark/data/`, runs the selected workload, then stops
the server and removes that directory. It refuses to run if another Maqistor
process is already listening on the benchmark port, and waits for every managed
worker configured in `benchmark/maqistor.toml` to register before testing.

For server setup and the component reading graph, start with the
[top-level README](../README.md).

HTTP driver: **[oha](https://github.com/hatoo/oha)** (orchestrated by
`benchmark/run.py`). Drain mode seeds SQLite directly; helpers live in
`benchmark/benchmark_util.py`.

| Mode | Load shape | Question |
|------|-----------|----------|
| `closed` | `-c` concurrency sweep | Where does durable ingest throughput peak? |
| `open` | `-q` offered QPS sweep | Can we absorb a target rate? |
| `both` | closed then open | Combined capacity sweep (default) |
| `drain` | direct SQLite seed + drain | How fast a preloaded batch completes, without HTTP load |
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
before running a benchmark:

```bash
sh benchmark/generate-certs.sh
docker build -f benchmark/noop-worker/Dockerfile -t maqistor-benchmark-noop-worker:0.1.0 .
```

The worker connects to `host.docker.internal:17829` (Docker Desktop) and
discards each JSON payload before returning an empty successful result. The
certificate directory is ignored by Git.

`--mode full` and `--mode drain` require this worker so jobs drain and complete.
The runner owns only its Maqistor process and database directory; managed Docker
worker containers remain available for the next benchmark run.

Prefer a **release** server for meaningful numbers:

```bash
cargo build -p maqistor --release
```

## Build maqistor

Build the release binary once from the workspace root (`maqistor/`):

```bash
cargo build -p maqistor --release
```

`run.py` starts `target/release/maqistor` with `benchmark/maqistor.toml` from
that workspace root. It listens on `http://127.0.0.1:18081`, creates
`maqistor-ingest.db` and `maqistor-results.db` in `benchmark/data/`, and deletes
the entire data directory when it exits (including after an error or Ctrl+C).
Enqueue and completion use **separate SQLite writers** so completes do not share
the ingest commit pipe. Each side still self-tunes batch size/wait from request
rate, SQL commit rate, commit duration, and batch fill.
`benchmark/maqistor.toml` sets persistence writer batching under
`[persistence.enqueue]` / `[persistence.completion]`, and dispatch ceilings
under `[dispatch]` (`batch_size_max`, `max_delivery_in_flight`). The delivery
limit is a safety ceiling: worker reservations determine the live capacity.
The scheduler also
reconciles inactive queues with `idle_probe_interval_ms` (default `1000`) and
`idle_probe_batch_size` (default `64`). Claim size follows
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

# Drain only: seed jobs in one SQLite transaction (excluded from timing), then drain them.
python benchmark\run.py --mode drain --drain-jobs 100000 `
  --drain-timeout-seconds 120
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--mode` | `both` | `closed` / `open` / `both` / `full` / `drain` |
| `--duration` | `30` | Seconds per point (`-z`) |
| `--closed-connections` | `50,100,200,400,800,1200` | Closed-loop `-c` values |
| `--open-qps` | `4000,6000,8000,10000,12000,16000` | Open/full `-q` values |
| `--open-connections` | `1000` | `-c` for every open/full point |
| `--max-p99-ms` | `100` | Stability p99 guardrail |
| `--settle-seconds` | `5` | Pause after each point before the next |
| `--drain-timeout-seconds` | `120` | Full/drain: max wait for queue drain |
| `--drain-poll-seconds` | `0.5` | Full/drain: drain poll interval |
| `--drain-jobs` | — | Drain only: required number of jobs to seed |
| `--db` | `benchmark/data/maqistor-ingest.db` | Must remain the default path because the runner starts the bundled benchmark config |
| `--server-startup-timeout-seconds` | `30` | Max wait for the runner-started server health check |
| `--worker-startup-timeout-seconds` | `90` | Max wait for all configured managed workers to connect before testing |

Open and full mode show `queue` stability when they have zero errors, achieve
at least 98% of their offered QPS, and stay below the HTTP p99 guardrail. Full
mode also shows `drain/done` stability only when the queue drains before the
timeout, every benchmark job completes, and none fail. The closed-loop peak is
an observed ceiling for this machine and local oha client, not a universal
SQLite limit.

Raw oha JSON lands under `benchmark/results/raw/`. Summaries are
`summary-capacity-*.json`, `summary-capacity-full-*.json`, or
`summary-drain-*.json`.

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

### Drain-only metrics

Drain mode writes the requested jobs directly into the existing `bench` queue
in one SQLite transaction. That seed time is reported separately and excluded
from all drain timings; no HTTP load generator is used. It reports `wake_s`
(seed commit to first persisted claim), `drain_s` (first claim to all seeded
jobs terminal), and `total_s` (seed commit to all seeded jobs terminal). It
refuses to start if `bench` already has open jobs, so the result measures worker,
dispatch, and completion throughput without concurrent ingest or prior queue
backlog.

## Manual oha (same methodology)

```powershell
oha -c 100 -z 90s --latency-correction http://127.0.0.1:18081/health

oha -c 100 -z 90s -m POST -H "Content-Type: application/json" `
  -D benchmark\oha-job.json --latency-correction http://127.0.0.1:18081/jobs
```

On Windows, prefer `-D` file body — inline `-d` JSON is often mangled by PowerShell.

## Notes

- `benchmark/artillery/` is leftover reference only; the runner no longer calls Artillery.
- Python owns the benchmark server lifecycle, then orchestrates oha, optional
  drain polling, and the result table. Server logs are retained in
  `benchmark/results/maqistor-*.log`; raw oha output and summaries are retained
  too.
