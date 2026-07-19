# Benchmark suite

External load tests against a **standing** maqistor process. Scripts do **not** start the server.

Driver: **[oha](https://github.com/hatoo/oha)** (orchestrated by Python). Same stages, two load shapes:

| Script | oha mode | Question |
|--------|----------|----------|
| `run_closed.py` | `-c` concurrent connections | Where does **throughput drop** across stages? |
| `run_open.py` | `-q` offered QPS | Can we **absorb a target rate**? |
| `run_capacity.py` | closed + open sweeps | What is the durable-ingest ceiling? |

Use **closed** to find stagger (health → ingest → full).  
Use **open** to check whether a chosen offer holds (ach/off).
All runners wait for requests already in flight at the duration deadline, so
the last durable batch is counted rather than aborted by the load generator.

Dispatch is internal — not a stage. It only appears inside full E2E later.

## Prerequisites

```bash
cargo install oha
# ensure ~/.cargo/bin (or %USERPROFILE%\.cargo\bin) is on PATH
```

Prefer a **release** server for meaningful numbers:

```bash
cargo build -p maqistor-dispatcher --release
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

Run the binary from the workspace root so config/`database_path` resolve.  
Avoid `cargo run -p maqistor-dispatcher -- --config ...` (wrong cwd).

Listens on `http://127.0.0.1:18081`, job name `bench`.  
DB under `benchmark/data/` (gitignored).  
Group-commit self-tunes from request rate, SQL commit rate, commit duration, and
batch fill. `benchmark/maqistor.toml` may set an EWMA window and optional hard
batch/wait caps, but never selects a fixed batch or timeout.

## Concurrent (`run_closed.py`)

oha keeps `-c` connections busy (same idea as your manual `oha -c 100 -z 90s` runs).

```bash
python benchmark/run_closed.py
python benchmark/run_closed.py --load high --sustain medium
python benchmark/run_closed.py --stages health,ingest
```

| Flag | Values | Default | Meaning |
|------|--------|---------|---------|
| `--load` | `low` / `medium` / `high` | `low` | `-c` 32 / 100 / 200 |
| `--sustain` | `low` / `medium` / `high` | `low` | `-z` 30s / 90s / 150s |
| `--stages` | comma list | all three | `health`, `ingest`, `full` |

Example:

```text
stage           ops/s   p50_ms   p99_ms   errors  delta_ops
-------------------------------------------------------------
health        55000.0      1.7      4.0        0          —
ingest         4200.0     18.0     69.0        0   -50800.0
full          skipped
```

- **ops/s** = oha `requestsPerSec` (ingest ⇒ **jobs/s**)
- Fails fast if status codes are not `204` (health) / `201` (ingest)
- Writes `benchmark/results/summary-closed-*.json` (+ raw oha JSON under `results/raw/`)

## Offer (`run_open.py`)

oha `-q` rate limit + enough `-c` to sustain the offer.

```bash
python benchmark/run_open.py
python benchmark/run_open.py --load medium --sustain low
```

| Flag | Values | Default | Meaning |
|------|--------|---------|---------|
| `--load` | `low` / `medium` / `high` | `low` | `-q` 10k / 50k / 100k (`-c` 100 / 200 / 400) |
| `--sustain` | `low` / `medium` / `high` | `low` | `-z` 10s / 15s / 30s |
| `--stages` | comma list | all three | same as closed |

Example:

```text
stage       offered   achieved   ach/off   p50_ms   p99_ms   errors
--------------------------------------------------------------------
health        10000   10000.2      100%      1.5      3.0        0
ingest        10000    9980.1      100%     12.0     40.0        0
full              —    skipped
```

ach/off ≈ 100% means the offer was absorbed — not that you found the ceiling.

## Capacity (`run_capacity.py`)

Use this for performance work. It tests only durable `POST /jobs`, first with a
closed-loop concurrency sweep and then with an open-loop QPS sweep. Raw oha
reports are retained for every point.

```powershell
# Six closed and six open points, 30 seconds each (about six minutes total).
python benchmark\run_capacity.py

# Discover the closed-loop ceiling first.
python benchmark\run_capacity.py --mode closed

# Test the 8k–14k region with a generous client concurrency.
python benchmark\run_capacity.py --mode open --open-connections 1000 `
  --open-qps 8000,9000,10000,11000,12000,14000
```

An open-loop point is marked **stable** only when it has zero errors, achieves
at least 98% of its offered QPS, and stays below the configured p99 guardrail
(100 ms by default; override with `--max-p99-ms`). The closed-loop peak is an
observed ceiling for this machine and local oha client, not a universal SQLite
limit.

For one-off points, both ordinary runners accept exact overrides:

```powershell
python benchmark\run_closed.py --connections 800 --stages ingest
python benchmark\run_open.py --qps 10000 --connections 1000 --stages ingest
```

## Stages

| Stage | Request | Status |
|-------|---------|--------|
| `health` | `GET /health` | live |
| `ingest` | `POST /jobs` (body `benchmark/oha-job.json`, auto-written) | live |
| `full` | post-run DB timestamps | skipped until sleep-job workers exist |

## Manual oha (same methodology)

```powershell
oha -c 100 -z 90s --latency-correction http://127.0.0.1:18081/health

oha -c 100 -z 90s -m POST -H "Content-Type: application/json" `
  -D benchmark\oha-job.json --latency-correction http://127.0.0.1:18081/jobs
```

On Windows, prefer `-D` file body — inline `-d` JSON is often mangled by PowerShell.

## Notes

- `benchmark/artillery/` is leftover reference only; runners no longer call Artillery.
- Python here only orchestrates oha and prints the stage table.
