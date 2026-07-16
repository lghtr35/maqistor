# Benchmark suite

External load tests against a **standing** maqistor process. Scripts do **not** start the server.

Driver: **[oha](https://github.com/hatoo/oha)** (orchestrated by Python). Same stages, two load shapes:

| Script | oha mode | Question |
|--------|----------|----------|
| `run_closed.py` | `-c` concurrent connections | Where does **throughput drop** across stages? |
| `run_open.py` | `-q` offered QPS | Can we **absorb a target rate**? |

Use **closed** to find stagger (health → ingest → full).  
Use **open** to check whether a chosen offer holds (ach/off).

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
Group-commit: `[persistence]` in `benchmark/maqistor.toml`
(`batch_size`, fixed `batch_wait_ms`, or optional `adaptive_batch_wait` + min/max).

## Concurrent (`run_closed.py`)

oha keeps `-c` connections busy (same idea as your manual `oha -c 100 -z 15s` runs).

```bash
python benchmark/run_closed.py
python benchmark/run_closed.py --load high --sustain medium
python benchmark/run_closed.py --stages health,ingest
```

| Flag | Values | Default | Meaning |
|------|--------|---------|---------|
| `--load` | `low` / `medium` / `high` | `low` | `-c` 8 / 32 / 100 |
| `--sustain` | `low` / `medium` / `high` | `low` | `-z` 10s / 15s / 30s |
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
- Fails fast if status codes are not `200` (health) / `201` (ingest)
- Writes `benchmark/results/summary-closed-*.json` (+ raw oha JSON under `results/raw/`)

## Offer (`run_open.py`)

oha `-q` rate limit + enough `-c` to sustain the offer.

```bash
python benchmark/run_open.py
python benchmark/run_open.py --load medium --sustain low
```

| Flag | Values | Default | Meaning |
|------|--------|---------|---------|
| `--load` | `low` / `medium` / `high` | `low` | `-q` 100 / 1000 / 5000 |
| `--sustain` | `low` / `medium` / `high` | `low` | `-z` 10s / 15s / 30s |
| `--stages` | comma list | all three | same as closed |

Example:

```text
stage       offered   achieved   ach/off   p50_ms   p99_ms   errors
--------------------------------------------------------------------
health         1000    1000.2      100%      1.5      3.0        0
ingest         1000     998.1      100%     12.0     40.0        0
full              —    skipped
```

ach/off ≈ 100% means the offer was absorbed — not that you found the ceiling.

## Stages

| Stage | Request | Status |
|-------|---------|--------|
| `health` | `GET /health` | live |
| `ingest` | `POST /jobs` (body `benchmark/oha-job.json`, auto-written) | live |
| `full` | post-run DB timestamps | skipped until sleep-job workers exist |

## Manual oha (same methodology)

```powershell
oha -c 100 -z 15s --latency-correction http://127.0.0.1:18081/health

oha -c 100 -z 15s -m POST -H "Content-Type: application/json" `
  -D benchmark\oha-job.json --latency-correction http://127.0.0.1:18081/jobs
```

On Windows, prefer `-D` file body — inline `-d` JSON is often mangled by PowerShell.

## Notes

- `benchmark/artillery/` is leftover reference only; runners no longer call Artillery.
- Python here only orchestrates oha and prints the stage table.
