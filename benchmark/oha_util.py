"""Shared oha and SQLite helpers for maqistor benchmarks."""

from __future__ import annotations

import json
import math
import shutil
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote


BASE_URL = "http://127.0.0.1:18081"
INGEST_BODY = '{"name":"bench","payload":{"n":1}}'
BENCH_QUEUE = "bench"


def workspace_root() -> Path:
    here = Path(__file__).resolve().parent
    root = here.parent
    if not (root / "Cargo.toml").is_file():
        raise SystemExit(
            "Run from workspace root (directory with Cargo.toml), e.g.\n"
            "  python benchmark/run.py"
        )
    return root


def default_db_path(root: Path) -> Path:
    return root / "benchmark" / "data" / "maqistor-ingest.db"


def default_results_path(ingest: Path) -> Path:
    """Pair `*-ingest.db` with `*-results.db`; otherwise `<stem>-results.db`."""
    stem = ingest.stem
    if stem.endswith("-ingest"):
        return ingest.with_name(f"{stem[: -len('-ingest')]}-results.db")
    return ingest.with_name(f"{stem}-results.db")


def open_db(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise SystemExit(f"database not found: {path}")
    # Absolute path with forward slashes for SQLite URI on Windows.
    uri_path = path.resolve().as_posix()
    uri = f"file:{quote(uri_path, safe='/')}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    except sqlite3.Error as err:
        raise SystemExit(f"failed to open database {path}: {err}") from err
    conn.row_factory = sqlite3.Row
    return conn


def max_job_id(ingest: sqlite3.Connection) -> int:
    row = ingest.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM jobs").fetchone()
    return int(row["max_id"])


def count_open(
    ingest: sqlite3.Connection,
    results: sqlite3.Connection,
    queue: str,
    after_id: int,
) -> int:
    """Pending ingest rows + running results attempts above the job watermark."""
    pending = ingest.execute(
        """
        SELECT COUNT(*) AS n FROM jobs
        WHERE queue_name = ?1 AND id > ?2 AND status = 'pending'
        """,
        (queue, after_id),
    ).fetchone()
    running = results.execute(
        """
        SELECT COUNT(*) AS n FROM job_attempts
        WHERE queue_name = ?1 AND job_id > ?2 AND status = 'running'
        """,
        (queue, after_id),
    ).fetchone()
    return int(pending["n"]) + int(running["n"])


def wait_drain(
    ingest: sqlite3.Connection,
    results: sqlite3.Connection,
    *,
    queue: str,
    after_id: int,
    timeout_s: float,
    poll_s: float,
) -> tuple[bool, float, int]:
    """Poll until no pending ingest / running results remain above after_id."""
    started = time.monotonic()
    remaining = count_open(ingest, results, queue, after_id)
    if remaining == 0:
        return True, 0.0, 0
    while True:
        elapsed = time.monotonic() - started
        if elapsed >= timeout_s:
            return False, elapsed, remaining
        time.sleep(poll_s)
        remaining = count_open(ingest, results, queue, after_id)
        if remaining == 0:
            return True, time.monotonic() - started, 0


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(sorted_values[low])
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def cycle_stats(
    ingest: sqlite3.Connection,
    results: sqlite3.Connection,
    queue: str,
    after_id: int,
) -> dict:
    """Create→complete cycle ms: results.updated_at - ingest.created_at."""
    jobs_in_window = ingest.execute(
        """
        SELECT COUNT(*) AS n FROM jobs
        WHERE queue_name = ?1 AND id > ?2
        """,
        (queue, after_id),
    ).fetchone()
    jobs_in_window = int(jobs_in_window["n"])

    attempts = results.execute(
        """
        SELECT job_id, status, updated_at FROM job_attempts
        WHERE queue_name = ?1 AND job_id > ?2
          AND status IN ('completed', 'failed')
        """,
        (queue, after_id),
    ).fetchall()

    created = {
        int(row["id"]): int(row["created_at"])
        for row in ingest.execute(
            """
            SELECT id, created_at FROM jobs
            WHERE queue_name = ?1 AND id > ?2
            """,
            (queue, after_id),
        ).fetchall()
    }

    completed = 0
    failed = 0
    cycles: list[float] = []
    for row in attempts:
        job_id = int(row["job_id"])
        status = row["status"]
        if status == "completed":
            completed += 1
            if job_id in created:
                cycles.append(float(int(row["updated_at"]) - created[job_id]))
        elif status == "failed":
            failed += 1
    cycles.sort()
    return {
        "jobs_in_window": jobs_in_window,
        "completed": completed,
        "failed": failed,
        "cycle_p50_ms": _percentile(cycles, 50),
        "cycle_p99_ms": _percentile(cycles, 99),
        "cycle_max_ms": float(cycles[-1]) if cycles else None,
    }


def require_oha() -> str:
    path = shutil.which("oha")
    if not path:
        raise SystemExit(
            "oha not found on PATH.\n"
            "Install with: cargo install oha\n"
            "Ensure %USERPROFILE%\\.cargo\\bin is on PATH."
        )
    return path


def require_standing_server(script_name: str) -> None:
    try:
        with urllib.request.urlopen(f"{BASE_URL}/health", timeout=2) as resp:
            if resp.status < 200 or resp.status >= 300:
                raise SystemExit(f"health returned {resp.status}")
    except urllib.error.URLError as err:
        raise SystemExit(
            f"maqistor is not reachable at {BASE_URL}\n\n"
            "Start it in another terminal first:\n"
            "  cargo build -p maqistor-dispatcher --release\n"
            "  ./target/release/maqistor --config benchmark/maqistor.toml\n\n"
            f"Then re-run: python benchmark/{script_name}\n({err})"
        ) from err


# --- oha load helpers -------------------------------------------------------


def ensure_ingest_body(root: Path) -> Path:
    path = root / "benchmark" / "oha-job.json"
    path.write_text(INGEST_BODY, encoding="ascii", newline="")
    return path


def run_oha(
    oha: str,
    *,
    url: str,
    connections: int | None,
    duration_s: int,
    qps: float | None = None,
    method: str = "GET",
    body_path: Path | None = None,
    raw_out: Path | None = None,
) -> dict:
    cmd = [
        oha,
        "-z",
        f"{duration_s}s",
        "--wait-ongoing-requests-after-deadline",
        "--latency-correction",
        "--output-format",
        "json",
        "--no-tui",
        "-m",
        method,
    ]
    if connections is not None:
        cmd.extend(["-c", str(connections)])
    if qps is not None:
        cmd.extend(["-q", str(qps)])
    if body_path is not None:
        cmd.extend(["-H", "Content-Type: application/json", "-D", str(body_path)])
    if raw_out is not None:
        cmd.extend(["-o", str(raw_out)])
    cmd.append(url)

    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        raise SystemExit(f"oha failed (exit {result.returncode})\n{err}")

    if raw_out is not None and raw_out.is_file():
        text = raw_out.read_text(encoding="utf-8")
    else:
        text = result.stdout
    try:
        return json.loads(text)
    except json.JSONDecodeError as err:
        raise SystemExit(f"failed to parse oha JSON:\n{text[:500]}\n({err})") from err


def rps(report: dict) -> float | None:
    summary = report.get("summary") or {}
    metrics = report.get("metrics") or {}
    if summary.get("requestsPerSec") is not None:
        return float(summary["requestsPerSec"])
    if metrics.get("requests_per_sec") is not None:
        return float(metrics["requests_per_sec"])
    return None


def latency_ms(report: dict, key: str) -> float | None:
    """key: p50 / p99 — oha percentiles are often in seconds."""
    metrics = report.get("metrics") or {}
    latency = metrics.get("latency_ms") or {}
    if latency.get(key) is not None:
        return float(latency[key])

    percentiles = report.get("latencyPercentiles") or {}
    if percentiles.get(key) is not None:
        return float(percentiles[key]) * 1000.0
    return None


def status_counts(report: dict) -> dict[str, int]:
    raw = report.get("statusCodeDistribution") or {}
    return {str(k): int(v) for k, v in raw.items()}


def error_count(report: dict) -> int:
    """Non-2xx HTTP statuses. Ignores oha end-of-run deadline aborts."""
    bad = 0
    for code, count in status_counts(report).items():
        try:
            n = int(code)
        except ValueError:
            bad += count
            continue
        if n < 200 or n >= 300:
            bad += count
    for name, count in (report.get("errorDistribution") or {}).items():
        label = str(name).lower()
        if "deadline" in label or "aborted" in label:
            continue
        bad += int(count)
    return bad


def success_rate(report: dict) -> float | None:
    summary = report.get("summary") or {}
    if summary.get("successRate") is not None:
        return float(summary["successRate"])
    metrics = report.get("metrics") or {}
    if metrics.get("success_rate") is not None:
        return float(metrics["success_rate"])
    return None
