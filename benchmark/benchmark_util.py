"""Shared HTTP, SQLite, and lifecycle helpers for Maqistor benchmarks."""

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


def seed_jobs(
    db_path: Path, *, queue: str, payload: bytes, count: int
) -> tuple[int, int, float, int]:
    """Insert benchmark jobs in one SQLite transaction, outside drain timing."""
    started = time.monotonic()
    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
    except sqlite3.Error as err:
        raise SystemExit(f"failed to open database {db_path} for benchmark seeding: {err}") from err
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("BEGIN IMMEDIATE")
        queue_exists = conn.execute(
            "SELECT 1 FROM job_queues WHERE name = ?1", (queue,)
        ).fetchone()
        if queue_exists is None:
            raise SystemExit(f"queue not found in ingest database: {queue}")
        now_ms = time.time_ns() // 1_000_000
        conn.executemany(
            """
            INSERT INTO accepted_jobs(queue_name, payload, dispatch_id, created_at, updated_at)
            VALUES (?1, ?2, NULL, ?3, ?3)
            """,
            ((queue, payload, now_ms) for _ in range(count)),
        )
        last_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.commit()
        committed_at_ms = time.time_ns() // 1_000_000
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    return last_id - count + 1, last_id, time.monotonic() - started, committed_at_ms


def max_job_id(ingest: sqlite3.Connection) -> int:
    row = ingest.execute(
        "SELECT COALESCE(MAX(id), 0) AS max_id FROM accepted_jobs"
    ).fetchone()
    return int(row["max_id"])


def count_open(
    ingest: sqlite3.Connection,
    results: sqlite3.Connection,
    queue: str,
    after_id: int,
    through_id: int | None = None,
) -> int:
    """Available ingest rows + running executions above the job watermark."""
    pending = ingest.execute(
        """
        SELECT COUNT(*) AS n FROM accepted_jobs
        WHERE queue_name = ?1 AND id > ?2 AND (?3 IS NULL OR id <= ?3)
          AND dispatch_id IS NULL
        """,
        (queue, after_id, through_id),
    ).fetchone()
    running = results.execute(
        """
        SELECT COUNT(*) AS n FROM executions
        WHERE queue_name = ?1 AND job_id > ?2 AND (?3 IS NULL OR job_id <= ?3)
          AND status = 'running'
        """,
        (queue, after_id, through_id),
    ).fetchone()
    return int(pending["n"]) + int(running["n"])


def wait_drain(
    ingest: sqlite3.Connection,
    results: sqlite3.Connection,
    *,
    queue: str,
    after_id: int,
    through_id: int | None = None,
    timeout_s: float,
    poll_s: float,
    started_at: float | None = None,
) -> tuple[bool, float, int]:
    """Poll until no available ingest / running executions remain above after_id."""
    started = time.monotonic() if started_at is None else started_at
    remaining = count_open(ingest, results, queue, after_id, through_id)
    if remaining == 0:
        return True, 0.0, 0
    while True:
        elapsed = time.monotonic() - started
        if elapsed >= timeout_s:
            return False, elapsed, remaining
        time.sleep(poll_s)
        remaining = count_open(ingest, results, queue, after_id, through_id)
        if remaining == 0:
            return True, time.monotonic() - started, 0


def wait_terminal(
    results: sqlite3.Connection,
    *,
    queue: str,
    after_id: int,
    through_id: int,
    expected: int,
    timeout_s: float,
    poll_s: float,
    started_at: float | None = None,
) -> tuple[bool, float, int]:
    """Poll until every job in an exact ID range has a terminal execution."""
    started = time.monotonic() if started_at is None else started_at

    def remaining() -> int:
        terminal = results.execute(
            """
            SELECT COUNT(*) AS n FROM executions
            WHERE queue_name = ?1 AND job_id > ?2 AND job_id <= ?3
              AND status IN ('completed', 'failed')
            """,
            (queue, after_id, through_id),
        ).fetchone()
        return max(0, expected - int(terminal["n"]))

    outstanding = remaining()
    if outstanding == 0:
        return True, time.monotonic() - started, 0
    while True:
        elapsed = time.monotonic() - started
        if elapsed >= timeout_s:
            return False, elapsed, outstanding
        time.sleep(poll_s)
        outstanding = remaining()
        if outstanding == 0:
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
    through_id: int | None = None,
) -> dict:
    """Create→complete cycle ms: execution.updated_at - accepted_jobs.created_at."""
    jobs_in_window = ingest.execute(
        """
        SELECT COUNT(*) AS n FROM accepted_jobs
        WHERE queue_name = ?1 AND id > ?2 AND (?3 IS NULL OR id <= ?3)
        """,
        (queue, after_id, through_id),
    ).fetchone()
    jobs_in_window = int(jobs_in_window["n"])

    executions = results.execute(
        """
        SELECT job_id, status, updated_at FROM executions
        WHERE queue_name = ?1 AND job_id > ?2
          AND (?3 IS NULL OR job_id <= ?3)
          AND status IN ('completed', 'failed')
        """,
        (queue, after_id, through_id),
    ).fetchall()
    first_claimed = ingest.execute(
        """
        SELECT MIN(updated_at) AS first_claimed_at_ms FROM accepted_jobs
        WHERE queue_name = ?1 AND id > ?2 AND (?3 IS NULL OR id <= ?3)
          AND dispatch_id IS NOT NULL
        """,
        (queue, after_id, through_id),
    ).fetchone()
    last_terminal = results.execute(
        """
        SELECT MAX(updated_at) AS last_terminal_at_ms FROM executions
        WHERE queue_name = ?1 AND job_id > ?2 AND (?3 IS NULL OR job_id <= ?3)
          AND status IN ('completed', 'failed')
        """,
        (queue, after_id, through_id),
    ).fetchone()

    created = {
        int(row["id"]): int(row["created_at"])
        for row in ingest.execute(
            """
            SELECT id, created_at FROM accepted_jobs
            WHERE queue_name = ?1 AND id > ?2 AND (?3 IS NULL OR id <= ?3)
            """,
            (queue, after_id, through_id),
        ).fetchall()
    }

    completed = 0
    failed = 0
    cycles: list[float] = []
    for row in executions:
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
        "first_claimed_at_ms": first_claimed["first_claimed_at_ms"],
        "last_terminal_at_ms": last_terminal["last_terminal_at_ms"],
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
