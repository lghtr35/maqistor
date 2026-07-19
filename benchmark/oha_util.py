"""Shared oha helpers for maqistor benchmarks."""

from __future__ import annotations

import json
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path


BASE_URL = "http://127.0.0.1:18081"
INGEST_BODY = '{"name":"bench","payload":{"n":1}}'


def workspace_root() -> Path:
    here = Path(__file__).resolve().parent
    root = here.parent
    if not (root / "Cargo.toml").is_file():
        raise SystemExit(
            "Run from workspace root (directory with Cargo.toml), e.g.\n"
            "  python benchmark/run_closed.py"
        )
    return root


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
