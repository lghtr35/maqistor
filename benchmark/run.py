#!/usr/bin/env python3
"""Standalone Maqistor benchmark runner.

The runner starts a release server against fresh benchmark databases, runs the
selected workload, and always stops the server and removes benchmark/data.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchmark_util import (
    BASE_URL,
    BENCH_QUEUE,
    INGEST_BODY,
    cycle_stats,
    count_open,
    default_db_path,
    default_results_path,
    ensure_ingest_body,
    error_count,
    latency_ms,
    max_job_id,
    open_db,
    require_oha,
    rps,
    run_oha,
    seed_jobs,
    status_counts,
    wait_drain,
    wait_terminal,
    workspace_root,
)

DEFAULT_CLOSED_CONNECTIONS = (50, 100, 200, 400, 800, 1200)
DEFAULT_OPEN_QPS = (4_000, 6_000, 8_000, 10_000, 12_000, 16_000)


@dataclass
class Result:
    mode: str
    offered_rps: int | None
    connections: int
    achieved_rps: float | None
    achieved_over_offered: float | None
    p50_ms: float | None
    p99_ms: float | None
    errors: int
    status_codes: dict[str, int]
    queue_stable: bool
    drain_done_stable: bool | None = None
    backlog_at_end: int | None = None
    drain_seconds: float | None = None
    drain_ok: bool | None = None
    jobs_in_window: int | None = None
    completed: int | None = None
    failed: int | None = None
    completed_rps: float | None = None
    cycle_p50_ms: float | None = None
    cycle_p99_ms: float | None = None
    cycle_max_ms: float | None = None


@dataclass
class DrainResult:
    jobs_seeded: int
    first_job_id: int
    last_job_id: int
    seed_seconds: float
    wake_seconds: float | None
    processing_drain_seconds: float | None
    total_seconds: float
    drain_ok: bool
    remaining: int
    completed: int
    failed: int
    completed_rps: float | None
    cycle_p50_ms: float | None
    cycle_p99_ms: float | None
    cycle_max_ms: float | None
    drain_done_stable: bool


def positive_csv(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as err:
        raise argparse.ArgumentTypeError("values must be comma-separated integers") from err
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("values must all be greater than zero")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark Maqistor ingest capacity, full-cycle flow, or a seeded drain.",
    )
    parser.add_argument(
        "--mode",
        choices=("closed", "open", "both", "full", "drain"),
        default="both",
        help="closed/open/both = ingest capacity; full = offer + drain; drain = seed then drain",
    )
    parser.add_argument("--duration", type=int, default=30, help="Seconds per point (default: 30)")
    parser.add_argument(
        "--closed-connections",
        type=positive_csv,
        default=DEFAULT_CLOSED_CONNECTIONS,
        help="Closed-loop -c values, comma-separated",
    )
    parser.add_argument(
        "--open-qps",
        type=positive_csv,
        default=DEFAULT_OPEN_QPS,
        help="Open-loop -q values, comma-separated",
    )
    parser.add_argument(
        "--open-connections",
        type=int,
        default=1000,
        help="Concurrent connections for every open-loop / full point (default: 1000)",
    )
    parser.add_argument(
        "--max-p99-ms",
        type=float,
        default=100.0,
        help="Largest acceptable p99 when judging open-loop stability (default: 100)",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=5.0,
        help="Pause after each point before the next (default: 5)",
    )
    parser.add_argument(
        "--drain-timeout-seconds",
        type=float,
        default=120.0,
        help="Full/drain mode: max seconds to wait for queue drain (default: 120)",
    )
    parser.add_argument(
        "--drain-poll-seconds",
        type=float,
        default=0.5,
        help="Full/drain mode: drain poll interval (default: 0.5)",
    )
    parser.add_argument(
        "--drain-jobs",
        type=int,
        default=None,
        help="Drain mode: number of jobs to seed directly into SQLite before timing",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Ingest SQLite path (must be benchmark/data/maqistor-ingest.db)",
    )
    parser.add_argument(
        "--server-startup-timeout-seconds",
        type=float,
        default=30.0,
        help="Seconds to wait for the runner-started server health check (default: 30)",
    )
    parser.add_argument(
        "--worker-startup-timeout-seconds",
        type=float,
        default=90.0,
        help="Seconds to wait for configured managed workers to connect (default: 90)",
    )
    args = parser.parse_args()
    if args.duration <= 0 or args.open_connections <= 0 or args.max_p99_ms <= 0:
        parser.error("duration, open-connections, and max-p99-ms must be greater than zero")
    if args.settle_seconds < 0:
        parser.error("settle-seconds must be >= 0")
    if args.drain_timeout_seconds <= 0 or args.drain_poll_seconds <= 0:
        parser.error("drain-timeout-seconds and drain-poll-seconds must be greater than zero")
    if args.server_startup_timeout_seconds <= 0:
        parser.error("server-startup-timeout-seconds must be greater than zero")
    if args.worker_startup_timeout_seconds <= 0:
        parser.error("worker-startup-timeout-seconds must be greater than zero")
    if args.mode == "drain" and (args.drain_jobs is None or args.drain_jobs <= 0):
        parser.error("--mode drain requires --drain-jobs greater than zero")
    return args


def fmt(value: float | None, digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:,.{digits}f}"


def ingest_result(
    *,
    oha: str,
    body: Path,
    duration: int,
    mode: str,
    connections: int,
    offered: int | None,
    max_p99_ms: float,
    raw_out: Path,
) -> Result:
    report = run_oha(
        oha,
        url=f"{BASE_URL}/jobs",
        connections=connections,
        duration_s=duration,
        qps=float(offered) if offered is not None else None,
        method="POST",
        body_path=body,
        raw_out=raw_out,
    )
    codes = status_counts(report)
    achieved = rps(report)
    p99 = latency_ms(report, "p99")
    errors = error_count(report)
    ratio = achieved / offered if achieved is not None and offered is not None else None
    queue_stable = errors == 0 and (p99 is None or p99 <= max_p99_ms)
    if offered is not None:
        queue_stable = queue_stable and ratio is not None and ratio >= 0.98
    return Result(
        mode=mode,
        offered_rps=offered,
        connections=connections,
        achieved_rps=achieved,
        achieved_over_offered=ratio,
        p50_ms=latency_ms(report, "p50"),
        p99_ms=p99,
        errors=errors,
        status_codes=codes,
        queue_stable=queue_stable,
    )


def run_full_point(
    *,
    oha: str,
    body: Path,
    db_path: Path,
    results_db_path: Path,
    duration: int,
    connections: int,
    offered: int,
    max_p99_ms: float,
    drain_timeout_s: float,
    drain_poll_s: float,
    raw_out: Path,
) -> Result:
    with open_db(db_path) as ingest:
        watermark = max_job_id(ingest)

    result = ingest_result(
        oha=oha,
        body=body,
        duration=duration,
        mode="full",
        connections=connections,
        offered=offered,
        max_p99_ms=max_p99_ms,
        raw_out=raw_out,
    )

    with open_db(db_path) as ingest, open_db(results_db_path) as results:
        backlog = count_open(ingest, results, BENCH_QUEUE, watermark)
        drained, drain_seconds, remaining = wait_drain(
            ingest,
            results,
            queue=BENCH_QUEUE,
            after_id=watermark,
            timeout_s=drain_timeout_s,
            poll_s=drain_poll_s,
        )
        stats = cycle_stats(ingest, results, BENCH_QUEUE, watermark)

    result.backlog_at_end = backlog
    result.drain_seconds = drain_seconds
    result.drain_ok = drained
    result.jobs_in_window = stats["jobs_in_window"]
    result.completed = stats["completed"]
    result.failed = stats["failed"]
    result.cycle_p50_ms = stats["cycle_p50_ms"]
    result.cycle_p99_ms = stats["cycle_p99_ms"]
    result.cycle_max_ms = stats["cycle_max_ms"]
    wall_s = float(duration) + drain_seconds
    if stats["completed"] and wall_s > 0:
        result.completed_rps = stats["completed"] / wall_s
    result.drain_done_stable = (
        drained
        and stats["completed"] == stats["jobs_in_window"]
        and stats["failed"] == 0
    )
    if not drained:
        print(
            f"  drain incomplete: {remaining} open jobs remaining after "
            f"{fmt(drain_seconds)}s (timeout {drain_timeout_s:g}s), "
            f"completed/s={fmt(result.completed_rps)}"
        )
    else:
        print(
            f"  drain {fmt(drain_seconds)}s, backlog_at_end={backlog}, "
            f"completed/s={fmt(result.completed_rps)}, "
            f"cycle p50={fmt(result.cycle_p50_ms)}ms p99={fmt(result.cycle_p99_ms)}ms "
            f"max={fmt(result.cycle_max_ms)}ms"
        )
    return result


def run_drain_point(
    *,
    db_path: Path,
    results_db_path: Path,
    jobs: int,
    drain_timeout_s: float,
    drain_poll_s: float,
) -> DrainResult:
    """Seed one durable batch, then measure its processing-only drain time."""
    with open_db(db_path) as ingest, open_db(results_db_path) as results:
        existing_open = count_open(ingest, results, BENCH_QUEUE, 0)
    if existing_open:
        raise SystemExit(
            f"cannot start drain test: {existing_open:,} open jobs already exist in {BENCH_QUEUE!r}; "
            "drain the queue first"
        )
    first_id, last_id, seed_seconds, seed_committed_at_ms = seed_jobs(
        db_path,
        queue=BENCH_QUEUE,
        payload=INGEST_BODY.encode("utf-8"),
        count=jobs,
    )
    drain_started = time.monotonic()
    with open_db(db_path) as ingest, open_db(results_db_path) as results:
        drained, drain_seconds, remaining = wait_terminal(
            results,
            queue=BENCH_QUEUE,
            after_id=first_id - 1,
            through_id=last_id,
            expected=jobs,
            timeout_s=drain_timeout_s,
            poll_s=drain_poll_s,
            started_at=drain_started,
        )
        stats = cycle_stats(
            ingest,
            results,
            BENCH_QUEUE,
            first_id - 1,
            through_id=last_id,
        )
    first_claimed_at_ms = stats["first_claimed_at_ms"]
    last_terminal_at_ms = stats["last_terminal_at_ms"]
    wake_seconds = (
        max(0.0, (int(first_claimed_at_ms) - seed_committed_at_ms) / 1_000.0)
        if first_claimed_at_ms is not None
        else None
    )
    processing_drain_seconds = (
        max(0.0, (int(last_terminal_at_ms) - int(first_claimed_at_ms)) / 1_000.0)
        if first_claimed_at_ms is not None and last_terminal_at_ms is not None
        else None
    )
    completed_rps = (
        stats["completed"] / processing_drain_seconds
        if processing_drain_seconds and processing_drain_seconds > 0
        else None
    )
    drain_done_stable = (
        drained and stats["completed"] == jobs and stats["failed"] == 0
    )
    return DrainResult(
        jobs_seeded=jobs,
        first_job_id=first_id,
        last_job_id=last_id,
        seed_seconds=seed_seconds,
        wake_seconds=wake_seconds,
        processing_drain_seconds=processing_drain_seconds,
        total_seconds=drain_seconds,
        drain_ok=drained,
        remaining=remaining,
        completed=stats["completed"],
        failed=stats["failed"],
        completed_rps=completed_rps,
        cycle_p50_ms=stats["cycle_p50_ms"],
        cycle_p99_ms=stats["cycle_p99_ms"],
        cycle_max_ms=stats["cycle_max_ms"],
        drain_done_stable=drain_done_stable,
    )


def print_drain_result(result: DrainResult) -> None:
    print()
    header = (
        f"{'seeded':>9} {'seed_s':>8} {'wake_s':>8} {'drain_s':>8} {'total_s':>8} {'done/s':>10} {'completed':>10} "
        f"{'failed':>7} {'remaining':>10} {'cyc_p50ms':>9} {'cyc_p99ms':>9} {'drain/done':>10}"
    )
    print(header)
    print("-" * len(header))
    print(
        f"{result.jobs_seeded:>9,} {result.seed_seconds:>8.3f} {fmt(result.wake_seconds):>8} "
        f"{fmt(result.processing_drain_seconds):>8} {result.total_seconds:>8.1f} "
        f"{fmt(result.completed_rps):>10} {result.completed:>10,} {result.failed:>7,} "
        f"{result.remaining:>10,} {fmt(result.cycle_p50_ms):>9} "
        f"{fmt(result.cycle_p99_ms):>9} {'yes' if result.drain_done_stable else 'no':>10}"
    )


def settle(seconds: float, *, remaining_points: int) -> None:
    if remaining_points <= 0 or seconds <= 0:
        return
    print(f"  settle {seconds:g}s")
    time.sleep(seconds)


def print_results(results: list[Result], max_p99_ms: float, *, full: bool) -> None:
    print()
    if full:
        header = (
            f"{'mode':<5} {'-c':>6} {'offered':>9} {'queued/s':>10} {'done/s':>10} "
            f"{'ach/off':>8} {'p50_ms':>8} {'p99_ms':>8} {'errors':>7} {'backlog':>8} "
            f"{'drain_s':>8} {'cyc_p50ms':>9} {'cyc_p99ms':>9} {'queue':>7} {'drain/done':>10}"
        )
    else:
        header = (
            f"{'mode':<7} {'-c':>6} {'offered':>9} {'achieved':>10} {'ach/off':>8} "
            f"{'p50_ms':>8} {'p99_ms':>8} {'errors':>7} {'queue':>7}"
        )
    print(header)
    print("-" * len(header))
    for row in results:
        ratio = "-" if row.achieved_over_offered is None else f"{row.achieved_over_offered:.0%}"
        offered = "-" if row.offered_rps is None else f"{row.offered_rps:,}"
        if full:
            backlog = "-" if row.backlog_at_end is None else f"{row.backlog_at_end:,}"
            print(
                f"{row.mode:<5} {row.connections:>6,} {offered:>9} {fmt(row.achieved_rps):>10} "
                f"{fmt(row.completed_rps):>10} {ratio:>8} {fmt(row.p50_ms):>8} "
                f"{fmt(row.p99_ms):>8} {row.errors:>7} {backlog:>8} "
                f"{fmt(row.drain_seconds):>8} {fmt(row.cycle_p50_ms):>9} "
                f"{fmt(row.cycle_p99_ms):>9} {'yes' if row.queue_stable else 'no':>7} "
                f"{'yes' if row.drain_done_stable else 'no':>10}"
            )
        else:
            print(
                f"{row.mode:<7} {row.connections:>6,} {offered:>9} {fmt(row.achieved_rps):>10} "
                f"{ratio:>8} {fmt(row.p50_ms):>8} {fmt(row.p99_ms):>8} {row.errors:>7} "
                f"{'yes' if row.queue_stable else 'no':>7}"
            )

    closed = [row for row in results if row.mode == "closed"]
    closed_stable = [row for row in closed if row.queue_stable]
    queue_stable_open = [
        row for row in results if row.mode in ("open", "full") and row.queue_stable
    ]
    drain_done_stable_full = [
        row for row in results if row.mode == "full" and row.drain_done_stable
    ]
    if closed:
        best = max(closed, key=lambda row: row.achieved_rps or 0.0)
        print(f"\nClosed-loop peak observed: {fmt(best.achieved_rps)} jobs/s at -c {best.connections}.")
    if closed_stable:
        best = max(closed_stable, key=lambda row: row.achieved_rps or 0.0)
        print(
            f"Highest closed-loop result within the {max_p99_ms:g} ms p99 guardrail: "
            f"{fmt(best.achieved_rps)} jobs/s at -c {best.connections}."
        )
    if queue_stable_open:
        best = max(queue_stable_open, key=lambda row: row.offered_rps or 0)
        print(
            f"Highest queue-stable offer: {best.offered_rps:,} jobs/s "
            f"(>=98% achieved, zero errors, p99 <= {max_p99_ms:g} ms"
            + ")."
        )
    if full and drain_done_stable_full:
        best = max(drain_done_stable_full, key=lambda row: row.offered_rps or 0)
        print(
            f"Highest drain/done-stable full-cycle offer: {best.offered_rps:,} jobs/s "
            "(queue drained, every benchmark job completed, zero failed)."
        )


def run_benchmark(
    args: argparse.Namespace,
    *,
    root: Path,
    stamp: str,
    db_path: Path,
    results_db_path: Path,
) -> None:
    if args.mode in ("full", "drain"):
        # The runner-created server has initialized both fresh databases.
        open_db(db_path).close()
        open_db(results_db_path).close()
    if args.mode == "drain":
        assert args.drain_jobs is not None
        print(f"drain: seed {args.drain_jobs:,} jobs, timeout {args.drain_timeout_seconds:g}s")
        result = run_drain_point(
            db_path=db_path,
            results_db_path=results_db_path,
            jobs=args.drain_jobs,
            drain_timeout_s=args.drain_timeout_seconds,
            drain_poll_s=args.drain_poll_seconds,
        )
        if result.drain_ok:
            print(
                f"  seed {fmt(result.seed_seconds, 3)}s (excluded); "
                f"wake {fmt(result.wake_seconds)}s, drain {fmt(result.processing_drain_seconds)}s, "
                f"total {fmt(result.total_seconds)}s, completed/s={fmt(result.completed_rps)}"
            )
        else:
            print(
                f"  drain incomplete: {result.remaining:,} open jobs remaining after "
                f"{fmt(result.total_seconds)}s (timeout {args.drain_timeout_seconds:g}s)"
            )
        summary = {
            "kind": "drain",
            "driver": "sqlite-seed",
            "timestamp": stamp,
            "mode": "drain",
            "drain_jobs": args.drain_jobs,
            "drain_timeout_seconds": args.drain_timeout_seconds,
            "drain_poll_seconds": args.drain_poll_seconds,
            "db": str(db_path),
            "results_db": str(results_db_path),
            "result": asdict(result),
        }
        summary_path = root / "benchmark" / "results" / f"summary-drain-{stamp}.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print_drain_result(result)
        print(f"Summary written to {summary_path}")
        return

    oha = require_oha()
    body = ensure_ingest_body(root)

    raw_dir = root / "benchmark" / "results" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    results: list[Result] = []
    is_full = args.mode == "full"

    if args.mode == "closed":
        plan: list[tuple[str, int | None, int]] = [
            ("closed", None, c) for c in args.closed_connections
        ]
    elif args.mode == "open":
        plan = [("open", q, args.open_connections) for q in args.open_qps]
    elif args.mode == "both":
        plan = [("closed", None, c) for c in args.closed_connections]
        plan.extend(("open", q, args.open_connections) for q in args.open_qps)
    else:
        plan = [("full", q, args.open_connections) for q in args.open_qps]

    for i, (mode, offered, connections) in enumerate(plan):
        if mode == "closed":
            print(f"closed: -c {connections}, -z {args.duration}s")
            results.append(
                ingest_result(
                    oha=oha,
                    body=body,
                    duration=args.duration,
                    mode="closed",
                    connections=connections,
                    offered=None,
                    max_p99_ms=args.max_p99_ms,
                    raw_out=raw_dir / f"oha-capacity-closed-c{connections}-{stamp}.json",
                )
            )
        elif mode == "open":
            assert offered is not None
            print(f"open: -q {offered}, -c {connections}, -z {args.duration}s")
            results.append(
                ingest_result(
                    oha=oha,
                    body=body,
                    duration=args.duration,
                    mode="open",
                    connections=connections,
                    offered=offered,
                    max_p99_ms=args.max_p99_ms,
                    raw_out=raw_dir / f"oha-capacity-open-q{offered}-{stamp}.json",
                )
            )
        else:
            assert offered is not None
            print(f"full: -q {offered}, -c {connections}, -z {args.duration}s")
            results.append(
                run_full_point(
                    oha=oha,
                    body=body,
                    db_path=db_path,
                    results_db_path=results_db_path,
                    duration=args.duration,
                    connections=connections,
                    offered=offered,
                    max_p99_ms=args.max_p99_ms,
                    drain_timeout_s=args.drain_timeout_seconds,
                    drain_poll_s=args.drain_poll_seconds,
                    raw_out=raw_dir / f"oha-capacity-full-q{offered}-{stamp}.json",
                )
            )
        settle(args.settle_seconds, remaining_points=len(plan) - i - 1)

    kind = "capacity-full" if is_full else "capacity"
    summary = {
        "kind": kind,
        "driver": "oha",
        "timestamp": stamp,
        "mode": args.mode,
        "duration_seconds": args.duration,
        "max_p99_ms": args.max_p99_ms,
        "settle_seconds": args.settle_seconds,
        "drain_timeout_seconds": args.drain_timeout_seconds if is_full else None,
        "drain_poll_seconds": args.drain_poll_seconds if is_full else None,
        "db": str(db_path) if is_full else None,
        "results_db": str(results_db_path) if is_full else None,
        "results": [asdict(row) for row in results],
    }
    summary_name = f"summary-{kind}-{stamp}.json"
    summary_path = root / "benchmark" / "results" / summary_name
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print_results(results, args.max_p99_ms, full=is_full)
    print(f"Summary written to {summary_path}")


def benchmark_data_dir(root: Path) -> Path:
    return root / "benchmark" / "data"


def clean_benchmark_data(root: Path, *, timeout_s: float = 10.0) -> None:
    """Remove generated benchmark databases, tolerating Windows handle release lag."""
    data_dir = benchmark_data_dir(root)
    deadline = time.monotonic() + timeout_s
    while data_dir.exists():
        try:
            shutil.rmtree(data_dir)
            return
        except PermissionError as error:
            if time.monotonic() >= deadline:
                raise SystemExit(
                    f"could not remove {data_dir} after {timeout_s:g}s because it is still in use: {error}"
                ) from error
            time.sleep(0.25)


def server_binary(root: Path) -> Path:
    name = "maqistor.exe" if os.name == "nt" else "maqistor"
    return root / "target" / "release" / name


def configured_managed_worker_count(root: Path) -> int:
    """Return the number of Docker worker replicas requested by the benchmark config."""
    config_path = root / "benchmark" / "maqistor.toml"
    with config_path.open("rb") as config_file:
        config = tomllib.load(config_file)
    queues = config.get("queues", [])
    if not isinstance(queues, list):
        raise SystemExit(f"invalid benchmark queue configuration in {config_path}")
    return sum(
        int(queue.get("managed_config", {}).get("replicas", 1))
        for queue in queues
        if isinstance(queue, dict) and isinstance(queue.get("managed_config"), dict)
    )


def registered_worker_count(log_path: Path) -> int:
    """Count distinct worker instances which registered during this server run."""
    try:
        log = log_path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return 0
    instance_ids = re.findall(
        r"worker registered.*?instance_id.*?([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})",
        log,
    )
    return len(set(instance_ids))


def wait_for_workers(
    process: subprocess.Popen[bytes],
    log_path: Path,
    *,
    expected: int,
    timeout_s: float,
) -> None:
    """Wait until every configured managed worker has connected before timing."""
    if expected == 0:
        return
    print(f"waiting for {expected} managed worker(s) to connect")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        connected = registered_worker_count(log_path)
        if connected >= expected:
            print(f"managed workers ready: {connected}/{expected}")
            return
        exit_code = process.poll()
        if exit_code is not None:
            raise SystemExit(
                f"maqistor exited while waiting for workers (code {exit_code}); see {log_path}"
            )
        time.sleep(0.25)
    connected = registered_worker_count(log_path)
    raise SystemExit(
        f"only {connected}/{expected} configured managed workers connected within {timeout_s:g}s; "
        f"see {log_path}"
    )


def health_check() -> bool:
    try:
        with urllib.request.urlopen(f"{BASE_URL}/health", timeout=0.5) as response:
            return 200 <= response.status < 300
    except urllib.error.URLError:
        return False


def start_server(
    root: Path, *, stamp: str, timeout_s: float
) -> tuple[subprocess.Popen[bytes], object, Path]:
    binary = server_binary(root)
    if not binary.is_file():
        raise SystemExit(
            f"release server not found: {binary}\n\nBuild it first:\n  cargo build -p maqistor --release"
        )

    results_dir = root / "benchmark" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    log_path = results_dir / f"maqistor-{stamp}.log"
    log_file = log_path.open("wb")
    process = subprocess.Popen(
        [str(binary), "--config", "benchmark/maqistor.toml"],
        cwd=root,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if health_check():
            print(f"started maqistor (pid {process.pid}); log: {log_path}")
            return process, log_file, log_path
        exit_code = process.poll()
        if exit_code is not None:
            log_file.close()
            raise SystemExit(
                f"maqistor exited during startup with code {exit_code}; see {log_path}"
            )
        time.sleep(0.1)

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    log_file.close()
    raise SystemExit(f"maqistor did not become healthy within {timeout_s:g}s; see {log_path}")


def stop_server(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def main() -> None:
    args = parse_args()
    root = workspace_root()
    expected_db_path = default_db_path(root)
    db_path = args.db.resolve() if args.db is not None else expected_db_path
    if db_path != expected_db_path.resolve():
        raise SystemExit(
            "--db must be benchmark/data/maqistor-ingest.db when the runner owns the server"
        )
    results_db_path = default_results_path(db_path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    # Do not delete databases which a manually-started server could still own.
    if health_check():
        raise SystemExit(
            f"maqistor is already reachable at {BASE_URL}; stop it before running the standalone benchmark"
        )
    clean_benchmark_data(root)
    process: subprocess.Popen[bytes] | None = None
    log_file: object | None = None
    try:
        process, log_file, log_path = start_server(
            root,
            stamp=stamp,
            timeout_s=args.server_startup_timeout_seconds,
        )
        wait_for_workers(
            process,
            log_path,
            expected=configured_managed_worker_count(root),
            timeout_s=args.worker_startup_timeout_seconds,
        )
        run_benchmark(
            args,
            root=root,
            stamp=stamp,
            db_path=db_path,
            results_db_path=results_db_path,
        )
    finally:
        if process is not None:
            print("stopping benchmark server")
            stop_server(process)
        if log_file is not None and not log_file.closed:
            log_file.close()
        clean_benchmark_data(root)
        print(f"removed benchmark data directory: {benchmark_data_dir(root)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
