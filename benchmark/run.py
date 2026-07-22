#!/usr/bin/env python3
"""Single Maqistor benchmark runner: closed/open capacity sweeps and full-cycle.

The server must already be running. This script measures durable POST /jobs
(and, in --mode full, post-step drain + create→complete cycle from SQLite).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from oha_util import (
    BASE_URL,
    BENCH_QUEUE,
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
    require_standing_server,
    rps,
    run_oha,
    status_counts,
    wait_drain,
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
    accepted: bool
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
        description="Sweep durable POST /jobs capacity with oha (optional full-cycle).",
    )
    parser.add_argument(
        "--mode",
        choices=("closed", "open", "both", "full"),
        default="both",
        help="closed/open/both = ingest capacity; full = open QPS + drain/cycle",
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
        help="Full mode: max seconds to wait for queue drain (default: 120)",
    )
    parser.add_argument(
        "--drain-poll-seconds",
        type=float,
        default=0.5,
        help="Full mode: drain poll interval (default: 0.5)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Ingest SQLite path for full mode (default: benchmark/data/maqistor-ingest.db)",
    )
    args = parser.parse_args()
    if args.duration <= 0 or args.open_connections <= 0 or args.max_p99_ms <= 0:
        parser.error("duration, open-connections, and max-p99-ms must be greater than zero")
    if args.settle_seconds < 0:
        parser.error("settle-seconds must be >= 0")
    if args.drain_timeout_seconds <= 0 or args.drain_poll_seconds <= 0:
        parser.error("drain-timeout-seconds and drain-poll-seconds must be greater than zero")
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
    accepted = errors == 0 and (p99 is None or p99 <= max_p99_ms)
    if offered is not None:
        accepted = accepted and ratio is not None and ratio >= 0.98
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
        accepted=accepted,
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
    if not drained:
        result.accepted = False
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
            f"{'drain_s':>8} {'cyc_p50ms':>9} {'cyc_p99ms':>9} {'stable':>7}"
        )
    else:
        header = (
            f"{'mode':<7} {'-c':>6} {'offered':>9} {'achieved':>10} {'ach/off':>8} "
            f"{'p50_ms':>8} {'p99_ms':>8} {'errors':>7} {'stable':>7}"
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
                f"{fmt(row.cycle_p99_ms):>9} {'yes' if row.accepted else 'no':>7}"
            )
        else:
            print(
                f"{row.mode:<7} {row.connections:>6,} {offered:>9} {fmt(row.achieved_rps):>10} "
                f"{ratio:>8} {fmt(row.p50_ms):>8} {fmt(row.p99_ms):>8} {row.errors:>7} "
                f"{'yes' if row.accepted else 'no':>7}"
            )

    closed = [row for row in results if row.mode == "closed"]
    closed_stable = [row for row in closed if row.accepted]
    open_rows = [row for row in results if row.mode in ("open", "full") and row.accepted]
    if closed:
        best = max(closed, key=lambda row: row.achieved_rps or 0.0)
        print(f"\nClosed-loop peak observed: {fmt(best.achieved_rps)} jobs/s at -c {best.connections}.")
    if closed_stable:
        best = max(closed_stable, key=lambda row: row.achieved_rps or 0.0)
        print(
            f"Highest closed-loop result within the {max_p99_ms:g} ms p99 guardrail: "
            f"{fmt(best.achieved_rps)} jobs/s at -c {best.connections}."
        )
    if open_rows:
        best = max(open_rows, key=lambda row: row.offered_rps or 0)
        label = "full-cycle" if full else "open-loop"
        print(
            f"Highest stable {label} offer: {best.offered_rps:,} jobs/s "
            f"(>=98% achieved, zero errors, p99 <= {max_p99_ms:g} ms"
            + (", drain ok" if full else "")
            + ")."
        )


def main() -> None:
    args = parse_args()
    root = workspace_root()
    oha = require_oha()
    require_standing_server("run.py")
    body = ensure_ingest_body(root)
    db_path = args.db if args.db is not None else default_db_path(root)
    results_db_path = default_results_path(db_path)
    if args.mode == "full":
        # Fail fast if DBs are missing before spending time on oha.
        open_db(db_path).close()
        open_db(results_db_path).close()

    raw_dir = root / "benchmark" / "results" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
