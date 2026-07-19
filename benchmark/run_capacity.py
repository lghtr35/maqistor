#!/usr/bin/env python3
"""Find Maqistor ingest capacity with closed- and open-loop oha sweeps.

The server must already be running. This script only measures POST /jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from oha_util import (
    BASE_URL,
    ensure_ingest_body,
    error_count,
    latency_ms,
    require_oha,
    require_standing_server,
    rps,
    run_oha,
    status_counts,
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


def positive_csv(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as err:
        raise argparse.ArgumentTypeError("values must be comma-separated integers") from err
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("values must all be greater than zero")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep durable POST /jobs capacity with oha.")
    parser.add_argument("--mode", choices=("closed", "open", "both"), default="both")
    parser.add_argument("--duration", type=int, default=30, help="Seconds per point (default: 30)")
    parser.add_argument(
        "--closed-connections", type=positive_csv, default=DEFAULT_CLOSED_CONNECTIONS,
        help="Closed-loop -c values, comma-separated",
    )
    parser.add_argument(
        "--open-qps", type=positive_csv, default=DEFAULT_OPEN_QPS,
        help="Open-loop -q values, comma-separated",
    )
    parser.add_argument(
        "--open-connections", type=int, default=1000,
        help="Concurrent connections for every open-loop point (default: 1000)",
    )
    parser.add_argument(
        "--max-p99-ms", type=float, default=100.0,
        help="Largest acceptable p99 when judging open-loop stability (default: 100)",
    )
    args = parser.parse_args()
    if args.duration <= 0 or args.open_connections <= 0 or args.max_p99_ms <= 0:
        parser.error("duration, open-connections, and max-p99-ms must be greater than zero")
    return args


def fmt(value: float | None, digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:,.{digits}f}"


def run_point(
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


def print_results(results: list[Result], max_p99_ms: float) -> None:
    print()
    header = (
        f"{'mode':<7} {'-c':>6} {'offered':>9} {'achieved':>10} {'ach/off':>8} "
        f"{'p50_ms':>8} {'p99_ms':>8} {'errors':>7} {'stable':>7}"
    )
    print(header)
    print("-" * len(header))
    for row in results:
        ratio = "-" if row.achieved_over_offered is None else f"{row.achieved_over_offered:.0%}"
        offered = "-" if row.offered_rps is None else f"{row.offered_rps:,}"
        print(
            f"{row.mode:<7} {row.connections:>6,} {offered:>9} {fmt(row.achieved_rps):>10} "
            f"{ratio:>8} {fmt(row.p50_ms):>8} {fmt(row.p99_ms):>8} {row.errors:>7} "
            f"{'yes' if row.accepted else 'no':>7}"
        )
    closed = [row for row in results if row.mode == "closed"]
    closed_stable = [row for row in closed if row.accepted]
    open_rows = [row for row in results if row.mode == "open" and row.accepted]
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
        print(
            f"Highest stable open-loop offer: {best.offered_rps:,} jobs/s "
            f"(>=98% achieved, zero errors, p99 <= {max_p99_ms:g} ms)."
        )


def main() -> None:
    args = parse_args()
    root = workspace_root()
    oha = require_oha()
    require_standing_server("run_capacity.py")
    body = ensure_ingest_body(root)
    raw_dir = root / "benchmark" / "results" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    results: list[Result] = []

    if args.mode in ("closed", "both"):
        for connections in args.closed_connections:
            print(f"closed: -c {connections}, -z {args.duration}s")
            results.append(run_point(
                oha=oha, body=body, duration=args.duration, mode="closed",
                connections=connections, offered=None, max_p99_ms=args.max_p99_ms,
                raw_out=raw_dir / f"oha-capacity-closed-c{connections}-{stamp}.json",
            ))
    if args.mode in ("open", "both"):
        for offered in args.open_qps:
            print(f"open: -q {offered}, -c {args.open_connections}, -z {args.duration}s")
            results.append(run_point(
                oha=oha, body=body, duration=args.duration, mode="open",
                connections=args.open_connections, offered=offered, max_p99_ms=args.max_p99_ms,
                raw_out=raw_dir / f"oha-capacity-open-q{offered}-{stamp}.json",
            ))

    summary = {
        "kind": "capacity", "driver": "oha", "timestamp": stamp,
        "duration_seconds": args.duration, "max_p99_ms": args.max_p99_ms,
        "results": [asdict(row) for row in results],
    }
    summary_path = root / "benchmark" / "results" / f"summary-capacity-{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print_results(results, args.max_p99_ms)
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
