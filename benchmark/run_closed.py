#!/usr/bin/env python3
"""Concurrent (connection-saturated) benchmarks via oha.

Answers: where does throughput drop under fixed concurrency across stages?

Does NOT start the server — start it yourself first.

Prerequisite:
  cargo install oha

Usage (from workspace root):
  python benchmark/run_closed.py
  python benchmark/run_closed.py --load high --sustain medium
  python benchmark/run_closed.py --stages health,ingest
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

# Local imports when run as `python benchmark/run_closed.py`
sys.path.insert(0, str(Path(__file__).resolve().parent))

from oha_util import (
    BASE_URL,
    ensure_ingest_body,
    error_count,
    expected_success_code,
    latency_ms,
    require_oha,
    require_standing_server,
    rps,
    run_oha,
    status_counts,
    workspace_root,
)

# Concurrent connections (-c)
LOAD_CONNECTIONS = {
    "low": 32,
    "medium": 100,
    "high": 200,
}

SUSTAIN_SECONDS = {
    "low": 30,
    "medium": 90,
    "high": 150,
}

SKIP_REASONS = {
    "full": "sleep-job complete path not implemented yet (post-run timestamp E2E)",
}

STAGE_ORDER = ("health", "ingest", "full")

DESCRIPTIONS = {
    "health": "GET /health — oha concurrent connections",
    "ingest": "POST /jobs — oha concurrent connections (jobs/s = req/s)",
}


@dataclass
class StageResult:
    stage: str
    description: str
    status: str
    skip_reason: str | None
    ops_per_s: float | None
    p50_ms: float | None
    p99_ms: float | None
    errors: int | None
    status_codes: dict[str, int] | None
    delta_ops: float | None


def fmt(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def parse_stages(raw: str) -> list[str]:
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    if not parts:
        raise SystemExit("--stages must list at least one stage")
    unknown = [p for p in parts if p not in STAGE_ORDER]
    if unknown:
        raise SystemExit(
            f"unknown stages {unknown}; choose from: {', '.join(STAGE_ORDER)}"
        )
    return [s for s in STAGE_ORDER if s in parts]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="oha concurrent-load benchmark against standing maqistor.",
    )
    parser.add_argument(
        "--load",
        choices=sorted(LOAD_CONNECTIONS),
        default="low",
        help="Concurrent connections (-c)",
    )
    parser.add_argument(
        "--sustain",
        choices=sorted(SUSTAIN_SECONDS),
        default="low",
        help="Duration (-z)",
    )
    parser.add_argument(
        "--stages",
        default="health,ingest,full",
        help="Comma-separated stages (default: health,ingest,full)",
    )
    return parser.parse_args()


def print_summary(
    stages: list[StageResult],
    *,
    load: str,
    sustain: str,
    connections: int,
    duration_s: int,
) -> None:
    print()
    print(
        f"oha concurrent: load={load} (-c {connections})  "
        f"sustain={sustain} (-z {duration_s}s)"
    )
    print()
    header = (
        f"{'stage':<10} {'ops/s':>10} {'p50_ms':>8} {'p99_ms':>8} "
        f"{'errors':>8} {'delta_ops':>10}"
    )
    print(header)
    print("-" * len(header))
    for row in stages:
        if row.status == "skipped":
            print(
                f"{row.stage:<10} {'skipped':>10} {'—':>8} {'—':>8} "
                f"{'—':>8} {'—':>10}"
            )
            continue
        delta = "—" if row.delta_ops is None else fmt(row.delta_ops)
        errs = "n/a" if row.errors is None else str(row.errors)
        print(
            f"{row.stage:<10} {fmt(row.ops_per_s):>10} {fmt(row.p50_ms):>8} "
            f"{fmt(row.p99_ms):>8} {errs:>8} {delta:>10}"
        )
    print()
    for row in stages:
        if row.status == "skipped":
            print(f"  {row.stage}: skipped — {row.skip_reason}")
        else:
            codes = row.status_codes or {}
            print(f"  {row.stage}: {row.description}  codes={codes}")
    print()
    print(
        "ops/s is oha requests/sec (for ingest, that is jobs/s). "
        "delta_ops = vs previous measured stage."
    )


def main() -> None:
    args = parse_args()
    connections = LOAD_CONNECTIONS[args.load]
    duration_s = SUSTAIN_SECONDS[args.sustain]
    stages = parse_stages(args.stages)

    root = workspace_root()
    oha = require_oha()
    require_standing_server("run_closed.py")
    body = ensure_ingest_body(root)

    results_dir = root / "benchmark" / "results"
    raw_dir = results_dir / "raw"
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (root / "benchmark" / "data").mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    summary_path = results_dir / f"summary-closed-{stamp}.json"

    results: list[StageResult] = []
    prev_ops: float | None = None

    print(
        f"Running oha stages {stages} "
        f"(-c {connections}, -z {duration_s}s)..."
    )

    for stage in stages:
        if stage in SKIP_REASONS:
            results.append(
                StageResult(
                    stage=stage,
                    description="",
                    status="skipped",
                    skip_reason=SKIP_REASONS[stage],
                    ops_per_s=None,
                    p50_ms=None,
                    p99_ms=None,
                    errors=None,
                    status_codes=None,
                    delta_ops=None,
                )
            )
            continue

        raw = raw_dir / f"oha-closed-{stage}-{stamp}.json"
        if stage == "health":
            report = run_oha(
                oha,
                url=f"{BASE_URL}/health",
                connections=connections,
                duration_s=duration_s,
                method="GET",
                raw_out=raw,
            )
        else:
            report = run_oha(
                oha,
                url=f"{BASE_URL}/jobs",
                connections=connections,
                duration_s=duration_s,
                method="POST",
                body_path=body,
                raw_out=raw,
            )

        codes = status_counts(report)
        want = expected_success_code(stage)
        if codes and want not in codes:
            raise SystemExit(
                f"{stage}: expected HTTP {want}, got status codes {codes}. "
                "Fix the request (body/queue) before trusting throughput."
            )

        ops = rps(report)
        delta = None if prev_ops is None or ops is None else ops - prev_ops
        results.append(
            StageResult(
                stage=stage,
                description=DESCRIPTIONS[stage],
                status="ok",
                skip_reason=None,
                ops_per_s=ops,
                p50_ms=latency_ms(report, "p50"),
                p99_ms=latency_ms(report, "p99"),
                errors=error_count(report),
                status_codes=codes,
                delta_ops=delta,
            )
        )
        if ops is not None:
            prev_ops = ops
        print(
            f"  finished {stage}: {fmt(ops)} ops/s  "
            f"p99={fmt(latency_ms(report, 'p99'))} ms"
        )

    summary = {
        "kind": "closed",
        "driver": "oha",
        "timestamp": stamp,
        "base_url": BASE_URL,
        "load": args.load,
        "sustain": args.sustain,
        "connections": connections,
        "duration_seconds": duration_s,
        "stages": [asdict(row) for row in results],
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print_summary(
        results,
        load=args.load,
        sustain=args.sustain,
        connections=connections,
        duration_s=duration_s,
    )
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
