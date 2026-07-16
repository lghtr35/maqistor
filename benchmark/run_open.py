#!/usr/bin/env python3
"""Rate-limited (open/offer) benchmarks via oha.

Answers: can we absorb a target arrival rate without falling behind?

Does NOT start the server — start it yourself first.

Prerequisite:
  cargo install oha

Usage (from workspace root):
  python benchmark/run_open.py
  python benchmark/run_open.py --load high --sustain medium
  python benchmark/run_open.py --stages health,ingest
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
    expected_success_code,
    latency_ms,
    require_oha,
    require_standing_server,
    rps,
    run_oha,
    status_counts,
    workspace_root,
)

# Offered queries/sec (-q); connections kept high enough to sustain the offer.
LOAD_QPS = {
    "low": 10_000,
    "medium": 50_000,
    "high": 100_000,
}

LOAD_CONNECTIONS = {
    "low": 100,
    "medium": 200,
    "high": 400,
}

SUSTAIN_SECONDS = {
    "low": 10,
    "medium": 15,
    "high": 30,
}

SKIP_REASONS = {
    "full": "sleep-job complete path not implemented yet",
}

STAGE_ORDER = ("health", "ingest", "full")

DESCRIPTIONS = {
    "health": "GET /health — oha rate-limited offer",
    "ingest": "POST /jobs — oha rate-limited offer (jobs/s = req/s)",
}


@dataclass
class StageResult:
    stage: str
    description: str
    status: str
    skip_reason: str | None
    offered_rps: float | None
    achieved_rps: float | None
    achieved_over_offered: float | None
    p50_ms: float | None
    p99_ms: float | None
    errors: int | None
    status_codes: dict[str, int] | None


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
        description="oha rate-limited benchmark against standing maqistor.",
    )
    parser.add_argument(
        "--load",
        choices=sorted(LOAD_QPS),
        default="low",
        help="Offered QPS (-q)",
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
    offered: int,
    duration_s: int,
) -> None:
    print()
    print(
        f"oha offer: load={load} (-q {offered})  "
        f"sustain={sustain} (-z {duration_s}s)"
    )
    print()
    header = (
        f"{'stage':<10} {'offered':>8} {'achieved':>10} {'ach/off':>8} "
        f"{'p50_ms':>8} {'p99_ms':>8} {'errors':>8}"
    )
    print(header)
    print("-" * len(header))
    for row in stages:
        if row.status == "skipped":
            print(
                f"{row.stage:<10} {'—':>8} {'skipped':>10} {'—':>8} "
                f"{'—':>8} {'—':>8} {'—':>8}"
            )
            continue
        ratio = (
            f"{row.achieved_over_offered:.0%}"
            if row.achieved_over_offered is not None
            else "n/a"
        )
        errs = "n/a" if row.errors is None else str(row.errors)
        print(
            f"{row.stage:<10} {fmt(row.offered_rps, 0):>8} {fmt(row.achieved_rps):>10} "
            f"{ratio:>8} {fmt(row.p50_ms):>8} {fmt(row.p99_ms):>8} {errs:>8}"
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
        "ach/off near 100% with low p99 means the offer was absorbed; "
        "it does not prove the ceiling (use run_closed.py for that)."
    )


def main() -> None:
    args = parse_args()
    offered = LOAD_QPS[args.load]
    connections = LOAD_CONNECTIONS[args.load]
    duration_s = SUSTAIN_SECONDS[args.sustain]
    stages = parse_stages(args.stages)

    root = workspace_root()
    oha = require_oha()
    require_standing_server("run_open.py")
    body = ensure_ingest_body(root)

    results_dir = root / "benchmark" / "results"
    raw_dir = results_dir / "raw"
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (root / "benchmark" / "data").mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    summary_path = results_dir / f"summary-open-{stamp}.json"

    results: list[StageResult] = []

    print(
        f"Running oha stages {stages} "
        f"(-q {offered}, -c {connections}, -z {duration_s}s)..."
    )

    for stage in stages:
        if stage in SKIP_REASONS:
            results.append(
                StageResult(
                    stage=stage,
                    description="",
                    status="skipped",
                    skip_reason=SKIP_REASONS[stage],
                    offered_rps=None,
                    achieved_rps=None,
                    achieved_over_offered=None,
                    p50_ms=None,
                    p99_ms=None,
                    errors=None,
                    status_codes=None,
                )
            )
            continue

        raw = raw_dir / f"oha-open-{stage}-{stamp}.json"
        if stage == "health":
            report = run_oha(
                oha,
                url=f"{BASE_URL}/health",
                connections=connections,
                duration_s=duration_s,
                qps=float(offered),
                method="GET",
                raw_out=raw,
            )
        else:
            report = run_oha(
                oha,
                url=f"{BASE_URL}/jobs",
                connections=connections,
                duration_s=duration_s,
                qps=float(offered),
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

        achieved = rps(report)
        ratio = None
        if achieved is not None and offered > 0:
            ratio = achieved / float(offered)

        results.append(
            StageResult(
                stage=stage,
                description=DESCRIPTIONS[stage],
                status="ok",
                skip_reason=None,
                offered_rps=float(offered),
                achieved_rps=achieved,
                achieved_over_offered=ratio,
                p50_ms=latency_ms(report, "p50"),
                p99_ms=latency_ms(report, "p99"),
                errors=error_count(report),
                status_codes=codes,
            )
        )
        print(
            f"  finished {stage}: {fmt(achieved)} ops/s  "
            f"p99={fmt(latency_ms(report, 'p99'))} ms"
        )

    summary = {
        "kind": "open",
        "driver": "oha",
        "timestamp": stamp,
        "base_url": BASE_URL,
        "load": args.load,
        "sustain": args.sustain,
        "offered_rps": offered,
        "connections": connections,
        "duration_seconds": duration_s,
        "stages": [asdict(row) for row in results],
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print_summary(
        results,
        load=args.load,
        sustain=args.sustain,
        offered=offered,
        duration_s=duration_s,
    )
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
