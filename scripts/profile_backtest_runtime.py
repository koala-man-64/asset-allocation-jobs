from __future__ import annotations

import argparse
import cProfile
import io
import os
import pstats
from pathlib import Path

from core.backtest_runtime import execute_backtest_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile a single backtest worker execution against a specific run id."
    )
    parser.add_argument("--run-id", required=True, help="Backtest run id to execute.")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("POSTGRES_DSN", ""),
        help="Postgres DSN. Defaults to POSTGRES_DSN.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="How many profiler rows to print.",
    )
    parser.add_argument(
        "--sort",
        default="cumtime",
        choices=("calls", "cumtime", "filename", "time"),
        help="pstats sort order.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional path for the raw .prof output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dsn = str(args.dsn or "").strip()
    if not dsn:
        raise SystemExit("--dsn or POSTGRES_DSN is required.")

    profiler = cProfile.Profile()
    result = profiler.runcall(execute_backtest_run, dsn, run_id=args.run_id, execution_name="profile-backtest-runtime")

    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats(args.sort)
    stats.print_stats(max(1, int(args.limit)))
    print(stream.getvalue())
    print(
        f"summary final_equity={result.get('summary', {}).get('final_equity')} "
        f"trades={result.get('summary', {}).get('trades')}"
    )
    print(
        f"BACKTEST_RANKING_MAX_WORKERS={str(os.environ.get('BACKTEST_RANKING_MAX_WORKERS') or '1').strip() or '1'} "
        "(multiprocessing gate remains disabled until parity + benchmark thresholds are proven)"
    )

    output_path = str(args.output or "").strip()
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        profiler.dump_stats(str(path))
        print(f"wrote profiler output to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
