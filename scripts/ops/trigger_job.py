from __future__ import annotations

import argparse
import os
import subprocess


JOB_CONFIG = {
    "bronze_market": ("BRONZE_MARKET_JOB", "bronze-market-job"),
    "bronze_finance": ("BRONZE_FINANCE_JOB", "bronze-finance-job"),
    "bronze_price_target": ("BRONZE_PRICE_TARGET_JOB", "bronze-price-target-job"),
    "bronze_earnings": ("BRONZE_EARNINGS_JOB", "bronze-earnings-job"),
    "silver_market": ("SILVER_MARKET_JOB", "silver-market-job"),
    "silver_finance": ("SILVER_FINANCE_JOB", "silver-finance-job"),
    "silver_price_target": ("SILVER_PRICE_TARGET_JOB", "silver-price-target-job"),
    "silver_earnings": ("SILVER_EARNINGS_JOB", "silver-earnings-job"),
    "gold_market": ("GOLD_MARKET_JOB", "gold-market-job"),
    "gold_finance": ("GOLD_FINANCE_JOB", "gold-finance-job"),
    "gold_price_target": ("GOLD_PRICE_TARGET_JOB", "gold-price-target-job"),
    "gold_earnings": ("GOLD_EARNINGS_JOB", "gold-earnings-job"),
    "gold_regime": ("GOLD_REGIME_JOB", "gold-regime-job"),
    "platinum_rankings": ("PLATINUM_RANKINGS_JOB", "platinum-rankings-job"),
    "backtests": ("BACKTEST_JOB", "backtests-job"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manually start an Azure Container Apps job.")
    parser.add_argument("--job", required=True, choices=sorted(JOB_CONFIG), help="Logical job key to start.")
    parser.add_argument(
        "--resource-group",
        default=os.environ.get("RESOURCE_GROUP", ""),
        help="Azure resource group containing the job. Defaults to RESOURCE_GROUP.",
    )
    return parser.parse_args()


def resolve_job_name(job_key: str, environment: dict[str, str]) -> str:
    env_name, default_name = JOB_CONFIG[job_key]
    return environment.get(env_name, default_name)


def start_job(*, job_key: str, resource_group: str, environment: dict[str, str] | None = None) -> str:
    if not resource_group.strip():
        raise SystemExit("RESOURCE_GROUP is required either via --resource-group or environment.")

    env = environment or os.environ
    job_name = resolve_job_name(job_key, env)
    subprocess.run(
        [
            "az",
            "containerapp",
            "job",
            "start",
            "--name",
            job_name,
            "--resource-group",
            resource_group,
        ],
        check=True,
    )
    print(f"Triggered job: {job_name}")
    return job_name


def main() -> None:
    args = parse_args()
    start_job(job_key=args.job, resource_group=args.resource_group, environment=os.environ)


if __name__ == "__main__":
    main()
