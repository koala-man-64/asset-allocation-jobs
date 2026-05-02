from __future__ import annotations

import argparse
import os
import subprocess
import tempfile

import yaml


JOB_CONFIG = {
    "bronze_market": ("BRONZE_MARKET_JOB", "bronze-market-job"),
    "bronze_finance": ("BRONZE_FINANCE_JOB", "bronze-finance-job"),
    "bronze_price_target": ("BRONZE_PRICE_TARGET_JOB", "bronze-price-target-job"),
    "bronze_earnings": ("BRONZE_EARNINGS_JOB", "bronze-earnings-job"),
    "bronze-quiver": ("BRONZE_QUIVER_JOB", "bronze-quiver-job"),
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
    "symbol-cleanup": ("SYMBOL_CLEANUP_JOB", "symbol-cleanup-job"),
    "results-reconcile": ("RESULTS_RECONCILE_JOB", "results-reconcile-job"),
    "backtests": ("BACKTEST_JOB", "backtests-job"),
    "backtests-reconcile": ("BACKTEST_RECONCILE_JOB", "backtests-reconcile-job"),
}

QUIVER_JOB_MODES = frozenset({"incremental", "historical_backfill"})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manually start an Azure Container Apps job.")
    parser.add_argument("--job", required=True, choices=sorted(JOB_CONFIG), help="Logical job key to start.")
    parser.add_argument(
        "--resource-group",
        default=os.environ.get("RESOURCE_GROUP", ""),
        help="Azure resource group containing the job. Defaults to RESOURCE_GROUP.",
    )
    parser.add_argument(
        "--mode",
        default="incremental",
        choices=sorted(QUIVER_JOB_MODES),
        help="Quiver Bronze run mode. Only historical_backfill changes the one-off execution template.",
    )
    return parser.parse_args()


def resolve_job_name(job_key: str, environment: dict[str, str]) -> str:
    env_name, default_name = JOB_CONFIG[job_key]
    return environment.get(env_name, default_name)


def _load_job_template(*, job_name: str, resource_group: str) -> dict:
    raw = subprocess.check_output(
        [
            "az",
            "containerapp",
            "job",
            "show",
            "--name",
            job_name,
            "--resource-group",
            resource_group,
            "--query",
            "properties.template",
            "--output",
            "yaml",
        ],
        text=True,
    )
    template = yaml.safe_load(raw)
    if not isinstance(template, dict):
        raise SystemExit(f"Azure returned an invalid template for {job_name}.")
    return template


def _override_quiver_job_mode(template: dict, *, mode: str) -> dict:
    containers = template.get("containers") if isinstance(template.get("containers"), list) else []
    for container in containers:
        if not isinstance(container, dict):
            continue
        env_items = container.get("env") if isinstance(container.get("env"), list) else []
        for env_item in env_items:
            if not isinstance(env_item, dict):
                continue
            if env_item.get("name") == "QUIVER_DATA_JOB_MODE":
                env_item["value"] = mode
                env_item.pop("secretRef", None)
                return template
    raise SystemExit("QUIVER_DATA_JOB_MODE was not found in the live job template.")


def _start_job(*, job_name: str, resource_group: str, yaml_path: str | None = None) -> None:
    command = [
        "az",
        "containerapp",
        "job",
        "start",
        "--name",
        job_name,
        "--resource-group",
        resource_group,
    ]
    if yaml_path:
        command.extend(["--yaml", yaml_path])
    subprocess.run(command, check=True)


def _start_quiver_backfill(*, job_name: str, resource_group: str) -> None:
    template = _override_quiver_job_mode(
        _load_job_template(job_name=job_name, resource_group=resource_group),
        mode="historical_backfill",
    )
    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False) as handle:
            temp_path = handle.name
            yaml.safe_dump(template, handle, sort_keys=False)
        _start_job(job_name=job_name, resource_group=resource_group, yaml_path=temp_path)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass


def start_job(
    *,
    job_key: str,
    resource_group: str,
    environment: dict[str, str] | None = None,
    mode: str = "incremental",
) -> str:
    if not resource_group.strip():
        raise SystemExit("RESOURCE_GROUP is required either via --resource-group or environment.")

    env = environment or os.environ
    if mode not in QUIVER_JOB_MODES:
        raise SystemExit("Unsupported --mode. Use incremental or historical_backfill.")
    if job_key != "bronze-quiver" and mode != "incremental":
        raise SystemExit("--mode is only supported with --job bronze-quiver.")

    job_name = resolve_job_name(job_key, env)
    if job_key == "bronze-quiver" and mode == "historical_backfill":
        _start_quiver_backfill(job_name=job_name, resource_group=resource_group)
    else:
        _start_job(job_name=job_name, resource_group=resource_group)

    print(f"Triggered job: {job_name}")
    return job_name


def main() -> None:
    args = parse_args()
    start_job(job_key=args.job, resource_group=args.resource_group, environment=os.environ, mode=args.mode)


if __name__ == "__main__":
    main()
