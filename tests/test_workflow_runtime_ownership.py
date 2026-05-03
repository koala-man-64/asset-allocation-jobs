from __future__ import annotations

from pathlib import Path

import yaml
from asset_allocation_runtime_common.job_metadata import expected_job_metadata, validate_job_metadata_tags


STALE_RUNTIME_PATHS = (
    "api",
    "ui",
    "tests/api",
    "tests/tools/test_ui_lockfile_integrity.py",
    "tests/architecture/test_system_facade_guard.py",
    "docker-compose.yml",
    "docker-compose.debug.yml",
    "deploy/app_api.yaml",
    "deploy/app_api_public.yaml",
    "scripts/run_api_dev.py",
    "scripts/check_ui_api_drift_guards.py",
    "scripts/validate_deploy_inputs.py",
    "scripts/verify_ui_api_health.py",
    "audit_snapshot.json",
)

STALE_DRIFT_REFERENCES = (
    "lint-ui",
    "typecheck-ui",
    "test-ui",
    'path_glob: "api/**"',
    "api/service/app.py",
)

STALE_DOC_REFERENCES = (
    ".github/workflows/run_tests.yml",
    ".github/workflows/dependency_governance.yml",
    "api/service/app.py",
    "ui/package.json",
    "deploy/app_api.yaml",
    "/config.js",
)

API_BOOTSTRAP_JOB_MANIFESTS = (
    "job_backtests.yaml",
    "job_backtests_reconcile.yaml",
    "job_intraday_monitor.yaml",
    "job_intraday_market_refresh.yaml",
    "job_platinum_rankings.yaml",
    "job_symbol_cleanup.yaml",
)

MARKET_PIPELINE_JOB_MANIFESTS = (
    "job_bronze_market_data.yaml",
    "job_silver_market_data.yaml",
    "job_gold_market_data.yaml",
)

QUIVER_PIPELINE_JOB_MANIFESTS = (
    "job_bronze_quiver.yaml",
    "job_silver_quiver_data.yaml",
    "job_gold_quiver_data.yaml",
)

EXPECTED_JOB_RESOURCE_NAMES = {
    "job_backtests.yaml": "backtests-job",
    "job_backtests_reconcile.yaml": "backtests-reconcile-job",
    "job_bronze_earnings_data.yaml": "bronze-earnings-job",
    "job_bronze_economic_catalyst_data.yaml": "bronze-economic-catalyst-job",
    "job_bronze_finance_data.yaml": "bronze-finance-job",
    "job_bronze_market_data.yaml": "bronze-market-job",
    "job_bronze_price_target_data.yaml": "bronze-price-target-job",
    "job_bronze_quiver.yaml": "bronze-quiver-job",
    "job_gold_earnings_data.yaml": "gold-earnings-job",
    "job_gold_economic_catalyst_data.yaml": "gold-economic-catalyst-job",
    "job_gold_finance_data.yaml": "gold-finance-job",
    "job_gold_market_data.yaml": "gold-market-job",
    "job_gold_price_target_data.yaml": "gold-price-target-job",
    "job_gold_quiver_data.yaml": "gold-quiver-data-job",
    "job_gold_regime_data.yaml": "gold-regime-job",
    "job_intraday_market_refresh.yaml": "intraday-market-refresh-job",
    "job_intraday_monitor.yaml": "intraday-monitor-job",
    "job_platinum_rankings.yaml": "platinum-rankings-job",
    "job_results_reconcile.yaml": "results-reconcile-job",
    "job_silver_earnings_data.yaml": "silver-earnings-job",
    "job_silver_economic_catalyst_data.yaml": "silver-economic-catalyst-job",
    "job_silver_finance_data.yaml": "silver-finance-job",
    "job_silver_market_data.yaml": "silver-market-job",
    "job_silver_price_target_data.yaml": "silver-price-target-job",
    "job_silver_quiver_data.yaml": "silver-quiver-data-job",
    "job_symbol_cleanup.yaml": "symbol-cleanup-job",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_jobs_has_only_current_runtime_workflows() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    expected = {
        "deploy-prod.yml",
        "quality.yml",
        "release.yml",
    }
    assert {path.name for path in workflow_dir.glob("*.yml")} == expected


def test_jobs_deployment_docs_point_to_control_plane_for_shared_bootstrap() -> None:
    text = (repo_root() / "DEPLOYMENT_SETUP.md").read_text(encoding="utf-8")
    assert "asset-allocation-control-plane" in text
    assert "scripts\\ops\\provision\\provision_azure.ps1" in text
    assert "http://asset-allocation-api`" in text


def test_jobs_bootstrap_defaults_internal_control_plane_target() -> None:
    env_template = (repo_root() / ".env.template").read_text(encoding="utf-8")
    assert "JOB_STARTUP_API_CONTAINER_APPS=asset-allocation-api\n" in env_template
    assert "ASSET_ALLOCATION_API_BASE_URL=http://asset-allocation-api\n" in env_template

    readme_text = (repo_root() / "README.md").read_text(encoding="utf-8")
    assert "http://asset-allocation-api`" in readme_text

    for workflow_name in ("quality.yml",):
        workflow_text = (repo_root() / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")
        assert "ASSET_ALLOCATION_API_BASE_URL: http://asset-allocation-api" in workflow_text, workflow_name
        assert "https://control-plane.example" not in workflow_text, workflow_name


def test_jobs_repo_does_not_ship_control_plane_or_ui_assets() -> None:
    root = repo_root()
    for relative_path in STALE_RUNTIME_PATHS:
        assert not (root / relative_path).exists(), f"unexpected stale path present: {relative_path}"


def test_codedrift_config_is_jobs_only() -> None:
    text = (repo_root() / ".codedrift.yml").read_text(encoding="utf-8")
    for stale_reference in STALE_DRIFT_REFERENCES:
        assert stale_reference not in text
    assert "python3 scripts/run_quality_gate.py test-fast" in text
    assert "python3 scripts/run_quality_gate.py test-full" in text


def test_api_backed_manual_jobs_define_control_plane_env_vars() -> None:
    deploy_dir = repo_root() / "deploy"
    for manifest_name in API_BOOTSTRAP_JOB_MANIFESTS:
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        assert "name: ASSET_ALLOCATION_API_BASE_URL" in text, manifest_name
        assert "value: ${ASSET_ALLOCATION_API_BASE_URL}" in text, manifest_name
        assert "name: ASSET_ALLOCATION_API_SCOPE" in text, manifest_name
        assert "value: ${ASSET_ALLOCATION_API_SCOPE}" in text, manifest_name


def test_job_manifests_define_valid_metadata_without_renaming_aca_jobs() -> None:
    deploy_dir = repo_root() / "deploy"
    manifests = sorted(deploy_dir.glob("job_*.yaml"))
    assert {path.name for path in manifests} == set(EXPECTED_JOB_RESOURCE_NAMES)

    for manifest in manifests:
        payload = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        assert isinstance(payload, dict), manifest.name
        job_name = str(payload.get("name") or "")
        assert job_name == EXPECTED_JOB_RESOURCE_NAMES[manifest.name]

        metadata = validate_job_metadata_tags(job_name, payload.get("tags") or {})
        expected = expected_job_metadata(job_name)
        assert expected is not None, manifest.name
        assert metadata.jobCategory == expected.jobCategory
        assert metadata.jobKey == expected.jobKey
        assert metadata.jobRole == expected.jobRole
        assert metadata.triggerOwner == expected.triggerOwner


def test_job_manifests_are_utf8_without_bom() -> None:
    for manifest in sorted((repo_root() / "deploy").glob("job_*.yaml")):
        assert not manifest.read_bytes().startswith(b"\xef\xbb\xbf"), manifest.name


def test_consolidated_quiver_bronze_job_metadata_is_cataloged() -> None:
    metadata = validate_job_metadata_tags(
        "bronze-quiver-job",
        {
            "job-category": "data-pipeline",
            "job-key": "quiver",
            "job-role": "load",
            "trigger-owner": "schedule",
        },
    )

    assert metadata.jobCategory == "data-pipeline"
    assert metadata.jobKey == "quiver"
    assert metadata.jobRole == "load"
    assert metadata.triggerOwner == "schedule"


def test_strategy_compute_target_jobs_have_required_classifications() -> None:
    deploy_dir = repo_root() / "deploy"
    expected = {
        "job_gold_regime_data.yaml": ("strategy-compute", "regime", "publish", "schedule"),
        "job_platinum_rankings.yaml": ("strategy-compute", "rankings", "materialize", "control-plane"),
        "job_backtests.yaml": ("strategy-compute", "backtests", "execute", "control-plane"),
        "job_backtests_reconcile.yaml": ("operational-support", "backtests", "reconcile", "reconciler"),
        "job_results_reconcile.yaml": ("operational-support", "results-reconcile", "reconcile", "reconciler"),
    }
    for manifest_name, classification in expected.items():
        payload = yaml.safe_load((deploy_dir / manifest_name).read_text(encoding="utf-8"))
        metadata = validate_job_metadata_tags(str(payload["name"]), payload["tags"])
        assert (
            metadata.jobCategory,
            metadata.jobKey,
            metadata.jobRole,
            metadata.triggerOwner,
        ) == classification


def test_gold_regime_job_runs_after_market_chain_with_bounded_retry() -> None:
    payload = yaml.safe_load((repo_root() / "deploy" / "job_gold_regime_data.yaml").read_text(encoding="utf-8"))
    configuration = payload["properties"]["configuration"]

    assert configuration["triggerType"] == "Schedule"
    assert configuration["scheduleTriggerConfig"]["cronExpression"] == "30 2 * * 2-6"
    assert configuration["replicaRetryLimit"] <= 1
    assert configuration["scheduleTriggerConfig"]["parallelism"] == 1
    assert configuration["scheduleTriggerConfig"]["replicaCompletionCount"] == 1


def test_quiver_job_manifests_define_control_plane_env_vars() -> None:
    deploy_dir = repo_root() / "deploy"
    for manifest_name in QUIVER_PIPELINE_JOB_MANIFESTS:
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        assert "name: ASSET_ALLOCATION_API_BASE_URL" in text, manifest_name
        assert "value: ${ASSET_ALLOCATION_API_BASE_URL}" in text, manifest_name
        assert "name: ASSET_ALLOCATION_API_SCOPE" in text, manifest_name
        assert "value: ${ASSET_ALLOCATION_API_SCOPE}" in text, manifest_name


def test_intraday_job_manifests_run_on_weekday_five_minute_cadence() -> None:
    deploy_dir = repo_root() / "deploy"
    for manifest_name in ("job_intraday_monitor.yaml", "job_intraday_market_refresh.yaml"):
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        assert "triggerType: Schedule" in text, manifest_name
        assert 'cronExpression: "*/5 * * * 1-5"' in text, manifest_name


def test_intraday_refresh_manifest_keeps_market_refresh_in_process() -> None:
    text = (repo_root() / "deploy" / "job_intraday_market_refresh.yaml").read_text(encoding="utf-8")
    assert 'command: ["python", "-m", "tasks.intraday_monitor.refresh_worker"]' in text
    assert "TRIGGER_NEXT_JOB_NAME" not in text


def test_market_job_manifests_use_contract_storage_account_variable() -> None:
    deploy_dir = repo_root() / "deploy"
    for manifest_name in MARKET_PIPELINE_JOB_MANIFESTS:
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        assert "name: AZURE_STORAGE_ACCOUNT_NAME" in text, manifest_name
        assert "value: ${AZURE_STORAGE_ACCOUNT_NAME}" in text, manifest_name
        assert "value: assetallocstorage001" not in text, manifest_name


def test_quiver_job_manifests_use_contract_storage_account_variable() -> None:
    deploy_dir = repo_root() / "deploy"
    for manifest_name in QUIVER_PIPELINE_JOB_MANIFESTS:
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        assert "name: AZURE_STORAGE_ACCOUNT_NAME" in text, manifest_name
        assert "value: ${AZURE_STORAGE_ACCOUNT_NAME}" in text, manifest_name
        assert "value: assetallocstorage001" not in text, manifest_name


def test_market_job_manifests_keep_folder_envs_aligned_to_contract_names() -> None:
    expected_lines = {
        "job_bronze_market_data.yaml": (
            "value: ${AZURE_FOLDER_MARKET}",
            "value: ${AZURE_FOLDER_FINANCE}",
            "value: ${AZURE_FOLDER_EARNINGS}",
            "value: ${AZURE_FOLDER_TARGETS}",
            "name: BRONZE_MARKET_ALPHA_VANTAGE_ENRICHMENT_ENABLED",
            "value: ${BRONZE_MARKET_ALPHA_VANTAGE_ENRICHMENT_ENABLED}",
        ),
        "job_silver_market_data.yaml": (
            "value: ${AZURE_FOLDER_MARKET}",
            "value: ${AZURE_FOLDER_FINANCE}",
            "value: ${AZURE_FOLDER_EARNINGS}",
            "value: ${AZURE_FOLDER_TARGETS}",
        ),
        "job_gold_market_data.yaml": (
            "value: ${AZURE_FOLDER_MARKET}",
            "value: ${AZURE_FOLDER_FINANCE}",
            "value: ${AZURE_FOLDER_EARNINGS}",
            "value: ${AZURE_FOLDER_TARGETS}",
        ),
    }
    deploy_dir = repo_root() / "deploy"
    for manifest_name, lines in expected_lines.items():
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        for line in lines:
            assert line in text, f"{manifest_name} missing expected folder mapping: {line}"


def test_quiver_job_manifests_keep_folder_envs_aligned_to_contract_names() -> None:
    expected_lines = {
        "job_bronze_quiver.yaml": ("value: ${AZURE_FOLDER_QUIVER}",),
        "job_silver_quiver_data.yaml": ("value: ${AZURE_FOLDER_QUIVER}",),
        "job_gold_quiver_data.yaml": ("value: ${AZURE_FOLDER_QUIVER}",),
    }
    deploy_dir = repo_root() / "deploy"
    for manifest_name, lines in expected_lines.items():
        text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
        for line in lines:
            assert line in text, f"{manifest_name} missing expected folder mapping: {line}"


def test_quiver_job_manifests_keep_expected_trigger_types_and_chaining() -> None:
    deploy_dir = repo_root() / "deploy"

    bronze_text = (deploy_dir / "job_bronze_quiver.yaml").read_text(encoding="utf-8")
    assert "triggerType: Schedule" in bronze_text
    assert 'cronExpression: "0 * * * 1-5"' in bronze_text
    assert "value: incremental" in bronze_text
    assert "name: TRIGGER_NEXT_JOB_NAME" in bronze_text
    assert "value: ${SILVER_QUIVER_JOB}" in bronze_text

    silver_text = (deploy_dir / "job_silver_quiver_data.yaml").read_text(encoding="utf-8")
    assert "triggerType: Manual" in silver_text
    assert "manualTriggerConfig:" in silver_text
    assert "name: TRIGGER_NEXT_JOB_NAME" in silver_text
    assert "value: ${GOLD_QUIVER_JOB}" in silver_text

    gold_text = (deploy_dir / "job_gold_quiver_data.yaml").read_text(encoding="utf-8")
    assert "triggerType: Manual" in gold_text
    assert "manualTriggerConfig:" in gold_text
    assert "TRIGGER_NEXT_JOB_NAME" not in gold_text


def test_quiver_bronze_manifests_define_mode_and_runtime_envs() -> None:
    deploy_dir = repo_root() / "deploy"
    manifest_name = "job_bronze_quiver.yaml"
    text = (deploy_dir / manifest_name).read_text(encoding="utf-8")
    for required_name in (
        "QUIVER_DATA_ENABLED",
        "QUIVER_DATA_JOB_MODE",
        "QUIVER_DATA_TICKER_BATCH_SIZE",
        "QUIVER_DATA_HISTORICAL_BATCH_SIZE",
        "QUIVER_DATA_SYMBOL_LIMIT",
        "QUIVER_DATA_PAGE_SIZE",
        "QUIVER_DATA_MAX_PAGES_PER_REQUEST",
        "QUIVER_DATA_SEC13F_TODAY_ONLY",
    ):
        assert f"name: {required_name}" in text, f"{manifest_name} missing {required_name}"
    assert 'name: ASSET_ALLOCATION_API_TIMEOUT_SECONDS' in text, manifest_name
    assert 'value: "120"' in text, manifest_name


def test_economic_catalyst_bronze_manifest_runs_weekdays_every_30_minutes_without_retries() -> None:
    text = (repo_root() / "deploy" / "job_bronze_economic_catalyst_data.yaml").read_text(encoding="utf-8")

    assert "triggerType: Schedule" in text
    assert 'cronExpression: "*/30 * * * 1-5"' in text
    assert "replicaRetryLimit: 0" in text


def test_platinum_rankings_job_does_not_define_deploy_time_ranking_overrides() -> None:
    text = (repo_root() / "deploy" / "job_platinum_rankings.yaml").read_text(encoding="utf-8")
    assert "RANKING_STRATEGY_NAME" not in text
    assert "RANKING_START_DATE" not in text
    assert "RANKING_END_DATE" not in text


def test_ranking_materialization_doc_lists_required_api_env_vars() -> None:
    text = (repo_root() / "docs" / "ops" / "ranking-materialization.md").read_text(encoding="utf-8")
    assert "`POSTGRES_DSN`" in text
    assert "`ASSET_ALLOCATION_API_BASE_URL`" in text
    assert "`ASSET_ALLOCATION_API_SCOPE`" in text


def test_contributor_and_security_docs_reference_live_jobs_assets_only() -> None:
    root = repo_root()
    for path in ("CONTRIBUTING.md", "SECURITY.md"):
        text = (root / path).read_text(encoding="utf-8")
        for stale_reference in STALE_DOC_REFERENCES:
            assert stale_reference not in text, f"{path} still references stale asset: {stale_reference}"


def test_results_reconcile_job_is_scheduled_reconciler_and_not_dry_run() -> None:
    text = (repo_root() / "deploy" / "job_results_reconcile.yaml").read_text(encoding="utf-8")
    assert "triggerType: Schedule" in text
    assert "scheduleTriggerConfig:" in text
    assert 'cronExpression: "*/30 * * * *"' in text
    assert "manualTriggerConfig:" not in text
    assert "RESULTS_RECONCILE_DRY_RUN" not in text
    assert "name: ASSET_ALLOCATION_API_TIMEOUT_SECONDS" in text
    assert 'value: "600"' in text


def test_symbol_cleanup_job_is_scheduled_and_points_to_worker_module() -> None:
    text = (repo_root() / "deploy" / "job_symbol_cleanup.yaml").read_text(encoding="utf-8")
    assert "triggerType: Schedule" in text
    assert "scheduleTriggerConfig:" in text
    assert 'cronExpression: "0 23 * * 1-5"' in text
    assert 'command: ["python", "-m", "tasks.symbol_cleanup.worker"]' in text


def test_backtests_reconcile_job_uses_lower_frequency_and_timeout() -> None:
    text = (repo_root() / "deploy" / "job_backtests_reconcile.yaml").read_text(encoding="utf-8")
    assert 'cronExpression: "*/5 * * * *"' in text
    assert "replicaTimeout: 300" in text


def test_silver_finance_job_limits_lock_wait_time() -> None:
    text = (repo_root() / "deploy" / "job_silver_finance_data.yaml").read_text(encoding="utf-8")
    assert "name: SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS" in text
    assert 'value: "60"' in text


def test_dockerfile_uses_repo_local_copy_paths_and_omits_psql_client() -> None:
    text = (repo_root() / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY asset-allocation-jobs/" not in text
    assert "postgresql-client" not in text
    assert "USER app" in text


def test_docker_context_excludes_local_env_files() -> None:
    text = (repo_root() / ".dockerignore").read_text(encoding="utf-8")
    assert ".env\n" in text
    assert ".env.*" in text


def test_cost_guardrails_docs_use_local_template_and_cli_entrypoint() -> None:
    text = (repo_root() / "docs" / "ops" / "cost-guardrails.md").read_text(encoding="utf-8")
    assert "/mnt/c/Users/rdpro/Projects/AssetAllocation/scripts/configure_cost_guardrails.ps1" not in text
    assert "pwsh ./scripts/configure_cost_guardrails.ps1" not in text
    assert "az deployment sub create" in text
    assert "deploy/cost_guardrails.parameters.example.json" in text


def test_cost_guardrails_parameters_example_exists() -> None:
    assert (repo_root() / "deploy" / "cost_guardrails.parameters.example.json").exists()


def test_deployment_setup_documents_cost_guardrails_and_manual_results_reconcile() -> None:
    text = (repo_root() / "DEPLOYMENT_SETUP.md").read_text(encoding="utf-8")
    assert "deploy/cost_guardrails.bicep" in text
    assert "results-reconcile" in text
