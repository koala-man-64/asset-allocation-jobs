from __future__ import annotations

from pathlib import Path


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
    "job_platinum_rankings.yaml",
)

MARKET_PIPELINE_JOB_MANIFESTS = (
    "job_bronze_market_data.yaml",
    "job_silver_market_data.yaml",
    "job_gold_market_data.yaml",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_jobs_has_only_current_runtime_workflows() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    expected = {
        "deploy-prod.yml",
        "integration.yml",
        "quality.yml",
        "release.yml",
    }
    assert {path.name for path in workflow_dir.glob("*.yml")} == expected


def test_jobs_deployment_docs_point_to_control_plane_for_shared_bootstrap() -> None:
    text = (repo_root() / "DEPLOYMENT_SETUP.md").read_text(encoding="utf-8")
    assert "asset-allocation-control-plane" in text
    assert "scripts\\ops\\provision\\provision_azure.ps1" in text


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


def test_market_job_manifests_use_contract_storage_account_variable() -> None:
    deploy_dir = repo_root() / "deploy"
    for manifest_name in MARKET_PIPELINE_JOB_MANIFESTS:
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
