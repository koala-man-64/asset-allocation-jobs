from __future__ import annotations

import csv
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


_REMOVED_DEPLOY_ENV_KEYS = (
    "API_KEY",
    "ASSET_ALLOCATION_API_KEY",
    "VITE_BACKTEST_API_BASE_URL",
)


def _deploy_validation_env(**overrides: str) -> dict[str, str]:
    env = {
        **os.environ,
        "AZURE_CLIENT_ID": "client-id",
        "AZURE_TENANT_ID": "tenant-id",
        "AZURE_SUBSCRIPTION_ID": "subscription-id",
        "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net",
        "ALPHA_VANTAGE_API_KEY": "alpha",
        "NASDAQ_API_KEY": "nasdaq",
        "POSTGRES_DSN": "postgresql://user:pass@db.example.com:5432/asset_allocation",
        "SERVICE_ACCOUNT_NAME": "asset-allocation-sa",
        "ASSET_ALLOCATION_API_BASE_URL": "https://asset-allocation.example.com",
        "API_OIDC_ISSUER": "https://issuer.example.com",
        "API_OIDC_AUDIENCE": "asset-allocation-api",
        "UI_OIDC_CLIENT_ID": "client-id",
        "UI_OIDC_AUTHORITY": "https://login.microsoftonline.com/tenant-id",
        "UI_OIDC_SCOPES": "api://asset-allocation-api/user_impersonation openid profile offline_access",
        "UI_OIDC_REDIRECT_URI": "https://asset-allocation.example.com/auth/callback",
        "ASSET_ALLOCATION_API_SCOPE": "api://asset-allocation-api/.default",
        "LOG_LEVEL": "INFO",
        "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID": "",
    }
    for key in _REMOVED_DEPLOY_ENV_KEYS:
        env.pop(key, None)
    env.update(overrides)
    return env


def _load_deploy_validation_module():
    repo_root = _repo_root()
    module_path = repo_root / "scripts" / "validate_deploy_inputs.py"
    spec = importlib.util.spec_from_file_location("validate_deploy_inputs", module_path)
    assert spec and spec.loader, f"unable to load module from {module_path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_deploy_validation(**overrides: str) -> subprocess.CompletedProcess[str]:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "validate_deploy_inputs.py"
    return subprocess.run(
        [sys.executable, str(script)],
        cwd=repo_root,
        env=_deploy_validation_env(**overrides),
        capture_output=True,
        text=True,
        check=False,
    )


def _assert_azure_client_id_env_present(doc: dict, *, source: str) -> None:
    template = (doc.get("properties") or {}).get("template") or {}
    containers = template.get("containers") or []
    assert containers, f"{source}: expected at least one container definition"

    for container in containers:
        env_list = container.get("env") or []
        for entry in env_list:
            if entry.get("name") == "AZURE_CLIENT_ID":
                assert entry.get("value") == "${ACR_PULL_IDENTITY_CLIENT_ID}", (
                    f"{source}: AZURE_CLIENT_ID env must be wired to ACR_PULL_IDENTITY_CLIENT_ID"
                )
                return

    raise AssertionError(f"{source}: missing env var AZURE_CLIENT_ID")


def _assert_job_manifest_uses_managed_identity_for_acr_pull(doc: dict, *, source: str) -> None:
    identity = doc.get("identity") or {}
    assert identity.get("type") == "UserAssigned", (
        f"{source}: expected top-level UserAssigned identity"
    )
    user_assigned = identity.get("userAssignedIdentities") or {}
    assert "${ACR_PULL_IDENTITY_RESOURCE_ID}" in user_assigned, (
        f"{source}: expected ACR pull identity placeholder in userAssignedIdentities"
    )

    configuration = (doc.get("properties") or {}).get("configuration") or {}
    registries = configuration.get("registries") or []
    assert registries, f"{source}: expected at least one registry entry"
    assert any(
        entry.get("identity") == "${ACR_PULL_IDENTITY_RESOURCE_ID}" for entry in registries
    ), f"{source}: expected registry identity to use ACR_PULL_IDENTITY_RESOURCE_ID"


def test_all_jobs_wire_user_assigned_identity_client_id() -> None:
    repo_root = _repo_root()
    for path in sorted((repo_root / "deploy").glob("job_*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"
        _assert_azure_client_id_env_present(doc, source=str(path))


def test_all_jobs_use_manifest_managed_identity_for_acr_pull() -> None:
    repo_root = _repo_root()
    for path in sorted((repo_root / "deploy").glob("job_*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"
        _assert_job_manifest_uses_managed_identity_for_acr_pull(doc, source=str(path))


def test_api_manifest_wires_user_assigned_identity_client_id() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "app_api.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"
    _assert_azure_client_id_env_present(doc, source=str(path))


def test_deploy_workflow_exports_acr_pull_identity_client_id() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    text = deploy_workflow.read_text(encoding="utf-8")
    assert "ACR_PULL_IDENTITY_CLIENT_ID" in text, "deploy workflow must export ACR_PULL_IDENTITY_CLIENT_ID"
    assert "BACKTEST_JOB" in text, "deploy workflow must define the backtest job name"
    assert "GOLD_REGIME_JOB" in text, "deploy workflow must define the gold regime job name"
    assert "python3 scripts/validate_deploy_inputs.py" in text, (
        "deploy workflow must validate deployment inputs through the shared script"
    )
    assert "API_INGRESS_EXTERNAL" not in text, (
        "deploy workflow must not use ingress as an env toggle"
    )
    assert "API_AUTH_MODE" not in text and "UI_AUTH_MODE" not in text, (
        "deploy workflow must not use auth mode env toggles"
    )


def test_run_tests_workflow_enforces_ui_format_and_lint() -> None:
    repo_root = _repo_root()
    workflow = repo_root / ".github" / "workflows" / "run_tests.yml"
    text = workflow.read_text(encoding="utf-8")

    assert "pnpm format:check && pnpm lint" in text, (
        "run_tests workflow must block on UI format and lint"
    )
    assert "UI format check failed (non-blocking)" not in text, (
        "run_tests workflow must not downgrade UI format failures to warnings"
    )
    assert "UI lint failed (non-blocking)" not in text, (
        "run_tests workflow must not downgrade UI lint failures to warnings"
    )


def test_repo_only_keeps_codex_agent_contract() -> None:
    repo_root = _repo_root()
    legacy_agent_files = (
        [path for path in (repo_root / ".agent").rglob("*") if path.is_file()]
        if (repo_root / ".agent").exists()
        else []
    )
    legacy_claude_files = (
        [path for path in (repo_root / ".claude").rglob("*") if path.is_file()]
        if (repo_root / ".claude").exists()
        else []
    )

    assert legacy_agent_files == [], "legacy .agent tree must not remain in the repository"
    assert legacy_claude_files == [], "legacy .claude tree must not remain in the repository"


def test_deploy_workflow_deploys_jobs_from_yaml_without_pre_mutating_job_identity() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    deploy_job_helper = repo_root / "scripts" / "deploy_containerapp_job.sh"
    workflow_text = deploy_workflow.read_text(encoding="utf-8")
    helper_text = deploy_job_helper.read_text(encoding="utf-8")

    assert "az containerapp job identity assign" not in workflow_text, (
        "deploy workflow should not mutate job identity before YAML update"
    )
    assert "az containerapp job registry set" not in workflow_text, (
        "deploy workflow should not mutate job registry before YAML update"
    )
    assert workflow_text.count("bash scripts/deploy_containerapp_job.sh") == 14, (
        "deploy workflow must route every managed Container App job through the shared YAML deploy helper"
    )
    assert "Updating job from YAML (image + identity + registry)..." in helper_text, (
        "job deploy helper should update existing jobs using the rendered manifest"
    )


def test_deploy_workflow_reconciles_shared_resources_before_rollout() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    deploy_job_helper = repo_root / "scripts" / "deploy_containerapp_job.sh"
    workflow_text = deploy_workflow.read_text(encoding="utf-8")
    helper_text = deploy_job_helper.read_text(encoding="utf-8")

    assert "Prepare Provisioning Env File" in workflow_text, (
        "deploy workflow must prepare a temporary env file for provisioning reconciliation"
    )
    assert "Reconcile Shared Azure Provisioning" in workflow_text, (
        "deploy workflow must reconcile shared Azure resources before rollout"
    )
    assert "scripts/provision_azure.ps1" in workflow_text, (
        "deploy workflow must route shared-resource reconciliation through provision_azure.ps1"
    )
    assert "Reconcile Postgres Provisioning" in workflow_text, (
        "deploy workflow must reconcile Postgres state before rollout"
    )
    assert "scripts/provision_azure_postgres.ps1" in workflow_text, (
        "deploy workflow must route Postgres reconciliation through provision_azure_postgres.ps1"
    )
    assert "-ResetBeforeMigrations:$false" in workflow_text, (
        "deploy workflow must apply Postgres migrations without resetting the database"
    )
    assert "Validate Provisioned Deploy Targets" in workflow_text, (
        "deploy workflow must validate storage targets after provisioning reconciliation"
    )
    assert "after provisioning reconciliation" in workflow_text, (
        "deploy workflow must fail closed when reconciliation did not provision required storage"
    )
    assert "Provision it outside GitHub Actions before running deploy." not in workflow_text, (
        "deploy workflow should no longer require shared infrastructure to be provisioned out of band"
    )
    assert "apply_postgres_migrations.ps1" not in workflow_text, (
        "deploy workflow should reconcile Postgres through the dedicated provisioner instead of the raw migration helper"
    )
    assert "Container App Job '" not in workflow_text, (
        "deploy workflow should no longer fail fast when a managed Container App job is absent"
    )
    assert "Deploy workflow only updates existing jobs. Provision it outside GitHub Actions." not in workflow_text, (
        "deploy workflow should not treat missing Container App jobs as an external provisioning issue"
    )
    assert "az storage container create" not in workflow_text, (
        "deploy workflow must not provision storage containers"
    )
    assert "az containerapp job create" in helper_text, (
        "job deploy helper must create missing Container App jobs from YAML"
    )
    assert 'az containerapp job create \\\n    --name "$job_name" \\\n    --resource-group "$RESOURCE_GROUP" \\\n    --yaml "$tmp_file" \\\n    --only-show-errors' in helper_text, (
        "job deploy helper must pass the explicit job name when creating from YAML"
    )
    assert "Creating job from YAML (image + identity + registry)..." in helper_text, (
        "job deploy helper must create missing jobs using the rendered manifest"
    )


def test_deploy_workflow_creates_missing_api_app_from_yaml() -> None:
    repo_root = _repo_root()
    deploy_workflow = repo_root / ".github" / "workflows" / "deploy.yml"
    text = deploy_workflow.read_text(encoding="utf-8")

    assert "Deploying unified app from public ingress YAML profile..." in text, (
        "deploy workflow must render the public unified Container App manifest"
    )
    assert 'envsubst < deploy/app_api_public.yaml > "$tmp"' in text, (
        "deploy workflow must deploy the public unified Container App manifest"
    )
    assert "Creating Container App from rendered YAML..." in text, (
        "deploy workflow must create the unified Container App when it is missing"
    )
    assert "az containerapp create" in text, (
        "deploy workflow must create the unified Container App from YAML"
    )
    assert 'az containerapp create \\\n              --name ${{ env.API_APP_NAME }} \\\n              --resource-group ${{ env.RESOURCE_GROUP }} \\\n              --yaml "$tmp" \\\n              --only-show-errors' in text, (
        "deploy workflow must pass the explicit Container App name when creating from YAML"
    )
    assert '--yaml "$tmp"' in text, (
        "deploy workflow must create the unified Container App from the rendered manifest"
    )
    assert "Deploy workflow only updates existing apps. Provision it outside GitHub Actions." not in text, (
        "deploy workflow should not fail fast when the unified Container App is absent"
    )


def test_api_manifest_allowlists_backtest_job() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "app_api.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"
    containers = ((doc.get("properties") or {}).get("template") or {}).get("containers") or []
    api_container = next(
        (container for container in containers if container.get("name") == "asset-allocation-api"),
        None,
    )
    assert api_container, f"{path}: expected asset-allocation-api container"
    env_vars = {entry.get("name"): entry.get("value") for entry in api_container.get("env") or []}
    assert "backtests-job" in str(env_vars.get("SYSTEM_HEALTH_ARM_JOBS") or ""), (
        "app_api manifest must allowlist the backtest ACA job"
    )
    assert env_vars.get("BACKTEST_ACA_JOB_NAME") == "backtests-job", (
        "app_api manifest must export BACKTEST_ACA_JOB_NAME"
    )
    assert "gold-regime-job" in str(env_vars.get("SYSTEM_HEALTH_ARM_JOBS") or ""), (
        "app_api manifest must allowlist the gold regime ACA job"
    )
    assert env_vars.get("REGIME_ACA_JOB_NAME") == "gold-regime-job", (
        "app_api manifest must export REGIME_ACA_JOB_NAME"
    )


def test_gold_regime_job_runs_daily_at_4pm_est() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "job_gold_regime_data.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"

    configuration = (doc.get("properties") or {}).get("configuration") or {}
    assert configuration.get("triggerType") == "Schedule", (
        "gold regime job must be scheduled"
    )
    schedule = configuration.get("scheduleTriggerConfig") or {}
    assert schedule.get("cronExpression") == "0 21 * * *", (
        "gold regime job must run daily at 21:00 UTC (4:00 PM EST)"
    )


def test_bronze_jobs_do_not_automatically_retry_failed_executions() -> None:
    repo_root = _repo_root()
    bronze_job_names = (
        "job_bronze_market_data.yaml",
        "job_bronze_finance_data.yaml",
        "job_bronze_earnings_data.yaml",
        "job_bronze_price_target_data.yaml",
    )

    for job_name in bronze_job_names:
        path = repo_root / "deploy" / job_name
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"

        configuration = (doc.get("properties") or {}).get("configuration") or {}
        assert configuration.get("replicaRetryLimit") == 0, (
            f"{path}: bronze jobs must not automatically retry failed replicas"
        )


def test_silver_jobs_retry_failed_executions_twice() -> None:
    repo_root = _repo_root()
    silver_job_names = (
        "job_silver_market_data.yaml",
        "job_silver_finance_data.yaml",
        "job_silver_earnings_data.yaml",
        "job_silver_price_target_data.yaml",
    )

    for job_name in silver_job_names:
        path = repo_root / "deploy" / job_name
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"

        configuration = (doc.get("properties") or {}).get("configuration") or {}
        assert configuration.get("replicaRetryLimit") == 2, (
            f"{path}: silver jobs must retry failed replicas twice"
        )


def test_gold_jobs_retry_failed_executions_three_times() -> None:
    repo_root = _repo_root()
    gold_job_names = (
        "job_gold_market_data.yaml",
        "job_gold_finance_data.yaml",
        "job_gold_earnings_data.yaml",
        "job_gold_price_target_data.yaml",
        "job_gold_regime_data.yaml",
    )

    for job_name in gold_job_names:
        path = repo_root / "deploy" / job_name
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"

        configuration = (doc.get("properties") or {}).get("configuration") or {}
        assert configuration.get("replicaRetryLimit") == 3, (
            f"{path}: gold jobs must retry failed replicas three times"
        )


def test_backtests_and_platinum_keep_existing_retry_policy() -> None:
    repo_root = _repo_root()
    expected_retry_limits = {
        "job_backtests.yaml": 1,
        "job_platinum_rankings.yaml": 1,
    }

    for job_name, expected_retry_limit in expected_retry_limits.items():
        path = repo_root / "deploy" / job_name
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), f"{path}: expected YAML mapping"

        configuration = (doc.get("properties") or {}).get("configuration") or {}
        assert configuration.get("replicaRetryLimit") == expected_retry_limit, (
            f"{path}: retry policy must remain unchanged"
        )


def test_gold_market_job_uses_tiered_retry_and_memory_settings() -> None:
    repo_root = _repo_root()
    path = repo_root / "deploy" / "job_gold_market_data.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), f"{path}: expected YAML mapping"

    configuration = (doc.get("properties") or {}).get("configuration") or {}
    assert configuration.get("replicaRetryLimit") == 3, (
        f"{path}: gold market job must retry failed replicas three times"
    )

    containers = (((doc.get("properties") or {}).get("template") or {}).get("containers") or [])
    gold_container = next((container for container in containers if container.get("name") == "gold-market-job"), None)
    assert gold_container, f"{path}: expected gold-market-job container"

    resources = gold_container.get("resources") or {}
    assert resources.get("cpu") == 4.0, (
        f"{path}: gold market job remediation must use a valid Consumption CPU/memory pair"
    )
    assert resources.get("memory") == "8Gi", (
        f"{path}: gold market job remediation must raise memory to 8Gi"
    )


def test_all_consumption_job_manifests_use_valid_resource_pairs() -> None:
    module = _load_deploy_validation_module()
    module.validate_job_manifest_resources(_repo_root() / "deploy")


def test_deploy_validation_rejects_invalid_consumption_resource_pair(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_deploy_validation_module()
    manifest = tmp_path / "job_invalid.yaml"
    manifest.write_text(
        "\n".join(
            (
                "name: invalid-job",
                "properties:",
                "  template:",
                "    containers:",
                "    - name: invalid-job",
                "      resources:",
                "        cpu: 2.0",
                "        memory: 8Gi",
                "  workloadProfileName: Consumption",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit):
        module.validate_job_manifest_resources(tmp_path)

    output = capsys.readouterr().out
    assert "job_invalid.yaml" in output
    assert "cpu=2 memory=8Gi" in output


def test_setup_env_seeds_job_defaults_for_github_sync() -> None:
    repo_root = _repo_root()
    setup_env = repo_root / "scripts" / "setup-env.ps1"
    text = setup_env.read_text(encoding="utf-8")

    assert "gold-regime-job" in text, "setup-env must seed the gold regime job name"
    assert '$IsGitHubSyncTarget = $EnvFileName -ieq ".env.web"' in text, (
        "setup-env must detect .env.web targets for GitHub sync defaults"
    )
    assert 'Prompt-Var "ASSET_ALLOCATION_API_BASE_URL" $DefaultAssetAllocationApiBaseUrl' in text, (
        "setup-env must use GitHub-safe API base URL defaults for .env.web"
    )
    assert 'Prompt-Var "VITE_API_PROXY_TARGET" $DefaultViteApiProxyTarget' in text, (
        "setup-env must use GitHub-safe UI proxy defaults for .env.web"
    )
    assert 'Prompt-Var "BACKTEST_ACA_JOB_NAME" "backtests-job"' in text, (
        "setup-env must default BACKTEST_ACA_JOB_NAME for GitHub sync"
    )
    assert 'Prompt-Var "REGIME_ACA_JOB_NAME" "gold-regime-job"' in text, (
        "setup-env must default REGIME_ACA_JOB_NAME for GitHub sync"
    )
    assert 'Prompt-Var "ENTRA_OPERATOR_USER_OBJECT_ID"' in text, (
        "setup-env must prompt for the operator Entra user object ID"
    )
    assert 'Prompt-Var "VITE_ALLOW_BROWSER_API_KEY"' not in text, (
        "setup-env must not prompt for browser API key fallbacks"
    )
    assert 'Prompt-Var "VITE_BACKTEST_API_KEY"' not in text, (
        "setup-env must not prompt for bundled browser API keys"
    )
    assert "API_INGRESS_EXTERNAL" not in text, (
        "setup-env must not expose ingress as an env toggle"
    )


def test_env_contract_tracks_aca_job_names_as_checked_in_defaults() -> None:
    repo_root = _repo_root()
    contract = repo_root / "docs" / "ops" / "env-contract.csv"
    with contract.open(encoding="utf-8", newline="") as handle:
        rows = {row["name"]: row for row in csv.DictReader(handle)}

    assert rows["BACKTEST_ACA_JOB_NAME"]["class"] == "deploy_var"
    assert rows["BACKTEST_ACA_JOB_NAME"]["github_storage"] == "none"
    assert rows["REGIME_ACA_JOB_NAME"]["class"] == "deploy_var"
    assert rows["REGIME_ACA_JOB_NAME"]["github_storage"] == "none"


def test_env_contract_tracks_log_analytics_workspace_bootstrap_key_as_deploy_var() -> None:
    repo_root = _repo_root()
    contract = repo_root / "docs" / "ops" / "env-contract.csv"
    with contract.open(encoding="utf-8", newline="") as handle:
        rows = {row["name"]: row for row in csv.DictReader(handle)}

    assert rows["SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID"]["class"] == "deploy_var"
    assert rows["SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID"]["github_storage"] == "var"


def test_env_contract_tracks_api_scope_for_managed_identity_callers() -> None:
    repo_root = _repo_root()
    contract = repo_root / "docs" / "ops" / "env-contract.csv"
    with contract.open(encoding="utf-8", newline="") as handle:
        rows = {row["name"]: row for row in csv.DictReader(handle)}

    assert rows["ASSET_ALLOCATION_API_SCOPE"]["class"] == "deploy_var"
    assert rows["ASSET_ALLOCATION_API_SCOPE"]["github_storage"] == "var"


def test_env_contract_tracks_entra_operator_user_as_provisioning_only() -> None:
    repo_root = _repo_root()
    contract = repo_root / "docs" / "ops" / "env-contract.csv"
    with contract.open(encoding="utf-8", newline="") as handle:
        rows = {row["name"]: row for row in csv.DictReader(handle)}

    assert rows["ENTRA_OPERATOR_USER_OBJECT_ID"]["class"] == "deploy_var"
    assert rows["ENTRA_OPERATOR_USER_OBJECT_ID"]["github_storage"] == "none"


def test_env_template_includes_regime_job_defaults() -> None:
    repo_root = _repo_root()
    env_template = repo_root / ".env.template"
    text = env_template.read_text(encoding="utf-8")

    assert "gold-regime-job" in text, ".env.template must include the gold regime job"
    assert "BACKTEST_ACA_JOB_NAME=backtests-job" in text, (
        ".env.template must define BACKTEST_ACA_JOB_NAME"
    )
    assert "REGIME_ACA_JOB_NAME=gold-regime-job" in text, (
        ".env.template must define REGIME_ACA_JOB_NAME"
    )
    assert "ASSET_ALLOCATION_API_SCOPE=" in text, (
        ".env.template must define ASSET_ALLOCATION_API_SCOPE for managed-identity callers"
    )
    assert "ENTRA_OPERATOR_USER_OBJECT_ID=" in text, (
        ".env.template must define ENTRA_OPERATOR_USER_OBJECT_ID for provisioning"
    )
    assert "VITE_ALLOW_BROWSER_API_KEY" not in text, (
        ".env.template must not define browser API-key fallbacks"
    )
    assert "VITE_BACKTEST_API_KEY" not in text, (
        ".env.template must not define bundled browser API keys"
    )
    assert "ASSET_ALLOCATION_API_KEY=" not in text, (
        ".env.template must not define ETL API-key compatibility fallbacks"
    )
    assert "\nAPI_KEY=" not in text, (
        ".env.template must not define API-key auth fallbacks"
    )
    assert "VITE_BACKTEST_API_BASE_URL=" not in text, (
        ".env.template must not define legacy backtest UI base URL fallbacks"
    )
    assert "API_INGRESS_EXTERNAL" not in text, (
        ".env.template must not expose ingress as an env toggle"
    )


def test_deploy_validation_accepts_oidc_only_defaults() -> None:
    result = _run_deploy_validation()
    assert result.returncode == 0, result.stdout + result.stderr


def test_deploy_validation_requires_api_oidc_configuration() -> None:
    result = _run_deploy_validation(API_OIDC_ISSUER="", API_OIDC_AUDIENCE="")
    assert result.returncode != 0
    assert "Production deploy requires API OIDC configuration." in (result.stdout + result.stderr)


def test_deploy_validation_rejects_invalid_log_stream_batch_size() -> None:
    result = _run_deploy_validation(
        SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID="workspace-id",
        REALTIME_LOG_STREAM_BATCH_SIZE="5",
    )
    assert result.returncode != 0
    assert "REALTIME_LOG_STREAM_BATCH_SIZE must be between 10 and 500." in (
        result.stdout + result.stderr
    )


def test_deploy_validation_requires_complete_ui_oidc_configuration() -> None:
    result = _run_deploy_validation(
        API_OIDC_ISSUER="https://issuer.example.com",
        API_OIDC_AUDIENCE="asset-allocation",
        UI_OIDC_CLIENT_ID="client-id",
        UI_OIDC_AUTHORITY="",
        UI_OIDC_SCOPES="",
        UI_OIDC_REDIRECT_URI="",
    )
    assert result.returncode != 0
    assert (
        "Missing: UI_OIDC_AUTHORITY, UI_OIDC_SCOPES, UI_OIDC_REDIRECT_URI. The deployed UI only supports OIDC."
        in (result.stdout + result.stderr)
    )

    missing_all_ui_oidc = _run_deploy_validation(
        UI_OIDC_CLIENT_ID="",
        UI_OIDC_AUTHORITY="",
        UI_OIDC_SCOPES="",
        UI_OIDC_REDIRECT_URI="",
    )
    assert missing_all_ui_oidc.returncode != 0
    assert (
        "Production deploy requires browser OIDC configuration for the UI." in
        (missing_all_ui_oidc.stdout + missing_all_ui_oidc.stderr)
    )
    assert "The deployed UI only supports OIDC." in (
        missing_all_ui_oidc.stdout + missing_all_ui_oidc.stderr
    )

    missing_api_oidc = _run_deploy_validation(
        API_OIDC_ISSUER="",
        API_OIDC_AUDIENCE="",
        UI_OIDC_CLIENT_ID="client-id",
        UI_OIDC_AUTHORITY="https://issuer.example.com",
        UI_OIDC_SCOPES="api://asset-allocation/user_impersonation",
        UI_OIDC_REDIRECT_URI="https://asset-allocation.example.com/oauth2-callback",
    )
    assert missing_api_oidc.returncode != 0
    assert "Production deploy requires API OIDC configuration." in (
        missing_api_oidc.stdout + missing_api_oidc.stderr
    )

    missing_redirect_uri = _run_deploy_validation(
        API_OIDC_ISSUER="https://issuer.example.com",
        API_OIDC_AUDIENCE="asset-allocation",
        UI_OIDC_CLIENT_ID="client-id",
        UI_OIDC_AUTHORITY="https://issuer.example.com",
        UI_OIDC_SCOPES="api://asset-allocation/user_impersonation",
        UI_OIDC_REDIRECT_URI="",
    )
    assert missing_redirect_uri.returncode != 0
    assert "Missing: UI_OIDC_REDIRECT_URI. The deployed UI only supports OIDC." in (
        missing_redirect_uri.stdout + missing_redirect_uri.stderr
    )

    invalid_redirect_uri = _run_deploy_validation(
        API_OIDC_ISSUER="https://issuer.example.com",
        API_OIDC_AUDIENCE="asset-allocation",
        UI_OIDC_CLIENT_ID="client-id",
        UI_OIDC_AUTHORITY="https://issuer.example.com",
        UI_OIDC_SCOPES="api://asset-allocation/user_impersonation",
        UI_OIDC_REDIRECT_URI="http://asset-allocation.example.com/auth/callback",
    )
    assert invalid_redirect_uri.returncode != 0
    assert "UI_OIDC_REDIRECT_URI must be an absolute https:// URL." in (
        invalid_redirect_uri.stdout + invalid_redirect_uri.stderr
    )


def test_deploy_validation_accepts_full_oidc_configuration() -> None:
    result = _run_deploy_validation(
        API_OIDC_ISSUER="https://issuer.example.com",
        API_OIDC_AUDIENCE="asset-allocation",
        UI_OIDC_CLIENT_ID="client-id",
        UI_OIDC_AUTHORITY="https://issuer.example.com",
        UI_OIDC_SCOPES="api://asset-allocation/user_impersonation",
        UI_OIDC_REDIRECT_URI="https://asset-allocation.example.com/oauth2-callback",
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_deploy_validation_requires_api_scope_for_bronze_jobs() -> None:
    result = _run_deploy_validation(ASSET_ALLOCATION_API_SCOPE="")
    assert result.returncode != 0
    assert "ASSET_ALLOCATION_API_SCOPE is required for bronze job managed-identity callers." in (
        result.stdout + result.stderr
    )


def test_deploy_validation_rejects_removed_auth_and_ui_compatibility_settings() -> None:
    for name in ("API_KEY", "ASSET_ALLOCATION_API_KEY", "VITE_BACKTEST_API_BASE_URL"):
        for value in ("stale-value", ""):
            result = _run_deploy_validation(**{name: value})
            assert result.returncode != 0
            assert f"{name} is no longer supported." in (result.stdout + result.stderr)


def test_provision_azure_does_not_source_deprecated_kubernetes_env_fallbacks() -> None:
    repo_root = _repo_root()
    text = (repo_root / "scripts" / "provision_azure.ps1").read_text(encoding="utf-8")

    assert 'Get-EnvValue -Key "KUBERNETES_NAMESPACE"' not in text
    assert 'Get-EnvValue -Key "AKS_CLUSTER_NAME"' not in text


def test_public_deploy_surfaces_no_longer_reference_shared_api_key_auth() -> None:
    repo_root = _repo_root()
    workflow_text = (repo_root / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
    public_manifest = (repo_root / "deploy" / "app_api_public.yaml").read_text(encoding="utf-8")
    internal_manifest = (repo_root / "deploy" / "app_api.yaml").read_text(encoding="utf-8")
    bronze_market = (repo_root / "deploy" / "job_bronze_market_data.yaml").read_text(encoding="utf-8")
    bronze_finance = (repo_root / "deploy" / "job_bronze_finance_data.yaml").read_text(encoding="utf-8")
    bronze_earnings = (repo_root / "deploy" / "job_bronze_earnings_data.yaml").read_text(encoding="utf-8")

    assert "secrets.API_KEY" not in workflow_text
    assert "ASSET_ALLOCATION_API_KEY_HEADER" not in workflow_text
    assert "API_KEY: ${{ secrets.API_KEY }}" not in workflow_text
    assert "secretRef: api-key" not in public_manifest
    assert "secretRef: api-key" not in internal_manifest
    assert "ASSET_ALLOCATION_API_KEY" not in bronze_market
    assert "ASSET_ALLOCATION_API_KEY" not in bronze_finance
    assert "ASSET_ALLOCATION_API_KEY" not in bronze_earnings
    assert "name: api-key" not in bronze_market
    assert "name: api-key" not in bronze_finance
    assert "name: api-key" not in bronze_earnings


def test_reset_postgres_script_uses_psql_reset_and_repo_migrations() -> None:
    repo_root = _repo_root()
    reset_script = repo_root / "scripts" / "reset_postgres_from_scratch.ps1"
    text = reset_script.read_text(encoding="utf-8")

    assert "reset_postgres.py" not in text, (
        "reset_postgres_from_scratch should not depend on a local Python helper"
    )
    assert 'Invoke-Psql -Args @($Dsn, "-v", "ON_ERROR_STOP=1", "-c", $resetSql)' in text, (
        "reset_postgres_from_scratch must perform the destructive reset through psql"
    )
    assert '& $migrationScript -Dsn $Dsn -MigrationsDir $resolvedDir -UseDockerPsql:$UseDockerPsql' in text, (
        "reset_postgres_from_scratch must reapply repo-owned migrations through the shared migration script"
    )
    assert "-UseDockerPsql is ignored" not in text, (
        "reset_postgres_from_scratch must honor the UseDockerPsql switch"
    )
