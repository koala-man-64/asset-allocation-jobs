from __future__ import annotations

from pathlib import Path


SHARED_PROVISIONERS = (
    "configure_cost_guardrails.ps1",
    "provision_azure.ps1",
    "provision_azure_interactive.ps1",
    "provision_azure_postgres.ps1",
    "provision_entra_oidc.ps1",
    "validate_acr_pull.ps1",
    "validate_azure_permissions.ps1",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_jobs_repo_does_not_ship_shared_azure_provisioners() -> None:
    scripts_dir = repo_root() / "scripts"
    for name in SHARED_PROVISIONERS:
        assert not (scripts_dir / name).exists(), f"jobs repo must not own shared provisioner {name}"


def test_jobs_repo_keeps_repo_local_env_bootstrap_scripts() -> None:
    scripts_dir = repo_root() / "scripts"
    assert (scripts_dir / "setup-env.ps1").exists()
    assert (scripts_dir / "sync-all-to-github.ps1").exists()
