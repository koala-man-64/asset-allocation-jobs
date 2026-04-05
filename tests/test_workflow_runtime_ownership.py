from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_jobs_has_only_current_runtime_workflows() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    expected = {
        "ci.yml",
        "control-plane-compat.yml",
        "deploy-prod.yml",
        "release.yml",
        "security.yml",
        "trigger-jobs.yml",
    }
    assert {path.name for path in workflow_dir.glob("*.yml")} == expected


def test_jobs_deployment_docs_point_to_control_plane_for_shared_bootstrap() -> None:
    text = (repo_root() / "DEPLOYMENT_SETUP.md").read_text(encoding="utf-8")
    assert "asset-allocation-control-plane" in text
    assert "scripts\\ops\\provision\\provision_azure.ps1" in text
