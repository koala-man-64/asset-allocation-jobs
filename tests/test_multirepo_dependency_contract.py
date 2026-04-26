from __future__ import annotations

from pathlib import Path
import tomllib
from packaging.requirements import Requirement


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def shared_dependencies() -> dict[str, str]:
    pyproject = tomllib.loads((repo_root() / "pyproject.toml").read_text(encoding="utf-8"))
    shared: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        if dependency.startswith("asset-allocation-"):
            shared[Requirement(dependency).name] = dependency
    return shared


def test_pyproject_declares_shared_package_specs() -> None:
    shared = shared_dependencies()
    assert shared["asset-allocation-contracts"]
    assert shared["asset-allocation-runtime-common"]


def test_runtime_requirement_manifests_exclude_first_party_shared_packages() -> None:
    requirements = (repo_root() / "requirements.txt").read_text(encoding="utf-8")
    lockfile = (repo_root() / "requirements.lock.txt").read_text(encoding="utf-8")
    assert "asset-allocation-contracts" not in requirements
    assert "asset-allocation-contracts" not in lockfile
    assert "asset-allocation-runtime-common" not in requirements
    assert "asset-allocation-runtime-common" not in lockfile


def test_jobs_dockerfile_does_not_copy_sibling_repos() -> None:
    text = (repo_root() / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY asset-allocation-contracts/" not in text
    assert "COPY asset-allocation-runtime-common/" not in text
    assert '"asset-allocation-contracts==${CONTRACTS_VERSION}"' in text
    assert '"asset-allocation-runtime-common==${RUNTIME_COMMON_VERSION}"' in text


def test_quality_and_release_workflows_do_not_checkout_sibling_repos() -> None:
    for name in ("quality.yml", "release.yml"):
        text = (repo_root() / ".github" / "workflows" / name).read_text(encoding="utf-8")
        assert "Checkout contracts repository" not in text
        assert "Checkout runtime-common repository" not in text

    quality = (repo_root() / ".github" / "workflows" / "quality.yml").read_text(encoding="utf-8")
    release = (repo_root() / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    for text in (quality, release):
        assert "scripts/workflows/resolve_shared_versions.py" in text
        assert "setup-python-jobs" in text
        assert '${{ steps.shared.outputs.contracts_version }}' in text
        assert '${{ steps.shared.outputs.runtime_common_version }}' in text


def test_integration_workflow_is_the_only_place_cross_repo_checkout_for_contracts_is_allowed() -> None:
    integration = (repo_root() / ".github" / "workflows" / "integration.yml").read_text(encoding="utf-8")
    assert "Checkout control-plane repository" in integration
    assert "Checkout runtime-common repository" in integration
    assert "Checkout contracts repository" in integration
    assert "contracts_released" in integration
    assert "requirements.lock.txt" in integration
    assert "git push origin HEAD:${{ steps.inputs.outputs.target_branch }}" not in integration
    assert "contracts-compat" in integration
