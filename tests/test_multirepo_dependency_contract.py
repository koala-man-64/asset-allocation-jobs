from __future__ import annotations

from importlib.metadata import distribution
from pathlib import Path
import tomllib


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def project_dependencies() -> dict[str, str]:
    pyproject = tomllib.loads((repo_root() / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        if "==" not in dependency:
            continue
        name, version = dependency.split("==", 1)
        dependencies[name] = version
    return dependencies


def shared_dependencies() -> dict[str, str]:
    return {name: version for name, version in project_dependencies().items() if name.startswith("asset-allocation-")}


def installed_exact_dependency_versions(package_name: str) -> dict[str, str]:
    versions: dict[str, str] = {}
    for requirement in distribution(package_name).requires or []:
        requirement_text = requirement.split(";", 1)[0].strip()
        if "==" not in requirement_text:
            continue
        name, version = requirement_text.split("==", 1)
        versions[name] = version
    return versions


def test_pyproject_pins_shared_packages() -> None:
    shared = shared_dependencies()
    assert shared["asset-allocation-contracts"]
    assert shared["asset-allocation-runtime-common"]


def test_python_dependency_manifests_stay_in_sync() -> None:
    shared = shared_dependencies()
    requirements = (repo_root() / "requirements.txt").read_text(encoding="utf-8")
    lockfile = (repo_root() / "requirements.lock.txt").read_text(encoding="utf-8")
    assert f"asset-allocation-contracts=={shared['asset-allocation-contracts']}" in requirements
    assert f"asset-allocation-contracts=={shared['asset-allocation-contracts']}" in lockfile
    assert f"asset-allocation-runtime-common=={shared['asset-allocation-runtime-common']}" in requirements
    assert f"asset-allocation-runtime-common=={shared['asset-allocation-runtime-common']}" in lockfile


def test_project_dependency_pins_stay_compatible_with_installed_shared_packages() -> None:
    project = project_dependencies()
    mismatches: dict[str, dict[str, tuple[str, str]]] = {}

    for package_name in ("asset-allocation-contracts", "asset-allocation-runtime-common"):
        overlaps = {
            name: (project[name], installed_version)
            for name, installed_version in installed_exact_dependency_versions(package_name).items()
            if name in project and project[name] != installed_version
        }
        if overlaps:
            mismatches[package_name] = overlaps

    assert mismatches == {}


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


def test_release_workflow_runs_from_successful_mainline_integration_or_manual_dispatch() -> None:
    release = (repo_root() / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "\n  workflow_run:\n" in release
    assert "\n      - Jobs Integration\n" in release
    assert "github.event.workflow_run.conclusion == 'success'" in release
    assert "github.event.workflow_run.head_branch == github.event.repository.default_branch" in release
    assert "github.event.workflow_run.head_sha" in release
    assert "steps.source.outputs.release_git_sha" in release
    assert "\n  push:\n" not in release


def test_integration_workflow_is_the_only_place_cross_repo_checkout_and_contract_adoption_are_allowed() -> None:
    integration = (repo_root() / ".github" / "workflows" / "integration.yml").read_text(encoding="utf-8")
    assert "Checkout control-plane repository" in integration
    assert "Checkout runtime-common repository" in integration
    assert "contracts_released" in integration
    assert "Validate shared dependency compatibility" in integration
    assert "scripts/workflows/validate_shared_dependency_compatibility.py" in integration
    assert "contents: write" in integration
    assert "requirements.lock.txt" in integration
    assert "git push origin HEAD:${{ steps.inputs.outputs.target_branch }}" in integration
