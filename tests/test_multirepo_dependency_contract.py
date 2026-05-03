from __future__ import annotations

import inspect
import sys
from importlib.metadata import PackageNotFoundError, distribution
from importlib.util import find_spec
from pathlib import Path
import tomllib

from scripts.workflows.validate_shared_dependency_compatibility import validate_repo_shared_dependency_compatibility


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


def package_pyproject_dependencies(package_name: str) -> list[str]:
    package_pyproject = repo_root().parent / package_name / "python" / "pyproject.toml"
    if not package_pyproject.exists():
        raise PackageNotFoundError(package_name)
    pyproject = tomllib.loads(package_pyproject.read_text(encoding="utf-8"))
    return list(pyproject["project"].get("dependencies", []))


def active_import_pyproject_dependencies(package_name: str) -> list[str] | None:
    module_name = package_name.replace("-", "_")
    spec = find_spec(module_name)
    if spec is None or spec.origin is None:
        return None
    pyproject_path = Path(spec.origin).resolve().parents[1] / "pyproject.toml"
    if not pyproject_path.exists():
        return None
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return list(pyproject["project"].get("dependencies", []))


def shared_dependencies() -> dict[str, str]:
    return {name: version for name, version in project_dependencies().items() if name.startswith("asset-allocation-")}


def dockerfile_build_arg_defaults() -> dict[str, str]:
    defaults: dict[str, str] = {}
    for line in (repo_root() / "Dockerfile").read_text(encoding="utf-8").splitlines():
        if not line.startswith("ARG ") or "=" not in line:
            continue
        name, value = line.removeprefix("ARG ").split("=", 1)
        defaults[name] = value
    return defaults


def installed_exact_dependency_versions(package_name: str) -> dict[str, str]:
    versions: dict[str, str] = {}
    requirements = active_import_pyproject_dependencies(package_name)
    if requirements is None:
        try:
            requirements = list(distribution(package_name).requires or [])
        except PackageNotFoundError:
            requirements = package_pyproject_dependencies(package_name)

    for requirement in requirements:
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


def test_project_dependency_pins_resolve_from_configured_package_index() -> None:
    validate_repo_shared_dependency_compatibility(repo_root=repo_root(), python_exe=sys.executable)


def test_runtime_common_backtest_persistence_contract_accepts_v7_data_quality_surface() -> None:
    from asset_allocation_runtime_common import backtest_results

    signature = inspect.signature(backtest_results.persist_backtest_results)

    assert "data_quality_event_rows" in signature.parameters
    assert backtest_results.BACKTEST_RESULTS_SCHEMA_VERSION == 7


def test_jobs_dockerfile_does_not_copy_sibling_repos() -> None:
    text = (repo_root() / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY asset-allocation-contracts/" not in text
    assert "COPY asset-allocation-runtime-common/" not in text
    assert '"asset-allocation-contracts==${CONTRACTS_VERSION}"' in text
    assert '"asset-allocation-runtime-common==${RUNTIME_COMMON_VERSION}"' in text


def test_jobs_dockerfile_shared_package_defaults_match_repo_pins() -> None:
    shared = shared_dependencies()
    defaults = dockerfile_build_arg_defaults()

    assert defaults["CONTRACTS_VERSION"] == shared["asset-allocation-contracts"]
    assert defaults["RUNTIME_COMMON_VERSION"] == shared["asset-allocation-runtime-common"]


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
    assert "scripts/workflows/validate_shared_dependency_compatibility.py --repo-root ." in quality


def test_release_workflow_runs_from_successful_mainline_quality_or_manual_dispatch() -> None:
    release = (repo_root() / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "\n  workflow_run:\n" in release
    assert "\n      - Jobs Quality\n" in release
    assert "github.event.workflow_run.conclusion == 'success'" in release
    assert "github.event.workflow_run.event == 'push'" in release
    assert "github.event.workflow_run.head_branch == github.event.repository.default_branch" in release
    assert "github.event.workflow_run.head_sha" in release
    assert "steps.source.outputs.release_git_sha" in release
    assert "\n  push:\n" not in release


def test_integration_workflow_has_been_retired() -> None:
    workflow_dir = repo_root() / ".github" / "workflows"
    assert not (workflow_dir / "integration.yml").exists()
    for workflow in workflow_dir.glob("*.yml"):
        text = workflow.read_text(encoding="utf-8")
        assert "Checkout control-plane repository" not in text
        assert "Checkout runtime-common repository" not in text
        assert "contracts_released" not in text
        assert "runtime_common_released" not in text
        assert "git push origin HEAD:${{ steps.inputs.outputs.target_branch }}" not in text
