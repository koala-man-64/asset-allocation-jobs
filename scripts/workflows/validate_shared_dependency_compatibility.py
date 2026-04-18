from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import tomllib


REQUIRED_SHARED_PACKAGES = (
    "asset-allocation-contracts",
    "asset-allocation-runtime-common",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that the pinned shared packages resolve together from the configured package indexes."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root containing pyproject.toml.")
    parser.add_argument("--contracts-version", help="Override the contracts version to validate.")
    parser.add_argument("--runtime-common-version", help="Override the runtime-common version to validate.")
    return parser.parse_args()


def read_shared_dependency_versions(pyproject_path: Path) -> dict[str, str]:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    versions: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        if dependency.startswith("asset-allocation-"):
            name, version = dependency.split("==", 1)
            versions[name] = version

    missing = [name for name in REQUIRED_SHARED_PACKAGES if name not in versions]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"Missing shared dependency pins in {pyproject_path}: {missing_text}")

    return versions


def validate_shared_dependency_compatibility(
    *,
    python_exe: str,
    contracts_version: str,
    runtime_common_version: str,
) -> None:
    command = [
        python_exe,
        "-m",
        "pip",
        "install",
        "--dry-run",
        "--ignore-installed",
        f"asset-allocation-contracts=={contracts_version}",
        f"asset-allocation-runtime-common=={runtime_common_version}",
    ]
    completed = subprocess.run(command, check=False, text=True, capture_output=True)
    if completed.returncode == 0:
        return

    details = completed.stderr.strip() or completed.stdout.strip() or "pip reported a dependency resolution failure."
    raise SystemExit(
        "Shared package pins do not resolve together:\n"
        f"- asset-allocation-contracts=={contracts_version}\n"
        f"- asset-allocation-runtime-common=={runtime_common_version}\n\n"
        f"{details}"
    )


def validate_repo_shared_dependency_compatibility(
    *,
    repo_root: Path,
    python_exe: str,
    contracts_version: str | None = None,
    runtime_common_version: str | None = None,
) -> tuple[str, str]:
    versions = read_shared_dependency_versions(repo_root / "pyproject.toml")
    resolved_contracts_version = contracts_version or versions["asset-allocation-contracts"]
    resolved_runtime_common_version = runtime_common_version or versions["asset-allocation-runtime-common"]
    validate_shared_dependency_compatibility(
        python_exe=python_exe,
        contracts_version=resolved_contracts_version,
        runtime_common_version=resolved_runtime_common_version,
    )
    return resolved_contracts_version, resolved_runtime_common_version


def main() -> None:
    args = parse_args()
    contracts_version, runtime_common_version = validate_repo_shared_dependency_compatibility(
        repo_root=Path(args.repo_root),
        python_exe=sys.executable,
        contracts_version=args.contracts_version,
        runtime_common_version=args.runtime_common_version,
    )
    print(
        "Shared package pins resolve: "
        f"asset-allocation-contracts=={contracts_version}, "
        f"asset-allocation-runtime-common=={runtime_common_version}"
    )


if __name__ == "__main__":
    main()
