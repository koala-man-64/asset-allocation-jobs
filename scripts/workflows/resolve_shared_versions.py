from __future__ import annotations

from pathlib import Path
import tomllib


def main() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    dependencies: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        if dependency.startswith("asset-allocation-"):
            name, version = dependency.split("==", 1)
            dependencies[name] = version

    print(f"contracts_version={dependencies['asset-allocation-contracts']}")
    print(f"runtime_common_version={dependencies['asset-allocation-runtime-common']}")
    print(f"jobs_version={pyproject['project']['version']}")


if __name__ == "__main__":
    main()
