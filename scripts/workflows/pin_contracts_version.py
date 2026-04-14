from __future__ import annotations

import argparse
from pathlib import Path
import re


PATTERNS = {
    Path("pyproject.toml"): r'asset-allocation-contracts==[^"]+',
    Path("requirements.txt"): r"asset-allocation-contracts==[^\r\n]+",
    Path("requirements.lock.txt"): r"asset-allocation-contracts==[^\r\n]+",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pin the contracts package version across dependency manifests.")
    parser.add_argument("--repo-root", default=".", help="Repository root containing the dependency manifests.")
    parser.add_argument("--contracts-version", required=True, help="Contracts package version to pin.")
    return parser.parse_args()


def pin_contracts_version(*, repo_root: Path, contracts_version: str) -> None:
    for relative_path, pattern in PATTERNS.items():
        path = repo_root / relative_path
        text = path.read_text(encoding="utf-8")
        updated, replacements = re.subn(
            pattern,
            f"asset-allocation-contracts=={contracts_version}",
            text,
            count=1,
        )
        if replacements != 1:
            raise SystemExit(f"Expected exactly one contracts dependency in {path}.")
        path.write_text(updated, encoding="utf-8")


def main() -> None:
    args = parse_args()
    pin_contracts_version(repo_root=Path(args.repo_root), contracts_version=args.contracts_version)


if __name__ == "__main__":
    main()
