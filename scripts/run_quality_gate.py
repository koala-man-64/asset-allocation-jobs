#!/usr/bin/env python3
"""Run repo quality gates with deterministic local tool resolution."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

FAST_TESTS = [
    "tests/test_env_contract.py",
    "tests/test_workflow_runtime_ownership.py",
    "tests/test_azure_provisioning_scripts.py",
    "tests/test_multirepo_dependency_contract.py",
    "tests/core/test_control_plane_transport.py",
    "tests/core/test_strategy_repository.py",
    "tests/core/test_ranking_repository.py",
    "tests/core/test_universe_repository.py",
    "tests/core/test_regime_repository.py",
    "tests/core/test_backtest_repository.py",
]


def resolve_python() -> str:
    candidates = [
        REPO_ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin") / ("python.exe" if os.name == "nt" else "python"),
        REPO_ROOT / "venv" / ("Scripts" if os.name == "nt" else "bin") / ("python.exe" if os.name == "nt" else "python"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def run(argv: list[str], cwd: pathlib.Path) -> int:
    completed = subprocess.run([str(part) for part in argv], cwd=str(cwd), check=False)
    return completed.returncode


def build_command(gate: str) -> tuple[list[str], pathlib.Path]:
    python = resolve_python()
    gates: dict[str, tuple[list[str], pathlib.Path]] = {
        "lint-python": ([python, "-m", "ruff", "check", "."], REPO_ROOT),
        "format-python": ([python, "-m", "ruff", "format", "."], REPO_ROOT),
        "lint-fix-python": ([python, "-m", "ruff", "check", "--fix", "."], REPO_ROOT),
        "test-fast": ([python, "-m", "pytest", "-q", *FAST_TESTS], REPO_ROOT),
        "test-full": ([python, "-m", "pytest", "-q"], REPO_ROOT),
    }
    if gate not in gates:
        available = ", ".join(sorted(gates))
        raise SystemExit(f"Unknown gate '{gate}'. Expected one of: {available}")
    return gates[gate]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("Usage: python3 scripts/run_quality_gate.py <gate>")
    command, cwd = build_command(argv[1])
    return run(command, cwd)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
