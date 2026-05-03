#!/usr/bin/env python3
"""Run repo quality gates with deterministic local tool resolution."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
CommandSpec = tuple[list[str], pathlib.Path]

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

BACKTESTING_RUNTIME_TESTS = [
    "tests/core/test_backtest_runtime.py",
    "tests/tasks/test_backtesting_worker.py",
]

CONTROL_PLANE_COMPAT_TESTS = [
    "tests/core/test_control_plane_transport.py",
    "tests/core/test_strategy_repository.py",
    "tests/core/test_ranking_repository.py",
    "tests/core/test_universe_repository.py",
    "tests/core/test_regime_repository.py",
    "tests/core/test_backtest_repository.py",
]

RUNTIME_COMMON_COMPAT_TESTS = [
    "tests/test_multirepo_dependency_contract.py",
    "tests/core/test_control_plane_transport.py",
]

REGIME_ROLLOUT_TESTS = [
    "tests/tasks/test_gold_regime_data.py",
    "tests/tasks/common/test_regime_publication.py",
    "tests/monitoring/test_system_health_staleness.py",
    "tests/test_workflow_runtime_ownership.py",
    "tests/test_workflow_scripts.py",
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


def build_commands(gate: str) -> list[CommandSpec]:
    python = resolve_python()
    gates: dict[str, list[CommandSpec]] = {
        "check-fast": [
            ([python, "-m", "ruff", "check", "."], REPO_ROOT),
            ([python, "-m", "pytest", "-q", *FAST_TESTS], REPO_ROOT),
        ],
        "lint-python": [([python, "-m", "ruff", "check", "."], REPO_ROOT)],
        "format-python": [([python, "-m", "ruff", "format", "."], REPO_ROOT)],
        "lint-fix-python": [([python, "-m", "ruff", "check", "--fix", "."], REPO_ROOT)],
        "test-fast": [([python, "-m", "pytest", "-q", *FAST_TESTS], REPO_ROOT)],
        "test-backtesting-runtime": [([python, "-m", "pytest", "-q", *BACKTESTING_RUNTIME_TESTS], REPO_ROOT)],
        "test-control-plane-compat": [([python, "-m", "pytest", "-q", *CONTROL_PLANE_COMPAT_TESTS], REPO_ROOT)],
        "test-runtime-common-compat": [([python, "-m", "pytest", "-q", *RUNTIME_COMMON_COMPAT_TESTS], REPO_ROOT)],
        "test-regime-rollout": [([python, "-m", "pytest", "-q", *REGIME_ROLLOUT_TESTS], REPO_ROOT)],
        "test-full": [([python, "-m", "pytest", "-q"], REPO_ROOT)],
    }
    if gate not in gates:
        available = ", ".join(sorted(gates))
        raise SystemExit(f"Unknown gate '{gate}'. Expected one of: {available}")
    return gates[gate]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("Usage: python3 scripts/run_quality_gate.py <gate>")
    for command, cwd in build_commands(argv[1]):
        exit_code = run(command, cwd)
        if exit_code != 0:
            return exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
