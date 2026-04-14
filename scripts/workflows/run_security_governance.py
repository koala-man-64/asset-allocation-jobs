from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys


IGNORED_DEV_VULN = "GHSA-5239-wwwm-4pmq"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dependency auditing and governance checks.")
    parser.add_argument("--artifacts-dir", default="artifacts", help="Directory to write generated reports into.")
    return parser.parse_args()


def run(command: list[str], *, allow_failure: bool = False) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, check=False, text=True)
    if not allow_failure and completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, command)
    return completed


def run_security_governance(artifacts_dir: Path) -> None:
    python_exe = sys.executable
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    run([python_exe, "-m", "pip", "install", "--upgrade", "pip"])
    run([python_exe, "-m", "pip", "install", "pip-audit"])
    run(
        [
            "pip-audit",
            "-r",
            "requirements.lock.txt",
            "--format",
            "json",
            "-o",
            str(artifacts_dir / "pip-audit-runtime.json"),
        ],
        allow_failure=True,
    )
    run(
        [
            "pip-audit",
            "-r",
            "requirements-dev.lock.txt",
            "--format",
            "json",
            "-o",
            str(artifacts_dir / "pip-audit-dev.json"),
        ],
        allow_failure=True,
    )
    run(["pip-audit", "--strict", "-r", "requirements.lock.txt"])
    run(["pip-audit", "--strict", "--ignore-vuln", IGNORED_DEV_VULN, "-r", "requirements-dev.lock.txt"])
    run(
        [
            python_exe,
            "scripts/dependency_governance.py",
            "check",
            "--report",
            str(artifacts_dir / "dependency_governance_report.json"),
        ]
    )


def main() -> None:
    args = parse_args()
    run_security_governance(Path(args.artifacts_dir))


if __name__ == "__main__":
    main()
