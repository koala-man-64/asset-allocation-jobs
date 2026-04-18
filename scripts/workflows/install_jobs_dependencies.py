from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Install repo dependencies for workflow jobs.")
    parser.add_argument("--jobs-path", default=".", help="Path to the jobs repository checkout.")
    parser.add_argument(
        "--requirements",
        action="append",
        default=[],
        help="requirements file to install. May be provided more than once.",
    )
    parser.add_argument(
        "--include-dev-lockfile",
        action="store_true",
        help="Install requirements-dev.lock.txt from the jobs repository checkout.",
    )
    parser.add_argument(
        "--editable",
        action="append",
        default=[],
        help="Editable path to install. May be provided more than once.",
    )
    parser.add_argument(
        "--editable-no-deps",
        action="append",
        default=[],
        help="Editable path to install with --no-deps. May be provided more than once.",
    )
    parser.add_argument(
        "--pip-check",
        action="store_true",
        help="Run pip check after installation.",
    )
    return parser.parse_args()


def run(command: list[str]) -> None:
    subprocess.run(command, check=True)


def install_jobs_dependencies(
    *,
    jobs_path: Path,
    requirement_paths: list[Path],
    include_dev_lockfile: bool,
    editable_paths: list[Path],
    editable_no_deps_paths: list[Path],
    pip_check: bool,
) -> None:
    python_exe = sys.executable
    run([python_exe, "-m", "pip", "install", "--upgrade", "pip"])

    for requirement_path in requirement_paths:
        run([python_exe, "-m", "pip", "install", "-r", str(requirement_path)])

    if include_dev_lockfile:
        run([python_exe, "-m", "pip", "install", "-r", str(jobs_path / "requirements-dev.lock.txt")])

    for editable_path in editable_paths:
        run([python_exe, "-m", "pip", "install", "-e", str(editable_path)])

    for editable_path in editable_no_deps_paths:
        run([python_exe, "-m", "pip", "install", "-e", str(editable_path), "--no-deps"])

    if pip_check:
        run([python_exe, "-m", "pip", "check"])


def main() -> None:
    args = parse_args()
    install_jobs_dependencies(
        jobs_path=Path(args.jobs_path),
        requirement_paths=[Path(path) for path in args.requirements],
        include_dev_lockfile=args.include_dev_lockfile,
        editable_paths=[Path(path) for path in args.editable],
        editable_no_deps_paths=[Path(path) for path in args.editable_no_deps],
        pip_check=args.pip_check,
    )


if __name__ == "__main__":
    main()
