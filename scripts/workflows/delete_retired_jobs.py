from __future__ import annotations

import argparse
import subprocess


RETIRED_JOB_NAMES = (
    "bronze-quiver-data-job",
    "bronze-quiver-backfill-job",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete retired ACA Jobs after replacement jobs verify successfully.")
    parser.add_argument("--resource-group", required=True, help="Azure resource group containing the jobs.")
    return parser.parse_args()


def job_exists(*, job_name: str, resource_group: str) -> bool:
    completed = subprocess.run(
        [
            "az",
            "containerapp",
            "job",
            "show",
            "--name",
            job_name,
            "--resource-group",
            resource_group,
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return completed.returncode == 0


def delete_job(*, job_name: str, resource_group: str) -> None:
    subprocess.check_call(
        [
            "az",
            "containerapp",
            "job",
            "delete",
            "--name",
            job_name,
            "--resource-group",
            resource_group,
            "--yes",
        ]
    )


def delete_retired_jobs(*, resource_group: str) -> tuple[str, ...]:
    deleted: list[str] = []
    for job_name in RETIRED_JOB_NAMES:
        if not job_exists(job_name=job_name, resource_group=resource_group):
            print(f"Retired job not present, skipping: {job_name}")
            continue
        delete_job(job_name=job_name, resource_group=resource_group)
        print(f"Deleted retired job: {job_name}")
        deleted.append(job_name)
    return tuple(deleted)


def main() -> None:
    args = parse_args()
    delete_retired_jobs(resource_group=args.resource_group)


if __name__ == "__main__":
    main()
