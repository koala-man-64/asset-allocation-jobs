from __future__ import annotations

import argparse
from pathlib import Path
import subprocess


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify that rendered job manifests deployed the expected image.")
    parser.add_argument("--rendered-dir", required=True, help="Directory containing rendered manifests.")
    parser.add_argument("--resource-group", required=True, help="Azure resource group containing the jobs.")
    parser.add_argument("--expected-image", required=True, help="Expected container image digest or reference.")
    return parser.parse_args()


def parse_manifest_job_name(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("name: "):
            return line.split(":", 1)[1].strip().strip('"')
    raise SystemExit(f"Could not find a manifest name in {path}")


def query_job_image(*, job_name: str, resource_group: str) -> str:
    return subprocess.check_output(
        [
            "az",
            "containerapp",
            "job",
            "show",
            "--name",
            job_name,
            "--resource-group",
            resource_group,
            "--query",
            "properties.template.containers[0].image",
            "-o",
            "tsv",
        ],
        text=True,
    ).strip()


def verify_deployed_job_images(*, rendered_dir: Path, resource_group: str, expected_image: str) -> None:
    for manifest in sorted(rendered_dir.glob("job_*.yaml")):
        job_name = parse_manifest_job_name(manifest)
        actual_image = query_job_image(job_name=job_name, resource_group=resource_group)
        if actual_image != expected_image:
            raise SystemExit(f"{job_name} image mismatch: expected {expected_image}, found {actual_image}")


def main() -> None:
    args = parse_args()
    verify_deployed_job_images(
        rendered_dir=Path(args.rendered_dir),
        resource_group=args.resource_group,
        expected_image=args.expected_image,
    )


if __name__ == "__main__":
    main()
