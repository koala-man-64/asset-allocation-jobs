from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture the currently deployed image for each job manifest.")
    parser.add_argument("--deploy-dir", default="deploy", help="Directory containing job manifests.")
    parser.add_argument("--resource-group", required=True, help="Azure resource group containing the jobs.")
    parser.add_argument("--output", required=True, help="Path to write the captured image map JSON.")
    return parser.parse_args()


def parse_manifest_job_name(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("name: "):
            return line.split(":", 1)[1].strip().strip('"')
    raise SystemExit(f"Could not find a manifest name in {path}")


def lookup_job_image(*, job_name: str, resource_group: str) -> str:
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
            "--query",
            "properties.template.containers[0].image",
            "-o",
            "tsv",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def capture_current_job_images(*, deploy_dir: Path, resource_group: str) -> dict[str, str]:
    images: dict[str, str] = {}
    for manifest in sorted(deploy_dir.glob("job_*.yaml")):
        job_name = parse_manifest_job_name(manifest)
        images[job_name] = lookup_job_image(job_name=job_name, resource_group=resource_group)
    return images


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    images = capture_current_job_images(
        deploy_dir=Path(args.deploy_dir),
        resource_group=args.resource_group,
    )
    output_path.write_text(json.dumps(images, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
