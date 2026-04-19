from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess


PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
DEFAULT_ENV_TEMPLATE_PATH = Path(__file__).resolve().parents[2] / ".env.template"
DEFAULT_REPOSITORY_TAGS = {
    "RESOURCE_TAG_COST_CENTER": "asset-allocation",
    "RESOURCE_TAG_WORKLOAD": "asset-allocation-jobs",
    "RESOURCE_TAG_ENVIRONMENT": "prod",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render deploy/job_*.yaml manifests and apply them to ACA Jobs.")
    parser.add_argument("--deploy-dir", default="deploy", help="Directory containing source job manifests.")
    parser.add_argument("--rendered-dir", required=True, help="Directory to write rendered manifests into.")
    parser.add_argument("--resource-group", required=True, help="Azure resource group containing the jobs.")
    return parser.parse_args()


def parse_manifest_job_name(text: str, source: Path) -> str:
    for line in text.splitlines():
        if line.startswith("name: "):
            return line.split(":", 1)[1].strip().strip('"')
    raise SystemExit(f"Could not find a manifest name in {source}")


def render_manifest(template_text: str, environment: dict[str, str]) -> str:
    rendered = template_text
    for key, value in environment.items():
        rendered = rendered.replace("${" + key + "}", value)
    return rendered


def load_template_environment(template_path: Path | None = None) -> dict[str, str]:
    template_path = template_path or DEFAULT_ENV_TEMPLATE_PATH
    if not template_path.exists():
        return {}

    resolved: dict[str, str] = {}
    for raw_line in template_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        resolved[key.strip()] = value.strip()
    return resolved


def render_environment(environment: dict[str, str]) -> dict[str, str]:
    resolved = load_template_environment()
    for key, value in environment.items():
        if value:
            resolved[key] = value
            continue
        resolved.setdefault(key, value)

    for key, value in DEFAULT_REPOSITORY_TAGS.items():
        if not resolved.get(key):
            resolved[key] = value

    if not resolved.get("RESOURCE_TAG_OWNER"):
        owner = resolved.get("GITHUB_REPOSITORY_OWNER", "")
        if not owner:
            repository = resolved.get("GITHUB_REPOSITORY", "")
            owner = repository.partition("/")[0]
        resolved["RESOURCE_TAG_OWNER"] = owner or "asset-allocation"

    return resolved


def unresolved_placeholders(text: str) -> list[str]:
    return sorted({match.group(1) for match in PLACEHOLDER_PATTERN.finditer(text)})


def ensure_manifest_fully_rendered(*, manifest_path: Path, rendered_text: str) -> None:
    unresolved = unresolved_placeholders(rendered_text)
    if not unresolved:
        return
    missing = ", ".join(unresolved)
    raise SystemExit(
        f"Manifest {manifest_path} still contains unresolved template variables: {missing}. "
        "Export them in the deploy environment before applying manifests."
    )


def manifest_exists(*, job_name: str, resource_group: str) -> bool:
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


def render_and_apply_manifests(*, deploy_dir: Path, rendered_dir: Path, resource_group: str, environment: dict[str, str]) -> None:
    resolved_environment = render_environment(environment)
    rendered_dir.mkdir(parents=True, exist_ok=True)
    for manifest in sorted(deploy_dir.glob("job_*.yaml")):
        rendered = render_manifest(manifest.read_text(encoding="utf-8"), resolved_environment)
        ensure_manifest_fully_rendered(manifest_path=manifest, rendered_text=rendered)
        rendered_path = rendered_dir / manifest.name
        rendered_path.write_text(rendered, encoding="utf-8")

        job_name = parse_manifest_job_name(rendered, manifest)
        verb = "update" if manifest_exists(job_name=job_name, resource_group=resource_group) else "create"
        subprocess.check_call(
            [
                "az",
                "containerapp",
                "job",
                verb,
                "--name",
                job_name,
                "--resource-group",
                resource_group,
                "--yaml",
                str(rendered_path),
            ]
        )


def main() -> None:
    args = parse_args()
    render_and_apply_manifests(
        deploy_dir=Path(args.deploy_dir),
        rendered_dir=Path(args.rendered_dir),
        resource_group=args.resource_group,
        environment=dict(os.environ),
    )


if __name__ == "__main__":
    main()
