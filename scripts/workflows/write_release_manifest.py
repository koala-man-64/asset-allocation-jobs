from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write the jobs release manifest.")
    parser.add_argument("--repo", required=True, help="Repository slug.")
    parser.add_argument("--git-sha", required=True, help="Git commit SHA.")
    parser.add_argument("--image-ref", required=True, help="Built image reference.")
    parser.add_argument("--image-digest", required=True, help="Published image digest.")
    parser.add_argument("--contracts-version", required=True, help="Pinned contracts package version.")
    parser.add_argument("--runtime-common-version", required=True, help="Pinned runtime-common package version.")
    parser.add_argument("--jobs-version", required=True, help="Jobs package version.")
    parser.add_argument("--output", required=True, help="Manifest file to write.")
    return parser.parse_args()


def build_manifest(
    *,
    repo: str,
    git_sha: str,
    image_ref: str,
    image_digest: str,
    contracts_version: str,
    runtime_common_version: str,
    jobs_version: str,
) -> dict[str, object]:
    return {
        "repo": repo,
        "git_sha": git_sha,
        "artifact_kind": "container-image",
        "artifact_ref": image_ref,
        "image_digest": image_digest,
        "version_matrix": {
            "contracts": contracts_version,
            "runtime_common": runtime_common_version,
            "control_plane": None,
            "jobs": jobs_version,
            "ui": None,
        },
    }


def write_release_manifest(output_path: Path, manifest: dict[str, object]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    manifest = build_manifest(
        repo=args.repo,
        git_sha=args.git_sha,
        image_ref=args.image_ref,
        image_digest=args.image_digest,
        contracts_version=args.contracts_version,
        runtime_common_version=args.runtime_common_version,
        jobs_version=args.jobs_version,
    )
    write_release_manifest(Path(args.output), manifest)


if __name__ == "__main__":
    main()
