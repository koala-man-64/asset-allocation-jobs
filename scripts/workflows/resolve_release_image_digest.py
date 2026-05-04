from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import zipfile


API_BASE_URL = "https://api.github.com"
API_VERSION = "2022-11-28"
MANIFEST_NAME = "release-manifest.json"
USER_AGENT = "asset-allocation-jobs-release-resolver"
IMAGE_DIGEST_PATTERN = re.compile(
    r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve and verify the jobs release image digest.")
    parser.add_argument("--repo", required=True, help="Repository slug, for example owner/repo.")
    parser.add_argument("--branch", required=True, help="Branch whose release workflow should be inspected.")
    parser.add_argument("--workflow", default="release.yml", help="Workflow file name to inspect.")
    parser.add_argument("--artifact", default="jobs-release", help="Artifact name containing the release manifest.")
    parser.add_argument("--image-repository", default="asset-allocation-jobs", help="Expected image repository name.")
    parser.add_argument("--run-id", type=int, help="Specific successful release workflow run to verify.")
    parser.add_argument("--expected-digest", help="Expected image digest to verify against the release manifest.")
    parser.add_argument("--expected-git-sha", help="Expected release git SHA to verify against the release manifest.")
    parser.add_argument("--allow-rollback", action="store_true", help="Allow an older verified release artifact.")
    parser.add_argument("--max-age-days", type=int, default=14, help="Maximum rollback release age.")
    parser.add_argument("--github-output", help="Optional GitHub output file path.")
    parser.add_argument("--token", help="GitHub token. Defaults to GITHUB_TOKEN from the environment.")
    return parser.parse_args()


def require_token(token: str | None) -> str:
    resolved = token or os.getenv("GITHUB_TOKEN")
    if resolved:
        return resolved
    raise SystemExit("GitHub token is required via --token or GITHUB_TOKEN")


def api_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": API_VERSION,
    }


def request_json(url: str, token: str) -> dict[str, Any]:
    request = Request(url, headers=api_headers(token))
    with urlopen(request) as response:
        loaded = json.load(response)
    if not isinstance(loaded, dict):
        raise SystemExit(f"GitHub API returned non-object JSON for {url}")
    return loaded


def download_bytes(url: str, token: str) -> bytes:
    request = Request(url)
    for key, value in api_headers(token).items():
        request.add_unredirected_header(key, value)
    with urlopen(request) as response:
        return response.read()


def branch_head_sha(*, repo: str, branch: str, token: str) -> str:
    url = f"{API_BASE_URL}/repos/{repo}/git/ref/heads/{quote(branch, safe='')}"
    payload = request_json(url, token)
    ref_object = payload.get("object")
    if not isinstance(ref_object, dict):
        raise SystemExit(f"Could not resolve origin branch {branch}")
    sha = str(ref_object.get("sha") or "").strip()
    if not sha:
        raise SystemExit(f"GitHub ref heads/{branch} did not include a SHA")
    return sha


def latest_successful_run(*, repo: str, branch: str, workflow: str, token: str) -> dict[str, Any]:
    query = urlencode({"branch": branch, "status": "completed", "per_page": 20})
    url = f"{API_BASE_URL}/repos/{repo}/actions/workflows/{quote(workflow)}/runs?{query}"
    payload = request_json(url, token)
    for run in payload.get("workflow_runs", []):
        if isinstance(run, dict) and run.get("conclusion") == "success":
            return run
    raise SystemExit(f"No successful {workflow} workflow run found for branch {branch}")


def successful_run_by_id(*, repo: str, run_id: int, token: str) -> dict[str, Any]:
    url = f"{API_BASE_URL}/repos/{repo}/actions/runs/{run_id}"
    run = request_json(url, token)
    if run.get("conclusion") != "success":
        raise SystemExit(f"Workflow run {run_id} is not a successful release run")
    return run


def release_artifact(*, repo: str, run_id: int, artifact_name: str, token: str) -> dict[str, Any]:
    query = urlencode({"per_page": 100})
    url = f"{API_BASE_URL}/repos/{repo}/actions/runs/{run_id}/artifacts?{query}"
    payload = request_json(url, token)
    for artifact in payload.get("artifacts", []):
        if not isinstance(artifact, dict) or artifact.get("name") != artifact_name:
            continue
        if artifact.get("expired"):
            raise SystemExit(f"Artifact {artifact_name} from run {run_id} has expired")
        return artifact
    raise SystemExit(f"Artifact {artifact_name} not found on workflow run {run_id}")


def read_release_manifest(archive_bytes: bytes) -> dict[str, Any]:
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        manifest_paths = [name for name in archive.namelist() if Path(name).name == MANIFEST_NAME]
        if not manifest_paths:
            raise SystemExit(f"{MANIFEST_NAME} not found in release artifact")
        with archive.open(manifest_paths[0]) as handle:
            loaded = json.load(handle)
    if not isinstance(loaded, dict):
        raise SystemExit(f"{MANIFEST_NAME} did not contain a JSON object")
    return loaded


def parse_github_timestamp(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"{label} is not a valid ISO-8601 timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def require_manifest_fields(manifest: dict[str, Any]) -> None:
    required = {
        "artifact_kind",
        "repo",
        "git_sha",
        "artifact_ref",
        "image_digest",
        "release_run_id",
        "release_run_attempt",
        "created_at",
    }
    missing = sorted(name for name in required if not str(manifest.get(name) or "").strip())
    if missing:
        raise SystemExit(f"{MANIFEST_NAME} is missing required fields: {', '.join(missing)}")


def validate_image_digest(image_digest: str, *, image_repository: str) -> None:
    if not IMAGE_DIGEST_PATTERN.fullmatch(image_digest):
        raise SystemExit("release image_digest must be a fully-qualified image@sha256:<64 hex> reference")
    image_without_digest = image_digest.split("@", 1)[0]
    if not image_without_digest.endswith(f"/{image_repository}"):
        raise SystemExit(f"release image_digest does not point to repository {image_repository}")


def validate_manifest(
    *,
    manifest: dict[str, Any],
    run: dict[str, Any],
    repo: str,
    branch: str,
    current_branch_sha: str,
    image_repository: str,
    expected_digest: str | None,
    expected_git_sha: str | None,
    allow_rollback: bool,
    max_age_days: int,
) -> dict[str, str]:
    require_manifest_fields(manifest)

    if str(manifest["artifact_kind"]) != "container-image":
        raise SystemExit(f"{MANIFEST_NAME} did not describe a container-image artifact")
    if str(manifest["repo"]) != repo:
        raise SystemExit(f"{MANIFEST_NAME} repo did not match the current repository")

    run_id = str(run.get("id") or "").strip()
    run_attempt = str(run.get("run_attempt") or "").strip()
    if str(manifest["release_run_id"]) != run_id:
        raise SystemExit(f"{MANIFEST_NAME} release_run_id did not match the selected workflow run")
    if run_attempt and str(manifest["release_run_attempt"]) != run_attempt:
        raise SystemExit(f"{MANIFEST_NAME} release_run_attempt did not match the selected workflow run")

    release_branch = str(run.get("head_branch") or "").strip()
    if release_branch != branch:
        raise SystemExit(f"Selected release run is for branch {release_branch}, expected {branch}")

    release_git_sha = str(manifest["git_sha"]).strip()
    run_head_sha = str(run.get("head_sha") or "").strip()
    if run_head_sha and release_git_sha != run_head_sha:
        raise SystemExit(f"{MANIFEST_NAME} git_sha did not match the selected workflow run")
    if expected_git_sha and release_git_sha != expected_git_sha:
        raise SystemExit(f"{MANIFEST_NAME} git_sha does not match the requested git SHA")
    if not allow_rollback and release_git_sha != current_branch_sha:
        raise SystemExit(
            f"Latest successful release {release_git_sha} does not match current {branch} HEAD {current_branch_sha}"
        )

    created_at = str(manifest["created_at"]).strip()
    parse_github_timestamp(created_at, label=f"{MANIFEST_NAME} created_at")
    run_created_at = parse_github_timestamp(str(run.get("created_at") or ""), label="release run created_at")
    if allow_rollback and datetime.now(timezone.utc) - run_created_at > timedelta(days=max_age_days):
        raise SystemExit(f"Rollback release run {run_id} is older than {max_age_days} days")

    image_digest = str(manifest["image_digest"]).strip()
    validate_image_digest(image_digest, image_repository=image_repository)
    if expected_digest and image_digest != expected_digest:
        raise SystemExit(f"{MANIFEST_NAME} image_digest does not match the requested digest")

    artifact_ref = str(manifest["artifact_ref"]).strip()
    if not artifact_ref or "@" in artifact_ref:
        raise SystemExit(f"{MANIFEST_NAME} artifact_ref must be the release tag image reference")

    return {
        "image_digest": image_digest,
        "image_source": "guarded-rollback" if allow_rollback else "current-main-release",
        "release_artifact": MANIFEST_NAME,
        "release_branch": branch,
        "release_git_sha": release_git_sha,
        "release_run_id": run_id,
        "release_run_attempt": str(manifest["release_run_attempt"]),
        "release_created_at": created_at,
        "release_run_html_url": str(run.get("html_url") or ""),
    }


def resolve_release_image(
    *,
    repo: str,
    branch: str,
    workflow: str,
    artifact_name: str,
    token: str,
    image_repository: str = "asset-allocation-jobs",
    run_id: int | None = None,
    expected_digest: str | None = None,
    expected_git_sha: str | None = None,
    allow_rollback: bool = False,
    max_age_days: int = 14,
) -> dict[str, str]:
    if allow_rollback and not (run_id and expected_digest and expected_git_sha):
        raise SystemExit("Guarded rollback requires run_id, expected_digest, and expected_git_sha")

    current_branch_sha = branch_head_sha(repo=repo, branch=branch, token=token)
    run = (
        successful_run_by_id(repo=repo, run_id=run_id, token=token)
        if run_id is not None
        else latest_successful_run(repo=repo, branch=branch, workflow=workflow, token=token)
    )
    artifact = release_artifact(repo=repo, run_id=int(run["id"]), artifact_name=artifact_name, token=token)
    manifest = read_release_manifest(download_bytes(str(artifact["archive_download_url"]), token))
    return validate_manifest(
        manifest=manifest,
        run=run,
        repo=repo,
        branch=branch,
        current_branch_sha=current_branch_sha,
        image_repository=image_repository,
        expected_digest=expected_digest,
        expected_git_sha=expected_git_sha,
        allow_rollback=allow_rollback,
        max_age_days=max_age_days,
    )


def emit_outputs(outputs: dict[str, str], github_output: str | None) -> None:
    if github_output:
        output_path = Path(github_output)
        with output_path.open("a", encoding="utf-8") as handle:
            for key, value in outputs.items():
                handle.write(f"{key}={value}\n")
        return

    for key, value in outputs.items():
        print(f"{key}={value}")


def main() -> None:
    args = parse_args()
    outputs = resolve_release_image(
        repo=args.repo,
        branch=args.branch,
        workflow=args.workflow,
        artifact_name=args.artifact,
        image_repository=args.image_repository,
        token=require_token(args.token),
        run_id=args.run_id,
        expected_digest=args.expected_digest,
        expected_git_sha=args.expected_git_sha,
        allow_rollback=args.allow_rollback,
        max_age_days=args.max_age_days,
    )
    emit_outputs(outputs, args.github_output)


if __name__ == "__main__":
    main()
