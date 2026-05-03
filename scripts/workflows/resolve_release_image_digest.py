from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import zipfile


API_BASE_URL = "https://api.github.com"
API_VERSION = "2022-11-28"
MANIFEST_NAME = "release-manifest.json"
USER_AGENT = "asset-allocation-jobs-release-resolver"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve the jobs image digest from the latest successful release workflow artifact."
    )
    parser.add_argument("--repo", required=True, help="Repository slug, for example owner/repo.")
    parser.add_argument("--branch", required=True, help="Branch whose release workflow should be inspected.")
    parser.add_argument("--workflow", default="release.yml", help="Workflow file name to inspect.")
    parser.add_argument("--artifact", default="jobs-release", help="Artifact name containing the release manifest.")
    parser.add_argument("--run-id", type=int, help="Specific successful release workflow run to verify.")
    parser.add_argument("--expected-digest", help="Expected image digest to verify against the release manifest.")
    parser.add_argument("--expected-git-sha", help="Expected release git SHA to verify against the release manifest.")
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
        return json.load(response)


def download_bytes(url: str, token: str) -> bytes:
    request = Request(url)
    for key, value in api_headers(token).items():
        request.add_unredirected_header(key, value)
    with urlopen(request) as response:
        return response.read()


def latest_successful_run(*, repo: str, branch: str, workflow: str, token: str) -> dict[str, Any]:
    query = urlencode({"branch": branch, "status": "completed", "per_page": 20})
    url = f"{API_BASE_URL}/repos/{repo}/actions/workflows/{quote(workflow)}/runs?{query}"
    payload = request_json(url, token)
    for run in payload.get("workflow_runs", []):
        if run.get("conclusion") == "success":
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
        if artifact.get("name") == artifact_name:
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
            return json.load(handle)


def resolve_release_image(
    *,
    repo: str,
    branch: str,
    workflow: str,
    artifact_name: str,
    token: str,
    run_id: int | None = None,
    expected_digest: str | None = None,
    expected_git_sha: str | None = None,
) -> dict[str, str]:
    run = (
        successful_run_by_id(repo=repo, run_id=run_id, token=token)
        if run_id is not None
        else latest_successful_run(repo=repo, branch=branch, workflow=workflow, token=token)
    )
    artifact = release_artifact(repo=repo, run_id=int(run["id"]), artifact_name=artifact_name, token=token)
    manifest = read_release_manifest(download_bytes(str(artifact["archive_download_url"]), token))

    image_digest = str(manifest.get("image_digest") or "").strip()
    if not image_digest:
        raise SystemExit(f"{MANIFEST_NAME} from run {run['id']} does not contain image_digest")
    if expected_digest and image_digest != expected_digest:
        raise SystemExit(f"{MANIFEST_NAME} image_digest does not match the requested dispatch digest")

    release_git_sha = str(manifest.get("git_sha") or run.get("head_sha") or "").strip()
    if expected_git_sha and release_git_sha != expected_git_sha:
        raise SystemExit(f"{MANIFEST_NAME} git_sha does not match the requested dispatch git SHA")

    return {
        "image_digest": image_digest,
        "image_source": "verified-dispatch-release" if run_id is not None else "latest-successful-release",
        "release_artifact": artifact_name,
        "release_branch": str(run.get("head_branch") or branch),
        "release_git_sha": release_git_sha,
        "release_run_id": str(run["id"]),
        "release_run_html_url": str(run.get("html_url") or ""),
    }


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
        token=require_token(args.token),
        run_id=args.run_id,
        expected_digest=args.expected_digest,
        expected_git_sha=args.expected_git_sha,
    )
    emit_outputs(outputs, args.github_output)


if __name__ == "__main__":
    main()
