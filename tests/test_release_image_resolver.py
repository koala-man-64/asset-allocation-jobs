from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


DIGEST = "assetalloc.azurecr.io/asset-allocation-jobs@sha256:" + ("a" * 64)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_resolver() -> ModuleType:
    path = repo_root() / "scripts" / "workflows" / "resolve_release_image_digest.py"
    spec = importlib.util.spec_from_file_location("resolve_release_image_digest", path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Could not load resolver from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "artifact_kind": "container-image",
        "repo": "owner/asset-allocation-jobs",
        "git_sha": "release-sha",
        "artifact_ref": "assetalloc.azurecr.io/asset-allocation-jobs:release-sha",
        "image_digest": DIGEST,
        "release_run_id": "42",
        "release_run_attempt": "1",
        "created_at": "2026-05-04T12:00:00Z",
    }
    payload.update(overrides)
    return payload


def release_run(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": 42,
        "run_attempt": 1,
        "conclusion": "success",
        "head_branch": "main",
        "head_sha": "release-sha",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "html_url": "https://github.example/owner/asset-allocation-jobs/actions/runs/42",
    }
    payload.update(overrides)
    return payload


def validate(
    *,
    manifest_payload: dict[str, object] | None = None,
    run_payload: dict[str, object] | None = None,
    current_branch_sha: str = "release-sha",
    expected_digest: str | None = None,
    expected_git_sha: str | None = None,
    allow_rollback: bool = False,
    max_age_days: int = 14,
) -> dict[str, str]:
    module = load_resolver()
    return module.validate_manifest(
        manifest=manifest_payload or manifest(),
        run=run_payload or release_run(),
        repo="owner/asset-allocation-jobs",
        branch="main",
        current_branch_sha=current_branch_sha,
        image_repository="asset-allocation-jobs",
        expected_digest=expected_digest,
        expected_git_sha=expected_git_sha,
        allow_rollback=allow_rollback,
        max_age_days=max_age_days,
    )


def test_release_artifact_rejects_expired_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_resolver()

    monkeypatch.setattr(
        module,
        "request_json",
        lambda url, token: {
            "artifacts": [
                {
                    "name": "jobs-release",
                    "expired": True,
                    "archive_download_url": "https://github.example/artifact.zip",
                }
            ]
        },
    )

    with pytest.raises(SystemExit, match="has expired"):
        module.release_artifact(
            repo="owner/asset-allocation-jobs",
            run_id=42,
            artifact_name="jobs-release",
            token="token",
        )


def test_validate_manifest_rejects_wrong_repo() -> None:
    with pytest.raises(SystemExit, match="repo did not match"):
        validate(manifest_payload=manifest(repo="owner/other-repo"))


def test_validate_manifest_rejects_wrong_artifact_kind() -> None:
    with pytest.raises(SystemExit, match="container-image"):
        validate(manifest_payload=manifest(artifact_kind="sbom"))


def test_validate_manifest_rejects_digest_mismatch() -> None:
    with pytest.raises(SystemExit, match="image_digest does not match"):
        validate(expected_digest="assetalloc.azurecr.io/asset-allocation-jobs@sha256:" + ("b" * 64))


def test_validate_manifest_rejects_git_sha_mismatch() -> None:
    with pytest.raises(SystemExit, match="git_sha does not match"):
        validate(expected_git_sha="other-sha")


def test_validate_manifest_rejects_stale_main_release() -> None:
    with pytest.raises(SystemExit, match="does not match current main HEAD"):
        validate(current_branch_sha="newer-main-sha")


def test_validate_manifest_rejects_non_main_release() -> None:
    with pytest.raises(SystemExit, match="expected main"):
        validate(run_payload=release_run(head_branch="feature/stale"))


def test_validate_manifest_rejects_expired_rollback_release() -> None:
    stale_created_at = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
    with pytest.raises(SystemExit, match="older than 14 days"):
        validate(
            run_payload=release_run(created_at=stale_created_at),
            current_branch_sha="newer-main-sha",
            expected_digest=DIGEST,
            expected_git_sha="release-sha",
            allow_rollback=True,
        )


def test_validate_manifest_accepts_guarded_rollback() -> None:
    outputs = validate(
        current_branch_sha="newer-main-sha",
        expected_digest=DIGEST,
        expected_git_sha="release-sha",
        allow_rollback=True,
    )

    assert outputs["image_digest"] == DIGEST
    assert outputs["image_source"] == "guarded-rollback"
    assert outputs["release_run_id"] == "42"
