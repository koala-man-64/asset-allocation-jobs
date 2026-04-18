from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
from types import ModuleType

import pytest


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_module(relative_path: str, module_name: str) -> ModuleType:
    path = repo_root() / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_install_jobs_dependencies_relies_on_requirement_files_and_editables(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/install_jobs_dependencies.py", "install_jobs_dependencies")
    commands: list[list[str]] = []
    monkeypatch.setattr(module, "run", lambda command: commands.append(list(command)))
    monkeypatch.setattr(module.sys, "executable", "/python")

    module.install_jobs_dependencies(
        jobs_path=tmp_path,
        requirement_paths=[Path("requirements.lock.txt")],
        include_dev_lockfile=True,
        editable_paths=[Path("asset-allocation-runtime-common/python")],
        editable_no_deps_paths=[Path("asset-allocation-jobs")],
        pip_check=True,
    )

    assert commands == [
        ["/python", "-m", "pip", "install", "--upgrade", "pip"],
        ["/python", "-m", "pip", "install", "-r", "requirements.lock.txt"],
        ["/python", "-m", "pip", "install", "-r", str(tmp_path / "requirements-dev.lock.txt")],
        ["/python", "-m", "pip", "install", "-e", str(Path("asset-allocation-runtime-common/python"))],
        ["/python", "-m", "pip", "install", "-e", str(Path("asset-allocation-jobs")), "--no-deps"],
        ["/python", "-m", "pip", "check"],
    ]


def test_pin_contracts_version_updates_dependency_manifests(tmp_path: Path) -> None:
    module = load_module("scripts/workflows/pin_contracts_version.py", "pin_contracts_version")
    (tmp_path / "pyproject.toml").write_text('asset-allocation-contracts==0.1.0"\n', encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("asset-allocation-contracts==0.1.0\n", encoding="utf-8")
    (tmp_path / "requirements.lock.txt").write_text("asset-allocation-contracts==0.1.0\n", encoding="utf-8")

    module.pin_contracts_version(repo_root=tmp_path, contracts_version="9.9.9")

    assert 'asset-allocation-contracts==9.9.9"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert "asset-allocation-contracts==9.9.9" in (tmp_path / "requirements.txt").read_text(encoding="utf-8")
    assert "asset-allocation-contracts==9.9.9" in (tmp_path / "requirements.lock.txt").read_text(encoding="utf-8")


def test_write_release_manifest_writes_expected_shape(tmp_path: Path) -> None:
    module = load_module("scripts/workflows/write_release_manifest.py", "write_release_manifest")
    manifest = module.build_manifest(
        repo="owner/repo",
        git_sha="abc123",
        image_ref="registry/image:tag",
        image_digest="registry/image@sha256:deadbeef",
        contracts_version="1.0.0",
        runtime_common_version="2.0.0",
        jobs_version="3.0.0",
    )
    output_path = tmp_path / "artifacts" / "release-manifest.json"

    module.write_release_manifest(output_path, manifest)

    written = output_path.read_text(encoding="utf-8")
    assert '"repo": "owner/repo"' in written
    assert '"image_digest": "registry/image@sha256:deadbeef"' in written
    assert '"contracts": "1.0.0"' in written


def test_read_release_manifest_extracts_manifest_from_artifact_zip(tmp_path: Path) -> None:
    module = load_module("scripts/workflows/resolve_release_image_digest.py", "resolve_release_image_digest")
    import zipfile

    artifact_path = tmp_path / "test-release-manifest.zip"
    with zipfile.ZipFile(artifact_path, "w") as archive:
        archive.writestr(
            "release-manifest.json",
            '{"image_digest": "registry/image@sha256:deadbeef", "git_sha": "abc123"}',
        )

    manifest = module.read_release_manifest(artifact_path.read_bytes())

    assert manifest["image_digest"] == "registry/image@sha256:deadbeef"
    assert manifest["git_sha"] == "abc123"


def test_download_bytes_uses_github_api_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/workflows/resolve_release_image_digest.py", "resolve_release_image_digest")
    seen_headers: dict[str, str] = {}
    seen_unredirected_headers: dict[str, str] = {}

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b"artifact-bytes"

    def fake_urlopen(request):
        seen_headers.update(request.headers)
        seen_unredirected_headers.update(request.unredirected_hdrs)
        return FakeResponse()

    monkeypatch.setattr(module, "urlopen", fake_urlopen)

    payload = module.download_bytes("https://github.example/artifacts/11.zip", "test-token")
    normalized_headers = {key.lower(): value for key, value in seen_headers.items()}
    normalized_unredirected_headers = {key.lower(): value for key, value in seen_unredirected_headers.items()}

    assert payload == b"artifact-bytes"
    assert normalized_headers == {}
    assert normalized_unredirected_headers["accept"] == "application/vnd.github+json"
    assert normalized_unredirected_headers["authorization"] == "Bearer test-token"
    assert normalized_unredirected_headers["x-github-api-version"] == module.API_VERSION
    assert normalized_unredirected_headers["user-agent"] == module.USER_AGENT


def test_resolve_release_image_uses_latest_successful_release_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/workflows/resolve_release_image_digest.py", "resolve_release_image_digest")

    def fake_request_json(url: str, token: str) -> dict[str, object]:
        assert token == "test-token"
        if "/actions/workflows/release.yml/runs" in url:
            return {
                "workflow_runs": [
                    {"id": 10, "conclusion": "failure", "head_branch": "main"},
                    {
                        "id": 11,
                        "conclusion": "success",
                        "head_branch": "main",
                        "head_sha": "def456",
                        "html_url": "https://github.example/runs/11",
                    },
                ]
            }
        if "/actions/runs/11/artifacts" in url:
            return {
                "artifacts": [
                    {
                        "name": "jobs-release",
                        "expired": False,
                        "archive_download_url": "https://github.example/artifacts/11.zip",
                    }
                ]
            }
        raise AssertionError(f"Unexpected GitHub API request: {url}")

    monkeypatch.setattr(module, "request_json", fake_request_json)

    import zipfile
    from io import BytesIO

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "release-manifest.json",
            '{"image_digest": "registry/image@sha256:feedbeef", "git_sha": "abc123"}',
        )
    monkeypatch.setattr(module, "download_bytes", lambda url, token: buffer.getvalue())

    outputs = module.resolve_release_image(
        repo="owner/repo",
        branch="main",
        workflow="release.yml",
        artifact_name="jobs-release",
        token="test-token",
    )

    assert outputs["image_digest"] == "registry/image@sha256:feedbeef"
    assert outputs["image_source"] == "latest-successful-release"
    assert outputs["release_branch"] == "main"
    assert outputs["release_git_sha"] == "abc123"
    assert outputs["release_run_id"] == "11"


def test_build_jobs_image_pushes_and_emits_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = load_module("scripts/workflows/build_jobs_image.py", "build_jobs_image")
    commands: list[list[str]] = []

    def fake_run(command, check, env=None):
        assert check is True
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setattr(module, "inspect_image_digest", lambda image_ref: f"{image_ref}@sha256:1234")

    output_path = tmp_path / "github_output.txt"
    outputs = module.build_jobs_image(
        dockerfile="Dockerfile",
        image_ref="registry/image:tag",
        contracts_version="1.0.0",
        runtime_common_version="2.0.0",
        context=".",
        pip_config_path="C:/tmp/pip.conf",
        push=True,
        github_output=str(output_path),
    )

    assert commands[0][:4] == ["docker", "build", "--file", "Dockerfile"]
    assert commands[1] == ["docker", "push", "registry/image:tag"]
    assert outputs["image_digest"] == "registry/image:tag@sha256:1234"
    assert "image_ref=registry/image:tag" in output_path.read_text(encoding="utf-8")
    assert "image_digest=registry/image:tag@sha256:1234" in output_path.read_text(encoding="utf-8")


def test_capture_current_job_images_uses_manifest_names(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = load_module("scripts/workflows/capture_current_job_images.py", "capture_current_job_images")
    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    (deploy_dir / "job_one.yaml").write_text("name: first-job\n", encoding="utf-8")
    (deploy_dir / "job_two.yaml").write_text("name: second-job\n", encoding="utf-8")

    def fake_run(command, check, capture_output, text):
        job_name = command[5]
        if job_name == "first-job":
            return subprocess.CompletedProcess(command, 0, stdout="image-one\n")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="missing")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    images = module.capture_current_job_images(deploy_dir=deploy_dir, resource_group="rg")

    assert images == {"first-job": "image-one", "second-job": ""}


def test_render_and_apply_manifests_renders_env_and_uses_update_or_create(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    rendered_dir = tmp_path / "rendered"
    (deploy_dir / "job_existing.yaml").write_text("name: existing-job\nimage: ${JOB_IMAGE}\n", encoding="utf-8")
    (deploy_dir / "job_new.yaml").write_text("name: new-job\nimage: ${JOB_IMAGE}\n", encoding="utf-8")

    exists_results = iter([True, False])
    commands: list[list[str]] = []
    monkeypatch.setattr(module, "manifest_exists", lambda **_: next(exists_results))
    monkeypatch.setattr(module.subprocess, "check_call", lambda command: commands.append(list(command)))

    module.render_and_apply_manifests(
        deploy_dir=deploy_dir,
        rendered_dir=rendered_dir,
        resource_group="rg",
        environment={"JOB_IMAGE": "registry/image@sha256:1234"},
    )

    assert "registry/image@sha256:1234" in (rendered_dir / "job_existing.yaml").read_text(encoding="utf-8")
    assert commands[0][3] == "update"
    assert commands[1][3] == "create"


def test_render_and_apply_manifests_supplies_repo_tag_defaults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    rendered_dir = tmp_path / "rendered"
    (deploy_dir / "job_tagged.yaml").write_text(
        "\n".join(
            [
                "name: tagged-job",
                "tags:",
                "  owner: ${RESOURCE_TAG_OWNER}",
                "  cost-center: ${RESOURCE_TAG_COST_CENTER}",
                "  workload: ${RESOURCE_TAG_WORKLOAD}",
                "  environment: ${RESOURCE_TAG_ENVIRONMENT}",
                "image: ${JOB_IMAGE}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    commands: list[list[str]] = []
    monkeypatch.setattr(module, "manifest_exists", lambda **_: False)
    monkeypatch.setattr(module.subprocess, "check_call", lambda command: commands.append(list(command)))

    module.render_and_apply_manifests(
        deploy_dir=deploy_dir,
        rendered_dir=rendered_dir,
        resource_group="rg",
        environment={
            "JOB_IMAGE": "registry/image@sha256:1234",
            "GITHUB_REPOSITORY_OWNER": "koala-man-64",
        },
    )

    rendered = (rendered_dir / "job_tagged.yaml").read_text(encoding="utf-8")
    assert "owner: koala-man-64" in rendered
    assert "cost-center: asset-allocation" in rendered
    assert "workload: asset-allocation-jobs" in rendered
    assert "environment: prod" in rendered
    assert commands[0][3] == "create"


def test_render_and_apply_manifests_fails_on_unresolved_placeholders(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    rendered_dir = tmp_path / "rendered"
    (deploy_dir / "job_broken.yaml").write_text(
        "name: broken-job\nsubscription: ${AZURE_SUBSCRIPTION_ID}\nimage: ${JOB_IMAGE}\n",
        encoding="utf-8",
    )

    commands: list[list[str]] = []
    monkeypatch.setattr(module, "manifest_exists", lambda **_: False)
    monkeypatch.setattr(module.subprocess, "check_call", lambda command: commands.append(list(command)))

    with pytest.raises(SystemExit, match="unresolved template variables: AZURE_SUBSCRIPTION_ID"):
        module.render_and_apply_manifests(
            deploy_dir=deploy_dir,
            rendered_dir=rendered_dir,
            resource_group="rg",
            environment={"JOB_IMAGE": "registry/image@sha256:1234"},
        )

    assert commands == []
    assert not (rendered_dir / "job_broken.yaml").exists()


def test_deploy_prod_workflow_does_not_define_ranking_override_env_vars() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "RANKING_STRATEGY_NAME" not in workflow_text
    assert "RANKING_START_DATE" not in workflow_text
    assert "RANKING_END_DATE" not in workflow_text


def test_deploy_prod_workflow_exports_subscription_id_for_manifest_rendering() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "AZURE_SUBSCRIPTION_ID: ${{ vars.AZURE_SUBSCRIPTION_ID }}" in workflow_text


def test_verify_deployed_job_images_detects_mismatch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = load_module("scripts/workflows/verify_deployed_job_images.py", "verify_deployed_job_images")
    rendered_dir = tmp_path / "rendered"
    rendered_dir.mkdir()
    (rendered_dir / "job_one.yaml").write_text("name: first-job\n", encoding="utf-8")

    monkeypatch.setattr(module, "query_job_image", lambda **_: "registry/image@sha256:wrong")

    with pytest.raises(SystemExit, match="image mismatch"):
        module.verify_deployed_job_images(
            rendered_dir=rendered_dir,
            resource_group="rg",
            expected_image="registry/image@sha256:expected",
        )


def test_run_security_governance_invokes_expected_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = load_module("scripts/workflows/run_security_governance.py", "run_security_governance")
    commands: list[list[str]] = []
    monkeypatch.setattr(module, "run", lambda command, allow_failure=False: commands.append(list(command)))

    module.run_security_governance(tmp_path / "artifacts")

    assert commands[0][:4] == [module.sys.executable, "-m", "pip", "install"]
    assert ["pip-audit", "--strict", "-r", "requirements.lock.txt"] in commands
    assert any("scripts/dependency_governance.py" in part for command in commands for part in command)


def test_trigger_job_uses_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/ops/trigger_job.py", "trigger_job")
    commands: list[list[str]] = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, check: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    resolved = module.start_job(
        job_key="silver_market",
        resource_group="rg",
        environment={"SILVER_MARKET_JOB": "custom-silver-market-job"},
    )

    assert resolved == "custom-silver-market-job"
    assert commands == [
        [
            "az",
            "containerapp",
            "job",
            "start",
            "--name",
            "custom-silver-market-job",
            "--resource-group",
            "rg",
        ]
    ]


def test_trigger_job_supports_backtest_reconcile_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/ops/trigger_job.py", "trigger_job_reconcile")
    commands: list[list[str]] = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, check: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    resolved = module.start_job(
        job_key="backtests-reconcile",
        resource_group="rg",
        environment={"BACKTEST_RECONCILE_JOB": "custom-backtests-reconcile-job"},
    )

    assert resolved == "custom-backtests-reconcile-job"
    assert commands == [
        [
            "az",
            "containerapp",
            "job",
            "start",
            "--name",
            "custom-backtests-reconcile-job",
            "--resource-group",
            "rg",
        ]
    ]
