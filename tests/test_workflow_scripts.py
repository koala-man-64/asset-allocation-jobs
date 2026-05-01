from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import tomllib
from types import ModuleType

import pytest


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def repo_shared_versions() -> tuple[str, str]:
    pyproject = tomllib.loads((repo_root() / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        if dependency.startswith("asset-allocation-"):
            name, version = dependency.split("==", 1)
            dependencies[name] = version
    return dependencies["asset-allocation-contracts"], dependencies["asset-allocation-runtime-common"]


CURRENT_CONTRACTS_VERSION, CURRENT_RUNTIME_COMMON_VERSION = repo_shared_versions()


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


def test_render_and_apply_job_manifests_blocks_invalid_metadata() -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    rendered = """
location: East US
name: gold-regime-job
type: Microsoft.App/jobs
tags:
  job-category: data-pipeline
  job-key: regime
  job-role: publish
  trigger-owner: schedule
"""

    with pytest.raises(SystemExit, match="invalid job metadata tags"):
        module.ensure_manifest_metadata_valid(
            manifest_path=Path("deploy/job_gold_regime_data.yaml"),
            rendered_text=rendered,
        )


def test_validate_repo_shared_dependency_compatibility_uses_pyproject_pins(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module(
        "scripts/workflows/validate_shared_dependency_compatibility.py",
        "validate_shared_dependency_compatibility",
    )
    (tmp_path / "pyproject.toml").write_text(
        "\n".join(
            [
                "[project]",
                (
                    'dependencies = ['
                    f'"asset-allocation-contracts=={CURRENT_CONTRACTS_VERSION}", '
                    f'"asset-allocation-runtime-common=={CURRENT_RUNTIME_COMMON_VERSION}"'
                    "]"
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )
    commands: list[list[str]] = []

    def fake_run(command, check, text, capture_output):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    versions = module.validate_repo_shared_dependency_compatibility(repo_root=tmp_path, python_exe="/python")

    assert versions == (CURRENT_CONTRACTS_VERSION, CURRENT_RUNTIME_COMMON_VERSION)
    assert commands == [
        [
            "/python",
            "-m",
            "pip",
            "install",
            "--dry-run",
            "--ignore-installed",
            f"asset-allocation-contracts=={CURRENT_CONTRACTS_VERSION}",
            f"asset-allocation-runtime-common=={CURRENT_RUNTIME_COMMON_VERSION}",
        ]
    ]


def test_validate_shared_dependency_compatibility_surfaces_resolver_failure() -> None:
    module = load_module(
        "scripts/workflows/validate_shared_dependency_compatibility.py",
        "validate_shared_dependency_compatibility",
    )

    def fake_run(command, check, text, capture_output):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="ResolutionImpossible")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(module.subprocess, "run", fake_run)
    try:
        with pytest.raises(SystemExit, match="Shared package pins do not resolve together"):
            module.validate_shared_dependency_compatibility(
                python_exe="/python",
                contracts_version="2.0.0",
                runtime_common_version="2.1.0",
            )
    finally:
        monkeypatch.undo()


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


def test_quality_and_release_workflows_build_with_repo_local_docker_context() -> None:
    root = repo_root()
    for relative_path in (".github/workflows/quality.yml", ".github/workflows/release.yml"):
        text = (root / relative_path).read_text(encoding="utf-8")
        assert "--context\n            asset-allocation-jobs" in text, relative_path


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
    (deploy_dir / "job_existing.yaml").write_text(
        "name: bronze-market-job\ntags:\n  job-category: data-pipeline\n  job-key: market\n  job-role: load\n  trigger-owner: schedule\nimage: ${JOB_IMAGE}\n",
        encoding="utf-8",
    )
    (deploy_dir / "job_new.yaml").write_text(
        "name: silver-market-job\ntags:\n  job-category: data-pipeline\n  job-key: market\n  job-role: transform\n  trigger-owner: pipeline-chain\nimage: ${JOB_IMAGE}\n",
        encoding="utf-8",
    )

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
                "name: bronze-market-job",
                "tags:",
                "  owner: ${RESOURCE_TAG_OWNER}",
                "  cost-center: ${RESOURCE_TAG_COST_CENTER}",
                "  workload: ${RESOURCE_TAG_WORKLOAD}",
                "  environment: ${RESOURCE_TAG_ENVIRONMENT}",
                "  job-category: data-pipeline",
                "  job-key: market",
                "  job-role: load",
                "  trigger-owner: schedule",
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


def test_render_and_apply_manifests_uses_env_template_defaults_when_env_is_blank(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    env_template = tmp_path / ".env.template"
    env_template.write_text(
        "\n".join(
            [
                "AZURE_FOLDER_ECONOMIC_CATALYST=economic-catalyst",
                "SILVER_ECONOMIC_CATALYST_JOB=silver-economic-catalyst-job",
                "JOB_IMAGE=template-image",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "DEFAULT_ENV_TEMPLATE_PATH", env_template)

    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    rendered_dir = tmp_path / "rendered"
    (deploy_dir / "job_defaults.yaml").write_text(
        "\n".join(
            [
                "name: silver-economic-catalyst-job",
                "tags:",
                "  job-category: data-pipeline",
                "  job-key: economic-catalyst",
                "  job-role: transform",
                "  trigger-owner: pipeline-chain",
                "folder: ${AZURE_FOLDER_ECONOMIC_CATALYST}",
                "nextJob: ${SILVER_ECONOMIC_CATALYST_JOB}",
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
            "AZURE_FOLDER_ECONOMIC_CATALYST": "",
            "SILVER_ECONOMIC_CATALYST_JOB": "",
            "JOB_IMAGE": "registry/image@sha256:1234",
        },
    )

    rendered = (rendered_dir / "job_defaults.yaml").read_text(encoding="utf-8")
    assert "folder: economic-catalyst" in rendered
    assert "nextJob: silver-economic-catalyst-job" in rendered
    assert "image: registry/image@sha256:1234" in rendered
    assert commands[0][3] == "create"


def test_render_and_apply_manifests_fails_on_unresolved_placeholders(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    monkeypatch.setattr(module, "DEFAULT_ENV_TEMPLATE_PATH", tmp_path / "missing.env.template")
    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    rendered_dir = tmp_path / "rendered"
    (deploy_dir / "job_broken.yaml").write_text(
        "name: broken-job\nsubscription: ${TOTALLY_UNKNOWN_PLACEHOLDER}\nimage: ${JOB_IMAGE}\n",
        encoding="utf-8",
    )

    commands: list[list[str]] = []
    monkeypatch.setattr(module, "manifest_exists", lambda **_: False)
    monkeypatch.setattr(module.subprocess, "check_call", lambda command: commands.append(list(command)))

    with pytest.raises(SystemExit, match="unresolved template variables: TOTALLY_UNKNOWN_PLACEHOLDER"):
        module.render_and_apply_manifests(
            deploy_dir=deploy_dir,
            rendered_dir=rendered_dir,
            resource_group="rg",
            environment={"JOB_IMAGE": "registry/image@sha256:1234"},
        )

    assert commands == []
    assert not (rendered_dir / "job_broken.yaml").exists()


def test_render_and_apply_manifests_fails_on_blank_secret_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")
    env_template = tmp_path / ".env.template"
    env_template.write_text("FRED_API_KEY=\n", encoding="utf-8")
    monkeypatch.setattr(module, "DEFAULT_ENV_TEMPLATE_PATH", env_template)

    deploy_dir = tmp_path / "deploy"
    deploy_dir.mkdir()
    rendered_dir = tmp_path / "rendered"
    (deploy_dir / "job_secret.yaml").write_text(
        "\n".join(
            [
                "name: secret-job",
                "properties:",
                "  configuration:",
                "    secrets:",
                "    - name: fred-api-key",
                "      value: ${FRED_API_KEY}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    commands: list[list[str]] = []
    monkeypatch.setattr(module, "manifest_exists", lambda **_: False)
    monkeypatch.setattr(module.subprocess, "check_call", lambda command: commands.append(list(command)))

    with pytest.raises(SystemExit, match="secret variables resolved to empty values: FRED_API_KEY"):
        module.render_and_apply_manifests(
            deploy_dir=deploy_dir,
            rendered_dir=rendered_dir,
            resource_group="rg",
            environment={},
        )

    assert commands == []
    assert not (rendered_dir / "job_secret.yaml").exists()


def test_render_and_apply_manifests_blocks_public_prod_control_plane_url() -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")

    with pytest.raises(SystemExit, match="public Azure Container Apps ingress host"):
        module.ensure_control_plane_base_url_policy(
            {
                "RESOURCE_TAG_ENVIRONMENT": "prod",
                "ASSET_ALLOCATION_API_BASE_URL": "https://asset-allocation-api.example.azurecontainerapps.io",
            }
        )


def test_render_and_apply_manifests_allows_public_control_plane_url_only_with_override() -> None:
    module = load_module("scripts/workflows/render_and_apply_job_manifests.py", "render_and_apply_job_manifests")

    module.ensure_control_plane_base_url_policy(
        {
            "RESOURCE_TAG_ENVIRONMENT": "prod",
            "ASSET_ALLOCATION_API_BASE_URL": "https://asset-allocation-api.example.azurecontainerapps.io",
            "ALLOW_PUBLIC_ASSET_ALLOCATION_API_BASE_URL": "true",
        }
    )


def test_deploy_prod_workflow_does_not_define_ranking_override_env_vars() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "RANKING_STRATEGY_NAME" not in workflow_text
    assert "RANKING_START_DATE" not in workflow_text
    assert "RANKING_END_DATE" not in workflow_text


def test_deploy_prod_workflow_exports_subscription_id_for_manifest_rendering() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "AZURE_SUBSCRIPTION_ID: ${{ vars.AZURE_SUBSCRIPTION_ID }}" in workflow_text


def test_deploy_prod_workflow_defaults_to_internal_api_var_not_public_secret() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "ASSET_ALLOCATION_API_BASE_URL: ${{ vars.ASSET_ALLOCATION_API_BASE_URL || 'http://asset-allocation-api' }}" in workflow_text
    assert "ASSET_ALLOCATION_API_BASE_URL: ${{ secrets.ASSET_ALLOCATION_API_BASE_URL }}" not in workflow_text
    assert "JOB_STARTUP_API_CONTAINER_APPS: ${{ vars.JOB_STARTUP_API_CONTAINER_APPS || 'asset-allocation-api' }}" in workflow_text


def test_deploy_prod_workflow_exports_economic_catalyst_secret_vars() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "FRED_API_KEY: ${{ secrets.FRED_API_KEY }}" in workflow_text
    assert "MASSIVE_API_KEY: ${{ secrets.MASSIVE_API_KEY }}" in workflow_text
    assert "ALPHA_VANTAGE_API_KEY: ${{ secrets.ALPHA_VANTAGE_API_KEY }}" in workflow_text
    assert "ALPACA_KEY_ID: ${{ secrets.ALPACA_KEY_ID }}" in workflow_text
    assert "ALPACA_SECRET_KEY: ${{ secrets.ALPACA_SECRET_KEY }}" in workflow_text


def test_deploy_prod_workflow_exports_bronze_runtime_safety_vars() -> None:
    workflow_text = (repo_root() / ".github" / "workflows" / "deploy-prod.yml").read_text(encoding="utf-8")

    assert "BRONZE_MARKET_ALPHA_VANTAGE_ENRICHMENT_ENABLED: ${{ vars.BRONZE_MARKET_ALPHA_VANTAGE_ENRICHMENT_ENABLED || 'false' }}" in workflow_text
    assert "ECONOMIC_CATALYST_VENDOR_SOURCES: ${{ vars.ECONOMIC_CATALYST_VENDOR_SOURCES || 'nasdaq_tables' }}" in workflow_text
    assert "ECONOMIC_CATALYST_GENERAL_POLL_MINUTES: ${{ vars.ECONOMIC_CATALYST_GENERAL_POLL_MINUTES || '30' }}" in workflow_text
    assert "QUIVER_DATA_ENABLED: ${{ vars.QUIVER_DATA_ENABLED || 'false' }}" in workflow_text
    assert "python scripts/workflows/verify_deployed_job_runtime.py" in workflow_text


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


def _runtime_manifest(*, image: str = "registry/image@sha256:expected") -> str:
    return f"""
location: East US
name: bronze-example-job
type: Microsoft.App/jobs
properties:
  configuration:
    triggerType: Schedule
    scheduleTriggerConfig:
      cronExpression: "*/30 * * * 1-5"
    replicaRetryLimit: 0
    replicaTimeout: 1800
    secrets:
    - name: pg-dsn
      value: rendered-secret-value
  template:
    containers:
    - image: {image}
      name: bronze-example-job
      env:
      - name: ASSET_ALLOCATION_API_BASE_URL
        value: http://asset-allocation-api-vnet
      - name: SAFE_FLAG
        value: "false"
      - name: SENSITIVE_LITERAL
        value: rendered-secret-like-value
      - name: POSTGRES_DSN
        secretRef: pg-dsn
"""


def _matching_live_runtime() -> dict:
    return {
        "properties": {
            "configuration": {
                "triggerType": "Schedule",
                "scheduleTriggerConfig": {"cronExpression": "*/30 * * * 1-5"},
                "replicaRetryLimit": 0,
                "replicaTimeout": 1800,
                "secrets": [{"name": "pg-dsn"}],
            },
            "template": {
                "containers": [
                    {
                        "image": "registry/image@sha256:expected",
                        "env": [
                            {"name": "ASSET_ALLOCATION_API_BASE_URL", "value": "http://asset-allocation-api-vnet"},
                            {"name": "SAFE_FLAG", "value": "false"},
                            {"name": "SENSITIVE_LITERAL", "value": "rendered-secret-like-value"},
                            {"name": "POSTGRES_DSN", "secretRef": "pg-dsn"},
                        ],
                    }
                ]
            },
        }
    }


def test_verify_deployed_job_runtime_accepts_matching_live_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/verify_deployed_job_runtime.py", "verify_deployed_job_runtime")
    rendered_dir = tmp_path / "rendered"
    rendered_dir.mkdir()
    (rendered_dir / "job_example.yaml").write_text(_runtime_manifest(), encoding="utf-8")
    monkeypatch.setattr(module, "query_job_runtime", lambda **_: _matching_live_runtime())

    module.verify_deployed_job_runtime(
        rendered_dir=rendered_dir,
        resource_group="rg",
        expected_image="registry/image@sha256:expected",
    )


def test_verify_deployed_job_runtime_detects_drift_without_printing_env_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_module("scripts/workflows/verify_deployed_job_runtime.py", "verify_deployed_job_runtime")
    rendered_dir = tmp_path / "rendered"
    rendered_dir.mkdir()
    (rendered_dir / "job_example.yaml").write_text(_runtime_manifest(), encoding="utf-8")
    live = _matching_live_runtime()
    live["properties"]["configuration"]["scheduleTriggerConfig"]["cronExpression"] = "*/5 * * * *"
    live["properties"]["configuration"]["replicaRetryLimit"] = 1
    live["properties"]["template"]["containers"][0]["image"] = "registry/image@sha256:wrong"
    live["properties"]["template"]["containers"][0]["env"] = [
        {"name": "ASSET_ALLOCATION_API_BASE_URL", "value": "https://public.example.azurecontainerapps.io"},
        {"name": "SAFE_FLAG", "value": "true"},
        {"name": "SENSITIVE_LITERAL", "value": "actual-secret-like-value"},
        {"name": "POSTGRES_DSN", "secretRef": "wrong-dsn"},
    ]
    monkeypatch.setattr(module, "query_job_runtime", lambda **_: live)

    with pytest.raises(SystemExit) as exc_info:
        module.verify_deployed_job_runtime(
            rendered_dir=rendered_dir,
            resource_group="rg",
            expected_image="registry/image@sha256:expected",
        )

    message = str(exc_info.value)
    assert "cronExpression mismatch" in message
    assert "replicaRetryLimit mismatch" in message
    assert "image mismatch" in message
    assert "env ASSET_ALLOCATION_API_BASE_URL value mismatch" in message
    assert "env SAFE_FLAG value mismatch" in message
    assert "env POSTGRES_DSN secretRef mismatch" in message
    assert "rendered-secret-like-value" not in message
    assert "actual-secret-like-value" not in message
    assert "https://public.example.azurecontainerapps.io" not in message


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


def test_trigger_job_supports_results_reconcile_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/ops/trigger_job.py", "trigger_job_results_reconcile")
    commands: list[list[str]] = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, check: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    resolved = module.start_job(
        job_key="results-reconcile",
        resource_group="rg",
        environment={"RESULTS_RECONCILE_JOB": "custom-results-reconcile-job"},
    )

    assert resolved == "custom-results-reconcile-job"
    assert commands == [
        [
            "az",
            "containerapp",
            "job",
            "start",
            "--name",
            "custom-results-reconcile-job",
            "--resource-group",
            "rg",
        ]
    ]


def test_trigger_job_supports_symbol_cleanup_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/ops/trigger_job.py", "trigger_job_symbol_cleanup")
    commands: list[list[str]] = []
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda command, check: commands.append(list(command)) or subprocess.CompletedProcess(command, 0),
    )

    resolved = module.start_job(
        job_key="symbol-cleanup",
        resource_group="rg",
        environment={"SYMBOL_CLEANUP_JOB": "custom-symbol-cleanup-job"},
    )

    assert resolved == "custom-symbol-cleanup-job"
    assert commands == [
        [
            "az",
            "containerapp",
            "job",
            "start",
            "--name",
            "custom-symbol-cleanup-job",
            "--resource-group",
            "rg",
        ]
    ]


def test_check_fast_gate_runs_ruff_before_fast_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/run_quality_gate.py", "run_quality_gate")
    commands: list[tuple[list[str], Path]] = []

    monkeypatch.setattr(module, "resolve_python", lambda: "/python")
    monkeypatch.setattr(module, "run", lambda command, cwd: commands.append((list(command), cwd)) or 0)

    exit_code = module.main(["scripts/run_quality_gate.py", "check-fast"])

    assert exit_code == 0
    assert commands == [
        (["/python", "-m", "ruff", "check", "."], module.REPO_ROOT),
        (["/python", "-m", "pytest", "-q", *module.FAST_TESTS], module.REPO_ROOT),
    ]


def test_check_fast_gate_stops_after_first_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module("scripts/run_quality_gate.py", "run_quality_gate_failure")
    commands: list[tuple[list[str], Path]] = []

    monkeypatch.setattr(module, "resolve_python", lambda: "/python")

    def fake_run(command: list[str], cwd: Path) -> int:
        commands.append((list(command), cwd))
        return 1 if command[:4] == ["/python", "-m", "ruff", "check"] else 0

    monkeypatch.setattr(module, "run", fake_run)

    exit_code = module.main(["scripts/run_quality_gate.py", "check-fast"])

    assert exit_code == 1
    assert commands == [
        (["/python", "-m", "ruff", "check", "."], module.REPO_ROOT),
    ]
