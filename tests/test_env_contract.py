from __future__ import annotations

import csv
import os
import re
import subprocess
from pathlib import Path


ALLOWED_CLASSES = {"deploy_var", "secret"}
ALLOWED_STORAGE = {"var", "secret"}
ALLOWED_SOURCES = {"deploy_config", "secret_store"}
ALLOWED_TEMPLATE_FLAGS = {"true", "false"}
WORKFLOW_VAR_PATTERN = re.compile(r"\bvars\.([A-Z][A-Z0-9_]+)\b")
WORKFLOW_SECRET_PATTERN = re.compile(r"\bsecrets\.([A-Z][A-Z0-9_]+)\b")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def contract_rows() -> list[dict[str, str]]:
    path = repo_root() / "docs" / "ops" / "env-contract.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def template_contract_rows() -> list[dict[str, str]]:
    return [row for row in contract_rows() if row["template"] == "true"]


def contract_map(rows: list[dict[str, str]] | None = None) -> dict[str, dict[str, str]]:
    selected_rows = contract_rows() if rows is None else rows
    return {row["name"]: row for row in selected_rows}


def env_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def workflow_refs(pattern: re.Pattern[str]) -> set[str]:
    refs: set[str] = set()
    for path in (repo_root() / ".github" / "workflows").glob("*.yml"):
        refs.update(pattern.findall(path.read_text(encoding="utf-8")))
    return refs


def powershell_exe() -> str:
    for candidate in ("pwsh", "powershell"):
        try:
            subprocess.run(
                [candidate, "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"],
                check=True,
                capture_output=True,
                text=True,
            )
            return candidate
        except Exception:
            continue
    raise AssertionError("PowerShell executable not found for setup-env dry-run test")


def powershell_script_command(script: Path, *args: str) -> list[str]:
    return [powershell_exe(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script), *args]


def test_contract_rows_are_well_formed() -> None:
    rows = contract_rows()
    names = [row["name"] for row in rows]
    assert len(names) == len(set(names))
    for row in rows:
        assert row["class"] in ALLOWED_CLASSES
        assert row["github_storage"] in ALLOWED_STORAGE
        assert row["source_of_truth"] in ALLOWED_SOURCES
        assert row["template"] in ALLOWED_TEMPLATE_FLAGS


def test_template_matches_contract_surface() -> None:
    assert env_keys(repo_root() / ".env.template") == set(contract_map(template_contract_rows()))


def test_non_template_rows_stay_out_of_env_template() -> None:
    non_template_names = {row["name"] for row in contract_rows() if row["template"] == "false"}
    assert env_keys(repo_root() / ".env.template").isdisjoint(non_template_names)


def test_workflow_refs_are_documented() -> None:
    contract = contract_map()
    for name in workflow_refs(WORKFLOW_VAR_PATTERN):
        assert name in contract
        assert contract[name]["github_storage"] == "var"

    for name in workflow_refs(WORKFLOW_SECRET_PATTERN):
        assert name in contract
        assert contract[name]["github_storage"] == "secret"


def test_sync_script_reads_repo_local_contract() -> None:
    text = (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8")
    assert 'Join-Path $repoRoot "docs\\ops\\env-contract.csv"' in text
    assert 'Join-Path $repoRoot ".env.web"' in text


def test_sync_script_dry_run_ignores_undocumented_env_keys(tmp_path: Path) -> None:
    root = repo_root()
    temp_repo = tmp_path / "repo"
    scripts_dir = temp_repo / "scripts"
    docs_ops_dir = temp_repo / "docs" / "ops"
    bin_dir = tmp_path / "bin"
    scripts_dir.mkdir(parents=True)
    docs_ops_dir.mkdir(parents=True)
    bin_dir.mkdir()

    (scripts_dir / "sync-all-to-github.ps1").write_text(
        (root / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (docs_ops_dir / "env-contract.csv").write_text(
        (root / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_repo / ".env.web").write_text(
        "\n".join(
            [
                "AZURE_CLIENT_ID=test-client-id",
                "ASSET_ALLOCATION_API_BASE_URL=http://asset-allocation-api",
                "ASSET_ALLOCATION_API_SCOPE=api://example/.default",
                "BEA_API_KEY=stale-key",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (bin_dir / "gh.cmd").write_text("@echo off\r\nexit /b 0\r\n", encoding="utf-8")

    completed = subprocess.run(
        powershell_script_command(scripts_dir / "sync-all-to-github.ps1", "-DryRun"),
        cwd=temp_repo,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(bin_dir) + ";" + os.environ.get("PATH", "")},
    )

    assert "Ignoring undocumented .env.web keys: BEA_API_KEY" in completed.stdout


def test_env_bootstrap_scripts_handle_control_plane_bootstrap_secrets() -> None:
    setup_text = (repo_root() / "scripts" / "setup-env.ps1").read_text(encoding="utf-8")
    sync_text = (repo_root() / "scripts" / "sync-all-to-github.ps1").read_text(encoding="utf-8")
    env_contract = (repo_root() / "docs" / "ops" / "env-contract.csv").read_text(encoding="utf-8")

    assert "Test-CanAutoDiscoverSecretValue" in setup_text
    assert '"ASSET_ALLOCATION_API_BASE_URL"' in setup_text
    assert '"ASSET_ALLOCATION_API_SCOPE"' in setup_text
    assert "asset-allocation-api" in setup_text
    assert "Get-ContainerAppFqdn" not in setup_text
    assert "ASSET_ALLOCATION_API_BASE_URL,deploy_var,var" in env_contract
    assert "ASSET_ALLOCATION_API_SCOPE" in sync_text


def test_setup_env_dry_run_reports_sources_without_prompting() -> None:
    script = repo_root() / "scripts" / "setup-env.ps1"
    completed = subprocess.run(
        powershell_script_command(script, "-DryRun"),
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout
    assert "source=azure" in stdout or "source=default" in stdout or "source=git" in stdout
    assert "prompt_required=" in stdout


def test_setup_env_dry_run_uses_template_defaults_without_marking_them_prompt_required(tmp_path: Path) -> None:
    script = repo_root() / "scripts" / "setup-env.ps1"
    env_file = tmp_path / ".env.web"
    completed = subprocess.run(
        powershell_script_command(script, "-DryRun", "-EnvFilePath", str(env_file)),
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout
    assert "CONTRACTS_REF=main [source=default; prompt_required=false]" in stdout
    assert (
        "ECONOMIC_CATALYST_HTTP_TIMEOUT_SECONDS=30 [source=default; prompt_required=false]" in stdout
    )


def test_setup_env_dry_run_still_marks_blank_default_values_prompt_required(tmp_path: Path) -> None:
    script = repo_root() / "scripts" / "setup-env.ps1"
    env_file = tmp_path / ".env.web"
    completed = subprocess.run(
        powershell_script_command(script, "-DryRun", "-EnvFilePath", str(env_file)),
        cwd=repo_root(),
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout
    assert "DEBUG_SYMBOLS= [source=default; prompt_required=true]" in stdout
