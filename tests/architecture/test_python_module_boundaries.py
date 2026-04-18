from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _python_files_under(package: str) -> list[Path]:
    base = REPO_ROOT / package
    return sorted(
        path
        for path in base.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _task_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(
                alias.name
                for alias in node.names
                if alias.name.startswith("tasks.")
            )
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("tasks."):
            imports.append(node.module)
    return sorted(set(imports))


def _forbidden_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        imported_names = {alias.name for alias in node.names}
        if node.module == "core.pipeline" and "DataPaths" in imported_names:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> from asset_allocation_runtime_common.market_data.pipeline import DataPaths")
        legacy_regime_imports = sorted(
            {"DEFAULT_REGIME_MODEL_NAME", "RegimePolicy"} & imported_names
        )
        if node.module == "core.regime" and legacy_regime_imports:
            offenders.append(
                f"{path.relative_to(REPO_ROOT)} -> "
                f"from asset_allocation_runtime_common.domain.regime import {', '.join(legacy_regime_imports)}"
            )
    return offenders


def test_api_has_no_direct_tasks_imports() -> None:
    offenders: list[str] = []
    for path in _python_files_under("api"):
        imports = _task_imports(path)
        if imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {imports}")
    assert offenders == []


def test_monitoring_has_no_direct_tasks_imports() -> None:
    offenders: list[str] = []
    for path in _python_files_under("monitoring"):
        imports = _task_imports(path)
        if imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {imports}")
    assert offenders == []


def test_core_has_no_direct_tasks_imports() -> None:
    offenders: list[str] = []
    for path in _python_files_under("core"):
        imports = _task_imports(path)
        if imports:
            offenders.append(f"{path.relative_to(REPO_ROOT)} -> {imports}")
    assert offenders == []


def test_repo_has_no_legacy_contract_facade_imports() -> None:
    offenders: list[str] = []
    for package in ("core", "tasks", "tests"):
        for path in _python_files_under(package):
            offenders.extend(_forbidden_imports(path))
    assert offenders == []
