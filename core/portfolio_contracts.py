from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from types import ModuleType


_PUBLIC_NAMES = (
    "PORTFOLIO_WEIGHT_TOLERANCE",
    "FreshnessState",
    "PortfolioAccountingDepth",
    "PortfolioAccount",
    "PortfolioAccountDetailResponse",
    "PortfolioAccountListResponse",
    "PortfolioAccountRevision",
    "PortfolioAccountUpsertRequest",
    "PortfolioAllocationMode",
    "PortfolioAlert",
    "PortfolioAlertListResponse",
    "PortfolioAlertSeverity",
    "PortfolioAlertStatus",
    "PortfolioAssignment",
    "PortfolioAssignmentRequest",
    "PortfolioAssignmentStatus",
    "PortfolioCadenceMode",
    "PortfolioDataDomain",
    "PortfolioDefinition",
    "PortfolioDefinitionDetailResponse",
    "PortfolioHistoryPoint",
    "PortfolioHistoryResponse",
    "PortfolioLedgerEvent",
    "PortfolioLedgerEventPayload",
    "PortfolioListResponse",
    "PortfolioMode",
    "PortfolioPosition",
    "PortfolioPositionContributor",
    "PortfolioPositionListResponse",
    "PortfolioRevision",
    "PortfolioRebalanceApplyRequest",
    "PortfolioRebalancePreviewRequest",
    "PortfolioSleeveAllocation",
    "PortfolioSnapshot",
    "PortfolioStatus",
    "PortfolioUpsertRequest",
    "RebalanceProposal",
    "RebalanceTradeProposal",
    "StrategySliceAttribution",
    "StrategyVersionReference",
    "TradeSide",
    "FreshnessStatus",
)


def _load_owner_module() -> ModuleType:
    try:
        return importlib.import_module("asset_allocation_contracts.portfolio")
    except ModuleNotFoundError as exc:
        if exc.name != "asset_allocation_contracts.portfolio":
            raise

    candidate = (
        Path(__file__).resolve().parents[2]
        / "asset-allocation-contracts"
        / "python"
        / "asset_allocation_contracts"
        / "portfolio.py"
    )
    if not candidate.exists():
        raise

    spec = importlib.util.spec_from_file_location(
        "asset_allocation_contracts_portfolio_fallback",
        candidate,
    )
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError("asset_allocation_contracts.portfolio")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_OWNER = _load_owner_module()

for _name in _PUBLIC_NAMES:
    globals()[_name] = getattr(_OWNER, _name)

for _name in _PUBLIC_NAMES:
    _value = globals()[_name]
    _rebuild = getattr(_value, "model_rebuild", None)
    if callable(_rebuild):
        _rebuild(force=True, _types_namespace=dict(getattr(_OWNER, "__dict__", {})))

__all__ = list(_PUBLIC_NAMES)
