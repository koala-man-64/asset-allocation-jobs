from __future__ import annotations

import pytest

from asset_allocation_contracts.symbol_enrichment import (
    SymbolCleanupRunSummary,
    SymbolCleanupWorkItem,
    SymbolEnrichmentResolveResponse,
    SymbolProfileValues,
    SymbolProviderFacts,
)
from core.symbol_cleanup_runtime import SymbolCleanupContext
from tasks.symbol_cleanup import worker


def _provider_facts(**overrides):
    payload = {
        "symbol": "SPY",
        "name": "SPDR S&P 500 ETF Trust",
        "description": "Exchange traded fund tracking the S&P 500.",
        "sector": "Financial Services",
        "industry": "Asset Management",
        "industry2": "Asset Management",
        "country": "US",
        "exchange": "NASDAQ",
        "assetType": "ETF",
        "ipoDate": None,
        "delistingDate": None,
        "status": "Active",
        "isOptionable": True,
        "sourceNasdaq": True,
        "sourceMassive": True,
        "sourceAlphaVantage": False,
    }
    payload.update(overrides)
    return SymbolProviderFacts.model_validate(payload)


class _FakeTransport:
    def close(self) -> None:
        return None


def test_main_returns_zero_when_no_work_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)

    class _FakeRepo:
        def __init__(self, *, transport) -> None:
            self.transport = transport

        def claim_work(self, *, execution_name=None):
            return None

    monkeypatch.setattr(worker, "SymbolEnrichmentRepository", _FakeRepo)

    assert worker.main() == 0


def test_process_work_item_merges_deterministic_and_ai_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    requested_payloads: list[object] = []

    class _FakeRepo:
        def get_run(self, run_id: str):
            assert run_id == "run-1"
            return SymbolCleanupRunSummary(runId="run-1", status="running", mode="fill_missing")

        def resolve_symbol_profile(self, payload):
            requested_payloads.append(payload)
            return SymbolEnrichmentResolveResponse(
                symbol="SPY",
                profile=SymbolProfileValues(
                    sector_norm="Financial Services",
                    industry_group_norm="Capital Markets",
                    industry_norm="Asset Management",
                    issuer_summary_short="Tracks the S&P 500 through a liquid ETF wrapper.",
                ),
                model="gpt-5.4-mini",
                confidence=0.96,
                sourceFingerprint="fp-123",
                warnings=[],
            )

    monkeypatch.setattr(
        worker,
        "load_symbol_cleanup_context",
        lambda _dsn, _symbol: SymbolCleanupContext(
            provider_facts=_provider_facts(),
            current_profile=SymbolProfileValues(),
            locked_fields=set(),
        ),
    )
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)

    result = worker.process_work_item(
        repo=_FakeRepo(),
        dsn="postgresql://test:test@localhost:5432/asset_allocation",
        work_id="work-1",
        run_id="run-1",
        symbol="SPY",
        requested_fields=[
            "security_type_norm",
            "exchange_mic",
            "is_etf",
            "sector_norm",
            "industry_group_norm",
            "industry_norm",
            "issuer_summary_short",
        ],
        execution_name="exec-1",
    )

    assert result is not None
    assert result.profile.security_type_norm == "etf"
    assert result.profile.exchange_mic == "XNAS"
    assert result.profile.is_etf is True
    assert result.profile.sector_norm == "Financial Services"
    assert requested_payloads[0].requestedFields == [
        "sector_norm",
        "industry_group_norm",
        "industry_norm",
        "issuer_summary_short",
    ]


def test_main_reports_failure_when_response_validation_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.logger, "exception", lambda *args, **kwargs: None)

    failures: list[tuple[str, str]] = []

    class _FakeRepo:
        def __init__(self, *, transport) -> None:
            self.transport = transport

        def claim_work(self, *, execution_name=None):
            return SymbolCleanupWorkItem(
                workId="work-1",
                runId="run-1",
                symbol="SPY",
                status="claimed",
                requestedFields=["sector_norm"],
                attemptCount=1,
                executionName=execution_name,
            )

        def get_run(self, run_id: str):
            return SymbolCleanupRunSummary(runId=run_id, status="running", mode="fill_missing")

        def resolve_symbol_profile(self, payload):
            return SymbolEnrichmentResolveResponse(
                symbol="SPY",
                profile=SymbolProfileValues(is_etf=False),
                model="gpt-5.4-mini",
                confidence=0.95,
                sourceFingerprint="fp-123",
                warnings=[],
            )

        def complete_work(self, work_id: str, *, result=None):
            raise AssertionError("complete_work should not run on invalid AI output")

        def fail_work(self, work_id: str, *, error: str) -> None:
            failures.append((work_id, error))

    monkeypatch.setattr(worker, "SymbolEnrichmentRepository", _FakeRepo)
    monkeypatch.setattr(
        worker,
        "load_symbol_cleanup_context",
        lambda _dsn, _symbol: SymbolCleanupContext(
            provider_facts=_provider_facts(),
            current_profile=SymbolProfileValues(),
            locked_fields=set(),
        ),
    )

    assert worker.main() == 1
    assert failures == [("work-1", "AI returned unsupported field 'is_etf' for symbol 'SPY'.")]
