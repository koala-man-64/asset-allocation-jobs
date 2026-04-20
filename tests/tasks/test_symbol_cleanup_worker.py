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


def _work_item(*, work_id: str, symbol: str = "SPY", execution_name: str | None = None) -> SymbolCleanupWorkItem:
    return SymbolCleanupWorkItem(
        workId=work_id,
        runId=f"run-{work_id}",
        symbol=symbol,
        status="claimed",
        requestedFields=["sector_norm"],
        attemptCount=1,
        executionName=execution_name,
    )


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
    queue = [_work_item(work_id="work-1")]

    class _FakeRepo:
        def __init__(self, *, transport) -> None:
            self.transport = transport

        def claim_work(self, *, execution_name=None):
            if not queue:
                return None
            work = queue.pop(0)
            return work.model_copy(update={"executionName": execution_name})

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


def test_main_drains_multiple_queued_items_in_one_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.logger, "exception", lambda *args, **kwargs: None)

    queue = [_work_item(work_id="work-1"), _work_item(work_id="work-2", symbol="QQQ")]
    completed: list[str] = []

    class _FakeRepo:
        def __init__(self, *, transport) -> None:
            self.transport = transport

        def claim_work(self, *, execution_name=None):
            if not queue:
                return None
            work = queue.pop(0)
            return work.model_copy(update={"executionName": execution_name})

        def get_run(self, run_id: str):
            return SymbolCleanupRunSummary(runId=run_id, status="running", mode="fill_missing")

        def resolve_symbol_profile(self, payload):
            return SymbolEnrichmentResolveResponse(
                symbol=str(payload.symbol),
                profile=SymbolProfileValues(sector_norm="Technology"),
                model="gpt-5.4-mini",
                confidence=0.95,
                sourceFingerprint=f"fp-{payload.symbol}",
                warnings=[],
            )

        def complete_work(self, work_id: str, *, result=None):
            completed.append(work_id)

        def fail_work(self, work_id: str, *, error: str) -> None:
            raise AssertionError("fail_work should not run for successful items")

    monkeypatch.setattr(worker, "SymbolEnrichmentRepository", _FakeRepo)
    monkeypatch.setattr(
        worker,
        "load_symbol_cleanup_context",
        lambda _dsn, symbol: SymbolCleanupContext(
            provider_facts=_provider_facts(symbol=symbol, name=f"{symbol} Corp", assetType="Stock"),
            current_profile=SymbolProfileValues(),
            locked_fields=set(),
        ),
    )

    assert worker.main() == 0
    assert completed == ["work-1", "work-2"]


def test_main_continues_after_item_failure_and_returns_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.logger, "exception", lambda *args, **kwargs: None)

    queue = [_work_item(work_id="work-1"), _work_item(work_id="work-2", symbol="QQQ")]
    completed: list[str] = []
    failures: list[tuple[str, str]] = []

    class _FakeRepo:
        def __init__(self, *, transport) -> None:
            self.transport = transport

        def claim_work(self, *, execution_name=None):
            if not queue:
                return None
            work = queue.pop(0)
            return work.model_copy(update={"executionName": execution_name})

        def get_run(self, run_id: str):
            return SymbolCleanupRunSummary(runId=run_id, status="running", mode="fill_missing")

        def resolve_symbol_profile(self, payload):
            symbol = str(payload.symbol)
            if symbol == "SPY":
                return SymbolEnrichmentResolveResponse(
                    symbol=symbol,
                    profile=SymbolProfileValues(is_etf=False),
                    model="gpt-5.4-mini",
                    confidence=0.95,
                    sourceFingerprint=f"fp-{symbol}",
                    warnings=[],
                )
            return SymbolEnrichmentResolveResponse(
                symbol=symbol,
                profile=SymbolProfileValues(sector_norm="Technology"),
                model="gpt-5.4-mini",
                confidence=0.95,
                sourceFingerprint=f"fp-{symbol}",
                warnings=[],
            )

        def complete_work(self, work_id: str, *, result=None):
            completed.append(work_id)

        def fail_work(self, work_id: str, *, error: str) -> None:
            failures.append((work_id, error))

    monkeypatch.setattr(worker, "SymbolEnrichmentRepository", _FakeRepo)
    monkeypatch.setattr(
        worker,
        "load_symbol_cleanup_context",
        lambda _dsn, symbol: SymbolCleanupContext(
            provider_facts=_provider_facts(symbol=symbol, name=f"{symbol} Corp", assetType="ETF" if symbol == "SPY" else "Stock"),
            current_profile=SymbolProfileValues(),
            locked_fields=set(),
        ),
    )

    assert worker.main() == 1
    assert failures == [("work-1", "AI returned unsupported field 'is_etf' for symbol 'SPY'.")]
    assert completed == ["work-2"]


def test_main_stops_when_execution_budget_is_reached(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.logger, "exception", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, "_EXECUTION_BUDGET_SECONDS", 10.0)

    time_values = iter([0.0, 1.0, 12.0, 12.0])
    monkeypatch.setattr(worker.monotonic_time, "monotonic", lambda: next(time_values))

    queue = [_work_item(work_id="work-1"), _work_item(work_id="work-2", symbol="QQQ")]
    completed: list[str] = []
    lifecycle_events: list[str] = []

    class _FakeRepo:
        def __init__(self, *, transport) -> None:
            self.transport = transport

        def claim_work(self, *, execution_name=None):
            if not queue:
                return None
            work = queue.pop(0)
            return work.model_copy(update={"executionName": execution_name})

        def get_run(self, run_id: str):
            return SymbolCleanupRunSummary(runId=run_id, status="running", mode="fill_missing")

        def resolve_symbol_profile(self, payload):
            return SymbolEnrichmentResolveResponse(
                symbol=str(payload.symbol),
                profile=SymbolProfileValues(sector_norm="Technology"),
                model="gpt-5.4-mini",
                confidence=0.95,
                sourceFingerprint=f"fp-{payload.symbol}",
                warnings=[],
            )

        def complete_work(self, work_id: str, *, result=None):
            completed.append(work_id)

        def fail_work(self, work_id: str, *, error: str) -> None:
            raise AssertionError("fail_work should not run when the pass stops on budget")

    monkeypatch.setattr(worker, "SymbolEnrichmentRepository", _FakeRepo)
    monkeypatch.setattr(
        worker,
        "load_symbol_cleanup_context",
        lambda _dsn, symbol: SymbolCleanupContext(
            provider_facts=_provider_facts(symbol=symbol, name=f"{symbol} Corp", assetType="Stock"),
            current_profile=SymbolProfileValues(),
            locked_fields=set(),
        ),
    )
    monkeypatch.setattr(
        worker,
        "_log_lifecycle",
        lambda phase, **fields: lifecycle_events.append(f"{phase}:{fields}"),
    )

    assert worker.main() == 0
    assert completed == ["work-1"]
    assert len(queue) == 1
    assert any(event.startswith("budget_exhausted:") for event in lifecycle_events)
