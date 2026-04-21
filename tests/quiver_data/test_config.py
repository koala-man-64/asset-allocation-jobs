from __future__ import annotations

import pytest

from tasks.quiver_data.config import QuiverDataConfig


def test_quiver_data_config_defaults_are_rollout_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUIVER_DATA_JOB_MODE", raising=False)
    monkeypatch.delenv("QUIVER_DATA_TICKER_BATCH_SIZE", raising=False)
    monkeypatch.delenv("QUIVER_DATA_HISTORICAL_BATCH_SIZE", raising=False)
    monkeypatch.delenv("QUIVER_DATA_SYMBOL_LIMIT", raising=False)
    monkeypatch.delenv("QUIVER_DATA_PAGE_SIZE", raising=False)
    monkeypatch.delenv("QUIVER_DATA_SEC13F_TODAY_ONLY", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    config = QuiverDataConfig.from_env()

    assert config.job_mode == "incremental"
    assert config.ticker_batch_size == 50
    assert config.historical_batch_size == 20
    assert config.symbol_limit == 500
    assert config.page_size == 100
    assert config.sec13f_today_only is True
    assert config.postgres_dsn is None


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("QUIVER_DATA_TICKER_BATCH_SIZE", "bad"),
        ("QUIVER_DATA_HISTORICAL_BATCH_SIZE", "bad"),
        ("QUIVER_DATA_SYMBOL_LIMIT", "bad"),
        ("QUIVER_DATA_PAGE_SIZE", "bad"),
    ],
)
def test_quiver_data_config_rejects_invalid_numeric_env(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        QuiverDataConfig.from_env()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("QUIVER_DATA_JOB_MODE", "bogus"),
    ],
)
def test_quiver_data_config_rejects_invalid_enum_env(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        QuiverDataConfig.from_env()
