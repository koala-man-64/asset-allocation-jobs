from __future__ import annotations

import pandas as pd

from asset_allocation_runtime_common.market_data import delta_core
from asset_allocation_contracts.paths import DataPaths
from tasks.market_data import gold_market_data as gold


def test_run_market_reconciliation_cutoff_store_path_sanitizes_index_artifacts(monkeypatch, tmp_path):
    class _FakeGoldClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_gold = _FakeGoldClient()
    monkeypatch.setattr(
        gold,
        "_resolve_gold_market_reconciliation_clients",
        lambda **_kwargs: (object(), fake_gold),
    )
    monkeypatch.setattr(gold, "collect_delta_market_symbols", lambda *, client, root_prefix: {"AAPL"})
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))
    monkeypatch.setattr(gold, "_load_gold_market_bucket", lambda _path, *, gold_container: None)

    captured: dict[str, object] = {}

    def _fake_store_delta(df: pd.DataFrame, container: str, path: str, mode: str = "overwrite") -> None:
        assert mode == "overwrite"
        captured["df"] = df.copy()
        captured["path"] = path
        captured["gold_container"] = container

    monkeypatch.setattr(delta_core, "store_delta", _fake_store_delta)

    def _fake_enforce_backfill_cutoff_on_bucket_tables(**kwargs):
        dirty = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-10")],
                "symbol": ["AAPL"],
                "close": [101.0],
                "__index_level_0__": [8],
            }
        )
        dirty.index = pd.Index([11])
        kwargs["store_table"](dirty, DataPaths.get_gold_market_bucket_path("A"))
        return type(
            "_Stats",
            (),
            {"tables_scanned": 1, "tables_rewritten": 1, "deleted_blobs": 0, "rows_dropped": 1, "errors": 0},
        )()

    monkeypatch.setattr(
        gold,
        "enforce_backfill_cutoff_on_bucket_tables",
        _fake_enforce_backfill_cutoff_on_bucket_tables,
    )

    gold._run_market_reconciliation(silver_container="silver", gold_container="gold")

    assert "__index_level_0__" not in captured["df"].columns
    assert isinstance(captured["df"].index, pd.RangeIndex)
    assert captured["df"].index.start == 0
    assert captured["df"].index.step == 1
