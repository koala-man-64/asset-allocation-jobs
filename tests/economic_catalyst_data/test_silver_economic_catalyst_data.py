from __future__ import annotations

from tasks.economic_catalyst_data import silver_economic_catalyst_data as silver
from tasks.economic_catalyst_data import storage


def test_silver_economic_catalyst_imports_storage_json_reader() -> None:
    assert silver.read_json_batches is storage.read_json_batches
