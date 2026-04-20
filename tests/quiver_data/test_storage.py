from __future__ import annotations

import pandas as pd

from tasks.quiver_data import constants
from tasks.quiver_data.storage import write_domain_artifact


def test_domain_layer_helpers_keep_gold_domain_distinct_from_bronze_and_silver() -> None:
    assert constants.domain_slug_for_layer("bronze") == "quiver-data"
    assert constants.domain_slug_for_layer("silver") == "quiver-data"
    assert constants.domain_slug_for_layer("gold") == "quiver"
    assert constants.domain_artifact_path_for_layer("gold") == "quiver/_metadata/domain.json"


def test_write_domain_artifact_uses_gold_root_path_for_gold_layer(monkeypatch) -> None:
    saved = {}

    monkeypatch.setattr(
        "tasks.quiver_data.storage.mdc.save_json_content",
        lambda data, file_path, client=None: saved.update({"path": str(file_path), "payload": dict(data)}),
    )

    payload = write_domain_artifact(
        client=object(),
        layer="gold",
        job_name="gold-quiver-data-job",
        run_id="run-123",
        tables={"government_contracts:A": pd.DataFrame([{"symbol": "PLTR"}])},
    )

    assert saved["path"] == "quiver/_metadata/domain.json"
    assert payload["domain"] == "quiver"
    assert payload["rootPath"] == "quiver"
    assert payload["artifactPath"] == "quiver/_metadata/domain.json"
