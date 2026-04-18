from __future__ import annotations

import re

import pandas as pd


_INDEX_ARTIFACT_COLUMN_NAMES = {
    "index",
    "level_0",
    "index_level_0",
}


def _normalize_for_artifact_detection(name: object) -> str:
    text = re.sub(r"[^0-9a-z]+", "_", str(name or "").strip().lower())
    return re.sub(r"_+", "_", text).strip("_")


def sanitize_delta_write_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    to_drop: list[object] = []
    for col in out.columns:
        normalized = _normalize_for_artifact_detection(col)
        if normalized in _INDEX_ARTIFACT_COLUMN_NAMES:
            to_drop.append(col)
            continue
        if normalized.startswith("unnamed_"):
            suffix = normalized[len("unnamed_") :]
            if suffix.replace("_", "").isdigit():
                to_drop.append(col)
                continue
        if normalized.startswith("index_level_"):
            suffix = normalized[len("index_level_") :]
            if suffix.replace("_", "").isdigit():
                to_drop.append(col)
                continue

    if to_drop:
        out = out.drop(columns=to_drop)
    return out.reset_index(drop=True)
