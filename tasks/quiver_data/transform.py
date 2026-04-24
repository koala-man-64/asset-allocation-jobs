from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from tasks.quiver_data import constants

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_RANGE_RE = re.compile(r"\$?\s*([\d,]+)(?:\s*-\s*\$?\s*([\d,]+))?")
_INTEGER_RE = re.compile(r"^[+-]?\d+$")


def normalize_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("%", " pct ").replace("$", " usd ").replace("/", " ").replace("-", " ")
    text = _NON_ALNUM_RE.sub("_", text)
    return text.strip("_")


def parse_timestamp(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            parsed = _parse_numeric_timestamp(float(value))
        elif _INTEGER_RE.fullmatch(text):
            parsed = _parse_numeric_timestamp(float(text))
        else:
            parsed = pd.to_datetime(text, utc=True, errors="raise")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


def _parse_numeric_timestamp(value: float) -> pd.Timestamp:
    abs_value = abs(value)
    if abs_value >= 10_000_000_000:
        return pd.to_datetime(value, unit="ms", utc=True, errors="raise")
    if abs_value >= 1_000_000_000:
        return pd.to_datetime(value, unit="s", utc=True, errors="raise")
    return pd.to_datetime(value, utc=True, errors="raise")


def range_midpoint(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = _RANGE_RE.search(text)
    if not match:
        try:
            return float(text.replace("$", "").replace(",", ""))
        except Exception:
            return None
    lower = float(match.group(1).replace(",", ""))
    upper_text = match.group(2)
    upper = float(upper_text.replace(",", "")) if upper_text else lower
    return (lower + upper) / 2.0


def extract_symbol(dataset_family: str, raw_row: dict[str, Any], *, requested_symbol: str | None = None) -> str | None:
    if requested_symbol:
        return str(requested_symbol).strip().upper() or None
    for field_name in constants.symbol_field_hints(dataset_family):
        value = raw_row.get(field_name)
        text = str(value or "").strip().upper()
        if text:
            return text
    return None


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _field_value(raw: dict[str, Any], snake: dict[str, Any], field_name: str) -> Any:
    if not field_name:
        return None
    value = raw.get(field_name)
    if value is not None:
        return value
    return snake.get(normalize_key(field_name))


def normalize_bronze_batch(batch: dict[str, Any]) -> pd.DataFrame:
    source_dataset = str(batch.get("source_dataset") or "").strip()
    dataset_family = str(batch.get("dataset_family") or "").strip()
    requested_symbol = str(batch.get("requested_symbol") or "").strip().upper() or None
    rows = batch.get("rows") or []
    ingested_at = parse_timestamp(batch.get("ingested_at")) or datetime.now(timezone.utc).isoformat()
    if not isinstance(rows, list):
        rows = []

    normalized_rows: list[dict[str, Any]] = []
    public_field = constants.public_availability_field(dataset_family)
    event_field = constants.event_time_field(dataset_family)

    for raw in rows:
        if not isinstance(raw, dict):
            continue
        snake = {normalize_key(key): value for key, value in raw.items()}
        symbol = extract_symbol(dataset_family, raw, requested_symbol=requested_symbol)
        bucket = constants.normalize_bucket(symbol)
        public_value = _field_value(raw, snake, public_field)
        event_value = (
            _field_value(raw, snake, event_field)
            or raw.get("Date")
            or raw.get("TransactionDate")
            or snake.get("date")
            or snake.get("transaction_date")
        )
        amount_source = raw.get("Range") or raw.get("Amount") or snake.get("range") or snake.get("amount")
        normalized = {
            "dataset_family": dataset_family,
            "source_dataset": source_dataset,
            "symbol": symbol,
            "bucket": bucket,
            "vendor_event_time": parse_timestamp(event_value),
            "public_availability_time": parse_timestamp(public_value) or parse_timestamp(event_value),
            "ingested_at": ingested_at,
            "amount_mid_usd": range_midpoint(amount_source),
            "source_hash": _stable_hash(raw),
            "vendor_payload_json": json.dumps(raw, sort_keys=True, default=str),
        }
        if dataset_family == "political_trading":
            normalized["chamber"] = str(
                snake.get("house")
                or ("senate" if "senate" in source_dataset else "house" if "house" in source_dataset else "congress")
            ).strip().lower()
        if dataset_family in {"government_contracts", "government_contracts_all"}:
            normalized["amount_numeric"] = range_midpoint(raw.get("Amount") or snake.get("amount"))
        normalized.update(snake)
        normalized_rows.append(normalized)

    frame = pd.DataFrame(normalized_rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "dataset_family",
                "source_dataset",
                "symbol",
                "bucket",
                "vendor_event_time",
                "public_availability_time",
                "ingested_at",
                "amount_mid_usd",
                "source_hash",
                "vendor_payload_json",
            ]
        )
    return frame.drop_duplicates(subset=["source_hash"], keep="last").reset_index(drop=True)


def merge_normalized_frames(existing: pd.DataFrame, new_frame: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new_frame.copy()
    if new_frame is None or new_frame.empty:
        return existing.copy()
    combined = pd.concat([existing, new_frame], ignore_index=True, sort=False)
    if "source_hash" in combined.columns:
        combined = combined.drop_duplicates(subset=["source_hash"], keep="last")
    return combined.reset_index(drop=True)


def feature_safe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    safe = frame.copy()
    for column in list(safe.columns):
        if column in constants.QUIVER_FORWARD_LOOKING_COLUMNS:
            safe = safe.drop(columns=[column])
    return safe


def _daily_base(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["as_of_date", "symbol"])
    working = frame.copy()
    working["public_availability_time"] = pd.to_datetime(working["public_availability_time"], utc=True, errors="coerce")
    working = working.dropna(subset=["symbol", "public_availability_time"])
    if working.empty:
        return pd.DataFrame(columns=["as_of_date", "symbol"])
    working["as_of_date"] = working["public_availability_time"].dt.date.astype(str)
    return working


def _series(frame: pd.DataFrame, column: str, default: object = "") -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _rolling_features(daily: pd.DataFrame, *, value_columns: list[str], windows: tuple[int, ...]) -> pd.DataFrame:
    if daily.empty:
        return daily
    frames: list[pd.DataFrame] = []
    daily = daily.sort_values(["symbol", "as_of_date"]).copy()
    daily["as_of_date"] = pd.to_datetime(daily["as_of_date"], utc=True)
    for symbol, symbol_frame in daily.groupby("symbol", sort=False):
        symbol_frame = symbol_frame.set_index("as_of_date").sort_index()
        out = pd.DataFrame(index=symbol_frame.index)
        out["symbol"] = symbol
        for column in value_columns:
            series = pd.to_numeric(symbol_frame[column], errors="coerce").fillna(0.0)
            for window in windows:
                out[f"{column}_{window}d"] = series.rolling(f"{window}D", min_periods=1).sum()
        frames.append(out.reset_index())
    merged = pd.concat(frames, ignore_index=True)
    merged["as_of_date"] = merged["as_of_date"].dt.date.astype(str)
    return merged


def build_insider_trading_features(frame: pd.DataFrame) -> pd.DataFrame:
    base = _daily_base(feature_safe_frame(frame))
    if base.empty:
        return pd.DataFrame(columns=["as_of_date", "symbol"])
    base["buy_count"] = (
        _series(base, "acquireddisposedcode").astype(str).str.upper().eq("A")
        | _series(base, "transactioncode").astype(str).str.upper().isin({"A", "P"})
    ).astype(int)
    base["sell_count"] = (
        _series(base, "acquireddisposedcode").astype(str).str.upper().eq("D")
        | _series(base, "transactioncode").astype(str).str.upper().isin({"D", "S"})
    ).astype(int)
    shares = pd.to_numeric(_series(base, "shares", 0.0), errors="coerce").fillna(0.0)
    price = pd.to_numeric(_series(base, "pricepershare", 0.0), errors="coerce").fillna(0.0)
    base["notional_proxy"] = shares * price
    daily = (
        base.groupby(["as_of_date", "symbol"], as_index=False)[["buy_count", "sell_count", "notional_proxy"]]
        .sum()
        .sort_values(["symbol", "as_of_date"])
    )
    return _rolling_features(daily, value_columns=["buy_count", "sell_count", "notional_proxy"], windows=(30, 90))


def build_institutional_holding_change_features(frame: pd.DataFrame) -> pd.DataFrame:
    base = _daily_base(feature_safe_frame(frame))
    if base.empty:
        return pd.DataFrame(columns=["as_of_date", "symbol"])
    base["breadth"] = 1
    base["net_change_pct"] = pd.to_numeric(_series(base, "change_pct", 0.0), errors="coerce").fillna(0.0)
    base["abs_change_pct"] = base["net_change_pct"].abs()
    base["held_normalized"] = pd.to_numeric(_series(base, "held_normalized", 0.0), errors="coerce").fillna(0.0)
    daily = (
        base.groupby(["as_of_date", "symbol"], as_index=False)[["breadth", "net_change_pct", "abs_change_pct", "held_normalized"]]
        .sum()
        .sort_values(["symbol", "as_of_date"])
    )
    return _rolling_features(
        daily,
        value_columns=["breadth", "net_change_pct", "abs_change_pct", "held_normalized"],
        windows=(30, 90),
    )


def build_political_trading_features(frame: pd.DataFrame) -> pd.DataFrame:
    base = _daily_base(feature_safe_frame(frame))
    if base.empty:
        return pd.DataFrame(columns=["as_of_date", "symbol"])
    transaction = _series(base, "transaction").astype(str).str.lower()
    base["purchase_count"] = transaction.str.contains("purchase|buy").fillna(False).astype(int)
    base["sale_count"] = transaction.str.contains("sale").fillna(False).astype(int)
    base["net_amount_proxy"] = pd.to_numeric(_series(base, "amount_mid_usd", 0.0), errors="coerce").fillna(0.0) * (
        base["purchase_count"] - base["sale_count"]
    )
    chamber = _series(base, "chamber").astype(str).str.lower()
    base["senate_count"] = chamber.eq("senate").astype(int)
    base["house_count"] = chamber.eq("house").astype(int)
    daily = (
        base.groupby(["as_of_date", "symbol"], as_index=False)[
            ["purchase_count", "sale_count", "net_amount_proxy", "senate_count", "house_count"]
        ]
        .sum()
        .sort_values(["symbol", "as_of_date"])
    )
    return _rolling_features(
        daily,
        value_columns=["purchase_count", "sale_count", "net_amount_proxy", "senate_count", "house_count"],
        windows=(30, 90),
    )


def build_government_contract_features(frame: pd.DataFrame) -> pd.DataFrame:
    base = _daily_base(feature_safe_frame(frame))
    if base.empty:
        return pd.DataFrame(columns=["as_of_date", "symbol"])
    amount = pd.to_numeric(_series(base, "amount_numeric", 0.0), errors="coerce").fillna(0.0)
    base["award_count"] = (amount > 0).astype(int)
    base["award_amount"] = amount
    base["large_award_flag"] = (amount >= 50_000_000).astype(int)
    daily = (
        base.groupby(["as_of_date", "symbol"], as_index=False)[["award_count", "award_amount", "large_award_flag"]]
        .sum()
        .sort_values(["symbol", "as_of_date"])
    )
    return _rolling_features(daily, value_columns=["award_count", "award_amount", "large_award_flag"], windows=(30, 90))


def bucket_rows(source_dataset: str, dataset_family: str, rows: list[dict[str, Any]], *, requested_symbol: str | None = None) -> dict[str, dict[str, Any]]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    batches: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = extract_symbol(dataset_family, row, requested_symbol=requested_symbol)
        bucket = constants.normalize_bucket(symbol)
        batch = batches.setdefault(
            bucket,
            {
                "version": 1,
                "source_dataset": source_dataset,
                "dataset_family": dataset_family,
                "requested_symbol": requested_symbol,
                "ingested_at": ingested_at,
                "rows": [],
            },
        )
        batch["rows"].append(row)
    return batches
