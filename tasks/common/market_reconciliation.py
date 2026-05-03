from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence, Set

import pandas as pd

from asset_allocation_runtime_common.market_data import bronze_bucketing
from asset_allocation_runtime_common.market_data import layer_bucketing
from tasks.common.backfill import apply_backfill_start_cutoff


def _use_index_symbol_source() -> bool:
    bronze_bucketing.bronze_layout_mode()
    return True


def _load_index_symbols(domain: str) -> Set[str]:
    return set(bronze_bucketing.load_symbol_set(domain))


@dataclass(frozen=True)
class CutoffSweepStats:
    tables_scanned: int
    tables_rewritten: int
    deleted_blobs: int
    rows_dropped: int
    errors: int


@dataclass(frozen=True)
class BucketRewriteStats:
    tables_scanned: int
    tables_rewritten: int
    deleted_blobs: int
    rows_deleted: int
    errors: int


def collect_bronze_market_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    return _load_index_symbols("market")


def collect_bronze_earnings_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    return _load_index_symbols("earnings")


def collect_bronze_price_target_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    return _load_index_symbols("price-target")


def collect_bronze_finance_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    return _load_index_symbols("finance")


def _resolve_layer_target(root_prefix: str) -> tuple[str, str, Optional[str]]:
    clean_root = str(root_prefix or "").strip("/").lower()
    if clean_root == "market-data":
        return "silver", "market", None
    if clean_root == "market":
        return "gold", "market", None
    if clean_root == "earnings-data":
        return "silver", "earnings", None
    if clean_root == "earnings":
        return "gold", "earnings", None
    if clean_root == "price-target-data":
        return "silver", "price-target", None
    if clean_root == "targets":
        return "gold", "price-target", None
    if clean_root == "finance":
        return "gold", "finance", None
    raise ValueError(f"Unsupported bucketed root prefix: {root_prefix!r}")


def collect_delta_symbols(*, client: Any, root_prefix: str) -> Set[str]:
    _ = client
    layer, domain, sub_domain = _resolve_layer_target(root_prefix)
    return layer_bucketing.load_layer_symbol_set(layer=layer, domain=domain, sub_domain=sub_domain)


def collect_delta_market_symbols(*, client: Any, root_prefix: str) -> Set[str]:
    return collect_delta_symbols(client=client, root_prefix=root_prefix)


def collect_delta_silver_finance_symbols(*, client: Any) -> Set[str]:
    _ = client
    return layer_bucketing.load_layer_symbol_set(layer="silver", domain="finance")


def _resolve_date_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    by_normalized: dict[str, str] = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key and key not in by_normalized:
            by_normalized[key] = str(col)
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in by_normalized:
            return by_normalized[key]
    return None


def _resolve_symbol_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    by_normalized: dict[str, str] = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key and key not in by_normalized:
            by_normalized[key] = str(col)
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in by_normalized:
            return by_normalized[key]
    return None


def purge_orphan_rows_from_bucket_tables(
    *,
    upstream_symbols: Set[str],
    downstream_symbols: Set[str],
    table_paths_for_symbol: Callable[[str], Sequence[str]],
    load_table: Callable[[str], Optional[pd.DataFrame]],
    store_table: Callable[[pd.DataFrame, str], None],
    delete_prefix: Callable[[str], int],
    symbol_column_candidates: Sequence[str] = ("symbol", "Symbol"),
    vacuum_table: Optional[Callable[[str], None]] = None,
    protected_symbols: Sequence[str] = (),
) -> tuple[list[str], BucketRewriteStats]:
    normalized_upstream = {str(symbol).strip().upper() for symbol in upstream_symbols if str(symbol).strip()}
    normalized_downstream = {str(symbol).strip().upper() for symbol in downstream_symbols if str(symbol).strip()}
    protected = {str(symbol).strip().upper() for symbol in protected_symbols if str(symbol).strip()}
    protected_orphans = sorted(protected.intersection(normalized_downstream).difference(normalized_upstream))
    if protected_orphans:
        raise RuntimeError(
            "Required-symbol purge blocked: "
            f"missing_upstream_symbols={protected_orphans}"
        )

    orphan_symbols = sorted(
        {
            str(symbol).strip().upper()
            for symbol in normalized_downstream.difference(normalized_upstream)
            if str(symbol).strip()
        }
    )
    if not orphan_symbols:
        return orphan_symbols, BucketRewriteStats(0, 0, 0, 0, 0)

    paths_to_symbols: dict[str, set[str]] = {}
    for symbol in orphan_symbols:
        for path in table_paths_for_symbol(symbol):
            clean_path = str(path or "").strip()
            if not clean_path:
                continue
            paths_to_symbols.setdefault(clean_path, set()).add(symbol)

    tables_scanned = 0
    tables_rewritten = 0
    deleted_blobs = 0
    rows_deleted = 0
    errors = 0

    for table_path, symbols_for_path in paths_to_symbols.items():
        tables_scanned += 1
        try:
            df = load_table(table_path)
        except Exception:
            errors += 1
            continue
        if df is None or df.empty:
            deleted_blobs += int(delete_prefix(table_path) or 0)
            continue

        symbol_col = _resolve_symbol_column(df, symbol_column_candidates)
        if not symbol_col:
            errors += 1
            continue

        normalized_symbols = df[symbol_col].astype("string").str.strip().str.upper()
        mask = normalized_symbols.isin(symbols_for_path)
        removed = int(mask.sum())
        if removed <= 0:
            continue

        rows_deleted += removed
        filtered = df.loc[~mask].copy().reset_index(drop=True)
        if filtered.empty:
            try:
                deleted_blobs += int(delete_prefix(table_path) or 0)
            except Exception:
                errors += 1
            continue

        try:
            store_table(filtered, table_path)
            tables_rewritten += 1
            if vacuum_table is not None:
                vacuum_table(table_path)
        except Exception:
            errors += 1

    return orphan_symbols, BucketRewriteStats(
        tables_scanned=tables_scanned,
        tables_rewritten=tables_rewritten,
        deleted_blobs=deleted_blobs,
        rows_deleted=rows_deleted,
        errors=errors,
    )


def enforce_backfill_cutoff_on_bucket_tables(
    *,
    table_paths: Sequence[str],
    load_table: Callable[[str], Optional[pd.DataFrame]],
    store_table: Callable[[pd.DataFrame, str], None],
    delete_prefix: Callable[[str], int],
    date_column_candidates: Sequence[str],
    backfill_start: Optional[pd.Timestamp],
    context: str,
    vacuum_table: Optional[Callable[[str], None]] = None,
) -> CutoffSweepStats:
    if backfill_start is None:
        return CutoffSweepStats(
            tables_scanned=0,
            tables_rewritten=0,
            deleted_blobs=0,
            rows_dropped=0,
            errors=0,
        )

    tables_scanned = 0
    tables_rewritten = 0
    deleted_blobs = 0
    rows_dropped = 0
    errors = 0

    for table_path in sorted({str(path or "").strip() for path in table_paths if str(path or "").strip()}):
        tables_scanned += 1
        try:
            df = load_table(table_path)
        except Exception:
            errors += 1
            continue
        if df is None or df.empty:
            continue

        date_col = _resolve_date_column(df, date_column_candidates)
        if not date_col:
            continue

        try:
            filtered, dropped = apply_backfill_start_cutoff(
                df,
                date_col=date_col,
                backfill_start=backfill_start,
                context=f"{context} {table_path}",
            )
        except Exception:
            errors += 1
            continue

        if dropped <= 0:
            continue

        rows_dropped += int(dropped)
        if filtered is None or filtered.empty:
            try:
                deleted_blobs += int(delete_prefix(table_path) or 0)
            except Exception:
                errors += 1
            continue

        try:
            filtered = filtered.reset_index(drop=True)
            store_table(filtered, table_path)
            tables_rewritten += 1
            if vacuum_table is not None:
                vacuum_table(table_path)
        except Exception:
            errors += 1

    return CutoffSweepStats(
        tables_scanned=tables_scanned,
        tables_rewritten=tables_rewritten,
        deleted_blobs=deleted_blobs,
        rows_dropped=rows_dropped,
        errors=errors,
    )
