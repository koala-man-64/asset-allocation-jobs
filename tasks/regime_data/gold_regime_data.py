from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, NoReturn, Sequence

import pandas as pd

from asset_allocation_contracts.regime import RegimeModelConfig
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.postgres import connect, copy_rows
from asset_allocation_runtime_common.domain.regime import build_regime_outputs
from asset_allocation_runtime_common.regime_repository import RegimeRepository
from asset_allocation_runtime_common.strategy_publication_repository import StrategyPublicationRepository
from asset_allocation_runtime_common.market_data.market_symbols import REGIME_REQUIRED_MARKET_SYMBOLS
from asset_allocation_runtime_common.market_data.gold_sync_contracts import load_domain_sync_state
from asset_allocation_runtime_common.market_data.symbol_identity import (
    canonicalize_provider_symbol,
    provider_symbol_for_query,
)
from tasks.common.job_trigger import ensure_api_awake_from_env
from tasks.common.regime_publication import (
    build_regime_publish_state,
    finalize_regime_publication,
)

JOB_NAME = "gold-regime-job"
WATERMARK_KEY = "gold_regime_features"
_MARKET_PROVIDER = "massive"
_MARKET_DOMAIN = "market"
_DEFAULT_INPUT_READINESS_RETRY_ATTEMPTS = 3
_DEFAULT_INPUT_READINESS_RETRY_SLEEP_SECONDS = 60.0
_MAX_INPUT_READINESS_RETRY_ATTEMPTS = 10
_MAX_INPUT_READINESS_RETRY_SLEEP_SECONDS = 900.0
_PARTIAL_SUCCESS_STATUS = "partial_success"
_INPUT_READINESS_FAILURE_MODE = "input_readiness"
_INPUT_READINESS_RETRY_EXHAUSTED_REASON = "input_readiness_retry_exhausted"
_INPUTS_COLUMNS = (
    "as_of_date",
    "spy_close",
    "qqq_close",
    "iwm_close",
    "acwi_close",
    "return_1d",
    "return_20d",
    "qqq_return_20d",
    "iwm_return_20d",
    "acwi_return_20d",
    "spy_sma_200d",
    "qqq_sma_200d",
    "atr_14d",
    "gap_atr",
    "bb_width_20d",
    "rsi_14d",
    "volume_pct_rank_252d",
    "vix_spot_close",
    "vix3m_close",
    "vix_slope",
    "hy_oas",
    "hy_oas_z_20d",
    "rate_2y",
    "rate_10y",
    "curve_2s10s",
    "rates_event_flag",
    "vix_gt_32_streak",
    "inputs_complete_flag",
    "computed_at",
)
_MACRO_INPUTS_COLUMNS = (
    "as_of_date",
    "rate_2y",
    "rate_10y",
    "curve_2s10s",
    "hy_oas",
    "hy_oas_z_20d",
    "rates_event_flag",
    "computed_at",
)
_HISTORY_COLUMNS = (
    "as_of_date",
    "effective_from_date",
    "model_name",
    "model_version",
    "regime_code",
    "display_name",
    "signal_state",
    "score",
    "activation_threshold",
    "is_active",
    "matched_rule_id",
    "halt_flag",
    "halt_reason",
    "evidence_json",
    "computed_at",
)
_TRANSITIONS_COLUMNS = (
    "model_name",
    "model_version",
    "effective_from_date",
    "regime_code",
    "transition_type",
    "prior_score",
    "new_score",
    "activation_threshold",
    "trigger_rule_id",
    "computed_at",
)
_ACTIVE_MODELS_SCOPE_TABLE = "pg_temp.regime_active_models_scope"


@dataclass(frozen=True)
class _RegimeApplyConfig:
    table: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    scope: str


@dataclass(frozen=True)
class _RegimePublishWindow:
    published_as_of_date: date
    input_as_of_date: date | None
    skipped_trailing_input_dates: tuple[date, ...]


@dataclass(frozen=True)
class _RegimeInputReadinessRetryConfig:
    attempts: int
    sleep_seconds: float


@dataclass(frozen=True)
class _RegimeInputReadinessResult:
    market_series: pd.DataFrame
    macro_inputs: pd.DataFrame
    inputs: pd.DataFrame
    publish_window: _RegimePublishWindow
    attempts_used: int
    retry_exhausted: bool


_REGIME_APPLY_CONFIGS: tuple[_RegimeApplyConfig, ...] = (
    _RegimeApplyConfig(
        table="gold.regime_macro_inputs_daily",
        columns=_MACRO_INPUTS_COLUMNS,
        key_columns=("as_of_date",),
        scope="all_rows",
    ),
    _RegimeApplyConfig(
        table="gold.regime_inputs_daily",
        columns=_INPUTS_COLUMNS,
        key_columns=("as_of_date",),
        scope="all_rows",
    ),
    _RegimeApplyConfig(
        table="gold.regime_history",
        columns=_HISTORY_COLUMNS,
        key_columns=("as_of_date", "model_name", "model_version", "regime_code"),
        scope="active_models",
    ),
    _RegimeApplyConfig(
        table="gold.regime_latest",
        columns=_HISTORY_COLUMNS,
        key_columns=("model_name", "model_version", "regime_code"),
        scope="active_models",
    ),
    _RegimeApplyConfig(
        table="gold.regime_transitions",
        columns=_TRANSITIONS_COLUMNS,
        key_columns=("model_name", "model_version", "effective_from_date", "regime_code", "transition_type"),
        scope="active_models",
    ),
)


def _require_postgres_dsn() -> str:
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for gold regime job.")
    return dsn


def _bounded_int_env(name: str, *, default: int, minimum: int, maximum: int) -> int:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        mdc.write_warning(f"Invalid {name}={raw!r}; using default={default}.")
        return default
    return min(max(value, minimum), maximum)


def _bounded_float_env(name: str, *, default: float, minimum: float, maximum: float) -> float:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        mdc.write_warning(f"Invalid {name}={raw!r}; using default={default:.1f}.")
        return default
    return min(max(value, minimum), maximum)


def _resolve_input_readiness_retry_config() -> _RegimeInputReadinessRetryConfig:
    config = _RegimeInputReadinessRetryConfig(
        attempts=_bounded_int_env(
            "GOLD_REGIME_INPUT_READINESS_RETRY_ATTEMPTS",
            default=_DEFAULT_INPUT_READINESS_RETRY_ATTEMPTS,
            minimum=1,
            maximum=_MAX_INPUT_READINESS_RETRY_ATTEMPTS,
        ),
        sleep_seconds=_bounded_float_env(
            "GOLD_REGIME_INPUT_READINESS_RETRY_SLEEP_SECONDS",
            default=_DEFAULT_INPUT_READINESS_RETRY_SLEEP_SECONDS,
            minimum=0.0,
            maximum=_MAX_INPUT_READINESS_RETRY_SLEEP_SECONDS,
        ),
    )
    mdc.write_line(
        "Gold regime input readiness retry config: "
        f"attempts={config.attempts} sleep_seconds={config.sleep_seconds:.1f}"
    )
    return config


def _coerce_cell(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.to_pydatetime()
        return value.tz_convert("UTC").to_pydatetime()
    return value


def _frame_rows(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[tuple[Any, ...]]:
    if frame.empty:
        return []
    return [
        tuple(_coerce_cell(row.get(column)) for column in columns)
        for row in frame.loc[:, list(columns)].to_dict("records")
    ]


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier or "").replace('"', '""')
    return f'"{escaped}"'


def _cursor_rowcount(cur: Any) -> int:
    try:
        value = int(getattr(cur, "rowcount", 0) or 0)
    except (TypeError, ValueError):
        return 0
    return max(value, 0)


def _ensure_connection_is_writable(cur: Any) -> None:
    cur.execute("SHOW transaction_read_only")
    read_only_row = cur.fetchone()
    transaction_read_only = str(read_only_row[0]).strip().lower() if read_only_row else "unknown"

    cur.execute("SHOW default_transaction_read_only")
    default_read_only_row = cur.fetchone()
    default_transaction_read_only = (
        str(default_read_only_row[0]).strip().lower() if default_read_only_row else "unknown"
    )

    cur.execute("SELECT pg_is_in_recovery()")
    recovery_row = cur.fetchone()
    in_recovery = bool(recovery_row[0]) if recovery_row else False
    if transaction_read_only == "on" or in_recovery:
        raise RuntimeError(
            "Postgres write target unavailable: "
            f"transaction_read_only={transaction_read_only} "
            f"default_transaction_read_only={default_transaction_read_only} "
            f"pg_is_in_recovery={'true' if in_recovery else 'false'}"
        )


def _table_stage_name(table: str) -> str:
    return f"regime_stage_{str(table or '').split('.')[-1]}"


def _create_regime_stage(cur: Any, *, config: _RegimeApplyConfig) -> str:
    stage_name = _table_stage_name(config.table)
    quoted_key_columns = ", ".join(_quote_identifier(column) for column in config.key_columns)
    cur.execute(
        f"CREATE TEMP TABLE {stage_name} (LIKE {config.table} INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    cur.execute(f"CREATE UNIQUE INDEX {stage_name}_key_idx ON {stage_name} ({quoted_key_columns})")
    return f"pg_temp.{stage_name}"


def _create_active_models_scope(cur: Any, *, active_models: Sequence[tuple[str, int]]) -> None:
    cur.execute(
        """
        CREATE TEMP TABLE regime_active_models_scope (
            model_name TEXT NOT NULL,
            model_version INTEGER NOT NULL
        ) ON COMMIT DROP
        """
    )
    cur.execute(
        "CREATE UNIQUE INDEX regime_active_models_scope_key_idx "
        "ON regime_active_models_scope (model_name, model_version)"
    )
    if active_models:
        copy_rows(
            cur,
            table=_ACTIVE_MODELS_SCOPE_TABLE,
            columns=("model_name", "model_version"),
            rows=active_models,
        )
        cur.execute("ANALYZE regime_active_models_scope")


def _build_key_match_sql(*, left_alias: str, right_alias: str, key_columns: Sequence[str]) -> str:
    return " AND ".join(
        f'{left_alias}.{_quote_identifier(column)} = {right_alias}.{_quote_identifier(column)}'
        for column in key_columns
    )


def _delete_missing_regime_rows(
    cur: Any,
    *,
    config: _RegimeApplyConfig,
    stage_table: str,
    active_models_count: int,
) -> int:
    key_match_sql = _build_key_match_sql(
        left_alias="stage",
        right_alias="target",
        key_columns=config.key_columns,
    )
    if config.scope == "all_rows":
        cur.execute(
            f"""
            DELETE FROM {config.table} AS target
            WHERE NOT EXISTS (
                SELECT 1
                FROM {stage_table} AS stage
                WHERE {key_match_sql}
            )
            """
        )
        return _cursor_rowcount(cur)

    if active_models_count <= 0:
        return 0

    cur.execute(
        f"""
        DELETE FROM {config.table} AS target
        WHERE EXISTS (
            SELECT 1
            FROM {_ACTIVE_MODELS_SCOPE_TABLE} AS scope
            WHERE scope.model_name = target.model_name
              AND scope.model_version = target.model_version
        )
          AND NOT EXISTS (
              SELECT 1
              FROM {stage_table} AS stage
              WHERE {key_match_sql}
          )
        """
    )
    return _cursor_rowcount(cur)


def _upsert_regime_stage_rows(cur: Any, *, config: _RegimeApplyConfig, stage_table: str) -> int:
    quoted_insert_columns = ", ".join(_quote_identifier(column) for column in config.columns)
    quoted_select_columns = ", ".join(f'stage.{_quote_identifier(column)}' for column in config.columns)
    quoted_conflict_columns = ", ".join(_quote_identifier(column) for column in config.key_columns)
    update_columns = [column for column in config.columns if column not in config.key_columns]
    if not update_columns:
        cur.execute(
            f"""
            INSERT INTO {config.table} ({quoted_insert_columns})
            SELECT {quoted_select_columns}
            FROM {stage_table} AS stage
            ON CONFLICT ({quoted_conflict_columns}) DO NOTHING
            """
        )
        return _cursor_rowcount(cur)

    assignments = ", ".join(
        f'{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}'
        for column in update_columns
    )
    changed_predicate = " OR ".join(
        f'target.{_quote_identifier(column)} IS DISTINCT FROM EXCLUDED.{_quote_identifier(column)}'
        for column in update_columns
    )
    cur.execute(
        f"""
        INSERT INTO {config.table} AS target ({quoted_insert_columns})
        SELECT {quoted_select_columns}
        FROM {stage_table} AS stage
        ON CONFLICT ({quoted_conflict_columns}) DO UPDATE
        SET {assignments}
        WHERE {changed_predicate}
        """
    )
    return _cursor_rowcount(cur)


def _apply_regime_table(
    cur: Any,
    *,
    config: _RegimeApplyConfig,
    frame: pd.DataFrame,
    active_models_count: int,
) -> None:
    stage_table = _create_regime_stage(cur, config=config)
    rows = _frame_rows(frame, config.columns)
    started_at = time.perf_counter()
    if rows:
        copy_rows(
            cur,
            table=stage_table,
            columns=config.columns,
            rows=rows,
        )
    cur.execute(f"ANALYZE {_table_stage_name(config.table)}")
    deleted_rows = _delete_missing_regime_rows(
        cur,
        config=config,
        stage_table=stage_table,
        active_models_count=active_models_count,
    )
    upserted_rows = _upsert_regime_stage_rows(cur, config=config, stage_table=stage_table)
    unchanged_rows = max(len(rows) - upserted_rows, 0)
    duration_ms = int(round((time.perf_counter() - started_at) * 1000.0))
    mdc.write_line(
        "gold_regime_postgres_apply_stats "
        f"table={config.table} staged_rows={len(rows)} deleted_rows={deleted_rows} "
        f"upserted_rows={upserted_rows} unchanged_rows={unchanged_rows} "
        f"scope={config.scope} scope_models={active_models_count} duration_ms={duration_ms}"
    )


def _canonical_regime_market_symbol(symbol: object) -> str:
    raw = str(symbol or "").strip()
    if not raw:
        return ""
    return canonicalize_provider_symbol(_MARKET_PROVIDER, _MARKET_DOMAIN, raw)


def _regime_market_query_symbols() -> tuple[str, ...]:
    symbols: list[str] = []
    for symbol in REGIME_REQUIRED_MARKET_SYMBOLS:
        canonical = _canonical_regime_market_symbol(symbol)
        if canonical:
            symbols.append(canonical)
        provider_symbol = provider_symbol_for_query(_MARKET_PROVIDER, _MARKET_DOMAIN, canonical)
        if provider_symbol and provider_symbol != canonical:
            symbols.append(provider_symbol)
    return tuple(dict.fromkeys(symbols))


def _normalize_market_series(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out.get("date"), errors="coerce").dt.date
    raw_symbols = (
        out.get("symbol", pd.Series("", index=out.index, dtype="string"))
        .astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
    )
    out["symbol"] = raw_symbols
    out["_raw_symbol"] = raw_symbols
    out["_original_order"] = range(len(out))
    out = out[(out["_raw_symbol"] != "") & out["date"].notna()].copy()
    if out.empty:
        return out.drop(columns=["_raw_symbol", "_original_order"], errors="ignore").reset_index(drop=True)

    out["symbol"] = out["_raw_symbol"].map(_canonical_regime_market_symbol)
    alias_rows = int((out["_raw_symbol"] != out["symbol"]).sum())
    out["_canonical_preference"] = (out["_raw_symbol"] != out["symbol"]).astype(int)
    before_dedup_count = len(out)
    out = (
        out.sort_values(["symbol", "date", "_canonical_preference", "_original_order"])
        .drop_duplicates(subset=["symbol", "date"], keep="first")
        .drop(columns=["_raw_symbol", "_canonical_preference", "_original_order"], errors="ignore")
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    dropped_duplicate_rows = max(before_dedup_count - len(out), 0)
    if alias_rows or dropped_duplicate_rows:
        mdc.write_warning(
            "Gold regime market symbol aliases normalized: "
            f"alias_rows={alias_rows} duplicate_rows_dropped={dropped_duplicate_rows}"
        )
    return out


def _summarize_market_series_coverage(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "no rows"

    summary: list[str] = []
    for symbol in REGIME_REQUIRED_MARKET_SYMBOLS:
        symbol_frame = frame[frame["symbol"] == symbol]
        if symbol_frame.empty:
            summary.append(f"{symbol}=missing")
            continue

        parsed_dates = pd.to_datetime(symbol_frame["date"], errors="coerce").dropna()
        if parsed_dates.empty:
            summary.append(f"{symbol}=no_valid_dates rows={len(symbol_frame)}")
            continue

        summary.append(
            f"{symbol}={parsed_dates.min().date().isoformat()}..{parsed_dates.max().date().isoformat()} "
            f"rows={len(symbol_frame)}"
        )
    return ", ".join(summary)


def _fail_fast(message: str) -> NoReturn:
    mdc.write_error(message)
    raise ValueError(message)


def _summarize_market_sync_state(dsn: str | None) -> str:
    if not dsn:
        return "market_sync_state=unknown"

    try:
        sync_state = load_domain_sync_state(dsn, domain="market")
    except Exception as exc:
        return f"market_sync_state=unavailable error={type(exc).__name__}: {exc}"

    if not sync_state:
        return "market_sync_state=empty"

    success_buckets = 0
    failed_buckets = 0
    total_rows = 0
    latest_synced_at: datetime | None = None

    for state in sync_state.values():
        status = str(state.get("status") or "").strip().lower()
        if status == "success":
            success_buckets += 1
        else:
            failed_buckets += 1

        try:
            total_rows += int(state.get("row_count") or 0)
        except (TypeError, ValueError):
            pass

        synced_at = state.get("synced_at")
        if isinstance(synced_at, datetime):
            normalized = (
                synced_at.replace(tzinfo=timezone.utc)
                if synced_at.tzinfo is None
                else synced_at.astimezone(timezone.utc)
            )
            if latest_synced_at is None or normalized > latest_synced_at:
                latest_synced_at = normalized

    latest_synced_at_iso = latest_synced_at.isoformat() if latest_synced_at is not None else "n/a"
    return (
        f"market_sync_state=buckets={len(sync_state)} success_buckets={success_buckets} "
        f"failed_buckets={failed_buckets} total_rows={total_rows} "
        f"latest_synced_at={latest_synced_at_iso}"
    )


def _validate_required_market_series(frame: pd.DataFrame, *, dsn: str | None = None) -> pd.DataFrame:
    present_symbols = {str(value).strip().upper() for value in frame["symbol"].dropna().tolist()}
    missing = [symbol for symbol in REGIME_REQUIRED_MARKET_SYMBOLS if symbol not in present_symbols]
    if missing:
        coverage = _summarize_market_series_coverage(frame)
        sync_summary = _summarize_market_sync_state(dsn)
        _fail_fast(
            "Gold regime fast-fail: gold.market_data is missing required regime symbols "
            f"{missing}. coverage={coverage}. {sync_summary}. "
            "Upstream dependency gold-market-job has not populated the required Postgres-serving inputs."
        )
    return frame


def _normalize_macro_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["as_of_date"] = pd.to_datetime(out.get("as_of_date"), errors="coerce").dt.date
    if "rate_2y" not in out.columns:
        out["rate_2y"] = pd.NA
    if "rate_10y" not in out.columns:
        out["rate_10y"] = pd.NA
    if "curve_2s10s" not in out.columns:
        out["curve_2s10s"] = pd.NA
    if "hy_oas" not in out.columns:
        out["hy_oas"] = pd.NA
    if "hy_oas_z_20d" not in out.columns:
        out["hy_oas_z_20d"] = pd.NA
    if "rates_event_flag" not in out.columns:
        out["rates_event_flag"] = pd.NA
    if "computed_at" not in out.columns:
        out["computed_at"] = pd.NaT
    out = out.dropna(subset=["as_of_date"]).sort_values("as_of_date").drop_duplicates(subset=["as_of_date"], keep="last")

    out["rate_2y"] = pd.to_numeric(out["rate_2y"], errors="coerce")
    out["rate_10y"] = pd.to_numeric(out["rate_10y"], errors="coerce")
    out["curve_2s10s"] = pd.to_numeric(out["curve_2s10s"], errors="coerce")
    out["hy_oas"] = pd.to_numeric(out["hy_oas"], errors="coerce")
    out["hy_oas_z_20d"] = pd.to_numeric(out["hy_oas_z_20d"], errors="coerce")
    out["rates_event_flag"] = out["rates_event_flag"].astype("boolean")
    out["computed_at"] = pd.to_datetime(out["computed_at"], utc=True, errors="coerce")
    out["curve_2s10s"] = out["curve_2s10s"].where(out["curve_2s10s"].notna(), out["rate_10y"] - out["rate_2y"])
    if out["hy_oas"].notna().any() and not out["hy_oas_z_20d"].notna().any():
        hy_mean_20 = out["hy_oas"].rolling(window=20, min_periods=20).mean()
        hy_std_20 = out["hy_oas"].rolling(window=20, min_periods=20).std(ddof=1)
        out["hy_oas_z_20d"] = (out["hy_oas"] - hy_mean_20) / hy_std_20.replace(0.0, pd.NA)
    return out[list(_MACRO_INPUTS_COLUMNS)].reset_index(drop=True)


def _summarize_macro_input_coverage(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "no rows"
    parsed_dates = pd.to_datetime(frame.get("as_of_date"), errors="coerce").dropna()
    if parsed_dates.empty:
        return f"rows={len(frame)} no_valid_dates"
    required_columns = ("rate_2y", "rate_10y", "curve_2s10s", "hy_oas", "hy_oas_z_20d", "rates_event_flag")
    present_counts = {
        column: int(pd.Series(frame.get(column), dtype="object").notna().sum())
        for column in required_columns
        if column != "rates_event_flag"
    }
    rates_event_series = pd.Series(frame.get("rates_event_flag"), dtype="boolean").fillna(False)
    rates_event_count = int(rates_event_series.sum())
    metrics = " ".join(f"{column}_nonnull={count}" for column, count in present_counts.items())
    return (
        f"{parsed_dates.min().date().isoformat()}..{parsed_dates.max().date().isoformat()} "
        f"rows={len(frame)} {metrics} rates_event_days={rates_event_count}"
    )


def _assert_complete_regime_inputs(
    inputs: pd.DataFrame,
    *,
    market_series: pd.DataFrame,
    macro_inputs: pd.DataFrame,
) -> None:
    complete_rows = (
        inputs["inputs_complete_flag"].fillna(False)
        if "inputs_complete_flag" in inputs.columns
        else pd.Series(dtype="bool")
    )
    if bool(complete_rows.any()):
        return

    inputs_range = "n/a"
    if not inputs.empty and "as_of_date" in inputs.columns:
        parsed_dates = pd.to_datetime(inputs["as_of_date"], errors="coerce").dropna()
        if not parsed_dates.empty:
            inputs_range = f"{parsed_dates.min().date().isoformat()}..{parsed_dates.max().date().isoformat()}"

    coverage = _summarize_market_series_coverage(market_series)
    macro_coverage = _summarize_macro_input_coverage(macro_inputs)
    _fail_fast(
        "Gold regime fast-fail: gold regime inputs contain no complete multi-label regime rows. "
        f"inputs_range={inputs_range}. market_coverage={coverage}. macro_coverage={macro_coverage}. "
        "Upstream market and macro dependencies have not produced overlapping regime-monitor inputs in Postgres."
    )


def _resolve_publish_window(
    inputs: pd.DataFrame,
    *,
    market_series: pd.DataFrame,
    macro_inputs: pd.DataFrame,
) -> _RegimePublishWindow:
    _assert_complete_regime_inputs(inputs, market_series=market_series, macro_inputs=macro_inputs)
    as_of_dates = pd.to_datetime(inputs.get("as_of_date"), errors="coerce")
    complete_rows = (
        inputs["inputs_complete_flag"].fillna(False)
        if "inputs_complete_flag" in inputs.columns
        else pd.Series(False, index=inputs.index, dtype="bool")
    )
    complete_dates = as_of_dates[complete_rows & as_of_dates.notna()]
    published_as_of_ts = complete_dates.max()
    if pd.isna(published_as_of_ts):
        _fail_fast("Gold regime fast-fail: unable to resolve publish cutoff from complete inputs.")

    raw_dates = as_of_dates.dropna()
    input_as_of_date = raw_dates.max().date() if not raw_dates.empty else None
    skipped_trailing_input_dates = tuple(
        value.date() for value in raw_dates[raw_dates > published_as_of_ts].sort_values().drop_duplicates().tolist()
    )
    return _RegimePublishWindow(
        published_as_of_date=published_as_of_ts.date(),
        input_as_of_date=input_as_of_date,
        skipped_trailing_input_dates=skipped_trailing_input_dates,
    )


def _publish_window_metadata(window: _RegimePublishWindow) -> dict[str, Any]:
    return {
        "published_as_of_date": window.published_as_of_date.isoformat(),
        "input_as_of_date": window.input_as_of_date.isoformat() if isinstance(window.input_as_of_date, date) else None,
        "skipped_trailing_input_dates": [value.isoformat() for value in window.skipped_trailing_input_dates],
    }


def _publish_window_warnings(window: _RegimePublishWindow) -> list[str]:
    skipped_dates = _publish_window_metadata(window)["skipped_trailing_input_dates"]
    if not skipped_dates:
        return []
    return [
        "Trailing incomplete regime input dates skipped from published regime surfaces: "
        f"{', '.join(skipped_dates)}. Published regime state remains capped at "
        f"{window.published_as_of_date.isoformat()}."
    ]


def _max_frame_date(frame: pd.DataFrame, column: str):
    if frame.empty or column not in frame.columns:
        return None
    parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
    if parsed.empty:
        return None
    return parsed.max().date()


def _assert_regime_publish_frames_ready(
    *,
    published_inputs: pd.DataFrame,
    published_macro_inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
    active_models: list[tuple[str, int]],
    published_as_of_date: date,
) -> None:
    if published_inputs.empty:
        _fail_fast("Gold regime fast-fail: refusing to publish with no regime input rows.")
    if published_macro_inputs.empty:
        _fail_fast("Gold regime fast-fail: refusing to publish with no macro input rows.")
    if history.empty:
        _fail_fast("Gold regime fast-fail: refusing to publish with no regime history rows.")
    if latest.empty:
        _fail_fast("Gold regime fast-fail: refusing to publish with no regime latest rows.")
    if not active_models:
        _fail_fast("Gold regime fast-fail: refusing to publish with no active models.")

    if "inputs_complete_flag" not in published_inputs.columns:
        _fail_fast("Gold regime fast-fail: published inputs are missing inputs_complete_flag.")
    incomplete_count = int((~published_inputs["inputs_complete_flag"].fillna(False).astype(bool)).sum())
    if incomplete_count:
        _fail_fast(
            "Gold regime fast-fail: refusing to publish incomplete regime input rows. "
            f"incomplete_rows={incomplete_count}"
        )

    for frame_name, frame in (("history", history), ("latest", latest)):
        max_date = _max_frame_date(frame, "as_of_date")
        if max_date != published_as_of_date:
            _fail_fast(
                "Gold regime fast-fail: output date mismatch before publication. "
                f"frame={frame_name} expected_as_of_date={published_as_of_date.isoformat()} "
                f"observed_max_as_of_date={max_date.isoformat() if max_date else 'none'}"
            )

    if transitions is None:
        _fail_fast("Gold regime fast-fail: transitions frame is None.")


def _load_market_series(dsn: str) -> pd.DataFrame:
    query_symbols = _regime_market_query_symbols()
    with connect(dsn) as conn:
        frame = pd.read_sql_query(
            """
            SELECT
                symbol,
                date,
                close,
                return_1d,
                return_20d,
                sma_200d,
                atr_14d,
                gap_atr,
                bb_width_20d,
                volume_pct_rank_252d,
                rsi_14d
            FROM gold.market_data
            WHERE symbol = ANY(%s)
            ORDER BY date ASC, symbol ASC
            """,
            conn,
            params=(list(query_symbols),),
        )
    normalized = _normalize_market_series(frame)
    return _validate_required_market_series(normalized, dsn=dsn)


def _load_macro_inputs(dsn: str) -> pd.DataFrame:
    with connect(dsn) as conn:
        macro_frame = pd.read_sql_query(
            """
            SELECT
                as_of_date,
                rate_2y,
                rate_10y,
                curve_2s10s,
                hy_oas,
                hy_oas_z_20d,
                rates_event_flag,
                computed_at
            FROM gold.regime_macro_inputs_daily
            ORDER BY as_of_date ASC
            """,
            conn,
        )
        try:
            catalyst_frame = pd.read_sql_query(
                """
                SELECT
                    as_of_date,
                    BOOL_OR(
                        COALESCE(rates_event_count, 0) > 0
                        OR COALESCE(policy_event_count, 0) > 0
                    ) AS rates_event_flag
                FROM gold.economic_catalyst_entity_daily
                GROUP BY as_of_date
                ORDER BY as_of_date ASC
                """,
                conn,
            )
        except Exception as exc:
            raise RuntimeError(
                "Gold regime macro input load failed: catalyst source gold.economic_catalyst_entity_daily "
                f"is unavailable ({type(exc).__name__}: {exc})."
            ) from exc

    macro_inputs = _normalize_macro_inputs(macro_frame)
    catalyst_inputs = _normalize_macro_inputs(catalyst_frame)
    if catalyst_inputs.empty:
        return macro_inputs

    combined = (
        macro_inputs.drop(columns=["rates_event_flag"], errors="ignore")
        .merge(
            catalyst_inputs[["as_of_date", "rates_event_flag"]],
            on="as_of_date",
            how="outer",
            suffixes=("", "_catalyst"),
        )
        .merge(
            macro_inputs[["as_of_date", "rates_event_flag"]],
            on="as_of_date",
            how="left",
            suffixes=("", "_macro"),
        )
    )
    combined["rates_event_flag"] = combined["rates_event_flag_macro"].combine_first(combined["rates_event_flag"])
    combined = combined.drop(columns=["rates_event_flag_macro"], errors="ignore")
    combined["computed_at"] = pd.to_datetime(combined.get("computed_at"), utc=True, errors="coerce")
    return _normalize_macro_inputs(combined)


def _compute_vix_streak(values: Sequence[Any], *, threshold: float) -> list[int]:
    streak = 0
    streak_values: list[int] = []
    for raw_value in values:
        value = float(raw_value) if raw_value is not None and not pd.isna(raw_value) else None
        if value is not None and value > threshold:
            streak += 1
        else:
            streak = 0
        streak_values.append(streak)
    return streak_values


def _symbol_slice(
    market_series: pd.DataFrame,
    *,
    symbol: str,
    keep_columns: Sequence[str],
    rename_map: dict[str, str],
) -> pd.DataFrame:
    selected_columns = ["date", *keep_columns]
    frame = market_series[market_series["symbol"] == symbol][selected_columns].copy()
    return frame.rename(columns={"date": "as_of_date", **rename_map})


def _build_inputs_daily(
    market_series: pd.DataFrame,
    macro_inputs: pd.DataFrame,
    *,
    computed_at: datetime,
) -> pd.DataFrame:
    spy = _symbol_slice(
        market_series,
        symbol="SPY",
        keep_columns=("close", "return_1d", "return_20d", "sma_200d", "atr_14d", "gap_atr", "bb_width_20d", "rsi_14d", "volume_pct_rank_252d"),
        rename_map={
            "close": "spy_close",
            "sma_200d": "spy_sma_200d",
        },
    )
    qqq = _symbol_slice(
        market_series,
        symbol="QQQ",
        keep_columns=("close", "return_20d", "sma_200d"),
        rename_map={
            "close": "qqq_close",
            "return_20d": "qqq_return_20d",
            "sma_200d": "qqq_sma_200d",
        },
    )
    iwm = _symbol_slice(
        market_series,
        symbol="IWM",
        keep_columns=("close", "return_20d"),
        rename_map={
            "close": "iwm_close",
            "return_20d": "iwm_return_20d",
        },
    )
    acwi = _symbol_slice(
        market_series,
        symbol="ACWI",
        keep_columns=("close", "return_20d"),
        rename_map={
            "close": "acwi_close",
            "return_20d": "acwi_return_20d",
        },
    )
    vix = _symbol_slice(
        market_series,
        symbol="^VIX",
        keep_columns=("close",),
        rename_map={"close": "vix_spot_close"},
    )
    vix3m = _symbol_slice(
        market_series,
        symbol="^VIX3M",
        keep_columns=("close",),
        rename_map={"close": "vix3m_close"},
    )

    inputs = spy.merge(qqq, on="as_of_date", how="outer")
    inputs = inputs.merge(iwm, on="as_of_date", how="outer")
    inputs = inputs.merge(acwi, on="as_of_date", how="outer")
    inputs = inputs.merge(vix, on="as_of_date", how="outer")
    inputs = inputs.merge(vix3m, on="as_of_date", how="outer")
    inputs = inputs.merge(macro_inputs, on="as_of_date", how="outer", suffixes=("", "_macro"))
    inputs = inputs.sort_values("as_of_date").reset_index(drop=True)

    if "computed_at_macro" in inputs.columns:
        inputs["computed_at"] = pd.to_datetime(inputs["computed_at"], utc=True, errors="coerce").fillna(
            pd.to_datetime(inputs["computed_at_macro"], utc=True, errors="coerce")
        )
        inputs = inputs.drop(columns=["computed_at_macro"], errors="ignore")

    inputs["vix_slope"] = inputs["vix3m_close"] - inputs["vix_spot_close"]
    inputs["vix_gt_32_streak"] = _compute_vix_streak(inputs["vix_spot_close"].tolist(), threshold=32.0)
    required_columns = [
        "spy_close",
        "qqq_close",
        "iwm_close",
        "acwi_close",
        "return_1d",
        "return_20d",
        "qqq_return_20d",
        "iwm_return_20d",
        "acwi_return_20d",
        "spy_sma_200d",
        "qqq_sma_200d",
        "atr_14d",
        "gap_atr",
        "bb_width_20d",
        "rsi_14d",
        "volume_pct_rank_252d",
        "vix_spot_close",
        "vix3m_close",
        "vix_slope",
        "hy_oas",
        "hy_oas_z_20d",
        "rate_2y",
        "rate_10y",
        "curve_2s10s",
        "rates_event_flag",
    ]
    inputs["inputs_complete_flag"] = inputs[required_columns].notna().all(axis=1)
    inputs["computed_at"] = pd.to_datetime(inputs["computed_at"], utc=True, errors="coerce").fillna(pd.Timestamp(computed_at))
    return inputs[list(_INPUTS_COLUMNS)].copy()


def _published_inputs(inputs: pd.DataFrame, *, window: _RegimePublishWindow) -> pd.DataFrame:
    as_of_dates = pd.to_datetime(inputs.get("as_of_date"), errors="coerce")
    return inputs.loc[as_of_dates <= pd.Timestamp(window.published_as_of_date)].copy()


def _build_revision_inputs(
    inputs: pd.DataFrame,
    *,
    config: RegimeModelConfig | dict[str, Any] | None,
) -> tuple[pd.DataFrame, RegimeModelConfig]:
    resolved_config = config if isinstance(config, RegimeModelConfig) else RegimeModelConfig.model_validate(config or {})
    revision_inputs = inputs.copy()
    revision_inputs["vix_gt_32_streak"] = _compute_vix_streak(
        revision_inputs["vix_spot_close"].tolist(),
        threshold=float(resolved_config.haltVixThreshold),
    )
    return revision_inputs.copy(), resolved_config


def _replace_postgres_tables(
    dsn: str,
    *,
    macro_inputs: pd.DataFrame,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
    active_models: list[tuple[str, int]],
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_connection_is_writable(cur)
            _create_active_models_scope(cur, active_models=active_models)
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[0],
                frame=macro_inputs,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[1],
                frame=inputs,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[2],
                frame=history,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[3],
                frame=latest,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[4],
                frame=transitions,
                active_models_count=len(active_models),
            )


def _write_storage_parquet_outputs(
    *,
    gold_container: str,
    macro_inputs: pd.DataFrame,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
) -> None:
    client = mdc.get_storage_client(gold_container)
    if client is None:
        raise ValueError(f"Storage client unavailable for container '{gold_container}'.")
    client.write_parquet("regime/macro_inputs.parquet", macro_inputs)
    client.write_parquet("regime/inputs.parquet", inputs)
    client.write_parquet("regime/history.parquet", history)
    client.write_parquet("regime/latest.parquet", latest)
    client.write_parquet("regime/transitions.parquet", transitions)


def _record_regime_reconcile_signal(
    *,
    publish_state: dict[str, Any],
    source_fingerprint: str | None,
    domain_artifact_path: str | None,
) -> None:
    source_fingerprint = str(source_fingerprint or "").strip()
    if not source_fingerprint:
        raise RuntimeError("Regime publication finalization did not produce a source fingerprint.")
    response = StrategyPublicationRepository().record_reconcile_signal(
        job_key="regime",
        source_fingerprint=source_fingerprint,
        metadata={
            "publishedAsOfDate": publish_state.get("published_as_of_date"),
            "inputAsOfDate": publish_state.get("input_as_of_date"),
            "historyRows": publish_state.get("history_rows"),
            "latestRows": publish_state.get("latest_rows"),
            "transitionRows": publish_state.get("transition_rows"),
            "activeModels": publish_state.get("active_models") or [],
            "domainArtifactPath": domain_artifact_path,
            "producerJobName": JOB_NAME,
        },
    )
    if response.status == "error":
        raise RuntimeError(
            "Gold regime reconcile signal is still in error state: "
            f"source_fingerprint={response.sourceFingerprint}"
        )
    mdc.write_line(
        "Gold regime reconcile signal recorded: "
        f"job_key={response.jobKey} source_fingerprint={response.sourceFingerprint} "
        f"status={response.status} created={str(response.created).lower()}"
    )


def _record_regime_reconcile_signal_after_artifact(
    artifact_payload: dict[str, Any],
    published: dict[str, Any],
) -> None:
    _record_regime_reconcile_signal(
        publish_state=artifact_payload,
        source_fingerprint=str(artifact_payload.get("sourceCommit") or "") or None,
        domain_artifact_path=str((published or {}).get("artifactPath") or artifact_payload.get("artifactPath") or "")
        or None,
    )


def _load_regime_input_readiness(dsn: str, *, computed_at: datetime) -> _RegimeInputReadinessResult:
    market_series = _load_market_series(dsn)
    macro_inputs = _load_macro_inputs(dsn)
    inputs = _build_inputs_daily(market_series, macro_inputs, computed_at=computed_at)
    publish_window = _resolve_publish_window(inputs, market_series=market_series, macro_inputs=macro_inputs)
    return _RegimeInputReadinessResult(
        market_series=market_series,
        macro_inputs=macro_inputs,
        inputs=inputs,
        publish_window=publish_window,
        attempts_used=1,
        retry_exhausted=False,
    )


def _resolve_regime_input_readiness(dsn: str, *, computed_at: datetime) -> _RegimeInputReadinessResult:
    retry_config = _resolve_input_readiness_retry_config()
    last_error: Exception | None = None
    for attempt in range(1, retry_config.attempts + 1):
        try:
            readiness = _load_regime_input_readiness(dsn, computed_at=computed_at)
        except Exception as exc:
            last_error = exc
            if attempt >= retry_config.attempts:
                raise
            mdc.write_warning(
                "Gold regime input readiness check failed; retrying: "
                f"attempt={attempt}/{retry_config.attempts} error={type(exc).__name__}: {exc}"
            )
            if retry_config.sleep_seconds > 0:
                time.sleep(retry_config.sleep_seconds)
            continue

        publish_window = readiness.publish_window
        if not publish_window.skipped_trailing_input_dates:
            return _RegimeInputReadinessResult(
                market_series=readiness.market_series,
                macro_inputs=readiness.macro_inputs,
                inputs=readiness.inputs,
                publish_window=publish_window,
                attempts_used=attempt,
                retry_exhausted=False,
            )

        metadata = _publish_window_metadata(publish_window)
        skipped_dates = ",".join(metadata["skipped_trailing_input_dates"]) or "-"
        if attempt >= retry_config.attempts:
            mdc.write_warning(
                "Gold regime input readiness retry exhausted; publishing latest complete window: "
                f"attempts={attempt} published_as_of_date={metadata['published_as_of_date']} "
                f"input_as_of_date={metadata['input_as_of_date']} "
                f"skipped_trailing_input_dates={skipped_dates}"
            )
            return _RegimeInputReadinessResult(
                market_series=readiness.market_series,
                macro_inputs=readiness.macro_inputs,
                inputs=readiness.inputs,
                publish_window=publish_window,
                attempts_used=attempt,
                retry_exhausted=True,
            )

        mdc.write_warning(
            "Gold regime input readiness incomplete; retrying before publication: "
            f"attempt={attempt}/{retry_config.attempts} "
            f"published_as_of_date={metadata['published_as_of_date']} "
            f"input_as_of_date={metadata['input_as_of_date']} "
            f"skipped_trailing_input_dates={skipped_dates}"
        )
        if retry_config.sleep_seconds > 0:
            time.sleep(retry_config.sleep_seconds)

    if last_error is not None:
        raise last_error
    raise RuntimeError("Gold regime input readiness retry loop exited without a result.")


def main() -> int:
    mdc.log_environment_diagnostics()
    dsn = _require_postgres_dsn()
    gold_container = str(os.environ.get("AZURE_CONTAINER_GOLD") or "").strip()
    if not gold_container:
        raise ValueError("AZURE_CONTAINER_GOLD is required for gold regime job.")

    computed_at = datetime.now(timezone.utc)
    readiness = _resolve_regime_input_readiness(dsn, computed_at=computed_at)
    macro_inputs = readiness.macro_inputs
    inputs = readiness.inputs
    publish_window = readiness.publish_window
    repo = RegimeRepository(dsn)
    active_revisions = repo.list_active_regime_model_revisions()
    if not active_revisions:
        raise ValueError("No active regime model revisions found.")
    publish_window_metadata = _publish_window_metadata(publish_window)
    publish_window_warnings = _publish_window_warnings(publish_window)
    skipped_dates_for_log = ",".join(publish_window_metadata["skipped_trailing_input_dates"]) or "-"
    if publish_window.skipped_trailing_input_dates:
        mdc.write_warning(
            "Gold regime publish window trimmed: "
            f"published_as_of_date={publish_window_metadata['published_as_of_date']} "
            f"input_as_of_date={publish_window_metadata['input_as_of_date']} "
            f"skipped_count={len(publish_window.skipped_trailing_input_dates)} "
            f"skipped_trailing_input_dates={skipped_dates_for_log}"
        )
    else:
        mdc.write_line(
            "Gold regime publish window resolved: "
            f"published_as_of_date={publish_window_metadata['published_as_of_date']} "
            f"input_as_of_date={publish_window_metadata['input_as_of_date']} "
            "skipped_count=0"
        )

    publication_status = "published"
    publication_reason = "none"
    publication_failure_mode = "none"
    if readiness.retry_exhausted and publish_window.skipped_trailing_input_dates:
        publication_status = _PARTIAL_SUCCESS_STATUS
        publication_reason = _INPUT_READINESS_RETRY_EXHAUSTED_REASON
        publication_failure_mode = _INPUT_READINESS_FAILURE_MODE

    published_inputs = _published_inputs(inputs, window=publish_window)
    published_macro_inputs = _published_inputs(macro_inputs, window=publish_window)

    history_frames: list[pd.DataFrame] = []
    latest_frames: list[pd.DataFrame] = []
    transition_frames: list[pd.DataFrame] = []
    active_models: list[tuple[str, int]] = []

    for revision in active_revisions:
        model_name = str(revision["name"])
        model_version = int(revision["version"])
        revision_config = revision.get("config")
        if not isinstance(revision_config, dict) or not revision_config:
            raise ValueError(
                "Active regime model revision has no explicit config: "
                f"model_name={model_name} model_version={model_version}"
            )
        revision_inputs, resolved_config = _build_revision_inputs(
            published_inputs,
            config=revision_config,
        )
        mdc.write_line(
            "Gold regime active revision: "
            f"model_name={model_name} model_version={model_version} "
            f"halt_vix_threshold={float(resolved_config.haltVixThreshold):.2f} "
            f"halt_vix_streak_days={int(resolved_config.haltVixStreakDays)} "
            f"published_as_of_date={publish_window_metadata['published_as_of_date']}"
        )
        history, latest, transitions = build_regime_outputs(
            revision_inputs,
            model_name=model_name,
            model_version=model_version,
            config=resolved_config,
            computed_at=computed_at,
        )
        history_frames.append(history)
        latest_frames.append(latest)
        transition_frames.append(transitions)
        active_models.append((model_name, model_version))

    history = pd.concat(history_frames, ignore_index=True) if history_frames else pd.DataFrame(columns=_HISTORY_COLUMNS)
    latest = pd.concat(latest_frames, ignore_index=True) if latest_frames else pd.DataFrame(columns=_HISTORY_COLUMNS)
    transitions = (
        pd.concat(transition_frames, ignore_index=True)
        if transition_frames
        else pd.DataFrame(columns=_TRANSITIONS_COLUMNS)
    )

    _assert_regime_publish_frames_ready(
        published_inputs=published_inputs,
        published_macro_inputs=published_macro_inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=active_models,
        published_as_of_date=publish_window.published_as_of_date,
    )
    _write_storage_parquet_outputs(
        gold_container=gold_container,
        macro_inputs=published_macro_inputs,
        inputs=published_inputs,
        history=history,
        latest=latest,
        transitions=transitions,
    )
    publish_state = build_regime_publish_state(
        published_as_of_date=publish_window_metadata["published_as_of_date"],
        input_as_of_date=publish_window_metadata["input_as_of_date"],
        history_rows=int(len(history)),
        latest_rows=int(len(latest)),
        transition_rows=int(len(transitions)),
        active_models=[
            {"model_name": model_name, "model_version": model_version}
            for model_name, model_version in active_models
        ],
        downstream_triggered=False,
        warnings=publish_window_warnings,
        status=publication_status,
        reason=publication_reason,
        failure_mode=publication_failure_mode,
    )
    _replace_postgres_tables(
        dsn,
        macro_inputs=published_macro_inputs,
        inputs=published_inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=active_models,
    )
    finalization = finalize_regime_publication(
        gold_container=gold_container,
        inputs=published_inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=active_revisions,
        publish_state=publish_state,
        job_name=JOB_NAME,
        watermark_key=WATERMARK_KEY,
        when=computed_at,
        after_artifact_published_fn=_record_regime_reconcile_signal_after_artifact,
    )
    if finalization.status not in {"published", _PARTIAL_SUCCESS_STATUS}:
        return 1

    mdc.write_line(
        "Gold regime complete: "
        f"macro_input_rows={len(macro_inputs)} published_macro_input_rows={len(published_macro_inputs)} "
        f"inputs_rows={len(inputs)} published_inputs_rows={len(published_inputs)} "
        f"history_rows={len(history)} latest_rows={len(latest)} transition_rows={len(transitions)} "
        f"active_models={len(active_models)} published_as_of_date={publish_window_metadata['published_as_of_date']} "
        f"input_as_of_date={publish_window_metadata['input_as_of_date']} "
        f"skipped_trailing_count={len(publish_window.skipped_trailing_input_dates)} "
        f"input_readiness_attempts={readiness.attempts_used} status={publication_status} "
        "downstream_triggered=false"
    )
    return 0


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    job_name = JOB_NAME
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=False)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
            )
        )
