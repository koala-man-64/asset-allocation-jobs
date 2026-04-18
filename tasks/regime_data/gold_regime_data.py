from __future__ import annotations

import hashlib
import json
import time
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from math import sqrt
from typing import Any, NoReturn, Sequence

import pandas as pd

from asset_allocation_contracts.regime import RegimeModelConfig
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.postgres import connect, copy_rows
from asset_allocation_runtime_common.domain.regime import build_regime_outputs, compute_curve_state, compute_trend_state
from asset_allocation_runtime_common.regime_repository import RegimeRepository
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.market_data.market_symbols import REGIME_REQUIRED_MARKET_SYMBOLS
from asset_allocation_runtime_common.market_data.gold_sync_contracts import load_domain_sync_state
from tasks.common.job_trigger import ensure_api_awake_from_env
from tasks.common.system_health_markers import write_system_health_marker
from tasks.common.watermarks import save_last_success, save_watermarks

JOB_NAME = "gold-regime-job"
WATERMARK_KEY = "gold_regime_features"
_INPUTS_COLUMNS = (
    "as_of_date",
    "spy_close",
    "return_1d",
    "return_20d",
    "rvol_10d_ann",
    "vix_spot_close",
    "vix3m_close",
    "vix_slope",
    "trend_state",
    "curve_state",
    "vix_gt_32_streak",
    "inputs_complete_flag",
    "computed_at",
)
_HISTORY_COLUMNS = (
    "as_of_date",
    "effective_from_date",
    "model_name",
    "model_version",
    "regime_code",
    "regime_status",
    "matched_rule_id",
    "halt_flag",
    "halt_reason",
    "spy_return_20d",
    "rvol_10d_ann",
    "vix_spot_close",
    "vix3m_close",
    "vix_slope",
    "trend_state",
    "curve_state",
    "vix_gt_32_streak",
    "computed_at",
)
_TRANSITIONS_COLUMNS = (
    "model_name",
    "model_version",
    "effective_from_date",
    "prior_regime_code",
    "new_regime_code",
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


_REGIME_APPLY_CONFIGS: tuple[_RegimeApplyConfig, ...] = (
    _RegimeApplyConfig(
        table="gold.regime_inputs_daily",
        columns=_INPUTS_COLUMNS,
        key_columns=("as_of_date",),
        scope="all_rows",
    ),
    _RegimeApplyConfig(
        table="gold.regime_history",
        columns=_HISTORY_COLUMNS,
        key_columns=("as_of_date", "model_name", "model_version"),
        scope="active_models",
    ),
    _RegimeApplyConfig(
        table="gold.regime_latest",
        columns=_HISTORY_COLUMNS,
        key_columns=("model_name", "model_version"),
        scope="active_models",
    ),
    _RegimeApplyConfig(
        table="gold.regime_transitions",
        columns=_TRANSITIONS_COLUMNS,
        key_columns=("model_name", "model_version", "effective_from_date"),
        scope="active_models",
    ),
)


def _require_postgres_dsn() -> str:
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for gold regime job.")
    return dsn


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


def _normalize_market_series(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["date"] = pd.to_datetime(out.get("date"), errors="coerce").dt.date
    out["symbol"] = out.get("symbol", pd.Series(dtype="string")).astype(str).str.strip().str.upper()
    out = out.dropna(subset=["date"]).reset_index(drop=True)
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


def _assert_complete_regime_inputs(inputs: pd.DataFrame, *, market_series: pd.DataFrame) -> None:
    complete_rows = inputs["inputs_complete_flag"].fillna(False) if "inputs_complete_flag" in inputs.columns else pd.Series(dtype="bool")
    if bool(complete_rows.any()):
        return

    inputs_range = "n/a"
    if not inputs.empty and "as_of_date" in inputs.columns:
        parsed_dates = pd.to_datetime(inputs["as_of_date"], errors="coerce").dropna()
        if not parsed_dates.empty:
            inputs_range = f"{parsed_dates.min().date().isoformat()}..{parsed_dates.max().date().isoformat()}"

    coverage = _summarize_market_series_coverage(market_series)
    _fail_fast(
        "Gold regime fast-fail: gold regime inputs contain no complete SPY/^VIX/^VIX3M rows. "
        f"inputs_range={inputs_range}. coverage={coverage}. "
        "Upstream dependency gold-market-job has not produced overlapping index history in Postgres."
    )


def _resolve_publish_window(inputs: pd.DataFrame, *, market_series: pd.DataFrame) -> _RegimePublishWindow:
    _assert_complete_regime_inputs(inputs, market_series=market_series)
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


def _load_market_series(dsn: str) -> pd.DataFrame:
    with connect(dsn) as conn:
        frame = pd.read_sql_query(
            """
            SELECT symbol, date, close, return_1d, return_20d
            FROM gold.market_data
            WHERE symbol = ANY(%s)
            ORDER BY date ASC, symbol ASC
            """,
            conn,
            params=(list(REGIME_REQUIRED_MARKET_SYMBOLS),),
        )
    normalized = _normalize_market_series(frame)
    return _validate_required_market_series(normalized, dsn=dsn)


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


def _build_inputs_daily(market_series: pd.DataFrame, *, computed_at: datetime) -> pd.DataFrame:
    spy = (
        market_series[market_series["symbol"] == "SPY"][["date", "close", "return_1d", "return_20d"]]
        .rename(columns={"date": "as_of_date", "close": "spy_close"})
        .copy()
    )
    vix = (
        market_series[market_series["symbol"] == "^VIX"][["date", "close"]]
        .rename(columns={"date": "as_of_date", "close": "vix_spot_close"})
        .copy()
    )
    vix3m = (
        market_series[market_series["symbol"] == "^VIX3M"][["date", "close"]]
        .rename(columns={"date": "as_of_date", "close": "vix3m_close"})
        .copy()
    )

    inputs = spy.merge(vix, on="as_of_date", how="outer").merge(vix3m, on="as_of_date", how="outer")
    inputs = inputs.sort_values("as_of_date").reset_index(drop=True)
    inputs["vix_slope"] = inputs["vix3m_close"] - inputs["vix_spot_close"]
    inputs["rvol_10d_ann"] = inputs["return_1d"].rolling(window=10, min_periods=10).std(ddof=1) * sqrt(252.0) * 100.0

    inputs["vix_gt_32_streak"] = _compute_vix_streak(inputs["vix_spot_close"].tolist(), threshold=32.0)
    inputs["trend_state"] = inputs["return_20d"].map(lambda value: compute_trend_state(value))
    inputs["curve_state"] = inputs["vix_slope"].map(lambda value: compute_curve_state(value))
    inputs["inputs_complete_flag"] = inputs[
        [
            "spy_close",
            "return_1d",
            "return_20d",
            "rvol_10d_ann",
            "vix_spot_close",
            "vix3m_close",
            "vix_slope",
        ]
    ].notna().all(axis=1)
    inputs["computed_at"] = pd.Timestamp(computed_at)
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
    return (
        revision_inputs[
            [
                "as_of_date",
                "return_1d",
                "return_20d",
                "rvol_10d_ann",
                "vix_spot_close",
                "vix3m_close",
                "vix_slope",
                "vix_gt_32_streak",
                "inputs_complete_flag",
            ]
        ].copy(),
        resolved_config,
    )


def _replace_postgres_tables(
    dsn: str,
    *,
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
                frame=inputs,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[1],
                frame=history,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[2],
                frame=latest,
                active_models_count=len(active_models),
            )
            _apply_regime_table(
                cur,
                config=_REGIME_APPLY_CONFIGS[3],
                frame=transitions,
                active_models_count=len(active_models),
            )


def _write_storage_outputs(
    *,
    gold_container: str,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
    active_revisions: Sequence[dict[str, Any]],
    warnings: Sequence[str] = (),
) -> None:
    client = mdc.get_storage_client(gold_container)
    if client is None:
        raise ValueError(f"Storage client unavailable for container '{gold_container}'.")
    client.write_parquet("regime/inputs.parquet", inputs)
    client.write_parquet("regime/history.parquet", history)
    client.write_parquet("regime/latest.parquet", latest)
    client.write_parquet("regime/transitions.parquet", transitions)

    history_dates = pd.to_datetime(history["as_of_date"], errors="coerce").dropna() if not history.empty else pd.Series(dtype="datetime64[ns]")
    date_range = None
    if not history_dates.empty:
        date_range = {
            "min": history_dates.min().isoformat(),
            "max": history_dates.max().isoformat(),
            "column": "as_of_date",
            "source": "artifact",
        }
    source_fingerprint = hashlib.md5(
        json.dumps(
            {
                "activeModels": [
                    {
                        "name": revision.get("name"),
                        "version": revision.get("version"),
                        "activatedAt": revision.get("activated_at"),
                    }
                    for revision in active_revisions
                ],
                "dateRange": date_range,
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    artifact_path = domain_artifacts.domain_artifact_path(layer="gold", domain="regime")
    now = computed_at_iso()
    payload = {
        "version": 1,
        "scope": "domain",
        "layer": "gold",
        "domain": "regime",
        "rootPath": "regime",
        "artifactPath": artifact_path,
        "updatedAt": now,
        "computedAt": now,
        "publishedAt": now,
        "producerJobName": JOB_NAME,
        "sourceCommit": source_fingerprint,
        "symbolCount": 0,
        "columnCount": len(sorted(set(inputs.columns) | set(history.columns) | set(latest.columns) | set(transitions.columns))),
        "columns": sorted(set(inputs.columns) | set(history.columns) | set(latest.columns) | set(transitions.columns)),
        "dateRange": date_range,
        "affectedAsOfStart": date_range.get("min") if isinstance(date_range, dict) else None,
        "affectedAsOfEnd": date_range.get("max") if isinstance(date_range, dict) else None,
        "totalRows": int(len(history)),
        "fileCount": 4,
        "warnings": list(warnings),
    }
    domain_artifacts.publish_domain_artifact_payload(payload=payload, client=client)


def computed_at_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    mdc.log_environment_diagnostics()
    dsn = _require_postgres_dsn()
    gold_container = str(os.environ.get("AZURE_CONTAINER_GOLD") or "").strip()
    if not gold_container:
        raise ValueError("AZURE_CONTAINER_GOLD is required for gold regime job.")

    computed_at = datetime.now(timezone.utc)
    market_series = _load_market_series(dsn)
    repo = RegimeRepository(dsn)
    active_revisions = repo.list_active_regime_model_revisions()
    if not active_revisions:
        raise ValueError("No active regime model revisions found.")
    inputs = _build_inputs_daily(market_series, computed_at=computed_at)
    publish_window = _resolve_publish_window(inputs, market_series=market_series)
    publish_window_metadata = _publish_window_metadata(publish_window)
    publish_window_warnings = _publish_window_warnings(publish_window)
    published_inputs = _published_inputs(inputs, window=publish_window)
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

    history_frames: list[pd.DataFrame] = []
    latest_frames: list[pd.DataFrame] = []
    transition_frames: list[pd.DataFrame] = []
    active_models: list[tuple[str, int]] = []

    for revision in active_revisions:
        model_name = str(revision["name"])
        model_version = int(revision["version"])
        revision_inputs, resolved_config = _build_revision_inputs(
            published_inputs,
            config=revision.get("config") or {},
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

    _replace_postgres_tables(
        dsn,
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=active_models,
    )
    _write_storage_outputs(
        gold_container=gold_container,
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_revisions=active_revisions,
        warnings=publish_window_warnings,
    )

    save_watermarks(
        WATERMARK_KEY,
        publish_window_metadata
        | {
            "as_of_date": publish_window_metadata["published_as_of_date"],
            "history_rows": int(len(history)),
            "active_models": [
                {"model_name": model_name, "model_version": model_version}
                for model_name, model_version in active_models
            ],
        },
    )
    save_last_success(
        WATERMARK_KEY,
        when=computed_at,
        metadata=publish_window_metadata
        | {
            "as_of_date": publish_window_metadata["published_as_of_date"],
            "history_rows": int(len(history)),
            "latest_rows": int(len(latest)),
            "transition_rows": int(len(transitions)),
        },
    )
    mdc.write_line(
        "Gold regime complete: "
        f"inputs_rows={len(inputs)} published_inputs_rows={len(published_inputs)} "
        f"history_rows={len(history)} latest_rows={len(latest)} transition_rows={len(transitions)} "
        f"active_models={len(active_models)} published_as_of_date={publish_window_metadata['published_as_of_date']} "
        f"input_as_of_date={publish_window_metadata['input_as_of_date']} "
        f"skipped_trailing_count={len(publish_window.skipped_trailing_input_dates)}"
    )
    return 0


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = JOB_NAME
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="gold", domain="regime", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
