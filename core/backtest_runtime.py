from __future__ import annotations

import logging
import math
import os
import re
import time as monotonic_time
import uuid
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd
from asset_allocation_runtime_common import BACKTEST_RESULTS_SCHEMA_VERSION, persist_backtest_results

from core.backtest_repository import BacktestRepository
from core.postgres import connect
from core.ranking_engine import service as ranking_service
from core.ranking_engine.contracts import RankingSchemaConfig
from asset_allocation_contracts.regime import DEFAULT_REGIME_MODEL_NAME, RegimePolicy
from core.regime_repository import RegimeRepository
from core.ranking_repository import RankingRepository
from core.strategy_engine import StrategyConfig, UniverseDefinition
from core.strategy_engine.exit_rules import ExitRuleEvaluator
from core.strategy_engine.position_state import PositionState, PriceBar
from core.strategy_engine import universe as universe_service
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

_PRICE_TABLE = "market_data"
_PRICE_COLUMNS = {"open", "high", "low", "close", "volume"}
_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
_TRADING_DAYS_PER_YEAR = 252.0
_TRADING_MINUTES_PER_DAY = 390.0
_DEFAULT_ROLLING_WINDOW_DAYS = 63


@dataclass(frozen=True)
class ResolvedBacktestDefinition:
    strategy_name: str
    strategy_version: int | None
    strategy_config: StrategyConfig
    strategy_config_raw: dict[str, Any]
    strategy_universe: UniverseDefinition
    ranking_schema_name: str
    ranking_schema_version: int | None
    ranking_schema: RankingSchemaConfig
    ranking_universe_name: str | None
    ranking_universe_version: int | None
    ranking_universe: UniverseDefinition
    regime_model_name: str | None = None
    regime_model_version: int | None = None
    regime_model_config: dict[str, Any] | None = None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _session_bounds(ts: datetime) -> tuple[datetime, datetime]:
    start = datetime.combine(ts.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine(ts.date(), time.max, tzinfo=timezone.utc)
    return start, end


def _normalize_timestamp_value(value: Any, *, kind: str) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Unable to normalize timestamp value: {value!r}")
    if kind == "slower":
        parsed = parsed.normalize()
    return parsed.to_pydatetime()


def _bounds_for_spec(spec: universe_service.UniverseTableSpec, start_ts: datetime, end_ts: datetime) -> tuple[Any, Any]:
    if spec.as_of_kind == "intraday":
        return start_ts, end_ts
    return start_ts.date(), end_ts.date()


def _load_run_schedule(
    dsn: str,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> list[datetime]:
    start_bound, end_bound = _bounds_for_spec(table_spec, start_ts, end_ts)
    sql = f"""
        SELECT DISTINCT {universe_service._quote_identifier(table_spec.as_of_column)} AS as_of_value
        FROM "gold".{universe_service._quote_identifier(table_name)}
        WHERE {universe_service._quote_identifier(table_spec.as_of_column)} >= %s
          AND {universe_service._quote_identifier(table_spec.as_of_column)} <= %s
    """
    params: list[Any] = [start_bound, end_bound]
    if bar_size and "bar_size" in table_spec.columns:
        sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
        params.append(bar_size)
    sql += " ORDER BY as_of_value"

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [_normalize_timestamp_value(row[0], kind=table_spec.as_of_kind) for row in rows if row and row[0] is not None]


def _load_exact_coverage(
    dsn: str,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> set[datetime]:
    return set(
        _load_run_schedule(
            dsn,
            table_name=table_name,
            table_spec=table_spec,
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=bar_size,
        )
    )


def _value_series(
    raw: pd.Series,
    *,
    column_spec: universe_service.UniverseColumnSpec,
) -> pd.Series:
    if column_spec.value_kind == "number":
        return pd.to_numeric(raw, errors="coerce")
    if column_spec.value_kind == "boolean":
        return raw.astype("boolean")
    if column_spec.value_kind in {"date", "datetime"}:
        return pd.to_datetime(raw, utc=True, errors="coerce")
    return raw.astype("string")


def _prepare_loaded_frame(
    frame: pd.DataFrame,
    *,
    table_name: str,
    table_spec: universe_service.UniverseTableSpec,
    selected_columns: Iterable[str],
) -> pd.DataFrame:
    normalized_columns = list(selected_columns)
    if frame.empty:
        return pd.DataFrame(columns=["as_of", "symbol", *[f"{table_name}__{name}" for name in normalized_columns]])
    out = frame.copy()
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()
    out["as_of"] = pd.to_datetime(out["as_of"], utc=True, errors="coerce")
    for column_name in normalized_columns:
        out[f"{table_name}__{column_name}"] = _value_series(out[column_name], column_spec=table_spec.columns[column_name])
    return out[["as_of", "symbol", *[f"{table_name}__{name}" for name in normalized_columns]]]


def _load_intraday_session_frames(
    dsn: str,
    *,
    table_specs: dict[str, universe_service.UniverseTableSpec],
    required_columns: dict[str, set[str]],
    session_start: datetime,
    session_end: datetime,
    bar_size: str | None,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs[table_name]
            if spec.as_of_kind != "intraday":
                continue
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS as_of",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            sql = f"""
                SELECT {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} >= %s
                  AND {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            params: list[Any] = [session_start, session_end]
            if bar_size and "bar_size" in spec.columns:
                sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
                params.append(bar_size)
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            frames[table_name] = _prepare_loaded_frame(
                frame,
                table_name=table_name,
                table_spec=spec,
                selected_columns=selected_columns,
            )
    return frames


def _load_slow_frames(
    dsn: str,
    *,
    table_specs: dict[str, universe_service.UniverseTableSpec],
    required_columns: dict[str, set[str]],
    as_of_ts: datetime,
    bar_size: str | None,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs[table_name]
            if spec.as_of_kind == "intraday":
                continue
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS as_of",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            sql = f"""
                SELECT DISTINCT ON ({universe_service._quote_identifier('symbol')})
                    {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            params: list[Any] = [as_of_ts.date()]
            if bar_size and "bar_size" in spec.columns:
                sql += f" AND {universe_service._quote_identifier('bar_size')} = %s"
                params.append(bar_size)
            sql += f"""
                ORDER BY
                    {universe_service._quote_identifier('symbol')},
                    {universe_service._quote_identifier(spec.as_of_column)} DESC NULLS LAST
            """
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            frames[table_name] = _prepare_loaded_frame(
                frame,
                table_name=table_name,
                table_spec=spec,
                selected_columns=selected_columns,
            )
    return frames


def _snapshot_for_timestamp(
    ts: datetime,
    *,
    intraday_frames: dict[str, pd.DataFrame],
    slow_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for frame in intraday_frames.values():
        if frame.empty:
            continue
        exact = frame[frame["as_of"] == pd.Timestamp(ts)]
        if exact.empty:
            continue
        frames.append(exact.drop(columns=["as_of"], errors="ignore"))
    for frame in slow_frames.values():
        if frame.empty:
            continue
        frames.append(frame.drop(columns=["as_of"], errors="ignore"))

    merged: pd.DataFrame | None = None
    for frame in frames:
        frame = frame.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
        if merged is None:
            merged = frame.copy()
        else:
            merged = merged.merge(frame, on="symbol", how="outer")
    if merged is None:
        return pd.DataFrame(columns=["date", "symbol"])
    merged["date"] = pd.Timestamp(ts)
    merged = merged.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    return merged


def _resolve_strategy_universe(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    fallback_universe: UniverseDefinition,
) -> UniverseDefinition:
    if strategy_config.universe is not None:
        return strategy_config.universe
    if strategy_config.universeConfigName:
        record = UniverseRepository(dsn).get_universe_config(strategy_config.universeConfigName)
        if record:
            return UniverseDefinition.model_validate(record.get("config") or {})
    return fallback_universe


def resolve_backtest_definition(
    dsn: str,
    *,
    strategy_name: str,
    strategy_version: int | None = None,
    regime_model_name: str | None = None,
    regime_model_version: int | None = None,
) -> ResolvedBacktestDefinition:
    strategy_repo = StrategyRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    universe_repo = UniverseRepository(dsn)

    strategy_revision = strategy_repo.get_strategy_revision(strategy_name, strategy_version)
    if strategy_revision:
        strategy_config_raw = dict(strategy_revision.get("config") or {})
    else:
        strategy_record = strategy_repo.get_strategy(strategy_name)
        if not strategy_record:
            raise ValueError(f"Strategy '{strategy_name}' not found.")
        strategy_config_raw = dict(strategy_record.get("config") or {})

    strategy_config = StrategyConfig.model_validate(strategy_config_raw)
    ranking_schema_name = str(
        (strategy_revision or {}).get("ranking_schema_name") or strategy_config.rankingSchemaName or ""
    ).strip()
    if not ranking_schema_name:
        raise ValueError(f"Strategy '{strategy_name}' does not reference a ranking schema.")

    ranking_schema_version = (
        int(strategy_revision["ranking_schema_version"])
        if strategy_revision and strategy_revision.get("ranking_schema_version") is not None
        else None
    )
    ranking_record = ranking_repo.get_ranking_schema_revision(ranking_schema_name, ranking_schema_version)
    if not ranking_record:
        raise ValueError(f"Ranking schema '{ranking_schema_name}' not found.")
    ranking_schema = RankingSchemaConfig.model_validate(ranking_record.get("config") or {})

    ranking_universe_name = str(
        (strategy_revision or {}).get("universe_name")
        or ranking_record.get("config", {}).get("universeConfigName")
        or ranking_schema.universeConfigName
        or ""
    ).strip() or None
    if not ranking_universe_name:
        raise ValueError(f"Ranking schema '{ranking_schema_name}' does not reference a universe config.")
    ranking_universe_version = (
        int(strategy_revision["universe_version"])
        if strategy_revision and strategy_revision.get("universe_version") is not None
        else None
    )
    universe_record = universe_repo.get_universe_config_revision(ranking_universe_name, ranking_universe_version)
    if not universe_record:
        raise ValueError(f"Universe config '{ranking_universe_name}' not found.")
    ranking_universe = UniverseDefinition.model_validate(universe_record.get("config") or {})
    strategy_universe = _resolve_strategy_universe(
        dsn,
        strategy_config=strategy_config,
        fallback_universe=ranking_universe,
    )
    resolved_regime_name, resolved_regime_version, resolved_regime_config = _resolve_regime_revision(
        dsn,
        strategy_config=strategy_config,
        regime_model_name=regime_model_name,
        regime_model_version=regime_model_version,
    )
    return ResolvedBacktestDefinition(
        strategy_name=strategy_name,
        strategy_version=(int(strategy_revision["version"]) if strategy_revision else None),
        strategy_config=strategy_config,
        strategy_config_raw=strategy_config_raw,
        strategy_universe=strategy_universe,
        ranking_schema_name=ranking_schema_name,
        ranking_schema_version=int(ranking_record["version"]),
        ranking_schema=ranking_schema,
        ranking_universe_name=ranking_universe_name,
        ranking_universe_version=int(universe_record["version"]),
        ranking_universe=ranking_universe,
        regime_model_name=resolved_regime_name,
        regime_model_version=resolved_regime_version,
        regime_model_config=resolved_regime_config,
    )


def _required_columns(definition: ResolvedBacktestDefinition) -> dict[str, set[str]]:
    required = ranking_service._collect_required_columns(
        definition.strategy_universe,
        definition.ranking_universe,
        definition.ranking_schema,
    )
    required.setdefault(_PRICE_TABLE, set()).update(_PRICE_COLUMNS)
    for rule in definition.strategy_config.exits:
        if rule.atrColumn:
            required[_PRICE_TABLE].add(str(rule.atrColumn))
    return required


def validate_backtest_submission(
    dsn: str,
    *,
    definition: ResolvedBacktestDefinition,
    start_ts: datetime,
    end_ts: datetime,
    bar_size: str | None,
) -> list[datetime]:
    table_specs = universe_service._load_gold_table_specs(dsn)
    required = _required_columns(definition)
    missing_tables = [name for name in required if name not in table_specs]
    if missing_tables:
        raise ValueError(f"Missing required gold tables: {sorted(missing_tables)}")

    price_spec = table_specs[_PRICE_TABLE]
    intraday_tables = sorted(name for name, spec in table_specs.items() if name in required and spec.as_of_kind == "intraday")
    schedule_source_name = _PRICE_TABLE if price_spec.as_of_kind == "intraday" else (intraday_tables[0] if intraday_tables else _PRICE_TABLE)
    schedule_source = table_specs[schedule_source_name]
    schedule = _load_run_schedule(
        dsn,
        table_name=schedule_source_name,
        table_spec=schedule_source,
        start_ts=start_ts,
        end_ts=end_ts,
        bar_size=bar_size,
    )
    if len(schedule) < 2:
        raise ValueError("Backtest window must resolve to at least two bars.")
    if intraday_tables and price_spec.as_of_kind != "intraday":
        raise ValueError(
            "Execution price table 'market_data' is not intraday while intraday feature tables are required."
        )

    schedule_set = set(schedule)
    for table_name in intraday_tables:
        coverage = _load_exact_coverage(
            dsn,
            table_name=table_name,
            table_spec=table_specs[table_name],
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=bar_size,
        )
        missing = sorted(schedule_set.difference(coverage))
        if missing:
            sample = ", ".join(item.isoformat() for item in missing[:5])
            raise ValueError(
                f"Intraday feature coverage gap for gold.{table_name}; missing {len(missing)} rebalance bars, sample={sample}"
            )
    if definition.regime_model_name and definition.regime_model_version is not None:
        _validate_regime_history_coverage(
            dsn,
            model_name=definition.regime_model_name,
            model_version=definition.regime_model_version,
            schedule=schedule,
        )
    return schedule


def _score_snapshot(
    snapshot: pd.DataFrame,
    *,
    definition: ResolvedBacktestDefinition,
    rebalance_ts: datetime,
    target_weight_multiplier: float = 1.0,
) -> pd.DataFrame:
    if snapshot.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])
    filtered = snapshot[
        ranking_service._evaluate_universe_mask(snapshot, definition.strategy_universe.root)
        & ranking_service._evaluate_universe_mask(snapshot, definition.ranking_universe.root)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    group_scores: list[tuple[str, float, pd.Series]] = []
    required_masks: list[pd.Series] = []
    for group in definition.ranking_schema.groups:
        group_series, group_required_masks = ranking_service._score_group(filtered, group)
        group_scores.append((group.name, group.weight, group_series))
        required_masks.extend(group_required_masks)

    if required_masks:
        keep_mask = pd.concat(required_masks, axis=1).all(axis=1)
        filtered = filtered[keep_mask].copy()
        group_scores = [(name, weight, series.loc[filtered.index]) for name, weight, series in group_scores]
        if filtered.empty:
            return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    weighted_total = pd.Series(0.0, index=filtered.index)
    total_weight = 0.0
    for _name, weight, series in group_scores:
        weighted_total = weighted_total.add(series * weight, fill_value=0.0)
        total_weight += weight
    if total_weight <= 0:
        raise ValueError("Ranking schema produced zero total group weight.")
    filtered["score"] = weighted_total / total_weight
    filtered["score"] = ranking_service._apply_transforms(
        filtered["score"],
        filtered["date"],
        definition.ranking_schema.overallTransforms,
    )
    filtered = filtered.dropna(subset=["score"]).copy()
    if filtered.empty:
        return pd.DataFrame(columns=["symbol", "score", "ordinal", "selected", "target_weight", "rebalance_ts"])

    filtered = filtered.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)
    filtered["ordinal"] = np.arange(1, len(filtered) + 1)
    top_n = min(definition.strategy_config.topN, len(filtered))
    filtered["selected"] = filtered["ordinal"] <= top_n
    target_weight = float(target_weight_multiplier) / top_n if top_n > 0 else 0.0
    filtered["target_weight"] = np.where(filtered["selected"], target_weight, 0.0)
    filtered["rebalance_ts"] = pd.Timestamp(rebalance_ts)
    return filtered[["rebalance_ts", "symbol", "score", "ordinal", "selected", "target_weight"]]


def _market_row(snapshot: pd.DataFrame | dict[str, pd.Series], symbol: str) -> pd.Series | None:
    if isinstance(snapshot, dict):
        return snapshot.get(symbol)
    matches = snapshot[snapshot["symbol"] == symbol]
    if matches.empty:
        return None
    return matches.iloc[0]


def _build_snapshot_symbol_index(snapshot: pd.DataFrame) -> dict[str, pd.Series]:
    if snapshot.empty or "symbol" not in snapshot.columns:
        return {}
    indexed: dict[str, pd.Series] = {}
    deduped = snapshot.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    for _, row in deduped.iterrows():
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            indexed[symbol] = row
    return indexed


def _price_bar(ts: datetime, row: pd.Series) -> PriceBar:
    features = {
        column.removeprefix(f"{_PRICE_TABLE}__"): row[column]
        for column in row.index
        if str(column).startswith(f"{_PRICE_TABLE}__")
    }
    return PriceBar(
        date=ts,
        open=_maybe_float(features.get("open")),
        high=_maybe_float(features.get("high")),
        low=_maybe_float(features.get("low")),
        close=_maybe_float(features.get("close")),
        features=features,
    )


def _maybe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return float(int(value))
    try:
        return float(value)
    except Exception:
        return None


def _apply_rebalance_target(
    position: PositionState | None,
    *,
    symbol: str,
    entry_date: datetime,
    entry_price: float,
    target_quantity: float,
) -> PositionState | None:
    if target_quantity <= 1e-9:
        return None
    if position is None:
        return PositionState(
            symbol=symbol,
            entry_date=entry_date,
            entry_price=entry_price,
            quantity=float(target_quantity),
        )
    return replace(position, quantity=float(target_quantity))


def _new_position_id() -> str:
    return uuid.uuid4().hex


def _trade_role_for_target(*, current_quantity: float, target_quantity: float) -> str:
    if current_quantity <= 1e-9:
        return "entry"
    if target_quantity <= 1e-9:
        return "exit"
    if target_quantity > current_quantity:
        return "rebalance_increase"
    return "rebalance_decrease"


def _apply_trade_to_position(
    position: PositionState | None,
    *,
    symbol: str,
    ts: datetime,
    quantity_delta: float,
    trade_price: float,
    commission: float,
    slippage: float,
    position_id: str | None = None,
    exit_reason: str | None = None,
    exit_rule_id: str | None = None,
) -> tuple[PositionState | None, dict[str, Any] | None]:
    if math.isclose(quantity_delta, 0.0, abs_tol=1e-12):
        return position, None

    trade_cost = float(commission + slippage)
    if position is None:
        if quantity_delta <= 0.0:
            return None, None
        created = PositionState(
            position_id=position_id or _new_position_id(),
            symbol=symbol,
            entry_date=ts,
            entry_price=float(trade_price),
            quantity=float(quantity_delta),
            opened_at=ts,
            average_cost=float(trade_price),
            commission_accrued=float(commission),
            slippage_accrued=float(slippage),
            max_quantity=float(abs(quantity_delta)),
            realized_pnl_accrued=float(-trade_cost),
        )
        return created, None

    current_quantity = float(position.quantity)
    average_cost = float(position.average_cost or position.entry_price)
    new_quantity = float(current_quantity + quantity_delta)
    total_commission = float(position.commission_accrued + commission)
    total_slippage = float(position.slippage_accrued + slippage)
    max_quantity = float(max(position.max_quantity or abs(current_quantity), abs(new_quantity)))

    if quantity_delta > 0.0:
        weighted_cost = (current_quantity * average_cost) + (float(quantity_delta) * float(trade_price))
        updated = replace(
            position,
            quantity=float(new_quantity),
            average_cost=float(weighted_cost / new_quantity),
            commission_accrued=total_commission,
            slippage_accrued=total_slippage,
            max_quantity=max_quantity,
            resize_count=position.resize_count + 1,
            realized_pnl_accrued=float(position.realized_pnl_accrued - trade_cost),
        )
        return updated, None

    sell_quantity = float(min(current_quantity, abs(quantity_delta)))
    realized_basis_increment = float(sell_quantity * average_cost)
    realized_pnl_accrued = float(
        position.realized_pnl_accrued + (sell_quantity * (float(trade_price) - average_cost)) - trade_cost
    )
    realized_basis_accrued = float(position.realized_basis_accrued + realized_basis_increment)

    if new_quantity <= 1e-9:
        total_transaction_cost = float(total_commission + total_slippage)
        realized_return = float(realized_pnl_accrued / realized_basis_accrued) if realized_basis_accrued > 0 else 0.0
        closed_position = {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "opened_at": (
                position.opened_at.isoformat()
                if isinstance(position.opened_at, datetime)
                else str(position.opened_at)
            ),
            "closed_at": ts.isoformat(),
            "holding_period_bars": int(position.bars_held),
            "average_cost": float(average_cost),
            "exit_price": float(trade_price),
            "max_quantity": float(position.max_quantity or abs(current_quantity)),
            "resize_count": int(position.resize_count),
            "realized_pnl": realized_pnl_accrued,
            "realized_return": realized_return,
            "total_commission": total_commission,
            "total_slippage_cost": total_slippage,
            "total_transaction_cost": total_transaction_cost,
            "exit_reason": exit_reason,
            "exit_rule_id": exit_rule_id,
        }
        return None, closed_position

    updated = replace(
        position,
        quantity=float(new_quantity),
        commission_accrued=total_commission,
        slippage_accrued=total_slippage,
        max_quantity=max_quantity,
        resize_count=position.resize_count + 1,
        realized_pnl_accrued=realized_pnl_accrued,
        realized_basis_accrued=realized_basis_accrued,
    )
    return updated, None


def _heartbeat_interval_seconds() -> float:
    raw_value = str(os.environ.get("BACKTEST_HEARTBEAT_INTERVAL_SECONDS") or "").strip()
    if not raw_value:
        return _DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    try:
        return max(5.0, float(raw_value))
    except Exception:
        return _DEFAULT_HEARTBEAT_INTERVAL_SECONDS


def _normalize_bar_size(bar_size: str | None) -> str | None:
    normalized = str(bar_size or "").strip().lower()
    return normalized or None


def _periods_per_year_from_bar_size(bar_size: str | None) -> float:
    normalized = _normalize_bar_size(bar_size)
    if normalized is None or normalized in {"1d", "d", "day", "days", "daily"}:
        return _TRADING_DAYS_PER_YEAR

    match = re.fullmatch(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>[a-z]+)", normalized)
    if not match:
        raise ValueError(f"Unsupported bar_size '{bar_size}'.")

    value = float(match.group("value"))
    if value <= 0:
        raise ValueError(f"Unsupported bar_size '{bar_size}'.")

    unit = match.group("unit")
    if unit in {"m", "min", "mins", "minute", "minutes"}:
        return _TRADING_DAYS_PER_YEAR * (_TRADING_MINUTES_PER_DAY / value)
    if unit in {"h", "hr", "hrs", "hour", "hours"}:
        return _TRADING_DAYS_PER_YEAR * (6.5 / value)
    if unit in {"w", "wk", "wks", "week", "weeks"}:
        return _TRADING_DAYS_PER_YEAR / (5.0 * value)
    if unit in {"mo", "mon", "month", "months"}:
        return _TRADING_DAYS_PER_YEAR / (21.0 * value)

    raise ValueError(f"Unsupported bar_size '{bar_size}'.")


def _rolling_window_periods(*, periods_per_year: float, window_days: int = _DEFAULT_ROLLING_WINDOW_DAYS) -> int:
    return max(1, int(round(window_days * periods_per_year / _TRADING_DAYS_PER_YEAR)))


def _log_stage_timing(phase: str, started_at: float, **fields: object) -> None:
    parts = [f"phase={phase}", f"duration_sec={monotonic_time.monotonic() - started_at:.2f}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    logger.info("backtest_stage_timing %s", " ".join(parts))


def _maybe_update_heartbeat(
    repo: BacktestRepository,
    *,
    run_id: str,
    state: dict[str, Any],
    phase: str,
    force: bool = False,
) -> bool:
    interval_seconds = float(state["interval_seconds"])
    now_monotonic = monotonic_time.monotonic()
    last_heartbeat_at = state.get("last_heartbeat_at")
    if not force and last_heartbeat_at is not None and (now_monotonic - float(last_heartbeat_at)) < interval_seconds:
        return False
    if last_heartbeat_at is not None and (now_monotonic - float(last_heartbeat_at)) > (interval_seconds * 1.5):
        logger.warning(
            "backtest_lifecycle_event phase=heartbeat_delay run_id=%s delay_sec=%.2f threshold_sec=%.2f",
            run_id,
            now_monotonic - float(last_heartbeat_at),
            interval_seconds,
        )
    repo.update_heartbeat(run_id)
    state["last_heartbeat_at"] = now_monotonic
    logger.info("backtest_lifecycle_event phase=heartbeat run_id=%s heartbeat_phase=%s", run_id, phase)
    return True


def _costs_from_raw_config(raw: dict[str, Any]) -> tuple[float, float]:
    costs = raw.get("costs") if isinstance(raw, dict) else None
    if not isinstance(costs, dict):
        return 0.0, 0.0
    commission_bps = float(costs.get("commissionBps") or costs.get("commission_bps") or 0.0)
    slippage_bps = float(costs.get("slippageBps") or costs.get("slippage_bps") or 0.0)
    return commission_bps, slippage_bps


def _resolve_regime_revision(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    regime_model_name: str | None = None,
    regime_model_version: int | None = None,
) -> tuple[str | None, int | None, dict[str, Any] | None]:
    policy = strategy_config.regimePolicy
    if policy is None:
        return None, None, None

    resolved_name = str(regime_model_name or policy.modelName or DEFAULT_REGIME_MODEL_NAME).strip()
    if not resolved_name:
        resolved_name = DEFAULT_REGIME_MODEL_NAME

    repo = RegimeRepository(dsn)
    revision = (
        repo.get_regime_model_revision(resolved_name, version=regime_model_version)
        if regime_model_version is not None
        else repo.get_active_regime_model_revision(resolved_name)
    )
    if not revision:
        if regime_model_version is not None:
            raise ValueError(f"Regime model '{resolved_name}' version '{regime_model_version}' not found.")
        raise ValueError(f"Regime model '{resolved_name}' does not have an active revision.")
    return resolved_name, int(revision["version"]), dict(revision.get("config") or {})


def _load_regime_history_frame(
    dsn: str,
    *,
    model_name: str,
    model_version: int,
    max_effective_from_date: date,
) -> pd.DataFrame:
    sql = """
        SELECT
            as_of_date,
            effective_from_date,
            model_name,
            model_version,
            regime_code,
            regime_status,
            matched_rule_id,
            halt_flag,
            halt_reason,
            spy_return_20d,
            rvol_10d_ann,
            vix_spot_close,
            vix3m_close,
            vix_slope,
            trend_state,
            curve_state,
            vix_gt_32_streak,
            computed_at
        FROM gold.regime_history
        WHERE model_name = %s
          AND model_version = %s
          AND effective_from_date <= %s
        ORDER BY effective_from_date ASC, as_of_date ASC
    """
    with connect(dsn) as conn:
        frame = pd.read_sql_query(
            sql,
            conn,
            params=(model_name, int(model_version), max_effective_from_date),
        )
    if frame.empty:
        return frame
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce").dt.date
    frame["effective_from_date"] = pd.to_datetime(frame["effective_from_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["as_of_date", "effective_from_date"]).reset_index(drop=True)
    return frame


def _materialize_regime_schedule(
    regime_history: pd.DataFrame,
    *,
    session_dates: list[date],
) -> pd.DataFrame:
    schedule_frame = pd.DataFrame({"session_date": sorted(set(session_dates))})
    if schedule_frame.empty:
        return schedule_frame
    schedule_frame["session_date"] = pd.to_datetime(schedule_frame["session_date"], errors="coerce")
    if regime_history.empty:
        schedule_frame["effective_from_date"] = pd.NaT
        return schedule_frame

    history = regime_history.copy()
    history["effective_from_date"] = pd.to_datetime(history["effective_from_date"], errors="coerce")
    history = history.dropna(subset=["effective_from_date"]).sort_values(["effective_from_date", "as_of_date"])
    schedule_frame = schedule_frame.dropna(subset=["session_date"]).sort_values("session_date")
    merged = pd.merge_asof(
        schedule_frame,
        history,
        left_on="session_date",
        right_on="effective_from_date",
        direction="backward",
    )
    merged["session_date"] = pd.to_datetime(merged["session_date"], errors="coerce").dt.date
    return merged


def _validate_regime_history_coverage(
    dsn: str,
    *,
    model_name: str,
    model_version: int,
    schedule: list[datetime],
) -> None:
    session_dates = sorted({ts.date() for ts in schedule})
    if not session_dates:
        return
    history = _load_regime_history_frame(
        dsn,
        model_name=model_name,
        model_version=model_version,
        max_effective_from_date=max(session_dates),
    )
    merged = _materialize_regime_schedule(history, session_dates=session_dates)
    if merged.empty:
        raise ValueError(
            f"Regime history coverage gap for {model_name}@v{model_version}; no rows found for requested backtest window."
        )
    missing = merged[merged["effective_from_date"].isna()]
    if not missing.empty:
        sample = ", ".join(str(value) for value in missing["session_date"].astype(str).tolist()[:5])
        raise ValueError(
            f"Regime history coverage gap for {model_name}@v{model_version}; missing {len(missing)} session dates, sample={sample}"
        )


def _load_regime_schedule_map(
    dsn: str,
    *,
    definition: ResolvedBacktestDefinition,
    schedule: list[datetime],
) -> dict[date, dict[str, Any]]:
    if not definition.regime_model_name or definition.regime_model_version is None:
        return {}
    session_dates = sorted({ts.date() for ts in schedule})
    if not session_dates:
        return {}
    history = _load_regime_history_frame(
        dsn,
        model_name=definition.regime_model_name,
        model_version=definition.regime_model_version,
        max_effective_from_date=max(session_dates),
    )
    merged = _materialize_regime_schedule(history, session_dates=session_dates)
    regime_map: dict[date, dict[str, Any]] = {}
    for row in merged.to_dict("records"):
        session_date = row.get("session_date")
        if isinstance(session_date, date):
            regime_map[session_date] = row
    return regime_map


def _regime_context_for_session(
    policy: RegimePolicy | None,
    regime_row: dict[str, Any] | None,
) -> dict[str, Any]:
    if policy is None or not regime_row:
        return {
            "blocked": False,
            "blocked_reason": None,
            "blocked_action": None,
            "exposure_multiplier": 1.0,
            "regime_code": None,
            "regime_status": None,
            "halt_flag": False,
            "halt_reason": None,
            "matched_rule_id": None,
            "as_of_date": None,
            "effective_from_date": None,
        }

    regime_code = str(regime_row.get("regime_code") or "").strip() or None
    regime_status = str(regime_row.get("regime_status") or "").strip() or None
    halt_flag = bool(regime_row.get("halt_flag"))
    halt_reason = regime_row.get("halt_reason")
    blocked_reason: str | None = None

    if halt_flag and policy.honorHaltFlag:
        blocked_reason = "halt_flag"
    elif regime_status == "transition" and policy.blockOnTransition:
        blocked_reason = "transition"
    elif regime_code == "unclassified" and policy.blockOnUnclassified:
        blocked_reason = "unclassified"

    exposure_targets = policy.targetGrossExposureByRegime.model_dump(mode="python")
    exposure_multiplier = 1.0
    if blocked_reason is None and regime_code:
        exposure_multiplier = float(exposure_targets.get(regime_code, 1.0))

    return {
        "blocked": blocked_reason is not None,
        "blocked_reason": blocked_reason,
        "blocked_action": policy.onBlocked if blocked_reason is not None else None,
        "exposure_multiplier": exposure_multiplier,
        "regime_code": regime_code,
        "regime_status": regime_status,
        "halt_flag": halt_flag,
        "halt_reason": halt_reason,
        "matched_rule_id": regime_row.get("matched_rule_id"),
        "as_of_date": regime_row.get("as_of_date"),
        "effective_from_date": regime_row.get("effective_from_date"),
    }


def _execute_trade(
    *,
    trades: list[dict[str, Any]],
    ts: datetime,
    symbol: str,
    quantity_delta: float,
    price: float,
    cash: float,
    commission_bps: float,
    slippage_bps: float,
    position_id: str | None,
    trade_role: str | None,
) -> tuple[float, float, float]:
    if math.isclose(quantity_delta, 0.0, abs_tol=1e-12):
        return cash, 0.0, 0.0
    notional = float(quantity_delta * price)
    abs_notional = abs(notional)
    commission = abs_notional * commission_bps / 10000.0
    slippage = abs_notional * slippage_bps / 10000.0
    cash_after = cash - notional - commission - slippage
    trades.append(
        {
            "execution_date": ts.isoformat(),
            "symbol": symbol,
            "quantity": float(quantity_delta),
            "price": float(price),
            "notional": float(notional),
            "commission": float(commission),
            "slippage_cost": float(slippage),
            "cash_after": float(cash_after),
            "position_id": position_id,
            "trade_role": trade_role,
        }
    )
    return cash_after, commission, slippage


def _compute_summary(
    timeseries: pd.DataFrame,
    trades: pd.DataFrame,
    closed_positions: pd.DataFrame,
    *,
    run_id: str,
    run_name: str | None,
    periods_per_year: float,
    initial_cash_override: float | None = None,
) -> dict[str, Any]:
    def _empty_summary() -> dict[str, Any]:
        return {
            "run_id": run_id,
            "run_name": run_name,
            "total_return": 0.0,
            "annualized_return": 0.0,
            "annualized_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "trades": int(len(trades)),
            "initial_cash": float(initial_cash_override or 0.0),
            "final_equity": 0.0,
            "gross_total_return": 0.0,
            "gross_annualized_return": 0.0,
            "total_commission": 0.0,
            "total_slippage_cost": 0.0,
            "total_transaction_cost": 0.0,
            "cost_drag_bps": 0.0,
            "avg_gross_exposure": 0.0,
            "avg_net_exposure": 0.0,
            "sortino_ratio": 0.0,
            "calmar_ratio": 0.0,
            "closed_positions": 0,
            "winning_positions": 0,
            "losing_positions": 0,
            "hit_rate": 0.0,
            "avg_win_pnl": 0.0,
            "avg_loss_pnl": 0.0,
            "avg_win_return": 0.0,
            "avg_loss_return": 0.0,
            "payoff_ratio": 0.0,
            "profit_factor": 0.0,
            "expectancy_pnl": 0.0,
            "expectancy_return": 0.0,
        }

    if timeseries.empty:
        return _empty_summary()

    initial_cash = float(initial_cash_override if initial_cash_override is not None else timeseries["portfolio_value"].iloc[0])
    final_equity = float(timeseries["portfolio_value"].iloc[-1])
    gross_final_equity = float(timeseries.get("gross_portfolio_value", timeseries["portfolio_value"]).iloc[-1])
    total_return = (final_equity / initial_cash - 1.0) if initial_cash else 0.0
    gross_total_return = (gross_final_equity / initial_cash - 1.0) if initial_cash else 0.0
    returns = pd.to_numeric(timeseries["period_return"], errors="coerce").fillna(0.0)
    periods = max(len(returns), 1)
    annualization = float(periods_per_year)
    annualized_return = (1.0 + total_return) ** (annualization / periods) - 1.0 if periods > 0 else 0.0
    gross_annualized_return = (1.0 + gross_total_return) ** (annualization / periods) - 1.0 if periods > 0 else 0.0
    annualized_volatility = float(returns.std(ddof=0) * math.sqrt(annualization)) if len(returns) > 1 else 0.0
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0.0
    max_drawdown = float(pd.to_numeric(timeseries["drawdown"], errors="coerce").min() or 0.0)
    downside_returns = returns.where(returns < 0.0, 0.0)
    downside_deviation = float(np.sqrt(np.square(downside_returns).mean()) * math.sqrt(annualization)) if len(returns) else 0.0
    sortino_ratio = annualized_return / downside_deviation if downside_deviation > 0 else 0.0
    calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown < 0 else 0.0
    total_commission = float(pd.to_numeric(trades.get("commission"), errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
    total_slippage_cost = float(pd.to_numeric(trades.get("slippage_cost"), errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
    total_transaction_cost = float(total_commission + total_slippage_cost)
    cost_drag_bps = float(((gross_final_equity - final_equity) / initial_cash) * 10000.0) if initial_cash else 0.0
    avg_gross_exposure = float(pd.to_numeric(timeseries.get("gross_exposure"), errors="coerce").fillna(0.0).mean())
    avg_net_exposure = float(pd.to_numeric(timeseries.get("net_exposure"), errors="coerce").fillna(0.0).mean())

    if closed_positions.empty:
        closed_positions_summary = {
            "closed_positions": 0,
            "winning_positions": 0,
            "losing_positions": 0,
            "hit_rate": 0.0,
            "avg_win_pnl": 0.0,
            "avg_loss_pnl": 0.0,
            "avg_win_return": 0.0,
            "avg_loss_return": 0.0,
            "payoff_ratio": 0.0,
            "profit_factor": 0.0,
            "expectancy_pnl": 0.0,
            "expectancy_return": 0.0,
        }
    else:
        realized_pnl = pd.to_numeric(closed_positions["realized_pnl"], errors="coerce").fillna(0.0)
        realized_return = pd.to_numeric(closed_positions["realized_return"], errors="coerce").fillna(0.0)
        winners = closed_positions[realized_pnl > 0.0]
        losers = closed_positions[realized_pnl < 0.0]
        winner_pnl = pd.to_numeric(winners.get("realized_pnl"), errors="coerce").fillna(0.0)
        loser_pnl = pd.to_numeric(losers.get("realized_pnl"), errors="coerce").fillna(0.0)
        winner_returns = pd.to_numeric(winners.get("realized_return"), errors="coerce").fillna(0.0)
        loser_returns = pd.to_numeric(losers.get("realized_return"), errors="coerce").fillna(0.0)
        gross_profit = float(winner_pnl.sum())
        gross_loss = float(abs(loser_pnl.sum()))
        avg_win_pnl = float(winner_pnl.mean()) if not winner_pnl.empty else 0.0
        avg_loss_pnl = float(loser_pnl.mean()) if not loser_pnl.empty else 0.0
        closed_positions_summary = {
            "closed_positions": int(len(closed_positions)),
            "winning_positions": int(len(winners)),
            "losing_positions": int(len(losers)),
            "hit_rate": float(len(winners) / len(closed_positions)) if len(closed_positions) else 0.0,
            "avg_win_pnl": avg_win_pnl,
            "avg_loss_pnl": avg_loss_pnl,
            "avg_win_return": float(winner_returns.mean()) if not winner_returns.empty else 0.0,
            "avg_loss_return": float(loser_returns.mean()) if not loser_returns.empty else 0.0,
            "payoff_ratio": float(abs(avg_win_pnl / avg_loss_pnl)) if avg_loss_pnl < 0 else 0.0,
            "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else 0.0,
            "expectancy_pnl": float(realized_pnl.mean()) if not realized_pnl.empty else 0.0,
            "expectancy_return": float(realized_return.mean()) if not realized_return.empty else 0.0,
        }

    return {
        "run_id": run_id,
        "run_name": run_name,
        "start_date": str(timeseries["date"].iloc[0]),
        "end_date": str(timeseries["date"].iloc[-1]),
        "total_return": float(total_return),
        "annualized_return": float(annualized_return),
        "annualized_volatility": float(annualized_volatility),
        "sharpe_ratio": float(sharpe_ratio),
        "max_drawdown": float(max_drawdown),
        "trades": int(len(trades)),
        "initial_cash": float(initial_cash),
        "final_equity": float(final_equity),
        "gross_total_return": float(gross_total_return),
        "gross_annualized_return": float(gross_annualized_return),
        "total_commission": total_commission,
        "total_slippage_cost": total_slippage_cost,
        "total_transaction_cost": total_transaction_cost,
        "cost_drag_bps": cost_drag_bps,
        "avg_gross_exposure": avg_gross_exposure,
        "avg_net_exposure": avg_net_exposure,
        "sortino_ratio": float(sortino_ratio),
        "calmar_ratio": float(calmar_ratio),
        **closed_positions_summary,
    }


def _compute_rolling_metrics(
    timeseries: pd.DataFrame,
    *,
    periods_per_year: float,
    window_periods: int,
) -> pd.DataFrame:
    if timeseries.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "window_days",
                "window_periods",
                "rolling_return",
                "rolling_volatility",
                "rolling_sharpe",
                "rolling_max_drawdown",
                "turnover_sum",
                "commission_sum",
                "slippage_cost_sum",
                "n_trades_sum",
                "gross_exposure_avg",
                "net_exposure_avg",
            ]
        )
    frame = timeseries.copy()
    returns = pd.to_numeric(frame["period_return"], errors="coerce").fillna(0.0)
    frame["rolling_return"] = (1.0 + returns).rolling(window_periods).apply(lambda values: float(np.prod(values) - 1.0), raw=True)
    frame["rolling_volatility"] = returns.rolling(window_periods).std(ddof=0) * math.sqrt(periods_per_year)
    annualized_rolling_return = (1.0 + frame["rolling_return"]).pow(periods_per_year / max(window_periods, 1)) - 1.0
    safe_volatility = frame["rolling_volatility"].replace(0.0, np.nan)
    frame["rolling_sharpe"] = (annualized_rolling_return / safe_volatility).fillna(0.0)
    frame["rolling_max_drawdown"] = frame["drawdown"].rolling(window_periods).min()
    frame["turnover_sum"] = pd.to_numeric(frame["turnover"], errors="coerce").fillna(0.0).rolling(window_periods).sum()
    frame["commission_sum"] = pd.to_numeric(frame["commission"], errors="coerce").fillna(0.0).rolling(window_periods).sum()
    frame["slippage_cost_sum"] = pd.to_numeric(frame["slippage_cost"], errors="coerce").fillna(0.0).rolling(window_periods).sum()
    frame["n_trades_sum"] = pd.to_numeric(frame["trade_count"], errors="coerce").fillna(0.0).rolling(window_periods).sum()
    frame["gross_exposure_avg"] = pd.to_numeric(frame["gross_exposure"], errors="coerce").fillna(0.0).rolling(window_periods).mean()
    frame["net_exposure_avg"] = pd.to_numeric(frame["net_exposure"], errors="coerce").fillna(0.0).rolling(window_periods).mean()
    frame["window_days"] = _DEFAULT_ROLLING_WINDOW_DAYS
    frame["window_periods"] = window_periods
    return frame[
        [
            "date",
            "window_days",
            "window_periods",
            "rolling_return",
            "rolling_volatility",
            "rolling_sharpe",
            "rolling_max_drawdown",
            "turnover_sum",
            "commission_sum",
            "slippage_cost_sum",
            "n_trades_sum",
            "gross_exposure_avg",
            "net_exposure_avg",
        ]
    ].copy()


def execute_backtest_run(
    dsn: str,
    *,
    run_id: str,
    execution_name: str | None = None,
) -> dict[str, Any]:
    runtime_started_at = monotonic_time.monotonic()
    repo = BacktestRepository(dsn)
    run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found.")
    if run["status"] == "queued":
        repo.start_run(run_id, execution_name=execution_name)
        run = repo.get_run(run_id)
    if not run:
        raise ValueError(f"Run '{run_id}' not found after start.")

    start_ts = _ensure_utc(run["start_ts"])
    end_ts = _ensure_utc(run["end_ts"])
    bar_size = str(run.get("bar_size") or "").strip() or None
    periods_per_year = _periods_per_year_from_bar_size(bar_size)
    rolling_window_periods = _rolling_window_periods(periods_per_year=periods_per_year)
    _log_stage_timing(
        "run_context_ready",
        runtime_started_at,
        run_id=run_id,
        execution_name=execution_name,
        bar_size=bar_size,
        periods_per_year=f"{periods_per_year:.2f}",
        rolling_window_periods=rolling_window_periods,
    )
    definition = resolve_backtest_definition(
        dsn,
        strategy_name=str(run["strategy_name"] or ""),
        strategy_version=run.get("strategy_version"),
        regime_model_name=run.get("regime_model_name"),
        regime_model_version=run.get("regime_model_version"),
    )
    schedule = validate_backtest_submission(
        dsn,
        definition=definition,
        start_ts=start_ts,
        end_ts=end_ts,
        bar_size=bar_size,
    )
    _log_stage_timing(
        "validation_complete",
        runtime_started_at,
        run_id=run_id,
        schedule_bars=len(schedule),
    )

    table_specs = universe_service._load_gold_table_specs(dsn)
    required_columns = _required_columns(definition)
    grouped_schedule: dict[date, list[datetime]] = defaultdict(list)
    for ts in schedule:
        grouped_schedule[ts.date()].append(ts)
    regime_schedule_map = _load_regime_schedule_map(dsn, definition=definition, schedule=schedule)
    _log_stage_timing(
        "schedule_materialized",
        runtime_started_at,
        run_id=run_id,
        sessions=len(grouped_schedule),
        schedule_bars=len(schedule),
    )

    evaluator = ExitRuleEvaluator()
    commission_bps, slippage_bps = _costs_from_raw_config(definition.strategy_config_raw)
    cash = float(definition.strategy_config_raw.get("initialCash") or 100000.0)
    gross_cash = float(cash)
    positions: dict[str, PositionState] = {}
    pending_target_weights: dict[str, float] = {}
    selection_trace_rows: list[dict[str, Any]] = []
    regime_trace_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    closed_position_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []
    previous_equity = cash
    initial_equity = cash
    running_peak = cash
    previous_close_by_symbol: dict[str, float] = {}
    first_signal_computed = False
    heartbeat_state: dict[str, Any] = {
        "interval_seconds": _heartbeat_interval_seconds(),
        "last_heartbeat_at": None,
    }

    _maybe_update_heartbeat(repo, run_id=run_id, state=heartbeat_state, phase="run_initialized", force=True)

    for session_date, session_schedule in grouped_schedule.items():
        session_start, session_end = _session_bounds(session_schedule[0])
        session_started_at = monotonic_time.monotonic()
        intraday_frames = _load_intraday_session_frames(
            dsn,
            table_specs=table_specs,
            required_columns=required_columns,
            session_start=session_start,
            session_end=session_end,
            bar_size=bar_size,
        )
        slow_frames = _load_slow_frames(
            dsn,
            table_specs=table_specs,
            required_columns=required_columns,
            as_of_ts=session_schedule[-1],
            bar_size=bar_size,
        )
        _maybe_update_heartbeat(repo, run_id=run_id, state=heartbeat_state, phase="session_frames_loaded")
        intraday_row_count = sum(len(frame) for frame in intraday_frames.values())
        slow_row_count = sum(len(frame) for frame in slow_frames.values())
        _log_stage_timing(
            "session_frames_loaded",
            session_started_at,
            run_id=run_id,
            session_date=session_date.isoformat(),
            session_bars=len(session_schedule),
            intraday_rows=intraday_row_count,
            slow_rows=slow_row_count,
        )
        for index, current_ts in enumerate(session_schedule):
            snapshot = _snapshot_for_timestamp(current_ts, intraday_frames=intraday_frames, slow_frames=slow_frames)
            snapshot_index = _build_snapshot_symbol_index(snapshot)
            price_bar_cache: dict[str, PriceBar] = {}
            _maybe_update_heartbeat(repo, run_id=run_id, state=heartbeat_state, phase="bar_loop")
            regime_row = regime_schedule_map.get(session_date)
            regime_context = _regime_context_for_session(definition.strategy_config.regimePolicy, regime_row)
            regime_trace_rows.append(
                {
                    "date": current_ts.isoformat(),
                    "session_date": session_date.isoformat(),
                    "model_name": definition.regime_model_name,
                    "model_version": definition.regime_model_version,
                    "as_of_date": (
                        regime_context["as_of_date"].isoformat()
                        if isinstance(regime_context["as_of_date"], date)
                        else regime_context["as_of_date"]
                    ),
                    "effective_from_date": (
                        regime_context["effective_from_date"].isoformat()
                        if isinstance(regime_context["effective_from_date"], date)
                        else regime_context["effective_from_date"]
                    ),
                    "regime_code": regime_context["regime_code"],
                    "regime_status": regime_context["regime_status"],
                    "matched_rule_id": regime_context["matched_rule_id"],
                    "halt_flag": bool(regime_context["halt_flag"]),
                    "halt_reason": regime_context["halt_reason"],
                    "blocked": bool(regime_context["blocked"]),
                    "blocked_reason": regime_context["blocked_reason"],
                    "blocked_action": regime_context["blocked_action"],
                    "exposure_multiplier": float(regime_context["exposure_multiplier"]),
                }
            )
            if not first_signal_computed:
                initial_ranking = _score_snapshot(
                    snapshot,
                    definition=definition,
                    rebalance_ts=current_ts,
                    target_weight_multiplier=float(regime_context["exposure_multiplier"]),
                )
                initial_ranking_records = initial_ranking.to_dict("records")
                selection_trace_rows.extend(initial_ranking_records)
                if regime_context["blocked"] and regime_context["blocked_action"] == "skip_rebalance":
                    pending_target_weights = {}
                else:
                    pending_target_weights = {
                        str(row["symbol"]): float(row["target_weight"])
                        for row in initial_ranking_records
                        if bool(row["selected"])
                    }
                first_signal_computed = True
                continue

            total_commission = 0.0
            total_slippage = 0.0
            trade_count = 0
            market_equity_open = cash

            for symbol, position in list(positions.items()):
                row = _market_row(snapshot_index, symbol)
                if row is None:
                    market_equity_open += position.quantity * previous_close_by_symbol.get(symbol, position.entry_price)
                    continue
                open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(row.get(f"{_PRICE_TABLE}__close")) or position.entry_price
                market_equity_open += position.quantity * open_price

            if not (regime_context["blocked"] and regime_context["blocked_action"] == "skip_rebalance"):
                target_qty_by_symbol: dict[str, float] = {}
                if pending_target_weights:
                    for symbol, target_weight in pending_target_weights.items():
                        row = _market_row(snapshot_index, symbol)
                        if row is None:
                            continue
                        open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(
                            row.get(f"{_PRICE_TABLE}__close")
                        )
                        if open_price is None or open_price <= 0:
                            continue
                        target_qty_by_symbol[symbol] = (market_equity_open * target_weight) / open_price

                all_symbols = sorted(set(positions.keys()) | set(target_qty_by_symbol.keys()))
                for symbol in all_symbols:
                    row = _market_row(snapshot_index, symbol)
                    if row is None:
                        continue
                    open_price = _maybe_float(row.get(f"{_PRICE_TABLE}__open")) or _maybe_float(
                        row.get(f"{_PRICE_TABLE}__close")
                    )
                    if open_price is None or open_price <= 0:
                        continue
                    current_qty = positions[symbol].quantity if symbol in positions else 0.0
                    target_qty = target_qty_by_symbol.get(symbol, 0.0)
                    if regime_context["blocked"] and regime_context["blocked_action"] == "skip_entries":
                        target_qty = min(target_qty, current_qty)
                    delta_qty = target_qty - current_qty
                    if math.isclose(delta_qty, 0.0, abs_tol=1e-9):
                        continue
                    existing_position = positions.get(symbol)
                    position_id = existing_position.position_id if existing_position is not None else _new_position_id()
                    trade_role = _trade_role_for_target(current_quantity=float(current_qty), target_quantity=float(target_qty))
                    cash, commission, slippage = _execute_trade(
                        trades=trade_rows,
                        ts=current_ts,
                        symbol=symbol,
                        quantity_delta=delta_qty,
                        price=open_price,
                        cash=cash,
                        commission_bps=commission_bps,
                        slippage_bps=slippage_bps,
                        position_id=position_id,
                        trade_role=trade_role,
                    )
                    gross_cash -= float(delta_qty * open_price)
                    total_commission += commission
                    total_slippage += slippage
                    trade_count += 1
                    updated_position, closed_position = _apply_trade_to_position(
                        existing_position,
                        symbol=symbol,
                        ts=current_ts,
                        quantity_delta=float(delta_qty),
                        trade_price=float(open_price),
                        commission=float(commission),
                        slippage=float(slippage),
                        position_id=position_id,
                        exit_reason="rebalance_exit" if target_qty <= 1e-9 else None,
                    )
                    if closed_position is not None:
                        closed_position_rows.append(closed_position)
                    if updated_position is None:
                        positions.pop(symbol, None)
                        previous_close_by_symbol.pop(symbol, None)
                    else:
                        positions[symbol] = updated_position

            pending_target_weights = {}

            for symbol, position in list(positions.items()):
                row = _market_row(snapshot_index, symbol)
                if row is None:
                    continue
                bar = price_bar_cache.get(symbol)
                if bar is None:
                    bar = _price_bar(current_ts, row)
                    price_bar_cache[symbol] = bar
                evaluation = evaluator.evaluate_bar(definition.strategy_config, position, bar)
                advanced_position = evaluation.position_state
                positions[symbol] = advanced_position
                if evaluation.decision is None:
                    previous_close_by_symbol[symbol] = bar.close or previous_close_by_symbol.get(symbol, position.entry_price)
                    continue
                cash, commission, slippage = _execute_trade(
                    trades=trade_rows,
                    ts=current_ts,
                    symbol=symbol,
                    quantity_delta=-advanced_position.quantity,
                    price=float(evaluation.decision.exit_price),
                    cash=cash,
                    commission_bps=commission_bps,
                    slippage_bps=slippage_bps,
                    position_id=advanced_position.position_id,
                    trade_role="exit",
                )
                gross_cash += float(advanced_position.quantity * float(evaluation.decision.exit_price))
                total_commission += commission
                total_slippage += slippage
                trade_count += 1
                updated_position, closed_position = _apply_trade_to_position(
                    advanced_position,
                    symbol=symbol,
                    ts=current_ts,
                    quantity_delta=float(-advanced_position.quantity),
                    trade_price=float(evaluation.decision.exit_price),
                    commission=float(commission),
                    slippage=float(slippage),
                    position_id=advanced_position.position_id,
                    exit_reason=evaluation.decision.exit_reason,
                    exit_rule_id=evaluation.decision.rule_id,
                )
                if closed_position is not None:
                    closed_position_rows.append(closed_position)
                if updated_position is not None:
                    positions[symbol] = updated_position
                    continue
                positions.pop(symbol, None)
                previous_close_by_symbol.pop(symbol, None)

            close_equity = cash
            gross_close_equity = gross_cash
            gross_exposure = 0.0
            net_market_value = 0.0
            for symbol, position in positions.items():
                row = _market_row(snapshot_index, symbol)
                close_price = None
                if row is not None:
                    close_price = _maybe_float(row.get(f"{_PRICE_TABLE}__close")) or _maybe_float(row.get(f"{_PRICE_TABLE}__open"))
                if close_price is None:
                    close_price = previous_close_by_symbol.get(symbol, position.entry_price)
                previous_close_by_symbol[symbol] = float(close_price)
                position_value = float(position.quantity * close_price)
                close_equity += position_value
                gross_close_equity += position_value
                gross_exposure += abs(position_value)
                net_market_value += position_value

            period_return = (close_equity / previous_equity - 1.0) if previous_equity else 0.0
            running_peak = max(running_peak, close_equity)
            drawdown = (close_equity / running_peak - 1.0) if running_peak else 0.0
            timeseries_rows.append(
                {
                    "date": current_ts.isoformat(),
                    "portfolio_value": float(close_equity),
                    "drawdown": float(drawdown),
                    "period_return": float(period_return),
                    "daily_return": float(period_return),
                    "cumulative_return": float(close_equity / initial_equity - 1.0) if initial_equity else 0.0,
                    "gross_portfolio_value": float(gross_close_equity),
                    "cash": float(cash),
                    "gross_exposure": float(gross_exposure / close_equity) if close_equity else 0.0,
                    "net_exposure": float(net_market_value / close_equity) if close_equity else 0.0,
                    "turnover": float(
                        sum(abs(trade["notional"]) for trade in trade_rows[-trade_count:]) / previous_equity
                    ) if previous_equity and trade_count else 0.0,
                    "commission": float(total_commission),
                    "slippage_cost": float(total_slippage),
                    "trade_count": int(trade_count),
                }
            )
            previous_equity = close_equity

            if index < len(session_schedule) - 1:
                ranking = _score_snapshot(
                    snapshot,
                    definition=definition,
                    rebalance_ts=current_ts,
                    target_weight_multiplier=float(regime_context["exposure_multiplier"]),
                )
                ranking_records = ranking.to_dict("records")
                selection_trace_rows.extend(ranking_records)
                if regime_context["blocked"] and regime_context["blocked_action"] == "skip_rebalance":
                    pending_target_weights = {}
                else:
                    pending_target_weights = {
                        str(row["symbol"]): float(row["target_weight"])
                        for row in ranking_records
                        if bool(row["selected"])
                    }

    timeseries = pd.DataFrame(timeseries_rows)
    trades = pd.DataFrame(trade_rows)
    closed_positions = pd.DataFrame(closed_position_rows)
    rolling_metrics = _compute_rolling_metrics(
        timeseries,
        periods_per_year=periods_per_year,
        window_periods=rolling_window_periods,
    )
    summary = _compute_summary(
        timeseries,
        trades,
        closed_positions,
        run_id=run_id,
        run_name=run.get("run_name"),
        periods_per_year=periods_per_year,
        initial_cash_override=initial_equity,
    )

    _maybe_update_heartbeat(repo, run_id=run_id, state=heartbeat_state, phase="postgres_publish_start")
    _log_stage_timing(
        "publish_start",
        runtime_started_at,
        run_id=run_id,
        timeseries_rows=len(timeseries_rows),
        rolling_rows=len(rolling_metrics),
        trade_rows=len(trade_rows),
        closed_position_rows=len(closed_position_rows),
        selection_rows=len(selection_trace_rows),
        regime_rows=len(regime_trace_rows),
    )
    persist_backtest_results(
        dsn,
        run_id=run_id,
        summary=summary,
        timeseries_rows=timeseries_rows,
        rolling_metric_rows=rolling_metrics.to_dict("records"),
        trade_rows=trade_rows,
        closed_position_rows=closed_position_rows,
        selection_trace_rows=selection_trace_rows,
        regime_trace_rows=regime_trace_rows,
        results_schema_version=BACKTEST_RESULTS_SCHEMA_VERSION,
    )
    _maybe_update_heartbeat(repo, run_id=run_id, state=heartbeat_state, phase="postgres_publish_complete", force=True)
    _log_stage_timing(
        "publish_complete",
        runtime_started_at,
        run_id=run_id,
        timeseries_rows=len(timeseries_rows),
        rolling_rows=len(rolling_metrics),
        trade_rows=len(trade_rows),
        closed_position_rows=len(closed_position_rows),
        selection_rows=len(selection_trace_rows),
        regime_rows=len(regime_trace_rows),
    )
    repo.complete_run(run_id, summary=summary)
    return {"summary": summary}
