from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

from asset_allocation_runtime_common.strategy_engine.position_state import PositionState

from core.backtest_runtime import _apply_trade_to_position
from core.portfolio_contracts import (
    FreshnessStatus,
    PortfolioAlert,
    PortfolioHistoryPoint,
    PortfolioPosition,
    PortfolioPositionContributor,
    PortfolioSnapshot,
    StrategySliceAttribution,
)
from core.portfolio_repository import PortfolioMaterializationBundle
from core.postgres import connect, copy_rows

_WRITE_SCOPE_COLUMNS = ("account_id",)
_CASH_ONLY_EVENT_TYPES = {
    "opening_balance",
    "deposit",
    "withdrawal",
    "fee",
    "dividend",
    "correction",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _quote(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _parse_json(value: object, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return default


def _json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _json_hash(value: object) -> str:
    return hashlib.md5(_json_dumps(value).encode("utf-8")).hexdigest()


def _normalize_as_of(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _normalize_text(value)
    return date.fromisoformat(text[:10]) if text else None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _synthetic_symbol(strategy_name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(strategy_name or "").upper()).strip("_")
    return (cleaned or "SLEEVE")[:32]


def _stable_alert_id(account_id: str, code: str, as_of: date | None) -> str:
    payload = f"{account_id}:{code}:{as_of.isoformat() if isinstance(as_of, date) else '-'}"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _cursor_rowcount(cur: Any) -> int:
    try:
        value = int(getattr(cur, "rowcount", 0) or 0)
    except Exception:
        value = 0
    return max(value, 0)


def _coerce_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, (dict, list, tuple)):
        return _json_dumps(value)
    return value


def _key_match(left_alias: str, right_alias: str, columns: Iterable[str]) -> str:
    return " AND ".join(
        f"{left_alias}.{_quote(column)} = {right_alias}.{_quote(column)}"
        for column in columns
    )


def _changed_match(left_alias: str, right_alias: str, columns: Iterable[str]) -> str:
    comparisons = [
        f"{left_alias}.{_quote(column)} IS DISTINCT FROM {right_alias}.{_quote(column)}"
        for column in columns
    ]
    return " OR ".join(comparisons) if comparisons else "FALSE"


@dataclass(frozen=True)
class ApplyConfig:
    table: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]


_SNAPSHOT_COLUMNS = (
    "account_id",
    "account_name",
    "as_of_date",
    "nav",
    "cash",
    "gross_exposure",
    "net_exposure",
    "since_inception_pnl",
    "since_inception_return",
    "current_drawdown",
    "max_drawdown",
    "open_alert_count",
    "active_assignment_json",
    "freshness_json",
    "dependency_fingerprint",
    "dependency_state_json",
    "materialized_at",
)

_HISTORY_COLUMNS = (
    "account_id",
    "as_of_date",
    "nav",
    "cash",
    "gross_exposure",
    "net_exposure",
    "period_pnl",
    "period_return",
    "cumulative_pnl",
    "cumulative_return",
    "drawdown",
    "turnover",
    "cost_drag_bps",
    "materialized_at",
)

_POSITION_COLUMNS = (
    "account_id",
    "as_of_date",
    "symbol",
    "quantity",
    "market_value",
    "weight",
    "average_cost",
    "last_price",
    "unrealized_pnl",
    "realized_pnl",
    "contributors_json",
    "materialized_at",
)

_ATTRIBUTION_COLUMNS = (
    "account_id",
    "as_of_date",
    "sleeve_id",
    "strategy_name",
    "strategy_version",
    "target_weight",
    "actual_weight",
    "market_value",
    "gross_exposure",
    "net_exposure",
    "pnl_contribution",
    "return_contribution",
    "drawdown_contribution",
    "turnover",
    "since_inception_return",
    "materialized_at",
)

_ALERT_COLUMNS = (
    "account_id",
    "alert_id",
    "severity",
    "status",
    "code",
    "title",
    "description",
    "detected_at",
    "acknowledged_at",
    "acknowledged_by",
    "resolved_at",
    "as_of_date",
    "materialized_at",
)

_SERVING_TABLE_CONFIGS = (
    ApplyConfig("core.portfolio_history", _HISTORY_COLUMNS, ("account_id", "as_of_date")),
    ApplyConfig("core.portfolio_positions", _POSITION_COLUMNS, ("account_id", "as_of_date", "symbol")),
    ApplyConfig("core.portfolio_attribution", _ATTRIBUTION_COLUMNS, ("account_id", "as_of_date", "sleeve_id")),
    ApplyConfig("core.portfolio_alerts", _ALERT_COLUMNS, ("account_id", "alert_id")),
)


class PortfolioMaterializationError(RuntimeError):
    pass


class PortfolioMaterializationStaleDependencyError(PortfolioMaterializationError):
    pass


@dataclass(frozen=True)
class PortfolioStrategyHistorySample:
    as_of: date
    portfolio_value: float
    cash: float
    gross_exposure: float
    net_exposure: float
    period_return: float | None
    cumulative_return: float | None
    drawdown: float | None
    turnover: float | None
    commission: float | None
    slippage_cost: float | None


@dataclass(frozen=True)
class PortfolioStrategyPositionSample:
    symbol: str
    quantity: float
    average_cost: float
    last_price: float
    market_value: float
    realized_pnl: float


@dataclass(frozen=True)
class PortfolioStrategyDependency:
    sleeve_id: str
    strategy_name: str
    strategy_version: int
    target_weight: float
    run_id: str
    canonical_target_id: str | None
    canonical_fingerprint: str | None
    completed_at: datetime | None
    latest_as_of: date
    initial_portfolio_value: float
    latest_portfolio_value: float
    latest_cash: float
    latest_gross_exposure: float
    latest_net_exposure: float
    latest_drawdown: float | None
    latest_period_return: float | None
    latest_cumulative_return: float | None
    latest_turnover: float | None
    history: tuple[PortfolioStrategyHistorySample, ...]
    positions: tuple[PortfolioStrategyPositionSample, ...]


@dataclass(frozen=True)
class PortfolioMaterializedSurfaces:
    snapshot: PortfolioSnapshot
    history: tuple[PortfolioHistoryPoint, ...]
    positions: tuple[PortfolioPosition, ...]
    attribution: tuple[StrategySliceAttribution, ...]
    alerts: tuple[PortfolioAlert, ...]


@dataclass(frozen=True)
class PortfolioMaterializationResult:
    snapshot: PortfolioSnapshot
    history: tuple[PortfolioHistoryPoint, ...]
    positions: tuple[PortfolioPosition, ...]
    attribution: tuple[StrategySliceAttribution, ...]
    alerts: tuple[PortfolioAlert, ...]
    dependency_fingerprint: str | None
    dependency_state: dict[str, Any]


def _allocation_mode(value: object) -> str:
    return _normalize_text(getattr(value, "allocationMode", "percent") or "percent") or "percent"


def _enabled_allocations(portfolio_revision: Any) -> tuple[Any, ...]:
    return tuple(
        allocation
        for allocation in getattr(portfolio_revision, "allocations", ())
        if getattr(allocation, "enabled", True)
    )


def _sleeve_label(allocation: Any) -> str:
    return _normalize_text(getattr(allocation, "sleeveId", "")) or "-"


def _portfolio_allocatable_capital(portfolio_revision: Any) -> float:
    value = getattr(portfolio_revision, "allocatableCapital", None)
    if value is None:
        raise PortfolioMaterializationError(
            "Notional portfolio materialization requires allocatableCapital on the portfolio revision."
        )
    capital = float(value)
    if capital <= 0:
        raise PortfolioMaterializationError(
            "Notional portfolio materialization requires allocatableCapital greater than zero."
        )
    return capital


def _percent_target_weight(allocation: Any) -> float:
    value = getattr(allocation, "targetWeight", None)
    if value is None:
        raise PortfolioMaterializationError(
            f"Percent allocation '{_sleeve_label(allocation)}' requires targetWeight."
        )
    return float(value)


def _notional_target(allocation: Any) -> float:
    value = getattr(allocation, "targetNotionalBaseCcy", None)
    if value is None:
        raise PortfolioMaterializationError(
            f"Notional allocation '{_sleeve_label(allocation)}' requires targetNotionalBaseCcy."
        )
    target = float(value)
    if target <= 0:
        raise PortfolioMaterializationError(
            f"Notional allocation '{_sleeve_label(allocation)}' requires targetNotionalBaseCcy greater than zero."
        )
    return target


def _allocation_target_weight_for_dependency(*, allocation: Any, portfolio_revision: Any) -> float:
    revision_mode = _allocation_mode(portfolio_revision)
    allocation_mode = _allocation_mode(allocation)
    if allocation_mode != revision_mode:
        raise PortfolioMaterializationError(
            f"Allocation '{_sleeve_label(allocation)}' mode {allocation_mode!r} does not match "
            f"portfolio revision mode {revision_mode!r}."
        )
    if revision_mode == "percent":
        return _percent_target_weight(allocation)
    if revision_mode == "notional_base_ccy":
        return _notional_target(allocation) / _portfolio_allocatable_capital(portfolio_revision)
    raise PortfolioMaterializationError(f"Unsupported portfolio allocationMode {revision_mode!r}.")


def _target_weights_by_sleeve(*, portfolio_revision: Any, seed_nav: float) -> dict[str, float]:
    revision_mode = _allocation_mode(portfolio_revision)
    weights: dict[str, float] = {}
    if revision_mode == "percent":
        for allocation in _enabled_allocations(portfolio_revision):
            weights[allocation.sleeveId] = _percent_target_weight(allocation)
        return weights

    if revision_mode != "notional_base_ccy":
        raise PortfolioMaterializationError(f"Unsupported portfolio allocationMode {revision_mode!r}.")

    if seed_nav <= 0:
        raise PortfolioMaterializationError(
            "Notional portfolio materialization requires positive seed capital."
        )

    allocated = 0.0
    for allocation in _enabled_allocations(portfolio_revision):
        target_notional = _notional_target(allocation)
        allocated += target_notional
        weights[allocation.sleeveId] = target_notional / seed_nav

    if allocated - seed_nav > 0.01:
        raise PortfolioMaterializationError(
            f"Notional portfolio allocations total {allocated:.2f}, exceeding seed capital {seed_nav:.2f}."
        )
    return weights


def _residual_cash(*, seed_nav: float, target_weights: Mapping[str, float]) -> float:
    allocated = sum(seed_nav * float(weight or 0.0) for weight in target_weights.values())
    if allocated - seed_nav > 0.01:
        raise PortfolioMaterializationError(
            f"Portfolio allocations require {allocated:.2f}, exceeding seed capital {seed_nav:.2f}."
        )
    return max(seed_nav - allocated, 0.0)


def _ensure_connection_is_writable(cur: Any) -> None:
    cur.execute("SHOW transaction_read_only")
    row = cur.fetchone()
    if row and str(row[0]).strip().lower() == "on":
        raise RuntimeError("Postgres target is transaction_read_only=on")
    cur.execute("SELECT pg_is_in_recovery()")
    recovery_row = cur.fetchone()
    if recovery_row and bool(recovery_row[0]):
        raise RuntimeError("Postgres target is in recovery mode")


def _stage_name(table: str) -> str:
    return f"portfolio_stage_{str(table or '').split('.')[-1]}"


def _create_stage(cur: Any, *, config: ApplyConfig) -> str:
    name = _stage_name(config.table)
    quoted_keys = ", ".join(_quote(column) for column in config.key_columns)
    cur.execute(f"CREATE TEMP TABLE {name} (LIKE {config.table} INCLUDING DEFAULTS) ON COMMIT DROP")
    cur.execute(f"CREATE UNIQUE INDEX {name}_key_idx ON {name} ({quoted_keys})")
    return f"pg_temp.{name}"


def _frame_rows(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> list[tuple[Any, ...]]:
    return [
        tuple(_coerce_cell(row.get(column)) for column in columns)
        for row in rows
    ]


def _delete_missing(
    cur: Any,
    *,
    config: ApplyConfig,
    stage_table: str,
    scope_values: Mapping[str, Any],
    has_rows: bool,
) -> int:
    scope_sql = " AND ".join(f"target.{_quote(column)} = %s" for column in _WRITE_SCOPE_COLUMNS)
    scope_params = tuple(scope_values.get(column) for column in _WRITE_SCOPE_COLUMNS)
    if not has_rows:
        cur.execute(f"DELETE FROM {config.table} AS target WHERE {scope_sql}", scope_params)
        return _cursor_rowcount(cur)
    match_sql = _key_match("stage", "target", config.key_columns)
    cur.execute(
        f"""
        DELETE FROM {config.table} AS target
        WHERE {scope_sql}
          AND NOT EXISTS (
              SELECT 1
              FROM {stage_table} AS stage
              WHERE {match_sql}
          )
        """,
        scope_params,
    )
    return _cursor_rowcount(cur)


def _upsert_changed(cur: Any, *, config: ApplyConfig, stage_table: str) -> int:
    non_key_columns = tuple(column for column in config.columns if column not in config.key_columns)
    quoted_columns = ", ".join(_quote(column) for column in config.columns)
    conflict_sql = ", ".join(_quote(column) for column in config.key_columns)
    assignment_sql = ", ".join(
        f"{_quote(column)} = EXCLUDED.{_quote(column)}"
        for column in non_key_columns
    )
    change_sql = _changed_match("target", "EXCLUDED", non_key_columns)
    cur.execute(
        f"""
        INSERT INTO {config.table} AS target ({quoted_columns})
        SELECT {quoted_columns}
        FROM {stage_table}
        ON CONFLICT ({conflict_sql}) DO UPDATE
        SET {assignment_sql}
        WHERE {change_sql}
        """
    )
    return _cursor_rowcount(cur)


def _apply_serving_table(
    cur: Any,
    *,
    config: ApplyConfig,
    rows: Sequence[Mapping[str, Any]],
    scope_values: Mapping[str, Any],
) -> None:
    stage_table = _create_stage(cur, config=config)
    staged_rows = _frame_rows(rows, config.columns)
    if staged_rows:
        copy_rows(cur, table=stage_table, columns=config.columns, rows=staged_rows)
    _delete_missing(
        cur,
        config=config,
        stage_table=stage_table,
        scope_values=scope_values,
        has_rows=bool(staged_rows),
    )
    if staged_rows:
        _upsert_changed(cur, config=config, stage_table=stage_table)


def _upsert_latest_snapshot(
    cur: Any,
    *,
    snapshot_row: Mapping[str, Any],
) -> None:
    quoted_columns = ", ".join(_quote(column) for column in _SNAPSHOT_COLUMNS)
    assignments = ", ".join(
        f"{_quote(column)} = EXCLUDED.{_quote(column)}"
        for column in _SNAPSHOT_COLUMNS
        if column != "account_id"
    )
    values = tuple(_coerce_cell(snapshot_row.get(column)) for column in _SNAPSHOT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_SNAPSHOT_COLUMNS))
    cur.execute(
        f"""
        INSERT INTO core.portfolio_latest_snapshot ({quoted_columns})
        VALUES ({placeholders})
        ON CONFLICT (account_id) DO UPDATE
        SET {assignments}
        """,
        values,
    )


def _upsert_materialization_state(
    cur: Any,
    *,
    account_id: str,
    dependency_fingerprint: str | None,
    dependency_state: Mapping[str, Any],
    as_of: date | None,
    materialized_at: datetime,
) -> None:
    cur.execute(
        """
        INSERT INTO core.portfolio_materialization_state (
            account_id,
            dependency_fingerprint,
            dependency_state_json,
            last_as_of_date,
            last_materialized_at
        )
        VALUES (%s, %s, %s::jsonb, %s, %s)
        ON CONFLICT (account_id) DO UPDATE
        SET dependency_fingerprint = EXCLUDED.dependency_fingerprint,
            dependency_state_json = EXCLUDED.dependency_state_json,
            last_as_of_date = EXCLUDED.last_as_of_date,
            last_materialized_at = EXCLUDED.last_materialized_at
        """,
        (
            account_id,
            dependency_fingerprint,
            _json_dumps(dict(dependency_state)),
            as_of,
            materialized_at,
        ),
    )


def _load_run_history(cur: Any, *, run_id: str) -> tuple[PortfolioStrategyHistorySample, ...]:
    cur.execute(
        """
        SELECT
            as_of_date,
            portfolio_value,
            cash,
            gross_exposure,
            net_exposure,
            period_return,
            cumulative_return,
            drawdown,
            turnover,
            commission,
            slippage_cost
        FROM (
            SELECT DISTINCT ON (((bar_ts AT TIME ZONE 'UTC')::date))
                (bar_ts AT TIME ZONE 'UTC')::date AS as_of_date,
                portfolio_value,
                COALESCE(cash, 0) AS cash,
                COALESCE(gross_exposure, 0) AS gross_exposure,
                COALESCE(net_exposure, 0) AS net_exposure,
                period_return,
                cumulative_return,
                drawdown,
                turnover,
                commission,
                slippage_cost
            FROM core.backtest_timeseries
            WHERE run_id = %s
            ORDER BY ((bar_ts AT TIME ZONE 'UTC')::date), bar_ts DESC
        ) AS samples
        ORDER BY as_of_date ASC
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    return tuple(
        PortfolioStrategyHistorySample(
            as_of=row[0],
            portfolio_value=float(row[1] or 0.0),
            cash=float(row[2] or 0.0),
            gross_exposure=float(row[3] or 0.0),
            net_exposure=float(row[4] or 0.0),
            period_return=(float(row[5]) if row[5] is not None else None),
            cumulative_return=(float(row[6]) if row[6] is not None else None),
            drawdown=(float(row[7]) if row[7] is not None else None),
            turnover=(float(row[8]) if row[8] is not None else None),
            commission=(float(row[9]) if row[9] is not None else None),
            slippage_cost=(float(row[10]) if row[10] is not None else None),
        )
        for row in rows
        if row and row[0] is not None
    )


def _load_symbol_last_prices(cur: Any, *, symbols: Sequence[str], as_of: date) -> dict[str, float]:
    if not symbols:
        return {}
    cur.execute(
        """
        SELECT DISTINCT ON (symbol)
            symbol,
            close
        FROM gold.market_data
        WHERE symbol = ANY(%s)
          AND date <= %s
        ORDER BY symbol, date DESC
        """,
        (list(symbols), as_of),
    )
    rows = cur.fetchall()
    prices: dict[str, float] = {}
    for row in rows:
        symbol = _normalize_text(row[0]).upper()
        if symbol and row[1] is not None:
            prices[symbol] = float(row[1])
    return prices


def _load_run_positions(cur: Any, *, run_id: str, as_of: date) -> tuple[PortfolioStrategyPositionSample, ...]:
    cur.execute(
        """
        SELECT
            execution_ts,
            symbol,
            quantity,
            price,
            commission,
            slippage_cost,
            position_id
        FROM core.backtest_trades
        WHERE run_id = %s
        ORDER BY execution_ts ASC, trade_seq ASC
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    positions: dict[str, PositionState] = {}
    for row in rows:
        execution_ts = _ensure_utc(row[0])
        symbol = _normalize_text(row[1]).upper()
        quantity_delta = float(row[2] or 0.0)
        if not symbol or math.isclose(quantity_delta, 0.0, abs_tol=1e-12):
            continue
        updated, _closed = _apply_trade_to_position(
            positions.get(symbol),
            symbol=symbol,
            ts=execution_ts,
            quantity_delta=quantity_delta,
            trade_price=float(row[3] or 0.0),
            commission=float(row[4] or 0.0),
            slippage=float(row[5] or 0.0),
            position_id=_normalize_text(row[6]) or None,
        )
        if updated is None or updated.quantity <= 1e-9:
            positions.pop(symbol, None)
            continue
        positions[symbol] = updated
    if not positions:
        return ()
    last_prices = _load_symbol_last_prices(cur, symbols=sorted(positions.keys()), as_of=as_of)
    samples: list[PortfolioStrategyPositionSample] = []
    for symbol, state in positions.items():
        last_price = float(last_prices.get(symbol) or state.average_cost or state.entry_price or 0.0)
        quantity = float(state.quantity or 0.0)
        market_value = quantity * last_price
        samples.append(
            PortfolioStrategyPositionSample(
                symbol=symbol,
                quantity=quantity,
                average_cost=float(state.average_cost or state.entry_price or 0.0),
                last_price=last_price,
                market_value=market_value,
                realized_pnl=float(state.realized_pnl_accrued or 0.0),
            )
        )
    samples.sort(key=lambda item: item.market_value, reverse=True)
    return tuple(samples)


def _load_latest_strategy_dependency(
    cur: Any,
    *,
    sleeve_id: str,
    strategy_name: str,
    strategy_version: int,
    target_weight: float,
    required_as_of: date | None,
) -> PortfolioStrategyDependency:
    cur.execute(
        """
        SELECT
            r.run_id,
            r.canonical_target_id,
            r.canonical_fingerprint,
            COALESCE(r.completed_at, r.results_ready_at, r.submitted_at) AS completed_at
        FROM core.runs AS r
        JOIN core.backtest_run_summary AS summary
          ON summary.run_id = r.run_id
        WHERE r.status = 'completed'
          AND r.strategy_name = %s
          AND COALESCE(r.strategy_version, %s) = %s
        ORDER BY
            CASE WHEN r.canonical_target_id IS NULL THEN 1 ELSE 0 END,
            COALESCE(r.completed_at, r.results_ready_at, r.submitted_at) DESC,
            r.run_id DESC
        LIMIT 1
        """,
        (strategy_name, strategy_version, strategy_version),
    )
    row = cur.fetchone()
    if not row:
        raise PortfolioMaterializationStaleDependencyError(
            f"Missing completed backtest dependency for {strategy_name}@v{strategy_version}."
        )
    run_id = _normalize_text(row[0])
    history = _load_run_history(cur, run_id=run_id)
    if not history:
        raise PortfolioMaterializationStaleDependencyError(
            f"Backtest dependency {run_id} has no timeseries rows for {strategy_name}@v{strategy_version}."
        )
    latest_sample = history[-1]
    if required_as_of is not None and latest_sample.as_of < required_as_of:
        raise PortfolioMaterializationStaleDependencyError(
            f"Dependency {strategy_name}@v{strategy_version} is stale at {latest_sample.as_of.isoformat()} "
            f"(required {required_as_of.isoformat()})."
        )
    positions = _load_run_positions(cur, run_id=run_id, as_of=latest_sample.as_of)
    return PortfolioStrategyDependency(
        sleeve_id=sleeve_id,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        target_weight=float(target_weight),
        run_id=run_id,
        canonical_target_id=_normalize_text(row[1]) or None,
        canonical_fingerprint=_normalize_text(row[2]) or None,
        completed_at=(_ensure_utc(row[3]) if isinstance(row[3], datetime) else None),
        latest_as_of=latest_sample.as_of,
        initial_portfolio_value=float(history[0].portfolio_value or 0.0),
        latest_portfolio_value=float(latest_sample.portfolio_value or 0.0),
        latest_cash=float(latest_sample.cash or 0.0),
        latest_gross_exposure=float(latest_sample.gross_exposure or 0.0),
        latest_net_exposure=float(latest_sample.net_exposure or 0.0),
        latest_drawdown=latest_sample.drawdown,
        latest_period_return=latest_sample.period_return,
        latest_cumulative_return=latest_sample.cumulative_return,
        latest_turnover=latest_sample.turnover,
        history=history,
        positions=positions,
    )


def _build_dependency_state(
    *,
    dependencies: Sequence[PortfolioStrategyDependency],
    as_of: date | None,
) -> dict[str, Any]:
    return {
        "expectedAsOf": as_of.isoformat() if isinstance(as_of, date) else None,
        "sleeveRuns": [
            {
                "sleeveId": dependency.sleeve_id,
                "strategyName": dependency.strategy_name,
                "strategyVersion": dependency.strategy_version,
                "runId": dependency.run_id,
                "canonicalTargetId": dependency.canonical_target_id,
                "canonicalFingerprint": dependency.canonical_fingerprint,
                "completedAt": (
                    dependency.completed_at.isoformat() if isinstance(dependency.completed_at, datetime) else None
                ),
                "asOf": dependency.latest_as_of.isoformat(),
            }
            for dependency in dependencies
        ],
    }


def _validate_expected_dependency_state(
    *,
    expected_state: Mapping[str, Any],
    current_state: Mapping[str, Any],
) -> None:
    expected_runs_raw = expected_state.get("sleeveRuns")
    if not isinstance(expected_runs_raw, list):
        return
    current_runs = {
        (_normalize_text(item.get("sleeveId")), _normalize_text(item.get("strategyName")), int(item.get("strategyVersion") or 0)): item
        for item in current_state.get("sleeveRuns", [])
        if isinstance(item, dict)
    }
    for expected in expected_runs_raw:
        if not isinstance(expected, dict):
            continue
        key = (
            _normalize_text(expected.get("sleeveId")),
            _normalize_text(expected.get("strategyName")),
            int(expected.get("strategyVersion") or 0),
        )
        current = current_runs.get(key)
        if current is None:
            raise PortfolioMaterializationStaleDependencyError(
                f"Dependency state drifted for sleeve '{key[0] or '-'}'."
            )
        expected_run_id = _normalize_text(expected.get("runId"))
        if expected_run_id and expected_run_id != _normalize_text(current.get("runId")):
            raise PortfolioMaterializationStaleDependencyError(
                f"Dependency run changed for {key[1]}@v{key[2]}: "
                f"expected {expected_run_id}, found {_normalize_text(current.get('runId'))}."
            )


def _resolve_strategy_dependencies(
    dsn: str,
    *,
    bundle: PortfolioMaterializationBundle,
    heartbeat: Callable[[], None] | None = None,
) -> tuple[tuple[PortfolioStrategyDependency, ...], str | None, dict[str, Any]]:
    portfolio_revision = bundle.portfolio_revision
    if portfolio_revision is None:
        state = dict(bundle.dependency_state)
        return (), bundle.dependency_fingerprint, state
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            dependencies = []
            for allocation in portfolio_revision.allocations:
                if not getattr(allocation, "enabled", True):
                    continue
                dependencies.append(
                    _load_latest_strategy_dependency(
                        cur,
                        sleeve_id=allocation.sleeveId,
                        strategy_name=allocation.strategy.strategyName,
                        strategy_version=int(allocation.strategy.strategyVersion),
                        target_weight=_allocation_target_weight_for_dependency(
                            allocation=allocation,
                            portfolio_revision=portfolio_revision,
                        ),
                        required_as_of=bundle.as_of,
                    )
                )
                if heartbeat is not None:
                    heartbeat()
    dependency_state = _build_dependency_state(dependencies=dependencies, as_of=bundle.as_of)
    dependency_fingerprint = _json_hash(dependency_state) if dependencies else bundle.dependency_fingerprint
    expected_fingerprint = _normalize_text(bundle.dependency_fingerprint)
    if expected_fingerprint and dependency_fingerprint and expected_fingerprint != dependency_fingerprint:
        raise PortfolioMaterializationStaleDependencyError(
            f"Dependency fingerprint drifted: expected {expected_fingerprint}, found {dependency_fingerprint}."
        )
    _validate_expected_dependency_state(expected_state=bundle.dependency_state, current_state=dependency_state)
    return tuple(dependencies), dependency_fingerprint, dependency_state


def _seed_capital(
    *,
    bundle: PortfolioMaterializationBundle,
    dependencies: Sequence[PortfolioStrategyDependency],
) -> float:
    net_cash = sum(
        float(event.cashAmount or 0.0)
        for event in bundle.ledger_events
        if _normalize_text(event.eventType) in _CASH_ONLY_EVENT_TYPES
    )
    if not math.isclose(net_cash, 0.0, abs_tol=1e-9):
        return float(net_cash)
    if bundle.portfolio_revision is None or not dependencies:
        return max(float(net_cash), 0.0)
    if _allocation_mode(bundle.portfolio_revision) == "notional_base_ccy":
        return _portfolio_allocatable_capital(bundle.portfolio_revision)
    dependency_by_sleeve = {dependency.sleeve_id: dependency for dependency in dependencies}
    weighted_seed = 0.0
    for allocation in bundle.portfolio_revision.allocations:
        dependency = dependency_by_sleeve.get(allocation.sleeveId)
        if dependency is None:
            continue
        weighted_seed += _percent_target_weight(allocation) * max(dependency.initial_portfolio_value, 0.0)
    return float(weighted_seed)


def _default_freshness(
    *,
    dependencies: Sequence[PortfolioStrategyDependency],
) -> list[FreshnessStatus]:
    as_of = max((dependency.completed_at for dependency in dependencies if dependency.completed_at is not None), default=None)
    return [
        FreshnessStatus(
            domain=domain,
            state="fresh",
            asOf=as_of,
            checkedAt=_utc_now(),
        )
        for domain in ("valuation", "positions", "risk", "attribution", "ledger", "alerts")
    ]


def _history_fallback(
    *,
    bundle: PortfolioMaterializationBundle,
    seed_nav: float,
) -> tuple[PortfolioHistoryPoint, ...]:
    as_of = bundle.as_of or bundle.account.inceptionDate
    return (
        PortfolioHistoryPoint(
            asOf=as_of,
            nav=float(seed_nav),
            cash=float(seed_nav),
            grossExposure=0.0,
            netExposure=0.0,
            periodPnl=0.0,
            periodReturn=0.0,
            cumulativePnl=0.0,
            cumulativeReturn=0.0,
            drawdown=0.0,
            turnover=None,
            costDragBps=None,
        ),
    )


def _sample_for_date(
    history: Sequence[PortfolioStrategyHistorySample],
    current_date: date,
) -> PortfolioStrategyHistorySample | None:
    latest: PortfolioStrategyHistorySample | None = None
    for sample in history:
        if sample.as_of > current_date:
            break
        latest = sample
    return latest


def _compute_history(
    *,
    bundle: PortfolioMaterializationBundle,
    dependencies: Sequence[PortfolioStrategyDependency],
    scale_by_sleeve: Mapping[str, float],
    seed_nav: float,
    residual_cash: float = 0.0,
) -> tuple[PortfolioHistoryPoint, ...]:
    if not dependencies:
        return _history_fallback(bundle=bundle, seed_nav=seed_nav)
    all_dates = sorted({sample.as_of for dependency in dependencies for sample in dependency.history})
    if bundle.as_of is not None and bundle.as_of not in all_dates:
        all_dates.append(bundle.as_of)
        all_dates.sort()
    if not all_dates:
        return _history_fallback(bundle=bundle, seed_nav=seed_nav)
    points: list[PortfolioHistoryPoint] = []
    running_peak = 0.0
    previous_nav: float | None = None
    for current_date in all_dates:
        total_nav = 0.0
        total_cash = 0.0
        weighted_gross = 0.0
        weighted_net = 0.0
        weighted_turnover = 0.0
        weighted_cost_drag = 0.0
        cost_drag_weight = 0.0
        for dependency in dependencies:
            sample = _sample_for_date(dependency.history, current_date)
            if sample is None:
                continue
            scale = float(scale_by_sleeve.get(dependency.sleeve_id) or 0.0)
            sleeve_nav = sample.portfolio_value * scale
            sleeve_cash = sample.cash * scale
            total_nav += sleeve_nav
            total_cash += sleeve_cash
            weighted_gross += sample.gross_exposure * sleeve_nav
            weighted_net += sample.net_exposure * sleeve_nav
            if sample.turnover is not None:
                weighted_turnover += sample.turnover * sleeve_nav
            transaction_cost = 0.0
            if sample.commission is not None:
                transaction_cost += float(sample.commission)
            if sample.slippage_cost is not None:
                transaction_cost += float(sample.slippage_cost)
            if sleeve_nav > 0 and transaction_cost > 0:
                weighted_cost_drag += (transaction_cost / sleeve_nav) * 10000.0 * sleeve_nav
                cost_drag_weight += sleeve_nav
        if residual_cash > 0:
            total_nav += residual_cash
            total_cash += residual_cash
        if math.isclose(total_nav, 0.0, abs_tol=1e-9) and current_date == all_dates[0] and seed_nav > 0:
            total_nav = float(seed_nav)
            total_cash = float(seed_nav)
        running_peak = max(running_peak, total_nav)
        drawdown = ((total_nav / running_peak) - 1.0) if running_peak > 0 else 0.0
        cumulative_pnl = total_nav - seed_nav
        cumulative_return = (cumulative_pnl / seed_nav) if seed_nav > 0 else 0.0
        period_pnl = (total_nav - previous_nav) if previous_nav is not None else cumulative_pnl
        period_return = (period_pnl / previous_nav) if previous_nav not in (None, 0.0) else cumulative_return
        points.append(
            PortfolioHistoryPoint(
                asOf=current_date,
                nav=total_nav,
                cash=total_cash,
                grossExposure=(weighted_gross / total_nav) if total_nav > 0 else 0.0,
                netExposure=(weighted_net / total_nav) if total_nav > 0 else 0.0,
                periodPnl=period_pnl,
                periodReturn=period_return,
                cumulativePnl=cumulative_pnl,
                cumulativeReturn=cumulative_return,
                drawdown=drawdown,
                turnover=(weighted_turnover / total_nav) if total_nav > 0 else None,
                costDragBps=(weighted_cost_drag / cost_drag_weight) if cost_drag_weight > 0 else None,
            )
        )
        previous_nav = total_nav
    return tuple(points)


def _compute_positions(
    *,
    dependencies: Sequence[PortfolioStrategyDependency],
    scale_by_sleeve: Mapping[str, float],
    snapshot_nav: float,
    as_of: date,
) -> tuple[PortfolioPosition, ...]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for dependency in dependencies:
        scale = float(scale_by_sleeve.get(dependency.sleeve_id) or 0.0)
        if math.isclose(scale, 0.0, abs_tol=1e-12):
            continue
        for sample in dependency.positions:
            quantity = float(sample.quantity) * scale
            if math.isclose(quantity, 0.0, abs_tol=1e-12):
                continue
            market_value = float(sample.market_value) * scale
            symbol = sample.symbol
            entry = by_symbol.setdefault(
                symbol,
                {
                    "quantity": 0.0,
                    "market_value": 0.0,
                    "average_cost_basis": 0.0,
                    "realized_pnl": 0.0,
                    "last_price": float(sample.last_price),
                    "contributors": [],
                },
            )
            entry["quantity"] += quantity
            entry["market_value"] += market_value
            entry["average_cost_basis"] += float(sample.average_cost) * quantity
            entry["realized_pnl"] += float(sample.realized_pnl) * scale
            entry["last_price"] = float(sample.last_price)
            entry["contributors"].append(
                PortfolioPositionContributor(
                    sleeveId=dependency.sleeve_id,
                    strategyName=dependency.strategy_name,
                    strategyVersion=dependency.strategy_version,
                    quantity=quantity,
                    marketValue=market_value,
                    weight=(market_value / snapshot_nav) if snapshot_nav > 0 else 0.0,
                )
            )
    positions: list[PortfolioPosition] = []
    for symbol, payload in by_symbol.items():
        quantity = float(payload["quantity"])
        if quantity <= 1e-9:
            continue
        average_cost = float(payload["average_cost_basis"] / quantity) if quantity > 0 else 0.0
        last_price = float(payload["last_price"] or 0.0)
        market_value = float(payload["market_value"])
        positions.append(
            PortfolioPosition(
                asOf=as_of,
                symbol=symbol,
                quantity=quantity,
                marketValue=market_value,
                weight=(market_value / snapshot_nav) if snapshot_nav > 0 else 0.0,
                averageCost=average_cost,
                lastPrice=last_price,
                unrealizedPnl=(market_value - (quantity * average_cost)),
                realizedPnl=float(payload["realized_pnl"]),
                contributors=payload["contributors"],
            )
        )
    positions.sort(key=lambda item: item.marketValue, reverse=True)
    return tuple(positions)


def _merge_alerts(
    *,
    existing: Iterable[PortfolioAlert],
    generated: Iterable[PortfolioAlert],
) -> tuple[PortfolioAlert, ...]:
    by_id: dict[str, PortfolioAlert] = {}
    for alert in list(existing) + list(generated):
        by_id[alert.alertId] = alert
    return tuple(sorted(by_id.values(), key=lambda item: (item.status != "open", item.detectedAt, item.alertId)))


def _compute_attribution(
    *,
    bundle: PortfolioMaterializationBundle,
    dependencies: Sequence[PortfolioStrategyDependency],
    scale_by_sleeve: Mapping[str, float],
    target_weights_by_sleeve: Mapping[str, float],
    snapshot_nav: float,
    as_of: date,
) -> tuple[StrategySliceAttribution, ...]:
    portfolio_revision = bundle.portfolio_revision
    if portfolio_revision is None:
        return ()
    dependency_by_sleeve = {dependency.sleeve_id: dependency for dependency in dependencies}
    slices: list[StrategySliceAttribution] = []
    for allocation in portfolio_revision.allocations:
        dependency = dependency_by_sleeve.get(allocation.sleeveId)
        if dependency is None:
            continue
        scale = float(scale_by_sleeve.get(allocation.sleeveId) or 0.0)
        market_value = dependency.latest_portfolio_value * scale
        actual_weight = (market_value / snapshot_nav) if snapshot_nav > 0 else 0.0
        initial_allocated_capital = dependency.initial_portfolio_value * scale
        pnl_contribution = market_value - initial_allocated_capital
        slices.append(
            StrategySliceAttribution(
                asOf=as_of,
                sleeveId=allocation.sleeveId,
                strategyName=allocation.strategy.strategyName,
                strategyVersion=allocation.strategy.strategyVersion,
                targetWeight=float(target_weights_by_sleeve.get(allocation.sleeveId) or 0.0),
                actualWeight=actual_weight,
                marketValue=market_value,
                grossExposure=max(dependency.latest_gross_exposure * actual_weight, 0.0),
                netExposure=dependency.latest_net_exposure * actual_weight,
                pnlContribution=pnl_contribution,
                returnContribution=(pnl_contribution / snapshot_nav) if snapshot_nav > 0 else 0.0,
                drawdownContribution=(dependency.latest_drawdown or 0.0) * actual_weight,
                turnover=dependency.latest_turnover,
                sinceInceptionReturn=dependency.latest_cumulative_return,
            )
        )
    slices.sort(key=lambda item: item.marketValue, reverse=True)
    return tuple(slices)


def _compute_generated_alerts(
    *,
    bundle: PortfolioMaterializationBundle,
    history: Sequence[PortfolioHistoryPoint],
    attribution: Sequence[StrategySliceAttribution],
    as_of: date,
) -> tuple[PortfolioAlert, ...]:
    alerts: list[PortfolioAlert] = []
    account_id = bundle.account.accountId
    if bundle.active_assignment is None:
        alerts.append(
            PortfolioAlert(
                alertId=_stable_alert_id(account_id, "no_assignment", as_of),
                accountId=account_id,
                severity="critical",
                status="open",
                code="no_assignment",
                title="No Active Portfolio Assignment",
                description="The account cannot be materialized without an active portfolio assignment.",
                detectedAt=_utc_now(),
                asOf=as_of,
            )
        )
    if bundle.portfolio_revision is None:
        alerts.append(
            PortfolioAlert(
                alertId=_stable_alert_id(account_id, "no_portfolio_revision", as_of),
                accountId=account_id,
                severity="critical",
                status="open",
                code="no_portfolio_revision",
                title="No Pinned Portfolio Revision",
                description="The account assignment does not resolve to a pinned portfolio revision.",
                detectedAt=_utc_now(),
                asOf=as_of,
            )
        )
    if history:
        latest_history = history[-1]
        cash_ratio = (latest_history.cash / latest_history.nav) if latest_history.nav > 0 else 0.0
        if latest_history.nav > 0 and abs(cash_ratio) >= 0.2:
            alerts.append(
                PortfolioAlert(
                    alertId=_stable_alert_id(account_id, "cash_residual_high", as_of),
                    accountId=account_id,
                    severity="warning",
                    status="open",
                    code="cash_residual_high",
                    title="High Cash Residual",
                    description=f"Cash is {cash_ratio:.2%} of NAV, above the default monitoring threshold.",
                    detectedAt=_utc_now(),
                    asOf=as_of,
                )
            )
    for slice_payload in attribution:
        if abs(float(slice_payload.actualWeight) - float(slice_payload.targetWeight)) < 0.1:
            continue
        alerts.append(
            PortfolioAlert(
                alertId=_stable_alert_id(account_id, f"drift_{slice_payload.sleeveId}", as_of),
                accountId=account_id,
                severity="warning",
                status="open",
                code=f"drift_{slice_payload.sleeveId}",
                title=f"Sleeve Drift: {slice_payload.sleeveId}",
                description=(
                    f"Sleeve {slice_payload.sleeveId} target weight {slice_payload.targetWeight:.2%} "
                    f"drifted to {slice_payload.actualWeight:.2%}."
                ),
                detectedAt=_utc_now(),
                asOf=as_of,
            )
        )
    return tuple(alerts)


def _compute_materialized_surfaces(
    *,
    bundle: PortfolioMaterializationBundle,
    dependencies: Sequence[PortfolioStrategyDependency],
) -> PortfolioMaterializedSurfaces:
    seed_nav = _seed_capital(bundle=bundle, dependencies=dependencies)
    dependency_by_sleeve = {dependency.sleeve_id: dependency for dependency in dependencies}
    scale_by_sleeve: dict[str, float] = {}
    target_weights_by_sleeve: dict[str, float] = {}
    residual_cash = 0.0
    if bundle.portfolio_revision is not None:
        target_weights_by_sleeve = _target_weights_by_sleeve(
            portfolio_revision=bundle.portfolio_revision,
            seed_nav=seed_nav,
        )
        for allocation in bundle.portfolio_revision.allocations:
            dependency = dependency_by_sleeve.get(allocation.sleeveId)
            if dependency is None:
                continue
            initial_capital = seed_nav * float(target_weights_by_sleeve.get(allocation.sleeveId) or 0.0)
            if dependency.initial_portfolio_value > 0:
                scale_by_sleeve[allocation.sleeveId] = initial_capital / dependency.initial_portfolio_value
            else:
                scale_by_sleeve[allocation.sleeveId] = 0.0
        residual_cash = _residual_cash(seed_nav=seed_nav, target_weights=target_weights_by_sleeve)
    history = _compute_history(
        bundle=bundle,
        dependencies=dependencies,
        scale_by_sleeve=scale_by_sleeve,
        seed_nav=seed_nav,
        residual_cash=residual_cash,
    )
    latest_history = history[-1]
    as_of = bundle.as_of or latest_history.asOf or bundle.account.inceptionDate
    snapshot_nav = float(latest_history.nav or 0.0)
    positions = _compute_positions(
        dependencies=dependencies,
        scale_by_sleeve=scale_by_sleeve,
        snapshot_nav=snapshot_nav,
        as_of=as_of,
    )
    attribution = _compute_attribution(
        bundle=bundle,
        dependencies=dependencies,
        scale_by_sleeve=scale_by_sleeve,
        target_weights_by_sleeve=target_weights_by_sleeve,
        snapshot_nav=snapshot_nav,
        as_of=as_of,
    )
    freshness = list(bundle.freshness) or _default_freshness(dependencies=dependencies)
    generated_alerts = _compute_generated_alerts(
        bundle=bundle,
        history=history,
        attribution=attribution,
        as_of=as_of,
    )
    alerts = _merge_alerts(existing=bundle.alerts, generated=generated_alerts)
    snapshot = PortfolioSnapshot(
        accountId=bundle.account.accountId,
        accountName=bundle.account.name,
        asOf=as_of,
        nav=snapshot_nav,
        cash=float(latest_history.cash or 0.0),
        grossExposure=float(latest_history.grossExposure or 0.0),
        netExposure=float(latest_history.netExposure or 0.0),
        sinceInceptionPnl=float(latest_history.cumulativePnl or 0.0),
        sinceInceptionReturn=float(latest_history.cumulativeReturn or 0.0),
        currentDrawdown=float(latest_history.drawdown or 0.0),
        maxDrawdown=min((float(point.drawdown or 0.0) for point in history), default=None),
        openAlertCount=sum(1 for alert in alerts if alert.status == "open"),
        activeAssignment=bundle.active_assignment,
        freshness=freshness,
        slices=list(attribution),
    )
    return PortfolioMaterializedSurfaces(
        snapshot=snapshot,
        history=history,
        positions=positions,
        attribution=attribution,
        alerts=alerts,
    )


def _snapshot_row(
    *,
    bundle: PortfolioMaterializationBundle,
    result: PortfolioMaterializedSurfaces,
    dependency_fingerprint: str | None,
    dependency_state: Mapping[str, Any],
    materialized_at: datetime,
) -> dict[str, Any]:
    snapshot = result.snapshot
    return {
        "account_id": bundle.account.accountId,
        "account_name": snapshot.accountName,
        "as_of_date": snapshot.asOf,
        "nav": snapshot.nav,
        "cash": snapshot.cash,
        "gross_exposure": snapshot.grossExposure,
        "net_exposure": snapshot.netExposure,
        "since_inception_pnl": snapshot.sinceInceptionPnl,
        "since_inception_return": snapshot.sinceInceptionReturn,
        "current_drawdown": snapshot.currentDrawdown,
        "max_drawdown": snapshot.maxDrawdown,
        "open_alert_count": snapshot.openAlertCount,
        "active_assignment_json": (
            snapshot.activeAssignment.model_dump(mode="json") if snapshot.activeAssignment is not None else None
        ),
        "freshness_json": [item.model_dump(mode="json") for item in snapshot.freshness],
        "dependency_fingerprint": dependency_fingerprint,
        "dependency_state_json": dict(dependency_state),
        "materialized_at": materialized_at,
    }


def _history_rows(
    *,
    account_id: str,
    history: Sequence[PortfolioHistoryPoint],
    materialized_at: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "account_id": account_id,
            "as_of_date": point.asOf,
            "nav": point.nav,
            "cash": point.cash,
            "gross_exposure": point.grossExposure,
            "net_exposure": point.netExposure,
            "period_pnl": point.periodPnl,
            "period_return": point.periodReturn,
            "cumulative_pnl": point.cumulativePnl,
            "cumulative_return": point.cumulativeReturn,
            "drawdown": point.drawdown,
            "turnover": point.turnover,
            "cost_drag_bps": point.costDragBps,
            "materialized_at": materialized_at,
        }
        for point in history
    ]


def _position_rows(
    *,
    account_id: str,
    positions: Sequence[PortfolioPosition],
    materialized_at: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "account_id": account_id,
            "as_of_date": position.asOf,
            "symbol": position.symbol,
            "quantity": position.quantity,
            "market_value": position.marketValue,
            "weight": position.weight,
            "average_cost": position.averageCost,
            "last_price": position.lastPrice,
            "unrealized_pnl": position.unrealizedPnl,
            "realized_pnl": position.realizedPnl,
            "contributors_json": [item.model_dump(mode="json") for item in position.contributors],
            "materialized_at": materialized_at,
        }
        for position in positions
    ]


def _attribution_rows(
    *,
    account_id: str,
    attribution: Sequence[StrategySliceAttribution],
    materialized_at: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "account_id": account_id,
            "as_of_date": slice_payload.asOf,
            "sleeve_id": slice_payload.sleeveId,
            "strategy_name": slice_payload.strategyName,
            "strategy_version": slice_payload.strategyVersion,
            "target_weight": slice_payload.targetWeight,
            "actual_weight": slice_payload.actualWeight,
            "market_value": slice_payload.marketValue,
            "gross_exposure": slice_payload.grossExposure,
            "net_exposure": slice_payload.netExposure,
            "pnl_contribution": slice_payload.pnlContribution,
            "return_contribution": slice_payload.returnContribution,
            "drawdown_contribution": slice_payload.drawdownContribution,
            "turnover": slice_payload.turnover,
            "since_inception_return": slice_payload.sinceInceptionReturn,
            "materialized_at": materialized_at,
        }
        for slice_payload in attribution
    ]


def _alert_rows(
    *,
    account_id: str,
    alerts: Sequence[PortfolioAlert],
    materialized_at: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "account_id": account_id,
            "alert_id": alert.alertId,
            "severity": alert.severity,
            "status": alert.status,
            "code": alert.code,
            "title": alert.title,
            "description": alert.description,
            "detected_at": alert.detectedAt,
            "acknowledged_at": alert.acknowledgedAt,
            "acknowledged_by": alert.acknowledgedBy,
            "resolved_at": alert.resolvedAt,
            "as_of_date": alert.asOf,
            "materialized_at": materialized_at,
        }
        for alert in alerts
    ]


def _persist_materialization(
    dsn: str,
    *,
    bundle: PortfolioMaterializationBundle,
    result: PortfolioMaterializedSurfaces,
    dependency_fingerprint: str | None,
    dependency_state: Mapping[str, Any],
) -> None:
    materialized_at = _utc_now()
    scope_values = {"account_id": bundle.account.accountId}
    snapshot_row = _snapshot_row(
        bundle=bundle,
        result=result,
        dependency_fingerprint=dependency_fingerprint,
        dependency_state=dependency_state,
        materialized_at=materialized_at,
    )
    history_rows = _history_rows(
        account_id=bundle.account.accountId,
        history=result.history,
        materialized_at=materialized_at,
    )
    position_rows = _position_rows(
        account_id=bundle.account.accountId,
        positions=result.positions,
        materialized_at=materialized_at,
    )
    attribution_rows = _attribution_rows(
        account_id=bundle.account.accountId,
        attribution=result.attribution,
        materialized_at=materialized_at,
    )
    alert_rows = _alert_rows(
        account_id=bundle.account.accountId,
        alerts=result.alerts,
        materialized_at=materialized_at,
    )
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_connection_is_writable(cur)
            _upsert_latest_snapshot(cur, snapshot_row=snapshot_row)
            _apply_serving_table(cur, config=_SERVING_TABLE_CONFIGS[0], rows=history_rows, scope_values=scope_values)
            _apply_serving_table(cur, config=_SERVING_TABLE_CONFIGS[1], rows=position_rows, scope_values=scope_values)
            _apply_serving_table(cur, config=_SERVING_TABLE_CONFIGS[2], rows=attribution_rows, scope_values=scope_values)
            _apply_serving_table(cur, config=_SERVING_TABLE_CONFIGS[3], rows=alert_rows, scope_values=scope_values)
            _upsert_materialization_state(
                cur,
                account_id=bundle.account.accountId,
                dependency_fingerprint=dependency_fingerprint,
                dependency_state=dependency_state,
                as_of=result.snapshot.asOf,
                materialized_at=materialized_at,
            )


def materialize_portfolio_bundle(
    dsn: str,
    bundle: PortfolioMaterializationBundle,
    *,
    heartbeat: Callable[[], None] | None = None,
) -> PortfolioMaterializationResult:
    dependencies, dependency_fingerprint, dependency_state = _resolve_strategy_dependencies(
        dsn,
        bundle=bundle,
        heartbeat=heartbeat,
    )
    if heartbeat is not None:
        heartbeat()
    result = _compute_materialized_surfaces(bundle=bundle, dependencies=dependencies)
    return PortfolioMaterializationResult(
        snapshot=result.snapshot,
        history=result.history,
        positions=result.positions,
        attribution=result.attribution,
        alerts=result.alerts,
        dependency_fingerprint=dependency_fingerprint,
        dependency_state=dependency_state,
    )


def materialize_and_persist_portfolio_bundle(
    dsn: str,
    bundle: PortfolioMaterializationBundle,
    *,
    heartbeat: Callable[[], None] | None = None,
) -> PortfolioMaterializationResult:
    result = materialize_portfolio_bundle(dsn, bundle, heartbeat=heartbeat)
    surfaces = PortfolioMaterializedSurfaces(
        snapshot=result.snapshot,
        history=result.history,
        positions=result.positions,
        attribution=result.attribution,
        alerts=result.alerts,
    )
    _persist_materialization(
        dsn,
        bundle=bundle,
        result=surfaces,
        dependency_fingerprint=result.dependency_fingerprint,
        dependency_state=result.dependency_state,
    )
    return result


class PortfolioServingRepository:
    def __init__(self, dsn: str):
        self.dsn = _normalize_text(dsn)
        if not self.dsn:
            raise ValueError("Postgres DSN is required.")

    def _get_latest_as_of(self, account_id: str) -> date | None:
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT as_of_date FROM core.portfolio_latest_snapshot WHERE account_id = %s",
                    (account_id,),
                )
                row = cur.fetchone()
        return row[0] if row and row[0] is not None else None

    def get_attribution(
        self,
        account_id: str,
        *,
        as_of: date | None = None,
    ) -> tuple[StrategySliceAttribution, ...]:
        resolved_as_of = as_of or self._get_latest_as_of(account_id)
        if resolved_as_of is None:
            return ()
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        as_of_date,
                        sleeve_id,
                        strategy_name,
                        strategy_version,
                        target_weight,
                        actual_weight,
                        market_value,
                        gross_exposure,
                        net_exposure,
                        pnl_contribution,
                        return_contribution,
                        drawdown_contribution,
                        turnover,
                        since_inception_return
                    FROM core.portfolio_attribution
                    WHERE account_id = %s
                      AND as_of_date = %s
                    ORDER BY market_value DESC, sleeve_id ASC
                    """,
                    (account_id, resolved_as_of),
                )
                rows = cur.fetchall()
        return tuple(
            StrategySliceAttribution(
                asOf=row[0],
                sleeveId=row[1],
                strategyName=row[2],
                strategyVersion=int(row[3]),
                targetWeight=float(row[4] or 0.0),
                actualWeight=float(row[5] or 0.0),
                marketValue=float(row[6] or 0.0),
                grossExposure=float(row[7] or 0.0),
                netExposure=float(row[8] or 0.0),
                pnlContribution=float(row[9] or 0.0),
                returnContribution=float(row[10] or 0.0),
                drawdownContribution=float(row[11] or 0.0),
                turnover=(float(row[12]) if row[12] is not None else None),
                sinceInceptionReturn=(float(row[13]) if row[13] is not None else None),
            )
            for row in rows
        )

    def get_latest_snapshot(self, account_id: str) -> PortfolioSnapshot | None:
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        account_id,
                        account_name,
                        as_of_date,
                        nav,
                        cash,
                        gross_exposure,
                        net_exposure,
                        since_inception_pnl,
                        since_inception_return,
                        current_drawdown,
                        max_drawdown,
                        open_alert_count,
                        active_assignment_json,
                        freshness_json
                    FROM core.portfolio_latest_snapshot
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        payload = {
            "accountId": row[0],
            "accountName": row[1],
            "asOf": row[2],
            "nav": float(row[3] or 0.0),
            "cash": float(row[4] or 0.0),
            "grossExposure": float(row[5] or 0.0),
            "netExposure": float(row[6] or 0.0),
            "sinceInceptionPnl": float(row[7] or 0.0),
            "sinceInceptionReturn": float(row[8] or 0.0),
            "currentDrawdown": float(row[9] or 0.0),
            "maxDrawdown": (float(row[10]) if row[10] is not None else None),
            "openAlertCount": int(row[11] or 0),
            "activeAssignment": _parse_json(row[12], None),
            "freshness": _parse_json(row[13], []),
            "slices": [item.model_dump(mode="json") for item in self.get_attribution(account_id, as_of=row[2])],
        }
        return PortfolioSnapshot.model_validate(payload)

    def get_history(self, account_id: str, *, limit: int | None = None) -> tuple[PortfolioHistoryPoint, ...]:
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT
                        as_of_date,
                        nav,
                        cash,
                        gross_exposure,
                        net_exposure,
                        period_pnl,
                        period_return,
                        cumulative_pnl,
                        cumulative_return,
                        drawdown,
                        turnover,
                        cost_drag_bps
                    FROM core.portfolio_history
                    WHERE account_id = %s
                    ORDER BY as_of_date DESC
                """
                params: list[Any] = [account_id]
                if limit is not None:
                    sql += " LIMIT %s"
                    params.append(max(1, int(limit)))
                cur.execute(sql, tuple(params))
                rows = list(reversed(cur.fetchall()))
        return tuple(
            PortfolioHistoryPoint(
                asOf=row[0],
                nav=float(row[1] or 0.0),
                cash=float(row[2] or 0.0),
                grossExposure=float(row[3] or 0.0),
                netExposure=float(row[4] or 0.0),
                periodPnl=(float(row[5]) if row[5] is not None else None),
                periodReturn=(float(row[6]) if row[6] is not None else None),
                cumulativePnl=(float(row[7]) if row[7] is not None else None),
                cumulativeReturn=(float(row[8]) if row[8] is not None else None),
                drawdown=(float(row[9]) if row[9] is not None else None),
                turnover=(float(row[10]) if row[10] is not None else None),
                costDragBps=(float(row[11]) if row[11] is not None else None),
            )
            for row in rows
        )

    def get_positions(
        self,
        account_id: str,
        *,
        as_of: date | None = None,
    ) -> tuple[PortfolioPosition, ...]:
        resolved_as_of = as_of or self._get_latest_as_of(account_id)
        if resolved_as_of is None:
            return ()
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        as_of_date,
                        symbol,
                        quantity,
                        market_value,
                        weight,
                        average_cost,
                        last_price,
                        unrealized_pnl,
                        realized_pnl,
                        contributors_json
                    FROM core.portfolio_positions
                    WHERE account_id = %s
                      AND as_of_date = %s
                    ORDER BY market_value DESC, symbol ASC
                    """,
                    (account_id, resolved_as_of),
                )
                rows = cur.fetchall()
        return tuple(
            PortfolioPosition.model_validate(
                {
                    "asOf": row[0],
                    "symbol": row[1],
                    "quantity": float(row[2] or 0.0),
                    "marketValue": float(row[3] or 0.0),
                    "weight": float(row[4] or 0.0),
                    "averageCost": (float(row[5]) if row[5] is not None else None),
                    "lastPrice": (float(row[6]) if row[6] is not None else None),
                    "unrealizedPnl": (float(row[7]) if row[7] is not None else None),
                    "realizedPnl": (float(row[8]) if row[8] is not None else None),
                    "contributors": _parse_json(row[9], []),
                }
            )
            for row in rows
        )

    def get_alerts(
        self,
        account_id: str,
        *,
        include_resolved: bool = True,
    ) -> tuple[PortfolioAlert, ...]:
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                predicate = "" if include_resolved else "AND status <> 'resolved'"
                cur.execute(
                    f"""
                    SELECT
                        alert_id,
                        account_id,
                        severity,
                        status,
                        code,
                        title,
                        description,
                        detected_at,
                        acknowledged_at,
                        acknowledged_by,
                        resolved_at,
                        as_of_date
                    FROM core.portfolio_alerts
                    WHERE account_id = %s
                      {predicate}
                    ORDER BY detected_at DESC, alert_id ASC
                    """,
                    (account_id,),
                )
                rows = cur.fetchall()
        return tuple(
            PortfolioAlert(
                alertId=row[0],
                accountId=row[1],
                severity=row[2],
                status=row[3],
                code=row[4],
                title=row[5],
                description=row[6],
                detectedAt=_ensure_utc(row[7]),
                acknowledgedAt=(_ensure_utc(row[8]) if isinstance(row[8], datetime) else None),
                acknowledgedBy=row[9],
                resolvedAt=(_ensure_utc(row[10]) if isinstance(row[10], datetime) else None),
                asOf=row[11],
            )
            for row in rows
        )


__all__ = [
    "PortfolioMaterializationError",
    "PortfolioMaterializationResult",
    "PortfolioMaterializationStaleDependencyError",
    "PortfolioServingRepository",
    "materialize_and_persist_portfolio_bundle",
    "materialize_portfolio_bundle",
]
