from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from core.postgres import connect, copy_rows
from core.ranking_engine.contracts import (
    RankingGroup,
    RankingMaterializationSummary,
    RankingPreviewRow,
    RankingSchemaConfig,
    RankingTransform,
)
from core.ranking_engine.naming import build_scoped_identifier, slugify_strategy_output_table
from core.ranking_repository import RankingRepository
from core.strategy_engine import StrategyConfig, UniverseCondition, UniverseDefinition, UniverseGroup
from core.strategy_engine import universe as universe_service
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MaterializationContext:
    strategy_name: str
    output_table_name: str
    strategy_config: StrategyConfig
    ranking_schema_name: str
    ranking_schema_version: int
    ranking_schema: RankingSchemaConfig
    strategy_universe: UniverseDefinition
    ranking_universe: UniverseDefinition
    table_specs: dict[str, Any]
    required_columns: dict[str, set[str]]


@dataclass(frozen=True)
class _ResolvedDateRange:
    start_date: date
    end_date: date
    source_start_date: date
    source_end_date: date
    previous_watermark: date | None
    noop: bool = False
    reason: str | None = None


def preview_strategy_rankings(
    dsn: str,
    *,
    strategy_name: str,
    schema: RankingSchemaConfig,
    as_of_date: date,
    limit: int = 25,
) -> dict[str, Any]:
    strategy = _load_strategy(dsn, strategy_name)
    strategy_config: StrategyConfig = strategy["config"]
    strategy_universe = _resolve_strategy_universe(dsn, strategy_config)
    ranking_universe = _resolve_ranking_universe(dsn, schema)
    table_specs = universe_service._load_gold_table_specs(dsn)
    required_columns = _collect_required_columns(strategy_universe, ranking_universe, schema)
    ranked = _compute_rankings_dataframe(
        dsn,
        strategy_config=strategy_config,
        ranking_schema=schema,
        start_date=as_of_date,
        end_date=as_of_date,
        table_specs=table_specs,
        strategy_universe=strategy_universe,
        ranking_universe=ranking_universe,
        required_columns=required_columns,
    )
    preview_rows = [
        RankingPreviewRow(symbol=str(row["symbol"]), rank=int(row["rank"]), score=float(row["score"])).model_dump()
        for _, row in ranked.head(limit).iterrows()
    ]
    return {
        "strategyName": strategy_name,
        "asOfDate": as_of_date,
        "rowCount": int(len(ranked)),
        "rows": preview_rows,
        "warnings": [] if not ranked.empty else ["Preview returned zero ranked symbols."],
    }


def materialize_strategy_rankings(
    dsn: str,
    *,
    strategy_name: str,
    start_date: date | None = None,
    end_date: date | None = None,
    triggered_by: str = "manual",
    strategy_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    context = _load_materialization_context(dsn, strategy_name, strategy_payload=strategy_payload)
    resolved_range = _resolve_date_range(
        dsn,
        strategy_name=strategy_name,
        strategy_config=context.strategy_config,
        ranking_schema=context.ranking_schema,
        start_date=start_date,
        end_date=end_date,
        table_specs=context.table_specs,
        strategy_universe=context.strategy_universe,
        ranking_universe=context.ranking_universe,
        required_columns=context.required_columns,
    )

    if resolved_range.noop:
        run_id = uuid.uuid4().hex
        try:
            _persist_noop_run(
                dsn,
                run_id=run_id,
                context=context,
                resolved_range=resolved_range,
                triggered_by=triggered_by,
            )
        except Exception:
            logger.exception("Ranking no-op run recording failed for strategy '%s'.", strategy_name)
            _update_ranking_run_after_failure(dsn, run_id=run_id, error="Failed to record no-op ranking run.")
            raise
        return _build_materialization_result(
            run_id=run_id,
            context=context,
            resolved_range=resolved_range,
            row_count=0,
            date_count=0,
            status="noop",
            reason=resolved_range.reason,
            current_watermark=resolved_range.previous_watermark,
        )

    ranked = _compute_rankings_dataframe(
        dsn,
        strategy_config=context.strategy_config,
        ranking_schema=context.ranking_schema,
        start_date=resolved_range.start_date,
        end_date=resolved_range.end_date,
        table_specs=context.table_specs,
        strategy_universe=context.strategy_universe,
        ranking_universe=context.ranking_universe,
        required_columns=context.required_columns,
    )
    run_id = uuid.uuid4().hex
    date_count = int(ranked["date"].nunique()) if not ranked.empty else 0

    try:
        rows_written = _persist_materialization(
            dsn,
            run_id=run_id,
            context=context,
            resolved_range=resolved_range,
            ranked=ranked,
            triggered_by=triggered_by,
            date_count=date_count,
        )
    except Exception as exc:
        logger.exception("Ranking materialization failed for strategy '%s'.", strategy_name)
        _update_ranking_run_after_failure(dsn, run_id=run_id, error=str(exc))
        raise

    return _build_materialization_result(
        run_id=run_id,
        context=context,
        resolved_range=resolved_range,
        row_count=rows_written,
        date_count=date_count,
        status="success",
        reason=None,
        current_watermark=resolved_range.end_date,
    )


def _load_strategy(
    dsn: str,
    strategy_name: str,
    *,
    strategy_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    strategy: dict[str, Any] | None = None
    if strategy_payload is not None:
        candidate = dict(strategy_payload)
        candidate_name = str(candidate.get("name") or strategy_name).strip()
        if candidate_name == strategy_name and candidate.get("config") is not None:
            strategy = candidate

    if strategy is None:
        repo = StrategyRepository(dsn)
        strategy = repo.get_strategy(strategy_name)

    if not strategy:
        raise ValueError(f"Strategy '{strategy_name}' not found.")

    normalized = dict(strategy)
    normalized["config"] = StrategyConfig.model_validate(strategy.get("config") or {})
    return normalized


def _load_materialization_context(
    dsn: str,
    strategy_name: str,
    *,
    strategy_payload: Mapping[str, Any] | None = None,
) -> _MaterializationContext:
    strategy = _load_strategy(dsn, strategy_name, strategy_payload=strategy_payload)
    strategy_config: StrategyConfig = strategy["config"]
    if not strategy_config.rankingSchemaName:
        raise ValueError(f"Strategy '{strategy_name}' does not reference a ranking schema.")

    ranking_repo = RankingRepository(dsn)
    ranking_schema_record = ranking_repo.get_ranking_schema(strategy_config.rankingSchemaName)
    if not ranking_schema_record:
        raise ValueError(f"Ranking schema '{strategy_config.rankingSchemaName}' not found.")

    ranking_schema = RankingSchemaConfig.model_validate(ranking_schema_record["config"])
    strategy_universe = _resolve_strategy_universe(dsn, strategy_config)
    ranking_universe = _resolve_ranking_universe(dsn, ranking_schema)
    table_specs = universe_service._load_gold_table_specs(dsn)
    required_columns = _collect_required_columns(strategy_universe, ranking_universe, ranking_schema)
    output_table_name = str(strategy.get("output_table_name") or "").strip() or slugify_strategy_output_table(strategy_name)
    return _MaterializationContext(
        strategy_name=strategy_name,
        output_table_name=output_table_name,
        strategy_config=strategy_config,
        ranking_schema_name=strategy_config.rankingSchemaName,
        ranking_schema_version=int(ranking_schema_record["version"]),
        ranking_schema=ranking_schema,
        strategy_universe=strategy_universe,
        ranking_universe=ranking_universe,
        table_specs=table_specs,
        required_columns=required_columns,
    )


def _resolve_date_range(
    dsn: str,
    *,
    strategy_name: str,
    strategy_config: StrategyConfig,
    ranking_schema: RankingSchemaConfig,
    start_date: date | None,
    end_date: date | None,
    table_specs: dict[str, Any] | None = None,
    strategy_universe: UniverseDefinition | None = None,
    ranking_universe: UniverseDefinition | None = None,
    required_columns: dict[str, set[str]] | None = None,
) -> _ResolvedDateRange:
    resolved_table_specs = table_specs or universe_service._load_gold_table_specs(dsn)
    resolved_strategy_universe = strategy_universe or _resolve_strategy_universe(dsn, strategy_config)
    resolved_ranking_universe = ranking_universe or _resolve_ranking_universe(dsn, ranking_schema)
    referenced_columns = required_columns or _collect_required_columns(
        resolved_strategy_universe,
        resolved_ranking_universe,
        ranking_schema,
    )
    source_start_date, source_end_date = _load_source_date_bounds(
        dsn,
        table_specs=resolved_table_specs,
        required_columns=referenced_columns,
    )
    previous_watermark = _get_ranking_watermark(dsn, strategy_name)

    resolved_start = start_date or (
        previous_watermark + timedelta(days=1) if previous_watermark is not None else source_start_date
    )
    resolved_end = end_date or source_end_date

    if resolved_start > resolved_end:
        if start_date is None and end_date is None and previous_watermark is not None and previous_watermark >= source_end_date:
            return _ResolvedDateRange(
                start_date=source_end_date,
                end_date=source_end_date,
                source_start_date=source_start_date,
                source_end_date=source_end_date,
                previous_watermark=previous_watermark,
                noop=True,
                reason="Ranking output already current.",
            )
        raise ValueError(
            "Resolved ranking date range is invalid: "
            f"start_date={resolved_start.isoformat()} end_date={resolved_end.isoformat()}"
        )

    return _ResolvedDateRange(
        start_date=resolved_start,
        end_date=resolved_end,
        source_start_date=source_start_date,
        source_end_date=source_end_date,
        previous_watermark=previous_watermark,
    )


def _load_source_date_bounds(
    dsn: str,
    *,
    table_specs: dict[str, Any],
    required_columns: dict[str, set[str]],
) -> tuple[date, date]:
    candidate_dates: list[date] = []
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            for table_name in required_columns.keys():
                spec = table_specs.get(table_name)
                if spec is None:
                    raise ValueError(f"Unknown gold table '{table_name}'.")
                cur.execute(
                    f"""
                    SELECT MIN({universe_service._quote_identifier(spec.as_of_column)}),
                           MAX({universe_service._quote_identifier(spec.as_of_column)})
                    FROM "gold".{universe_service._quote_identifier(table_name)}
                    """
                )
                row = cur.fetchone()
                if not row:
                    continue
                for value in row:
                    normalized = _normalize_as_of_value(value)
                    if normalized is not None:
                        candidate_dates.append(normalized)

    if not candidate_dates:
        raise ValueError("No ranking source data is available for the referenced gold tables.")

    return min(candidate_dates), max(candidate_dates)


def _get_ranking_watermark(dsn: str, strategy_name: str) -> date | None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_ranked_date
                FROM core.ranking_watermarks
                WHERE strategy_name = %s
                """,
                (strategy_name,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return _normalize_as_of_value(row[0])


def _normalize_as_of_value(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    normalized = pd.to_datetime(value, errors="coerce")
    if pd.isna(normalized):
        return None
    return normalized.date()


def _build_materialization_result(
    *,
    run_id: str,
    context: _MaterializationContext,
    resolved_range: _ResolvedDateRange,
    row_count: int,
    date_count: int,
    status: str,
    reason: str | None,
    current_watermark: date | None,
) -> dict[str, Any]:
    result = RankingMaterializationSummary(
        runId=run_id,
        strategyName=context.strategy_name,
        rankingSchemaName=context.ranking_schema_name,
        rankingSchemaVersion=context.ranking_schema_version,
        outputTableName=context.output_table_name,
        startDate=resolved_range.start_date,
        endDate=resolved_range.end_date,
        rowCount=row_count,
        dateCount=date_count,
    ).model_dump()
    result.update(
        {
            "status": status,
            "reason": reason,
            "previousWatermark": resolved_range.previous_watermark,
            "currentWatermark": current_watermark,
        }
    )
    return result


def _compute_rankings_dataframe(
    dsn: str,
    *,
    strategy_config: StrategyConfig,
    ranking_schema: RankingSchemaConfig,
    start_date: date,
    end_date: date,
    table_specs: dict[str, Any] | None = None,
    strategy_universe: UniverseDefinition | None = None,
    ranking_universe: UniverseDefinition | None = None,
    required_columns: dict[str, set[str]] | None = None,
) -> pd.DataFrame:
    resolved_table_specs = table_specs or universe_service._load_gold_table_specs(dsn)
    resolved_strategy_universe = strategy_universe or _resolve_strategy_universe(dsn, strategy_config)
    resolved_ranking_universe = ranking_universe or _resolve_ranking_universe(dsn, ranking_schema)
    resolved_required_columns = required_columns or _collect_required_columns(
        resolved_strategy_universe,
        resolved_ranking_universe,
        ranking_schema,
    )
    frames = _load_table_frames(
        dsn,
        table_specs=resolved_table_specs,
        required_columns=resolved_required_columns,
        start_date=start_date,
        end_date=end_date,
    )
    merged = _merge_frames(frames)
    if merged.empty:
        return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    filtered = merged[
        _evaluate_universe_mask(merged, resolved_strategy_universe.root)
        & _evaluate_universe_mask(merged, resolved_ranking_universe.root)
    ].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    group_scores: list[tuple[str, float, pd.Series]] = []
    required_factor_columns: list[pd.Series] = []
    for group in ranking_schema.groups:
        group_series, group_required_masks = _score_group(filtered, group)
        group_scores.append((group.name, group.weight, group_series))
        required_factor_columns.extend(group_required_masks)

    if required_factor_columns:
        required_mask = pd.concat(required_factor_columns, axis=1).all(axis=1)
        filtered = filtered[required_mask].copy()
        group_scores = [(name, weight, series.loc[filtered.index]) for name, weight, series in group_scores]
        if filtered.empty:
            return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    weighted_total = pd.Series(0.0, index=filtered.index)
    total_weight = 0.0
    for _name, weight, series in group_scores:
        weighted_total = weighted_total.add(series * weight, fill_value=0.0)
        total_weight += weight
    if total_weight <= 0:
        raise ValueError("Ranking schema produced zero total group weight.")
    filtered["score"] = weighted_total / total_weight
    filtered["score"] = _apply_transforms(filtered["score"], filtered["date"], ranking_schema.overallTransforms)
    filtered = filtered.dropna(subset=["score"]).copy()
    if filtered.empty:
        return pd.DataFrame(columns=["date", "symbol", "score", "rank"])

    filtered = filtered.sort_values(["date", "score", "symbol"], ascending=[True, False, True]).reset_index(drop=True)
    filtered["rank"] = filtered.groupby("date").cumcount() + 1
    return filtered[["date", "symbol", "score", "rank"]]


def _resolve_strategy_universe(dsn: str, strategy_config: StrategyConfig) -> UniverseDefinition:
    if strategy_config.universe is not None:
        return strategy_config.universe
    if not strategy_config.universeConfigName:
        raise ValueError("Strategy config must reference universeConfigName.")
    repo = UniverseRepository(dsn)
    universe = repo.get_universe_config(strategy_config.universeConfigName)
    if not universe:
        raise ValueError(f"Universe config '{strategy_config.universeConfigName}' not found.")
    return UniverseDefinition.model_validate(universe.get("config") or {})


def _resolve_ranking_universe(dsn: str, ranking_schema: RankingSchemaConfig) -> UniverseDefinition:
    if not ranking_schema.universeConfigName:
        raise ValueError("Ranking schema config must reference universeConfigName.")
    repo = UniverseRepository(dsn)
    universe = repo.get_universe_config(ranking_schema.universeConfigName)
    if not universe:
        raise ValueError(f"Universe config '{ranking_schema.universeConfigName}' not found.")
    return UniverseDefinition.model_validate(universe.get("config") or {})


def _collect_required_columns(
    strategy_universe: UniverseDefinition,
    ranking_universe: UniverseDefinition,
    ranking_schema: RankingSchemaConfig,
) -> dict[str, set[str]]:
    required: dict[str, set[str]] = {}
    _collect_universe_columns(strategy_universe.root, required)
    _collect_universe_columns(ranking_universe.root, required)
    for group in ranking_schema.groups:
        for factor in group.factors:
            required.setdefault(factor.table, set()).add(factor.column)
    return required


def _collect_universe_columns(node: UniverseGroup | UniverseCondition, required: dict[str, set[str]]) -> None:
    if isinstance(node, UniverseCondition):
        required.setdefault(node.table, set()).add(node.column)
        return
    for clause in node.clauses:
        _collect_universe_columns(clause, required)


def _load_table_frames(
    dsn: str,
    *,
    table_specs: dict[str, Any],
    required_columns: dict[str, set[str]],
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    with connect(dsn) as conn:
        for table_name, columns in required_columns.items():
            spec = table_specs.get(table_name)
            if spec is None:
                raise ValueError(f"Unknown gold table '{table_name}'.")
            selected_columns = sorted(columns)
            select_parts = [
                f"{universe_service._quote_identifier(spec.as_of_column)} AS date",
                f'{universe_service._quote_identifier("symbol")} AS symbol',
            ]
            select_parts.extend(universe_service._quote_identifier(column) for column in selected_columns)
            query = f"""
                SELECT {", ".join(select_parts)}
                FROM "gold".{universe_service._quote_identifier(table_name)}
                WHERE {universe_service._quote_identifier(spec.as_of_column)} >= %s
                  AND {universe_service._quote_identifier(spec.as_of_column)} <= %s
            """
            with conn.cursor() as cur:
                cur.execute(query, (start_date, end_date))
                rows = cur.fetchall()
                columns_in_result = [desc.name for desc in cur.description]
            frame = pd.DataFrame(rows, columns=columns_in_result)
            normalized_columns = [f"{table_name}__{column}" for column in selected_columns]
            if frame.empty:
                frames[table_name] = pd.DataFrame(columns=["date", "symbol", *normalized_columns])
                continue
            frame["symbol"] = frame["symbol"].astype("string").str.upper()
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
            for column in selected_columns:
                column_spec = spec.columns.get(column)
                if column_spec is None:
                    raise ValueError(f"Unknown column '{column}' for gold.{table_name}.")
                normalized = f"{table_name}__{column}"
                frame[normalized] = _normalize_loaded_column(frame[column], value_kind=column_spec.value_kind)
            frames[table_name] = frame[["date", "symbol", *normalized_columns]]
    return frames


def _normalize_loaded_column(series: pd.Series, *, value_kind: str) -> pd.Series:
    if value_kind == "number":
        return pd.to_numeric(series, errors="coerce")
    if value_kind == "boolean":
        return series.astype("boolean")
    if value_kind == "date":
        return pd.to_datetime(series, errors="coerce").dt.date
    if value_kind == "datetime":
        return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_localize(None)
    return series.astype("string")


def _merge_frames(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for frame in frames.values():
        if merged is None:
            merged = frame.copy()
            continue
        merged = merged.merge(frame, on=["date", "symbol"], how="outer")
    if merged is None:
        return pd.DataFrame(columns=["date", "symbol"])
    merged = merged.drop_duplicates(subset=["date", "symbol"]).reset_index(drop=True)
    return merged


def _evaluate_universe_mask(df: pd.DataFrame, node: UniverseGroup | UniverseCondition) -> pd.Series:
    if isinstance(node, UniverseCondition):
        column_name = f"{node.table}__{node.column}"
        if column_name not in df.columns:
            return pd.Series(False, index=df.index, dtype="boolean")
        series = df[column_name]
        operator = node.operator
        if operator == "is_null":
            return _finalize_mask(series.isna(), df.index)
        if operator == "is_not_null":
            return _finalize_mask(series.notna(), df.index)

        if operator == "in":
            values = _normalize_comparison_values(series, node.values or [])
            return _finalize_mask(series.notna() & series.isin(values), df.index)
        if operator == "not_in":
            values = _normalize_comparison_values(series, node.values or [])
            return _finalize_mask(series.notna() & ~series.isin(values), df.index)

        comparison_value = _normalize_comparison_value(series, node.value)
        if operator == "eq":
            return _finalize_mask(series.notna() & series.eq(comparison_value), df.index)
        if operator == "ne":
            return _finalize_mask(series.notna() & series.ne(comparison_value), df.index)
        if operator == "gt":
            return _finalize_mask(series.notna() & series.gt(comparison_value), df.index)
        if operator == "gte":
            return _finalize_mask(series.notna() & series.ge(comparison_value), df.index)
        if operator == "lt":
            return _finalize_mask(series.notna() & series.lt(comparison_value), df.index)
        if operator == "lte":
            return _finalize_mask(series.notna() & series.le(comparison_value), df.index)
        raise ValueError(f"Unsupported universe operator '{operator}'.")

    child_masks = [_evaluate_universe_mask(df, clause) for clause in node.clauses]
    if node.operator == "and":
        result = child_masks[0].copy()
        for mask in child_masks[1:]:
            result &= mask
        return _finalize_mask(result, df.index)
    result = child_masks[0].copy()
    for mask in child_masks[1:]:
        result |= mask
    return _finalize_mask(result, df.index)


def _normalize_comparison_values(series: pd.Series, values: list[Any]) -> list[Any]:
    return [_normalize_comparison_value(series, value) for value in values]


def _normalize_comparison_value(series: pd.Series, value: Any) -> Any:
    if value is None:
        return None
    if pd.api.types.is_datetime64_any_dtype(series):
        normalized = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(normalized):
            return value
        return normalized.tz_convert("UTC").tz_localize(None)
    non_null = series[series.notna()]
    if not non_null.empty:
        sample = non_null.iloc[0]
        if isinstance(sample, date) and not isinstance(sample, datetime):
            if isinstance(value, date) and not isinstance(value, datetime):
                return value
            text = str(value or "").strip()
            if not text:
                return value
            normalized = text.replace("Z", "+00:00")
            try:
                return date.fromisoformat(normalized)
            except ValueError:
                try:
                    return datetime.fromisoformat(normalized).date()
                except ValueError:
                    return value
    return value


def _finalize_mask(mask: pd.Series, index: pd.Index) -> pd.Series:
    normalized = mask.reindex(index, fill_value=False).fillna(False)
    if str(normalized.dtype) != "bool":
        normalized = normalized.astype(bool)
    return normalized


def _score_group(df: pd.DataFrame, group: RankingGroup) -> tuple[pd.Series, list[pd.Series]]:
    weighted_total = pd.Series(0.0, index=df.index)
    total_weight = 0.0
    required_masks: list[pd.Series] = []
    for factor in group.factors:
        column_name = f"{factor.table}__{factor.column}"
        if column_name not in df.columns:
            raise ValueError(f"Missing ranking factor column '{column_name}'.")
        values = pd.to_numeric(df[column_name], errors="coerce")
        if factor.direction == "asc":
            values = values * -1
        values = _apply_transforms(values, df["date"], factor.transforms)
        if factor.missingValuePolicy == "zero":
            values = values.fillna(0.0)
        else:
            required_masks.append(values.notna())
        weighted_total = weighted_total.add(values.fillna(0.0) * factor.weight, fill_value=0.0)
        total_weight += factor.weight
    if total_weight <= 0:
        raise ValueError(f"Ranking group '{group.name}' produced zero factor weight.")
    group_score = weighted_total / total_weight
    group_score = _apply_transforms(group_score, df["date"], group.transforms)
    return group_score, required_masks


def _apply_transforms(series: pd.Series, dates: pd.Series, transforms: list[RankingTransform]) -> pd.Series:
    current = pd.to_numeric(series, errors="coerce")
    groups = dates.astype("string")
    for transform in transforms:
        transform_type = transform.type
        params = transform.params
        if transform_type == "coalesce":
            current = current.fillna(params.get("value"))
        elif transform_type == "clip":
            current = current.clip(lower=params.get("lower"), upper=params.get("upper"))
        elif transform_type == "winsorize":
            current = current.groupby(groups, group_keys=False).apply(
                lambda item: _winsorize(
                    item,
                    lower_quantile=_optional_float(params.get("lowerQuantile")),
                    upper_quantile=_optional_float(params.get("upperQuantile")),
                )
            )
        elif transform_type == "log1p":
            current = current.where(current > -1).map(lambda value: math.log1p(value) if pd.notna(value) else np.nan)
        elif transform_type == "negate":
            current = current * -1
        elif transform_type == "abs":
            current = current.abs()
        elif transform_type == "percentile_rank":
            current = current.groupby(groups, group_keys=False).rank(method="average", pct=True)
        elif transform_type == "zscore":
            current = current.groupby(groups, group_keys=False).apply(_zscore)
        elif transform_type == "minmax":
            current = current.groupby(groups, group_keys=False).apply(_minmax)
        else:
            raise ValueError(f"Unsupported transform '{transform_type}'.")
    return current


def _winsorize(series: pd.Series, *, lower_quantile: float | None, upper_quantile: float | None) -> pd.Series:
    lower = series.quantile(lower_quantile) if lower_quantile is not None else None
    upper = series.quantile(upper_quantile) if upper_quantile is not None else None
    return series.clip(lower=lower, upper=upper)


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(0.0, index=series.index)
    return (series - series.mean()) / std


def _minmax(series: pd.Series) -> pd.Series:
    min_value = series.min()
    max_value = series.max()
    if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
        return pd.Series(0.0, index=series.index)
    return (series - min_value) / (max_value - min_value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _persist_materialization(
    dsn: str,
    *,
    run_id: str,
    context: _MaterializationContext,
    resolved_range: _ResolvedDateRange,
    ranked: pd.DataFrame,
    triggered_by: str,
    date_count: int,
) -> int:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _insert_ranking_run(
                cur,
                run_id=run_id,
                strategy_name=context.strategy_name,
                ranking_schema_name=context.ranking_schema_name,
                ranking_schema_version=context.ranking_schema_version,
                output_table_name=context.output_table_name,
                start_date=resolved_range.start_date,
                end_date=resolved_range.end_date,
                status="running",
                triggered_by=triggered_by,
            )
            rows_written = _write_rankings_to_platinum(
                cur,
                table_name=context.output_table_name,
                ranked=ranked,
                start_date=resolved_range.start_date,
                end_date=resolved_range.end_date,
            )
            _update_ranking_run(
                cur,
                run_id=run_id,
                status="success",
                row_count=rows_written,
                date_count=date_count,
                error=None,
            )
            _upsert_ranking_watermark(
                cur,
                strategy_name=context.strategy_name,
                ranking_schema_name=context.ranking_schema_name,
                ranking_schema_version=context.ranking_schema_version,
                output_table_name=context.output_table_name,
                last_ranked_date=resolved_range.end_date,
            )
    return int(len(ranked))


def _persist_noop_run(
    dsn: str,
    *,
    run_id: str,
    context: _MaterializationContext,
    resolved_range: _ResolvedDateRange,
    triggered_by: str,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _insert_ranking_run(
                cur,
                run_id=run_id,
                strategy_name=context.strategy_name,
                ranking_schema_name=context.ranking_schema_name,
                ranking_schema_version=context.ranking_schema_version,
                output_table_name=context.output_table_name,
                start_date=resolved_range.start_date,
                end_date=resolved_range.end_date,
                status="running",
                triggered_by=triggered_by,
            )
            _update_ranking_run(
                cur,
                run_id=run_id,
                status="noop",
                row_count=0,
                date_count=0,
                error=None,
            )


def _write_rankings_to_platinum(
    cursor: Any,
    *,
    table_name: str,
    ranked: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> int:
    cursor.execute("CREATE SCHEMA IF NOT EXISTS platinum")
    _ensure_platinum_output_table(cursor, table_name)
    cursor.execute(
        f"""
        DELETE FROM "platinum".{universe_service._quote_identifier(table_name)}
        WHERE date >= %s AND date <= %s
        """,
        (start_date, end_date),
    )
    if ranked.empty:
        return 0
    last_updated_date = datetime.now(timezone.utc).date()
    rows = (
        (
            row.date,
            str(row.symbol),
            int(row.rank),
            float(row.score),
            last_updated_date,
        )
        for row in ranked.itertuples(index=False)
    )
    copy_rows(
        cursor,
        table=f'"platinum".{universe_service._quote_identifier(table_name)}',
        columns=("date", "symbol", "rank", "score", "last_updated_date"),
        rows=rows,
    )
    return int(len(ranked))


def _ensure_platinum_output_table(cursor: Any, table_name: str) -> None:
    identifier = universe_service._quote_identifier(table_name)
    symbol_date_index = build_scoped_identifier(table_name, "symbol", "date", "idx")
    date_rank_index = build_scoped_identifier(table_name, "date", "rank", "idx")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "platinum".{identifier} (
            date DATE NOT NULL,
            symbol TEXT NOT NULL,
            rank INTEGER NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            last_updated_date DATE NOT NULL DEFAULT CURRENT_DATE,
            PRIMARY KEY (date, symbol)
        )
        """
    )
    cursor.execute(
        f"""
        ALTER TABLE "platinum".{identifier}
        ADD COLUMN IF NOT EXISTS score DOUBLE PRECISION
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {symbol_date_index}
        ON "platinum".{identifier}(symbol, date DESC)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {date_rank_index}
        ON "platinum".{identifier}(date DESC, rank)
        """
    )


def _insert_ranking_run(
    cursor: Any,
    *,
    run_id: str,
    strategy_name: str,
    ranking_schema_name: str,
    ranking_schema_version: int,
    output_table_name: str,
    start_date: date,
    end_date: date,
    status: str,
    triggered_by: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO core.ranking_runs (
            run_id,
            strategy_name,
            ranking_schema_name,
            ranking_schema_version,
            output_table_name,
            start_date,
            end_date,
            status,
            triggered_by,
            started_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            run_id,
            strategy_name,
            ranking_schema_name,
            ranking_schema_version,
            output_table_name,
            start_date,
            end_date,
            status,
            triggered_by,
        ),
    )


def _update_ranking_run(
    cursor: Any,
    *,
    run_id: str,
    status: str,
    row_count: int,
    date_count: int,
    error: str | None,
) -> None:
    cursor.execute(
        """
        UPDATE core.ranking_runs
        SET status = %s,
            row_count = %s,
            date_count = %s,
            error = %s,
            finished_at = NOW()
        WHERE run_id = %s
        """,
        (status, row_count, date_count, error, run_id),
    )


def _update_ranking_run_after_failure(
    dsn: str,
    *,
    run_id: str,
    error: str,
) -> None:
    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                _update_ranking_run(
                    cur,
                    run_id=run_id,
                    status="error",
                    row_count=0,
                    date_count=0,
                    error=error,
                )
    except Exception:
        logger.exception("Failed to record ranking run failure for run '%s'.", run_id)


def _upsert_ranking_watermark(
    cursor: Any,
    *,
    strategy_name: str,
    ranking_schema_name: str,
    ranking_schema_version: int,
    output_table_name: str,
    last_ranked_date: date,
) -> None:
    cursor.execute(
        """
        INSERT INTO core.ranking_watermarks (
            strategy_name,
            ranking_schema_name,
            ranking_schema_version,
            output_table_name,
            last_ranked_date,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (strategy_name)
        DO UPDATE SET
            ranking_schema_name = EXCLUDED.ranking_schema_name,
            ranking_schema_version = EXCLUDED.ranking_schema_version,
            output_table_name = EXCLUDED.output_table_name,
            last_ranked_date = EXCLUDED.last_ranked_date,
            updated_at = NOW()
        """,
        (
            strategy_name,
            ranking_schema_name,
            ranking_schema_version,
            output_table_name,
            last_ranked_date,
        ),
    )
