from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from collections.abc import Mapping
from typing import Any

from asset_allocation_runtime_common.foundation.postgres import connect
from asset_allocation_runtime_common.strategy_engine.contracts import (
    UniverseCondition,
    UniverseConditionOperator,
    UniverseDefinition,
    UniverseGroup,
)

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_AS_OF_COLUMN_CANDIDATES = ("as_of_ts", "timestamp", "ts", "datetime", "date", "obs_date")
_NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "real",
    "double precision",
    "decimal",
}
_BOOLEAN_TYPES = {"boolean"}
_DATE_TYPES = {"date"}
_DATETIME_TYPES = {"timestamp without time zone", "timestamp with time zone"}
_STRING_TYPES = {"text", "character varying", "character", "uuid"}
_NUMBER_OPERATORS: tuple[UniverseConditionOperator, ...] = (
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
)
_STRING_OPERATORS: tuple[UniverseConditionOperator, ...] = (
    "eq",
    "ne",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
)
_BOOLEAN_OPERATORS: tuple[UniverseConditionOperator, ...] = (
    "eq",
    "ne",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
)
def _is_catalog_table_name(table_name: str) -> bool:
    normalized = str(table_name or "").strip().lower()
    return bool(normalized) and not normalized.endswith(("_backup", "_by_date"))


@dataclass(frozen=True)
class UniverseColumnSpec:
    name: str
    data_type: str
    value_kind: str
    operators: tuple[UniverseConditionOperator, ...]


@dataclass(frozen=True)
class UniverseTableSpec:
    name: str
    as_of_column: str
    columns: dict[str, UniverseColumnSpec]
    as_of_kind: str = "slower"


@dataclass(frozen=True)
class UniverseFieldDefinition:
    field: str
    value_kind: str
    operators: tuple[UniverseConditionOperator, ...]


@dataclass(frozen=True)
class _UniverseFieldBinding:
    field: str
    table: str
    column: str
    value_kind: str
    operators: tuple[UniverseConditionOperator, ...]


_FIELD_BINDING_SPECS: tuple[tuple[str, str, str, str, tuple[UniverseConditionOperator, ...]], ...] = (
    ("market.close", "market_data", "close", "number", _NUMBER_OPERATORS),
    ("security.is_active", "market_data", "active", "boolean", _BOOLEAN_OPERATORS),
    ("security.sector", "market_data", "sector", "string", _STRING_OPERATORS),
    ("security.delisted_at", "market_data", "delisted_at", "date", _NUMBER_OPERATORS),
    ("market.trade_date", "market_data", "trade_date", "date", _NUMBER_OPERATORS),
    ("market.timestamp", "market_data", "timestamp", "datetime", _NUMBER_OPERATORS),
    ("returns.return_20d", "market_data", "return_20d", "number", _NUMBER_OPERATORS),
    ("returns.return_126d", "market_data", "return_126d", "number", _NUMBER_OPERATORS),
    ("quality.piotroski_f_score", "finance_data", "piotroski_f_score", "number", _NUMBER_OPERATORS),
    ("earnings.surprise_pct", "earnings_data", "surprise_pct", "number", _NUMBER_OPERATORS),
)

_FIELD_BINDINGS: tuple[_UniverseFieldBinding, ...] = tuple(
    _UniverseFieldBinding(
        field=field,
        table=table,
        column=column,
        value_kind=value_kind,
        operators=operators,
    )
    for field, table, column, value_kind, operators in _FIELD_BINDING_SPECS
)
_FIELD_BINDINGS_BY_FIELD = {binding.field: binding for binding in _FIELD_BINDINGS}
_FIELD_BINDINGS_BY_SOURCE = {(binding.table, binding.column): binding for binding in _FIELD_BINDINGS}


def list_gold_universe_catalog(dsn: str) -> dict[str, Any]:
    table_specs = _load_gold_table_specs(dsn)
    fields = [
        {
            "field": definition.field,
            "valueKind": definition.value_kind,
            "operators": list(definition.operators),
        }
        for binding in _FIELD_BINDINGS
        if binding.table in table_specs and binding.column in table_specs[binding.table].columns
        for definition in (_field_definition_payload(binding),)
    ]
    logger.info("Universe catalog loaded: gold_fields=%d", len(fields))
    return {"source": "postgres_gold", "fields": fields}


def preview_gold_universe(
    dsn: str,
    universe: UniverseDefinition,
    *,
    sample_limit: int = 25,
) -> dict[str, Any]:
    if sample_limit < 1:
        raise ValueError("sample_limit must be >= 1.")

    table_specs = _load_gold_table_specs(dsn)
    with connect(dsn) as conn:
        symbols, fields_used = _evaluate_node(conn, _node_attr(universe, "root"), table_specs)

    ordered_symbols = sorted(symbols)
    warnings: list[str] = []
    if not ordered_symbols:
        warnings.append("Universe preview matched zero symbols.")

    logger.info(
        "Universe preview resolved: fields=%s symbol_count=%d sample_limit=%d",
        ",".join(sorted(fields_used)),
        len(ordered_symbols),
        sample_limit,
    )
    return {
        "source": "postgres_gold",
        "symbolCount": len(ordered_symbols),
        "sampleSymbols": ordered_symbols[:sample_limit],
        "fieldsUsed": sorted(fields_used),
        "warnings": warnings,
    }


def _load_gold_table_specs(dsn: str) -> dict[str, UniverseTableSpec]:
    query = """
        SELECT table_name, column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'gold'
        ORDER BY table_name, ordinal_position
    """
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    rows = [row for row in rows if _is_catalog_table_name(str(row[0] or ""))]
    return _build_table_specs(rows)


def _build_table_specs(rows: list[tuple[Any, ...]]) -> dict[str, UniverseTableSpec]:
    by_table: dict[str, list[tuple[str, str, str]]] = {}
    for table_name_raw, column_name_raw, data_type_raw, udt_name_raw in rows:
        table_name = _normalize_identifier(str(table_name_raw or ""), "table")
        column_name = _normalize_identifier(str(column_name_raw or ""), "column")
        data_type = str(data_type_raw or "").strip().lower()
        udt_name = str(udt_name_raw or "").strip().lower()
        by_table.setdefault(table_name, []).append((column_name, data_type, udt_name))

    table_specs: dict[str, UniverseTableSpec] = {}
    for table_name, column_rows in sorted(by_table.items()):
        column_specs: dict[str, UniverseColumnSpec] = {}
        has_symbol = False
        as_of_column: str | None = None
        for column_name, data_type, udt_name in column_rows:
            if column_name == "symbol":
                has_symbol = True
            if as_of_column is None and column_name in _AS_OF_COLUMN_CANDIDATES:
                as_of_column = column_name

            value_kind = _classify_value_kind(data_type, udt_name)
            if value_kind is None:
                continue
            column_specs[column_name] = UniverseColumnSpec(
                name=column_name,
                data_type=data_type,
                value_kind=value_kind,
                operators=_operators_for_value_kind(value_kind),
            )

        if not has_symbol or not as_of_column:
            continue
        as_of_kind = "intraday" if _is_intraday_data_type(as_of_column, column_rows) else "slower"
        table_specs[table_name] = UniverseTableSpec(
            name=table_name,
            as_of_column=as_of_column,
            as_of_kind=as_of_kind,
            columns=column_specs,
        )
    return table_specs


def _is_intraday_data_type(as_of_column: str, column_rows: list[tuple[str, str, str]]) -> bool:
    for column_name, data_type, _udt_name in column_rows:
        if column_name != as_of_column:
            continue
        return data_type in _DATETIME_TYPES
    return False


def is_intraday_table_spec(spec: UniverseTableSpec) -> bool:
    return spec.as_of_kind == "intraday"


def _field_definition_payload(binding: _UniverseFieldBinding) -> UniverseFieldDefinition:
    return UniverseFieldDefinition(
        field=binding.field,
        value_kind=binding.value_kind,
        operators=binding.operators,
    )


def _resolve_condition_binding(
    condition: UniverseCondition | Mapping[str, Any] | Any,
    table_specs: dict[str, UniverseTableSpec],
) -> _UniverseFieldBinding:
    field = str(_node_attr(condition, "field") or "").strip()
    if field:
        binding = _FIELD_BINDINGS_BY_FIELD.get(field)
        if binding is None:
            raise ValueError(f"Unknown universe field '{field}'.")
        table_spec = table_specs.get(binding.table)
        if table_spec is None:
            raise ValueError(f"Unknown gold table '{binding.table}' for field '{field}'.")
        if binding.column not in table_spec.columns:
            raise ValueError(
                f"Unknown column '{binding.column}' for gold.{binding.table} referenced by field '{field}'."
            )
        return binding

    table_name = str(_node_attr(condition, "table") or "").strip().lower()
    column_name = str(_node_attr(condition, "column") or "").strip().lower()
    if not table_name or not column_name:
        raise ValueError("Universe condition must define either 'field' or 'table'/'column'.")
    normalized_table_name = _normalize_identifier(table_name, "table")
    normalized_column_name = _normalize_identifier(column_name, "column")
    table_spec = table_specs.get(normalized_table_name)
    if table_spec is None:
        raise ValueError(f"Unknown gold table '{table_name}'.")
    if normalized_column_name not in table_spec.columns:
        raise ValueError(f"Unknown column '{column_name}' for gold.{table_spec.name}.")
    return _FIELD_BINDINGS_BY_SOURCE.get(
        (table_spec.name, normalized_column_name),
        _UniverseFieldBinding(
            field=f"{table_spec.name}.{normalized_column_name}",
            table=table_spec.name,
            column=normalized_column_name,
            value_kind=table_spec.columns[normalized_column_name].value_kind,
            operators=table_spec.columns[normalized_column_name].operators,
        ),
    )


def _collect_required_source_columns(
    node: UniverseGroup | UniverseCondition | Mapping[str, Any] | Any,
    required: dict[str, set[str]],
    *,
    table_specs: dict[str, UniverseTableSpec],
) -> None:
    if _node_kind(node) == "condition":
        binding = _resolve_condition_binding(node, table_specs)
        required.setdefault(binding.table, set()).add(binding.column)
        return
    for clause in _node_clauses(node):
        _collect_required_source_columns(clause, required, table_specs=table_specs)


def _node_attr(node: UniverseGroup | UniverseCondition | Mapping[str, Any] | Any, name: str, default: Any = None) -> Any:
    if isinstance(node, Mapping):
        return node.get(name, default)
    return getattr(node, name, default)


def _node_kind(node: UniverseGroup | UniverseCondition | Mapping[str, Any] | Any) -> str:
    return str(_node_attr(node, "kind") or "").strip().lower()


def _node_operator(node: UniverseGroup | UniverseCondition | Mapping[str, Any] | Any) -> str:
    return str(_node_attr(node, "operator") or "").strip().lower()


def _node_clauses(node: UniverseGroup | UniverseCondition | Mapping[str, Any] | Any) -> list[Any]:
    clauses = _node_attr(node, "clauses", [])
    if clauses is None:
        return []
    return list(clauses)


def _evaluate_node(
    conn: Any,
    node: UniverseGroup | UniverseCondition | Mapping[str, Any] | Any,
    table_specs: dict[str, UniverseTableSpec],
) -> tuple[set[str], set[str]]:
    node_kind = _node_kind(node)
    if node_kind == "condition":
        binding = _resolve_condition_binding(node, table_specs)
        table_spec = table_specs[binding.table]
        symbols = _fetch_condition_symbols(conn, table_spec, node, binding)
        return symbols, {binding.field}

    child_symbols: list[set[str]] = []
    fields_used: set[str] = set()
    for clause in _node_clauses(node):
        clause_symbols, clause_tables = _evaluate_node(conn, clause, table_specs)
        child_symbols.append(clause_symbols)
        fields_used.update(clause_tables)

    if _node_operator(node) == "and":
        resolved = set(child_symbols[0])
        for item in child_symbols[1:]:
            resolved.intersection_update(item)
        return resolved, fields_used

    resolved = set()
    for item in child_symbols:
        resolved.update(item)
    return resolved, fields_used


def _fetch_condition_symbols(
    conn: Any,
    table_spec: UniverseTableSpec,
    condition: UniverseCondition | Mapping[str, Any] | Any,
    binding: _UniverseFieldBinding,
) -> set[str]:
    column_spec = table_spec.columns.get(binding.column)
    if column_spec is None:
        raise ValueError(f"Unknown column '{binding.column}' for gold.{table_spec.name}.")
    if _node_attr(condition, "operator") not in column_spec.operators:
        raise ValueError(
            f"Operator '{_node_attr(condition, 'operator')}' is not supported for gold.{table_spec.name}.{column_spec.name}."
        )

    predicate_sql, params = _build_predicate(condition, column_spec)
    symbol_identifier = _quote_identifier("symbol")
    column_identifier = _quote_identifier(column_spec.name)
    as_of_identifier = _quote_identifier(table_spec.as_of_column)
    query = f"""
        WITH latest AS (
            SELECT DISTINCT ON ({symbol_identifier})
              {symbol_identifier} AS symbol,
              {column_identifier} AS candidate_value
            FROM "gold".{_quote_identifier(table_spec.name)}
            WHERE {symbol_identifier} IS NOT NULL
            ORDER BY {symbol_identifier}, {as_of_identifier} DESC NULLS LAST
        )
        SELECT symbol
        FROM latest
        WHERE {predicate_sql}
        ORDER BY symbol
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        return {str(row[0]).strip().upper() for row in cur.fetchall() if str(row[0]).strip()}


def _build_predicate(
    condition: UniverseCondition | Mapping[str, Any] | Any,
    column_spec: UniverseColumnSpec,
) -> tuple[str, list[Any]]:
    operator = _node_attr(condition, "operator")
    if operator == "is_null":
        return "candidate_value IS NULL", []
    if operator == "is_not_null":
        return "candidate_value IS NOT NULL", []

    if operator in {"in", "not_in"}:
        values = _node_attr(condition, "values")
        assert values is not None
        coerced = _coerce_values(list(values), column_spec)
        placeholders = ", ".join(["%s"] * len(coerced))
        comparator = "IN" if operator == "in" else "NOT IN"
        return f"candidate_value {comparator} ({placeholders})", coerced

    value = _node_attr(condition, "value")
    assert value is not None
    coerced_value = _coerce_value(value, column_spec)
    comparator = {
        "eq": "=",
        "ne": "<>",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }.get(operator)
    if comparator is None:
        raise ValueError(f"Unsupported operator '{operator}'.")
    return f"candidate_value {comparator} %s", [coerced_value]


def _coerce_values(values: list[Any], column_spec: UniverseColumnSpec) -> list[Any]:
    if not values:
        raise ValueError("values must not be empty.")
    return [_coerce_value(value, column_spec) for value in values]


def _coerce_value(value: Any, column_spec: UniverseColumnSpec) -> Any:
    if column_spec.value_kind == "number":
        if isinstance(value, bool):
            raise ValueError(f"{column_spec.name} expects a numeric value.")
        if isinstance(value, (int, float)):
            return value
        try:
            text = str(value or "").strip()
            if not text:
                raise ValueError
            return float(text)
        except ValueError as exc:
            raise ValueError(f"{column_spec.name} expects a numeric value.") from exc

    if column_spec.value_kind == "boolean":
        if isinstance(value, bool):
            return value
        normalized = str(value or "").strip().lower()
        if normalized in {"true", "1", "yes", "y", "on", "t"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", "f"}:
            return False
        raise ValueError(f"{column_spec.name} expects a boolean value.")

    if column_spec.value_kind == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{column_spec.name} expects a date value.")
        normalized = text.replace("Z", "+00:00")
        try:
            return date.fromisoformat(normalized)
        except ValueError:
            try:
                return datetime.fromisoformat(normalized).date()
            except ValueError as exc:
                raise ValueError(f"{column_spec.name} expects an ISO date value.") from exc

    if column_spec.value_kind == "datetime":
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{column_spec.name} expects a datetime value.")
        normalized = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"{column_spec.name} expects an ISO datetime value.") from exc

    return str(value or "")


def _classify_value_kind(data_type: str, udt_name: str) -> str | None:
    normalized_data_type = str(data_type or "").strip().lower()
    normalized_udt_name = str(udt_name or "").strip().lower()

    if normalized_data_type in _NUMERIC_TYPES:
        return "number"
    if normalized_data_type in _BOOLEAN_TYPES:
        return "boolean"
    if normalized_data_type in _DATE_TYPES:
        return "date"
    if normalized_data_type in _DATETIME_TYPES:
        return "datetime"
    if normalized_data_type in _STRING_TYPES or normalized_udt_name in {"varchar", "text", "bpchar", "uuid"}:
        return "string"
    return None


def _operators_for_value_kind(value_kind: str) -> tuple[UniverseConditionOperator, ...]:
    if value_kind == "number" or value_kind == "date" or value_kind == "datetime":
        return _NUMBER_OPERATORS
    if value_kind == "boolean":
        return _BOOLEAN_OPERATORS
    return _STRING_OPERATORS


def _normalize_identifier(value: str, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or not _IDENTIFIER_PATTERN.match(normalized):
        raise ValueError(f"Invalid {label} identifier '{value}'.")
    return normalized


def _quote_identifier(identifier: str) -> str:
    normalized = _normalize_identifier(identifier, "identifier")
    return '"' + normalized.replace('"', '""') + '"'
