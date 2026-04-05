from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core.gold_column_lookup_catalog import (
    TABLE_SOURCE_JOBS,
    SUPPORTED_GOLD_LOOKUP_TABLES,
)
from core.postgres import PostgresError, connect, get_dsn

CALCULATION_TYPES = {"source", "derived_sql", "derived_python", "external", "manual"}
STATUSES = {"draft", "reviewed", "approved"}
PLACEHOLDER_DESCRIPTION_PREFIX = "TODO: Describe"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_seed_path() -> Path:
    return _repo_root() / "core" / "metadata" / "gold_column_lookup_seed.json"


def _resolve_dsn(cli_dsn: Optional[str]) -> Optional[str]:
    if cli_dsn and str(cli_dsn).strip():
        return str(cli_dsn).strip()
    return get_dsn("POSTGRES_DSN")


def _placeholder_description(table_name: str, column_name: str) -> str:
    return f"{PLACEHOLDER_DESCRIPTION_PREFIX} gold.{table_name}.{column_name}."


def _is_placeholder_description(description: str) -> bool:
    return str(description or "").strip().lower().startswith(PLACEHOLDER_DESCRIPTION_PREFIX.lower())


def _normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        if not value.strip():
            return []
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            pass
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(value).strip()]


def _normalize_calculation_type(value: Any) -> Optional[str]:
    text = _normalize_text(value)
    if not text:
        return None
    lowered = text.lower()
    if lowered not in CALCULATION_TYPES:
        return None
    return lowered


def _normalize_status(value: Any) -> Optional[str]:
    text = _normalize_text(value)
    if not text:
        return None
    lowered = text.lower()
    if lowered not in STATUSES:
        return None
    return lowered


def _table_level_defaults(table_name: str) -> Dict[str, Any]:
    return {
        "schema_name": "gold",
        "source_job": TABLE_SOURCE_JOBS.get(table_name),
        "status": "draft",
    }


def _load_seed_entries(seed_path: Path) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if not seed_path.exists():
        raise PostgresError(f"Seed file not found: {seed_path}")

    payload = json.loads(seed_path.read_text(encoding="utf-8"))
    table_defaults = payload.get("table_defaults") or {}
    entries = payload.get("entries") or []
    if not isinstance(entries, list):
        raise PostgresError("Seed file format is invalid: 'entries' must be an array.")

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for raw in entries:
        if not isinstance(raw, Mapping):
            continue
        table_name = _normalize_text(raw.get("table_name"))
        column_name = _normalize_text(raw.get("column_name"))
        if not table_name or not column_name:
            continue

        table_seed = table_defaults.get(table_name) if isinstance(table_defaults, Mapping) else None
        merged: Dict[str, Any] = {}
        if isinstance(table_seed, Mapping):
            merged.update(dict(table_seed))
        merged.update(dict(raw))

        merged["table_name"] = table_name
        merged["column_name"] = column_name
        merged["schema_name"] = _normalize_text(merged.get("schema_name")) or "gold"
        merged["description"] = _normalize_text(merged.get("description"))
        merged["calculation_type"] = _normalize_calculation_type(merged.get("calculation_type"))
        merged["calculation_notes"] = _normalize_text(merged.get("calculation_notes"))
        merged["calculation_expression"] = _normalize_text(merged.get("calculation_expression"))
        merged["calculation_dependencies"] = _normalize_list(merged.get("calculation_dependencies"))
        merged["source_job"] = _normalize_text(merged.get("source_job"))
        merged["status"] = _normalize_status(merged.get("status"))

        out[(table_name, column_name)] = merged
    return out


def _infer_calculation_type(table_name: str, column_name: str) -> str:
    source_columns: Dict[str, set[str]] = {
        "market_data": {"date", "symbol", "open", "high", "low", "close", "volume"},
        "finance_data": {"date", "symbol", "market_cap", "pe_ratio"},
        "earnings_data": {"date", "symbol", "reported_eps", "eps_estimate", "surprise"},
        "price_target_data": {
            "obs_date",
            "symbol",
            "tp_mean_est",
            "tp_std_dev_est",
            "tp_high_est",
            "tp_low_est",
            "tp_cnt_est",
            "tp_cnt_est_rev_up",
            "tp_cnt_est_rev_down",
        },
    }
    return "source" if column_name in source_columns.get(table_name, set()) else "derived_python"


def _infer_calculation_notes(calculation_type: str) -> str:
    if calculation_type == "source":
        return "Replicated from the canonical gold dataset serving column."
    return "Computed by gold ETL/domain processing logic. See DATA.md for formulas and lineage."


def _read_live_gold_columns(cur: Any, tables: Sequence[str]) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            table_name,
            column_name,
            data_type,
            (is_nullable = 'YES') AS is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'gold'
          AND table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
        """,
        (list(tables),),
    )
    rows = []
    for table_name, column_name, data_type, is_nullable in cur.fetchall():
        rows.append(
            {
                "schema_name": "gold",
                "table_name": str(table_name),
                "column_name": str(column_name),
                "data_type": str(data_type),
                "is_nullable": bool(is_nullable),
            }
        )
    return rows


def _read_existing_lookup_rows(cur: Any, tables: Sequence[str]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            schema_name,
            table_name,
            column_name,
            data_type,
            description,
            is_nullable,
            calculation_type,
            calculation_notes,
            calculation_expression,
            calculation_dependencies,
            source_job,
            status
        FROM gold.column_lookup
        WHERE schema_name = 'gold'
          AND table_name = ANY(%s)
        """,
        (list(tables),),
    )
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for (
        schema_name,
        table_name,
        column_name,
        data_type,
        description,
        is_nullable,
        calculation_type,
        calculation_notes,
        calculation_expression,
        calculation_dependencies,
        source_job,
        status,
    ) in cur.fetchall():
        key = (str(table_name), str(column_name))
        out[key] = {
            "schema_name": _normalize_text(schema_name) or "gold",
            "table_name": str(table_name),
            "column_name": str(column_name),
            "data_type": str(data_type),
            "description": _normalize_text(description),
            "is_nullable": bool(is_nullable),
            "calculation_type": _normalize_calculation_type(calculation_type) or "source",
            "calculation_notes": _normalize_text(calculation_notes),
            "calculation_expression": _normalize_text(calculation_expression),
            "calculation_dependencies": _normalize_list(calculation_dependencies),
            "source_job": _normalize_text(source_job),
            "status": _normalize_status(status) or "draft",
        }
    return out


def _choose_curated_value(
    *,
    existing: Optional[Any],
    seed: Optional[Any],
    default: Any,
    force_metadata: bool,
) -> Any:
    if not force_metadata and existing not in (None, "", []):
        return existing
    if seed not in (None, "", []):
        return seed
    return default


def _build_lookup_row(
    *,
    live_row: Mapping[str, Any],
    existing: Optional[Mapping[str, Any]],
    seed: Optional[Mapping[str, Any]],
    updated_by: str,
    force_metadata: bool,
) -> Dict[str, Any]:
    table_name = str(live_row["table_name"])
    column_name = str(live_row["column_name"])
    table_defaults = _table_level_defaults(table_name)
    existing_row = dict(existing or {})
    seed_row = dict(seed or {})

    default_description = _placeholder_description(table_name, column_name)
    calculation_type_default = _infer_calculation_type(table_name, column_name)
    calculation_notes_default = _infer_calculation_notes(calculation_type_default)
    source_job_default = table_defaults.get("source_job")
    status_default = "draft"

    description = _choose_curated_value(
        existing=existing_row.get("description"),
        seed=seed_row.get("description"),
        default=default_description,
        force_metadata=force_metadata,
    )
    calc_type = _choose_curated_value(
        existing=existing_row.get("calculation_type"),
        seed=seed_row.get("calculation_type"),
        default=calculation_type_default,
        force_metadata=force_metadata,
    )
    calc_notes = _choose_curated_value(
        existing=existing_row.get("calculation_notes"),
        seed=seed_row.get("calculation_notes"),
        default=calculation_notes_default,
        force_metadata=force_metadata,
    )
    calc_expression = _choose_curated_value(
        existing=existing_row.get("calculation_expression"),
        seed=seed_row.get("calculation_expression"),
        default=None,
        force_metadata=force_metadata,
    )
    calc_dependencies = _choose_curated_value(
        existing=existing_row.get("calculation_dependencies"),
        seed=seed_row.get("calculation_dependencies"),
        default=[],
        force_metadata=force_metadata,
    )
    source_job = _choose_curated_value(
        existing=existing_row.get("source_job"),
        seed=seed_row.get("source_job"),
        default=source_job_default,
        force_metadata=force_metadata,
    )
    status = _choose_curated_value(
        existing=existing_row.get("status"),
        seed=seed_row.get("status"),
        default=status_default,
        force_metadata=force_metadata,
    )

    normalized_calc_type = _normalize_calculation_type(calc_type) or calculation_type_default
    normalized_status = _normalize_status(status) or status_default
    normalized_description = _normalize_text(description) or default_description
    normalized_dependencies = _normalize_list(calc_dependencies)

    if normalized_status == "approved" and _is_placeholder_description(normalized_description):
        raise PostgresError(
            f"Approved metadata cannot use placeholder description: gold.{table_name}.{column_name}"
        )

    if _is_placeholder_description(normalized_description):
        normalized_status = "draft"

    return {
        "schema_name": "gold",
        "table_name": table_name,
        "column_name": column_name,
        "data_type": str(live_row["data_type"]),
        "description": normalized_description,
        "is_nullable": bool(live_row["is_nullable"]),
        "calculation_type": normalized_calc_type,
        "calculation_notes": _normalize_text(calc_notes),
        "calculation_expression": _normalize_text(calc_expression),
        "calculation_dependencies": normalized_dependencies,
        "source_job": _normalize_text(source_job),
        "status": normalized_status,
        "updated_by": updated_by,
    }


def sync_gold_column_lookup(
    *,
    dsn: str,
    seed_path: Path,
    force_metadata: bool,
    dry_run: bool,
    updated_by: str,
) -> Dict[str, int]:
    seed_entries = _load_seed_entries(seed_path)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            live_rows = _read_live_gold_columns(cur, SUPPORTED_GOLD_LOOKUP_TABLES)
            existing_rows = _read_existing_lookup_rows(cur, SUPPORTED_GOLD_LOOKUP_TABLES)

            upserts: List[Dict[str, Any]] = []
            missing_seed = 0
            placeholders = 0
            for live_row in live_rows:
                key = (str(live_row["table_name"]), str(live_row["column_name"]))
                seed_row = seed_entries.get(key)
                if seed_row is None:
                    missing_seed += 1
                row = _build_lookup_row(
                    live_row=live_row,
                    existing=existing_rows.get(key),
                    seed=seed_row,
                    updated_by=updated_by,
                    force_metadata=force_metadata,
                )
                if _is_placeholder_description(row["description"]):
                    placeholders += 1
                upserts.append(row)

            if not dry_run:
                for row in upserts:
                    cur.execute(
                        """
                        INSERT INTO gold.column_lookup (
                            schema_name,
                            table_name,
                            column_name,
                            data_type,
                            description,
                            is_nullable,
                            calculation_type,
                            calculation_notes,
                            calculation_expression,
                            calculation_dependencies,
                            source_job,
                            status,
                            updated_by
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (schema_name, table_name, column_name) DO UPDATE
                        SET data_type = EXCLUDED.data_type,
                            description = EXCLUDED.description,
                            is_nullable = EXCLUDED.is_nullable,
                            calculation_type = EXCLUDED.calculation_type,
                            calculation_notes = EXCLUDED.calculation_notes,
                            calculation_expression = EXCLUDED.calculation_expression,
                            calculation_dependencies = EXCLUDED.calculation_dependencies,
                            source_job = EXCLUDED.source_job,
                            status = EXCLUDED.status,
                            updated_at = NOW(),
                            updated_by = EXCLUDED.updated_by
                        """,
                        (
                            row["schema_name"],
                            row["table_name"],
                            row["column_name"],
                            row["data_type"],
                            row["description"],
                            row["is_nullable"],
                            row["calculation_type"],
                            row["calculation_notes"],
                            row["calculation_expression"],
                            row["calculation_dependencies"],
                            row["source_job"],
                            row["status"],
                            row["updated_by"],
                        ),
                    )
        if not dry_run:
            conn.commit()

    return {
        "live_rows": len(live_rows),
        "upserts": len(upserts),
        "missing_seed": missing_seed,
        "placeholder_descriptions": placeholders,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Synchronize gold.column_lookup from Postgres information_schema "
            "and repo-managed seed metadata."
        )
    )
    parser.add_argument("--dsn", help="Postgres DSN. Defaults to POSTGRES_DSN.")
    parser.add_argument(
        "--seed-path",
        default=str(_default_seed_path()),
        help="Path to repo-managed gold column lookup seed JSON.",
    )
    parser.add_argument(
        "--force-metadata",
        action="store_true",
        help="Override existing curated metadata using seed/default values.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print computed sync stats without writing to Postgres.",
    )
    parser.add_argument(
        "--updated-by",
        default="sync_gold_column_lookup.py",
        help="Value stored in gold.column_lookup.updated_by for writes.",
    )
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    dsn = _resolve_dsn(args.dsn)
    if not dsn:
        raise PostgresError("POSTGRES_DSN is not configured. Pass --dsn or set POSTGRES_DSN.")

    seed_path = Path(args.seed_path).resolve()
    stats = sync_gold_column_lookup(
        dsn=dsn,
        seed_path=seed_path,
        force_metadata=bool(args.force_metadata),
        dry_run=bool(args.dry_run),
        updated_by=str(args.updated_by),
    )
    print(json.dumps(stats, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
