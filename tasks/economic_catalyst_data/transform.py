from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from io import BytesIO
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.economic_catalyst_data import constants


_UTC = ZoneInfo("UTC")
_ET = ZoneInfo("America/New_York")
_CET = ZoneInfo("Europe/Berlin")
_TOKYO = ZoneInfo("Asia/Tokyo")
_LONDON = ZoneInfo("Europe/London")

_MONTH_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
_DOW_PATTERN = r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\xa0", " ").split()).strip()


def _normalize_key(value: Any) -> str:
    text = _normalize_text(value).lower()
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else [], sort_keys=True, ensure_ascii=False, default=str)


def _listify(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    elif isinstance(value, str):
        if not value.strip():
            return []
        try:
            parsed = json.loads(value)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            items = parsed
        else:
            items = [part.strip() for part in value.split(",") if part.strip()]
    else:
        items = [value]
    return [item for item in dict.fromkeys(_normalize_text(item) for item in items) if item]


def _parse_datetime(value: Any, *, zone: ZoneInfo | None = None) -> datetime | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            base = value.replace(tzinfo=zone or _UTC)
        else:
            base = value.astimezone(_UTC)
        return base.astimezone(_UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=zone or _UTC).astimezone(_UTC)
    text = _normalize_text(value)
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    for parser in (pd.to_datetime,):
        try:
            parsed = parser(text, utc=False)
        except Exception:
            continue
        if isinstance(parsed, pd.Timestamp):
            if pd.isna(parsed):
                continue
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize(zone or _UTC)
            return parsed.tz_convert(_UTC).to_pydatetime()
    return None


def _parse_date_only(value: Any, *, year: int | None = None) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    candidates = [text]
    if year is not None and re.search(r"\b\d{4}\b", text) is None:
        candidates.append(f"{text} {year}")
    for candidate in candidates:
        try:
            parsed = pd.to_datetime(candidate, errors="raise")
        except Exception:
            continue
        if isinstance(parsed, pd.Timestamp) and not pd.isna(parsed):
            return datetime(parsed.year, parsed.month, parsed.day, tzinfo=_UTC)
    return None


def _with_local_time(day: datetime | None, hour: int, minute: int, zone: ZoneInfo) -> datetime | None:
    if day is None:
        return None
    local = day.astimezone(zone)
    return datetime(local.year, local.month, local.day, hour, minute, tzinfo=zone).astimezone(_UTC)


def _coerce_float(value: Any) -> float | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)
    text = _normalize_text(value)
    if not text or text.lower() in {"n/a", "na", "none", "nan", "-"}:
        return None
    text = text.replace("%", "").replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _hash_record(*parts: Any) -> str:
    payload = "||".join(_normalize_text(part) for part in parts)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _safe_bool(value: Any, *, default: bool = False) -> bool:
    if value is None or value is pd.NA:
        return default
    if isinstance(value, bool):
        return value
    text = _normalize_text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "scheduled", "released"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return default


def _payload_preview(value: Any, *, max_chars: int = 320) -> str:
    try:
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = repr(value)
    compact = _normalize_text(text)
    if len(compact) <= max_chars:
        return compact
    return compact[:max_chars] + "..."


def _blank_frame(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _ensure_frame(records: Sequence[dict[str, Any]], columns: Sequence[str]) -> pd.DataFrame:
    if not records:
        return _blank_frame(columns)
    frame = pd.DataFrame(records)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, list(columns)].copy()


def _keyword_group(text: str) -> tuple[str, str | None]:
    lower = _normalize_text(text).lower()
    rules: list[tuple[str, tuple[str, ...], str | None]] = [
        ("Labor", ("nonfarm payroll", "employment situation", "jobless", "unemployment", "labor"), None),
        ("Inflation", ("consumer price", "cpi", "ppi", "pce", "inflation"), None),
        ("GrowthActivity", ("gdp", "gross domestic product", "retail sales", "industrial production", "pmi", "ism"), None),
        ("Housing", ("housing", "building permits", "home sales", "case-shiller"), None),
        ("ConsumerSentiment", ("sentiment", "confidence"), None),
        ("TradeExternal", ("trade", "imports", "exports", "current account"), None),
        ("RatesFiscal", ("treasury auction", "auction", "yield", "federal funds"), None),
        ("CentralBankPolicy", ("fomc", "monetary policy", "bank rate", "governing council", "statement on monetary policy", "minutes"), None),
        ("CreditRegulatory", ("credit", "loan", "regulatory", "supervision"), None),
    ]
    for event_group, needles, subgroup in rules:
        if any(needle in lower for needle in needles):
            return event_group, subgroup
    return "GrowthActivity", None


def _importance_tier(text: str, event_group: str) -> str:
    lower = _normalize_text(text).lower()
    if event_group == "CentralBankPolicy":
        if "minutes" in lower or "account" in lower:
            return "medium"
        return "high"
    if any(keyword in lower for keyword in ("cpi", "nonfarm payroll", "employment situation", "gdp", "pce")):
        return "high"
    if event_group in {"Inflation", "Labor"}:
        return "high"
    if event_group in {"GrowthActivity", "RatesFiscal"}:
        return "medium"
    return "low"


def _country_region_currency(source_name: str, text: str) -> tuple[str | None, str | None, str | None]:
    lower = _normalize_text(text).lower()
    if source_name in {"fred_releases", "bls_release_calendar", "bea_release_schedule", "fomc_schedule", "treasury_auction_schedule"}:
        return "US", "North America", "USD"
    if source_name == "ecb_policy_calendar":
        return "EA", "Europe", "EUR"
    if source_name == "boe_mpc_calendar":
        return "GB", "Europe", "GBP"
    if source_name == "boj_release_schedule":
        return "JP", "Asia", "JPY"
    if "euro" in lower or "ecb" in lower:
        return "EA", "Europe", "EUR"
    if "bank of england" in lower or "uk " in lower or "britain" in lower:
        return "GB", "Europe", "GBP"
    if "bank of japan" in lower or "japan" in lower or "boj" in lower:
        return "JP", "Asia", "JPY"
    if "fed" in lower or "fomc" in lower or "u.s." in lower or "us " in lower:
        return "US", "North America", "USD"
    return None, None, None


def _factor_tags(event_group: str) -> list[str]:
    mapping = {
        "Inflation": ["inflation"],
        "Labor": ["labor"],
        "GrowthActivity": ["growth"],
        "RatesFiscal": ["rates"],
        "CentralBankPolicy": ["policy", "rates"],
        "CreditRegulatory": ["credit"],
    }
    return mapping.get(event_group, [])


def _parse_ics_events(text: str) -> list[dict[str, str]]:
    unfolded_lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        if raw_line.startswith((" ", "\t")) and unfolded_lines:
            unfolded_lines[-1] += raw_line[1:]
        else:
            unfolded_lines.append(raw_line.strip())
    events: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in unfolded_lines:
        if line == "BEGIN:VEVENT":
            current = {}
            continue
        if line == "END:VEVENT":
            if current:
                events.append(current)
            current = None
            continue
        if current is None or ":" not in line:
            continue
        key, value = line.split(":", 1)
        current[key.split(";", 1)[0]] = value.strip()
    return events


def _append_quarantine(
    items: list[dict[str, Any]],
    *,
    source_name: str,
    dataset_name: str,
    record_kind: str,
    raw_identifier: Any,
    reason: str,
    observed_at: Any,
    source_updated_at: Any,
    payload: Any,
    source_hash: Any,
) -> None:
    items.append(
        {
            "source_name": source_name,
            "dataset_name": dataset_name,
            "record_kind": record_kind,
            "raw_identifier": _normalize_text(raw_identifier),
            "reason": _normalize_text(reason),
            "observed_at": _parse_datetime(observed_at),
            "source_updated_at": _parse_datetime(source_updated_at),
            "payload_preview": _payload_preview(payload),
            "source_hash": _normalize_text(source_hash),
        }
    )


def _build_source_event_record(
    *,
    source_name: str,
    dataset_name: str,
    source_event_key: str,
    event_name: str,
    effective_at: datetime | None,
    published_at: datetime | None,
    source_updated_at: datetime | None,
    ingested_at: datetime | None,
    event_type: str = "macro_release",
    country: str | None = None,
    region: str | None = None,
    currency: str | None = None,
    actual_numeric: float | None = None,
    actual_text: str | None = None,
    consensus_numeric: float | None = None,
    consensus_text: str | None = None,
    previous_numeric: float | None = None,
    previous_text: str | None = None,
    revised_previous_numeric: float | None = None,
    revised_previous_text: str | None = None,
    unit: str | None = None,
    period_label: str | None = None,
    frequency: str | None = None,
    time_precision: str = "unknown",
    schedule_status: str = "unknown",
    is_confirmed: bool = True,
    summary: str | None = None,
    source_url: str | None = None,
    market_sensitivity_tags: Sequence[str] | None = None,
    sector_tags: Sequence[str] | None = None,
    factor_tags: Sequence[str] | None = None,
    withdrawal_flag: bool = False,
) -> dict[str, Any]:
    event_group, event_subgroup = _keyword_group(event_name)
    inferred_country, inferred_region, inferred_currency = _country_region_currency(source_name, event_name)
    importance_tier = _importance_tier(event_name, event_group)
    tags = list(market_sensitivity_tags or [])
    if importance_tier == "high":
        tags = [*tags, "high-impact"]
    return {
        "source_record_id": _hash_record(
            source_name,
            dataset_name,
            source_event_key,
            _normalize_text(event_name),
            effective_at,
            published_at,
            source_updated_at,
            actual_numeric,
            actual_text,
            consensus_numeric,
            previous_numeric,
            revised_previous_numeric,
        ),
        "source_name": source_name,
        "dataset_name": dataset_name,
        "source_event_key": _normalize_text(source_event_key),
        "event_name": _normalize_text(event_name),
        "event_group": event_group,
        "event_subgroup": event_subgroup,
        "event_type": _normalize_text(event_type) or "macro_release",
        "importance_tier": importance_tier,
        "impact_domain": "macro",
        "country": country or inferred_country,
        "region": region or inferred_region,
        "currency": currency or inferred_currency,
        "effective_at": effective_at,
        "published_at": published_at,
        "source_updated_at": source_updated_at,
        "ingested_at": ingested_at,
        "time_precision": time_precision if time_precision in constants.TIME_PRECISION_VALUES else "unknown",
        "schedule_status": schedule_status if schedule_status in constants.SCHEDULE_STATUS_VALUES else "unknown",
        "is_confirmed": bool(is_confirmed),
        "actual_numeric": actual_numeric,
        "actual_text": _normalize_text(actual_text),
        "consensus_numeric": consensus_numeric,
        "consensus_text": _normalize_text(consensus_text),
        "previous_numeric": previous_numeric,
        "previous_text": _normalize_text(previous_text),
        "revised_previous_numeric": revised_previous_numeric,
        "revised_previous_text": _normalize_text(revised_previous_text),
        "unit": _normalize_text(unit),
        "period_label": _normalize_text(period_label),
        "frequency": _normalize_text(frequency),
        "summary": _normalize_text(summary),
        "market_sensitivity_tags_json": _json_dumps(_listify(tags)),
        "sector_tags_json": _json_dumps(_listify(sector_tags)),
        "factor_tags_json": _json_dumps(_listify(factor_tags or _factor_tags(event_group))),
        "is_high_impact": importance_tier == "high",
        "is_routine": event_group not in {"CentralBankPolicy", "RatesFiscal"},
        "is_revisionable": event_group in {"Inflation", "Labor", "GrowthActivity"},
        "withdrawal_flag": bool(withdrawal_flag),
        "source_url": _normalize_text(source_url),
        "source_hash": _hash_record(
            source_name,
            dataset_name,
            source_event_key,
            event_name,
            effective_at,
            schedule_status,
            actual_numeric,
            actual_text,
            consensus_numeric,
            previous_numeric,
            revised_previous_numeric,
            summary,
        ),
        "raw_identifier": _normalize_text(source_event_key),
    }


def _build_source_headline_record(
    *,
    source_name: str,
    dataset_name: str,
    source_item_id: str,
    headline: str,
    summary: str | None,
    url: str | None,
    author: str | None,
    published_at: datetime | None,
    source_updated_at: datetime | None,
    ingested_at: datetime | None,
    country: str | None = None,
    region: str | None = None,
    event_group: str | None = None,
    importance_tier: str | None = None,
    relevance_tier: str | None = None,
    withdrawal_flag: bool = False,
    tags: Sequence[str] | None = None,
    tickers: Sequence[str] | None = None,
    channels: Sequence[str] | None = None,
) -> dict[str, Any]:
    inferred_group, _ = _keyword_group(headline)
    inferred_country, inferred_region, _ = _country_region_currency(source_name, headline)
    resolved_group = _normalize_text(event_group) or inferred_group
    resolved_importance = importance_tier or _importance_tier(headline, resolved_group)
    return {
        "source_record_id": _hash_record(
            source_name,
            dataset_name,
            source_item_id,
            headline,
            published_at,
            source_updated_at,
            url,
        ),
        "source_name": source_name,
        "dataset_name": dataset_name,
        "source_item_id": _normalize_text(source_item_id),
        "headline": _normalize_text(headline),
        "summary": _normalize_text(summary),
        "url": _normalize_text(url),
        "author": _normalize_text(author),
        "published_at": published_at,
        "source_updated_at": source_updated_at,
        "ingested_at": ingested_at,
        "country": country or inferred_country,
        "region": region or inferred_region,
        "event_group": resolved_group,
        "importance_tier": resolved_importance if resolved_importance in constants.IMPORTANCE_TIERS else "medium",
        "relevance_tier": relevance_tier if relevance_tier in constants.IMPORTANCE_TIERS else resolved_importance,
        "withdrawal_flag": bool(withdrawal_flag),
        "tags_json": _json_dumps(_listify(tags)),
        "tickers_json": _json_dumps([ticker.upper() for ticker in _listify(tickers)]),
        "channels_json": _json_dumps(_listify(channels)),
        "source_hash": _hash_record(source_name, dataset_name, source_item_id, headline, summary, url, source_updated_at),
        "raw_identifier": _normalize_text(source_item_id),
    }


def _parse_fred_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    payload = batch.get("payload") or {}
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    for item in payload.get("release_dates", []) or []:
        release_date = _parse_date_only(item.get("date"))
        event_name = _normalize_text(item.get("release_name"))
        source_event_key = f"{item.get('release_id')}:{item.get('date')}"
        if not event_name or release_date is None:
            _append_quarantine(
                quarantine,
                source_name="fred_releases",
                dataset_name=str(batch.get("dataset_name") or "release_dates"),
                record_kind="event",
                raw_identifier=source_event_key,
                reason="missing_release_name_or_date",
                observed_at=batch.get("fetched_at"),
                source_updated_at=item.get("release_last_updated"),
                payload=item,
                source_hash=_hash_record(source_event_key, item),
            )
            continue
        schedule_status = "scheduled" if release_date.date() >= (ingested_at or datetime.now(UTC)).date() else "released"
        events.append(
            _build_source_event_record(
                source_name="fred_releases",
                dataset_name=str(batch.get("dataset_name") or "release_dates"),
                source_event_key=source_event_key,
                event_name=event_name,
                effective_at=release_date,
                published_at=release_date,
                source_updated_at=_parse_datetime(item.get("release_last_updated")),
                ingested_at=ingested_at,
                time_precision="date_only",
                schedule_status=schedule_status,
                summary="FRED release calendar entry",
                source_url=batch.get("request_url"),
            )
        )
    return events, [], quarantine


def _parse_bls_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    for item in _parse_ics_events(batch.get("payload") or ""):
        summary = item.get("SUMMARY")
        dtstart = item.get("DTSTART")
        uid = item.get("UID") or f"{summary}:{dtstart}"
        if not summary or not dtstart:
            _append_quarantine(
                quarantine,
                source_name="bls_release_calendar",
                dataset_name=str(batch.get("dataset_name") or "release_calendar"),
                record_kind="event",
                raw_identifier=uid,
                reason="missing_summary_or_dtstart",
                observed_at=batch.get("fetched_at"),
                source_updated_at=item.get("LAST-MODIFIED"),
                payload=item,
                source_hash=_hash_record(uid, item),
            )
            continue
        effective_at = _parse_datetime(dtstart, zone=_ET)
        time_precision = "exact" if "T" in str(dtstart) else "date_only"
        events.append(
            _build_source_event_record(
                source_name="bls_release_calendar",
                dataset_name=str(batch.get("dataset_name") or "release_calendar"),
                source_event_key=uid,
                event_name=summary,
                effective_at=effective_at,
                published_at=effective_at,
                source_updated_at=_parse_datetime(item.get("LAST-MODIFIED"), zone=_ET),
                ingested_at=ingested_at,
                time_precision=time_precision,
                schedule_status="scheduled" if effective_at and effective_at >= (ingested_at or datetime.now(UTC)) else "released",
                summary=item.get("DESCRIPTION"),
                source_url=batch.get("request_url"),
            )
        )
    return events, [], quarantine


def _strip_html(text: Any) -> str:
    raw = str(text or "")
    clean = re.sub(r"<[^>]+>", " ", raw)
    clean = clean.replace("&nbsp;", " ").replace("&amp;", "&")
    return _normalize_text(clean)


def _parse_bea_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    text = _strip_html(batch.get("payload"))
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    pattern = re.compile(
        rf"({_MONTH_PATTERN}\s+\d{{1,2}}\s+\d{{1,2}}:\d{{2}}\s+[AP]M)\s+(?:News|Data Release|Visual Data|Article|N|D|V|A)\s+(.+?)(?={_MONTH_PATTERN}\s+\d{{1,2}}\s+\d{{1,2}}:\d{{2}}\s+[AP]M|$)"
    )
    for match in pattern.finditer(text):
        stamp = _normalize_text(match.group(1))
        title = _normalize_text(match.group(13))
        effective_at = _parse_datetime(stamp, zone=_ET)
        if not title or effective_at is None:
            continue
        events.append(
            _build_source_event_record(
                source_name="bea_release_schedule",
                dataset_name=str(batch.get("dataset_name") or "release_schedule"),
                source_event_key=f"{title}:{stamp}",
                event_name=title,
                effective_at=effective_at,
                published_at=effective_at,
                source_updated_at=ingested_at,
                ingested_at=ingested_at,
                time_precision="exact",
                schedule_status="scheduled" if effective_at >= (ingested_at or datetime.now(UTC)) else "released",
                source_url=batch.get("request_url"),
            )
        )
    if not events:
        _append_quarantine(
            quarantine,
            source_name="bea_release_schedule",
            dataset_name=str(batch.get("dataset_name") or "release_schedule"),
            record_kind="event",
            raw_identifier="bea-release-schedule",
            reason="no_schedule_rows_parsed",
            observed_at=batch.get("fetched_at"),
            source_updated_at=batch.get("fetched_at"),
            payload=text,
            source_hash=_hash_record(text),
        )
    return events, [], quarantine


def _parse_fomc_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    text = _strip_html(batch.get("payload"))
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    year_sections = re.findall(r"For (\d{4}):(.+?)(?=For \d{4}:|$)", text)
    for year_text, block in year_sections:
        year = int(year_text)
        bullet_matches = re.findall(
            rf"{_DOW_PATTERN},\s+([A-Z][a-z]+\s+\d{{1,2}}),\s+and\s+{_DOW_PATTERN},\s+([A-Z][a-z]+\s+\d{{1,2}})",
            block,
        )
        for match in bullet_matches:
            first_day = match[1]
            second_day = match[3]
            effective_at = _with_local_time(_parse_date_only(second_day, year=year), 14, 0, _ET)
            if effective_at is None:
                continue
            name = "FOMC rate decision"
            events.append(
                _build_source_event_record(
                    source_name="fomc_schedule",
                    dataset_name=str(batch.get("dataset_name") or "schedule"),
                    source_event_key=f"fomc:{year}:{first_day}:{second_day}",
                    event_name=name,
                    effective_at=effective_at,
                    published_at=effective_at,
                    source_updated_at=ingested_at,
                    ingested_at=ingested_at,
                    event_type="policy_decision",
                    time_precision="exact",
                    schedule_status="scheduled" if effective_at >= (ingested_at or datetime.now(UTC)) else "released",
                    summary=f"FOMC meeting running from {first_day} to {second_day}",
                    source_url=batch.get("request_url"),
                )
            )
    if not events:
        _append_quarantine(
            quarantine,
            source_name="fomc_schedule",
            dataset_name=str(batch.get("dataset_name") or "schedule"),
            record_kind="event",
            raw_identifier=batch.get("request_url"),
            reason="no_fomc_rows_parsed",
            observed_at=batch.get("fetched_at"),
            source_updated_at=batch.get("fetched_at"),
            payload=text,
            source_hash=_hash_record(text),
        )
    return events, [], quarantine


def _parse_ecb_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    text = _strip_html(batch.get("payload"))
    events: list[dict[str, Any]] = []
    pattern = re.compile(r"(\d{2}/\d{2}/\d{4})\s+Governing Council of the ECB: monetary policy meeting.*?Day 2\), followed by press conference")
    for date_text in pattern.findall(text):
        parsed = _parse_datetime(date_text, zone=_CET)
        effective_at = _with_local_time(parsed, 14, 15, _CET)
        if effective_at is None:
            continue
        events.append(
            _build_source_event_record(
                source_name="ecb_policy_calendar",
                dataset_name=str(batch.get("dataset_name") or "meeting_calendar"),
                source_event_key=f"ecb:{date_text}",
                event_name="ECB monetary policy decision",
                effective_at=effective_at,
                published_at=effective_at,
                source_updated_at=_parse_datetime(batch.get("fetched_at")),
                ingested_at=_parse_datetime(batch.get("fetched_at")),
                event_type="policy_decision",
                time_precision="exact",
                schedule_status="scheduled" if effective_at >= (_parse_datetime(batch.get("fetched_at")) or datetime.now(UTC)) else "released",
                source_url=batch.get("request_url"),
            )
        )
    quarantine: list[dict[str, Any]] = []
    if not events:
        _append_quarantine(
            quarantine,
            source_name="ecb_policy_calendar",
            dataset_name=str(batch.get("dataset_name") or "meeting_calendar"),
            record_kind="event",
            raw_identifier="ecb-meeting-calendar",
            reason="no_ecb_rows_parsed",
            observed_at=batch.get("fetched_at"),
            source_updated_at=batch.get("fetched_at"),
            payload=text,
            source_hash=_hash_record(text),
        )
    return events, [], quarantine


def _parse_boe_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    text = _strip_html(batch.get("payload"))
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    matches = re.findall(rf"{_DOW_PATTERN}\s+(\d{{1,2}}\s+[A-Z][a-z]+(?:\s+\d{{4}})?)", text)
    for raw_date in matches:
        effective_day = _parse_date_only(raw_date, year=(ingested_at or datetime.now(UTC)).year)
        if effective_day is None:
            continue
        effective_at = _with_local_time(effective_day, 12, 0, _LONDON)
        events.append(
            _build_source_event_record(
                source_name="boe_mpc_calendar",
                dataset_name=str(batch.get("dataset_name") or "mpc_dates"),
                source_event_key=f"boe:{raw_date}",
                event_name="BoE MPC rate decision",
                effective_at=effective_at,
                published_at=effective_at,
                source_updated_at=ingested_at,
                ingested_at=ingested_at,
                event_type="policy_decision",
                time_precision="exact",
                schedule_status="scheduled" if effective_at and effective_at >= (ingested_at or datetime.now(UTC)) else "released",
                source_url=batch.get("request_url"),
            )
        )
    dedup = {
        row["source_event_key"]: row
        for row in sorted(events, key=lambda item: item.get("effective_at") or datetime.min.replace(tzinfo=UTC))
    }
    events = list(dedup.values())
    if not events:
        _append_quarantine(
            quarantine,
            source_name="boe_mpc_calendar",
            dataset_name=str(batch.get("dataset_name") or "mpc_dates"),
            record_kind="event",
            raw_identifier="boe-mpc-calendar",
            reason="no_boe_rows_parsed",
            observed_at=batch.get("fetched_at"),
            source_updated_at=batch.get("fetched_at"),
            payload=text,
            source_hash=_hash_record(text),
        )
    return events, [], quarantine


def _parse_boj_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    text = _strip_html(batch.get("payload"))
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    for raw_date in re.findall(r"([A-Z][a-z]{2}\.\s+\d{1,2})\s+\(.+?\),\s+(\d{1,2})\s+\(.+?\)", text):
        first_day, second_day = raw_date
        effective_day = _parse_date_only(second_day, year=(ingested_at or datetime.now(UTC)).year)
        if effective_day is None:
            continue
        effective_at = _with_local_time(effective_day, 14, 0, _TOKYO)
        events.append(
            _build_source_event_record(
                source_name="boj_release_schedule",
                dataset_name=str(batch.get("dataset_name") or "release_schedule"),
                source_event_key=f"boj:{first_day}:{second_day}",
                event_name="BoJ monetary policy decision",
                effective_at=effective_at,
                published_at=effective_at,
                source_updated_at=ingested_at,
                ingested_at=ingested_at,
                event_type="policy_decision",
                time_precision="approximate",
                schedule_status="scheduled" if effective_at and effective_at >= (ingested_at or datetime.now(UTC)) else "released",
                source_url=batch.get("request_url"),
            )
        )
    if not events:
        for match in re.findall(r"(\d{1,2})\s+\|\s+undecided\s+\|\s+Statement on Monetary Policy", text):
            effective_day = _parse_date_only(match, year=(ingested_at or datetime.now(UTC)).year)
            effective_at = _with_local_time(effective_day, 14, 0, _TOKYO)
            if effective_at is not None:
                events.append(
                    _build_source_event_record(
                        source_name="boj_release_schedule",
                        dataset_name=str(batch.get("dataset_name") or "release_schedule"),
                        source_event_key=f"boj:{match}",
                        event_name="BoJ monetary policy decision",
                        effective_at=effective_at,
                        published_at=effective_at,
                        source_updated_at=ingested_at,
                        ingested_at=ingested_at,
                        event_type="policy_decision",
                        time_precision="approximate",
                        schedule_status="scheduled" if effective_at >= (ingested_at or datetime.now(UTC)) else "released",
                        source_url=batch.get("request_url"),
                    )
                )
    if not events:
        _append_quarantine(
            quarantine,
            source_name="boj_release_schedule",
            dataset_name=str(batch.get("dataset_name") or "release_schedule"),
            record_kind="event",
            raw_identifier="boj-release-schedule",
            reason="no_boj_rows_parsed",
            observed_at=batch.get("fetched_at"),
            source_updated_at=batch.get("fetched_at"),
            payload=text,
            source_hash=_hash_record(text),
        )
    return events, [], quarantine


def _parse_treasury_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    payload = batch.get("payload") or ""
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    try:
        root = ET.fromstring(str(payload))
    except Exception:
        root = None
    if root is not None:
        children = list(root)
        records = children if children and len(children) > 1 else list(root.iter())[1:]
        for item in records:
            data = {child.tag.split("}", 1)[-1]: _normalize_text(child.text) for child in list(item)}
            security_type = data.get("securityType") or data.get("type")
            auction_date = _parse_date_only(data.get("auctionDate") or data.get("auction_date"))
            if not security_type or auction_date is None:
                continue
            events.append(
                _build_source_event_record(
                    source_name="treasury_auction_schedule",
                    dataset_name=str(batch.get("dataset_name") or "upcoming_auctions"),
                    source_event_key=data.get("cusip") or f"{security_type}:{auction_date.date().isoformat()}",
                    event_name=f"U.S. Treasury {security_type} auction",
                    effective_at=_with_local_time(auction_date, 11, 30, _ET),
                    published_at=auction_date,
                    source_updated_at=ingested_at,
                    ingested_at=ingested_at,
                    event_type="auction",
                    time_precision="approximate",
                    schedule_status="scheduled" if auction_date.date() >= (ingested_at or datetime.now(UTC)).date() else "released",
                    source_url=batch.get("request_url"),
                    summary=data.get("announcementType") or data.get("operationStatus"),
                )
            )
    if not events:
        _append_quarantine(
            quarantine,
            source_name="treasury_auction_schedule",
            dataset_name=str(batch.get("dataset_name") or "upcoming_auctions"),
            record_kind="event",
            raw_identifier="treasury-upcoming-auctions",
            reason="no_treasury_rows_parsed",
            observed_at=batch.get("fetched_at"),
            source_updated_at=batch.get("fetched_at"),
            payload=payload,
            source_hash=_hash_record(payload),
        )
    return events, [], quarantine


def _parse_nasdaq_batch(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    payload = batch.get("payload") or {}
    mapping = payload.get("mapping") or {}
    rows = payload.get("rows") or []
    events: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    ingested_at = _parse_datetime(batch.get("fetched_at"))
    static_values = mapping.get("static_values") or {}
    for row in rows:
        title_column = mapping.get("title_column") or "event_name"
        event_name = _normalize_text(row.get(title_column) or static_values.get("event_name"))
        source_event_key = _normalize_text(row.get("id") or row.get(title_column) or _hash_record(row))
        effective_at = _parse_datetime(row.get(mapping.get("effective_at_column"))) if mapping.get("effective_at_column") else None
        if effective_at is None and mapping.get("date_column"):
            effective_at = _parse_date_only(row.get(mapping.get("date_column")))
        if not event_name or effective_at is None:
            _append_quarantine(
                quarantine,
                source_name="nasdaq_tables",
                dataset_name=str(batch.get("dataset_name") or "table"),
                record_kind="event",
                raw_identifier=source_event_key,
                reason="missing_title_or_effective_at",
                observed_at=batch.get("fetched_at"),
                source_updated_at=row.get(mapping.get("updated_at_column") or ""),
                payload=row,
                source_hash=_hash_record(source_event_key, row),
            )
            continue
        importance = _normalize_text(row.get(mapping.get("importance_column") or "")) or _normalize_text(static_values.get("importance"))
        lowered_importance = importance.lower()
        if lowered_importance not in constants.IMPORTANCE_TIERS:
            if lowered_importance in {"3", "high", "important"}:
                lowered_importance = "high"
            elif lowered_importance in {"2", "medium", "moderate"}:
                lowered_importance = "medium"
            else:
                lowered_importance = "low"
        record = _build_source_event_record(
            source_name="nasdaq_tables",
            dataset_name=str(batch.get("dataset_name") or "table"),
            source_event_key=source_event_key,
            event_name=event_name,
            effective_at=effective_at,
            published_at=_parse_datetime(row.get(mapping.get("published_at_column"))) if mapping.get("published_at_column") else effective_at,
            source_updated_at=_parse_datetime(row.get(mapping.get("updated_at_column"))) if mapping.get("updated_at_column") else ingested_at,
            ingested_at=ingested_at,
            event_type=_normalize_text(mapping.get("event_type")) or "macro_release",
            country=_normalize_text(row.get(mapping.get("country_column"))) or _normalize_text(static_values.get("country")),
            region=_normalize_text(row.get(mapping.get("region_column"))) or _normalize_text(static_values.get("region")),
            currency=_normalize_text(row.get(mapping.get("currency_column"))) or _normalize_text(static_values.get("currency")),
            actual_numeric=_coerce_float(row.get(mapping.get("actual_column"))) if mapping.get("actual_column") else None,
            actual_text=_normalize_text(row.get(mapping.get("actual_column"))) if mapping.get("actual_column") else None,
            consensus_numeric=_coerce_float(row.get(mapping.get("consensus_column"))) if mapping.get("consensus_column") else None,
            consensus_text=_normalize_text(row.get(mapping.get("consensus_column"))) if mapping.get("consensus_column") else None,
            previous_numeric=_coerce_float(row.get(mapping.get("previous_column"))) if mapping.get("previous_column") else None,
            previous_text=_normalize_text(row.get(mapping.get("previous_column"))) if mapping.get("previous_column") else None,
            revised_previous_numeric=_coerce_float(row.get(mapping.get("revised_previous_column"))) if mapping.get("revised_previous_column") else None,
            revised_previous_text=_normalize_text(row.get(mapping.get("revised_previous_column"))) if mapping.get("revised_previous_column") else None,
            unit=_normalize_text(row.get(mapping.get("unit_column"))) if mapping.get("unit_column") else _normalize_text(static_values.get("unit")),
            period_label=_normalize_text(row.get(mapping.get("period_column"))) if mapping.get("period_column") else None,
            frequency=_normalize_text(row.get(mapping.get("frequency_column"))) if mapping.get("frequency_column") else None,
            time_precision="exact" if mapping.get("effective_at_column") else "date_only",
            schedule_status=_normalize_text(row.get(mapping.get("status_column"))) or "scheduled",
            source_url=batch.get("request_url"),
        )
        record["importance_tier"] = lowered_importance
        record["is_high_impact"] = lowered_importance == "high"
        events.append(record)
    return events, [], quarantine


def _parse_massive_headlines(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    payload = batch.get("payload") or {}
    headlines: list[dict[str, Any]] = []
    for item in payload.get("results", []) or []:
        headline = _normalize_text(item.get("title"))
        if not headline:
            continue
        headlines.append(
            _build_source_headline_record(
                source_name="massive_news",
                dataset_name=str(batch.get("dataset_name") or "benzinga_news"),
                source_item_id=str(item.get("benzinga_id") or item.get("id") or headline),
                headline=headline,
                summary=item.get("teaser"),
                url=item.get("url"),
                author=item.get("author"),
                published_at=_parse_datetime(item.get("published")),
                source_updated_at=_parse_datetime(item.get("last_updated")),
                ingested_at=_parse_datetime(batch.get("fetched_at")),
                tags=item.get("tags"),
                tickers=item.get("tickers"),
                channels=item.get("channels"),
            )
        )
    return [], headlines, []


def _parse_alpaca_headlines(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    payload = batch.get("payload") or {}
    records = payload.get("news") or payload.get("results") or []
    headlines: list[dict[str, Any]] = []
    for item in records:
        headline = _normalize_text(item.get("headline"))
        if not headline:
            continue
        headlines.append(
            _build_source_headline_record(
                source_name="alpaca_news",
                dataset_name=str(batch.get("dataset_name") or "historical_news"),
                source_item_id=str(item.get("id") or headline),
                headline=headline,
                summary=item.get("summary"),
                url=item.get("url"),
                author=item.get("author"),
                published_at=_parse_datetime(item.get("created_at")),
                source_updated_at=_parse_datetime(item.get("updated_at")),
                ingested_at=_parse_datetime(batch.get("fetched_at")),
                tickers=item.get("symbols"),
            )
        )
    return [], headlines, []


def _parse_alpha_vantage_headlines(batch: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    payload = batch.get("payload") or {}
    records = payload.get("feed") or []
    headlines: list[dict[str, Any]] = []
    for item in records:
        headline = _normalize_text(item.get("title"))
        if not headline:
            continue
        tickers = [entry.get("ticker") for entry in item.get("ticker_sentiment", []) or []]
        topics = [entry.get("topic") for entry in item.get("topics", []) or []]
        headlines.append(
            _build_source_headline_record(
                source_name="alpha_vantage_news",
                dataset_name=str(batch.get("dataset_name") or "news_sentiment"),
                source_item_id=str(item.get("url") or headline),
                headline=headline,
                summary=item.get("summary"),
                url=item.get("url"),
                author=item.get("authors"),
                published_at=_parse_datetime(item.get("time_published")),
                source_updated_at=_parse_datetime(item.get("time_published")),
                ingested_at=_parse_datetime(batch.get("fetched_at")),
                tags=topics,
                tickers=tickers,
                channels=topics,
            )
        )
    return [], headlines, []


_PARSERS: dict[str, Any] = {
    "fred_releases": _parse_fred_batch,
    "bls_release_calendar": _parse_bls_batch,
    "bea_release_schedule": _parse_bea_batch,
    "fomc_schedule": _parse_fomc_batch,
    "ecb_policy_calendar": _parse_ecb_batch,
    "boe_mpc_calendar": _parse_boe_batch,
    "boj_release_schedule": _parse_boj_batch,
    "treasury_auction_schedule": _parse_treasury_batch,
    "nasdaq_tables": _parse_nasdaq_batch,
    "massive_news": _parse_massive_headlines,
    "alpaca_news": _parse_alpaca_headlines,
    "alpha_vantage_news": _parse_alpha_vantage_headlines,
}


def parse_raw_batches_to_source_frames(
    batches: Sequence[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source_events: list[dict[str, Any]] = []
    source_headlines: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    for batch in batches:
        source_name = _normalize_text(batch.get("source_name"))
        parser = _PARSERS.get(source_name)
        if parser is None:
            _append_quarantine(
                quarantine,
                source_name=source_name or "unknown",
                dataset_name=str(batch.get("dataset_name") or "unknown"),
                record_kind="batch",
                raw_identifier=batch.get("request_url"),
                reason="unsupported_source_name",
                observed_at=batch.get("fetched_at"),
                source_updated_at=batch.get("fetched_at"),
                payload=batch,
                source_hash=_hash_record(batch),
            )
            continue
        try:
            parsed_events, parsed_headlines, parsed_quarantine = parser(batch)
            source_events.extend(parsed_events)
            source_headlines.extend(parsed_headlines)
            quarantine.extend(parsed_quarantine)
        except Exception as exc:
            _append_quarantine(
                quarantine,
                source_name=source_name or "unknown",
                dataset_name=str(batch.get("dataset_name") or "unknown"),
                record_kind="batch",
                raw_identifier=batch.get("request_url"),
                reason=f"parser_failure:{type(exc).__name__}",
                observed_at=batch.get("fetched_at"),
                source_updated_at=batch.get("fetched_at"),
                payload=batch,
                source_hash=_hash_record(batch, exc),
            )
            mdc.write_error(f"Economic catalyst parser failure source={source_name}: {type(exc).__name__}: {exc}")
    return (
        _ensure_frame(source_events, constants.INTERNAL_SOURCE_EVENT_COLUMNS),
        _ensure_frame(source_headlines, constants.INTERNAL_SOURCE_HEADLINE_COLUMNS),
        _ensure_frame(quarantine, constants.QUARANTINE_COLUMNS),
    )


def _event_identity(row: dict[str, Any]) -> str:
    effective_at = _parse_datetime(row.get("effective_at"))
    effective_part = effective_at.isoformat() if effective_at is not None else _normalize_text(row.get("source_event_key"))
    return _hash_record(
        row.get("country"),
        row.get("event_group"),
        row.get("event_subgroup"),
        row.get("event_type"),
        row.get("period_label"),
        effective_part,
    )


def _headline_identity(row: dict[str, Any]) -> str:
    url = _normalize_text(row.get("url"))
    published_at = _parse_datetime(row.get("published_at"))
    anchor = url or _normalize_text(row.get("headline"))
    return _hash_record(anchor, published_at.isoformat() if published_at is not None else "", row.get("author"))


def _row_observed_at(row: dict[str, Any]) -> datetime:
    for key in ("ingested_at", "source_updated_at", "published_at", "effective_at"):
        value = _parse_datetime(row.get(key))
        if value is not None:
            return value
    return datetime(1970, 1, 1, tzinfo=UTC)


def _choose_by_priority(rows: Sequence[dict[str, Any]], *, fields: Sequence[str]) -> dict[str, Any]:
    best: dict[str, Any] = {}
    ordered = sorted(
        rows,
        key=lambda row: (constants.SOURCE_PRIORITY.get(str(row.get("source_name")), 999), -_row_observed_at(row).timestamp()),
    )
    for field in fields:
        chosen = None
        for row in ordered:
            value = row.get(field)
            if value is None or value is pd.NA:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            chosen = value
            break
        best[field] = chosen
    return best


def _choose_latest_non_null(rows: Sequence[dict[str, Any]], field: str) -> Any:
    for row in sorted(rows, key=lambda item: _row_observed_at(item), reverse=True):
        value = row.get(field)
        if value is None or value is pd.NA:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _event_state_from_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    event_id = _event_identity(rows[-1])
    event_fields = _choose_by_priority(
        rows,
        fields=(
            "event_name",
            "event_group",
            "event_subgroup",
            "event_type",
            "importance_tier",
            "impact_domain",
            "country",
            "region",
            "currency",
            "time_precision",
            "schedule_status",
            "is_confirmed",
            "unit",
            "period_label",
            "frequency",
        ),
    )
    official_rows = [row for row in rows if str(row.get("source_name")) in constants.OFFICIAL_SOURCES]
    vendor_rows = [row for row in rows if str(row.get("source_name")) in constants.STRUCTURED_VENDOR_SOURCES]
    base_rows = official_rows or vendor_rows or rows
    effective_at = _choose_by_priority(base_rows, fields=("effective_at",)).get("effective_at")
    if _parse_datetime(effective_at) is None:
        effective_at = _choose_by_priority(rows, fields=("effective_at",)).get("effective_at")
    actual_numeric = _coerce_float(_choose_by_priority(base_rows, fields=("actual_numeric",)).get("actual_numeric"))
    if actual_numeric is None:
        actual_numeric = _coerce_float(
            _choose_by_priority(vendor_rows or rows, fields=("actual_numeric",)).get("actual_numeric")
        )
    actual_text = _choose_by_priority(base_rows, fields=("actual_text",)).get("actual_text")
    if not _normalize_text(actual_text):
        actual_text = _choose_by_priority(vendor_rows or rows, fields=("actual_text",)).get("actual_text")
    consensus_numeric = _coerce_float(
        _choose_by_priority(vendor_rows or rows, fields=("consensus_numeric",)).get("consensus_numeric")
    )
    consensus_text = _choose_by_priority(vendor_rows or rows, fields=("consensus_text",)).get("consensus_text")
    previous_numeric = _coerce_float(
        _choose_by_priority(vendor_rows or rows, fields=("previous_numeric",)).get("previous_numeric")
    )
    previous_text = _choose_by_priority(vendor_rows or rows, fields=("previous_text",)).get("previous_text")
    revised_previous_numeric = _coerce_float(
        _choose_by_priority(vendor_rows or rows, fields=("revised_previous_numeric",)).get("revised_previous_numeric")
    )
    revised_previous_text = _choose_by_priority(vendor_rows or rows, fields=("revised_previous_text",)).get("revised_previous_text")
    published_at_values = [_parse_datetime(row.get("published_at")) for row in rows if _parse_datetime(row.get("published_at")) is not None]
    published_at = min(published_at_values) if published_at_values else None
    source_updated_at_values = [
        _parse_datetime(row.get("source_updated_at"))
        for row in rows
        if _parse_datetime(row.get("source_updated_at")) is not None
    ]
    source_updated_at = max(source_updated_at_values) if source_updated_at_values else None
    ingested_at = max((_row_observed_at(row) for row in rows), default=None)
    provenance = {
        "contributors": [
            {
                "source": row.get("source_name"),
                "dataset": row.get("dataset_name"),
                "sourceEventKey": row.get("source_event_key"),
                "observedAt": _row_observed_at(row).isoformat(),
            }
            for row in sorted(rows, key=_row_observed_at)
        ]
    }
    source_name = _choose_by_priority(base_rows, fields=("source_name",)).get("source_name")
    official_source_name = (
        _choose_by_priority(official_rows, fields=("source_name",)).get("source_name") if official_rows else None
    )
    source_event_key = _choose_by_priority(base_rows, fields=("source_event_key",)).get("source_event_key")
    market_sensitivity = _choose_latest_non_null(rows, "market_sensitivity_tags_json") or "[]"
    sector_tags = _choose_latest_non_null(rows, "sector_tags_json") or "[]"
    factor_tags = _choose_latest_non_null(rows, "factor_tags_json") or "[]"
    surprise_abs = None
    surprise_pct = None
    if actual_numeric is not None and consensus_numeric is not None:
        surprise_abs = float(actual_numeric) - float(consensus_numeric)
        if float(consensus_numeric) != 0.0:
            surprise_pct = surprise_abs / float(abs(consensus_numeric))
    schedule_status = event_fields.get("schedule_status") or "unknown"
    if (actual_numeric is not None or _normalize_text(actual_text)) and schedule_status in {"scheduled", "unknown"}:
        schedule_status = "released"
    state = {
        "event_id": event_id,
        "event_key": event_id,
        "event_name": event_fields.get("event_name"),
        "event_group": event_fields.get("event_group"),
        "event_subgroup": event_fields.get("event_subgroup"),
        "event_type": event_fields.get("event_type"),
        "importance_tier": event_fields.get("importance_tier"),
        "impact_domain": event_fields.get("impact_domain"),
        "country": event_fields.get("country"),
        "region": event_fields.get("region"),
        "currency": event_fields.get("currency"),
        "source_name": source_name,
        "source_event_key": source_event_key,
        "official_source_name": official_source_name,
        "effective_at": effective_at,
        "published_at": published_at,
        "source_updated_at": source_updated_at,
        "ingested_at": ingested_at,
        "time_precision": event_fields.get("time_precision") or "unknown",
        "schedule_status": schedule_status,
        "is_confirmed": bool(event_fields.get("is_confirmed", True)),
        "actual_numeric": actual_numeric,
        "actual_text": _normalize_text(actual_text),
        "consensus_numeric": consensus_numeric,
        "consensus_text": _normalize_text(consensus_text),
        "previous_numeric": previous_numeric,
        "previous_text": _normalize_text(previous_text),
        "revised_previous_numeric": revised_previous_numeric,
        "revised_previous_text": _normalize_text(revised_previous_text),
        "surprise_abs": surprise_abs,
        "surprise_pct": surprise_pct,
        "unit": _normalize_text(event_fields.get("unit")),
        "period_label": _normalize_text(event_fields.get("period_label")),
        "frequency": _normalize_text(event_fields.get("frequency")),
        "market_sensitivity_tags_json": market_sensitivity,
        "sector_tags_json": sector_tags,
        "factor_tags_json": factor_tags,
        "is_high_impact": _safe_bool(event_fields.get("importance_tier") == "high", default=False),
        "is_routine": all(_safe_bool(row.get("is_routine"), default=True) for row in rows),
        "is_revisionable": any(_safe_bool(row.get("is_revisionable"), default=False) for row in rows),
        "withdrawal_flag": any(_safe_bool(row.get("withdrawal_flag"), default=False) for row in rows),
        "source_hash": "",
        "provenance_json": _json_dumps(provenance),
    }
    state["source_hash"] = _hash_record(*(state.get(column) for column in constants.EVENT_COLUMNS if column != "source_hash"))
    return state


def _event_version_kind(previous: dict[str, Any] | None, current: dict[str, Any]) -> str:
    if previous is None:
        if current.get("withdrawal_flag"):
            return "withdrawal"
        if current.get("actual_numeric") is not None or _normalize_text(current.get("actual_text")):
            return "release"
        return "schedule"
    if current.get("withdrawal_flag") and not previous.get("withdrawal_flag"):
        return "withdrawal"
    if current.get("schedule_status") == "cancelled" and previous.get("schedule_status") != "cancelled":
        return "cancellation"
    if previous.get("actual_numeric") is None and current.get("actual_numeric") is not None:
        return "release"
    if previous.get("actual_text") != current.get("actual_text") or previous.get("actual_numeric") != current.get("actual_numeric"):
        return "revision"
    if previous.get("effective_at") != current.get("effective_at"):
        return "schedule"
    return "correction"


def _headline_state_from_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    headline_id = _headline_identity(rows[-1])
    headline_fields = _choose_by_priority(
        rows,
        fields=("headline", "summary", "url", "author", "country", "region", "event_group", "importance_tier", "relevance_tier"),
    )
    published_values = [_parse_datetime(row.get("published_at")) for row in rows if _parse_datetime(row.get("published_at")) is not None]
    published_at = min(published_values) if published_values else None
    updated_values = [
        _parse_datetime(row.get("source_updated_at"))
        for row in rows
        if _parse_datetime(row.get("source_updated_at")) is not None
    ]
    source_updated_at = max(updated_values) if updated_values else None
    ingested_at = max((_row_observed_at(row) for row in rows), default=None)
    source_name = _choose_by_priority(rows, fields=("source_name",)).get("source_name")
    source_item_id = _choose_by_priority(rows, fields=("source_item_id",)).get("source_item_id")
    state = {
        "headline_id": headline_id,
        "headline_key": headline_id,
        "source_name": source_name,
        "source_item_id": source_item_id,
        "headline": headline_fields.get("headline"),
        "summary": headline_fields.get("summary"),
        "url": headline_fields.get("url"),
        "author": headline_fields.get("author"),
        "published_at": published_at,
        "source_updated_at": source_updated_at,
        "ingested_at": ingested_at,
        "country": headline_fields.get("country"),
        "region": headline_fields.get("region"),
        "event_group": headline_fields.get("event_group"),
        "importance_tier": headline_fields.get("importance_tier") or "medium",
        "relevance_tier": headline_fields.get("relevance_tier") or headline_fields.get("importance_tier") or "medium",
        "withdrawal_flag": any(_safe_bool(row.get("withdrawal_flag"), default=False) for row in rows),
        "tags_json": _choose_latest_non_null(rows, "tags_json") or "[]",
        "tickers_json": _choose_latest_non_null(rows, "tickers_json") or "[]",
        "channels_json": _choose_latest_non_null(rows, "channels_json") or "[]",
        "source_hash": "",
        "provenance_json": _json_dumps(
            {
                "contributors": [
                    {
                        "source": row.get("source_name"),
                        "dataset": row.get("dataset_name"),
                        "sourceItemId": row.get("source_item_id"),
                        "observedAt": _row_observed_at(row).isoformat(),
                    }
                    for row in sorted(rows, key=_row_observed_at)
                ]
            }
        ),
    }
    state["source_hash"] = _hash_record(*(state.get(column) for column in constants.HEADLINE_COLUMNS if column != "source_hash"))
    return state


def _headline_version_kind(previous: dict[str, Any] | None, current: dict[str, Any]) -> str:
    if previous is None:
        return "publish"
    if current.get("withdrawal_flag") and not previous.get("withdrawal_flag"):
        return "withdrawal"
    if previous.get("headline") != current.get("headline") or previous.get("summary") != current.get("summary"):
        return "edit"
    return "correction"


def _append_versions(
    rows: Sequence[dict[str, Any]],
    *,
    state_builder: Any,
    version_kind_builder: Any,
    id_field: str,
    version_columns: Sequence[str],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    ordered_rows = sorted(rows, key=_row_observed_at)
    latest_by_source: dict[str, dict[str, Any]] = {}
    versions: list[dict[str, Any]] = []
    previous_state: dict[str, Any] | None = None
    version_seq = 0
    current_state: dict[str, Any] | None = None
    for row in ordered_rows:
        source_key = _hash_record(row.get("source_name"), row.get("dataset_name"), row.get("raw_identifier"))
        latest_by_source[source_key] = row
        candidate_state = state_builder(list(latest_by_source.values()))
        if previous_state is not None and candidate_state.get("source_hash") == previous_state.get("source_hash"):
            current_state = candidate_state
            continue
        version_seq += 1
        version_id = _hash_record(candidate_state.get(id_field), version_seq, candidate_state.get("source_hash"))
        version_kind = version_kind_builder(previous_state, candidate_state)
        version = {
            "version_id": version_id,
            id_field: candidate_state.get(id_field),
            "version_seq": version_seq,
            "version_kind": version_kind,
            "version_observed_at": _row_observed_at(row),
        }
        for column in version_columns:
            if column in version:
                continue
            version[column] = candidate_state.get(column)
        versions.append(version)
        previous_state = candidate_state
        current_state = candidate_state
    return current_state, versions


def _build_mentions(
    *,
    events: pd.DataFrame,
    headlines: pd.DataFrame,
) -> pd.DataFrame:
    mentions: list[dict[str, Any]] = []
    rule_version = "v1"
    for _, row in events.iterrows():
        event_id = row.get("event_id")
        base = {
            "item_kind": "event",
            "item_id": event_id,
            "source_name": row.get("source_name"),
            "published_at": row.get("published_at"),
            "effective_at": row.get("effective_at"),
            "ingested_at": row.get("ingested_at"),
            "relevance_tier": row.get("importance_tier"),
            "confidence": 1.0,
            "mapping_rule_version": rule_version,
        }
        for entity_type, entity_key in (
            ("country", row.get("country")),
            ("region", row.get("region")),
            ("currency", row.get("currency")),
            ("indicator", row.get("event_group")),
        ):
            if _normalize_text(entity_key):
                mentions.append({**base, "entity_type": entity_type, "entity_key": _normalize_text(entity_key)})
        lower_name = _normalize_text(row.get("event_name")).lower()
        central_bank = None
        if "fomc" in lower_name or "fed" in lower_name:
            central_bank = "FED"
        elif "ecb" in lower_name:
            central_bank = "ECB"
        elif "boe" in lower_name or "bank rate" in lower_name:
            central_bank = "BOE"
        elif "boj" in lower_name or "bank of japan" in lower_name:
            central_bank = "BOJ"
        if central_bank:
            mentions.append({**base, "entity_type": "central_bank", "entity_key": central_bank})
        for factor in _listify(json.loads(row.get("factor_tags_json") or "[]")):
            mentions.append({**base, "entity_type": "factor", "entity_key": factor})
    for _, row in headlines.iterrows():
        headline_id = row.get("headline_id")
        base = {
            "item_kind": "headline",
            "item_id": headline_id,
            "source_name": row.get("source_name"),
            "published_at": row.get("published_at"),
            "effective_at": row.get("published_at"),
            "ingested_at": row.get("ingested_at"),
            "relevance_tier": row.get("relevance_tier"),
            "confidence": 0.9,
            "mapping_rule_version": rule_version,
        }
        for entity_type, entity_key in (
            ("country", row.get("country")),
            ("region", row.get("region")),
            ("indicator", row.get("event_group")),
        ):
            if _normalize_text(entity_key):
                mentions.append({**base, "entity_type": entity_type, "entity_key": _normalize_text(entity_key)})
        for ticker in _listify(row.get("tickers_json")):
            mentions.append({**base, "entity_type": "symbol", "entity_key": ticker.upper(), "confidence": 1.0})
    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for mention in mentions:
        key = (
            _normalize_text(mention.get("item_kind")),
            _normalize_text(mention.get("item_id")),
            _normalize_text(mention.get("entity_type")),
            _normalize_text(mention.get("entity_key")),
        )
        deduped[key] = mention
    return _ensure_frame(list(deduped.values()), constants.MENTION_COLUMNS)


def build_entity_daily(
    *,
    events: pd.DataFrame,
    headlines: pd.DataFrame,
    mentions: pd.DataFrame,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    anchor = as_of or datetime.now(UTC)
    event_lookup = {str(row["event_id"]): row for _, row in events.iterrows()}
    headline_lookup = {str(row["headline_id"]): row for _, row in headlines.iterrows()}
    buckets: dict[tuple[date, str, str], dict[str, Any]] = {}
    for _, mention in mentions.iterrows():
        item_kind = _normalize_text(mention.get("item_kind"))
        item_id = _normalize_text(mention.get("item_id"))
        entity_type = _normalize_text(mention.get("entity_type"))
        entity_key = _normalize_text(mention.get("entity_key"))
        if not item_kind or not item_id or not entity_type or not entity_key:
            continue
        parent = event_lookup.get(item_id) if item_kind == "event" else headline_lookup.get(item_id)
        if parent is None:
            continue
        anchor_dt = _parse_datetime(parent.get("effective_at") if item_kind == "event" else parent.get("published_at"))
        if anchor_dt is None:
            anchor_dt = anchor
        bucket_key = (anchor_dt.date(), entity_type, entity_key)
        bucket = buckets.setdefault(
            bucket_key,
            {
                "as_of_date": anchor_dt.date(),
                "entity_type": entity_type,
                "entity_key": entity_key,
                "headline_count": 0,
                "event_count": 0,
                "high_impact_event_count": 0,
                "release_count": 0,
                "scheduled_count": 0,
                "policy_event_count": 0,
                "inflation_event_count": 0,
                "labor_event_count": 0,
                "growth_event_count": 0,
                "rates_event_count": 0,
                "last_published_at": None,
                "last_effective_at": None,
                "ingested_at": anchor,
            },
        )
        bucket["last_published_at"] = max(
            [value for value in (bucket.get("last_published_at"), _parse_datetime(parent.get("published_at"))) if value is not None],
            default=None,
        )
        bucket["last_effective_at"] = max(
            [value for value in (bucket.get("last_effective_at"), _parse_datetime(parent.get("effective_at"))) if value is not None],
            default=None,
        )
        if item_kind == "headline":
            bucket["headline_count"] += 1
            continue
        bucket["event_count"] += 1
        if _safe_bool(parent.get("is_high_impact"), default=False):
            bucket["high_impact_event_count"] += 1
        status = _normalize_text(parent.get("schedule_status")).lower()
        if status in {"released", "revised"}:
            bucket["release_count"] += 1
        elif status == "scheduled":
            bucket["scheduled_count"] += 1
        group = _normalize_text(parent.get("event_group"))
        if group == "CentralBankPolicy":
            bucket["policy_event_count"] += 1
        elif group == "Inflation":
            bucket["inflation_event_count"] += 1
        elif group == "Labor":
            bucket["labor_event_count"] += 1
        elif group == "GrowthActivity":
            bucket["growth_event_count"] += 1
        elif group == "RatesFiscal":
            bucket["rates_event_count"] += 1
    return _ensure_frame(list(buckets.values()), constants.ENTITY_DAILY_COLUMNS)


def canonicalize_source_state(
    *,
    source_events: pd.DataFrame,
    source_headlines: pd.DataFrame,
    existing_quarantine: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    current_events: list[dict[str, Any]] = []
    event_versions: list[dict[str, Any]] = []
    current_headlines: list[dict[str, Any]] = []
    headline_versions: list[dict[str, Any]] = []

    if not source_events.empty:
        source_event_records = source_events.to_dict(orient="records")
        grouped_events: dict[str, list[dict[str, Any]]] = {}
        for row in source_event_records:
            grouped_events.setdefault(_event_identity(row), []).append(row)
        for rows in grouped_events.values():
            latest_state, versions = _append_versions(
                rows,
                state_builder=_event_state_from_rows,
                version_kind_builder=_event_version_kind,
                id_field="event_id",
                version_columns=constants.EVENT_BASE_COLUMNS,
            )
            if latest_state is not None:
                current_events.append(latest_state)
            event_versions.extend(versions)

    if not source_headlines.empty:
        source_headline_records = source_headlines.to_dict(orient="records")
        grouped_headlines: dict[str, list[dict[str, Any]]] = {}
        for row in source_headline_records:
            grouped_headlines.setdefault(_headline_identity(row), []).append(row)
        for rows in grouped_headlines.values():
            latest_state, versions = _append_versions(
                rows,
                state_builder=_headline_state_from_rows,
                version_kind_builder=_headline_version_kind,
                id_field="headline_id",
                version_columns=constants.HEADLINE_BASE_COLUMNS,
            )
            if latest_state is not None:
                current_headlines.append(latest_state)
            headline_versions.extend(versions)

    events_frame = _ensure_frame(current_events, constants.EVENT_COLUMNS)
    event_versions_frame = _ensure_frame(event_versions, constants.EVENT_VERSION_COLUMNS)
    headlines_frame = _ensure_frame(current_headlines, constants.HEADLINE_COLUMNS)
    headline_versions_frame = _ensure_frame(headline_versions, constants.HEADLINE_VERSION_COLUMNS)
    mentions_frame = _build_mentions(events=events_frame, headlines=headlines_frame)
    quarantine_frame = (
        existing_quarantine.copy()
        if existing_quarantine is not None and not existing_quarantine.empty
        else _blank_frame(constants.QUARANTINE_COLUMNS)
    )
    return {
        "events": events_frame,
        "event_versions": event_versions_frame,
        "headlines": headlines_frame,
        "headline_versions": headline_versions_frame,
        "mentions": mentions_frame,
        "quarantine": quarantine_frame,
    }


def read_parquet_frame(raw_bytes: bytes | bytearray | None, *, columns: Sequence[str]) -> pd.DataFrame:
    if not raw_bytes:
        return _blank_frame(columns)
    try:
        frame = pd.read_parquet(BytesIO(bytes(raw_bytes)))
    except Exception:
        return _blank_frame(columns)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, list(columns)].copy()


def dedupe_source_frames(
    *,
    existing_source_events: pd.DataFrame,
    existing_source_headlines: pd.DataFrame,
    existing_quarantine: pd.DataFrame,
    new_source_events: pd.DataFrame,
    new_source_headlines: pd.DataFrame,
    new_quarantine: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source_events = pd.concat([existing_source_events, new_source_events], ignore_index=True, sort=False)
    source_headlines = pd.concat([existing_source_headlines, new_source_headlines], ignore_index=True, sort=False)
    quarantine = pd.concat([existing_quarantine, new_quarantine], ignore_index=True, sort=False)
    if not source_events.empty:
        source_events = (
            source_events.drop_duplicates(subset=["source_record_id"], keep="last")
            .sort_values(["source_name", "source_record_id"])
            .reset_index(drop=True)
        )
    if not source_headlines.empty:
        source_headlines = (
            source_headlines.drop_duplicates(subset=["source_record_id"], keep="last")
            .sort_values(["source_name", "source_record_id"])
            .reset_index(drop=True)
        )
    if not quarantine.empty:
        quarantine = (
            quarantine.drop_duplicates(subset=["source_name", "dataset_name", "record_kind", "raw_identifier", "reason"], keep="last")
            .sort_values(["source_name", "dataset_name", "raw_identifier"])
            .reset_index(drop=True)
        )
    return (
        _ensure_frame(source_events.to_dict("records"), constants.INTERNAL_SOURCE_EVENT_COLUMNS),
        _ensure_frame(source_headlines.to_dict("records"), constants.INTERNAL_SOURCE_HEADLINE_COLUMNS),
        _ensure_frame(quarantine.to_dict("records"), constants.QUARANTINE_COLUMNS),
    )
