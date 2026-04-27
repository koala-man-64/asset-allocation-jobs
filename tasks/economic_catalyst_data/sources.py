from __future__ import annotations

import xml.etree.ElementTree as ET
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

import httpx
import nasdaqdatalink

from alpha_vantage import AlphaVantageClient
from alpha_vantage.config import AlphaVantageConfig
from asset_allocation_runtime_common.market_data import core as mdc

from tasks.economic_catalyst_data.config import EconomicCatalystConfig


@dataclass(frozen=True)
class RawSourceBatch:
    source_name: str
    dataset_name: str
    fetched_at: str
    request_url: str
    payload_format: str
    payload: Any
    metadata: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": 1,
            "source_name": self.source_name,
            "dataset_name": self.dataset_name,
            "fetched_at": self.fetched_at,
            "request_url": self.request_url,
            "payload_format": self.payload_format,
            "metadata": dict(self.metadata),
            "payload": self.payload,
        }


class SourceFetchError(RuntimeError):
    pass


_SECRET_LABEL_PATTERN = r"(api[\s_-]?key|apikey|token|secret|password|authorization)"
_SECRET_TOKEN_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._~+/=-]*"
_SECRET_PROSE_TOKEN_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._~+/=-]{5,}"
_PUBLIC_CALENDAR_USER_AGENT = "AssetAllocationJobs/1.0 (+https://github.com/koala-man-64/asset-allocation-jobs)"


def redact_secret_phrases(text: str) -> str:
    redacted = re.sub(
        rf"(?i)\bauthorization\s*[:=]\s*(Bearer|Basic)\s+{_SECRET_TOKEN_PATTERN}",
        "authorization=<redacted>",
        text,
    )
    redacted = re.sub(
        rf"(?i)\b{_SECRET_LABEL_PATTERN}\s*[:=]\s*{_SECRET_TOKEN_PATTERN}",
        r"\1=<redacted>",
        redacted,
    )
    redacted = re.sub(
        rf"(?i)\b{_SECRET_LABEL_PATTERN}\s+(as|is|was)\s+['\"]{_SECRET_TOKEN_PATTERN}['\"]",
        r"\1 \2 <redacted>",
        redacted,
    )
    redacted = re.sub(
        rf"(?i)\b{_SECRET_LABEL_PATTERN}\s+(as|is|was)\s+{_SECRET_TOKEN_PATTERN}",
        r"\1 \2 <redacted>",
        redacted,
    )
    redacted = re.sub(
        rf"(?i)\b{_SECRET_LABEL_PATTERN}\s+['\"]{_SECRET_TOKEN_PATTERN}['\"]",
        r"\1 <redacted>",
        redacted,
    )
    return re.sub(
        rf"(?i)\b{_SECRET_LABEL_PATTERN}\s+{_SECRET_PROSE_TOKEN_PATTERN}",
        r"\1 <redacted>",
        redacted,
    )


def _sanitize_issue_detail(value: object, *, limit: int = 220) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return ""
    text = re.sub(r"https?://\S+", "<url>", text)
    text = redact_secret_phrases(text)
    text = re.sub(
        r"(?i)\b(api[_-]?key|apikey|token|secret|password|authorization)=([^\s&]+)",
        r"\1=<redacted>",
        text,
    )
    text = re.sub(r"(?i)(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", r"\1 <redacted>", text)
    text = " ".join(text.split())
    if len(text) > limit:
        return text[:limit].rstrip() + "..."
    return text


def _source_failure_message(source_name: str, exc: BaseException) -> str:
    detail = _sanitize_issue_detail(exc)
    if detail:
        return f"{source_name}: {type(exc).__name__}: {detail}"
    return f"{source_name}: {type(exc).__name__}"


def _utc_now(now: datetime | None = None) -> datetime:
    return now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _http_client(timeout_seconds: float) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(timeout_seconds), follow_redirects=True, trust_env=False)


def _public_calendar_headers(accept: str) -> dict[str, str]:
    return {
        "User-Agent": _PUBLIC_CALENDAR_USER_AGENT,
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
    }


def _json_batch(
    *,
    source_name: str,
    dataset_name: str,
    url: str,
    payload: Any,
    fetched_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> RawSourceBatch:
    return RawSourceBatch(
        source_name=source_name,
        dataset_name=dataset_name,
        fetched_at=_iso(fetched_at),
        request_url=url,
        payload_format="json",
        payload=payload,
        metadata=dict(metadata or {}),
    )


def _text_batch(
    *,
    source_name: str,
    dataset_name: str,
    url: str,
    payload: str,
    fetched_at: datetime,
    payload_format: str,
    metadata: dict[str, Any] | None = None,
) -> RawSourceBatch:
    return RawSourceBatch(
        source_name=source_name,
        dataset_name=dataset_name,
        fetched_at=_iso(fetched_at),
        request_url=url,
        payload_format=payload_format,
        payload=str(payload or ""),
        metadata=dict(metadata or {}),
    )


def _request_json(
    client: httpx.Client,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> Any:
    response = client.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def _request_text(
    client: httpx.Client,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> str:
    response = client.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.text


def _fred_release_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    if not config.fred_api_key:
        return []
    url = "https://api.stlouisfed.org/fred/releases/dates"
    payload = _request_json(
        _http_client(config.http_timeout_seconds),
        url,
        params={
            "api_key": config.fred_api_key,
            "file_type": "json",
            "realtime_start": config.structured_window_start(now=now).date().isoformat(),
            "realtime_end": config.future_window_end(now=now).date().isoformat(),
            "include_release_dates_with_no_data": "true",
            "limit": 1000,
            "sort_order": "asc",
        },
    )
    return [_json_batch(source_name="fred_releases", dataset_name="release_dates", url=url, payload=payload, fetched_at=now)]


def _bls_release_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    text = _request_text(
        _http_client(config.http_timeout_seconds),
        config.bls_ics_url,
        headers=_public_calendar_headers("text/calendar, text/plain, */*"),
    )
    return [
        _text_batch(
            source_name="bls_release_calendar",
            dataset_name="release_calendar",
            url=config.bls_ics_url,
            payload=text,
            fetched_at=now,
            payload_format="ics",
        )
    ]


def _bea_schedule_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    text = _request_text(
        _http_client(config.http_timeout_seconds),
        config.bea_schedule_url,
        headers=_public_calendar_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    )
    return [
        _text_batch(
            source_name="bea_release_schedule",
            dataset_name="release_schedule",
            url=config.bea_schedule_url,
            payload=text,
            fetched_at=now,
            payload_format="html",
        )
    ]


def _fomc_schedule_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    client = _http_client(config.http_timeout_seconds)
    batches: list[RawSourceBatch] = []
    for index, url in enumerate(config.fomc_schedule_urls, start=1):
        text = _request_text(
            client,
            url,
            headers=_public_calendar_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        )
        batches.append(
            _text_batch(
                source_name="fomc_schedule",
                dataset_name=f"schedule_{index}",
                url=url,
                payload=text,
                fetched_at=now,
                payload_format="html",
            )
        )
    return batches


def _ecb_schedule_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    text = _request_text(
        _http_client(config.http_timeout_seconds),
        config.ecb_calendar_url,
        headers=_public_calendar_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    )
    return [
        _text_batch(
            source_name="ecb_policy_calendar",
            dataset_name="meeting_calendar",
            url=config.ecb_calendar_url,
            payload=text,
            fetched_at=now,
            payload_format="html",
        )
    ]


def _boe_schedule_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    text = _request_text(
        _http_client(config.http_timeout_seconds),
        config.boe_calendar_url,
        headers=_public_calendar_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    )
    return [
        _text_batch(
            source_name="boe_mpc_calendar",
            dataset_name="mpc_dates",
            url=config.boe_calendar_url,
            payload=text,
            fetched_at=now,
            payload_format="html",
        )
    ]


def _boj_schedule_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    text = _request_text(
        _http_client(config.http_timeout_seconds),
        config.boj_schedule_url,
        headers=_public_calendar_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    )
    return [
        _text_batch(
            source_name="boj_release_schedule",
            dataset_name="release_schedule",
            url=config.boj_schedule_url,
            payload=text,
            fetched_at=now,
            payload_format="html",
        )
    ]


def _treasury_auction_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    text = _request_text(
        _http_client(config.http_timeout_seconds),
        config.treasury_auctions_url,
        headers=_public_calendar_headers("application/xml,text/xml,text/html;q=0.9,*/*;q=0.8"),
    )
    payload_format = "xml"
    try:
        ET.fromstring(text)
    except Exception:
        payload_format = "html"
    return [
        _text_batch(
            source_name="treasury_auction_schedule",
            dataset_name="upcoming_auctions",
            url=config.treasury_auctions_url,
            payload=text,
            fetched_at=now,
            payload_format=payload_format,
        )
    ]


def _alpha_vantage_news_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    if not config.alpha_vantage_api_key:
        return []
    client = AlphaVantageClient(
        AlphaVantageConfig(
            api_key=config.alpha_vantage_api_key,
            rate_limit_per_min=5,
            max_workers=1,
            timeout=config.http_timeout_seconds,
        )
    )
    try:
        payload = client.fetch(
            "NEWS_SENTIMENT",
            None,
            topics=config.alpha_vantage_news_topics,
            time_from=config.headline_window_start(now=now).strftime("%Y%m%dT%H%M"),
            sort="LATEST",
            limit=1000,
        )
    finally:
        client.close()
    return [
        _json_batch(
            source_name="alpha_vantage_news",
            dataset_name="news_sentiment",
            url="https://www.alphavantage.co/query",
            payload=payload,
            fetched_at=now,
            metadata={"topics": config.alpha_vantage_news_topics},
        )
    ]


def _massive_news_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    if not config.massive_api_key:
        return []
    url = f"{config.massive_base_url.rstrip('/')}/benzinga/v2/news"
    payload = _request_json(
        _http_client(config.http_timeout_seconds),
        url,
        headers={"Authorization": f"Bearer {config.massive_api_key}"},
        params={
            "published.gte": _iso(config.headline_window_start(now=now)),
            "limit": 1000,
            "sort": "published.desc",
        },
    )
    return [_json_batch(source_name="massive_news", dataset_name="benzinga_news", url=url, payload=payload, fetched_at=now)]


def _alpaca_news_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    if not config.alpaca_key_id or not config.alpaca_secret_key:
        return []
    url = f"{config.alpaca_news_base_url.rstrip('/')}/news"
    payload = _request_json(
        _http_client(config.http_timeout_seconds),
        url,
        headers={
            "APCA-API-KEY-ID": config.alpaca_key_id,
            "APCA-API-SECRET-KEY": config.alpaca_secret_key,
        },
        params={
            "start": _iso(config.headline_window_start(now=now)),
            "end": _iso(now),
            "sort": "desc",
            "limit": 1000,
        },
    )
    return [_json_batch(source_name="alpaca_news", dataset_name="historical_news", url=url, payload=payload, fetched_at=now)]


def _nasdaq_table_batches(config: EconomicCatalystConfig, *, now: datetime) -> list[RawSourceBatch]:
    if not config.nasdaq_api_key or not config.nasdaq_table_configs:
        return []
    nasdaqdatalink.ApiConfig.api_key = config.nasdaq_api_key
    batches: list[RawSourceBatch] = []
    start_date = config.structured_window_start(now=now).date().isoformat()
    end_date = config.future_window_end(now=now).date().isoformat()
    for table_config in config.nasdaq_table_configs:
        kwargs: dict[str, Any] = dict(table_config.filters)
        if table_config.date_column:
            kwargs[table_config.date_column] = {"gte": start_date, "lte": end_date}
        frame = nasdaqdatalink.get_table(table_config.table, paginate=True, **kwargs)
        payload = {
            "rows": frame.to_dict(orient="records"),
            "mapping": {
                "table": table_config.table,
                "dataset_name": table_config.dataset_name,
                "date_column": table_config.date_column,
                "title_column": table_config.title_column,
                "effective_at_column": table_config.effective_at_column,
                "published_at_column": table_config.published_at_column,
                "updated_at_column": table_config.updated_at_column,
                "country_column": table_config.country_column,
                "region_column": table_config.region_column,
                "currency_column": table_config.currency_column,
                "actual_column": table_config.actual_column,
                "consensus_column": table_config.consensus_column,
                "previous_column": table_config.previous_column,
                "revised_previous_column": table_config.revised_previous_column,
                "unit_column": table_config.unit_column,
                "period_column": table_config.period_column,
                "frequency_column": table_config.frequency_column,
                "status_column": table_config.status_column,
                "importance_column": table_config.importance_column,
                "event_type": table_config.event_type,
                "static_values": table_config.static_values,
            },
        }
        url = f"https://data.nasdaq.com/api/v3/datatables/{table_config.table}.json"
        batches.append(
            _json_batch(
                source_name="nasdaq_tables",
                dataset_name=table_config.dataset_name,
                url=url,
                payload=payload,
                fetched_at=now,
                metadata={"table": table_config.table, "filters": kwargs},
            )
        )
    return batches


_FETCHERS: dict[str, Callable[[EconomicCatalystConfig, datetime], list[RawSourceBatch]]] = {
    "fred_releases": lambda config, now: _fred_release_batches(config, now=now),
    "bls_release_calendar": lambda config, now: _bls_release_batches(config, now=now),
    "bea_release_schedule": lambda config, now: _bea_schedule_batches(config, now=now),
    "fomc_schedule": lambda config, now: _fomc_schedule_batches(config, now=now),
    "ecb_policy_calendar": lambda config, now: _ecb_schedule_batches(config, now=now),
    "boe_mpc_calendar": lambda config, now: _boe_schedule_batches(config, now=now),
    "boj_release_schedule": lambda config, now: _boj_schedule_batches(config, now=now),
    "treasury_auction_schedule": lambda config, now: _treasury_auction_batches(config, now=now),
    "nasdaq_tables": lambda config, now: _nasdaq_table_batches(config, now=now),
    "massive_news": lambda config, now: _massive_news_batches(config, now=now),
    "alpaca_news": lambda config, now: _alpaca_news_batches(config, now=now),
    "alpha_vantage_news": lambda config, now: _alpha_vantage_news_batches(config, now=now),
}


def fetch_requested_sources(
    config: EconomicCatalystConfig,
    *,
    now: datetime | None = None,
    source_names: Sequence[str] | None = None,
) -> tuple[list[RawSourceBatch], list[str], list[str]]:
    anchor = _utc_now(now)
    batches: list[RawSourceBatch] = []
    warnings: list[str] = []
    failures: list[str] = []
    missing = config.missing_credentials()
    requested_sources = tuple(
        source_name
        for source_name in (source_names or config.enabled_sources())
        if source_name in config.enabled_sources()
    )
    for source_name, message in missing.items():
        if source_names is not None and source_name not in source_names:
            continue
        warnings.append(f"{source_name}: {message}")
    for source_name in requested_sources:
        fetcher = _FETCHERS.get(source_name)
        if fetcher is None:
            warnings.append(f"{source_name}: no fetcher is registered.")
            continue
        try:
            source_batches = fetcher(config, anchor)
            batches.extend(source_batches)
            mdc.write_line(
                f"economic_catalyst_source_fetch source={source_name} status=ok batches={len(source_batches)}"
            )
        except Exception as exc:
            message = _source_failure_message(source_name, exc)
            failures.append(message)
            mdc.write_warning(f"Economic catalyst source fetch failed: {message}")
    return batches, warnings, failures
