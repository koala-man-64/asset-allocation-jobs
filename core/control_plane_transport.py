from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Optional

import httpx

from core.api_gateway_auth import build_access_token_provider

logger = logging.getLogger(__name__)


class ControlPlaneRequestError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, detail: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_base_url(value: str) -> str:
    base_url = str(value or "").strip().rstrip("/")
    if base_url.lower().endswith("/api"):
        return base_url[:-4]
    return base_url


@dataclass(frozen=True)
class ControlPlaneTransportConfig:
    base_url: str
    api_scope: str
    timeout_seconds: float = 30.0


class ControlPlaneTransport:
    def __init__(
        self,
        config: ControlPlaneTransportConfig,
        *,
        http_client: httpx.Client | None = None,
        access_token_provider: Callable[[], str] | None = None,
    ) -> None:
        self.config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(config.timeout_seconds), trust_env=False)
        self._access_token_provider = access_token_provider or build_access_token_provider(config.api_scope)

    @classmethod
    def from_env(cls) -> "ControlPlaneTransport":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL"))
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for control-plane access.")
        api_scope = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_SCOPE"))
        if not api_scope:
            raise ValueError("ASSET_ALLOCATION_API_SCOPE is required for control-plane access.")
        raw_timeout = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_TIMEOUT_SECONDS")) or "30"
        try:
            timeout_seconds = max(5.0, float(raw_timeout))
        except Exception:
            timeout_seconds = 30.0
        return cls(
            ControlPlaneTransportConfig(
                base_url=_normalize_base_url(base_url),
                api_scope=api_scope,
                timeout_seconds=timeout_seconds,
            )
        )

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> "ControlPlaneTransport":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._access_token_provider()}",
            "Accept": "application/json",
        }
        caller_job = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_NAME"))
        caller_execution = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME"))
        if caller_job:
            headers["X-Caller-Job"] = caller_job
        if caller_execution:
            headers["X-Caller-Execution"] = caller_execution
        return headers

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.config.base_url}{path}"
        response = self._http.request(
            method.upper(),
            url,
            params=params,
            json=json_body,
            headers=self._build_headers(),
        )
        if response.status_code >= 400:
            detail = self._extract_detail(response)
            raise ControlPlaneRequestError(
                f"{method.upper()} {path} failed with status {response.status_code}: {detail}",
                status_code=int(response.status_code),
                detail=detail,
            )
        if not response.content:
            return None
        return response.json()

    def _extract_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            text = (response.text or "").strip()
            return text or response.reason_phrase
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()
            return json.dumps(payload, sort_keys=True)
        if isinstance(payload, str):
            return payload.strip()
        return response.reason_phrase
