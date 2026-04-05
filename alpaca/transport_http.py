import logging
import time
from typing import Any, Dict, Optional

import httpx
from alpaca.config import AlpacaConfig

logger = logging.getLogger(__name__)


class AlpacaHttpTransport:
    def __init__(self, config: AlpacaConfig):
        self._config = config
        self._base_url = config.get_trading_base_url()
        self._headers = {
            "APCA-API-KEY-ID": config.get_api_key(),
            "APCA-API-SECRET-KEY": config.get_api_secret(),
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(
            base_url=self._base_url,
            headers=self._headers,
            timeout=config.http.timeout_s,
        )

    def close(self):
        self._client.close()

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute HTTP request with retries and error handling.
        """
        url = endpoint
        retries = self._config.http.max_retries
        backoff = self._config.http.backoff_base_s

        for attempt in range(retries + 1):
            try:
                response = self._client.request(
                    method,
                    url,
                    params=params,
                    json=json_data,
                )
                response.raise_for_status()
                # Handle empty responses (like cancels)
                if response.status_code == 204:
                    return {}
                return response.json()

            except httpx.HTTPStatusError as e:
                # 4xx errors are generally client errors, don't retry unless 429
                if e.response.status_code == 429:
                    logger.warning(f"Rate limited on {method} {url}. Retrying in {backoff}s...")
                elif 400 <= e.response.status_code < 500:
                    logger.error(f"Client Error {e.response.status_code} on {method} {url}: {e.response.text}")
                    raise
                else:
                    logger.warning(f"Server Error {e.response.status_code} on {method} {url}. Retrying in {backoff}s...")
                
                if attempt == retries:
                    raise

            except httpx.RequestError as e:
                logger.warning(f"Network error on {method} {url}: {e}. Retrying in {backoff}s...")
                if attempt == retries:
                    raise

            time.sleep(backoff)
            backoff *= 2.0  # Exponential backoff

        raise RuntimeError("Unreachable code")

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("POST", endpoint, json_data=json_data)

    def delete(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("DELETE", endpoint, params=params)

    def put(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("PUT", endpoint, json_data=json_data)

    def patch(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("PATCH", endpoint, json_data=json_data)
