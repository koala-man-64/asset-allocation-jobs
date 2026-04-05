import json
import logging
from typing import Any, AsyncIterator, List, Optional, Union

import websockets
try:
    import msgpack  # type: ignore
except ImportError:
    msgpack = None

from alpaca.config import AlpacaConfig

logger = logging.getLogger(__name__)


class AlpacaWsTransport:
    def __init__(self, config: AlpacaConfig):
        self._config = config
        self._url = config.get_trading_ws_url()
        self._api_key = config.get_api_key()
        self._api_secret = config.get_api_secret()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False

    async def connect(self):
        logger.info(f"Connecting to Alpaca WS: {self._url}")
        self._ws = await websockets.connect(self._url)
        await self._authenticate()
        logger.info("Connected and authenticated.")
        self._running = True

    async def _authenticate(self):
        if not self._ws:
            raise RuntimeError("WebSocket not connected")
        
        auth_payload = {
            "action": "auth",
            "key": self._api_key,
            "secret": self._api_secret,
        }
        await self._ws.send(json.dumps(auth_payload))
        
        response = await self._ws.recv()
        msg = self._parse_message(response)
        if isinstance(msg, dict) and msg.get("stream") == "authorization":
             if msg.get("data", {}).get("status") == "authorized":
                 return
             else:
                 raise RuntimeError(f"Auth failed: {msg}")
        # Sometimes auth response is in a list
        if isinstance(msg, list):
             for m in msg:
                 if m.get("stream") == "authorization":
                     if m.get("data", {}).get("status") == "authorized":
                         return
                     else:
                         raise RuntimeError(f"Auth failed: {m}")
        
        raise RuntimeError(f"Unexpected auth response: {msg}")

    async def subscribe(self, streams: List[str]):
        if not self._ws:
            raise RuntimeError("WebSocket not connected")
        
        payload = {
            "action": "listen",
            "data": {
                "streams": streams
            }
        }
        await self._ws.send(json.dumps(payload))
        # Wait for confirmation not strictly required here if we assume it works, 
        # but good practice to handle logging in the loop.

    async def listen(self) -> AsyncIterator[Any]:
        if not self._ws:
            raise RuntimeError("WebSocket not connected")
        
        while self._running:
            try:
                message = await self._ws.recv()
                data = self._parse_message(message)
                if isinstance(data, list):
                    for item in data:
                        yield item
                else:
                    yield data
            except websockets.ConnectionClosed:
                logger.warning("WebSocket connection closed.")
                break
            except Exception as e:
                logger.error(f"Error reading WebSocket message: {e}")
                break

    def _parse_message(self, message: Union[str, bytes]) -> Any:
        if isinstance(message, bytes):
            if msgpack:
                return msgpack.unpackb(message, raw=False)
            else:
                # Fallback if msgpack not installed, though Alpaca paper sends bytes
                try:
                    return json.loads(message.decode("utf-8"))
                except:
                    return json.loads(message) # Attempt direct load
        else:
            return json.loads(message)

    async def close(self):
        self._running = False
        if self._ws:
            await self._ws.close()
