"""
Kite WebSocket streaming client for hedgium_stream_worker.

Standalone port of ``optionchain/mystream/kite_ws_stream.py`` +
``binary_quotes.py`` — no Django / ORM imports.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from datetime import date, datetime
from typing import Any, Callable, Optional, Sequence

import websocket

import config as cfg

logger = logging.getLogger(__name__)

WS_URL_TEMPLATE = "wss://ws.kite.trade?api_key={api_key}&access_token={access_token}"
STREAM_MODE = "full"
CHUNK = cfg.MAX_INSTRUMENTS_PER_WS


# ──────────────────────────────────────────────────────────────────────────────
# Binary frame parser (uses kiteconnect library)
# ──────────────────────────────────────────────────────────────────────────────

_parser_cache: Optional[tuple[str, str, Any]] = None


def _get_kite_parser(api_key: str, access_token: str):
    global _parser_cache
    if _parser_cache and _parser_cache[0] == api_key and _parser_cache[1] == access_token:
        return _parser_cache[2]
    from kiteconnect.ticker import KiteTicker

    p = KiteTicker(api_key, access_token)
    _parser_cache = (api_key, access_token, p)
    return p


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _enrich_bid_ask(tick: dict) -> dict:
    ltp = float(tick.get("last_price") or 0)
    depth = tick.get("depth") or {}
    buys = depth.get("buy") or []
    sells = depth.get("sell") or []
    try:
        if buys and sells:
            bp = buys[0].get("price")
            ap = sells[0].get("price")
            if bp is not None and ap is not None:
                tick["bid_price"] = float(bp)
                tick["ask_price"] = float(ap)
                return tick
    except (TypeError, ValueError, IndexError):
        pass
    tick.setdefault("bid_price", ltp)
    tick.setdefault("ask_price", ltp)
    return tick


def parse_binary_ticks(api_key: str, access_token: str, data: bytes) -> list[dict]:
    if not data or len(data) < 2:
        return []
    try:
        parser = _get_kite_parser(api_key, access_token)
        ticks = parser._parse_binary(data)
    except Exception as e:
        logger.exception("ws_stream: binary parse failed: %s", e)
        return []
    out = []
    for t in ticks:
        if not isinstance(t, dict):
            continue
        safe = _json_safe(t)
        if isinstance(safe, dict):
            out.append(_enrich_bid_ask(safe))
    return out


# ──────────────────────────────────────────────────────────────────────────────
# WebSocket streamer
# ──────────────────────────────────────────────────────────────────────────────

def _subscribe_payload(tokens: Sequence[int], action: str = "subscribe") -> str:
    return json.dumps({"a": action, "v": [int(t) for t in tokens]})


def _mode_payload(tokens: Sequence[int]) -> str:
    return json.dumps({"a": "mode", "v": [STREAM_MODE, [int(t) for t in tokens]]})


class KiteQuoteStreamer:
    """
    One WebSocket connection for up to ``CHUNK`` (3000) instruments.
    Auto-reconnects with exponential backoff.
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        tokens: list[int],
        on_ticks: Callable[[list[dict]], None],
        name: str = "ws0",
    ):
        self.api_key = api_key
        self.access_token = access_token
        self.url = WS_URL_TEMPLATE.format(api_key=api_key, access_token=access_token)
        self.tokens = list(tokens)
        self.on_ticks = on_ticks
        self.name = name
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._reconnect_delay_s = cfg.WS_RECONNECT_BASE_DELAY_S
        self._ping_timeout = cfg.WS_PING_TIMEOUT
        self._ping_interval = cfg.WS_PING_INTERVAL
        if self._ping_interval <= self._ping_timeout:
            self._ping_interval = self._ping_timeout + 5.0
            logger.warning(
                "%s: WS_PING_INTERVAL must be > WS_PING_TIMEOUT; using %.1f",
                name, self._ping_interval,
            )
        self._reconnect_max_s = cfg.WS_RECONNECT_MAX_DELAY_S

    def _on_open(self, ws):
        self._reconnect_delay_s = cfg.WS_RECONNECT_BASE_DELAY_S
        n = len(self.tokens)
        logger.info(
            "%s: connected — %s instruments (ping interval=%.1fs timeout=%.1fs)",
            self.name, n, self._ping_interval, self._ping_timeout,
        )
        print(
            f"{self.name}: Kite WS connected — {n} instruments "
            f"(ping {self._ping_interval:.1f}s / {self._ping_timeout:.1f}s)",
            flush=True,
        )
        for i in range(0, len(self.tokens), CHUNK):
            chunk = self.tokens[i: i + CHUNK]
            ws.send(_subscribe_payload(chunk))
            ws.send(_mode_payload(chunk))

    def _on_message(self, ws, message):
        if isinstance(message, bytes):
            ticks = parse_binary_ticks(self.api_key, self.access_token, message)
            if ticks:
                self.on_ticks(ticks)
        else:
            try:
                data = json.loads(message)
                if data.get("type") == "error":
                    logger.error("%s: kite ws error: %s", self.name, data)
            except json.JSONDecodeError:
                logger.debug("%s: non-json text: %s", self.name, message[:200])

    def _on_error(self, ws, error):
        logger.error("%s: websocket error: %s", self.name, error)

    def _on_close(self, ws, status, msg):
        logger.warning("%s: closed status=%s msg=%s", self.name, status, msg)

    def _connection_loop(self) -> None:
        while not self._stop.is_set():
            self._ws = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            try:
                self._ws.run_forever(
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                )
            except Exception as e:
                if not self._stop.is_set():
                    logger.error("%s: run_forever failed: %s", self.name, e)
            finally:
                self._ws = None
            if self._stop.is_set():
                break
            wait_s = self._reconnect_delay_s
            logger.warning(
                "%s: ended, reconnecting in %.1fs (cap %.1fs)",
                self.name, wait_s, self._reconnect_max_s,
            )
            if self._stop.wait(timeout=wait_s):
                break
            self._reconnect_delay_s = min(
                self._reconnect_max_s,
                max(wait_s * 1.5, cfg.WS_RECONNECT_BASE_DELAY_S),
            )

    def start_background(self):
        self._thread = threading.Thread(
            target=self._connection_loop, name=self.name, daemon=True
        )
        self._thread.start()

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def update_subscriptions(
        self,
        add_tokens: list[int],
        remove_tokens: list[int],
    ) -> None:
        """
        Dynamically subscribe/unsubscribe tokens on the live WebSocket connection.

        Safe to call from any thread.  If no connection is open (reconnecting)
        the token list is updated in-place and will take effect on the next
        ``_on_open`` call.
        """
        if remove_tokens:
            remove_set = set(int(t) for t in remove_tokens)
            self.tokens = [t for t in self.tokens if t not in remove_set]

        if add_tokens:
            add_set = set(int(t) for t in add_tokens)
            existing = set(self.tokens)
            new_tokens = [t for t in add_set if t not in existing]
            self.tokens.extend(new_tokens)

        ws = self._ws
        if ws is None:
            return

        try:
            if remove_tokens:
                ws.send(_subscribe_payload(remove_tokens, action="unsubscribe"))
                logger.info(
                    "%s: unsubscribed %s tokens", self.name, len(remove_tokens)
                )
            if add_tokens:
                ws.send(_subscribe_payload(add_tokens, action="subscribe"))
                ws.send(_mode_payload(add_tokens))
                logger.info(
                    "%s: subscribed %s new tokens", self.name, len(add_tokens)
                )
        except Exception as exc:
            logger.warning(
                "%s: update_subscriptions send error: %s", self.name, exc
            )

    def stop(self):
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None


def run_multi_connection_stream(
    *,
    api_key: str,
    access_token: str,
    all_tokens: list[int],
    on_ticks: Callable[[list[dict]], None],
    max_connections: int = cfg.MAX_WS_CONNECTIONS,
) -> list[KiteQuoteStreamer]:
    per = CHUNK * max_connections
    if len(all_tokens) > per:
        raise ValueError(
            f"Too many instruments ({len(all_tokens)} > {per}). "
            "Reduce active builders or strike-distance window."
        )
    n_conn = min(max_connections, max(1, math.ceil(len(all_tokens) / CHUNK)))
    streams: list[KiteQuoteStreamer] = []
    for i in range(n_conn):
        slice_toks = all_tokens[i * CHUNK: (i + 1) * CHUNK]
        if not slice_toks:
            break
        s = KiteQuoteStreamer(
            api_key, access_token, slice_toks, on_ticks, name=f"worker-ws{i}"
        )
        s.start_background()
        streams.append(s)
    return streams
