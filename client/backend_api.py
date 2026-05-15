"""
HTTP client for the Django backend internal APIs.

All calls use ``Authorization: Bearer <SERVICE_TOKEN>`` and retry on
transient failures (5xx, connection errors) with exponential backoff.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

import config as cfg

logger = logging.getLogger(__name__)

_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({
            "Authorization": f"Bearer {cfg.SERVICE_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        _SESSION = s
    return _SESSION


def _url(path: str) -> str:
    base = cfg.BACKEND_API_URL.rstrip("/")
    return f"{base}/internal/{path.lstrip('/')}"


def _request(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json: Any = None,
) -> dict:
    """
    Execute an HTTP request with retry on transient errors.
    Raises ``RuntimeError`` on persistent failure.
    """
    url = _url(path)
    attempts = max(1, cfg.BACKEND_RETRY_ATTEMPTS)
    delay = cfg.BACKEND_RETRY_BASE_DELAY_S

    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = _session().request(
                method,
                url,
                params=params,
                json=json,
                timeout=cfg.BACKEND_HTTP_TIMEOUT,
            )
            if resp.status_code < 500:
                # 4xx — don't retry; caller decides how to handle
                if resp.status_code >= 400:
                    try:
                        body = resp.json()
                    except Exception:
                        body = {"_raw": resp.text}
                    logger.warning(
                        "backend_api: %s %s → %s: %s",
                        method, url, resp.status_code, body,
                    )
                    return {**body, "_status": resp.status_code}
                try:
                    return resp.json()
                except Exception:
                    return {"_raw": resp.text, "_status": resp.status_code}
            logger.warning(
                "backend_api: %s %s → %s (attempt %s/%s)",
                method, url, resp.status_code, attempt, attempts,
            )
            last_exc = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "backend_api: %s %s connection error (attempt %s/%s): %s",
                method, url, attempt, attempts, exc,
            )
            last_exc = exc

        if attempt < attempts:
            time.sleep(delay)
            delay = min(delay * 2, 30.0)

    raise RuntimeError(
        f"backend_api: {method} {url} failed after {attempts} attempts: {last_exc}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Public helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_credentials() -> dict:
    """
    Fetch Kite broker credentials from the backend.

    Returns::

        {"api_key": str, "api_secret": str, "access_token": str}
    """
    return _request("GET", "stream/credentials")


def get_stream_config(mode: str = "auto") -> dict:
    """
    Fetch token subscription config.

    Returns::

        {
          "legs": [...],
          "underlying_tokens": [int, ...],
          "underlying_token_by_symbol": {...},
          "option_chains": [...],
          "auto_strike_distance_buffer": int
        }
    """
    return _request("GET", "stream/config", params={"mode": mode})


def post_greeks_bulk_upsert(greeks: list[dict]) -> dict:
    """
    Bulk-upsert OptionChain Greek fields on the backend.

    Each item in ``greeks``::

        {
          "zerodha_instrument_token": int,
          "greeks_delta": float | None,
          "greeks_gamma": float | None,
          "greeks_vega":  float | None,
          "greeks_theta": float | None,
        }

    MANUAL rows are never overwritten by the backend.
    """
    return _request("POST", "greeks/bulk-upsert", json={"greeks": greeks})


def post_atm_sync(underlying_symbols: list[str] | None = None) -> dict:
    """
    Trigger ATM-sync on the backend (reads LTPs from shared Redis).
    """
    body: dict = {}
    if underlying_symbols:
        body["underlying_symbols"] = underlying_symbols
    return _request("POST", "stream/atm-sync", json=body)


def get_option_chains(
    underlying_symbols: list[str] | None = None,
    mode: str = "auto",
) -> dict:
    """
    Fetch full OptionChain rows for Greek computation.

    ``mode="auto"`` (default) — only rows for open-position expiries ±
    AUTO_STRIKE_DISTANCE_BUFFER around current positions (same filter as
    ``GET /internal/stream/config?mode=auto``).

    ``mode="full"`` — every OptionChain row for active-builder underlyings.

    Returns::

        {
          "option_chains": [
              {
                "zerodha_instrument_token": int,
                "underlying_symbol": str,
                "strike": float,
                "option_type": str,
                "expiry": str,
                "lot_size": int,
                "zerodha_tradingsymbol": str,
                "exchange": str,
                "strike_distance": int
              }, ...
          ],
          "count": int,
          "mode": str
        }
    """
    params: dict = {"mode": mode}
    if underlying_symbols:
        params["underlying_symbols"] = ",".join(underlying_symbols)
    return _request("GET", "option-chains/", params=params)


def get_live_positions_for_profile(profile_id: int) -> dict:
    """
    Fetch live broker positions for a single profile directly from the broker
    API (not the DB positions table).

    Returns the raw broker response::

        {
          "status": "success" | "error",
          "data": {
            "net": [
                {
                  "tradingsymbol": str,
                  "exchange": str,
                  "instrument_token": int,
                  "quantity": int | float,
                  "product": str,
                  "average_price": float | null,
                  "last_price": float | null,
                  ...
                }, ...
            ]
          }
        }
    """
    return _request("GET", f"positions/live/{profile_id}")


def get_live_positions() -> dict:
    """
    Fetch live broker positions for master trade-cycle profiles of all active
    builders, enriched with OptionChain metadata (fallback / legacy endpoint).

    Prefer ``get_live_positions_for_profile(profile_id)`` when you have the
    profile id from the builder data, as it calls the broker directly without
    the intermediary DB enrichment step.

    Returns::

        {
          "positions": [...],
          "count": int,
          "profiles_fetched": int
        }
    """
    return _request("GET", "positions/live")


def get_adjustment_builders() -> dict:
    """
    Fetch ACTIVE builders with legs, delta-band config, and open positions.

    Returns::

        {"builders": [...], "count": int}
    """
    return _request("GET", "adjustments/builders")


def post_adjustment_propose(strategy_id: int, adjustment_payload: dict) -> dict:
    """
    Push a delta-breach adjustment proposal to the backend.

    Returns::

        {"status": "pushed"|"skipped"|"error", "adjustment_id": int?, ...}
    """
    return _request(
        "POST",
        "adjustments/propose",
        json={
            "strategy_id": strategy_id,
            "adjustment_payload": adjustment_payload,
        },
    )


def post_adjustment_trigger(
    builder_id: int,
    strategy_id: int,
    net_delta_by_underlying: dict,
    spot_by_underlying: dict,
    book_positions: list[dict],
    net_greeks: dict | None = None,
) -> dict:
    """
    Send worker-computed Greek snapshot to the backend for full delta-band
    check + proposal generation + push.

    Returns::

        {"status": "ok"|"pushed"|"skipped"|"error", "push": {...}?, ...}
    """
    body: dict = {
        "builder_id": builder_id,
        "strategy_id": strategy_id,
        "net_delta_by_underlying": net_delta_by_underlying,
        "spot_by_underlying": spot_by_underlying,
        "book_positions": book_positions,
    }
    if net_greeks is not None:
        body["net_greeks"] = net_greeks
    return _request("POST", "adjustments/trigger", json=body)


def post_strategy_spot_snapshot(
    strategy_id: int,
    spot_by_underlying: dict[str, float],
    reason: str | None = None,
) -> dict:
    """
    Persist per-underlying StrategySpot snapshot rows for a strategy.
    """
    body: dict = {
        "strategy_id": strategy_id,
        "spot_by_underlying": spot_by_underlying,
    }
    if reason:
        body["reason"] = reason
    return _request("POST", "strategies/spot-snapshots", json=body)
