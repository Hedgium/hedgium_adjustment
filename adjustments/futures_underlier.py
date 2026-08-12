"""
NFO futures underlier resolution for Greeks.

Selects the futures contract for ``(underlying, option_expiry)`` and prices it
with the liquid-LTP vs bid/ask-mid rule.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Optional

from stream.redis_writer import fetch_tick_by_token, fetch_ltps

logger = logging.getLogger(__name__)

# BS rate when underlier is a futures price (carry already in F).
FUTURES_RISK_FREE_RATE = 0.0

# Refresh Kite NFO instrument dump at most this often.
_NFO_CACHE_TTL_S = 6 * 3600

_lock = threading.Lock()
_nfo_futs: list[dict] = []
_nfo_loaded_at: float = 0.0


def future_price_from_quote(
    bid: float,
    ask: float,
    ltp: float,
    spot: float = 0.0,
) -> tuple[Optional[float], str]:
    """
    Pick futures underlier price from a quote.

    Returns ``(price, source)`` where source is:
      - ``ltp``          — liquid (LTP inside live bid/ask)
      - ``mid``          — bid/ask mid when LTP not liquid
      - ``ltp_fallback`` — LTP only (no usable book)
      - ``spot``         — cash/index spot as last resort
      - ``none``         — no usable price
    """
    try:
        bid_f = float(bid or 0)
        ask_f = float(ask or 0)
        ltp_f = float(ltp or 0)
        spot_f = float(spot or 0)
    except (TypeError, ValueError):
        return None, "none"

    # Liquid future: last trade inside the live book
    if ltp_f > 0 and bid_f > 0 and ask_f > 0 and bid_f <= ltp_f <= ask_f:
        return ltp_f, "ltp"
    if bid_f > 0 and ask_f > 0:
        return (bid_f + ask_f) / 2.0, "mid"
    if ltp_f > 0:
        return ltp_f, "ltp_fallback"
    if spot_f > 0:
        return spot_f, "spot"
    return None, "none"


def _cash_spot_from_redis(r, underlying: str) -> float:
    """Cash/index LTP for ``underlying`` from Redis (tick, then LTP hash)."""
    if r is None:
        return 0.0
    from stream.redis_writer import resolve_underlying_zerodha_token

    u = (underlying or "").strip().upper()
    if not u:
        return 0.0
    utok = resolve_underlying_zerodha_token(r, u)
    if not utok:
        return 0.0
    tick = fetch_tick_by_token(r, utok)
    if tick:
        try:
            lp = float(tick.get("last_price") or 0)
            if lp > 0:
                return lp
        except (TypeError, ValueError):
            pass
    ltps = fetch_ltps(r)
    try:
        return float(ltps.get(utok) or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_expiry(raw) -> Optional[date]:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    try:
        return date.fromisoformat(str(raw)[:10])
    except (TypeError, ValueError):
        return None


def refresh_nfo_futures(api_key: str, access_token: str, *, force: bool = False) -> int:
    """
    Load / refresh in-memory NFO FUT instruments from Kite.

    Returns the number of FUT rows cached.
    """
    global _nfo_futs, _nfo_loaded_at

    with _lock:
        age = time.monotonic() - _nfo_loaded_at if _nfo_loaded_at else 1e18
        if not force and _nfo_futs and age < _NFO_CACHE_TTL_S:
            return len(_nfo_futs)

    if not api_key or not access_token:
        return len(_nfo_futs)

    try:
        from kiteconnect import KiteConnect

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        rows = kite.instruments("NFO") or []
    except Exception as exc:
        logger.warning("futures_underlier: instruments(NFO) failed: %s", exc)
        with _lock:
            return len(_nfo_futs)

    futs: list[dict] = []
    for row in rows:
        if (row.get("instrument_type") or "").upper() != "FUT":
            continue
        name = (row.get("name") or "").strip().upper()
        tok = row.get("instrument_token")
        exp = _parse_expiry(row.get("expiry"))
        if not name or tok is None or exp is None:
            continue
        futs.append({
            "name": name,
            "instrument_token": int(tok),
            "tradingsymbol": row.get("tradingsymbol") or "",
            "expiry": exp,
        })

    with _lock:
        _nfo_futs = futs
        _nfo_loaded_at = time.monotonic()

    logger.info("futures_underlier: cached %s NFO FUT instruments", len(futs))
    return len(futs)


def _cached_futs() -> list[dict]:
    with _lock:
        return list(_nfo_futs)


def resolve_future(
    underlying: str,
    option_expiry: date | str,
) -> Optional[dict]:
    """
    Pick NFO future for ``underlying`` matching ``option_expiry``.

    Preference:
      1. exact expiry match
      2. nearest expiry >= option_expiry
      3. nearest overall (fallback)
    """
    u = (underlying or "").strip().upper()
    exp = _parse_expiry(option_expiry)
    if not u or exp is None:
        return None

    futs = [f for f in _cached_futs() if f["name"] == u]
    if not futs:
        return None

    exact = [f for f in futs if f["expiry"] == exp]
    if exact:
        chosen = sorted(exact, key=lambda f: f["tradingsymbol"])[0]
        return {**chosen, "match_kind": "exact"}

    on_or_after = [f for f in futs if f["expiry"] >= exp]
    if on_or_after:
        chosen = sorted(on_or_after, key=lambda f: f["expiry"])[0]
        return {**chosen, "match_kind": "next"}

    chosen = sorted(futs, key=lambda f: f["expiry"])[-1]
    return {**chosen, "match_kind": "fallback"}


def resolve_near_month_future(
    underlying: str,
    *,
    as_of: date | str | None = None,
) -> Optional[dict]:
    """
    Pick nearest NFO FUT for ``underlying`` with expiry >= as_of (default: today).
    """
    u = (underlying or "").strip().upper()
    if not u:
        return None
    if as_of is None:
        today = date.today()
    else:
        today = _parse_expiry(as_of)
        if today is None:
            return None

    futs = [f for f in _cached_futs() if f["name"] == u and f["expiry"] >= today]
    if not futs:
        return None
    chosen = sorted(futs, key=lambda f: (f["expiry"], f["tradingsymbol"]))[0]
    return {**chosen, "match_kind": "near_month"}


def get_future_price(
    r,
    credentials: Optional[dict],
    fut_token: int,
    spot: float = 0.0,
) -> tuple[Optional[float], str]:
    """
    Resolve futures price for ``fut_token``.

    Redis tick first; else Kite full quote when credentials are provided.
    ``spot`` (cash/index LTP) is used only when futures quote has no LTP/book.
    """
    tok = int(fut_token)
    tick = fetch_tick_by_token(r, tok) if r is not None else None
    if tick:
        bid = float(tick.get("bid_price") or 0)
        ask = float(tick.get("ask_price") or 0)
        ltp = float(tick.get("last_price") or 0)
        price, source = future_price_from_quote(bid, ask, ltp, spot=spot)
        if price is not None:
            return price, source
        # Redis may only have LTP hash seed for the future token
        ltps = fetch_ltps(r)
        seed = float(ltps.get(tok) or 0)
        if seed > 0:
            return seed, "ltp_fallback"

    if credentials:
        api_key = credentials.get("api_key") or ""
        access_token = credentials.get("access_token") or ""
        if api_key and access_token:
            from stream.token_fetcher import fetch_quotes_from_kite

            quotes = fetch_quotes_from_kite(api_key, access_token, [tok])
            q = quotes.get(tok)
            if q:
                return future_price_from_quote(
                    float(q.get("bid_price") or 0),
                    float(q.get("ask_price") or 0),
                    float(q.get("last_price") or 0),
                    spot=spot,
                )

    if float(spot or 0) > 0:
        return float(spot), "spot"
    return None, "none"


def get_future_price_for_option(
    r,
    credentials: Optional[dict],
    underlying: str,
    option_expiry: date | str,
) -> tuple[Optional[float], Optional[dict], str]:
    """
    Resolve FUT contract + price for an option underlier/expiry.

    Returns ``(price, fut_meta, quote_source)``.
    Falls back to cash/index spot when futures LTP/book are unavailable.
    """
    if credentials:
        refresh_nfo_futures(
            credentials.get("api_key") or "",
            credentials.get("access_token") or "",
        )

    cash_spot = _cash_spot_from_redis(r, underlying)

    fut = resolve_future(underlying, option_expiry)
    if not fut:
        if cash_spot > 0:
            return cash_spot, None, "spot"
        return None, None, "none"

    price, source = get_future_price(
        r, credentials, fut["instrument_token"], spot=cash_spot,
    )
    return price, fut, source


def future_tokens_for_pairs(
    pairs: list[tuple[str, date | str]],
    credentials: Optional[dict] = None,
) -> list[int]:
    """Resolve unique FUT instrument tokens for ``(underlying, expiry)`` pairs."""
    if credentials:
        refresh_nfo_futures(
            credentials.get("api_key") or "",
            credentials.get("access_token") or "",
        )
    tokens: set[int] = set()
    for underlying, expiry in pairs:
        fut = resolve_future(underlying, expiry)
        if fut:
            tokens.add(int(fut["instrument_token"]))
    return sorted(tokens)
