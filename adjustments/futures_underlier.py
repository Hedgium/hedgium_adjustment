"""
NFO/BFO futures underlier resolution for Greeks.

Selects the futures contract for ``(underlying, option_expiry)`` and prices it
with the liquid-LTP vs bid/ask-mid rule. Weekly expiries (no listed FUT) use a
synthetic price interpolated from cash/index spot and the covering monthly FUT.
When no future quote exists, cash/index spot is carried with simple interest.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Optional

from market_sessions import IST
from stream.redis_writer import fetch_tick_by_token, fetch_ltps

logger = logging.getLogger(__name__)

# BS rate when underlier is a futures price (carry already in F).
FUTURES_RISK_FREE_RATE = 0.0

# Simple interest when F falls back to cash/index spot (no listed future quote).
SPOT_INTEREST_RATE = 0.065

# Refresh Kite NFO/BFO instrument dump at most this often.
_NFO_CACHE_TTL_S = 6 * 3600
_FUT_EXCHANGES = ("NFO", "BFO")

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


def _ist_today() -> date:
    return datetime.now(IST).date()


def synthetic_weekly_future_price(
    spot: float,
    monthly_price: Optional[float],
    days_to_weekly: int,
    days_to_monthly: int,
) -> Optional[float]:
    """
    Linear calendar-day interpolation onto a weekly expiry.

    Spot-to-near: ``F = spot + (F_near - spot) * days_weekly / days_near``
    Near-to-away: ``F = F_near + (F_away - F_near) * days_weekly / days_span``
    (same helper; first two args are the two anchors).
    """
    try:
        spot_f = float(spot or 0)
    except (TypeError, ValueError):
        return None
    if spot_f <= 0 or monthly_price is None:
        return None
    try:
        monthly_f = float(monthly_price)
    except (TypeError, ValueError):
        return None
    if monthly_f <= 0:
        return None
    if int(days_to_monthly) <= 0:
        return monthly_f
    if int(days_to_weekly) <= 0:
        return spot_f
    if int(days_to_weekly) >= int(days_to_monthly):
        return monthly_f
    return spot_f + (monthly_f - spot_f) * (int(days_to_weekly) / int(days_to_monthly))


def spot_with_interest(
    spot: float,
    days_to_expiry: int,
    rate: float = SPOT_INTEREST_RATE,
) -> Optional[float]:
    """
    Cash/index spot plus simple interest to option expiry.

    ``F = spot * (1 + r * days / 365)``. Expiry day (days ≤ 0) returns spot.
    """
    try:
        spot_f = float(spot or 0)
        rate_f = float(rate)
    except (TypeError, ValueError):
        return None
    if spot_f <= 0:
        return None
    days = int(days_to_expiry)
    if days <= 0:
        return spot_f
    return spot_f * (1.0 + rate_f * days / 365.0)


def _spot_interest_underlier(
    underlying: str,
    option_expiry: date | str,
    spot: float,
) -> tuple[Optional[float], str]:
    today = _ist_today()
    exp = _parse_expiry(option_expiry)
    days = (exp - today).days if exp is not None else 0
    price = spot_with_interest(spot, days)
    if price is None:
        return None, "none"
    # logger.info(
    #     "[spot-interest] underlying=%s option_expiry=%s spot=%.2f days=%s r=%.4f F=%.2f",
    #     underlying,
    #     exp,
    #     float(spot),
    #     days,
    #     SPOT_INTEREST_RATE,
    #     price,
    # )
    return price, "spot_interest"


def refresh_nfo_futures(api_key: str, access_token: str, *, force: bool = False) -> int:
    """
    Load / refresh in-memory NFO and BFO FUT instruments from Kite.

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
    except Exception as exc:
        logger.warning("futures_underlier: KiteConnect init failed: %s", exc)
        with _lock:
            return len(_nfo_futs)

    futs: list[dict] = []
    fetched_any = False
    for exchange in _FUT_EXCHANGES:
        try:
            rows = kite.instruments(exchange) or []
        except Exception as exc:
            logger.warning("futures_underlier: instruments(%s) failed: %s", exchange, exc)
            continue
        fetched_any = True
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
                "exchange": exchange,
            })

    if not fetched_any:
        with _lock:
            return len(_nfo_futs)

    with _lock:
        _nfo_futs = futs
        _nfo_loaded_at = time.monotonic()

    logger.info(
        "futures_underlier: cached %s NFO/BFO FUT instruments",
        len(futs),
    )
    return len(futs)


def _cached_futs() -> list[dict]:
    with _lock:
        return list(_nfo_futs)


def resolve_future(
    underlying: str,
    option_expiry: date | str,
) -> Optional[dict]:
    """
    Pick NFO/BFO future for ``underlying`` matching ``option_expiry``.

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
    Pick nearest NFO/BFO FUT for ``underlying`` with expiry >= as_of (default: today).
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


def resolve_away_month_future(
    underlying: str,
    near_expiry: date | str,
) -> Optional[dict]:
    """Pick the next NFO/BFO FUT for ``underlying`` with expiry strictly after ``near_expiry``."""
    u = (underlying or "").strip().upper()
    exp = _parse_expiry(near_expiry)
    if not u or exp is None:
        return None
    futs = [f for f in _cached_futs() if f["name"] == u and f["expiry"] > exp]
    if not futs:
        return None
    chosen = sorted(futs, key=lambda f: (f["expiry"], f["tradingsymbol"]))[0]
    return {**chosen, "match_kind": "away_month"}


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

    Monthly expiries (exact FUT match) use the listed quote.
    Weeklies before the near-month FUT: synthetic from spot and near-month.
    Weeklies between near-month and away-month: synthetic from those two FUTs.

    Returns ``(price, fut_meta, quote_source)``.
    Falls back to cash/index spot plus simple interest when futures
    LTP/book are unavailable.
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
            price, source = _spot_interest_underlier(
                underlying, option_expiry, cash_spot,
            )
            return price, None, source
        return None, None, "none"

    price, source = get_future_price(
        r, credentials, fut["instrument_token"], spot=cash_spot,
    )
    if source == "spot":
        carried, src = _spot_interest_underlier(
            underlying, option_expiry, cash_spot if cash_spot > 0 else float(price or 0),
        )
        return carried, fut, src
    if fut.get("match_kind") == "exact":
        return price, fut, source

    today = _ist_today()
    opt_exp = _parse_expiry(option_expiry)
    near = resolve_near_month_future(underlying, as_of=today)
    away = None
    if near:
        away = resolve_away_month_future(underlying, near["expiry"])

    between_two = (
        near is not None
        and away is not None
        and opt_exp is not None
        and near["expiry"] < opt_exp <= away["expiry"]
        and int(near["instrument_token"]) != int(away["instrument_token"])
    )

    if between_two:
        p_near, src_near = get_future_price(
            r, credentials, int(near["instrument_token"]), spot=cash_spot,
        )
        p_away = price
        if (
            src_near != "spot"
            and p_near is not None
            and float(p_near) > 0
            and p_away is not None
            and float(p_away) > 0
        ):
            d_weekly = (opt_exp - near["expiry"]).days
            d_span = (away["expiry"] - near["expiry"]).days
            synth = synthetic_weekly_future_price(
                float(p_near), float(p_away), d_weekly, d_span,
            )
            if synth is not None:
                # logger.info(
                #     "[synthetic-F] underlying=%s option_expiry=%s "
                #     "near=%s away=%s F_near=%.2f F_away=%.2f "
                #     "d_weekly=%s d_span=%s F=%.2f",
                #     underlying,
                #     opt_exp,
                #     near.get("tradingsymbol"),
                #     away.get("tradingsymbol"),
                #     float(p_near),
                #     float(p_away),
                #     d_weekly,
                #     d_span,
                #     synth,
                # )
                return synth, away, "synthetic"

    if cash_spot > 0 and price is not None and float(price) > 0:
        monthly_exp = _parse_expiry(fut.get("expiry"))
        days_to_weekly = (opt_exp - today).days if opt_exp is not None else 0
        days_to_monthly = (monthly_exp - today).days if monthly_exp is not None else 0
        synth = synthetic_weekly_future_price(
            cash_spot, price, days_to_weekly, days_to_monthly,
        )
        if synth is not None:
            # logger.info(
            #     "[synthetic-F] underlying=%s option_expiry=%s covering=%s "
            #     "spot=%.2f monthly_F=%.2f d_weekly=%s d_monthly=%s F=%.2f",
            #     underlying,
            #     opt_exp,
            #     fut.get("tradingsymbol") or monthly_exp,
            #     cash_spot,
            #     float(price),
            #     days_to_weekly,
            #     days_to_monthly,
            #     synth,
            # )
            return synth, fut, "synthetic"

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
        near = resolve_near_month_future(underlying)
        if near:
            tokens.add(int(near["instrument_token"]))
            away = resolve_away_month_future(underlying, near["expiry"])
            if away:
                tokens.add(int(away["instrument_token"]))
    return sorted(tokens)
