"""
Standalone Greek computation for the worker.

Reads bid/ask from Redis ticks (written by ws_stream), then computes
Black-Scholes Greeks via py_vollib — same core math as the Django
``greeks_service.get_greeks_for_position``.

Does NOT write to the DB; results are used only for delta-band checks
and then forwarded to the backend ``/internal/adjustments/trigger`` endpoint.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Optional

import config as cfg
from stream.redis_writer import fetch_tick_by_token, fetch_ltps, resolve_underlying_zerodha_token

logger = logging.getLogger(__name__)

DEFAULT_RISK_FREE_RATE = 0.065


# ──────────────────────────────────────────────────────────────────────────────
# Time helpers
# ──────────────────────────────────────────────────────────────────────────────

def _days_to_expiry(expiry: date) -> float:
    today = datetime.utcnow().date()
    delta = expiry - today
    return max(delta.days, 0)


def _tte_years(expiry: date) -> float:
    days = _days_to_expiry(expiry)
    return max(days / 365.0, 1 / 365.0)


# ──────────────────────────────────────────────────────────────────────────────
# Black-Scholes via py_vollib
# ──────────────────────────────────────────────────────────────────────────────

def _bs_greeks(
    flag: str,
    spot: float,
    strike: float,
    t: float,
    r: float,
    iv: float,
) -> dict:
    """
    Compute BS greeks using py_vollib.

    Returns dict with iv, delta, gamma, theta, vega or empty dict on failure.
    """
    try:
        from py_vollib.black_scholes import black_scholes as bs
        from py_vollib.black_scholes.greeks.analytical import (
            delta, gamma, rho, theta, vega,
        )

        price = bs(flag, spot, strike, t, r, iv)
        return {
            "bs_price": price,
            "iv": iv,
            "delta": delta(flag, spot, strike, t, r, iv),
            "gamma": gamma(flag, spot, strike, t, r, iv),
            "theta": theta(flag, spot, strike, t, r, iv),
            "vega": vega(flag, spot, strike, t, r, iv),
        }
    except Exception as exc:
        logger.debug("bs_greeks: %s", exc)
        return {}


def _implied_vol(
    flag: str,
    mid: float,
    spot: float,
    strike: float,
    t: float,
    r: float,
) -> Optional[float]:
    if mid <= 0 or spot <= 0 or strike <= 0 or t <= 0:
        return None
    try:
        from py_vollib.black_scholes.implied_volatility import implied_volatility as iv_func

        iv = iv_func(mid, spot, strike, t, r, flag)
        if math.isfinite(iv) and 0.0001 < iv < 50.0:
            return iv
    except Exception:
        pass
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Per-position Greek computation
# ──────────────────────────────────────────────────────────────────────────────

def get_greeks_for_position(
    r,
    *,
    zerodha_instrument_token: int,
    underlying_spot: float,
    strike: float,
    option_type: str,
    expiry: date,
    quantity: int,
    instrument_label: str,
) -> Optional[dict]:
    """
    Compute net Greeks for one position.

    Returns a dict or None if data is insufficient.
    """
    flag = "c" if str(option_type).upper() == "CE" else "p"
    t = _tte_years(expiry)
    r_rate = DEFAULT_RISK_FREE_RATE

    tick = fetch_tick_by_token(r, zerodha_instrument_token)
    bid: float = 0.0
    ask: float = 0.0
    ltp: float = 0.0

    if tick:
        try:
            bid = float(tick.get("bid_price") or tick.get("last_price") or 0)
            ask = float(tick.get("ask_price") or tick.get("last_price") or 0)
            ltp = float(tick.get("last_price") or 0)
        except (TypeError, ValueError):
            pass

    if bid <= 0 and ask <= 0:
        logger.debug(
            "greeks: no tick data for token=%s instrument=%r",
            zerodha_instrument_token, instrument_label,
        )
        return None

    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, ltp)

    iv = _implied_vol(flag, mid, underlying_spot, strike, t, r_rate)
    if iv is None or iv <= 0:
        logger.debug(
            "greeks: IV failed for token=%s instrument=%r mid=%s spot=%s strike=%s t=%.4f",
            zerodha_instrument_token, instrument_label, mid, underlying_spot, strike, t,
        )
        return None

    g = _bs_greeks(flag, underlying_spot, strike, t, r_rate, iv)
    if not g:
        return None

    q = quantity
    sign = 1 if q > 0 else -1 if q < 0 else 0
    abs_qty = abs(q)

    return {
        "instrument": instrument_label,
        "zerodha_instrument_token": zerodha_instrument_token,
        "bid": bid,
        "ask": ask,
        "ltp": ltp,
        "mid": mid,
        "iv": g["iv"],
        "delta": g["delta"],
        "gamma": g["gamma"],
        "theta": g["theta"],
        "vega": g["vega"],
        "net_delta": g["delta"] * abs_qty * sign,
        "net_gamma": g["gamma"] * abs_qty * sign,
        "net_theta": g["theta"] * abs_qty * sign,
        "net_vega": g["vega"] * abs_qty * sign,
        "quantity": q,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Underlying spot resolution
# ──────────────────────────────────────────────────────────────────────────────

def get_underlying_spot(r, underlying_symbol: str, leg_token: Optional[int] = None) -> float:
    """
    Resolve underlying spot from Redis.

    Priority:
    1. Live tick for the underlying token (written by ws_stream equity subscription)
    2. LTP hash (seeded at startup by token_fetcher)
    """
    utok = leg_token
    if utok is None:
        utok = resolve_underlying_zerodha_token(r, underlying_symbol)

    if utok:
        tick = fetch_tick_by_token(r, utok)
        if tick:
            lp = float(tick.get("last_price") or 0)
            if lp > 0:
                return lp

        ltps = fetch_ltps(r)
        sp = ltps.get(utok)
        if sp and sp > 0:
            return float(sp)

    return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Full builder Greek snapshot
# ──────────────────────────────────────────────────────────────────────────────

def compute_greeks_for_builder(r, builder_data: dict) -> Optional[dict]:
    """
    Compute Greek snapshot for a single builder using data from the backend API.

    ``builder_data`` is one item from ``/internal/adjustments/builders``
    ``builders`` list.

    Returns a snapshot dict or None if skipped.
    """
    positions = builder_data.get("positions") or []
    legs = builder_data.get("legs") or []
    builder_id = builder_data["builder_id"]
    strategy_id = builder_data["strategy_id"]

    if not positions:
        return None

    # Build underlying→token map from legs
    leg_token_by_symbol: dict[str, int] = {}
    for leg in legs:
        tok = leg.get("token")
        sym = (leg.get("symbol") or "").strip().upper()
        if tok and sym:
            leg_token_by_symbol[sym] = int(tok)

    spot_by_underlying: dict[str, float] = {}
    per_leg: list[dict] = []
    book_positions: list[dict] = []

    for pos in positions:
        tok = pos.get("zerodha_instrument_token")
        under = (pos.get("underlying_symbol") or "").strip().upper()
        strike = pos.get("strike")
        option_type = pos.get("option_type")
        expiry_str = pos.get("expiry")
        quantity = pos.get("quantity", 0)
        exchange = pos.get("exchange") or "NFO"
        lot_size = pos.get("lot_size") or 1
        instrument = pos.get("instrument") or ""

        if not tok or not under or not strike or not option_type or not expiry_str:
            continue

        try:
            expiry = date.fromisoformat(expiry_str)
        except (ValueError, TypeError):
            logger.warning(
                "greeks: builder_id=%s bad expiry %r for position %s",
                builder_id, expiry_str, pos.get("position_id"),
            )
            continue

        if under not in spot_by_underlying:
            utok = leg_token_by_symbol.get(under)
            spot = get_underlying_spot(r, under, utok)
            spot_by_underlying[under] = spot

        spot = spot_by_underlying.get(under, 0.0)
        if spot <= 0:
            logger.debug(
                "greeks: builder_id=%s no spot for underlying=%s — skipping position",
                builder_id, under,
            )
            continue

        greeks = get_greeks_for_position(
            r,
            zerodha_instrument_token=int(tok),
            underlying_spot=spot,
            strike=float(strike),
            option_type=option_type,
            expiry=expiry,
            quantity=int(quantity),
            instrument_label=instrument,
        )

        if greeks:
            greeks["underlying_symbol"] = under
            per_leg.append(greeks)
            book_positions.append({
                "underlying_symbol": under,
                "option_type": option_type,
                "strike": float(strike),
                "quantity": int(quantity),
                "exchange": exchange,
                "zerodha_tradingsymbol": "",
                "lot_size": lot_size,
                "expiry": expiry_str,
                "bid": greeks.get("bid"),
                "ask": greeks.get("ask"),
                "instrument_token": int(tok),
            })

    if not per_leg:
        logger.warning(
            "greeks: builder_id=%s strategy_id=%s — no legs greeked (positions=%s)",
            builder_id, strategy_id, len(positions),
        )
        return None

    # Net Greeks
    net: dict[str, float] = defaultdict(float)
    nd_by_u: dict[str, float] = defaultdict(float)
    for g in per_leg:
        for k in ("net_delta", "net_gamma", "net_theta", "net_vega"):
            net[k] += float(g.get(k) or 0.0)
        u = g.get("underlying_symbol")
        if u:
            nd_by_u[u] += float(g.get("net_delta") or 0.0)

    net_greeks = {k: round(v, 6) for k, v in net.items()}
    net_delta_by_underlying = {k: round(v, 6) for k, v in sorted(nd_by_u.items())}
    spot_out = {u: float(spot_by_underlying.get(u) or 0.0) for u in nd_by_u}

    underlyings = sorted(nd_by_u.keys())

    if cfg.GREEKS_PIPELINE_TRACE:
        logger.info(
            "[greeks-pipeline] builder_id=%s strategy_id=%s net_delta_by_u=%s per_leg=%s",
            builder_id, strategy_id, net_delta_by_underlying, len(per_leg),
        )

    return {
        "builder_id": builder_id,
        "strategy_id": strategy_id,
        "underlyings": underlyings,
        "net_greeks": net_greeks,
        "net_delta_by_underlying": net_delta_by_underlying,
        "spot_by_underlying": spot_out,
        "per_leg": per_leg,
        "book_positions": book_positions,
        "positions_open": len(positions),
        "positions_greeked": len(per_leg),
    }
