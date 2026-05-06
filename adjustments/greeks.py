"""
Standalone Greek computation for the worker.

Mirrors the three-path logic of ``optionchain.greeks_service.get_greeks_for_position``:

  1. MANUAL source  — stored delta adjusted for spot move via stored gamma.
                      Never uses Black-Scholes.  Never writes to DB.
  2. AUTO baseline  — stored AUTO delta adjusted for current spot via stored gamma.
  3. BS fallback    — fresh implied-vol → Black-Scholes when no usable stored baseline.

Stored Greek data comes from the ``OptionChainStore`` which is loaded from
``GET /internal/option-chains/`` at startup and refreshed on every session reload.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import date, datetime
from typing import Optional

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
    Compute full BS Greeks using py_vollib.
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
    stored_chain: Optional[dict] = None,
) -> Optional[dict]:
    """
    Compute net Greeks for one position using the same three-path logic as
    ``optionchain.greeks_service.get_greeks_for_position``:

    Path 1 — MANUAL
        ``stored_chain["greeks_calculated_by"] == "MANUAL"`` and
        ``stored_chain["stored_delta"]`` is not None.
        Delta = (S - manual_spot) × gamma + old_delta   (spot-adjusted)
        Falls back to old_delta when manual_delta_spot is None.
        No Black-Scholes is run.

    Path 2 — AUTO baseline
        ``greeks_calculated_by == "AUTO"`` and both ``stored_delta`` and
        ``auto_delta_spot`` are available.
        Delta = (S - auto_spot) × gamma + old_delta

    Path 3 — Black-Scholes (default)
        Fresh IV calculation from the current market mid-price, then full BS.
        Used when stored_chain is None, stored_delta is None, or expiry has
        no stored baseline.

    ``stored_chain`` is a row from ``OptionChainStore.get_chain_by_token()``.
    It may be None when the store hasn't loaded the token yet.

    Returns a dict or None if data is insufficient.
    """
    if not zerodha_instrument_token or int(zerodha_instrument_token) <= 0:
        return None

    tok = int(zerodha_instrument_token)
    if quantity == 0:
        return None

    flag = "c" if str(option_type).upper() == "CE" else "p"
    t = _tte_years(expiry)
    r_rate = DEFAULT_RISK_FREE_RATE
    S = float(underlying_spot)

    # ── Market quotes from Redis ──────────────────────────────────────────────
    tick = fetch_tick_by_token(r, tok)
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

    # For MANUAL / AUTO-baseline paths we only need bid/ask for output, not for
    # delta computation.  For the BS path we need a usable mid-price.
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, ltp)

    # ── Determine calculation path ────────────────────────────────────────────
    sc = stored_chain or {}
    calc_by = (sc.get("greeks_calculated_by") or "").upper()
    stored_delta = sc.get("stored_delta")
    stored_gamma = sc.get("stored_gamma") or 0.0
    stored_vega  = sc.get("stored_vega")  or 0.0
    stored_theta = sc.get("stored_theta") or 0.0

    manual_mode = (
        calc_by == "MANUAL"
        and stored_delta is not None
    )
    auto_baseline_mode = (
        not manual_mode
        and calc_by == "AUTO"
        and stored_delta is not None
        and sc.get("auto_delta_spot") is not None
    )

    # ── Path 1: MANUAL ───────────────────────────────────────────────────────
    if manual_mode:
        manual_spot = sc.get("manual_delta_spot")
        if manual_spot is not None:
            delta_pu = (S - float(manual_spot)) * float(stored_gamma) + float(stored_delta)
        else:
            delta_pu = float(stored_delta)
        gamma_pu = float(stored_gamma)
        vega_pu  = float(stored_vega)
        theta_pu = float(stored_theta)
        iv_val   = 0.0

        logger.debug(
            "greeks[MANUAL]: token=%s spot=%.2f manual_spot=%s stored_delta=%s "
            "gamma=%s → delta_pu=%.6f",
            tok, S, manual_spot, stored_delta, stored_gamma, delta_pu,
        )

    # ── Path 2: AUTO baseline ────────────────────────────────────────────────
    elif auto_baseline_mode:
        auto_spot = float(sc["auto_delta_spot"])
        delta_pu = (S - auto_spot) * float(stored_gamma) + float(stored_delta)
        gamma_pu = float(stored_gamma)
        vega_pu  = float(stored_vega)
        theta_pu = float(stored_theta)
        iv_val   = 0.0

        logger.debug(
            "greeks[AUTO]: token=%s spot=%.2f auto_spot=%.2f stored_delta=%s "
            "gamma=%s → delta_pu=%.6f",
            tok, S, auto_spot, stored_delta, stored_gamma, delta_pu,
        )

    # ── Path 3: Black-Scholes ────────────────────────────────────────────────
    else:
        if bid <= 0 and ask <= 0:
            logger.debug(
                "greeks[BS]: no tick data token=%s instrument=%r",
                tok, instrument_label,
            )
            return None

        iv = _implied_vol(flag, mid, S, float(strike), t, r_rate)
        if iv is None or iv <= 0:
            logger.debug(
                "greeks[BS]: IV failed token=%s instrument=%r mid=%s spot=%s "
                "strike=%s t=%.4f",
                tok, instrument_label, mid, S, strike, t,
            )
            return None

        g = _bs_greeks(flag, S, float(strike), t, r_rate, iv)
        if not g:
            return None

        delta_pu = g["delta"]
        gamma_pu = g["gamma"]
        vega_pu  = g["vega"]
        theta_pu = g["theta"]
        iv_val   = g["iv"]

    # ── Build output (mirrors greeks_service.py output shape) ────────────────
    q    = quantity
    sign = 1 if q > 0 else -1 if q < 0 else 0
    aq   = abs(q)

    print(f"greeks_for_position: {instrument_label} tok:{tok} bid:{bid} ask:{ask} ltp:{ltp} mid:{mid} iv:{iv_val} net_delta:{round(delta_pu * aq * sign, 6)}")

    return {
        "instrument":              instrument_label,
        "zerodha_instrument_token": tok,
        "bid":       bid,
        "ask":       ask,
        "ltp":       ltp,
        "mid":       mid,
        "iv":        iv_val,
        "delta":     delta_pu,
        "gamma":     gamma_pu,
        "theta":     theta_pu,
        "vega":      vega_pu,
        "net_delta": round(delta_pu * aq * sign, 6),
        "net_gamma": round(gamma_pu * aq * sign, 6),
        "net_theta": round(theta_pu * aq * sign, 6),
        "net_vega":  round(vega_pu  * aq * sign, 6),
        "quantity":  q,
        "greeks_source": (
            "manual" if manual_mode
            else "auto_baseline" if auto_baseline_mode
            else "bs"
        ),
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

def compute_greeks_for_builder(
    r,
    builder_data: dict,
    option_chain_store=None,
) -> Optional[dict]:
    """
    Compute Greek snapshot for a single builder using data from the backend API.

    ``builder_data`` is one item from ``/internal/adjustments/builders``
    ``builders`` list.

    ``option_chain_store`` is an optional ``OptionChainStore`` instance.  When
    provided, stored Greek data (MANUAL / AUTO-baseline) is looked up per token
    and passed to ``get_greeks_for_position``, enabling the same three-path
    calculation as the Django backend's ``greeks_service``.

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

        # Look up stored Greeks from the option chain store (for MANUAL / AUTO paths)
        stored_chain: Optional[dict] = None
        if option_chain_store is not None:
            stored_chain = option_chain_store.get_chain_by_token(int(tok))

        greeks = get_greeks_for_position(
            r,
            zerodha_instrument_token=int(tok),
            underlying_spot=spot,
            strike=float(strike),
            option_type=option_type,
            expiry=expiry,
            quantity=int(quantity),
            instrument_label=instrument,
            stored_chain=stored_chain,
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
                "zerodha_tradingsymbol": pos.get("zerodha_tradingsymbol") or "",
                "lot_size": int(lot_size),
                "expiry": expiry_str,
                "bid": float(greeks.get("bid") or 0.0),
                "ask": float(greeks.get("ask") or 0.0),
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
