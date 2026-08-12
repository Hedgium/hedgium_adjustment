"""
Standalone Greek computation for the worker.

Uses NFO futures underlier ``F`` matched to option expiry (no dividend), with
Black-Scholes at ``r=0``:

  1. Gamma-adjusted — stored delta adjusted for futures move via stored gamma.
  2. BS fallback — fresh implied-vol → Black-Scholes when no usable stored baseline.

Stored Greek data comes from the ``OptionChainStore`` which is loaded from
``GET /internal/option-chains/`` at startup and refreshed on every session reload.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import date, datetime, timezone, datetime
from typing import Optional

import config as cfg
from adjustments.futures_underlier import (
    FUTURES_RISK_FREE_RATE,
    get_future_price,
    get_future_price_for_option,
    resolve_near_month_future,
)
from stream.redis_writer import fetch_tick_by_token, fetch_ltps, resolve_underlying_zerodha_token

logger = logging.getLogger(__name__)

# Legacy cash-spot BS rate (unused on futures underlier path).
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

def _stored_greeks_fresh(sc: dict) -> bool:
    """
    Return True if the in-memory Greeks are present and were computed within
    the last ``GREEKS_STALE_THRESHOLD_S`` seconds.
    """
    if sc.get("delta") is None or sc.get("computed_at_spot") is None:
        return False
    last_at = sc.get("last_greeks_at")
    if not last_at:
        return False
    try:
        computed_dt = datetime.fromisoformat(last_at)
        age_s = (datetime.utcnow() - computed_dt).total_seconds()
        return age_s <= cfg.GREEKS_STALE_THRESHOLD_S
    except Exception:
        return False


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
    dividend: Optional[dict] = None,
) -> Optional[dict]:
    """
    Compute net Greeks for one position for the adjustment check.

    ``underlying_spot`` is the futures underlier ``F`` (no dividend adjustment).

    Path 1 — Gamma-adjusted (in-memory baseline is fresh)
        Uses the delta/gamma/spot stored by the last ``update_greeks()`` run.
        Applies:  delta = (S - computed_at_spot) × gamma + stored_delta
        No Black-Scholes is run.

    Path 2 — Black-Scholes fallback
        Used when the in-memory baseline is absent or older than
        ``GREEKS_STALE_THRESHOLD_S`` seconds (default 5 min).
        Runs fresh IV → BS from live Redis tick with ``r=0``.

    ``stored_chain`` is a row from ``OptionChainStore.get_chain_by_token()``.
    Returns a dict or None if data is insufficient.
    """
    if not zerodha_instrument_token or int(zerodha_instrument_token) <= 0:
        return None

    tok = int(zerodha_instrument_token)
    if quantity == 0:
        return None

    # Futures underlier — ignore dividend (carry already in F).
    _ = dividend
    S = float(underlying_spot)
    if S <= 0:
        return None

    # Always fetch tick — needed for bid/ask/ltp output and BS fallback.
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
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, ltp)

    sc = stored_chain or {}

    # ── Path 1: gamma-adjusted from in-memory baseline ────────────────────────
    if _stored_greeks_fresh(sc):
        _w_delta = sc["delta"]
        _w_gamma = sc.get("gamma") or 0.0
        _w_vega  = sc.get("vega")  or 0.0
        _w_theta = sc.get("theta") or 0.0
        ref_spot = float(sc["computed_at_spot"])

        gamma_pu = float(_w_gamma)
        vega_pu  = float(_w_vega)
        theta_pu = float(_w_theta)
        delta_pu = (S - ref_spot) * gamma_pu + float(_w_delta)
        iv_val   = 0.0
        path     = "gamma_adj"

    # ── Path 2: Black-Scholes fallback (stale or missing baseline) ───────────
    else:
        age_info = ""
        last_at = sc.get("last_greeks_at")
        if last_at:
            try:
                age_s = (datetime.utcnow() - datetime.fromisoformat(last_at)).total_seconds()
                age_info = f" (age={age_s:.0f}s > threshold={cfg.GREEKS_STALE_THRESHOLD_S}s)"
            except Exception:
                pass
        logger.debug(
            "greeks[BS-fallback]: token=%s — in-memory Greeks stale/missing%s",
            tok, age_info,
        )

        if bid <= 0 and ask <= 0:
            logger.debug("greeks[BS]: no tick data token=%s instrument=%r", tok, instrument_label)
            return None

        flag  = "c" if str(option_type).upper() == "CE" else "p"
        t     = _tte_years(expiry)
        r_rate = FUTURES_RISK_FREE_RATE

        iv = _implied_vol(flag, mid, S, float(strike), t, r_rate)
        if iv is None or iv <= 0:
            logger.debug(
                "greeks[BS]: IV failed token=%s mid=%s spot=%s strike=%s t=%.4f",
                tok, mid, S, strike, t,
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
        path     = "bs_fallback"

    # ── Build output (mirrors greeks_service.py output shape) ────────────────
    q    = quantity
    sign = 1 if q > 0 else -1 if q < 0 else 0
    aq   = abs(q)

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
        "greeks_source": path,
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
    credentials: Optional[dict] = None,
) -> Optional[dict]:
    """
    Compute Greek snapshot for a single builder using data from the backend API.

    ``builder_data`` is one item from ``/internal/adjustments/builders``
    ``builders`` list.

    Underlier is the NFO futures price matched to each position's expiry
    (no dividend).  ``credentials`` enables Kite quote fallback when Redis
    has no futures tick yet.

    ``option_chain_store`` is an optional ``OptionChainStore`` instance.  When
    provided, stored Greek data (MANUAL / AUTO-baseline) is looked up per token
    and passed to ``get_greeks_for_position``.

    Returns a snapshot dict or None if skipped.
    """
    positions = builder_data.get("positions") or []
    builder_id = builder_data["builder_id"]
    strategy_id = builder_data["strategy_id"]
    master_profile_id = builder_data.get("master_profile_id")
    master_trade_cycle_id = builder_data.get("master_trade_cycle_id")

    if not positions:
        return None

    # Futures F keyed by (underlying, expiry) — used only for Greek math
    fut_by_key: dict[tuple[str, date], float] = {}
    # Cash/index LTP per underlying — persisted as greek_spot_by_underlying
    spot_by_underlying: dict[str, float] = {}
    legs = builder_data.get("legs") or []
    leg_token_by_symbol: dict[str, int] = {}
    for leg in legs:
        tok_leg = leg.get("token")
        sym = (leg.get("symbol") or "").strip().upper()
        if tok_leg and sym:
            leg_token_by_symbol[sym] = int(tok_leg)

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

        fkey = (under, expiry)
        if fkey not in fut_by_key:
            price, _fut, _src = get_future_price_for_option(
                r, credentials, under, expiry,
            )
            fut_by_key[fkey] = float(price) if price and price > 0 else 0.0

        if under not in spot_by_underlying:
            cash_spot = get_underlying_spot(r, under, leg_token_by_symbol.get(under))
            spot_by_underlying[under] = float(cash_spot) if cash_spot and cash_spot > 0 else 0.0

        fut = fut_by_key.get(fkey, 0.0)
        if fut <= 0:
            logger.debug(
                "greeks: builder_id=%s no futures price for underlying=%s expiry=%s — skipping",
                builder_id, under, expiry_str,
            )
            continue

        # Look up stored Greeks from the option chain store (for MANUAL / AUTO paths)
        stored_chain: Optional[dict] = None
        if option_chain_store is not None:
            stored_chain = option_chain_store.get_chain_by_token(int(tok))

        greeks = get_greeks_for_position(
            r,
            zerodha_instrument_token=int(tok),
            underlying_spot=fut,
            strike=float(strike),
            option_type=option_type,
            expiry=expiry,
            quantity=int(quantity),
            instrument_label=instrument,
            stored_chain=stored_chain,
            dividend=None,
        )

        if greeks is None:
            logger.warning(
                "greeks: builder_id=%s — could not compute Greeks for position "
                "token=%s instrument=%r under=%s qty=%s — aborting adjustment for this builder",
                builder_id, tok, instrument, under, quantity,
            )
            return None

        greeks["underlying_symbol"] = under
        greeks["quantity"] = int(quantity)
        greeks["position_id"] = pos.get("position_id")
        greeks["instrument"] = instrument or pos.get("instrument") or ""
        greeks["exchange"] = exchange
        greeks["spot"] = fut  # futures underlier used for this greek calc
        greeks["calculated_at"] = datetime.now(timezone.utc).isoformat()
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
    greeks_by_legs: dict[str, dict] = {}
    for g in per_leg:
        for k in ("net_delta", "net_gamma", "net_theta", "net_vega"):
            net[k] += float(g.get(k) or 0.0)
        greeks_by_legs[g.get("instrument")] = {k: round(float(g.get(k) or 0.0), 6) for k in ("quantity", "net_delta", "net_gamma", "net_theta", "net_vega")}

        u = g.get("underlying_symbol")
        if u:
            nd_by_u[u] += float(g.get("net_delta") or 0.0)

    net_greeks = {k: round(v, 6) for k, v in net.items()}
    net_delta_by_underlying = {k: round(v, 6) for k, v in sorted(nd_by_u.items())}
    spot_out = {u: float(spot_by_underlying.get(u) or 0.0) for u in nd_by_u}

    # Near-month futures LTP per underlying (hybrid spot-deviation)
    future_by_underlying: dict[str, float] = {}
    future_expiry_by_underlying: dict[str, str] = {}
    for under in nd_by_u.keys():
        fut = resolve_near_month_future(under)
        if not fut:
            continue
        cash = float(spot_by_underlying.get(under) or 0.0)
        price, _src = get_future_price(
            r, credentials, int(fut["instrument_token"]), spot=cash,
        )
        if price is not None and float(price) > 0:
            future_by_underlying[under] = float(price)
            exp = fut.get("expiry")
            if isinstance(exp, date):
                future_expiry_by_underlying[under] = exp.isoformat()

    # logger.info(f"greeks_by_legs: {greeks_by_legs}")
    logger.info("")
    underlyings = sorted(nd_by_u.keys())

    logger.info(
        "[net-delta] builder_id=%s strategy_id=%s legs=%s "
        "net_delta_by_underlying=%s net_gamma=%.6f cash_spot=%s near_month_fut=%s",
        builder_id,
        strategy_id,
        len(per_leg),
        {u: round(v, 4) for u, v in net_delta_by_underlying.items()},
        net_greeks.get("net_gamma", 0.0),
        {u: round(v, 2) for u, v in spot_out.items()},
        {u: round(v, 2) for u, v in future_by_underlying.items()},
    )
    logger.info("")

    return {
        "builder_id": builder_id,
        "strategy_id": strategy_id,
        "master_profile_id": master_profile_id,
        "master_trade_cycle_id": master_trade_cycle_id,
        "underlyings": underlyings,
        "net_greeks": net_greeks,
        "net_delta_by_underlying": net_delta_by_underlying,
        "spot_by_underlying": spot_out,
        "future_by_underlying": future_by_underlying,
        "future_expiry_by_underlying": future_expiry_by_underlying,
        "per_leg": per_leg,
        "book_positions": book_positions,
        "positions_open": len(positions),
        "positions_greeked": len(per_leg),
    }
