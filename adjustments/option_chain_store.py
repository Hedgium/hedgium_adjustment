"""
In-memory OptionChain store for the stream worker.

Loads full option-chain rows from the backend (strike, expiry, option_type,
lot_size, etc.), then periodically recomputes Black-Scholes Greeks from live
Redis ticks.  Falls back to Kite REST when a tick is not yet in Redis.

Thread-safety: all public methods acquire ``_lock`` so they can be called
from the Greek-update thread and the persist thread simultaneously.
"""

from __future__ import annotations

import logging
import math
import threading
from collections import defaultdict
from datetime import date, datetime
from typing import Optional

import config as cfg
from adjustments.futures_underlier import (
    FUTURES_RISK_FREE_RATE,
    future_tokens_for_pairs,
    get_future_price,
    get_future_price_for_option,
    refresh_nfo_futures,
)
from stream.redis_writer import fetch_tick_by_token, resolve_underlying_zerodha_token, fetch_ltps

logger = logging.getLogger(__name__)

# Kept for MANUAL / legacy call sites that still import the name; AUTO uses futures r=0.
DEFAULT_RISK_FREE_RATE = 0.065

# Maximum tokens to batch in a single Kite REST fallback call.
_KITE_BATCH_SIZE = 500


def _float_or_none(v) -> Optional[float]:
    return float(v) if v is not None else None


def _cash_spot_for_underlying(r, underlying: str) -> float:
    """Cash/index LTP from Redis for MANUAL gamma-adj."""
    u = (underlying or "").strip().upper()
    if not u or r is None:
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
    try:
        return float(fetch_ltps(r).get(utok) or 0)
    except (TypeError, ValueError):
        return 0.0


def _iso_or_none(v) -> Optional[str]:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _seed_computed_from_db_row(entry: dict, row: dict) -> None:
    """
    Seed worker computed Greek fields from persisted OptionChain DB values so
    gamma-adjusted adjustments work before the first update_greeks() cycle.
    """
    calc_by = (row.get("greeks_calculated_by") or "").upper()
    db_delta = _float_or_none(row.get("greeks_delta"))
    if db_delta is None:
        return

    if calc_by == "MANUAL":
        ref_spot = _float_or_none(row.get("manual_delta_spot"))
    else:
        ref_spot = _float_or_none(row.get("auto_delta_spot"))

    if ref_spot is None:
        return

    entry["delta"] = db_delta
    entry["gamma"] = _float_or_none(row.get("greeks_gamma"))
    entry["theta"] = _float_or_none(row.get("greeks_theta"))
    entry["vega"] = _float_or_none(row.get("greeks_vega"))
    entry["computed_at_spot"] = ref_spot
    entry["last_greeks_at"] = _iso_or_none(row.get("greeks_updated_at"))


# ──────────────────────────────────────────────────────────────────────────────
# Black-Scholes helpers (mirrors adjustments/greeks.py but self-contained)
# ──────────────────────────────────────────────────────────────────────────────

def _tte_years(expiry: date) -> float:
    today = datetime.utcnow().date()
    days = max((expiry - today).days, 0)
    return max(days / 365.0, 1 / 365.0)


def _implied_vol(
    flag: str, mid: float, spot: float, strike: float, t: float, r: float
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


def _bs_greeks(
    flag: str, spot: float, strike: float, t: float, r: float, iv: float
) -> dict:
    try:
        from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega

        return {
            "iv": iv,
            "delta": delta(flag, spot, strike, t, r, iv),
            "gamma": gamma(flag, spot, strike, t, r, iv),
            "theta": theta(flag, spot, strike, t, r, iv),
            "vega": vega(flag, spot, strike, t, r, iv),
        }
    except Exception as exc:
        logger.debug("option_chain_store bs_greeks: %s", exc)
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# OptionChainStore
# ──────────────────────────────────────────────────────────────────────────────

class OptionChainStore:
    """
    Thread-safe in-memory store for OptionChain rows + computed Greeks.

    Lifecycle::

        store = OptionChainStore()
        store.load(chains_data)              # called at startup / after reload
        store.update_greeks(r, credentials)  # called every ~90s
        payload = store.get_greeks_payload() # called every ~300s to persist
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # keyed by zerodha_instrument_token
        self._chains: dict[int, dict] = {}
        self._dividend_by_underlying: dict[str, dict] = {}

    # ── public ────────────────────────────────────────────────────────────────

    # Computed Greek fields written by update_greeks().
    # Preserved across load() reloads so the race window between load() and
    # the next update_greeks() cycle does not blank live Greek data.
    _COMPUTED_FIELDS = (
        "iv", "delta", "gamma", "theta", "vega",
        "last_greeks_at", "computed_at_spot",
    )

    def load(
        self,
        chains_data: list[dict],
        dividend_by_underlying: Optional[dict[str, dict]] = None,
    ) -> None:
        """
        Replace the in-memory chain store with rows from the backend API.

        Each item in ``chains_data`` must have at minimum::

            zerodha_instrument_token, underlying_symbol, strike,
            option_type, expiry (ISO str), lot_size

        On reload (store already has data), previously-computed Greek values
        are carried over for tokens that survive the reload so that callers
        always see fresh Greeks rather than a brief None window between load()
        and the next update_greeks() run.
        """
        new = self._build_entries_from_rows(chains_data)

        with self._lock:
            # Carry over previously-computed Greeks for tokens that survive the
            # reload.  This prevents a brief None window (and unwanted BS
            # fallback) between this load() call and the next update_greeks().
            old = self._chains
            for tok, entry in new.items():
                prev = old.get(tok)
                if prev is None:
                    continue
                for field in self._COMPUTED_FIELDS:
                    if prev.get(field) is not None:
                        entry[field] = prev[field]
            self._chains = new
            if dividend_by_underlying is not None:
                self._dividend_by_underlying = dict(dividend_by_underlying)

        logger.info("OptionChainStore: loaded %s rows", len(new))

    def merge_rows(self, chains_data: list[dict]) -> int:
        """
        Add or update rows without replacing the full store.

        Returns the number of new tokens added (existing tokens are updated in
        place but preserve computed Greeks unless the row is brand-new).
        """
        if not chains_data:
            return 0

        incoming = self._build_entries_from_rows(chains_data)
        added = 0

        with self._lock:
            for tok, entry in incoming.items():
                if tok in self._chains:
                    prev = self._chains[tok]
                    for field in self._COMPUTED_FIELDS:
                        if prev.get(field) is not None:
                            entry[field] = prev[field]
                    self._chains[tok].update(entry)
                else:
                    self._chains[tok] = entry
                    added += 1

        if added:
            logger.info("OptionChainStore: merged %s new rows (total=%s)", added, len(self._chains))
        return added

    def _build_entries_from_rows(self, chains_data: list[dict]) -> dict[int, dict]:
        new: dict[int, dict] = {}
        for row in chains_data:
            tok = row.get("zerodha_instrument_token")
            if tok is None:
                continue
            tok = int(tok)
            expiry_raw = row.get("expiry")
            try:
                expiry = date.fromisoformat(expiry_raw) if expiry_raw else None
            except (ValueError, TypeError):
                expiry = None

            new[tok] = {
                "zerodha_instrument_token": tok,
                "underlying_symbol": (row.get("underlying_symbol") or "").upper(),
                "strike": float(row["strike"]) if row.get("strike") is not None else None,
                "option_type": row.get("option_type") or "",
                "expiry": expiry,
                "lot_size": int(row.get("lot_size") or 1),
                "zerodha_tradingsymbol": row.get("zerodha_tradingsymbol") or "",
                "shoonya_tradingsymbol": row.get("shoonya_tradingsymbol") or "",
                "kotakneo_tradingsymbol": row.get("kotakneo_tradingsymbol") or "",
                "exchange": row.get("exchange") or "NFO",
                "strike_distance": row.get("strike_distance") or 0,
                "greeks_calculated_by": row.get("greeks_calculated_by") or None,
                "stored_delta": _float_or_none(row.get("greeks_delta")),
                "stored_gamma": _float_or_none(row.get("greeks_gamma")),
                "stored_vega": _float_or_none(row.get("greeks_vega")),
                "stored_theta": _float_or_none(row.get("greeks_theta")),
                "manual_delta_spot": _float_or_none(row.get("manual_delta_spot")),
                "iv": None,
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
                "last_greeks_at": None,
                "computed_at_spot": None,
            }
            _seed_computed_from_db_row(new[tok], row)
        return new

    def get_tokens(self) -> list[int]:
        with self._lock:
            return list(self._chains.keys())

    def get_all_rows(self) -> list[dict]:
        """Return a snapshot of all chain rows (thread-safe copy)."""
        with self._lock:
            return [dict(r) for r in self._chains.values()]

    def get_chain_by_token(self, token: int) -> Optional[dict]:
        with self._lock:
            row = self._chains.get(int(token))
            return dict(row) if row else None

    def size(self) -> int:
        with self._lock:
            return len(self._chains)

    def underlying_expiry_pairs(self) -> list[tuple[str, date]]:
        """Unique ``(underlying_symbol, expiry)`` pairs present in the store."""
        with self._lock:
            rows = list(self._chains.values())
        pairs: set[tuple[str, date]] = set()
        for row in rows:
            u = (row.get("underlying_symbol") or "").upper()
            exp = row.get("expiry")
            if u and isinstance(exp, date):
                pairs.add((u, exp))
        return sorted(pairs)

    def future_tokens_for_subscription(self, credentials: dict | None = None) -> list[int]:
        """Resolve NFO/BFO FUT tokens needed for Greeks underliers."""
        return future_tokens_for_pairs(self.underlying_expiry_pairs(), credentials)

    def has_fresh_greeks(self) -> bool:
        """True if at least one row has a non-stale gamma-adjustment baseline."""
        with self._lock:
            rows = list(self._chains.values())
        for row in rows:
            if row.get("delta") is None or row.get("computed_at_spot") is None:
                continue
            last_at = row.get("last_greeks_at")
            if not last_at:
                continue
            try:
                age_s = (datetime.utcnow() - datetime.fromisoformat(last_at)).total_seconds()
                if age_s <= cfg.GREEKS_STALE_THRESHOLD_S:
                    return True
            except Exception:
                continue
        return False

    def update_greeks(self, r, credentials: dict) -> int:
        """
        Recompute IV + BS Greeks for every chain row using futures underlier ``F``.

        Underlier: NFO future matched to option expiry; price = liquid LTP else
        bid/ask mid (no dividend).  BS uses ``r=0``.  When both CE and PE mids
        exist for a strike, a shared IV is used so ``|Δ_CE|+|Δ_PE|≈1``.

        Returns the number of tokens successfully updated.
        """
        with self._lock:
            chains_snapshot = dict(self._chains)

        if not chains_snapshot:
            return 0

        api_key = credentials.get("api_key", "")
        access_token = credentials.get("access_token", "")
        refresh_nfo_futures(api_key, access_token)

        # 1. Futures price cache keyed by (underlying, expiry) — AUTO path
        fut_price_cache: dict[tuple[str, date], float] = {}
        fut_meta_cache: dict[tuple[str, date], dict] = {}
        # Cash/index spot per underlying — MANUAL gamma-adj vs manual_delta_spot
        cash_spot_cache: dict[str, float] = {}
        for row in chains_snapshot.values():
            u = (row.get("underlying_symbol") or "").upper()
            exp = row.get("expiry")
            if not u:
                continue
            if u not in cash_spot_cache:
                cash_spot_cache[u] = _cash_spot_for_underlying(r, u)
            if not isinstance(exp, date):
                continue
            key = (u, exp)
            if key in fut_price_cache:
                continue
            price, fut, _src = get_future_price_for_option(r, credentials, u, exp)
            if price is not None and price > 0:
                fut_price_cache[key] = float(price)
            if fut:
                fut_meta_cache[key] = fut

        # 2. Option ticks from Redis; Kite quote fallback for missing
        missing_tokens: list[int] = []
        tick_cache: dict[int, dict] = {}
        for token in chains_snapshot:
            tick = fetch_tick_by_token(r, token)
            if tick:
                tick_cache[token] = tick
            else:
                missing_tokens.append(token)

        if missing_tokens:
            from stream.token_fetcher import fetch_ltps_from_kite, fetch_quotes_from_kite

            for i in range(0, len(missing_tokens), _KITE_BATCH_SIZE):
                batch = missing_tokens[i: i + _KITE_BATCH_SIZE]
                quotes = fetch_quotes_from_kite(api_key, access_token, batch)
                if quotes:
                    for tok, q in quotes.items():
                        tick_cache[tok] = q
                    continue
                # OHLC LTP-only fallback if full quote unavailable
                kite_ltps = fetch_ltps_from_kite(api_key, access_token, batch)
                for tok, ltp in kite_ltps.items():
                    if ltp > 0:
                        tick_cache[tok] = {
                            "last_price": ltp,
                            "bid_price": ltp,
                            "ask_price": ltp,
                        }

        # Also ensure futures quotes are warm in Redis path (batch missing FUT tokens)
        fut_tokens_needed = [
            int(m["instrument_token"])
            for m in fut_meta_cache.values()
            if m.get("instrument_token")
        ]
        missing_futs = [
            t for t in fut_tokens_needed
            if get_future_price(r, None, t)[0] is None
        ]
        if missing_futs:
            from stream.token_fetcher import fetch_quotes_from_kite

            for i in range(0, len(missing_futs), _KITE_BATCH_SIZE):
                batch = missing_futs[i: i + _KITE_BATCH_SIZE]
                fetch_quotes_from_kite(api_key, access_token, batch)
            # Re-resolve prices after quote fetch (get_future_price will use kite)
            for key, meta in list(fut_meta_cache.items()):
                if key in fut_price_cache:
                    continue
                price, _src = get_future_price(r, credentials, int(meta["instrument_token"]))
                if price is not None and price > 0:
                    fut_price_cache[key] = float(price)

        now_iso = datetime.utcnow().isoformat()
        updates: dict[int, dict] = {}
        r_rate = FUTURES_RISK_FREE_RATE

        # 3a. MANUAL path (cash spot vs manual_delta_spot; no BS)
        # 3b. AUTO path (futures F)
        auto_candidates: list[tuple[int, dict, float, float, str]] = []
        # token, row, F, mid, flag

        for token, row in chains_snapshot.items():
            expiry = row.get("expiry")
            strike = row.get("strike")
            option_type = row.get("option_type", "")
            underlying = (row.get("underlying_symbol") or "").upper()

            if not expiry or strike is None or not option_type or not underlying:
                continue

            calc_by = (row.get("greeks_calculated_by") or "").upper()
            if calc_by == "MANUAL":
                stored_delta = row.get("stored_delta")
                if stored_delta is None:
                    continue
                cash_spot = float(cash_spot_cache.get(underlying) or 0.0)
                if cash_spot <= 0:
                    continue
                stored_gamma = float(row.get("stored_gamma") or 0.0)
                stored_vega = float(row.get("stored_vega") or 0.0)
                stored_theta = float(row.get("stored_theta") or 0.0)
                manual_spot = row.get("manual_delta_spot")
                if manual_spot is not None:
                    eff_delta = (
                        (cash_spot - float(manual_spot)) * stored_gamma
                        + float(stored_delta)
                    )
                else:
                    eff_delta = float(stored_delta)
                updates[token] = {
                    "iv": 0.0,
                    "delta": round(eff_delta, 6),
                    "gamma": round(stored_gamma, 8),
                    "theta": round(stored_theta, 6),
                    "vega": round(stored_vega, 6),
                    "last_greeks_at": now_iso,
                    "computed_at_spot": cash_spot,
                }
                continue

            F = fut_price_cache.get((underlying, expiry), 0.0)
            if F <= 0:
                continue

            tick = tick_cache.get(token)
            if not tick:
                continue
            bid = float(tick.get("bid_price") or tick.get("last_price") or 0)
            ask = float(tick.get("ask_price") or tick.get("last_price") or 0)
            ltp = float(tick.get("last_price") or 0)
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, ltp)
            if mid <= 0:
                continue
            flag = "c" if option_type.upper() == "CE" else "p"
            auto_candidates.append((token, row, F, mid, flag))

        # 3b. AUTO path with shared IV per (underlying, expiry, strike)
        groups: dict[tuple, list[tuple[int, dict, float, float, str]]] = defaultdict(list)
        for item in auto_candidates:
            token, row, F, mid, flag = item
            key = (
                row["underlying_symbol"],
                row["expiry"],
                float(row["strike"]),
            )
            groups[key].append(item)

        for _gkey, items in groups.items():
            t = _tte_years(items[0][1]["expiry"])
            F = items[0][2]
            strike = float(items[0][1]["strike"])

            ivs: list[float] = []
            for token, row, _F, mid, flag in items:
                iv = _implied_vol(flag, mid, F, strike, t, r_rate)
                if iv:
                    ivs.append(iv)

            if not ivs:
                continue
            shared_iv = sum(ivs) / len(ivs)

            ce_d = None
            pe_d = None
            for token, row, _F, mid, flag in items:
                g = _bs_greeks(flag, F, strike, t, r_rate, shared_iv)
                if not g:
                    continue
                updates[token] = {
                    "iv": round(g["iv"], 6),
                    "delta": round(g["delta"], 6),
                    "gamma": round(g["gamma"], 8),
                    "theta": round(g["theta"], 6),
                    "vega": round(g["vega"], 6),
                    "last_greeks_at": now_iso,
                    "computed_at_spot": F,
                }
                ot = (row.get("option_type") or "").upper()
                if ot == "CE":
                    ce_d = float(g["delta"])
                elif ot == "PE":
                    pe_d = float(g["delta"])

            if ce_d is not None and pe_d is not None:
                abs_sum = abs(ce_d) + abs(pe_d)
                if abs(abs_sum - 1.0) > cfg.GREEKS_DELTA_SUM_TOLERANCE:
                    logger.warning(
                        "OptionChainStore: delta-sum check failed "
                        "underlying=%s expiry=%s strike=%s "
                        "|dCE|+|dPE|=%.6f (tol=%.4f) shared_iv=%.4f F=%.2f",
                        items[0][1]["underlying_symbol"],
                        items[0][1]["expiry"],
                        strike,
                        abs_sum,
                        cfg.GREEKS_DELTA_SUM_TOLERANCE,
                        shared_iv,
                        F,
                    )

        # 4. Write updates
        with self._lock:
            for token, greeks in updates.items():
                if token in self._chains:
                    self._chains[token].update(greeks)

        updated = len(updates)
        if updated:
            logger.info(
                "OptionChainStore: updated Greeks for %s/%s tokens (futures underlier)",
                updated, len(chains_snapshot),
            )
        else:
            logger.debug(
                "OptionChainStore: no Greeks updated (total=%s fut_prices=%s)",
                len(chains_snapshot), len(fut_price_cache),
            )

        return updated

    def get_greeks_payload(self) -> list[dict]:
        """
        Return a list of dicts ready for ``POST /internal/greeks/bulk-upsert``.
        Only includes tokens that have successfully computed Greeks.
        Keys match OptionChain model field names (greeks_delta, etc.).
        Includes ``auto_delta_spot`` so the DB always reflects the spot at
        which the worker last computed Greeks (prevents stale baseline spot).
        """
        with self._lock:
            rows = list(self._chains.values())

        payload = []
        for row in rows:
            # Only persist rows where Greeks have been computed
            if row.get("delta") is None:
                continue
            payload.append({
                "zerodha_instrument_token": row["zerodha_instrument_token"],
                "greeks_delta": row.get("delta"),
                "greeks_gamma": row.get("gamma"),
                "greeks_vega":  row.get("vega"),
                "greeks_theta": row.get("theta"),
                "auto_delta_spot": row.get("computed_at_spot"),
            })
        return payload

    def underlying_symbols(self) -> list[str]:
        with self._lock:
            return sorted({r["underlying_symbol"] for r in self._chains.values() if r.get("underlying_symbol")})
