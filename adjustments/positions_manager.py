"""
Live broker positions manager for the stream worker.

Periodically fetches live positions directly from the broker via the
``GET /internal/positions/live/{profile_id}`` endpoint (one call per
builder's master profile).  Profile IDs are supplied by the caller from
the ``adjustments/builders`` response (``master_profile_id`` field).

Falls back to the aggregated ``GET /internal/positions/live`` endpoint if
no profile IDs are provided.

Maps positions to active builders by underlying symbol and exposes the
instrument tokens so that the WebSocket subscription set can be updated
dynamically.
"""

from __future__ import annotations

import copy
import logging
import threading
from datetime import date
from typing import Optional

from client import backend_api

logger = logging.getLogger(__name__)


class LivePositionsManager:
    """
    Manages live broker positions and their mapping to strategy builders.

    Thread-safety: all mutable state is protected by ``_lock``.  The refresh
    loop in ``main.py`` calls ``refresh()`` from its own thread; ``map_to_builders``
    is called from the ``AdjustmentRunner`` thread.

    Usage::

        pm = LivePositionsManager()
        pm.set_profile_ids([101, 203])   # from adjustments/builders response
        changed = pm.refresh()           # fetch from broker via backend
        builders_live = pm.map_to_builders(builders_raw)
        new_tokens = pm.get_all_tokens()
    """

    def __init__(self, option_chain_store=None) -> None:
        self._lock = threading.Lock()
        self._positions: list[dict] = []
        self._tokens: set[int] = set()
        self._last_signature: str = ""
        self._profile_ids: list[int] = []
        self._option_chain_store = option_chain_store

    def set_profile_ids(self, profile_ids: list[int]) -> None:
        """Update the set of master profile IDs to fetch positions for."""
        with self._lock:
            self._profile_ids = list(profile_ids)

    # ── public ────────────────────────────────────────────────────────────────

    def refresh(self) -> bool:
        """
        Fetch live positions directly from the broker for each master profile
        and update internal state.

        Each profile is queried via ``GET /internal/positions/live/{profile_id}``
        which calls the broker API directly (not the DB positions table).

        Falls back to the aggregated ``GET /internal/positions/live`` if no
        profile IDs have been set yet.

        Returns ``True`` if the set of instrument tokens changed (caller should
        then update WebSocket subscriptions), ``False`` otherwise.
        """
        with self._lock:
            profile_ids = list(self._profile_ids)

        if profile_ids:
            raw_positions = self._fetch_by_profiles(profile_ids)
        else:
            # Fallback: use aggregated endpoint (complex builder→TC→profile lookup)
            raw_positions = self._fetch_aggregated()

        if not raw_positions:
            logger.debug("LivePositionsManager.refresh: no live positions returned")
        else:
            logger.info(
                "LivePositionsManager.refresh: %s live positions across %s profile(s)",
                len(raw_positions), len(profile_ids) if profile_ids else "?",
            )

        new_tokens: set[int] = set()
        for pos in raw_positions:
            tok = pos.get("instrument_token")
            if tok is not None:
                try:
                    new_tokens.add(int(tok))
                except (TypeError, ValueError):
                    pass

        # Signature: sorted token list as string — cheap change detector
        new_sig = ",".join(str(t) for t in sorted(new_tokens))
        changed = new_sig != self._last_signature

        with self._lock:
            self._positions = raw_positions
            self._tokens = new_tokens
            self._last_signature = new_sig

        if changed:
            logger.info(
                "LivePositionsManager.refresh: token set changed → %s tokens",
                len(new_tokens),
            )

    def _fetch_by_profiles(self, profile_ids: list[int]) -> list[dict]:
        """
        Call ``GET /internal/positions/live/{profile_id}`` for each profile.

        The per-profile endpoint calls the broker API directly and returns raw
        net positions (tradingsymbol, quantity, exchange, instrument_token,
        average_price, last_price, product, …).

        We enrich each position with OptionChain metadata so the result has the
        same shape that ``map_to_builders`` expects: underlying_symbol, strike,
        option_type, expiry, lot_size, instrument_token (zerodha).
        """
        from client import backend_api as _api

        all_positions: list[dict] = []
        for pid in profile_ids:
            try:
                resp = _api.get_live_positions_for_profile(pid)
            except Exception as exc:
                logger.warning(
                    "LivePositionsManager: profile_id=%s fetch error: %s", pid, exc
                )
                continue

            if resp.get("status") == "error" or resp.get("detail"):
                logger.warning(
                    "LivePositionsManager: profile_id=%s broker error: %s",
                    pid, resp.get("detail") or resp.get("status"),
                )
                continue

            net = (resp.get("data") or {}).get("net") or []
            for pos in net:
                qty = pos.get("quantity") or 0
                try:
                    qty = float(qty)
                except (TypeError, ValueError):
                    qty = 0.0
                if qty == 0:
                    continue
                p = dict(pos)
                p["profile_id"] = pid
                all_positions.append(p)

        if not all_positions:
            return []

        # Enrich with OptionChain metadata (underlying_symbol, strike, expiry, …)
        if self._option_chain_store is not None:
            from optionchain_lookup import enrich_positions_with_option_chain
            return enrich_positions_with_option_chain(all_positions, self._option_chain_store)

        # If no store yet (e.g. called before first load), fall back to aggregated
        logger.warning(
            "LivePositionsManager: no option_chain_store — falling back to aggregated endpoint"
        )
        return self._fetch_aggregated()

    def _fetch_aggregated(self) -> list[dict]:
        """Fallback: use the aggregated backend endpoint (legacy path)."""
        try:
            resp = backend_api.get_live_positions()
        except Exception as exc:
            logger.warning("LivePositionsManager._fetch_aggregated error: %s", exc)
            return []
        return resp.get("positions") or []

        return changed

    def get_all_tokens(self) -> set[int]:
        """Return the set of all instrument tokens from live positions."""
        with self._lock:
            return set(self._tokens)

    def get_positions(self) -> list[dict]:
        """Return a snapshot of the current live positions list."""
        with self._lock:
            return copy.deepcopy(self._positions)

    def map_to_builders(self, builders: list[dict]) -> list[dict]:
        """
        Replace each builder's ``positions`` list with live broker positions
        that match the builder's underlying symbols (derived from its legs).

        Falls back to the original book positions if no live positions are
        found for a builder's underlyings (so Greek computation still runs).

        Returns a new list; the original ``builders`` dicts are not mutated.
        """
        with self._lock:
            live_positions = list(self._positions)

        if not live_positions:
            return builders

        # Group live positions by underlying_symbol
        live_by_underlying: dict[str, list[dict]] = {}
        for pos in live_positions:
            u = (pos.get("underlying_symbol") or "").strip().upper()
            if not u:
                continue
            live_by_underlying.setdefault(u, []).append(pos)

        result: list[dict] = []
        for builder in builders:
            b = dict(builder)

            # Collect underlyings from legs
            underlyings: set[str] = set()
            for leg in b.get("legs") or []:
                sym = (leg.get("symbol") or "").strip().upper()
                if sym:
                    underlyings.add(sym)

            # Gather live positions that belong to these underlyings
            matched: list[dict] = []
            for u in underlyings:
                matched.extend(live_by_underlying.get(u) or [])

            if matched:
                # Normalize live positions into the same shape as book positions
                b["positions"] = [_normalize_live_position(pos) for pos in matched]
                logger.debug(
                    "LivePositionsManager.map_to_builders: builder_id=%s → "
                    "%s live positions (underlyings=%s)",
                    b.get("builder_id"), len(matched), sorted(underlyings),
                )
            else:
                logger.debug(
                    "LivePositionsManager.map_to_builders: builder_id=%s — "
                    "no live match for underlyings=%s, keeping book positions",
                    b.get("builder_id"), sorted(underlyings),
                )

            result.append(b)

        return result


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_live_position(pos: dict) -> dict:
    """
    Convert a live-broker position dict (from /internal/positions/live) to the
    shape expected by ``adjustments/greeks.py::compute_greeks_for_builder``.

    The backend endpoint already resolves ``instrument_token`` to the Zerodha
    token for all broker types (Zerodha, Shoonya, KotakNeo), so we can use it
    directly for tick lookups in Redis.

    The builder data from ``adjustments_builders`` uses::

        {position_id, instrument, quantity, exchange,
         zerodha_instrument_token, underlying_symbol, strike,
         option_type, expiry (ISO str), lot_size}
    """
    expiry_raw = pos.get("expiry")
    qty = pos.get("quantity") or 0
    try:
        qty_int = int(float(qty))
    except (TypeError, ValueError):
        qty_int = 0
    return {
        "position_id": None,
        "instrument": pos.get("tradingsymbol") or "",
        "quantity": qty_int,
        "exchange": (pos.get("exchange") or "NFO").strip().upper() or "NFO",
        "zerodha_instrument_token": pos.get("instrument_token"),
        "underlying_symbol": (pos.get("underlying_symbol") or "").strip().upper() or None,
        "strike": pos.get("strike"),
        "option_type": pos.get("option_type"),
        "expiry": expiry_raw,
        "lot_size": int(pos.get("lot_size") or 1),
        "broker_name": pos.get("broker_name") or "",
    }
